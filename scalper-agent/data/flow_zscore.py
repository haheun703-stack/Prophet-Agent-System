# -*- coding: utf-8 -*-
"""
Flow Z-Score  --  시장 수급 통계적 이상치 감지
==================================================
KISOpenAPI 봇의 Z-score 정규화 기법을 Body Hunter BRAIN에 통합.

Z = (현재값 - 20일 평균) / (20일 표준편차 + 1e-9)

기관/외인 Z-score >= 2.0  : 2-sigma 이상 이례적 매수 → STRONG_BUY
기관/외인 Z-score <= -2.0 : 2-sigma 이상 이례적 매도 → SELL

데이터: pykrx.stock.get_market_trading_value_by_date (KOSPI/KOSDAQ)
출력:   data_store/flow_zscore.json
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Tuple

logger = logging.getLogger("BH.FlowZ")

DATA_DIR = Path(__file__).parent.parent / "data_store"
ZSCORE_PATH = DATA_DIR / "flow_zscore.json"

# Z-score 롤링 윈도우 (영업일)
LOOKBACK = 20
# 캘린더일 기준 여유 (20영업일 확보용)
CALENDAR_DAYS = 45
# 최소 데이터 일수
MIN_DAYS = 5


# ─────────────────────────────────────
#  Core: Z-score 계산
# ─────────────────────────────────────

def _calc_zscore(values: list, lookback: int = LOOKBACK) -> float:
    """리스트의 마지막 값에 대한 롤링 Z-score 계산.

    Z = (values[-1] - mean(values[-lookback:])) / (std(values[-lookback:]) + 1e-9)
    """
    clean = [v for v in values if v is not None and v == v]  # NaN 제거
    if len(clean) < MIN_DAYS:
        return 0.0

    window = clean[-lookback:] if len(clean) >= lookback else clean
    current = window[-1]

    n = len(window)
    mean_val = sum(window) / n
    variance = sum((x - mean_val) ** 2 for x in window) / n
    std_val = variance ** 0.5

    return (current - mean_val) / (std_val + 1e-9)


# ─────────────────────────────────────
#  pykrx 시장 수급 데이터 수집
# ─────────────────────────────────────

def _fetch_market_trading_data(market: str = "KOSPI",
                                days: int = CALENDAR_DAYS) -> list:
    """pykrx로 시장별 투자자 매매동향 가져오기.

    _calc_market_flow() (market_brain.py) 패턴과 동일.
    주말/공휴일에는 KRX 서버가 빈 응답을 줄 수 있으므로 최대 3일 전까지 재시도.

    Returns: [{"date": str, "inst": float, "foreign": float, "indiv": float}, ...]
    """
    from pykrx import stock

    # 비거래일 대응: 오늘부터 최대 3일 전까지 시도
    df = None
    for offset in range(4):
        end = date.today() - timedelta(days=offset)
        start = end - timedelta(days=days)
        try:
            df = stock.get_market_trading_value_by_date(
                start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), market
            )
            if df is not None and len(df) >= MIN_DAYS:
                break
        except Exception:
            continue
        df = None

    if df is None or len(df) < MIN_DAYS:
        return []

    # 컬럼명 매칭
    inst_col = "기관합계" if "기관합계" in df.columns else None
    foreign_col = "외국인합계" if "외국인합계" in df.columns else None
    indiv_col = "개인" if "개인" in df.columns else None

    if not inst_col or not foreign_col:
        logger.warning(f"[FlowZ] {market} 컬럼 매칭 실패: {list(df.columns)}")
        return []

    result = []
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)
        result.append({
            "date": d,
            "inst": float(row.get(inst_col, 0)),
            "foreign": float(row.get(foreign_col, 0)),
            "indiv": float(row.get(indiv_col, 0)) if indiv_col else 0.0,
        })
    return result


# ─────────────────────────────────────
#  시그널 도출
# ─────────────────────────────────────

def _derive_signal(combined_z: dict) -> Tuple[str, str, float]:
    """Z-score 조합에서 시그널 + BRAIN 보정점수 도출.

    Returns: (signal_text, signal_level, brain_adj)
    """
    iz = combined_z.get("inst_z", 0.0)
    fz = combined_z.get("foreign_z", 0.0)

    # 시그널 매트릭스
    if iz >= 2.0 and fz >= 2.0:
        level = "STRONG_BUY"
        adj = 5.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 2-sigma 동반 폭발매수"
    elif iz >= 1.5 and fz >= 1.0:
        level = "BUY"
        adj = 3.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 기관 주도 강매수"
    elif fz >= 1.5 and iz >= 1.0:
        level = "BUY"
        adj = 3.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 외인 주도 강매수"
    elif iz >= 1.0 and fz >= 1.0:
        level = "BUY"
        adj = 2.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 평균 이상 동반매수"
    elif iz <= -2.0 and fz <= -2.0:
        level = "SELL"
        adj = -5.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 2-sigma 동반 폭발매도"
    elif (iz <= -1.5 and fz <= -1.0) or (fz <= -1.5 and iz <= -1.0):
        level = "CAUTION"
        adj = -3.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 동반 순매도"
    elif (iz >= 1.5 and fz <= -1.0) or (fz >= 1.5 and iz <= -1.0):
        level = "DIVERGENCE"
        adj = 0.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 수급 괴리"
    else:
        level = "NEUTRAL"
        adj = 0.0
        txt = f"기관Z{iz:+.1f} 외인Z{fz:+.1f} 정상 범위"

    return txt, level, adj


# ─────────────────────────────────────
#  메인: 시장 Z-score 계산
# ─────────────────────────────────────

def calc_market_zscore(lookback: int = LOOKBACK) -> dict:
    """KOSPI + KOSDAQ 시장 수급 Z-score 계산.

    Returns: {kospi: {...}, kosdaq: {...}, combined: {...},
              signal, signal_level, brain_adj, data_date, computed_at}
    """
    result = {
        "kospi": {}, "kosdaq": {}, "combined": {},
        "signal": "", "signal_level": "NEUTRAL",
        "brain_adj": 0.0,
        "data_date": "", "computed_at": "",
    }

    markets = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        data = _fetch_market_trading_data(mkt, CALENDAR_DAYS)
        if not data:
            logger.warning(f"[FlowZ] {mkt} 데이터 수집 실패")
            continue
        markets[mkt.lower()] = data

    if not markets:
        result["signal"] = "데이터 수집 실패"
        return result

    # 시장별 Z-score 계산
    for mkt_key, data in markets.items():
        inst_vals = [d["inst"] for d in data]
        foreign_vals = [d["foreign"] for d in data]
        indiv_vals = [d["indiv"] for d in data]

        inst_z = _calc_zscore(inst_vals, lookback)
        foreign_z = _calc_zscore(foreign_vals, lookback)
        indiv_z = _calc_zscore(indiv_vals, lookback)

        # 억 단위 변환 (pykrx는 원 단위)
        last = data[-1]
        result[mkt_key] = {
            "inst_z": round(inst_z, 2),
            "foreign_z": round(foreign_z, 2),
            "indiv_z": round(indiv_z, 2),
            "inst_raw_억": round(last["inst"] / 1e8, 0),
            "foreign_raw_억": round(last["foreign"] / 1e8, 0),
            "indiv_raw_억": round(last["indiv"] / 1e8, 0),
            "data_days": min(len(data), lookback),
        }

    # Combined Z-score (KOSPI 가중 0.7 + KOSDAQ 0.3)
    kospi = result.get("kospi", {})
    kosdaq = result.get("kosdaq", {})
    if kospi and kosdaq:
        result["combined"] = {
            "inst_z": round(kospi.get("inst_z", 0) * 0.7
                            + kosdaq.get("inst_z", 0) * 0.3, 2),
            "foreign_z": round(kospi.get("foreign_z", 0) * 0.7
                               + kosdaq.get("foreign_z", 0) * 0.3, 2),
            "indiv_z": round(kospi.get("indiv_z", 0) * 0.7
                             + kosdaq.get("indiv_z", 0) * 0.3, 2),
        }
    elif kospi:
        result["combined"] = {
            "inst_z": kospi.get("inst_z", 0),
            "foreign_z": kospi.get("foreign_z", 0),
            "indiv_z": kospi.get("indiv_z", 0),
        }

    # 시그널 도출
    signal_txt, signal_level, brain_adj = _derive_signal(result["combined"])
    result["signal"] = signal_txt
    result["signal_level"] = signal_level
    result["brain_adj"] = brain_adj

    # 데이터 날짜
    for mkt_data in markets.values():
        if mkt_data:
            result["data_date"] = mkt_data[-1]["date"]
            break
    result["computed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return result


# ─────────────────────────────────────
#  영속화
# ─────────────────────────────────────

def save_flow_zscore(result: dict) -> None:
    """flow_zscore.json 원자적 저장."""
    tmp = ZSCORE_PATH.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        tmp.replace(ZSCORE_PATH)
    except Exception as e:
        logger.error(f"[FlowZ] 저장 실패: {e}")
        if tmp.exists():
            tmp.unlink()


def load_flow_zscore() -> dict:
    """flow_zscore.json 로드."""
    if not ZSCORE_PATH.exists():
        return {}
    try:
        with open(ZSCORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ─────────────────────────────────────
#  BRAIN 연동 API
# ─────────────────────────────────────

def get_brain_flow_zscore_adj() -> Tuple[float, str]:
    """BRAIN에서 호출하는 API.

    1. flow_zscore.json 캐시 확인 (오늘 또는 최근 3일 이내면 재사용)
    2. 캐시 없으면 calc_market_zscore() 실행 + 저장
    3. (brain_adj, signal_text) 반환

    비거래일(주말/공휴일)에는 가장 최근 거래일 캐시를 재사용.

    Returns: (brain_adj: float, detail: str)
    """
    try:
        cached = load_flow_zscore()
        today = date.today()

        # 캐시가 최근 3일 이내면 재사용 (주말/공휴일 대응)
        if cached.get("data_date"):
            try:
                cache_date = datetime.strptime(cached["data_date"], "%Y-%m-%d").date()
                if (today - cache_date).days <= 3:
                    return cached.get("brain_adj", 0.0), cached.get("signal", "")
            except (ValueError, TypeError):
                pass

        # 신규 계산
        result = calc_market_zscore()
        if result.get("combined"):
            save_flow_zscore(result)
            logger.info(f"[FlowZ] Z-score 계산 완료: {result['signal_level']} "
                        f"adj={result['brain_adj']:+.1f}")
            return result.get("brain_adj", 0.0), result.get("signal", "")

        # 신규 계산 실패 시 기존 캐시 fallback (stale이라도)
        if cached.get("brain_adj") is not None and cached.get("signal"):
            logger.info("[FlowZ] 신규 계산 실패 — 기존 캐시 재사용")
            return cached.get("brain_adj", 0.0), cached.get("signal", "")

        return result.get("brain_adj", 0.0), result.get("signal", "")

    except Exception as e:
        logger.warning(f"[FlowZ] BRAIN 연동 실패: {e}")
        return 0.0, ""


# ─────────────────────────────────────
#  CLI 테스트
# ─────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    print("=" * 60)
    print("  Flow Z-Score 계산 시작")
    print("=" * 60)

    result = calc_market_zscore()

    if result.get("kospi"):
        k = result["kospi"]
        print(f"\n[KOSPI]")
        print(f"  기관  Z: {k['inst_z']:+.2f}  (당일 {k['inst_raw_억']:+,.0f}억)")
        print(f"  외인  Z: {k['foreign_z']:+.2f}  (당일 {k['foreign_raw_억']:+,.0f}억)")
        print(f"  개인  Z: {k['indiv_z']:+.2f}  (당일 {k['indiv_raw_억']:+,.0f}억)")
        print(f"  데이터: {k['data_days']}일")

    if result.get("kosdaq"):
        q = result["kosdaq"]
        print(f"\n[KOSDAQ]")
        print(f"  기관  Z: {q['inst_z']:+.2f}  (당일 {q['inst_raw_억']:+,.0f}억)")
        print(f"  외인  Z: {q['foreign_z']:+.2f}  (당일 {q['foreign_raw_억']:+,.0f}억)")
        print(f"  개인  Z: {q['indiv_z']:+.2f}  (당일 {q['indiv_raw_억']:+,.0f}억)")

    if result.get("combined"):
        c = result["combined"]
        print(f"\n[COMBINED]")
        print(f"  기관  Z: {c['inst_z']:+.2f}")
        print(f"  외인  Z: {c['foreign_z']:+.2f}")
        print(f"  개인  Z: {c['indiv_z']:+.2f}")

    print(f"\n시그널:    {result['signal']}")
    print(f"레벨:      {result['signal_level']}")
    print(f"BRAIN adj: {result['brain_adj']:+.1f}")
    print(f"기준일:    {result['data_date']}")
    print("=" * 60)

    save_flow_zscore(result)
    print(f"\n저장 완료: {ZSCORE_PATH}")
