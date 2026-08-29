# -*- coding: utf-8 -*-
"""
상한가 눌림목 엔진 v3.0 (Limit-Up Pullback Engine)
====================================================
백테스트 확정 파라미터 기반 시그널 감지 + 감시풀 관리

v3.0 핵심 업그레이드:
  ■ 6자 수급 통합 분석 (외인/기관/연기금/금투/개인/기타법인)
  ■ 수급 생존 필터 — 급등 후 스마트머니 잔류 확인
  ■ 섹터 로테이션 연동 — EARLY/MID 우선, LATE/REVERSAL 경고
  ■ 눌림 진입 정밀화 — 매도소진 + 거래량압축 동시 충족

2층 구조:
  1층: 3번째+ 상한가 → 수급생존 확인 후 분할매수
  2층: 10%+ 급등(품질유니버스) → 5일 감시 → 수급생존+눌림+로테이션 확인 시 분할매수

수급 분석 (v3 6자 통합):
  - 외인/기관/연기금/금투/개인/기타법인 10일 순매수
  - 수급등급: S(강매집)/A(매집중)/B(중립)/C(이탈중)/F(강이탈)
  - 매도소진율 + 거래량압축률
  - 섹터 로테이션 페이즈 (EARLY/MID/LATE/REVERSAL/STAGING)
  - 분할매수 3단계 계획 (1차 50% / 2차 30% / 3차 20%)

백테스트 확정 필터:
  - 이력 2회+ (3번째 상한가부터)
  - 직전 상한가로부터 10일 이내
  - 첫 상한가 원점 대비 +300% 미만 (과열 필터)
  - 주가 >= 1,000원

Usage:
  python -m data.limit_up_engine                # 일일 스캔 + 시그널 생성
  python -m data.limit_up_engine --check        # 감시풀 눌림목 체크만
  python -m data.limit_up_engine --status       # 현재 감시풀 상태 출력
"""
import json
import logging
import sys
from dataclasses import dataclass, field, asdict

# Windows cp949 이모지 출력 에러 방지 (pythonw.exe 대응)
if sys.platform == "win32":
    for _stream in ("stdout", "stderr"):
        _s = getattr(sys, _stream, None)
        if _s and hasattr(_s, "reconfigure"):
            try:
                _s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from data.limit_up_scanner import score_continuation, LimitUpStock
from data.sajang_rules import SAJANG
from utils.stock_utils import is_etf as _is_etf, load_daily

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
UNIVERSE_PATH = DATA_DIR / "universe.json"
LIMIT_UP_DIR = DATA_DIR / "limit_up"

# 엔진 데이터 파일
WATCHLIST_PATH = LIMIT_UP_DIR / "watchlist.json"
SIGNALS_PATH = LIMIT_UP_DIR / "signals.json"
SIGNAL_HISTORY_PATH = LIMIT_UP_DIR / "signal_history.json"

logger = logging.getLogger("BH.LimitUpEngine")

# ── 백테스트 확정 파라미터 ──
LIMIT_UP_PCT = 29.5          # 상한가 기준 (29.5%+)
STRONG_SURGE_PCT = 10.0      # 급등 기준 (10%+ → 품질유니버스 필터와 결합)
MIN_LIMIT_HISTORY = 2        # 최소 이전 상한가 횟수 (3번째부터 진입)
MAX_DAYS_BETWEEN = 10        # 직전 상한가로부터 최대 허용 간격 (영업일)
MAX_OVERHEAT_PCT = 300.0     # 원점 대비 최대 상승률 (과열 필터)
MIN_PRICE = 1000             # 최소 주가 (원)
PULLBACK_TRIGGER_PCT = -10.0 # 눌림목 진입 트리거 (고점 대비 %)
MONITOR_DAYS = 5             # 눌림목 감시 기간 (영업일) — 10%급등→연속급등→눌림 패턴 대응
TP_PCT = SAJANG.FIXED_TP_FORCE_ZERO
MAX_HOLD_DAYS = 20           # 최대 보유 기간 (영업일)


# ═══════════════════════════════════════════════════════
#  데이터 로드 유틸
# ═══════════════════════════════════════════════════════

def _load_daily(code: str) -> pd.DataFrame | None:
    return load_daily(code, DAILY_DIR)


def _load_universe() -> dict:
    """유니버스 로드"""
    if not UNIVERSE_PATH.exists():
        return {}
    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)



# ═══════════════════════════════════════════════════════
#  0. 6자 수급 통합 분석 (v3.0)
# ═══════════════════════════════════════════════════════

def _analyze_flow_6(code: str) -> dict:
    """6자 수급 통합 분석 — 외인/기관/연기금/금투/개인/기타법인

    v3 변경: 2자→6자 확대, 금액 가중 등급, 매도소진/거래량압축 포함

    Returns:
        {
            "foreign_5d": int,        # 5일 외국인 순매수 (백만원)
            "inst_5d": int,           # 5일 기관 순매수 (백만원)
            "pension_5d": int,        # 5일 연기금 순매수 (백만원)
            "fi_5d": int,             # 5일 금투 순매수 (백만원)
            "smart_total_5d": int,    # 스마트머니 합산 (외인+기관+연기금+금투)
            "grade": str,             # 수급등급: S/A/B/C/F/미확인
            "grade_label": str,       # 강매집/매집중/중립/이탈중/강이탈
            "foreign_trend": str,     # 외국인 추세
            "inst_trend": str,        # 기관 추세
            "exh_rate": float,        # 외국인 소진율 (%)
            "sell_exhaustion": float,  # 매도소진율 (개인, 1.0 미만=소진)
            "vol_compression": float,  # 거래량압축률 (1.0 미만=축적)
            "smart_consec": int,      # 스마트머니 연속매수 일수
            "dual_buy": bool,         # 외인+기관 동시 연속매수
            "survival_ok": bool,      # 수급 생존 필터 통과
            "survival_reason": str,   # 생존/탈락 사유
        }
    """
    import csv as csv_mod
    from data.flow_collector import load_investor_flow, load_foreign_exhaustion

    result = {
        "foreign_5d": 0, "inst_5d": 0, "pension_5d": 0, "fi_5d": 0,
        "smart_total_5d": 0,
        "grade": "미확인", "grade_label": "미확인",
        "foreign_trend": "", "inst_trend": "",
        "exh_rate": 0.0,
        "sell_exhaustion": 1.0, "vol_compression": 1.0,
        "smart_consec": 0, "dual_buy": False,
        "survival_ok": False, "survival_reason": "데이터 부족",
    }

    # ── 4자 데이터 (flow CSV: 외인/기관/개인/기타법인) ──
    flow_df = load_investor_flow(code)
    frgn_series, inst_series, indi_series = [], [], []

    if flow_df is not None and len(flow_df) >= 3:
        n = min(10, len(flow_df))
        recent = flow_df.tail(n)
        n5 = min(5, len(recent))
        last5 = recent.tail(n5)

        f_col = "외국인_금액" if "외국인_금액" in recent.columns else None
        i_col = "기관_금액" if "기관_금액" in recent.columns else None
        ind_col = "개인_금액" if "개인_금액" in recent.columns else None

        if f_col:
            result["foreign_5d"] = int(last5[f_col].sum() / 1_000_000)
            frgn_series = (recent[f_col] / 1_000_000).tolist()
            f_buy_days = int((last5[f_col] > 0).sum())
            result["foreign_trend"] = (
                "연속매수" if f_buy_days >= n5 - 1
                else ("연속매도" if f_buy_days <= 1 else "혼조")
            )
        if i_col:
            result["inst_5d"] = int(last5[i_col].sum() / 1_000_000)
            inst_series = (recent[i_col] / 1_000_000).tolist()
            i_buy_days = int((last5[i_col] > 0).sum())
            result["inst_trend"] = (
                "연속매수" if i_buy_days >= n5 - 1
                else ("연속매도" if i_buy_days <= 1 else "혼조")
            )
        if ind_col:
            indi_series = (recent[ind_col] / 1_000_000).tolist()

    # ── 2자 데이터 (quant_investor_extra.json: 연기금/금투) ──
    pension_series, fi_series = [], []
    try:
        pension_json = DATA_DIR / "quant_investor_extra.json"
        if pension_json.exists():
            import json as _json
            raw = _json.loads(pension_json.read_text("utf-8"))
            stock_dates = raw.get("daily", {}).get(code, {}).get("dates", {})
            if stock_dates:
                # 최근 10일 키 정렬
                sorted_keys = sorted(stock_dates.keys())[-10:]
                for dk in sorted_keys:
                    dd = stock_dates[dk]
                    pension_series.append(dd.get("pension_net", 0) * 100)  # 억→백만
                    fi_series.append(dd.get("finance_net", 0) * 100)
                if len(pension_series) >= 5:
                    result["pension_5d"] = int(sum(pension_series[-5:]))
                    result["fi_5d"] = int(sum(fi_series[-5:]))
    except Exception:
        pass

    # ── 스마트머니 합산 ──
    result["smart_total_5d"] = (
        result["foreign_5d"] + result["inst_5d"]
        + result["pension_5d"] + result["fi_5d"]
    )

    # ── 연속매수 일수 (스마트머니) ──
    if frgn_series and inst_series:
        smart_combined = [
            (frgn_series[i] if i < len(frgn_series) else 0)
            + (inst_series[i] if i < len(inst_series) else 0)
            + (pension_series[i] if i < len(pension_series) else 0)
            + (fi_series[i] if i < len(fi_series) else 0)
            for i in range(max(len(frgn_series), len(inst_series)))
        ]
        consec = 0
        for v in reversed(smart_combined):
            if v > 0:
                consec += 1
            else:
                break
        result["smart_consec"] = consec

        # 쌍매수 (외인+기관 동시 연속매수 2일+)
        f_c, i_c = 0, 0
        for v in reversed(frgn_series):
            if v > 0:
                f_c += 1
            else:
                break
        for v in reversed(inst_series):
            if v > 0:
                i_c += 1
            else:
                break
        result["dual_buy"] = f_c >= 2 and i_c >= 2

    # ── 매도 소진율 (개인) ──
    if indi_series and len(indi_series) >= 5:
        sell_3d = sum(abs(min(0, v)) for v in indi_series[-3:]) / 3
        sell_avg = sum(abs(min(0, v)) for v in indi_series) / len(indi_series)
        if sell_avg > 0:
            result["sell_exhaustion"] = round(sell_3d / sell_avg, 2)

    # ── 거래량 압축률 ──
    all_series = [frgn_series, inst_series, indi_series]
    min_len = min(len(s) for s in all_series)
    if min_len >= 5:
        turnover = []
        for i in range(min_len):
            t = abs(frgn_series[i]) + abs(inst_series[i]) + abs(indi_series[i])
            turnover.append(t)
        if turnover:
            vol_3d = sum(turnover[-3:]) / 3
            vol_avg = sum(turnover) / len(turnover)
            if vol_avg > 0:
                result["vol_compression"] = round(vol_3d / vol_avg, 2)

    # ── 수급등급 (v3: 금액 가중 + 일관성) ──
    smart = result["smart_total_5d"]
    s_consec = result["smart_consec"]

    if smart > 0 and s_consec >= 3 and result["sell_exhaustion"] < 0.7:
        result["grade"] = "S"
        result["grade_label"] = "강매집"
    elif smart > 0 and (s_consec >= 2 or result["dual_buy"]):
        result["grade"] = "A"
        result["grade_label"] = "매집중"
    elif smart >= 0:
        result["grade"] = "B"
        result["grade_label"] = "중립"
    elif smart < 0 and s_consec == 0:
        result["grade"] = "C"
        result["grade_label"] = "이탈중"
    elif smart < 0 and result["sell_exhaustion"] > 1.0:
        result["grade"] = "F"
        result["grade_label"] = "강이탈"
    else:
        result["grade"] = "B"
        result["grade_label"] = "중립"

    # ── 외국인 소진율 ──
    try:
        exh_df = load_foreign_exhaustion(code)
        if exh_df is not None and len(exh_df) > 0:
            result["exh_rate"] = float(exh_df["소진율"].iloc[-1])
    except Exception:
        pass

    # ── 수급 생존 필터 판정 ──
    # "급등 후 스마트머니가 빠졌으면 → 로테이션 때 안 돌아온다"
    if result["grade"] in ("S", "A"):
        result["survival_ok"] = True
        result["survival_reason"] = f"스마트머니 유지 ({result['grade_label']})"
    elif result["grade"] == "B":
        # 중립이라도 매도소진+거래량압축이면 코일 상태 → 허용
        if result["sell_exhaustion"] < 0.8 and result["vol_compression"] < 0.85:
            result["survival_ok"] = True
            result["survival_reason"] = "중립이나 코일 상태 (소진+압축)"
        elif result["smart_consec"] >= 1:
            result["survival_ok"] = True
            result["survival_reason"] = "중립이나 직전 매수 유지"
        else:
            result["survival_ok"] = False
            result["survival_reason"] = "스마트머니 방향 불확실"
    else:
        result["survival_ok"] = False
        result["survival_reason"] = f"스마트머니 이탈 ({result['grade_label']})"

    return result


def _get_sector_rotation(sector: str) -> dict:
    """섹터 로테이션 페이즈 조회

    Returns:
        {
            "phase": "EARLY/MID/LATE/REVERSAL/STAGING/DORMANT/미확인",
            "hot_days": int,
            "signal": str,
            "favorable": bool,  # 진입에 유리한 페이즈인가
        }
    """
    result = {"phase": "미확인", "hot_days": 0, "signal": "", "favorable": True}

    try:
        # ★8/29 [F-165] — `detect_rotation` 이라는 이름은 저장소에 **존재한 적이 없다.**
        #   `try/except` 안이라 매일 조용히 ImportError로 빠져 이 함수는 **항상 기본값**
        #   ("미확인"·hot_days 0)만 돌려주고 있었다 = 섹터 로테이션 정보가 사실상 0.
        #   정답은 `analyze_rotation()`(rotation_detector:347 · RotationReport 반환 ·
        #   history=None이면 파일에서 로드 · `.hot_sectors` 필드 보유 — 아래 사용부와 일치).
        from data.rotation_detector import analyze_rotation
        report = analyze_rotation()
        if not report:
            return result

        # 섹터명으로 매칭
        sector_lower = sector.lower() if sector else ""
        for phase in report.hot_sectors:
            if sector_lower in phase.sector_name.lower():
                result["phase"] = phase.phase
                result["hot_days"] = phase.hot_days
                result["signal"] = phase.signal
                result["favorable"] = phase.phase in ("EARLY", "MID")
                return result

        for phase in report.staging_sectors:
            if sector_lower in phase.sector_name.lower():
                result["phase"] = "STAGING"
                result["hot_days"] = 0
                result["signal"] = phase.signal
                result["favorable"] = True  # 스테이징 = 다음 차례
                return result

        for phase in report.cooling_sectors:
            if sector_lower in phase.sector_name.lower():
                result["phase"] = phase.phase
                result["hot_days"] = phase.hot_days
                result["signal"] = phase.signal
                result["favorable"] = False  # 쿨링 = 비추
                return result

    except Exception as e:
        logger.debug(f"로테이션 조회 실패({sector}): {e}")

    return result


# v2 하위호환 — 기존 호출 유지
def _analyze_flow(code: str) -> dict:
    """v2 하위호환 래퍼. v3 결과에서 v2 형식 추출."""
    r = _analyze_flow_6(code)
    return {
        "foreign_5d": r["foreign_5d"],
        "inst_5d": r["inst_5d"],
        "grade": r["grade_label"],  # v2는 한글 등급 사용
        "foreign_trend": r["foreign_trend"],
        "inst_trend": r["inst_trend"],
        "exh_rate": r["exh_rate"],
    }


def _calc_continuation_score(
    code: str, name: str, sector: str,
    close: int, change_pct: float, volume: int,
    volume_ratio: float, tv_억: float, cap_억: float,
    close_strength: float, consecutive_limit: int,
    frgn_net: float = 0, inst_net: float = 0,
) -> float:
    """score_continuation 래퍼 - 엔진 스캔 데이터로 연속성 점수 계산"""
    turnover = (tv_억 / cap_억 * 100) if cap_억 > 0 else 0
    stock = LimitUpStock(
        code=code, name=name, sector=sector,
        close=close, change_pct=round(change_pct, 2),
        volume=volume, volume_ratio=round(volume_ratio, 2),
        trading_value_억=round(tv_억, 1),
        market_cap_억=cap_억,
        turnover_pct=round(turnover, 2),
        close_strength=round(close_strength, 3),
        frgn_net=frgn_net, inst_net=inst_net,
        consecutive_limit=consecutive_limit,
    )
    return score_continuation(stock)


def _make_split_plan(entry_type: str, signal_close: float,
                     entry_price: float = 0) -> list:
    """분할매수 3단계 계획 생성

    Args:
        entry_type: "next_day" (상한가) / "pullback" (눌림목)
        signal_close: 시그널일 종가 (상한가 당일)
        entry_price: 눌림목 트리거 가격 (pullback용)

    Returns:
        [{"tranche": 1, "pct": 50, "price": 1095, "label": "1차 (시가-3%)"},
         {"tranche": 2, "pct": 30, "price": 1129, "label": "2차 (시가)"},
         {"tranche": 3, "pct": 20, "price": 1049, "label": "3차 (눌림-7%)"}]
    """
    if entry_type == "next_day":
        # 3회차+ 상한가: 시가 근처 분할 진입
        return [
            {"tranche": 1, "pct": 50, "price": int(signal_close * 0.97),
             "label": "1차 (시가-3%)"},
            {"tranche": 2, "pct": 30, "price": int(signal_close * 1.00),
             "label": "2차 (시가)"},
            {"tranche": 3, "pct": 20, "price": int(signal_close * 0.93),
             "label": "3차 (눌림-7%)"},
        ]
    else:
        # 눌림목: 눌림 구간 분할 진입
        base = entry_price if entry_price > 0 else signal_close
        return [
            {"tranche": 1, "pct": 50, "price": int(base * 1.00),
             "label": "1차 (눌림확인)"},
            {"tranche": 2, "pct": 30, "price": int(base * 0.97),
             "label": "2차 (추가눌림-3%)"},
            {"tranche": 3, "pct": 20, "price": int(base * 1.03),
             "label": "3차 (반등확인+3%)"},
        ]


# ═══════════════════════════════════════════════════════
#  1. 상한가 이력 분석
# ═══════════════════════════════════════════════════════

@dataclass
class LimitUpHistory:
    """종목의 상한가 이력 요약"""
    code: str
    name: str
    total_limit_count: int = 0       # 기간 내 총 상한가 횟수
    surge_15_count: int = 0          # 15%+ 급등 횟수
    limit_dates: list = field(default_factory=list)  # 상한가 발생일 리스트
    origin_price: float = 0.0        # 첫 상한가 직전 종가 (원점)
    origin_date: str = ""            # 원점 기준일
    current_price: float = 0.0       # 현재가
    overheat_pct: float = 0.0        # 원점 대비 상승률 (%)
    days_since_last: int = 999       # 마지막 상한가로부터 경과일
    is_qualified: bool = False       # 진입 자격 충족 여부


def analyze_limit_history(code: str, name: str = "",
                          lookback_days: int = 30) -> LimitUpHistory:
    """종목의 최근 상한가 이력 분석

    Args:
        code: 종목코드
        name: 종목명
        lookback_days: 분석 기간 (영업일)

    Returns:
        LimitUpHistory with qualification status
    """
    result = LimitUpHistory(code=code, name=name)

    df = _load_daily(code)
    if df is None or len(df) < lookback_days + 1:
        return result

    close_col = "종가" if "종가" in df.columns else "close"
    closes = df[close_col].values.astype(float)
    dates = df.index

    # 최근 lookback_days 내 상한가/급등 이벤트 탐색
    start_idx = max(1, len(closes) - lookback_days)
    limit_events = []
    surge_events = []

    for i in range(start_idx, len(closes)):
        if closes[i - 1] <= 0:
            continue
        change = (closes[i] / closes[i - 1] - 1) * 100

        if change >= LIMIT_UP_PCT:
            limit_events.append({
                "idx": i,
                "date": str(dates[i].date()),
                "close": float(closes[i]),
                "prev_close": float(closes[i - 1]),
                "change_pct": round(change, 2),
            })
        elif change >= STRONG_SURGE_PCT:
            surge_events.append({
                "idx": i,
                "date": str(dates[i].date()),
                "close": float(closes[i]),
                "change_pct": round(change, 2),
            })

    result.total_limit_count = len(limit_events)
    result.surge_15_count = len(limit_events) + len(surge_events)
    result.limit_dates = [e["date"] for e in limit_events]
    result.current_price = float(closes[-1])

    if not limit_events:
        return result

    # 원점 가격 (첫 상한가 직전 종가)
    first_event = limit_events[0]
    result.origin_price = first_event["prev_close"]
    result.origin_date = str(dates[first_event["idx"] - 1].date())

    # 과열도 (원점 대비 현재가)
    if result.origin_price > 0:
        result.overheat_pct = round(
            (result.current_price / result.origin_price - 1) * 100, 1
        )

    # 마지막 상한가로부터 경과일 (영업일)
    last_limit_idx = limit_events[-1]["idx"]
    result.days_since_last = len(closes) - 1 - last_limit_idx

    # 진입 자격 판정
    result.is_qualified = (
        result.total_limit_count >= MIN_LIMIT_HISTORY  # 이력 2회+
        and result.days_since_last <= MAX_DAYS_BETWEEN  # 10일 이내
        and result.overheat_pct < MAX_OVERHEAT_PCT      # 과열 <300%
        and result.current_price >= MIN_PRICE            # 주가 1000원+
    )

    return result


# ═══════════════════════════════════════════════════════
#  2. 감시풀 (Watchlist) 관리
# ═══════════════════════════════════════════════════════

@dataclass
class WatchItem:
    """감시풀 종목"""
    code: str
    name: str
    signal_date: str                 # 시그널 발생일 (상한가 발생일)
    signal_close: float              # 상한가 당일 종가
    origin_price: float              # 원점 가격
    overheat_pct: float              # 과열도
    limit_count: int                 # 상한가 이력 횟수
    monitor_until: str               # 감시 만료일 (YYYY-MM-DD)
    signal_close_price: float = 0.0   # Rule D canonical alias of signal_close
    entry_type: str = "next_day"     # next_day (3번째+ 상한가) / pullback (눌림목 대기)
    status: str = "monitoring"       # monitoring / triggered / expired / entered
    entry_price: float = 0.0         # 추천 진입가
    entry_low: float = 0.0           # 진입 하한 (시가-3%)
    entry_high: float = 0.0          # 진입 상한 (시가)
    tp_price: float = 0.0            # 목표가
    sl_price: float = 0.0            # 손절가 (없음 = 0)
    pullback_high: float = 0.0       # 감시 중 최고가 (눌림목 기준점)
    triggered_date: str = ""         # 진입 트리거 발생일
    triggered_price: float = 0.0     # 진입 트리거 가격
    reasons: list = field(default_factory=list)
    sector: str = ""
    market_cap: float = 0.0
    volume_ratio: float = 0.0
    # ── 수급 분석 (v3: 6자 통합) ──
    flow_foreign_5d: int = 0         # 5일 외국인 순매수 (백만원)
    flow_inst_5d: int = 0            # 5일 기관 순매수 (백만원)
    flow_pension_5d: int = 0         # 5일 연기금 순매수 (백만원)
    flow_fi_5d: int = 0              # 5일 금투 순매수 (백만원)
    flow_smart_total: int = 0        # 스마트머니 합산
    flow_grade: str = ""             # 수급등급: S/A/B/C/F/미확인
    flow_grade_label: str = ""       # 한글: 강매집/매집중/중립/이탈중/강이탈
    foreign_exh_rate: float = 0.0    # 외국인 소진율 (%)
    sell_exhaustion: float = 1.0     # 매도소진율 (개인, <1.0=소진 진행)
    vol_compression: float = 1.0     # 거래량압축률 (<1.0=축적 중)
    smart_consec: int = 0            # 스마트머니 연속매수 일수
    dual_buy: bool = False           # 외인+기관 쌍매수
    survival_ok: bool = True         # 수급 생존 필터 통과
    survival_reason: str = ""        # 생존/탈락 사유
    rotation_phase: str = ""         # 섹터 로테이션 페이즈
    rotation_favorable: bool = True  # 로테이션 유리 여부
    continuation_score: float = 0.0  # 연속성 점수 (0~100)
    split_plan: list = field(default_factory=list)  # 분할매수 계획

    def __post_init__(self):
        if not self.signal_close_price and self.signal_close:
            self.signal_close_price = self.signal_close
        elif not self.signal_close and self.signal_close_price:
            self.signal_close = self.signal_close_price


def load_watchlist() -> list[WatchItem]:
    """감시풀 로드"""
    if not WATCHLIST_PATH.exists():
        return []
    try:
        data = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
        items = []
        for d in data.get("items", []):
            if "signal_close_price" not in d and d.get("signal_close") is not None:
                d["signal_close_price"] = d.get("signal_close")
            if "signal_close" not in d and d.get("signal_close_price") is not None:
                d["signal_close"] = d.get("signal_close_price")
            item = WatchItem(**{k: v for k, v in d.items()
                                if k in WatchItem.__dataclass_fields__})
            items.append(item)
        return items
    except Exception as e:
        logger.error(f"감시풀 로드 실패: {e}")
        return []


def save_watchlist(items: list[WatchItem]):
    """감시풀 저장"""
    LIMIT_UP_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now().isoformat(),
        "count": len(items),
        "monitoring": sum(1 for i in items if i.status == "monitoring"),
        "triggered": sum(1 for i in items if i.status == "triggered"),
        "items": [asdict(i) for i in items],
    }
    WATCHLIST_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"감시풀 저장: {len(items)}건 (감시중 {data['monitoring']}, 트리거 {data['triggered']})")


# ═══════════════════════════════════════════════════════
#  3. 시그널 생성
# ═══════════════════════════════════════════════════════

def scan_new_signals(universe: dict = None) -> list[WatchItem]:
    """오늘의 상한가 종목 스캔 → 자격 충족 시 감시풀 추가 시그널 생성

    전략 1 (3번째+ 상한가):
      - 오늘 상한가 달성
      - 이전 상한가 이력 2회+ (→ 오늘이 3번째+)
      - 직전 상한가로부터 10일 이내
      - 원점 대비 +300% 미만
      → 다음날 하이브리드 진입 (시가-3% 지정가 / 안잡히면 시가)
      → 유니버스 필터 미적용 (워낙 희귀하고 100% 승률)

    전략 2 (10%+ 급등 눌림목 대기):
      - 오늘 10%+ 급등 (상한가 포함)
      - 5일간 감시 → 고점 대비 -10% 눌리면 진입
      - ★ 품질 유니버스 필터: EWY + 섹터유니버스 175종목만 허용
      (전략 1 자격 미충족 종목 대상)
    """
    if universe is None:
        universe = _load_universe()
    if not universe:
        logger.error("유니버스 없음")
        return []

    # ★ 품질 유니버스 로드 (전략2 필터용)
    from data.quality_universe import build_quality_universe, save_surge_stock
    quality_codes, quality_details = build_quality_universe()
    logger.info(f"품질 유니버스: {len(quality_codes)}종목 로드")

    new_signals = []
    scanned = 0
    skipped_quality = 0  # 품질 필터로 제외된 종목수
    surge_saved = 0      # 동적 유니버스에 저장된 종목수
    today = datetime.now().strftime("%Y-%m-%d")

    for code, info in universe.items():
        name = info.get("name", "")
        if _is_etf(name):
            continue

        df = _load_daily(code)
        if df is None:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        vol_col = "거래량" if "거래량" in df.columns else "volume"
        high_col = "고가" if "고가" in df.columns else "high"
        low_col = "저가" if "저가" in df.columns else "low"

        closes = df[close_col].values.astype(float)
        if len(closes) < 2 or closes[-2] <= 0:
            scanned += 1
            continue

        # 오늘 등락률
        change = (closes[-1] / closes[-2] - 1) * 100
        if change < STRONG_SURGE_PCT:
            scanned += 1
            continue

        current_close = float(closes[-1])
        if current_close < MIN_PRICE:
            scanned += 1
            continue

        # 상한가 이력 분석
        history = analyze_limit_history(code, name, lookback_days=30)

        # 거래량 배수
        volumes = df[vol_col].values.astype(float)
        vol_avg20 = volumes[-20:].mean() if len(volumes) >= 20 else volumes.mean()
        vol_ratio = round(volumes[-1] / vol_avg20, 2) if vol_avg20 > 0 else 1.0

        # 종가 강도
        high = float(df[high_col].iloc[-1])
        low = float(df[low_col].iloc[-1])
        price_range = high - low
        close_strength = (current_close - low) / price_range if price_range > 0 else 0.5

        # 거래대금 (억원)
        tv = current_close * float(volumes[-1]) / 1e8
        cap = info.get("cap_억", 0)
        sector = info.get("sector", "")

        # ── 전략 1: 3번째+ 상한가 (분할매수 + 6자 수급 생존확인) ──
        if change >= LIMIT_UP_PCT:
            # ★ 상한가 종목도 동적 유니버스에 기록 (다음 로테이션 대비)
            try:
                save_surge_stock(
                    code=code, name=name, sector=sector,
                    change_pct=round(change, 1),
                    close_price=current_close, market_cap=cap,
                )
                surge_saved += 1
            except Exception as e:
                logger.debug(f"동적유니버스 저장 실패 {code}: {e}")

        if change >= LIMIT_UP_PCT and history.is_qualified:
            entry_price = int(current_close * 0.97)  # 시가-3% 예상
            tp_price = SAJANG.get_take_profit(entry_price)

            # ★ 6자 수급 분석 (v3)
            flow = _analyze_flow_6(code)

            # ★ 수급 생존 필터 — 전략1은 희귀하므로 경고만 (차단 안 함)
            survival_warning = ""
            if not flow["survival_ok"]:
                survival_warning = f"⚠️ 수급주의: {flow['survival_reason']}"

            # ★ 섹터 로테이션 확인
            rot = _get_sector_rotation(sector)

            reasons = [
                f"상한가 {change:+.1f}% ({history.total_limit_count}회차)",
                f"원점대비 +{history.overheat_pct:.0f}%",
            ]
            if vol_ratio >= 5:
                reasons.append(f"거래량 {vol_ratio:.0f}x")
            if close_strength >= 0.95:
                reasons.append("상한가마감")

            # 6자 수급 정보
            reasons.append(f"수급[{flow['grade']}] {flow['grade_label']}")
            if flow["smart_total_5d"] != 0:
                s = "+" if flow["smart_total_5d"] > 0 else ""
                reasons.append(f"스마트머니5일 {s}{flow['smart_total_5d']:,}백만")
            if flow["dual_buy"]:
                reasons.append("🔥외인+기관 쌍매수")
            if flow["sell_exhaustion"] < 0.7:
                reasons.append(f"매도소진 {flow['sell_exhaustion']:.2f}")
            if flow["vol_compression"] < 0.7:
                reasons.append(f"거래량압축 {flow['vol_compression']:.2f}")
            if rot["phase"] != "미확인":
                reasons.append(f"섹터로테이션: {rot['phase']}")
            if survival_warning:
                reasons.append(survival_warning)

            # ★ 연속성 점수 계산
            cont_score = _calc_continuation_score(
                code=code, name=name, sector=sector,
                close=int(current_close), change_pct=change,
                volume=int(volumes[-1]), volume_ratio=vol_ratio,
                tv_억=tv, cap_억=cap, close_strength=close_strength,
                consecutive_limit=history.total_limit_count,
                frgn_net=flow["foreign_5d"], inst_net=flow["inst_5d"],
            )

            # ★ 분할매수 계획
            split = _make_split_plan("next_day", current_close)

            signal = WatchItem(
                code=code,
                name=name,
                signal_date=today,
                signal_close=current_close,
                origin_price=history.origin_price,
                overheat_pct=history.overheat_pct,
                limit_count=history.total_limit_count,
                monitor_until=today,
                entry_type="next_day",
                status="triggered",
                entry_price=entry_price,
                entry_low=int(current_close * 0.97),
                entry_high=int(current_close * 1.00),
                tp_price=tp_price,
                sl_price=0,
                reasons=reasons,
                sector=sector,
                market_cap=cap,
                volume_ratio=vol_ratio,
                flow_foreign_5d=flow["foreign_5d"],
                flow_inst_5d=flow["inst_5d"],
                flow_pension_5d=flow["pension_5d"],
                flow_fi_5d=flow["fi_5d"],
                flow_smart_total=flow["smart_total_5d"],
                flow_grade=flow["grade"],
                flow_grade_label=flow["grade_label"],
                foreign_exh_rate=flow["exh_rate"],
                sell_exhaustion=flow["sell_exhaustion"],
                vol_compression=flow["vol_compression"],
                smart_consec=flow["smart_consec"],
                dual_buy=flow["dual_buy"],
                survival_ok=flow["survival_ok"],
                survival_reason=flow["survival_reason"],
                rotation_phase=rot["phase"],
                rotation_favorable=rot["favorable"],
                continuation_score=cont_score,
                split_plan=split,
            )
            new_signals.append(signal)
            grade_icon = "🟢" if flow["grade"] in ("S", "A") else "🟡" if flow["grade"] == "B" else "🔴"
            logger.info(
                f"[전략1] {name}({code}) 상한가 {history.total_limit_count}회차 "
                f"| 원점+{history.overheat_pct:.0f}% | 연속성:{cont_score:.0f} "
                f"| {grade_icon}수급[{flow['grade']}]{flow['grade_label']} "
                f"| 스마트{flow['smart_total_5d']:+,}백만 | 소진{flow['sell_exhaustion']:.2f} "
                f"| 생존{'✓' if flow['survival_ok'] else '✗'} | 로테이션:{rot['phase']}"
            )

        # ── 전략 2: 10%+ 급등 눌림목 대기 (품질유니버스 + 수급 생존 필터) ──
        elif change >= STRONG_SURGE_PCT and tv >= 10.0:
            # ★★ 동적 유니버스에 저장 (품질필터 이전! — 다음 로테이션 대비)
            try:
                save_surge_stock(
                    code=code, name=name, sector=sector,
                    change_pct=round(change, 1),
                    close_price=current_close, market_cap=cap,
                )
                surge_saved += 1
            except Exception as e:
                logger.debug(f"동적유니버스 저장 실패 {code}: {e}")

            # ★ 품질 유니버스 필터
            if code not in quality_codes:
                skipped_quality += 1
                scanned += 1
                continue

            # ★ 6자 수급 분석 (v3)
            flow = _analyze_flow_6(code)

            # ★★ 수급 생존 필터 — 전략2는 엄격 적용 (이탈 시 차단)
            if not flow["survival_ok"]:
                logger.debug(
                    f"[전략2] {name}({code}) 수급생존 탈락: {flow['survival_reason']}"
                )
                scanned += 1
                continue

            # ★ 섹터 로테이션 확인
            rot = _get_sector_rotation(sector)

            # 로테이션 LATE/REVERSAL → 감시기간 단축 (3일), 그 외 5일
            if rot["phase"] in ("LATE", "REVERSAL"):
                monitor_days_adj = 3
            elif rot["phase"] in ("EARLY", "STAGING"):
                monitor_days_adj = 7  # 유리한 로테이션 → 감시 연장
            else:
                monitor_days_adj = 5

            monitor_end = datetime.now() + timedelta(days=monitor_days_adj + 2)
            monitor_until = monitor_end.strftime("%Y-%m-%d")

            # 품질 유니버스 정보
            q_info = quality_details.get(code, {})
            sector_theme = q_info.get("sector_theme", q_info.get("sector", sector))

            reasons = [f"급등 {change:+.1f}% (수급생존 확인됨)"]
            if q_info.get("source") == "ewy":
                reasons.append(f"EWY바스켓 (비중 {q_info.get('weight', 0):.1f}%)")
            if sector_theme:
                reasons.append(f"섹터: {sector_theme}")
            if history.total_limit_count > 0:
                reasons.append(f"상한가이력 {history.total_limit_count}회")
            if vol_ratio >= 3:
                reasons.append(f"거래량 {vol_ratio:.0f}x")

            # 6자 수급 정보
            reasons.append(f"수급[{flow['grade']}] {flow['grade_label']}")
            if flow["smart_total_5d"] != 0:
                s = "+" if flow["smart_total_5d"] > 0 else ""
                reasons.append(f"스마트머니5일 {s}{flow['smart_total_5d']:,}백만")
            if flow["dual_buy"]:
                reasons.append("🔥외인+기관 쌍매수")
            if flow["sell_exhaustion"] < 0.7:
                reasons.append(f"매도소진 {flow['sell_exhaustion']:.2f}")
            if flow["vol_compression"] < 0.7:
                reasons.append(f"거래량압축 {flow['vol_compression']:.2f}")
            if rot["phase"] != "미확인":
                rot_icon = "🟢" if rot["favorable"] else "🟡"
                reasons.append(f"{rot_icon}로테이션: {rot['phase']}")

            # ★ 연속성 점수 계산
            cont_score = _calc_continuation_score(
                code=code, name=name, sector=sector,
                close=int(current_close), change_pct=change,
                volume=int(volumes[-1]), volume_ratio=vol_ratio,
                tv_억=tv, cap_억=cap, close_strength=close_strength,
                consecutive_limit=history.total_limit_count,
                frgn_net=flow["foreign_5d"], inst_net=flow["inst_5d"],
            )

            signal = WatchItem(
                code=code,
                name=name,
                signal_date=today,
                signal_close=current_close,
                origin_price=history.origin_price if history.origin_price > 0 else closes[-2],
                overheat_pct=history.overheat_pct,
                limit_count=history.total_limit_count,
                monitor_until=monitor_until,
                entry_type="pullback",
                status="monitoring",
                pullback_high=current_close,
                reasons=reasons,
                sector=sector,
                market_cap=cap,
                volume_ratio=vol_ratio,
                flow_foreign_5d=flow["foreign_5d"],
                flow_inst_5d=flow["inst_5d"],
                flow_pension_5d=flow["pension_5d"],
                flow_fi_5d=flow["fi_5d"],
                flow_smart_total=flow["smart_total_5d"],
                flow_grade=flow["grade"],
                flow_grade_label=flow["grade_label"],
                foreign_exh_rate=flow["exh_rate"],
                sell_exhaustion=flow["sell_exhaustion"],
                vol_compression=flow["vol_compression"],
                smart_consec=flow["smart_consec"],
                dual_buy=flow["dual_buy"],
                survival_ok=flow["survival_ok"],
                survival_reason=flow["survival_reason"],
                rotation_phase=rot["phase"],
                rotation_favorable=rot["favorable"],
                continuation_score=cont_score,
            )
            new_signals.append(signal)
            grade_icon = "🟢" if flow["grade"] in ("S", "A") else "🟡"
            logger.info(
                f"[전략2] {name}({code}) 급등 {change:+.1f}% "
                f"| 감시 ~{monitor_until} | 연속성:{cont_score:.0f} "
                f"| {grade_icon}수급[{flow['grade']}]{flow['grade_label']} "
                f"| 스마트{flow['smart_total_5d']:+,}백만 | 소진{flow['sell_exhaustion']:.2f} "
                f"| 압축{flow['vol_compression']:.2f} | 로테이션:{rot['phase']}"
            )

        scanned += 1
        if scanned % 500 == 0:
            logger.info(f"  스캔 진행: {scanned}종목")

    logger.info(
        f"스캔 완료: {scanned}종목 → 신규 시그널 {len(new_signals)}건 "
        f"(품질필터 제외 {skipped_quality}건, 동적유니버스 저장 {surge_saved}건)"
    )
    return new_signals


# ═══════════════════════════════════════════════════════
#  4. 감시풀 눌림목 체크
# ═══════════════════════════════════════════════════════

def check_pullback_entries(watchlist: list[WatchItem]) -> list[WatchItem]:
    """감시풀의 monitoring 종목에 대해 눌림목 진입 조건 체크

    - 고점 대비 -10% 이상 눌리면 → triggered
    - 감시 만료일 지나면 → expired
    - 고점 갱신 시 pullback_high 업데이트
    """
    today = datetime.now().strftime("%Y-%m-%d")
    triggered = []

    for item in watchlist:
        if item.status != "monitoring":
            continue

        # 만료 체크
        if today > item.monitor_until:
            item.status = "expired"
            logger.info(f"  감시만료: {item.name}({item.code})")
            continue

        # 현재가 확인
        df = _load_daily(item.code)
        if df is None:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        high_col = "고가" if "고가" in df.columns else "high"
        current_close = float(df[close_col].iloc[-1])
        current_high = float(df[high_col].iloc[-1])

        # 고점 갱신
        if current_high > item.pullback_high:
            item.pullback_high = current_high

        # 눌림목 체크: 고점 대비 -10% 이상 하락
        if item.pullback_high > 0:
            drop_pct = (current_close / item.pullback_high - 1) * 100

            if drop_pct <= PULLBACK_TRIGGER_PCT:
                # ★ 눌림목 트리거 시 6자 수급 재분석 (v3)
                flow = _analyze_flow_6(item.code)

                # ★★ 수급 생존 재검증 — 눌림 구간에서 스마트머니 빠졌으면 진입 취소
                if not flow["survival_ok"]:
                    item.reasons.append(
                        f"눌림목 {drop_pct:+.1f}% BUT 수급탈락: {flow['survival_reason']}"
                    )
                    logger.info(
                        f"  눌림목 수급탈락: {item.name}({item.code}) "
                        f"{drop_pct:+.1f}% | {flow['grade_label']} | {flow['survival_reason']}"
                    )
                    continue  # 트리거하지 않음

                # ★ 매도소진 + 거래량압축 추가 확인 (v3 정밀화)
                # 최소 하나는 충족해야 "진짜 눌림" (패닉 셀이 아님)
                pullback_quality = (
                    flow["sell_exhaustion"] < 0.9
                    or flow["vol_compression"] < 0.9
                    or flow["smart_consec"] >= 2
                )
                if not pullback_quality:
                    item.reasons.append(
                        f"눌림목 {drop_pct:+.1f}% BUT 품질미달 "
                        f"(소진{flow['sell_exhaustion']:.2f} 압축{flow['vol_compression']:.2f})"
                    )
                    logger.info(
                        f"  눌림목 품질미달: {item.name}({item.code}) "
                        f"소진{flow['sell_exhaustion']:.2f} 압축{flow['vol_compression']:.2f}"
                    )
                    continue

                item.status = "triggered"
                item.triggered_date = today
                item.triggered_price = current_close
                item.entry_price = int(current_close)
                item.entry_low = int(current_close * 0.97)
                item.entry_high = int(current_close * 1.02)
                item.tp_price = SAJANG.get_take_profit(current_close)

                # 수급 필드 업데이트 (v3 전체)
                item.flow_foreign_5d = flow["foreign_5d"]
                item.flow_inst_5d = flow["inst_5d"]
                item.flow_pension_5d = flow["pension_5d"]
                item.flow_fi_5d = flow["fi_5d"]
                item.flow_smart_total = flow["smart_total_5d"]
                item.flow_grade = flow["grade"]
                item.flow_grade_label = flow["grade_label"]
                item.foreign_exh_rate = flow["exh_rate"]
                item.sell_exhaustion = flow["sell_exhaustion"]
                item.vol_compression = flow["vol_compression"]
                item.smart_consec = flow["smart_consec"]
                item.dual_buy = flow["dual_buy"]
                item.survival_ok = flow["survival_ok"]
                item.survival_reason = flow["survival_reason"]

                # 섹터 로테이션 확인
                rot = _get_sector_rotation(item.sector)
                item.rotation_phase = rot["phase"]
                item.rotation_favorable = rot["favorable"]

                # 분할매수 계획 생성
                item.split_plan = _make_split_plan("pullback", item.signal_close, current_close)

                item.reasons.append(
                    f"눌림목 {drop_pct:+.1f}% (고점 {item.pullback_high:,.0f} → {current_close:,.0f})"
                )
                item.reasons.append(f"수급[{flow['grade']}] {flow['grade_label']} (생존확인)")
                if flow["smart_total_5d"] != 0:
                    s = "+" if flow["smart_total_5d"] > 0 else ""
                    item.reasons.append(f"스마트머니5일 {s}{flow['smart_total_5d']:,}백만")
                if flow["dual_buy"]:
                    item.reasons.append("🔥외인+기관 쌍매수")
                if flow["sell_exhaustion"] < 0.7:
                    item.reasons.append(f"매도소진 {flow['sell_exhaustion']:.2f}")
                if rot["phase"] != "미확인":
                    item.reasons.append(f"로테이션: {rot['phase']}")

                triggered.append(item)
                grade_icon = "🟢" if flow["grade"] in ("S", "A") else "🟡"
                logger.info(
                    f"  눌림목 트리거: {item.name}({item.code}) "
                    f"{drop_pct:+.1f}% | {grade_icon}수급[{flow['grade']}]{flow['grade_label']} "
                    f"| 스마트{flow['smart_total_5d']:+,}백만 | 소진{flow['sell_exhaustion']:.2f} "
                    f"| 압축{flow['vol_compression']:.2f} | 로테이션:{rot['phase']}"
                )

    return triggered


# ═══════════════════════════════════════════════════════
#  5. 시그널 저장 + 텔레그램 알림
# ═══════════════════════════════════════════════════════

def save_signals(signals: list[WatchItem]):
    """트리거된 시그널 저장"""
    LIMIT_UP_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "generated_at": datetime.now().isoformat(),
        "count": len(signals),
        "signals": [asdict(s) for s in signals],
    }
    SIGNALS_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 히스토리에도 누적
    history = []
    if SIGNAL_HISTORY_PATH.exists():
        try:
            h = json.loads(SIGNAL_HISTORY_PATH.read_text(encoding="utf-8"))
            history = h.get("signals", [])
        except Exception:
            pass

    for s in signals:
        history.append({
            **asdict(s),
            "generated_at": datetime.now().isoformat(),
        })

    # 최근 90일만 유지
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    history = [h for h in history if h.get("generated_at", "") >= cutoff]

    SIGNAL_HISTORY_PATH.write_text(
        json.dumps({"signals": history}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def format_telegram_alert(signals: list[WatchItem]) -> str:
    """텔레그램 알림 메시지 생성 (v3: 6자수급+생존+로테이션)"""
    if not signals:
        return ""

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"<b>상한가 엔진 v3 시그널</b> ({len(signals)}건)",
        f"{now}",
        "",
    ]

    for i, s in enumerate(signals, 1):
        type_label = "분할매수" if s.entry_type == "next_day" else "눌림목진입"
        emoji = "🔴" if s.entry_type == "next_day" else "🟡"

        # 수급등급 아이콘
        grade_icon = {"S": "🟢", "A": "🟢", "B": "🟡", "C": "🔴", "F": "⛔"}.get(
            s.flow_grade, "⚪"
        )

        lines.append(f"{emoji} <b>{s.name}</b> ({s.code}) [{type_label}]")
        lines.append(f"  현재가: {s.signal_close:,.0f}원 | 시총: {s.market_cap:,.0f}억")

        if s.entry_type == "next_day":
            lines.append(f"  상한가 {s.limit_count}회차 | 원점+{s.overheat_pct:.0f}% | 연속성:{s.continuation_score:.0f}")
        else:
            lines.append(f"  진입: {s.entry_price:,} (눌림 트리거)")

        lines.append(f"  목표: {s.tp_price:,} (+{TP_PCT:.0f}%)")

        # 6자 수급 분석 (v3)
        if s.flow_grade:
            flow_line = f"  {grade_icon}수급[{s.flow_grade}] {s.flow_grade_label}"
            if s.flow_smart_total != 0:
                sign = "+" if s.flow_smart_total > 0 else ""
                flow_line += f" | 스마트 {sign}{s.flow_smart_total:,}백만"
            lines.append(flow_line)

            detail_parts = []
            if s.flow_foreign_5d != 0:
                detail_parts.append(f"외인{'+' if s.flow_foreign_5d > 0 else ''}{s.flow_foreign_5d:,}")
            if s.flow_inst_5d != 0:
                detail_parts.append(f"기관{'+' if s.flow_inst_5d > 0 else ''}{s.flow_inst_5d:,}")
            if s.flow_pension_5d != 0:
                detail_parts.append(f"연기금{'+' if s.flow_pension_5d > 0 else ''}{s.flow_pension_5d:,}")
            if s.flow_fi_5d != 0:
                detail_parts.append(f"금투{'+' if s.flow_fi_5d > 0 else ''}{s.flow_fi_5d:,}")
            if detail_parts:
                lines.append(f"  5일: {' | '.join(detail_parts)}")

        # v3 추가 지표
        v3_parts = []
        if s.dual_buy:
            v3_parts.append("🔥쌍매수")
        if s.sell_exhaustion < 0.7:
            v3_parts.append(f"소진{s.sell_exhaustion:.2f}")
        if s.vol_compression < 0.7:
            v3_parts.append(f"압축{s.vol_compression:.2f}")
        if s.smart_consec >= 3:
            v3_parts.append(f"연속{s.smart_consec}일")
        if s.rotation_phase and s.rotation_phase != "미확인":
            v3_parts.append(f"섹터:{s.rotation_phase}")
        if v3_parts:
            lines.append(f"  {' | '.join(v3_parts)}")

        # 분할매수 계획
        if s.split_plan:
            lines.append("  [분할매수]")
            for sp in s.split_plan:
                lines.append(f"    {sp['label']}: {sp['price']:,}원 ({sp['pct']}%)")

        lines.append("")

    lines.append("만기: 20영업일 | 손절: 없음(만기청산)")
    lines.append("* v3 수급생존 검증 완료 시그널")

    return "\n".join(lines)


def send_telegram_alert(message: str) -> bool:
    """텔레그램 전송 (output.telegram_alert.TelegramAlert 사용)"""
    if not message:
        return False
    try:
        from output.telegram_alert import TelegramAlert
        tg = TelegramAlert()  # .env에서 토큰/채팅ID 자동 로드
        if not tg.enabled:
            logger.info("[Telegram 미설정] 메시지 생략")
            return False
        # HTML → 일반텍스트 변환 (TelegramAlert._send는 plain text 전용)
        import re
        plain = re.sub(r'<[^>]+>', '', message)
        tg._send(plain)
        return True
    except Exception as e:
        logger.warning(f"텔레그램 전송 실패: {e}")
        return False


# ═══════════════════════════════════════════════════════
#  6. 일일 실행 (메인 루틴)
# ═══════════════════════════════════════════════════════

def run_daily(send_telegram: bool = True) -> dict:
    """일일 스캔 + 감시풀 관리 + 시그널 생성

    매일 장 마감 후 (16:10~) 실행:
      1. 기존 감시풀 로드 → 눌림목 체크
      2. 오늘 상한가/급등 스캔 → 신규 시그널
      3. 감시풀 업데이트 (신규 추가, 만료 제거)
      4. 트리거된 시그널 → 텔레그램 알림

    Returns:
        결과 요약 dict
    """
    logger.info("=" * 60)
    logger.info("상한가 눌림목 엔진 v3.0 - 일일 스캔 (6자수급+생존필터+로테이션)")
    logger.info("=" * 60)

    # 1. 기존 감시풀 → 눌림목 체크
    watchlist = load_watchlist()
    monitoring_count = sum(1 for w in watchlist if w.status == "monitoring")
    logger.info(f"[1/4] 기존 감시풀: {len(watchlist)}건 (감시중 {monitoring_count})")

    pullback_triggered = []
    if monitoring_count > 0:
        pullback_triggered = check_pullback_entries(watchlist)
        logger.info(f"  눌림목 트리거: {len(pullback_triggered)}건")

    # 2. 오늘 상한가/급등 스캔
    logger.info("[2/4] 오늘 상한가/급등 스캔...")
    new_signals = scan_new_signals()

    # 기존 감시풀과 중복 제거
    existing_codes = {w.code for w in watchlist}
    new_unique = [s for s in new_signals if s.code not in existing_codes]
    logger.info(f"  신규 시그널: {len(new_signals)}건 (중복제거 후 {len(new_unique)}건)")

    # 기존 감시풀 종목의 continuation_score 보강 (이전 스캔에서 누락된 경우)
    new_score_map = {s.code: s.continuation_score for s in new_signals if s.continuation_score > 0}
    score_updated = 0
    for w in watchlist:
        if w.code in new_score_map and w.continuation_score == 0:
            w.continuation_score = new_score_map[w.code]
            score_updated += 1
    if score_updated:
        logger.info(f"  연속성 점수 보강: {score_updated}건")

    # 3. 감시풀 업데이트
    # 품질 유니버스에 없는 전략2(pullback) 종목 정리
    from data.quality_universe import build_quality_universe
    quality_codes, _ = build_quality_universe()
    pre_clean = len(watchlist)
    watchlist = [
        w for w in watchlist
        if w.entry_type == "next_day"  # 전략1은 유지
        or w.code in quality_codes     # 품질 유니버스에 있는 종목 유지
    ]
    cleaned = pre_clean - len(watchlist)
    if cleaned > 0:
        logger.info(f"  품질필터 감시풀 정리: {cleaned}건 제거")

    # 만료 제거 (7일 이상 지난 expired)
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    active_watchlist = [
        w for w in watchlist
        if w.status in ("monitoring", "triggered")
        or (w.status == "expired" and w.signal_date >= cutoff)
    ]

    # 신규 추가
    active_watchlist.extend(new_unique)
    save_watchlist(active_watchlist)
    logger.info(f"[3/4] 감시풀 갱신: {len(active_watchlist)}건")

    # 4. 트리거된 시그널 수집 + 알림
    all_triggered = []
    # 전략 1: 3번째+ 상한가 (즉시 진입)
    strategy1 = [s for s in new_unique if s.entry_type == "next_day" and s.status == "triggered"]
    # 전략 2: 눌림목 트리거
    all_triggered = strategy1 + pullback_triggered

    logger.info(f"[4/4] 진입 시그널: {len(all_triggered)}건 "
                f"(전략1: {len(strategy1)}, 눌림목: {len(pullback_triggered)})")

    if all_triggered:
        save_signals(all_triggered)
        msg = format_telegram_alert(all_triggered)
        print()
        print(msg)
        print()

        if send_telegram:
            ok = send_telegram_alert(msg)
            logger.info(f"텔레그램 전송: {'성공' if ok else '실패'}")

    # 감시중 종목 상태 출력
    monitoring_now = [w for w in active_watchlist if w.status == "monitoring"]
    if monitoring_now:
        print()
        print(f"감시중 종목 ({len(monitoring_now)}건):")
        for w in monitoring_now:
            print(f"  {w.name}({w.code}) | 시그널 {w.signal_date} "
                  f"| 종가 {w.signal_close:,.0f} | 감시~{w.monitor_until}")

    result = {
        "scan_date": datetime.now().isoformat(),
        "watchlist_total": len(active_watchlist),
        "monitoring": len(monitoring_now),
        "new_signals": len(new_unique),
        "triggered": len(all_triggered),
        "strategy1_count": len(strategy1),
        "pullback_count": len(pullback_triggered),
    }

    logger.info(f"완료: 감시 {len(monitoring_now)}건, 트리거 {len(all_triggered)}건")
    return result


def is_realtime_window(now: datetime | None = None) -> bool:
    """Return True during the 09:30-14:45 KST realtime scan window."""
    from data.trading_calendar import is_trading_day

    if now is None:
        try:
            from zoneinfo import ZoneInfo
            now = datetime.now(ZoneInfo("Asia/Seoul"))
        except Exception:
            now = datetime.now()
    if not is_trading_day(now.date()):
        return False
    minutes = now.hour * 60 + now.minute
    return (9 * 60 + 30) <= minutes <= (14 * 60 + 45)


def run_realtime(send_telegram: bool = False, now: datetime | None = None) -> dict:
    """Intraday LimitUpEngine pass for the single scalper-agent service.

    This does not place orders. It updates watchlist/signals so Rule D can see
    D+0 candidates before the 14:50 entry job.
    """
    if now is None:
        try:
            from zoneinfo import ZoneInfo
            now = datetime.now(ZoneInfo("Asia/Seoul"))
        except Exception:
            now = datetime.now()
    if not is_realtime_window(now):
        logger.info("[realtime] outside realtime window — skip")
        return {
            "mode": "realtime",
            "skipped": True,
            "reason": "outside_window",
            "scan_date": now.isoformat(),
        }

    watchlist = load_watchlist()
    pullback_triggered = check_pullback_entries(watchlist)

    new_signals = scan_new_signals()
    existing_codes = {w.code for w in watchlist}
    new_unique = [s for s in new_signals if s.code not in existing_codes]

    active_watchlist = [
        w for w in watchlist
        if w.status in ("monitoring", "triggered", "entered")
    ]
    active_watchlist.extend(new_unique)
    save_watchlist(active_watchlist)

    strategy1 = [s for s in new_unique if s.entry_type == "next_day" and s.status == "triggered"]
    all_triggered = strategy1 + pullback_triggered
    if all_triggered:
        save_signals(all_triggered)
        msg = format_telegram_alert(all_triggered)
        if msg:
            print(msg)
            if send_telegram:
                send_telegram_alert(msg)

    result = {
        "mode": "realtime",
        "skipped": False,
        "scan_date": now.isoformat(),
        "watchlist_total": len(active_watchlist),
        "new_signals": len(new_unique),
        "triggered": len(all_triggered),
        "strategy1_count": len(strategy1),
        "pullback_count": len(pullback_triggered),
    }
    logger.info(
        "[realtime] 완료: 신규 %s, 트리거 %s, 감시풀 %s",
        len(new_unique), len(all_triggered), len(active_watchlist),
    )
    return result


def print_status():
    """현재 감시풀 상태 출력"""
    watchlist = load_watchlist()
    if not watchlist:
        print("감시풀 비어있음")
        return

    print(f"\n상한가 엔진 감시풀 ({len(watchlist)}건)")
    print("=" * 80)

    for status_label, status_code in [("감시중", "monitoring"), ("트리거", "triggered"), ("만료", "expired")]:
        items = [w for w in watchlist if w.status == status_code]
        if not items:
            continue
        print(f"\n[{status_label}] {len(items)}건")
        for w in items:
            type_label = "상한가" if w.entry_type == "next_day" else "눌림목"
            print(f"  {w.name:10} ({w.code}) | {type_label} | "
                  f"시그널 {w.signal_date} | 종가 {w.signal_close:>8,.0f} | "
                  f"이력 {w.limit_count}회 | 원점+{w.overheat_pct:.0f}% | 연속성:{w.continuation_score:.0f}")
            if w.entry_price > 0:
                print(f"    -> 진입 {w.entry_low:,}~{w.entry_high:,} | 목표 {w.tp_price:,}")

    # 시그널 히스토리 요약
    if SIGNAL_HISTORY_PATH.exists():
        try:
            h = json.loads(SIGNAL_HISTORY_PATH.read_text(encoding="utf-8"))
            signals = h.get("signals", [])
            if signals:
                print(f"\n누적 시그널 히스토리: {len(signals)}건")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════

def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="상한가 눌림목 엔진 v3.0 (6자수급+생존필터+로테이션)")
    parser.add_argument("--check", action="store_true",
                        help="감시풀 눌림목 체크만 (신규 스캔 없음)")
    parser.add_argument("--realtime", action="store_true",
                        help="장중 09:30~14:45 실시간 스캔 (주문 없음)")
    parser.add_argument("--status", action="store_true",
                        help="현재 감시풀 상태 출력")
    parser.add_argument("--no-telegram", action="store_true",
                        help="텔레그램 전송 안 함")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.realtime:
        result = run_realtime(send_telegram=not args.no_telegram)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.check:
        watchlist = load_watchlist()
        triggered = check_pullback_entries(watchlist)
        save_watchlist(watchlist)
        if triggered:
            msg = format_telegram_alert(triggered)
            print(msg)
            if not args.no_telegram:
                send_telegram_alert(msg)
        else:
            print("눌림목 트리거 없음")
        return

    run_daily(send_telegram=not args.no_telegram)


if __name__ == "__main__":
    main()
