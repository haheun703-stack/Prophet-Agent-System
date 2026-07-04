# -*- coding: utf-8 -*-
"""시장 레짐 게이트 (7/4 신설) — 전일(D) 마감 데이터만으로 다음 거래일(D+1) 진입환경 라벨.

배경(7/4 레짐검증·63거래일 2026-04-01~07-03 실측):
  - UP 예측은 안 된다: 전일 breadth/외인/지수 어떤 조합도 익일 UP precision이
    base 34.4% 대비 최대 +2.4%p(36.8%) — 사실상 무정보. UP→UP 지속성 33.3% = base.
  - DOWN 회피만 약하게 된다: 전일 breadth≤0.45 → 익일 DOWN precision 51.6%
    (base 42.2%)·recall 59.3%. GO날 -3% 손절터치 종목수 26~28% 감소(x0.72~0.74).
  - +5% 수확 기회(hv5)는 GO/NO-GO 차이 4~10%뿐 — 매일 500~600개 존재.
    → 이 게이트의 실질 가치 = "수확 늘리기"가 아니라 "손절 줄이기"(회피 전용).

라벨 의미(정직 해석 의무):
  NO_GO   = 전일 breadth≤0.45. 검증된 유일한 신호(익일 하락우세·손절터치↑). 신규진입 회피 후보.
  CAUTION = 전일 breadth<0.55 또는 코스피 프록시(069500)가 20일선 아래. 약한 참고 신호.
  GO      = "회피 아님"일 뿐, 상승 예측이 아니다(UP 예측 무정보 실측).
  UNKNOWN = breadth 산출 불가(ledger 없음) — 게이트 미작동(보수적 통과).

지수 추세(정배열) = 관측 라벨 (사장님 6/7 숙제: 지수추세를 매수타이밍에 엮기):
  KODEX200(069500)/KODEX코스닥150(229200) 프록시 일봉 487일 보유 → MA5/20/60/120·정배열
  즉시 계산. 진짜 지수 이력(flow_market)은 아직 짧아(26~47일) 프록시 우선(추적오차 한계 인지).
  ※ 검증 전이므로 regime 판정에는 MA20 위치만 CAUTION 참고로 반영, 정배열은 기록만.

forward 자가검증 루프: 매 실행 시 (1) 과거 예측 레코드에 실제 결과(actual_breadth) 충전
  (2) NO_GO precision 등 누적 성적 요약 → 이 게이트 자체가 관측 대상. 성적 미달이면 폐기.

★ 안전 불변식: record-only(data_store/market_regime*.json 기록만)·읽기전용 입력·
  매수/매도/picks/SAJANG/주문 0 접촉·관측 없이 flip 금지(라이브 게이트 승격=사장님 결정).
사용: python -m data.market_regime_gate [--asof YYYY-MM-DD]
"""
from __future__ import annotations

import csv as _csv
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("BH.MarketRegime")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
DAILY_DIR = DATA_STORE / "daily"
LEDGER_DIR = DATA_STORE / "paper_3type"
OUT_PATH = DATA_STORE / "market_regime.json"
HISTORY_PATH = DATA_STORE / "market_regime_history.json"

# 프록시 ETF (KIS 일봉·매일 갱신·487일 보유 — KRX 무관)
KOSPI_PROXY = "069500"    # KODEX 200
KOSDAQ_PROXY = "229200"   # KODEX 코스닥150

# 7/4 레짐검증 임계 (관측용 모듈 상수 — 라이브 승격 시 사장님 결정+SAJANG 등재)
NO_GO_BREADTH = 0.45      # 전일 breadth ≤ 0.45 → NO_GO (검증된 유일 신호)
GO_BREADTH = 0.55         # 전일 breadth ≥ 0.55 → GO 후보 (회피 아님일 뿐)


def _read_daily_bars(code: str) -> list:
    """daily/{code}.csv → [(date, close)] 시간순. 실패 시 []."""
    p = DAILY_DIR / f"{code}.csv"
    if not p.exists():
        return []
    out = []
    try:
        with p.open(encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                d = str(row.get("날짜", ""))[:10]
                try:
                    c = float(row.get("종가") or 0)
                except (ValueError, TypeError):
                    continue
                if d and c > 0:
                    out.append((d, c))
    except Exception:  # noqa: BLE001
        return []
    out.sort(key=lambda x: x[0])
    return out


def _index_trend(code: str, asof: str) -> dict | None:
    """프록시 ETF 종가로 asof 이하 마지막 봉 기준 MA5/20/60/120·정배열·20일선 위치.
    봉<120이면 계산 가능한 MA까지만(부족분 None)."""
    bars = [(d, c) for d, c in _read_daily_bars(code) if d <= asof]
    if len(bars) < 20:
        return None
    closes = [c for _, c in bars]

    def _ma(n: int):
        return round(sum(closes[-n:]) / n, 2) if len(closes) >= n else None

    ma5, ma20, ma60, ma120 = _ma(5), _ma(20), _ma(60), _ma(120)
    close = closes[-1]
    stacked = (ma5 is not None and ma20 is not None and ma60 is not None
               and ma120 is not None and ma5 > ma20 > ma60 > ma120)
    return {
        "asof": bars[-1][0], "close": close,
        "ma5": ma5, "ma20": ma20, "ma60": ma60, "ma120": ma120,
        "above_ma20": bool(ma20 is not None and close >= ma20),
        "stacked_bull": bool(stacked),   # 정배열(5>20>60>120) — 관측 라벨(검증 전·기록만)
        "bars": len(bars),
    }


def _breadth_of(asof: str) -> dict | None:
    """asof 날짜 ledger의 market_context에서 breadth 수치. 없으면 None."""
    p = LEDGER_DIR / f"ledger_{asof}.json"
    if not p.exists():
        return None
    try:
        mc = (json.loads(p.read_text(encoding="utf-8")).get("market_context") or {})
        if mc.get("breadth_pct") is None:
            return None
        return {
            "breadth_pct": mc.get("breadth_pct"),
            "breadth_state": mc.get("breadth_state"),
            "up_3pct": mc.get("up_3pct"),
            "down_3pct": mc.get("down_3pct"),
            "limit_up": mc.get("limit_up"),
        }
    except Exception:  # noqa: BLE001
        return None


def _flows_of(asof: str) -> dict | None:
    """flow_market/kospi_investor.csv에서 asof 행의 외인/기관 순매수 부호(참고용)."""
    p = DATA_STORE / "flow_market" / "kospi_investor.csv"
    if not p.exists():
        return None
    key = asof.replace("-", "")
    try:
        with p.open(encoding="utf-8-sig") as f:
            for row in _csv.DictReader(f):
                if str(row.get("date", "")).strip() == key:
                    def _sign(v):
                        try:
                            x = float(v)
                        except (ValueError, TypeError):
                            return None
                        return "+" if x > 0 else ("-" if x < 0 else "0")
                    return {"foreign": _sign(row.get("외국인_금액")),
                            "inst": _sign(row.get("기관계_금액"))}
    except Exception:  # noqa: BLE001
        return None
    return None


def _next_trading_day(asof: str) -> str:
    """asof 다음 거래일 (trading_calendar 재사용·최대 10일 탐색)."""
    try:
        from data.trading_calendar import is_trading_day
        d = date.fromisoformat(asof)
        for _ in range(10):
            d = d + timedelta(days=1)
            if is_trading_day(d):
                return d.isoformat()
    except Exception:  # noqa: BLE001
        pass
    return ""


def _decide(breadth_pct, kospi_trend) -> str:
    """레짐 판정 — NO_GO만 검증된 신호. GO=회피 아님(상승예측 아님)."""
    if breadth_pct is None:
        return "UNKNOWN"
    if breadth_pct <= NO_GO_BREADTH:
        return "NO_GO"
    if breadth_pct < GO_BREADTH:
        return "CAUTION"
    if kospi_trend is not None and not kospi_trend.get("above_ma20"):
        return "CAUTION"
    return "GO"


def _load_history() -> list:
    if not HISTORY_PATH.exists():
        return []
    try:
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8")).get("records", [])
    except Exception:  # noqa: BLE001
        return []


def _validate_history(records: list) -> dict:
    """과거 예측에 실제 결과 충전 + NO_GO 성적 요약 (forward 자가검증)."""
    n_scored = 0
    stats = {"NO_GO": {"n": 0, "hit_down": 0}, "CAUTION": {"n": 0, "hit_down": 0},
             "GO": {"n": 0, "hit_down": 0}}
    for r in records:
        if r.get("actual_breadth_pct") is None:
            b = _breadth_of(r.get("applies_to", ""))
            if b:
                r["actual_breadth_pct"] = b["breadth_pct"]
                r["actual_state"] = b["breadth_state"]
        ab = r.get("actual_breadth_pct")
        reg = r.get("regime")
        if ab is not None and reg in stats:
            n_scored += 1
            stats[reg]["n"] += 1
            if ab <= NO_GO_BREADTH:
                stats[reg]["hit_down"] += 1
    for k, v in stats.items():
        v["down_rate_pct"] = round(100 * v["hit_down"] / v["n"], 1) if v["n"] else None
    return {"n_scored": n_scored, "by_regime": stats,
            "read": "NO_GO.down_rate가 GO.down_rate보다 유의하게 높아야 게이트 유효 — 미달 시 폐기"}


def _is_trading_day_safe(asof: str) -> bool:
    """asof가 거래일인지 (trading_calendar 실패 시 True=저장 허용·보수적)."""
    try:
        from data.trading_calendar import is_trading_day
        return bool(is_trading_day(date.fromisoformat(asof)))
    except Exception:  # noqa: BLE001
        return True


def build_regime(asof: str | None = None, save: bool = True) -> dict:
    """asof(기본=오늘) 마감 데이터로 다음 거래일 레짐 라벨 생성 + 이력 자가검증.

    7/4 검수 fix 저장 가드 3중:
    ① based_on이 비거래일이면 저장 스킵 (주말 수동 실행이 금요일의 유효 레코드를
       UNKNOWN으로 덮어쓰는 사고 방지 — applies_to 멱등 키 충돌)
    ② applies_to 산출 실패("")면 저장 스킵 (빈 키 상호 교체·영구 미채점 잔존 방지)
    ③ 기존 유효(non-UNKNOWN) 레코드를 UNKNOWN이 교체 금지 (방어 2겹)"""
    asof = asof or date.today().isoformat()
    breadth = _breadth_of(asof)
    kospi = _index_trend(KOSPI_PROXY, asof)
    kosdaq = _index_trend(KOSDAQ_PROXY, asof)
    flows = _flows_of(asof)
    bpct = breadth["breadth_pct"] if breadth else None
    regime = _decide(bpct, kospi)
    applies_to = _next_trading_day(asof)

    record = {
        "based_on": asof,
        "applies_to": applies_to,
        "regime": regime,
        "breadth": breadth,
        "index_proxy": {"kospi_069500": kospi, "kosdaq_229200": kosdaq},
        "flows_d": flows,
        "actual_breadth_pct": None,   # applies_to 지난 뒤 자가검증 충전
        "actual_state": None,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 이력 갱신 (applies_to 멱등 — 같은 날 재실행 시 교체) + 자가검증 충전
    prior = _load_history()
    existing = next((r for r in prior if r.get("applies_to") == applies_to), None)
    skip_reason = None
    if not _is_trading_day_safe(asof):
        skip_reason = f"based_on={asof} 비거래일 — 저장 스킵(가드①)"
    elif not applies_to:
        skip_reason = "applies_to 산출 실패 — 저장 스킵(가드②)"
    elif (existing and regime == "UNKNOWN"
          and existing.get("regime") not in (None, "UNKNOWN")):
        skip_reason = (f"기존 유효 레코드({existing.get('regime')}) 보호 — "
                       f"UNKNOWN 교체 금지(가드③)")
    if skip_reason:
        save = False
        logger.warning(f"[market_regime] {skip_reason}")
        records = prior
    else:
        records = [r for r in prior if r.get("applies_to") != applies_to]
        records.append(record)
        records.sort(key=lambda r: r.get("applies_to", ""))
    validation = _validate_history(records)

    out = {
        "latest": record,
        "validation": validation,
        "save_skipped": skip_reason,   # None=정상 저장 / 문자열=가드 발동(미저장)
        "thresholds": {"no_go_breadth": NO_GO_BREADTH, "go_breadth": GO_BREADTH},
        "note": "record-only 시장 레짐 관측(7/4 신설). NO_GO(전일 breadth≤0.45)만 검증된 회피 신호 "
                "(63거래일: DOWN precision 51.6% vs base 42.2%·손절터치 -26~28%). "
                "GO=회피 아님일 뿐 상승예측 아님(UP 예측 무정보 실측). 정배열=관측 라벨(검증 전). "
                "매수/매도/picks/SAJANG 무접촉·관측 없이 flip 금지·라이브 승격=forward 성적+사장님 결정.",
    }
    if save:
        try:
            HISTORY_PATH.write_text(
                json.dumps({"records": records,
                            "updated_at": record["generated_at"]},
                           ensure_ascii=False, indent=1), encoding="utf-8")
            OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[market_regime] 저장 실패: {e}")

    logger.info(
        f"[market_regime] {asof} 마감 기준 → {applies_to or '다음 거래일'} 레짐={regime} "
        f"(breadth={bpct} / 코스피프록시 20일선 {'위' if kospi and kospi.get('above_ma20') else '아래/미상'}"
        f"{'·정배열' if kospi and kospi.get('stacked_bull') else ''}) — record-only"
    )
    return out


if __name__ == "__main__":
    import argparse
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=None, help="기준일 YYYY-MM-DD (기본=오늘)")
    a = ap.parse_args()
    r = build_regime(asof=a.asof)
    lt = r["latest"]
    v = r["validation"]
    print(f"\n=== 시장 레짐 게이트 (based_on {lt['based_on']} → applies {lt['applies_to']}) ===")
    print(f"레짐: {lt['regime']}  breadth={lt['breadth'] and lt['breadth'].get('breadth_pct')}")
    k = lt["index_proxy"]["kospi_069500"]
    if k:
        print(f"코스피프록시(069500): close {k['close']} | MA20 {k['ma20']} | MA60 {k['ma60']} | "
              f"MA120 {k['ma120']} | 20일선 {'위' if k['above_ma20'] else '아래'} | "
              f"정배열 {'O' if k['stacked_bull'] else 'X'}")
    q = lt["index_proxy"]["kosdaq_229200"]
    if q:
        print(f"코스닥프록시(229200): close {q['close']} | MA20 {q['ma20']} | 정배열 {'O' if q['stacked_bull'] else 'X'}")
    print(f"자가검증: {v['n_scored']}건 채점 | regime별 익일 DOWN율 "
          f"{ {kk: vv.get('down_rate_pct') for kk, vv in v['by_regime'].items()} }")
    print("★ record-only·NO_GO만 검증된 회피 신호·GO=상승예측 아님·flip은 사장님 결정.")
