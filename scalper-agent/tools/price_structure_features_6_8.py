# -*- coding: utf-8 -*-
"""price_structure_features — 6/8 관측 레이어: 가격구조 feature/label (read-only).

★ 사장님 지시서(6/6): 기존 3-Type 구조(A1/A2/B/C) 불변. 매수 조건 변경 X.
  이번 자료의 개념을 ★ feature/label 추가로만 ★ ledger에 붙여 6/8~6/12 forward 검증:
  "주봉 시가 위 + 반기 주도축 + 같은 세부그룹 동조 + 첫봉보다 투봉/하프 확인"이 실제로 더 돈이 되는가.

추가 라벨(5+1):
  1) 주봉 시가 위/아래   weekly_open_state   (ABOVE/BELOW)
  2) 월봉/반기 시가 위/아래  monthly_open_state · half_year_open_state
  3) 첫봉/투봉           float_candle_state  (FIRST_FLOAT / SECOND_FLOAT / FLOAT_CONTINUED / NOT_FLOAT)
  4) 돌파(C)/하프 눌림(B)  breakout_state · half_pullback_state
  5) 섹터·그룹 동조화     sector_peer_sync_state (STRONG/MODERATE/WEAK)
  6) 연간 +500% 위험      annual_overheat_warning · overheat_grade

★ 원칙(불변): 실주문 0 / 주문함수 0 / scheduler·SAJANG 무변경 / picks 불변 / 봇 OFF.
  ★ hard gate 0 — 전부 기록·태그만. feature를 진입 조건으로 승격 금지(사장님 12장 금지선).
  ★ 계산 불가 시 None — 0/거짓으로 채우기 금지("키 존재=계산 시도 / 값 None=미도달", ledger 무결성).

데이터: stock_data_daily 일봉 d=[(date,open,high,low,close,volume)...] 오름차순, i=asof 인덱스.
  순수 함수 — 네트워크 0, 부작용 0, SAJANG·주문경로 미참조.

한계(정직 — 6/12 판정 시 caveat):
  - 섹터 동조화: 현 SECTORS는 12 대분류 + tier만 보유. 자료가 강조한 '세부그룹(HBM/장비/소재
    /PCB/MLCC)'은 데이터 부재 → 12-섹터 근사(sync_granularity로 명시). 정밀 세부그룹은 별도 숙제.
  - 기준선: 일목 기준선(26일 (최고+최저)/2) — 자료의 '검은 기준선' 근사.
  - 일봉 기반: 장중 흔들림 과소(다른 forward 부품과 동일 한계). 절대수익 신뢰 X, 상대비교만.
"""
from __future__ import annotations

from datetime import date

# data row 인덱스: 0=date 1=open 2=high 3=low 4=close 5=volume
BASELINE_N = 26               # 일목 기준선 기간 ('검은 기준선' 근사)
TRADING_YEAR = 252            # 1년 거래일 근사
OVERHEAT_EXTREME = 500.0      # 연간 +500%↑ = 추격 위험(자료 핵심)
HALF_PULLBACK_LOOKBACK = 20   # 하프 눌림 스윙 구간
BREAKOUT_LOOKBACK = 20        # 돌파 = 최근 20일 고가
BREAKOUT_VOL_X = 1.2          # 돌파 거래량 증가 임계
SYNC_STRONG = 3               # 같은 섹터 동반 강세 ≥3 = STRONG (자료: 3종목 이상)
SYNC_MODERATE = 2             # 2종목 = MODERATE


# ───────────────────────── 기간 시가 (주봉/월봉/반기) ─────────────────────────
def find_index_asof(d, asof):
    """d에서 asof(YYYY-MM-DD) 이하의 가장 늦은 인덱스. 없으면 None (lookahead 없음)."""
    return next((j for j in range(len(d) - 1, -1, -1) if d[j][0] <= asof), None)


def _period_first_open(d, i, key_fn):
    """d[i]가 속한 기간(key_fn으로 그룹)의 첫 거래일 open. 양수 아니면 None."""
    if i < 0 or i >= len(d):
        return None
    cur = key_fn(d[i][0])
    j = i
    while j - 1 >= 0 and key_fn(d[j - 1][0]) == cur:
        j -= 1
    op = d[j][1]
    return op if op and op > 0 else None


def _state(close, open_px):
    """종가가 기준 시가 위/아래. 계산 불가 시 None."""
    if open_px is None or open_px <= 0 or close is None or close <= 0:
        return None
    return "ABOVE" if close > open_px else "BELOW"


def _wk(s):    # ISO (year, week) — 주 경계
    y, w, _ = date.fromisoformat(s).isocalendar()
    return (y, w)


def _mo(s):    # YYYY-MM
    return s[:7]


def _half(s):  # (YYYY, H1/H2) — 1~6월=H1, 7~12월=H2
    return (s[:4], "H1" if int(s[5:7]) <= 6 else "H2")


def weekly_open(d, i):
    op = _period_first_open(d, i, _wk)
    return {"weekly_open": round(op, 2) if op else None,
            "weekly_open_state": _state(d[i][4], op)}


def monthly_open(d, i):
    op = _period_first_open(d, i, _mo)
    return {"monthly_open": round(op, 2) if op else None,
            "monthly_open_state": _state(d[i][4], op)}


def half_year_open(d, i):
    op = _period_first_open(d, i, _half)
    return {"half_year_open": round(op, 2) if op else None,
            "half_year_open_state": _state(d[i][4], op)}


# ───────────────────────── 첫봉 / 투봉 (일목 기준선) ─────────────────────────
def _baseline(d, i, n=BASELINE_N):
    """일목 기준선 = 최근 n일 (최고가+최저가)/2. 데이터 부족 시 None."""
    if i < n - 1:
        return None
    win = d[i - n + 1:i + 1]
    return (max(x[2] for x in win) + min(x[3] for x in win)) / 2.0


def _is_float(d, k, base):
    """뜬봉 = OHLC 4개 중 3개↑ 기준선 위 / 떨봉 = 3개↑ 아래 / 2:2 = 중립(None).
    base None(데이터 부족)이면 None."""
    if base is None:
        return None
    o, h, l, c = d[k][1], d[k][2], d[k][3], d[k][4]
    above = sum(1 for v in (o, h, l, c) if v > base)
    if above >= 3:
        return True
    if above <= 1:
        return False
    return None


def float_candle(d, i):
    """하락 구간 이후 기준선 위로 뜬 봉. 첫봉(전환 시작, 흔들림 큼) vs 투봉(더 안정).
    연속 뜬봉 run 길이로 판정: 1=FIRST, 2=SECOND, 3+=FLOAT_CONTINUED, 뜬봉아님=NOT_FLOAT."""
    base = _baseline(d, i)
    if base is None:
        return {"baseline_26": None, "first_float_candle": None,
                "second_float_candle": None, "float_candle_state": None}
    if not _is_float(d, i, base):     # False(떨봉) or None(중립) → 뜬봉 아님
        return {"baseline_26": round(base, 2), "first_float_candle": False,
                "second_float_candle": False, "float_candle_state": "NOT_FLOAT"}
    run = 1
    k = i - 1
    while k >= 0 and _is_float(d, k, _baseline(d, k)):
        run += 1
        k -= 1
    first, second = (run == 1), (run == 2)
    state = ("FIRST_FLOAT" if first else "SECOND_FLOAT" if second else "FLOAT_CONTINUED")
    return {"baseline_26": round(base, 2), "first_float_candle": first,
            "second_float_candle": second, "float_candle_state": state}


# ───────────────────────── 연간 +500% 과열 위험 ─────────────────────────
def annual_overheat(d, i):
    """1년(252거래일) 수익률. +500%↑ = 추격 위험(자료: 다음 해 위험 가능성). hard gate 아님."""
    k = i - TRADING_YEAR
    if k < 0 or d[k][4] <= 0:
        return {"return_1y_pct": None, "annual_overheat_warning": None, "overheat_grade": None}
    ret = (d[i][4] / d[k][4] - 1) * 100
    grade = ("EXTREME_500" if ret >= 500 else "HIGH_300" if ret >= 300
             else "ELEVATED_200" if ret >= 200 else "NORMAL")
    return {"return_1y_pct": round(ret, 1),
            "annual_overheat_warning": bool(ret >= OVERHEAT_EXTREME),
            "overheat_grade": grade}


# ───────────────────────── 통합 가격구조 라벨 (A/B/C 공통) ─────────────────────────
def price_structure_labels(d, i):
    """주봉/월봉/반기 시가 + 첫봉·투봉 + 연간 과열 (flat dict — 지시서 2장 예시 구조).
    i가 유효하지 않으면 전 필드 None."""
    if d is None or i is None or i < 0 or i >= len(d):
        return {k: None for k in (
            "weekly_open", "weekly_open_state", "monthly_open", "monthly_open_state",
            "half_year_open", "half_year_open_state", "baseline_26",
            "first_float_candle", "second_float_candle", "float_candle_state",
            "return_1y_pct", "annual_overheat_warning", "overheat_grade")}
    out = {}
    out.update(weekly_open(d, i))
    out.update(monthly_open(d, i))
    out.update(half_year_open(d, i))
    out.update(float_candle(d, i))
    out.update(annual_overheat(d, i))
    return out


# ───────────────────────── 하프 눌림 (B = ROTATION_PULLBACK) ─────────────────────────
def half_pullback(d, i, weekly_open_state=None, half_year_open_state=None,
                  lookback=HALF_PULLBACK_LOOKBACK):
    """진짜 눌림 = 하프(스윙 고/저 중간) 자리. 강한 종목은 하프 지키고 다시 간다.
    VALID = 절반 위 유지 + (주봉 or 반기 시가 위) / BROKEN = 절반 이탈 or 구조 깨짐 /
    NO_HALF_PULLBACK = 눌림 거의 없음(고점 근처)."""
    if i < lookback:
        return {"half_pullback_state": None, "half_pullback_mid": None, "half_pullback_pos": None}
    win = d[i - lookback:i + 1]
    hi = max(x[2] for x in win)
    lo = min(x[3] for x in win)
    close = d[i][4]
    if hi <= lo or close <= 0:
        return {"half_pullback_state": None, "half_pullback_mid": None, "half_pullback_pos": None}
    half = (hi + lo) / 2.0
    draw = (close / hi - 1) * 100
    structure_ok = (weekly_open_state == "ABOVE") or (half_year_open_state == "ABOVE")
    if draw > -3.0:
        state = "NO_HALF_PULLBACK"          # 눌림 거의 없음 — 하프 논할 단계 아님
    elif close < half:
        state = "HALF_PULLBACK_BROKEN"      # 절반 아래로 이탈
    elif structure_ok:
        state = "HALF_PULLBACK_VALID"       # 절반 위 유지 + 주봉/반기 시가 위
    else:
        state = "HALF_PULLBACK_BROKEN"      # 절반 위지만 구조(주봉/반기) 깨짐 = 위험
    return {"half_pullback_state": state, "half_pullback_mid": round(half, 2),
            "half_pullback_pos": round((close - lo) / (hi - lo), 3)}


# ───────────────────────── 돌파 (C = ROTATION_RIDE) ─────────────────────────
def breakout(d, i, sector_status=None, lookback=BREAKOUT_LOOKBACK):
    """돌파 = 추세 시작의 열쇠. 최근 lookback 고가 돌파 + 거래량 증가 + 섹터 강세 동반.
    BREAKOUT = 종가까지 돌파 유지 + 거래량 + 섹터강세 / BREAKOUT_FAIL = 장중만 or 미흡 /
    NO_BREAKOUT = 돌파 없음."""
    if i < lookback:
        return {"breakout_state": None, "breakout_high20": None, "breakout_vol_ratio": None}
    prev = d[i - lookback:i]                 # 직전 lookback (당일 제외)
    hi_prev = max(x[2] for x in prev)
    close, high = d[i][4], d[i][2]
    vols = [x[5] for x in d[max(0, i - 5):i]]
    avg = sum(vols) / len(vols) if vols else d[i][5]
    vr = d[i][5] / avg if avg > 0 else 0.0
    sector_ok = sector_status in ("HOT", "WARMING", "RELAY")
    if high < hi_prev:
        state = "NO_BREAKOUT"
    elif close >= hi_prev and vr >= BREAKOUT_VOL_X and sector_ok:
        state = "BREAKOUT"
    else:
        state = "BREAKOUT_FAIL"              # 장중 돌파했으나 종가/거래량/섹터 미흡
    return {"breakout_state": state, "breakout_high20": round(hi_prev, 2),
            "breakout_vol_ratio": round(vr, 2)}


# ───────────────────────── 섹터·그룹 동조화 ─────────────────────────
def sector_peer_sync(sec_stocks, self_code):
    """주도주는 혼자 안 간다 — 같은 섹터 동반 강세 종목 수. 자기 제외.
    강세 = 당일 상승(c1d>0) AND 5일 양(c5d>0). STRONG≥3 / MODERATE 2 / WEAK≤1.
    ★ 한계: 현 데이터는 12-섹터 대분류만(세부그룹 부재) → 근사. sync_granularity로 명시."""
    if not sec_stocks:
        return {"sector_peer_sync_count": None, "sector_peer_sync_names": None,
                "sector_peer_sync_state": None, "sync_granularity": "sector_12_approx(no_subgroup)"}
    peers = [s for s in sec_stocks if s.get("code") != self_code]
    strong = [s for s in peers if (s.get("c1d") or 0) > 0 and (s.get("c5d") or 0) > 0]
    cnt = len(strong)
    state = ("STRONG" if cnt >= SYNC_STRONG else "MODERATE" if cnt >= SYNC_MODERATE else "WEAK")
    return {"sector_peer_sync_count": cnt,
            "sector_peer_sync_names": [s.get("name") for s in strong][:10],
            "sector_peer_sync_state": state,
            "sync_granularity": "sector_12_approx(no_subgroup)"}


# ─────────────────────────────── selftest (합성, 네트워크 0) ───────────────────────────────
def _seq_dates(n, start="2024-01-01"):
    out, cur = [], date.fromisoformat(start)
    from datetime import timedelta
    while len(out) < n:
        if cur.weekday() < 5:           # 평일만(거래일 근사)
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def selftest():
    ok = []

    # T1 주봉/월봉 시가 — 같은 주(2026-06-01 월~04 목) 첫날 open
    dw = [("2026-06-01", 1000, 1010, 990, 1005, 1),
          ("2026-06-02", 1005, 1020, 1000, 1010, 1),
          ("2026-06-03", 1010, 1030, 1005, 1020, 1),
          ("2026-06-04", 1020, 1040, 1015, 1100, 1)]
    w = weekly_open(dw, 3)
    m = monthly_open(dw, 3)
    ok.append(("T1 weekly_open=첫날 open · ABOVE",
               w["weekly_open"] == 1000 and w["weekly_open_state"] == "ABOVE"))
    ok.append(("T1b monthly_open=월 첫날 · ABOVE",
               m["monthly_open"] == 1000 and m["monthly_open_state"] == "ABOVE"))
    # 종가가 시가 아래 → BELOW
    dw2 = list(dw); dw2[3] = ("2026-06-04", 1020, 1040, 980, 990, 1)
    ok.append(("T1c close<weekly_open → BELOW", weekly_open(dw2, 3)["weekly_open_state"] == "BELOW"))

    # T2 반기 시가 — H1 첫 거래일 open
    h = half_year_open(dw, 3)
    ok.append(("T2 half_year_open(H1 첫날) · ABOVE",
               h["half_year_open"] == 1000 and h["half_year_open_state"] == "ABOVE"))

    # T3 첫봉/투봉 — 횡보26 → 하락6(떨봉) → 급등(첫 뜬봉)
    ds = _seq_dates(40)
    d = [(ds[k], 10000, 10050, 9950, 10000, 100000) for k in range(26)]          # 횡보
    d += [(ds[26 + k], 9200, 9250, 9100, 9150, 100000) for k in range(6)]        # 하락(떨봉)
    d.append((ds[32], 11000, 11100, 10900, 11050, 300000))                        # 급등(첫 뜬봉)
    fc = float_candle(d, 32)
    ok.append(("T3 첫 뜬봉 → FIRST_FLOAT",
               fc["first_float_candle"] is True and fc["float_candle_state"] == "FIRST_FLOAT"))
    d.append((ds[33] if len(ds) > 33 else "2024-02-20", 11050, 11200, 11000, 11150, 200000))
    fc2 = float_candle(d, 33)
    ok.append(("T3b 둘째 뜬봉 → SECOND_FLOAT",
               fc2["second_float_candle"] is True and fc2["float_candle_state"] == "SECOND_FLOAT"))
    ok.append(("T3c 떨봉 → NOT_FLOAT", float_candle(d, 30)["float_candle_state"] == "NOT_FLOAT"))
    ok.append(("T3d 데이터 부족(i<26) → None", float_candle(d, 10)["float_candle_state"] is None))

    # T4 연간 과열 — 252일 전 대비 +500%
    da = [(_seq_dates(253)[k], 1000.0, 1000, 1000, 1000.0, 1) for k in range(252)]
    da.append((_seq_dates(253)[252], 6000, 6000, 6000, 6000.0, 1))
    ao = annual_overheat(da, 252)
    ok.append(("T4 +500% → EXTREME_500 · warning True",
               ao["overheat_grade"] == "EXTREME_500" and ao["annual_overheat_warning"] is True
               and abs(ao["return_1y_pct"] - 500.0) < 0.1))
    ok.append(("T4b 데이터 부족 → None", annual_overheat(da, 10)["return_1y_pct"] is None))

    # T5 하프 눌림 — 스윙 고12000/저8000, half=10000, close=10000
    ds5 = _seq_dates(22)
    dh = [(ds5[0], 8000, 8000, 7990, 8000, 1)]
    dh += [(ds5[k], 10000, 12000, 9000, 11000, 1) for k in range(1, 20)]
    dh.append((ds5[20], 10100, 10100, 9900, 10000, 1))
    ok.append(("T5 절반 위+구조 위 → VALID",
               half_pullback(dh, 20, "ABOVE", "ABOVE")["half_pullback_state"] == "HALF_PULLBACK_VALID"))
    ok.append(("T5b 절반 위지만 구조 깨짐 → BROKEN",
               half_pullback(dh, 20, "BELOW", "BELOW")["half_pullback_state"] == "HALF_PULLBACK_BROKEN"))
    dh2 = list(dh); dh2[20] = (ds5[20], 9000, 9000, 8500, 8800, 1)              # close<half
    ok.append(("T5c 절반 아래 이탈 → BROKEN",
               half_pullback(dh2, 20, "ABOVE", "ABOVE")["half_pullback_state"] == "HALF_PULLBACK_BROKEN"))
    dh3 = list(dh); dh3[20] = (ds5[20], 11900, 12000, 11800, 11950, 1)         # 고점 근처
    ok.append(("T5d 눌림 거의 없음 → NO_HALF_PULLBACK",
               half_pullback(dh3, 20, "ABOVE", "ABOVE")["half_pullback_state"] == "NO_HALF_PULLBACK"))

    # T6 돌파 — 직전20 고가 11000, 당일 12000 돌파 + 거래량 3x
    ds6 = _seq_dates(22)
    db = [(ds6[k], 10000, 11000, 9900, 10500, 100000) for k in range(20)]
    db.append((ds6[20], 11500, 12000, 11400, 11900, 300000))
    ok.append(("T6 돌파+거래량+섹터HOT → BREAKOUT",
               breakout(db, 20, "HOT")["breakout_state"] == "BREAKOUT"))
    ok.append(("T6b 섹터 약함 → BREAKOUT_FAIL",
               breakout(db, 20, "COLD")["breakout_state"] == "BREAKOUT_FAIL"))
    db2 = list(db); db2[20] = (ds6[20], 10400, 10600, 10300, 10500, 300000)    # 고가 미돌파
    ok.append(("T6c 돌파 없음 → NO_BREAKOUT",
               breakout(db2, 20, "HOT")["breakout_state"] == "NO_BREAKOUT"))

    # T7 섹터 동조화
    st = [{"code": "A", "name": "a", "c1d": 1, "c5d": 1},
          {"code": "B", "name": "b", "c1d": 1, "c5d": 1},
          {"code": "C", "name": "c", "c1d": 1, "c5d": 1},
          {"code": "SELF", "name": "self", "c1d": 5, "c5d": 5}]
    ps = sector_peer_sync(st, "SELF")
    ok.append(("T7 peer 3종 강세(자기제외) → STRONG count3",
               ps["sector_peer_sync_count"] == 3 and ps["sector_peer_sync_state"] == "STRONG"))
    st2 = [{"code": "A", "name": "a", "c1d": 1, "c5d": 1},
           {"code": "B", "name": "b", "c1d": -1, "c5d": 1}, {"code": "SELF", "name": "self"}]
    ps2 = sector_peer_sync(st2, "SELF")
    ok.append(("T7b 1종 강세 → WEAK", ps2["sector_peer_sync_count"] == 1
               and ps2["sector_peer_sync_state"] == "WEAK"))
    ok.append(("T7c 빈 섹터 → None", sector_peer_sync([], "SELF")["sector_peer_sync_count"] is None))

    # T8 통합 라벨 — 13키 존재, 부족 인덱스도 키 보존(None)
    lab = price_structure_labels(d, 32)
    keys = ("weekly_open_state", "monthly_open_state", "half_year_open_state",
            "float_candle_state", "annual_overheat_warning", "overheat_grade")
    ok.append(("T8 통합 라벨 전 키 존재", all(k in lab for k in keys)))
    lab_bad = price_structure_labels(d, None)
    ok.append(("T8b i=None → 전 키 None(키 보존)",
               lab_bad["weekly_open_state"] is None and "float_candle_state" in lab_bad))

    # T9 find_index_asof — lookahead 없음
    ok.append(("T9 find_index_asof(중간날)",
               find_index_asof(dw, "2026-06-02") == 1))
    ok.append(("T9b find_index_asof(데이터 이전) → None",
               find_index_asof(dw, "2020-01-01") is None))

    print("price_structure_features 셀프테스트:")
    for nme, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
    return all(p for _, p in ok)


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(0 if selftest() else 1)
