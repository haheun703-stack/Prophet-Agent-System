# -*- coding: utf-8 -*-
"""10종목 1개월(D-30) 7대 요소 분석 — 사장님 5/31 지시.

7요소: 재무·재료·수급·기술위치·끼부림·스케쥴·시나리오.
★ 정직: 일봉 OHLC + 수급(investor) + universe(시총/PER/PBR) = 실측.
  재료(뉴스)·스케쥴(이벤트)은 로컬 데이터 없음 → '별도소스 필요'로 표기(날조 금지).
score_kki(4단 Stage1)를 실종목에 적용 = 자기 검증.
"""
from __future__ import annotations
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # scalper-agent
PROJ = ROOT.parent                                      # 프로젝트 루트
sys.path.insert(0, str(ROOT))

from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
FLOW = ROOT / "data_store" / "flow"
WIN = 22   # 약 1개월 거래일

STOCKS = [
    ("삼성전기", "009150"), ("LG이노텍", "011070"), ("삼화콘덴서", "001820"),
    ("삼성전기우", "009155"), ("LG전자", "066570"), ("현대오토에버", "307950"),
    ("피델릭스", "032580"), ("오션스바이오", "332190"), ("에스에이엠티", "031330"),
    ("나무기술", "242040"),
]


def _load_daily(name, code):
    for p in DAILY.glob(f"{name}_{code}.csv"):
        rows = []
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    rows.append((r.get("날짜") or r.get("") or "",
                                 float(r["open"]), float(r["high"]),
                                 float(r["low"]), float(r["close"]), float(r["volume"])))
                except (ValueError, KeyError):
                    continue
        return rows
    return []


def _load_flow(code):
    p = FLOW / f"{code}_investor.csv"
    if not p.exists():
        return []
    rows = []
    with open(p, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                rows.append((r["date"], float(r.get("외국인_금액", 0) or 0),
                             float(r.get("기관_금액", 0) or 0)))
            except (ValueError, KeyError):
                continue
    return rows


def _ma(closes, n):
    return sum(closes[-n:]) / n if len(closes) >= n else 0.0


def _atr_pct(rows):
    if len(rows) < 15:
        return 0.0
    seg = rows[-15:]
    trs = []
    for i in range(1, len(seg)):
        h, l, pc = seg[i][2], seg[i][3], seg[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    c = rows[-1][4]
    return (sum(trs) / len(trs)) / c * 100 if c > 0 else 0.0


def main():
    uni = {}
    try:
        uni = json.load(open(ROOT / "data_store" / "universe.json", encoding="utf-8"))
    except Exception:
        pass

    for name, code in STOCKS:
        rows = _load_daily(name, code)
        print("=" * 64)
        if len(rows) < 5:
            print(f"[{name} {code}] 일봉 데이터 부족 — 스킵")
            continue
        win = rows[-WIN:] if len(rows) >= WIN else rows
        closes = [r[4] for r in rows]
        c_now = rows[-1][4]
        c_30 = win[0][4]
        chg_30 = (c_now / c_30 - 1) * 100 if c_30 else 0
        hi_1m = max(r[2] for r in win)
        lo_1m = min(r[3] for r in win)
        rng_pos = (c_now - lo_1m) / (hi_1m - lo_1m) * 100 if hi_1m > lo_1m else 50
        ma20, ma60, ma120 = _ma(closes, 20), _ma(closes, 60), _ma(closes, 120)
        정배열 = c_now > ma20 > ma60 > ma120 and ma120 > 0
        u = uni.get(code, {})

        print(f"[{name} ({code})] {u.get('sector','?')} | 현재 {c_now:,.0f} | 1개월 {chg_30:+.1f}%")
        # 1 재무
        per, pbr, cap = u.get("per"), u.get("pbr"), u.get("cap_억", 0)
        print(f" ① 재무   : PER {per} / PBR {pbr} / 시총 {cap:,}억  ★상세재무(부채·유보)=DART 별도필요")
        # 2 재료 (없음)
        print(f" ② 재료   : ✖ 로컬 뉴스/테마 데이터 없음 → 정보봇(jgis)/뉴스 별도필요 (날조 안 함)")
        # 3 수급 (D-30 누적)
        flow = _load_flow(code)
        if flow:
            fwin = flow[-WIN:]
            f_net = sum(x[1] for x in fwin)
            i_net = sum(x[2] for x in fwin)
            f5 = sum(x[1] for x in flow[-5:])
            i5 = sum(x[2] for x in flow[-5:])
            dual = "동반매수" if f_net > 0 and i_net > 0 else ("외인만" if f_net > 0 else ("기관만" if i_net > 0 else "쌍매도/중립"))
            print(f" ③ 수급   : 1개월 외인 {f_net:+,.0f} / 기관 {i_net:+,.0f} (백만원추정) | 최근5일 외{f5:+,.0f}·기{i5:+,.0f} | {dual}")
        else:
            print(f" ④ 수급   : ✖ {code} 수급파일 없음")
        # 4 기술위치
        ev = "정배열✅" if 정배열 else "정배열아님"
        print(f" ④ 기술위치: {ev} | MA20 {ma20:,.0f}/MA60 {ma60:,.0f}/MA120 {ma120:,.0f} | 1M고 {hi_1m:,.0f}/저 {lo_1m:,.0f} | 위치 {rng_pos:.0f}%")
        # 5 끼부림 (score_kki)
        vr = rows[-1][5] / (sum(r[5] for r in rows[-20:]) / 20) if len(rows) >= 20 and sum(r[5] for r in rows[-20:]) > 0 else 1.0
        cs = (rows[-1][4] - rows[-1][3]) / (rows[-1][2] - rows[-1][3]) if rows[-1][2] > rows[-1][3] else 0.5
        tv_억 = c_now * rows[-1][5] / 1e8
        turnover = tv_억 / cap * 100 if cap else 0.0
        consec = 0
        for i in range(len(rows) - 1, 0, -1):
            if rows[i - 1][4] > 0 and (rows[i][4] / rows[i - 1][4] - 1) >= 0.295:
                consec += 1
            else:
                break
        surge, limit = count_surge_limit_days(closes)
        st = LimitUpStock(code=code, name=name, close=int(c_now), volume_ratio=round(vr, 2),
                          trading_value_억=round(tv_억, 1), turnover_pct=round(turnover, 2),
                          close_strength=round(cs, 3), consecutive_limit=consec)
        kki = score_kki(st, _atr_pct(rows), surge, limit)
        print(f" ⑤ 끼부림 : score_kki {kki} = {SAJANG.kki_grade(kki)} | ATR {_atr_pct(rows):.1f}% | 급등{surge}/상한{limit}일 | 거래대금 {tv_억:.0f}억 | 거래량 {vr:.1f}x")
        # 6 스케쥴 (없음)
        print(f" ⑥ 스케쥴 : ✖ 로컬 이벤트캘린더 없음 → 실적·이벤트 D-day 별도필요")
        # 7 시나리오 (SAJANG)
        sl = SAJANG.get_normal_sl(c_now)
        trail = SAJANG.get_trailing_sl(hi_1m)
        print(f" ⑦ 시나리오: 진입 {c_now:,.0f} | 손절(-3%) {sl:,.0f} | 고점트레일 {trail:,.0f} | 보유 D+3(高끼면 RIDE) | 줄먹 +{SAJANG.RULE_B_THRESHOLD:.0f}% 절반")

    print("=" * 64)
    print("★ 실측: 재무(PER/PBR/시총)·수급·기술위치·끼부림·시나리오. 재료·스케쥴=별도소스(정직).")
    print("★ score_kki = 4단 Stage1 실적용. 끼 등급으로 '나무기술류 단타 적합' 1차 선별.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
