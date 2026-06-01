# -*- coding: utf-8 -*-
"""휴면→첫급등 연속자 워치리스트 생성기 (6/1, shadow-first, read-only).

사장님 5/31 reframe: 3번째 상한가(끝물) 말고, 멀쩡하던 놈이 갑자기 첫 급등/상한가 →
  그 이후 '계속 갈 놈'을 패턴으로 거른다. 5/31 데이터 검증(db5cd76, 20f89a7):
  필터=종가강도≥0.8 & 거래량≥5x & 과열회피(당일상승≤30%) → STOP_REENTER 리프트 +1.9~2.4%p·
  승률+7~8%p, 전 regime 견고. OHLCV만으로 계산(수급 불요).

★ 순수 read-only: 후보 JSON 저장 + 로그만. 주문경로·매도·picks·SAJANG 매매룰 무접촉.
★ shadow: 자동매수 X. 후보 관측 → forward 추적 후 검증 → (사장님 승인) reentry 실행 배선(별도, paper).
★ 수급(11주체 세력)은 hook(현재 None) — market_investor_collector(시장)/키움 opt10059(종목별, 대기).
설계: docs/02-design/watchlist_continuation_6_1.md  의뢰확정: 16:45 evening / 0.8·5x·30% / 시장11주체 병행.

사용: python -m data.watchlist_continuation [--date YYYYMMDD] [--top N]
"""
from __future__ import annotations
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np

from data.sajang_rules import SAJANG
from data.limit_up_scanner import (
    LimitUpStock, score_kki, count_surge_limit_days, _load_daily,
    DAILY_DIR, UNIVERSE_PATH,
)
from data.trading_calendar import is_trading_day
from utils.stock_utils import is_etf as _is_etf

logger = logging.getLogger("BH.WatchlistCont")

WL_DIR = DAILY_DIR.parent / "watchlist"


def _col(df, *names):
    for n in names:
        if n in df.columns:
            return n
    return names[-1]


def _atr_pct(highs, lows, closes, i, n=14) -> float:
    a = max(0, i - n)
    trs = []
    for k in range(a + 1, i + 1):
        trs.append(max(highs[k] - lows[k], abs(highs[k] - closes[k - 1]), abs(lows[k] - closes[k - 1])))
    return (sum(trs) / len(trs)) / closes[i] * 100 if trs and closes[i] > 0 else 0.0


def scan_continuation_watchlist(date_str: str | None = None, top_n: int = 0) -> list:
    """휴면→첫급등 연속자 후보 스캔. 반환 후보 dict 리스트(저장도 수행).

    Args:
        date_str: YYYYMMDD (None=각 종목 일봉 마지막행=최근거래일).
        top_n: >0이면 끼점수 상위 N만.
    """
    # 거래일 가드 (date 지정 시; None은 마지막행=거래일 데이터라 통과)
    if date_str:
        try:
            if not is_trading_day(datetime.strptime(date_str, "%Y%m%d").date()):
                logger.info(f"[WL] {date_str} 휴장 — 스캔 skip")
                return []
        except Exception:
            pass

    try:
        uni = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"[WL] universe 로드 실패: {e}")
        return []

    PRE = SAJANG.WL_PRE_DAYS
    cands = []
    for code, info in (uni.items() if isinstance(uni, dict) else []):
        name = info.get("name", code) if isinstance(info, dict) else code
        cap = info.get("cap_억", 0) if isinstance(info, dict) else 0
        if _is_etf(code):
            continue
        df = _load_daily(code)
        if df is None or len(df) < PRE + 2:
            continue
        cc = _col(df, "종가", "close"); ch = _col(df, "고가", "high")
        cl = _col(df, "저가", "low"); cv = _col(df, "거래량", "volume")
        closes = df[cc].values.astype(float); highs = df[ch].values.astype(float)
        lows = df[cl].values.astype(float); vols = df[cv].values.astype(float)
        n = len(closes)

        # 기준일 i (date 지정 시 해당일, 없으면 마지막행)
        i = n - 1
        if date_str:
            dates = [str(x)[:10].replace("-", "") for x in df.index]
            hit = [k for k, d in enumerate(dates) if d == date_str]
            if not hit:
                continue
            i = hit[-1]
        if i < PRE + 1 or i < 21:
            continue

        pc = closes[i - 1]
        if pc <= 0:
            continue
        up = closes[i] / pc - 1.0
        if up < SAJANG.WL_SURGE_MIN or up > SAJANG.WL_OVERHEAT:   # 첫급등 & 과열회피
            continue
        # 휴면: 직전 PRE일 최대 일간상승 < WL_DORMANT_MAX & 기지(pos20)
        prior_max = 0.0
        for j in range(i - PRE, i):
            ppc = closes[j - 1]
            if ppc > 0:
                prior_max = max(prior_max, closes[j] / ppc - 1.0)
        if prior_max >= SAJANG.WL_DORMANT_MAX:
            continue
        win_lo = lows[i - 20:i]; win_hi = highs[i - 20:i]
        band = win_hi.max() - win_lo.min()
        pos20 = (pc - win_lo.min()) / band if band > 0 else 0.5
        if pos20 > SAJANG.WL_BASE_POS:
            continue
        # 연속자 필터: 종가강도 & 거래량
        h0, l0, c0, v0 = highs[i], lows[i], closes[i], vols[i]
        cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
        if cs < SAJANG.WL_CS_MIN:
            continue
        vma = vols[i - 20:i].mean()
        volx = v0 / vma if vma > 0 else 0.0
        if volx < SAJANG.WL_VOL_MIN:
            continue
        tv_억 = c0 * v0 / 1e8
        if tv_억 < SAJANG.WL_LIQ_FLOOR_억:
            continue

        # 끼 (재사용)
        atrp = _atr_pct(highs, lows, closes, i)
        surge, limit = count_surge_limit_days(list(closes[:i + 1]))
        st = LimitUpStock(
            code=code, name=name, close=int(c0), volume_ratio=round(float(volx), 2),
            trading_value_억=round(float(tv_억), 1),
            turnover_pct=round(tv_억 / cap * 100, 2) if cap else 0,
            close_strength=round(float(cs), 3), consecutive_limit=0,
        )
        kki = score_kki(st, atrp, surge, limit)
        cands.append({
            "code": code, "name": name,
            "date": str(df.index[i])[:10],
            "surge_pct": round(up * 100, 1),
            "close_strength": round(float(cs), 3),
            "volume_ratio": round(float(volx), 1),
            "trading_value_억": round(float(tv_억), 1),
            "kki_score": kki, "kki_grade": SAJANG.kki_grade(kki),
            "supply_sehyeok": None,   # hook: 11주체 세력(금융투자) — Kiwoom 종목별 대기
        })

    cands.sort(key=lambda x: x["kki_score"], reverse=True)
    if top_n > 0:
        cands = cands[:top_n]

    # 저장 (shadow)
    WL_DIR.mkdir(parents=True, exist_ok=True)
    ds = date_str or datetime.now().strftime("%Y%m%d")
    fp = WL_DIR / f"continuation_{ds}.json"
    payload = {"generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
               "count": len(cands),
               "filter": {"surge_min": SAJANG.WL_SURGE_MIN, "cs_min": SAJANG.WL_CS_MIN,
                          "vol_min": SAJANG.WL_VOL_MIN, "overheat": SAJANG.WL_OVERHEAT},
               "candidates": cands}
    try:
        fp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"[WL] 저장 실패: {e}")

    grades = {}
    for c in cands:
        grades[c["kki_grade"]] = grades.get(c["kki_grade"], 0) + 1
    logger.info(f"[워치리스트/shadow] {len(cands)}후보 (끼분포 {grades}) -> {fp.name}")
    return cands


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYYMMDD (기본=일봉 마지막행)")
    ap.add_argument("--top", type=int, default=0)
    a = ap.parse_args()
    res = scan_continuation_watchlist(a.date, a.top)
    print(f"\n워치리스트 후보 {len(res)}건 (휴면→첫급등 연속자 필터)")
    for c in res[:20]:
        print(f"  {c['name']:<14}({c['code']}) 급등{c['surge_pct']:+.0f}% 종가강도{c['close_strength']:.2f} "
              f"거래량{c['volume_ratio']:.0f}x 끼{c['kki_score']:.0f}={c['kki_grade']}")
    print("★ shadow: 후보 관측·forward추적용. 자동매수 X. 수급(세력)은 hook(Kiwoom 대기).")
