# -*- coding: utf-8 -*-
"""바닥 초입 포착 early variant shadow (6/16 설계 구현, shadow-first·read-only).

설계: docs/02-design/early_entry_shadow_6_15.md
근거: 원익IPS 6/4 pos20=0.03 초입(종가→6/12 +44%) vs 6/12 pos20=0.96 추격(d1=-4.8%).
  변별자 = 거래량 아닌 직전 위치(pos20). 거래량 역설(초입 1.6x < 추격 3.8x).

strict(현행 WL_*)와 early(EARLY_*)를 같은 유니버스·같은 날 병렬 shadow로 산출,
한 누적 json에 variant 라벨로 적재 → 6/16~ 2주 forward 관측 → 사장님 결정.

★★★ 안전 불변식 (기존 shadow와 동일) ★★★
- read-only: 일봉 csv만(네트워크 0). 주문함수·picks·recommendation.json·SAJANG 매도헬퍼·order_intent 0 접촉.
- record-only: 후보 json + forward만 기록. 매수 행동 0. ON/OFF 플래그 없음(생성기는 항상 돌되 shadow=관측만).
- would_stop_day는 SAJANG.TRAILING_PCT(=3.0) 상수만 참조(매도 헬퍼 호출 X) — "-3% 손절 가정 시 빈도" 관측용.
- 관측 없이 flip 금지 (5/31·5/26 사고 교훈). 검증 후 사장님 승인 시에만 매수 연결(별도, paper).

계산 단일진실(재사용):
- 끼/일봉: data.limit_up_scanner (score_kki, count_surge_limit_days, _load_daily, LimitUpStock, ...)
- atr%/컬럼: data.watchlist_continuation (_atr_pct, _col) — strict 게이트와 동일 계산 보장
- forward 일봉행: data.sector_reversal_shadow (_daily_rows) — update_forward 규격 일치

실행(러너): tools/run_early_variant_shadow_daily.py
  python -c "from data.early_variant_shadow import build_early_shadow,update_early_forward; \
             build_early_shadow(); update_early_forward()"
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from data.sajang_rules import SAJANG
from data.limit_up_scanner import (
    LimitUpStock, score_kki, count_surge_limit_days, _load_daily,
    DAILY_DIR, UNIVERSE_PATH,
)
from data.watchlist_continuation import _atr_pct, _col
from data.sector_reversal_shadow import _daily_rows
from data.trading_calendar import is_trading_day
from utils.stock_utils import is_etf as _is_etf

logger = logging.getLogger("BH.EarlyVariantShadow")

SHADOW_OUT = DAILY_DIR.parent / "early_variant_shadow.json"

FWD_OFFSETS = (1, 3, 5)
MFE_MAE_WINDOW = 5      # D+1~D+5


# ───────────────────────── variant 게이트 (SAJANG 단일진실) ─────────────────────────
def _strict_pass(up, prior_max, pos20, cs, volx, tv) -> bool:
    """현행 strict (휴면<10% · pos20≤0.60 · 거래량≥5x · 급등 · 과열 · 종가강도 · 유동성)."""
    return (
        SAJANG.WL_SURGE_MIN <= up <= SAJANG.WL_OVERHEAT
        and prior_max < SAJANG.WL_DORMANT_MAX
        and pos20 <= SAJANG.WL_BASE_POS
        and cs >= SAJANG.WL_CS_MIN
        and volx >= SAJANG.WL_VOL_MIN
        and tv >= SAJANG.WL_LIQ_FLOOR_억
    )


def _early_pass(up, prior_max, pos20, cs, volx, tv) -> bool:
    """early (휴면<20% · pos20≤0.40 · 거래량=feature(gate 제거) · 급등 · 과열 · 종가강도 · 유동성).

    ★ 거래량(volx)은 hard gate 완전 제거 — feature로만 기록(§7 사장님 6/15 결정).
    """
    return (
        SAJANG.WL_SURGE_MIN <= up <= SAJANG.WL_OVERHEAT
        and prior_max < SAJANG.EARLY_DORMANT_MAX
        and pos20 <= SAJANG.EARLY_BASE_POS
        and cs >= SAJANG.WL_CS_MIN
        and tv >= SAJANG.WL_LIQ_FLOOR_억
    )


def _record(asof_date, code, name, variant, up, pos20, prior_max, cs, volx, tv, kki) -> dict:
    return {
        "date": asof_date,
        "code": code,
        "name": name,
        "variant": variant,                       # "strict" | "early"
        "surge_pct": round(up * 100, 1),
        "pos20": round(float(pos20), 3),
        "dormant_max_pct": round(prior_max * 100, 1),
        "close_strength": round(float(cs), 3),
        "volume_ratio": round(float(volx), 2),    # ★ early에선 feature(gate 아님)
        "trading_value_억": round(float(tv), 1),
        "kki_score": kki,
        "kki_grade": SAJANG.kki_grade(kki),
        "representative_codes": [code],           # forward 충전 재사용(sector_reversal 규격)
        "forward_d1": None, "forward_d3": None, "forward_d5": None,
        "mfe": None, "mae": None,
        "would_stop_day": None,                   # -3% 최초 도달일(D+n) — 재진입 룰 필요성 관측
        "holding_days": None,                     # forward 충전된 거래일 수
        "verdict": None,
    }


def _load() -> dict:
    if SHADOW_OUT.exists():
        try:
            return json.loads(SHADOW_OUT.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"records": [], "note": "early variant shadow (관측 전용, 매수 무접촉)"}


def _save(data: dict) -> None:
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SHADOW_OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def build_early_shadow(asof: str | None = None, save: bool = True) -> dict:
    """asof 당일의 strict / early variant 후보를 둘 다 shadow로 기록(누적 merge).

    Args:
        asof: 'YYYY-MM-DD' (None=각 종목 일봉 마지막행=최근거래일).
        save: True면 SHADOW_OUT에 멱등 적재.

    forward 필드는 None으로 시작 — update_early_forward()가 나중에 채운다.
    """
    # 거래일 가드 (asof 지정 시; None은 마지막행=거래일 데이터라 통과)
    if asof:
        try:
            if not is_trading_day(datetime.strptime(asof, "%Y-%m-%d").date()):
                logger.info(f"[EARLY] {asof} 휴장 — 스캔 skip")
                return {"date": asof, "records": [], "note": "non_trading_day"}
        except Exception:
            pass

    try:
        uni = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"[EARLY] universe 로드 실패: {e}")
        return {"date": asof, "records": [], "note": "universe_load_fail"}

    PRE = SAJANG.WL_PRE_DAYS
    strict_recs, early_recs = [], []

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

        # 기준일 i (asof 지정 시 해당일, 없으면 마지막행)
        i = n - 1
        if asof:
            dates = [str(x)[:10] for x in df.index]
            hit = [k for k, d in enumerate(dates) if d == asof]
            if not hit:
                continue
            i = hit[-1]
        if i < PRE + 1 or i < 21:
            continue

        pc = closes[i - 1]
        if pc <= 0:
            continue
        up = closes[i] / pc - 1.0

        # 휴면(직전 PRE일 최대 일간상승)
        prior_max = 0.0
        for j in range(i - PRE, i):
            ppc = closes[j - 1]
            if ppc > 0:
                prior_max = max(prior_max, closes[j] / ppc - 1.0)
        # pos20 (전일 종가의 직전 20일 range 내 위치)
        win_lo = lows[i - 20:i]; win_hi = highs[i - 20:i]
        band = win_hi.max() - win_lo.min()
        pos20 = (pc - win_lo.min()) / band if band > 0 else 0.5
        # 종가강도 · 거래량배수 · 유동성
        h0, l0, c0, v0 = highs[i], lows[i], closes[i], vols[i]
        cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
        vma = vols[i - 20:i].mean()
        volx = v0 / vma if vma > 0 else 0.0
        tv_억 = c0 * v0 / 1e8

        pass_strict = _strict_pass(up, prior_max, pos20, cs, volx, tv_억)
        pass_early = _early_pass(up, prior_max, pos20, cs, volx, tv_억)
        if not (pass_strict or pass_early):
            continue

        # 끼 (단일진실 재사용) — 통과 종목만 계산
        atrp = _atr_pct(highs, lows, closes, i)
        surge, limit = count_surge_limit_days(list(closes[:i + 1]))
        st = LimitUpStock(
            code=code, name=name, close=int(c0), volume_ratio=round(float(volx), 2),
            trading_value_억=round(float(tv_억), 1),
            turnover_pct=round(tv_억 / cap * 100, 2) if cap else 0,
            close_strength=round(float(cs), 3), consecutive_limit=0,
        )
        kki = score_kki(st, atrp, surge, limit)
        asof_date = str(df.index[i])[:10]

        if pass_strict:
            strict_recs.append(_record(asof_date, code, name, "strict",
                                       up, pos20, prior_max, cs, volx, tv_억, kki))
        if pass_early:
            early_recs.append(_record(asof_date, code, name, "early",
                                      up, pos20, prior_max, cs, volx, tv_억, kki))

    # 끼 정렬. early는 후보 폭발 통제(EARLY_TOP_N). strict는 무제한(현행과 동일).
    strict_recs.sort(key=lambda x: x["kki_score"], reverse=True)
    early_recs.sort(key=lambda x: x["kki_score"], reverse=True)
    if SAJANG.EARLY_TOP_N > 0:
        early_recs = early_recs[:SAJANG.EARLY_TOP_N]
    records = strict_recs + early_recs

    if save and records:
        data = _load()
        idx = {(r["date"], r["code"], r["variant"]): k for k, r in enumerate(data["records"])}
        for rec in records:
            key = (rec["date"], rec["code"], rec["variant"])
            if key in idx:
                old = data["records"][idx[key]]
                # 기존 forward/관측 보존 (라벨만 재계산)
                for fld in ("forward_d1", "forward_d3", "forward_d5", "mfe", "mae",
                            "would_stop_day", "holding_days", "verdict"):
                    rec[fld] = old.get(fld)
                data["records"][idx[key]] = rec
            else:
                data["records"].append(rec)
        _save(data)

    s_grades, e_grades = _grade_dist(strict_recs), _grade_dist(early_recs)
    logger.info(f"[early_variant/shadow] strict {len(strict_recs)} {s_grades} / "
                f"early {len(early_recs)} {e_grades} -> {SHADOW_OUT.name}")
    return {"date": asof, "strict": len(strict_recs), "early": len(early_recs),
            "records": records}


def _grade_dist(recs) -> dict:
    g = {}
    for r in recs:
        g[r["kki_grade"]] = g.get(r["kki_grade"], 0) + 1
    return g


def update_early_forward(save: bool = True) -> int:
    """기록된 record의 forward_d1/d3/d5 + mfe/mae + would_stop_day + holding_days 충전.

    virtual_entry = T0(date) 종가. base date 이후 N거래일 일봉이 있어야 채움.
    이미 forward_d5가 채워진 record는 skip(멱등). 반환=새로 채운 record 수.
    sector_reversal_shadow.update_forward 규격 + would_stop_day/holding_days 추가.
    """
    data = _load()
    trail_pct = float(SAJANG.TRAILING_PCT)   # 상수 참조만 (매도 헬퍼 호출 X)
    filled = 0
    for rec in data["records"]:
        if rec.get("forward_d5") is not None:
            continue
        codes = rec.get("representative_codes") or ([rec["code"]] if rec.get("code") else [])
        if not codes:
            continue
        base_date = rec["date"]

        # forward_d1/d3/d5 (단일 종목 = representative_codes[0], 평균은 길이1이라 동일)
        fwd = {}
        ok_any = False
        for off in FWD_OFFSETS:
            rets = []
            for c in codes:
                rows = _daily_rows(c)
                di = next((k for k, r in enumerate(rows) if r[0] == base_date), None)
                if di is None or di + off >= len(rows):
                    continue
                bc = rows[di][1]
                if bc > 0:
                    rets.append((rows[di + off][1] / bc - 1) * 100)
            if rets:
                fwd[off] = round(sum(rets) / len(rets), 2)
                ok_any = True

        # mfe/mae + would_stop_day + holding_days over D+1..D+WINDOW
        mfes, maes, stop_days, hold_lens = [], [], [], []
        for c in codes:
            rows = _daily_rows(c)
            di = next((k for k, r in enumerate(rows) if r[0] == base_date), None)
            if di is None:
                continue
            bc = rows[di][1]
            window = rows[di + 1: di + 1 + MFE_MAE_WINDOW]
            if not window or bc <= 0:
                continue
            mfes.append((max(r[2] for r in window) / bc - 1) * 100)
            maes.append((min(r[3] for r in window) / bc - 1) * 100)
            hold_lens.append(len(window))
            # -3% 손절선(저가 기준) 최초 도달일 (관측: 재진입 룰 필요성)
            stop_line = bc * (1 - trail_pct / 100)
            sd = next((off for off, r in enumerate(window, start=1) if r[3] <= stop_line), None)
            if sd is not None:
                stop_days.append(sd)

        if ok_any:
            rec["forward_d1"] = fwd.get(1)
            rec["forward_d3"] = fwd.get(3)
            rec["forward_d5"] = fwd.get(5)
            if mfes:
                rec["mfe"] = round(max(mfes), 2)
            if maes:
                rec["mae"] = round(min(maes), 2)
            if hold_lens:
                rec["holding_days"] = max(hold_lens)
            # would_stop_day: 단일 종목이라 도달 시 그 날, 미도달이면 None 유지
            rec["would_stop_day"] = min(stop_days) if stop_days else rec.get("would_stop_day")
            filled += 1

    if save and filled:
        _save(data)
    return filled


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (기본=일봉 마지막행)")
    a = ap.parse_args()
    res = build_early_shadow(a.date)
    n = update_early_forward()
    print(f"\n[early variant shadow] strict {res.get('strict', 0)} / early {res.get('early', 0)} "
          f"후보 · forward 충전 {n}건")
    print("★ shadow: strict/early 병렬 관측·forward추적용. 자동매수 X. 관측 없이 flip 금지.")
