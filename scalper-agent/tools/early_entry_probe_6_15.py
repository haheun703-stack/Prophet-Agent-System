# -*- coding: utf-8 -*-
"""초입 포착 진단 (6/15, read-only) — 원익IPS(240810) 등에 watchlist_continuation 필터 수동 적용.

사장님 발견 검증: "원익 6/4 +37% 초입 vs 6/12 추격 -4.8%, 변별자=pos20(직전 위치)".
현재 휴면→첫급등 필터(휴면<10% · pos20<=0.6 · 급등>=15% · 과열<=30% · 종가강도>=0.8 ·
거래량>=5x · 거래대금>=30억)가 원익 6/4를 잡나/놓치나 + 어느 조건 완화가 필요한지 데이터로.

★ 순수 read-only: 일봉 CSV 읽기 + 조건 계산 + 출력만. 매수/picks/SAJANG매도/order 무접촉.
사용: python tools/early_entry_probe_6_15.py [--code 240810] [--dates 2026-06-04,2026-06-12]
"""
from __future__ import annotations
import sys, os, json, argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.watchlist_continuation import _col, _atr_pct  # noqa: E402
from data.limit_up_scanner import (  # noqa: E402
    _load_daily, score_kki, count_surge_limit_days, LimitUpStock, UNIVERSE_PATH,
)
from data.sajang_rules import SAJANG  # noqa: E402

PRE = SAJANG.WL_PRE_DAYS


def _mark(ok: bool, fail_note: str = "") -> str:
    return "PASS" if ok else ("FAIL" + (f"({fail_note})" if fail_note else ""))


def probe(code: str, target: str, uni: dict) -> None:
    df = _load_daily(code)
    if df is None or len(df) < PRE + 2:
        print(f"[{code}] 일봉 부족/없음"); return
    cc = _col(df, "종가", "close"); ch = _col(df, "고가", "high")
    cl = _col(df, "저가", "low"); cv = _col(df, "거래량", "volume")
    closes = df[cc].values.astype(float); highs = df[ch].values.astype(float)
    lows = df[cl].values.astype(float); vols = df[cv].values.astype(float)
    try:
        co = _col(df, "시가", "open"); opens = df[co].values.astype(float)
    except Exception:
        opens = closes
    dates = [str(x)[:10] for x in df.index]
    name = uni.get(code, {}).get("name", code)
    cap = uni.get(code, {}).get("cap_억", 0)

    if target not in dates:
        print(f"\n=== {name}({code}) {target} — 해당일 데이터 없음 (마지막={dates[-1]}) ==="); return
    i = dates.index(target)
    if i < PRE + 1 or i < 21:
        print(f"\n=== {name}({code}) {target} — 직전 데이터 부족(i={i}) ==="); return

    pc = closes[i - 1]
    up = closes[i] / pc - 1.0 if pc > 0 else 0.0
    up_high = highs[i] / pc - 1.0 if pc > 0 else 0.0
    # dormant (직전 PRE일 최대 일간상승)
    prior_max = 0.0
    for j in range(i - PRE, i):
        ppc = closes[j - 1]
        if ppc > 0:
            prior_max = max(prior_max, closes[j] / ppc - 1.0)
    # pos20 (전일 종가의 직전 20일 범위 내 위치)
    win_lo = lows[i - 20:i]; win_hi = highs[i - 20:i]
    band = win_hi.max() - win_lo.min()
    pos20 = (pc - win_lo.min()) / band if band > 0 else 0.5
    # 연속자
    h0, l0, c0, v0 = highs[i], lows[i], closes[i], vols[i]
    cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
    vma = vols[i - 20:i].mean()
    volx = v0 / vma if vma > 0 else 0.0
    tv = c0 * v0 / 1e8
    atrp = _atr_pct(highs, lows, closes, i)
    surge, limit = count_surge_limit_days(list(closes[:i + 1]))
    st = LimitUpStock(code=code, name=name, close=int(c0), volume_ratio=round(float(volx), 2),
                      trading_value_억=round(float(tv), 1),
                      turnover_pct=round(tv / cap * 100, 2) if cap else 0,
                      close_strength=round(float(cs), 3), consecutive_limit=0)
    kki = score_kki(st, atrp, surge, limit)

    # forward (해당일 종가 진입 가정)
    fwd = []
    for n in (1, 3, 5):
        if i + n < len(closes):
            fwd.append((n, (closes[i + n] / c0 - 1) * 100))
    win = range(i + 1, min(i + 6, len(closes)))
    mfe = max((highs[k] / c0 - 1) * 100 for k in win) if i + 1 < len(closes) else None
    mae = min((lows[k] / c0 - 1) * 100 for k in win) if i + 1 < len(closes) else None

    print(f"\n=== {name}({code}) {target} (i={i}) ===")
    print(f"  전일종가 {pc:,.0f}  시가 {opens[i]:,.0f}  고가 {h0:,.0f}  저가 {l0:,.0f}  종가 {c0:,.0f}")
    print(f"  종가상승 up={up*100:+.1f}%   고가상승 {up_high*100:+.1f}%   끼 {kki:.0f}={SAJANG.kki_grade(kki)}")
    print(f"  [급등 >=15%]    {up*100:+5.1f}%   -> {_mark(up >= SAJANG.WL_SURGE_MIN)}")
    print(f"  [과열 <=30%]    {up*100:+5.1f}%   -> {_mark(up <= SAJANG.WL_OVERHEAT, '과열탈락')}")
    print(f"  [휴면 <10%]     직전20최대 {prior_max*100:+5.1f}%   -> {_mark(prior_max < SAJANG.WL_DORMANT_MAX, '휴면아님')}")
    print(f"  [pos20 <=0.60]  {pos20:.2f}   -> {_mark(pos20 <= SAJANG.WL_BASE_POS, '고점')}")
    print(f"  [종가강도>=0.80] {cs:.2f}   -> {_mark(cs >= SAJANG.WL_CS_MIN)}")
    print(f"  [거래량 >=5x]   {volx:.1f}x   -> {_mark(volx >= SAJANG.WL_VOL_MIN)}")
    print(f"  [거래대금>=30억] {tv:,.0f}억   -> {_mark(tv >= SAJANG.WL_LIQ_FLOOR_억)}")
    if fwd:
        s = "  ".join(f"d{n}={r:+.1f}%" for n, r in fwd)
        extra = (f"   MFE5={mfe:+.1f}%  MAE5={mae:+.1f}%" if mfe is not None else "")
        print(f"  forward(종가진입): {s}{extra}")

    # 완화 스윕 — 휴면/과열 임계를 올리면 통과하는가
    sweeps = []
    for dm in (0.10, 0.15, 0.20, 0.30):
        for oh in (0.30, 0.40, 0.50, 0.99):
            ok = (up >= SAJANG.WL_SURGE_MIN and up <= oh and prior_max < dm
                  and pos20 <= SAJANG.WL_BASE_POS and cs >= SAJANG.WL_CS_MIN
                  and volx >= SAJANG.WL_VOL_MIN and tv >= SAJANG.WL_LIQ_FLOOR_억)
            if ok:
                sweeps.append((dm, oh))
    if sweeps:
        looser = min(sweeps, key=lambda t: (t[0], t[1]))
        print(f"  ※ 완화 통과 최소조건: 휴면<{looser[0]*100:.0f}% & 과열<={looser[1]*100:.0f}%"
              + ("  (현행 통과)" if looser == (0.10, 0.30) else "  (현행은 탈락 → 완화 필요)"))
    else:
        print("  ※ 휴면/과열 완화로도 미통과 (다른 조건이 탈락 — pos20/종가강도/거래량/유동성 확인)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", default="240810")
    ap.add_argument("--dates", default="2026-06-04,2026-06-12")
    a = ap.parse_args()
    uni = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    print(f"현행 SAJANG 임계: 휴면<{SAJANG.WL_DORMANT_MAX*100:.0f}% · pos20<={SAJANG.WL_BASE_POS} · "
          f"급등>={SAJANG.WL_SURGE_MIN*100:.0f}% · 과열<={SAJANG.WL_OVERHEAT*100:.0f}% · "
          f"종가강도>={SAJANG.WL_CS_MIN} · 거래량>={SAJANG.WL_VOL_MIN}x · 거래대금>={SAJANG.WL_LIQ_FLOOR_억}억")
    for d in [x.strip() for x in a.dates.split(",") if x.strip()]:
        probe(a.code, d, uni)


if __name__ == "__main__":
    main()
