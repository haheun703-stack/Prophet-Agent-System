# -*- coding: utf-8 -*-
"""점화 신호 엣지 측정 — 생존편향 보정 (사장님 5/31 '무기가 있냐').

ignition_footprint(승자 9종목 묘사=필요조건)의 다음 단계.
질문: "기지에서 거래량 터지며 +10%↑ 돌파" 신호가 **전체 유니버스 10년**에서
       꽝 대비 승자를 실제로 더 잘 맞히나(엣지)? = 무기의 진위.

방법(look-ahead 없음):
  - 전 종목(stock_data_daily) 전 거래일 스캔.
  - 각 거래일: 과거 20일로만 점화 판정(gain≥10% & volx≥2.5 & pos20≤60% & 거래대금≥30억).
  - 미래 H=20거래일 forward 수익(fwd_max=고점기준, fwd_close=종가기준) — 미래 데이터로만.
  - 베이스라인(control) = 같은 유동성 게이트의 '모든 거래일' 표본 → 무조건부 +20% 적중률.
  - 엣지 = 점화일 P(+20%) / 베이스라인 P(+20%). >1이면 신호에 엣지.
  - 끼 등급별 분해 → 끼가 엣지를 올리나.
★ 정직 한계: 일봉 / 상장폐지 종목 부재(유니버스 생존편향 잔존) / 단일 H=20 / 표본기간=데이터범위.
"""
from __future__ import annotations
import csv, sys, random
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
sys.path.insert(0, str(ROOT))
from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
H = 20                 # forward horizon (거래일)
IGNITE_GAIN = 0.10
IGNITE_VOLX = 2.5
BASE_POS = 0.6
LIQ_FLOOR = 30.0       # 점화일 거래대금 하한(억) — 유동성/노이즈 게이트
WINS = (0.20, 0.30, 0.50)
COOLDOWN = H           # 같은 종목 연속 점화 중복 방지
BASE_SAMPLE_P = 0.04   # 베이스라인 표본 추출 확률
SEED = 20260531


def _load(p):
    ds, o, h, l, c, v = [], [], [], [], [], []
    try:
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    c.append(float(r["close"])); o.append(float(r["open"]))
                    h.append(float(r["high"])); l.append(float(r["low"]))
                    v.append(float(r["volume"])); ds.append(r.get("날짜") or r.get("") or "")
                except (ValueError, KeyError):
                    continue
    except Exception:
        return None
    if len(c) < 21 + H + 1:
        return None
    return (np.array(o), np.array(h), np.array(l), np.array(c), np.array(v))


def main():
    rng = random.Random(SEED)
    fires = []          # (fwd_max, fwd_close, code, name, i_global_close, kki)
    base_fwdmax = []    # 베이스라인 fwd_max 표본
    n_elig = 0
    files = sorted(DAILY.glob("*.csv"))
    print(f"스캔 종목: {len(files)} | H={H}일 | 점화=gain≥{IGNITE_GAIN:.0%}&vol≥{IGNITE_VOLX}x&기지≤{BASE_POS:.0%}&거래대금≥{LIQ_FLOOR:.0f}억\n")

    for p in files:
        stem = p.stem
        name, _, code = stem.rpartition("_")
        d = _load(p)
        if d is None:
            continue
        o, hi, lo, c, v = d
        n = len(c)
        last_fire = -COOLDOWN - 1
        for i in range(21, n - H):
            pc = c[i - 1]
            if pc <= 0:
                continue
            vavg = v[i - 20:i].mean()
            if vavg <= 0:
                continue
            tv = c[i] * v[i] / 1e8
            if tv < LIQ_FLOOR:
                continue
            n_elig += 1
            fwd_max = c[i + 1:i + 1 + H].max() / c[i] - 1.0
            fwd_close = c[i + H] / c[i] - 1.0
            if rng.random() < BASE_SAMPLE_P:
                base_fwdmax.append(fwd_max)
            gain = c[i] / pc - 1.0
            volx = v[i] / vavg
            lows = lo[i - 20:i]; highs = hi[i - 20:i]
            rngband = highs.max() - lows.min()
            pos20 = (pc - lows.min()) / rngband if rngband > 0 else 0.5
            if gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS and i >= last_fire + COOLDOWN:
                # 끼 (점화 시점까지 데이터로만)
                seg_h = hi[max(0, i - 14):i + 1]; seg_l = lo[max(0, i - 14):i + 1]; seg_c = c[max(0, i - 14):i + 1]
                trs = [max(seg_h[k] - seg_l[k], abs(seg_h[k] - seg_c[k - 1]), abs(seg_l[k] - seg_c[k - 1]))
                       for k in range(1, len(seg_c))]
                atr = (sum(trs) / len(trs)) / c[i] * 100 if trs and c[i] > 0 else 0
                surge, limit = count_surge_limit_days(list(c[:i + 1]))
                st = LimitUpStock(code=code, name=name, close=int(c[i]), volume_ratio=round(float(volx), 2),
                                  trading_value_억=round(float(tv), 1),
                                  close_strength=round(float((c[i] - lo[i]) / (hi[i] - lo[i])), 3) if hi[i] > lo[i] else 0.5,
                                  consecutive_limit=0)
                kki = score_kki(st, atr, surge, limit)
                fires.append((float(fwd_max), float(fwd_close), code, name, float(kki)))
                last_fire = i

    if not fires or not base_fwdmax:
        print("데이터 부족 — 점화/베이스라인 표본 없음"); return 1

    def rate(arr, w):
        a = np.array(arr); return (a >= w).mean() * 100

    fm = np.array([f[0] for f in fires]); fc = np.array([f[1] for f in fires])
    bm = np.array(base_fwdmax)
    print(f"── 베이스라인 (유동성 게이트 모든 거래일 표본 {len(bm):,}건, 무조건부) ──")
    for w in WINS:
        print(f"  P(fwd_max≥{w:+.0%}) = {rate(bm, w):.1f}%")
    print(f"  avg fwd_max = {bm.mean()*100:+.1f}% | median = {np.median(bm)*100:+.1f}%\n")

    print(f"── 점화 신호 (fire {len(fm):,}건 / 유동 거래일 {n_elig:,}건 중 {len(fm)/n_elig*100:.2f}%) ──")
    for w in WINS:
        fr, br = rate(fm, w), rate(bm, w)
        edge = fr / br if br > 0 else float("inf")
        print(f"  P(fwd_max≥{w:+.0%}) = {fr:.1f}%  (베이스 {br:.1f}% → 엣지 x{edge:.2f})")
    print(f"  avg fwd_max = {fm.mean()*100:+.1f}% | median = {np.median(fm)*100:+.1f}%")
    print(f"  avg fwd_close(H={H}) = {fc.mean()*100:+.1f}% | median = {np.median(fc)*100:+.1f}%\n")

    # 끼 등급별 엣지
    print("── 끼 등급별 점화 엣지 (끼가 승자 적중을 올리나) ──")
    grades = {}
    for fmx, _fc, _cd, _nm, kki in fires:
        g = SAJANG.kki_grade(kki); grades.setdefault(g, []).append(fmx)
    base20 = rate(bm, 0.20)
    for g in ("EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH"):
        arr = grades.get(g, [])
        if not arr:
            continue
        r20 = rate(arr, 0.20)
        print(f"  {g:<10} {len(arr):>5}건 | P(+20%)={r20:.1f}% (엣지 x{r20/base20:.2f}) | avg={np.mean(arr)*100:+.1f}%")

    print("\n★ 해석: 점화 P(+20%)가 베이스보다 충분히 높으면(엣지>1.3~) 신호에 값어치. "
          "끼 등급이 높을수록 엣지↑면 '점화+끼'가 무기 코어.")
    print("★ 한계: 일봉/상폐종목부재(유니버스 생존편향)/단일 H/표본기간 데이터범위. 정밀화는 분봉·상폐포함·다중H.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
