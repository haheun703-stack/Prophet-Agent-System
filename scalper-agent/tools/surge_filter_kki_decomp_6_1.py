# -*- coding: utf-8 -*-
"""워치리스트 끼 하한 — 유동성 효과 vs 변동성 기질 효과 분해 (6/1, 코워크 통계함정 캐치).

코워크 발견: 크라우드웍스 끼=0 = 30~50억 데드존 인공물(KKI_MIN_LIQUIDITY=50 > WL_LIQ_FLOOR=30).
  → "끼 하한 추가"를 그냥 돌리면 사실상 '유동성 문턱 30→50'을 변장 = 혼동.
이 도구: STOP_REENTER 수익을 다음 버킷으로 분해해 두 효과를 분리.
  A  : LIQ≥30, 끼 무게이트 (현재 검증된 필터, db5cd76 +1.89p)
  B  : LIQ≥50, 끼 무게이트 (유동성 문턱 30→50 효과만)
  C  : LIQ≥50 AND 끼≥MODERATE(30) (50억 위에서 진짜 변동성 기질 효과)
  D  : 30≤LIQ<50 데드존 (이 구간 종목이 forward 약한가?)
판정: B>A면 데드존 약함→유동성 50. C>B면 기질 게이트가 추가 리프트(50 위에서 끼 진짜).
공통 필터(검증값): 휴면→첫급등(≥15%·기지) + 종가강도≥0.8 + 거래량≥5x. STOP_REENTER(고점-3%·재진입·비용0.48%/leg·보유5).
★ 한계: 일봉/생존편향(절대 과대, 상대만)/초강세장 베타/cap 없는 종목 turnover=0(끼 약간 보수적).
"""
from __future__ import annotations
import csv, glob, json, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
sys.path.insert(0, str(ROOT))
from data.limit_up_scanner import score_kki, count_surge_limit_days, LimitUpStock
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
SURGE = 0.15; DORMANT_MAX = 0.10; BASE_POS = 0.60
CS_MIN = 0.80; VOL_MIN = 5.0; OVERHEAT = 0.30
TRAIL = 0.03; COST_LEG = 0.0048; HOLD_K = 5; PRE = 20
KKI_MOD = SAJANG.KKI_MODERATE  # 30

# cap 로드(turnover용)
CAP = {}
try:
    u = json.loads((ROOT / "data_store" / "universe.json").read_text(encoding="utf-8"))
    for c, info in u.items():
        if isinstance(info, dict):
            CAP[c] = float(info.get("cap_억", 0) or 0)
except Exception:
    pass


def _load(p):
    rows = []
    try:
        with open(p, encoding="utf-8") as f:
            rd = csv.reader(f); next(rd, None)
            for x in rd:
                try:
                    rows.append((float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])))
                except (ValueError, IndexError):
                    pass
    except Exception:
        return []
    return rows


def _sim_reenter(rows, e):
    entry = rows[e][3]
    end = min(e + HOLD_K, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None
    legs = 0.0; in_pos = True; p_in = entry; hw = entry; last_stop = entry * (1 - TRAIL)
    for d in days:
        o, h, l, c, v = rows[d]
        if in_pos:
            hw = max(hw, h); stop = hw * (1 - TRAIL); last_stop = stop
            if l <= stop:
                legs += (stop / p_in - 1) - COST_LEG; in_pos = False
        else:
            if c >= last_stop:
                in_pos = True; p_in = c; hw = c
    if in_pos:
        legs += (rows[end][3] / p_in - 1) - COST_LEG
    return legs * 100


def main():
    buckets = {"A_liq30_no끼": [], "B_liq50_no끼": [], "C_liq50_끼MOD+": [], "D_deadzone30~50": []}
    for p in glob.glob(str(DAILY / "*.csv")):
        code = Path(p).stem.rsplit("_", 1)[-1]
        rows = _load(p)
        if len(rows) < PRE + HOLD_K + 2:
            continue
        n = len(rows)
        for i in range(PRE, n - HOLD_K):
            pc = rows[i - 1][3]
            if pc <= 0:
                continue
            up = rows[i][3] / pc - 1
            if up < SURGE or up > OVERHEAT:
                continue
            prior_max = max([(rows[j][3] / rows[j - 1][3] - 1) for j in range(i - PRE, i) if rows[j - 1][3] > 0] or [0])
            if prior_max >= DORMANT_MAX:
                continue
            lows = [rows[j][2] for j in range(i - 20, i)]; highs = [rows[j][1] for j in range(i - 20, i)]
            band = max(highs) - min(lows); pos20 = (pc - min(lows)) / band if band > 0 else 0.5
            if pos20 > BASE_POS:
                continue
            h, l, c, v = rows[i][1], rows[i][2], rows[i][3], rows[i][4]
            cs = (c - l) / (h - l) if h > l else 0.5
            if cs < CS_MIN:
                continue
            vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
            volx = v / vma if vma > 0 else 0
            if volx < VOL_MIN:
                continue
            tv = c * v / 1e8
            if tv < 30:
                continue   # 워치 최소 하한
            r = _sim_reenter(rows, i)
            if r is None or not np.isfinite(r):
                continue
            # 끼 계산
            seg = rows[max(0, i - 14):i + 1]
            trs = [max(seg[k][1] - seg[k][2], abs(seg[k][1] - seg[k - 1][3]), abs(seg[k][2] - seg[k - 1][3])) for k in range(1, len(seg))]
            atrp = (sum(trs) / len(trs)) / c * 100 if trs else 0
            surge, limit = count_surge_limit_days([x[3] for x in rows[:i + 1]])
            cap = CAP.get(code, 0)
            st = LimitUpStock(code=code, name=code, close=int(c), volume_ratio=round(volx, 2),
                              trading_value_억=round(tv, 1), turnover_pct=round(tv / cap * 100, 2) if cap else 0,
                              close_strength=round(cs, 3), consecutive_limit=0)
            kki = score_kki(st, atrp, surge, limit)
            # 버킷 분류
            buckets["A_liq30_no끼"].append(r)
            if 30 <= tv < 50:
                buckets["D_deadzone30~50"].append(r)
            if tv >= 50:
                buckets["B_liq50_no끼"].append(r)
                if kki >= KKI_MOD:
                    buckets["C_liq50_끼MOD+"].append(r)

    print(f"워치리스트 끼하한 분해 — STOP_REENTER(보유{HOLD_K}·고점-3%·비용{COST_LEG*100:.2f}%/leg)")
    print(f"공통: 휴면→첫급등≥{SURGE:.0%}·기지·종가강도≥{CS_MIN}·거래량≥{VOL_MIN}x | KKI_MIN_LIQUIDITY={SAJANG.KKI_MIN_LIQUIDITY_억}·WL_LIQ={SAJANG.WL_LIQ_FLOOR_억}\n")
    print(f'{"버킷":<18}{"n":>7}{"avg":>9}{"median":>9}{"승률":>7}{"≥+10%":>7}')
    for k in ("A_liq30_no끼", "D_deadzone30~50", "B_liq50_no끼", "C_liq50_끼MOD+"):
        a = np.array(buckets[k])
        if len(a) == 0:
            print(f"{k:<18}{0:>7}"); continue
        print(f'{k:<18}{len(a):>7,}{a.mean():>+8.2f}%{np.median(a):>+8.2f}%{(a>0).mean()*100:>6.0f}%{(a>=10).mean()*100:>6.0f}%')
    print("\n★ 판정:")
    print("  ① 데드존(D 30~50) vs 전체(A): D가 확 낮으면 → 그 구간 약함 = 유동성 하한 50으로 올려라(끼게이트 아님)")
    print("  ② B(liq50,끼무) vs C(liq50,끼MOD+): C가 B보다 확실히 높으면 → 50 위에서 끼 기질 게이트가 진짜 추가리프트")
    print("  ③ B vs A: 유동성 30→50 자체 효과(데드존 제거)")
    print("★ 한계: 일봉/생존편향(상대만)/초강세장 베타/cap없는 종목 turnover=0. 결론은 forward shadow와 일치 시에만 코드반영.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
