# -*- coding: utf-8 -*-
"""score_kki / SAJANG.kki_grade 단위테스트 — 4단 Stage1 브릭1 (5/31).
shadow 단계 순수함수 검증: 유동성가드·6팩터·F2캡·등급경계·NaN가드.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
from data.sajang_rules import SAJANG

P = F = 0


def chk(cond, name):
    global P, F
    if cond:
        P += 1
    else:
        F += 1
        print(f"  FAIL: {name}")


def mk(**kw):
    base = dict(code="000000", name="T", trading_value_억=100.0, turnover_pct=0.0,
                consecutive_limit=0, close_strength=0.0, volume_ratio=0.0)
    base.update(kw)
    return LimitUpStock(**base)


# 1. 유동성 가드: 거래대금<50억 → 끼 0
chk(score_kki(mk(trading_value_억=10), atr_pct=7, surge_days=5, limit_days=2) == 0.0,
    "유동성<50억 → kki 0")

# 2. EXPLOSIVE: 고끼 풀 → ≥70
explosive = score_kki(mk(turnover_pct=25, consecutive_limit=3, close_strength=0.96,
                         volume_ratio=12), atr_pct=7, surge_days=5, limit_days=2)
chk(explosive >= 70, f"고끼 ≥70 (={explosive})")
chk(SAJANG.kki_grade(explosive) == "EXPLOSIVE", "고끼 → EXPLOSIVE 등급")

# 3. SLUGGISH: 저끼 → <30
sluggish = score_kki(mk(turnover_pct=1, close_strength=0.5, volume_ratio=1),
                     atr_pct=1, surge_days=0, limit_days=0)
chk(sluggish < 30, f"저끼 <30 (={sluggish})")
chk(SAJANG.kki_grade(sluggish) == "SLUGGISH", "저끼 → SLUGGISH 등급")

# 4. F2 급등빈도 캡 = 25 (surge 20일×2=40 → 25로 캡)
#    F1(atr1=3) + F2(25) + 나머지0 = 28
capped = score_kki(mk(), atr_pct=1, surge_days=20, limit_days=0)
chk(abs(capped - 28.0) < 0.01, f"F2 캡 25 (총 28 기대, ={capped})")

# 5. 등급 경계 (SAJANG 단일진실)
chk(SAJANG.kki_grade(70) == "EXPLOSIVE" and SAJANG.kki_grade(69.9) == "HUNTABLE", "70 경계")
chk(SAJANG.kki_grade(50) == "HUNTABLE" and SAJANG.kki_grade(49.9) == "MODERATE", "50 경계")
chk(SAJANG.kki_grade(30) == "MODERATE" and SAJANG.kki_grade(29.9) == "SLUGGISH", "30 경계")

# 6. NaN atr 가드 (크래시 없이 F1 최저 3점 처리)
nan_ok = score_kki(mk(), atr_pct=float("nan"), surge_days=0, limit_days=0)
chk(nan_ok == 3.0, f"NaN atr → F1 3점 (={nan_ok})")

# 7. count_surge_limit_days
closes = [1000, 1160, 1160, 900, 1170]  # +16%, 0%, -22%, +30%
surge, limit = count_surge_limit_days(closes, lookback=60, surge_pct=15, limit_pct=29)
chk(surge == 1 and limit == 1, f"급등1/상한가1 (={surge}/{limit})")

print(f"\n결과: {P}/{P+F} PASS, {F} FAIL")
sys.exit(0 if F == 0 else 1)
