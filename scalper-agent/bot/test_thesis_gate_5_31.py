# -*- coding: utf-8 -*-
"""_aggregate_thesis_buckets 단위테스트 — 4단 Stage0 브릭4 (5/31 shadow).
{재료·수급·기술위치} 강≥2 → 명분통과. 재무는 modifier. 정규화 클램프.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.morning_recommendation import _aggregate_thesis_buckets
from data.sajang_rules import SAJANG

P = F = 0


def chk(cond, name):
    global P, F
    if cond:
        P += 1
    else:
        F += 1
        print(f"  FAIL: {name}")


# 1. 재료+수급 강 2개 → 통과
b, s, p = _aggregate_thesis_buckets({"relay_sc": 45, "nat_sc": 50})
chk(p is True and len(s) == 2, f"재료+수급 강2 → 통과 (strong={s})")

# 2. 재료만 강 1개 → 미통과 (MIN_STRONG=2)
b, s, p = _aggregate_thesis_buckets({"relay_sc": 45})
chk(p is False and s == ["재료"], f"강1 → 미통과 ({s})")

# 3. 약신호 더미(전부 약) → 미통과
b, s, p = _aggregate_thesis_buckets({"relay_sc": 10, "nat_sc": 10, "tech_sc": 5})
chk(p is False and len(s) == 0, f"약신호더미 → 미통과 ({b})")

# 4. 기술위치+수급 강 → 통과
b, s, p = _aggregate_thesis_buckets({"tech_sc": 25, "nat_sc": 50})
chk(p is True and set(s) == {"기술위치", "수급"}, f"기술+수급 → 통과 ({s})")

# 5. 재무만 강(pension) → gate 미반영 → 미통과
b, s, p = _aggregate_thesis_buckets({"pension_sc": 15})
chk(p is False and b["재무"] >= SAJANG.THESIS_GATE_STRONG_NORM and "재무" not in s,
    f"재무 강이어도 gate 미반영 ({b['재무']})")

# 6. 정규화 클램프 (cap 초과 → 1.0)
b, s, p = _aggregate_thesis_buckets({"relay_sc": 999})
chk(b["재료"] == 1.0, f"클램프 1.0 ({b['재료']})")

# 7. 빈 입력 → 전부 0 → 미통과 (예외 아님)
b, s, p = _aggregate_thesis_buckets({})
chk(p is False and b["재료"] == 0.0, "빈입력 → 미통과(크래시X)")

print(f"\n결과: {P}/{P+F} PASS, {F} FAIL")
sys.exit(0 if F == 0 else 1)
