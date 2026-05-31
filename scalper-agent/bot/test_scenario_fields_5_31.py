# -*- coding: utf-8 -*-
"""TradeObject 시나리오 필드 / build_scenario 단위테스트 — 4단 Stage3 브릭3 (5/31).
선언적 필드가 SAJANG 경유(리터럴0)로 채워지는지 + 직렬화 round-trip 안전성.
"""
import sys
from dataclasses import asdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.trade_object import TradeObject
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
    base = dict(trade_id="T1", code="000000", name="테스트", total_score=50.0, sources=[],
                stop_loss=9700, stop_source="ATR", expected_hold_days=3)
    base.update(kw)
    return TradeObject(**base)


# 1. 기본값(하위호환): build_scenario 전엔 0/빈값
to = mk()
chk(to.tp1_trigger_pct == 0.0 and to.kki_grade == "" and to.hold_plan == "", "신규필드 기본값(하위호환)")

# 2. build_scenario → SAJANG 경유 채움 (리터럴 아님)
to.build_scenario()
chk(to.tp1_trigger_pct == SAJANG.RULE_B_THRESHOLD, f"tp1=RULE_B_THRESHOLD ({to.tp1_trigger_pct})")
chk(to.trail_arm_pct == SAJANG.TRAILING_ACTIVATION_PCT, f"trail_arm=TRAILING_ACTIVATION ({to.trail_arm_pct})")
chk(to.invalidation_price == to.stop_loss, "invalidation_price = stop_loss")
chk(to.tp1_action == "PARTIAL_SELL_50", "tp1_action 줄먹 절반")
chk(to.hold_plan == "D+0~D+3", f"hold_plan D+0~D+3 ({to.hold_plan})")
chk(to.invalidation_kind == "STRUCTURE_BREAK", f"ATR→STRUCTURE_BREAK ({to.invalidation_kind})")

# 3. INST_COST → SUPPLY_EXIT
to2 = mk(stop_source="INST_COST").build_scenario()
chk(to2.invalidation_kind == "SUPPLY_EXIT", f"INST_COST→SUPPLY_EXIT ({to2.invalidation_kind})")

# 4. 직렬화 round-trip (신규필드 포함, 구버전 dict 복원)
d = asdict(to)
chk("kki_grade" in d and "hold_plan" in d and d["tp1_trigger_pct"] == SAJANG.RULE_B_THRESHOLD,
    "asdict 신규필드 직렬화")
restored = TradeObject(**d)
chk(restored.hold_plan == "D+0~D+3" and restored.invalidation_price == 9700, "dict→객체 복원")
# 구버전 JSON(신규필드 없음) 복원 = 기본값
old = {"trade_id": "X", "code": "000001", "name": "구", "total_score": 10.0, "sources": []}
chk(TradeObject(**old).kki_grade == "", "구버전 dict 복원 → 기본값")

print(f"\n결과: {P}/{P+F} PASS, {F} FAIL")
sys.exit(0 if F == 0 else 1)
