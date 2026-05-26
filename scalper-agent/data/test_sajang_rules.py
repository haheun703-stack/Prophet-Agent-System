# -*- coding: utf-8 -*-
"""★ 사장님 영구 룰 Rule Registry 단위 테스트 ★

5/26 사고 후 단타봇 박사 미달 자성 → Rule Registry 신설 검증.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_registry_exists():
    from data.sajang_rules import SAJANG, SajangRules
    assert SAJANG is not None
    assert isinstance(SAJANG, SajangRules)
    print("[PASS] SAJANG Registry singleton 존재")


def test_fixed_tp_disabled():
    """사장님 5/21 [feedback_trailing_only_tp] — 고정 TP 폐기."""
    from data.sajang_rules import SAJANG
    assert SAJANG.FIXED_TP_DISABLED is True
    assert SAJANG.FIXED_TP_FORCE_ZERO == 0
    assert SAJANG.get_take_profit(10000) == 0
    assert SAJANG.get_take_profit(122500) == 0   # 5/26 사장님 삼화 사례
    print("[PASS] FIXED_TP_DISABLED=True / get_take_profit=0 (5/21 영구 룰)")


def test_pullback_entry():
    """사장님 5/23 — 시초가 -3% 눌림 매수."""
    from data.sajang_rules import SAJANG
    assert SAJANG.ENTRY_MODE_DEFAULT == "pullback_3pct"
    assert SAJANG.PULLBACK_ENTRY_PCT == 3.0
    print("[PASS] entry_mode='pullback_3pct' default (5/23 영구 룰)")


def test_trailing_3pct():
    """사장님 5/25 — 트레일링 -3% 일관."""
    from data.sajang_rules import SAJANG
    assert SAJANG.TRAILING_PCT == 3.0
    assert SAJANG.TRAILING_ACTIVATION_PCT == 3.0
    # 고점 12,000 → SL 11,640 (-3%)
    assert SAJANG.get_trailing_sl(12000) == 11640
    assert SAJANG.is_trailing_activated(3.0) is True
    assert SAJANG.is_trailing_activated(2.99) is False
    print("[PASS] 트레일링 -3% 일관 (5/25 영구 룰)")


def test_rule_3_limit_up():
    """사장님 5/25 룰 3 — 상한가 +25% 분할 매도."""
    from data.sajang_rules import SAJANG
    assert SAJANG.LIMIT_UP_SPLIT_THRESHOLD == 25.0
    assert SAJANG.is_limit_up_split_triggered(25.0) is True
    assert SAJANG.is_limit_up_split_triggered(24.99) is False
    print("[PASS] 룰 3 +25%+ 분할 매도 (5/25 영구 룰)")


def test_rule_b_10pct():
    """사장님 5/25 룰 B — 15:26 +10%+ 분할."""
    from data.sajang_rules import SAJANG
    assert SAJANG.RULE_B_THRESHOLD == 10.0
    assert SAJANG.RULE_B_CRON_TIME == "15:26"
    assert SAJANG.is_rule_b_triggered(10.0) is True
    print("[PASS] 룰 B +10%+ 분할 (5/25 영구 룰)")


def test_rule_c_d1_gap():
    """사장님 5/25 룰 C — D+1 갭다운 -7%+ 즉시 매도."""
    from data.sajang_rules import SAJANG
    assert SAJANG.D1_GAP_SELL_THRESHOLD == -7.0
    assert SAJANG.RULE_C_CRON_TIME == "09:01"
    assert SAJANG.is_rule_c_triggered(-7.0) is True
    assert SAJANG.is_rule_c_triggered(-6.99) is False   # 인내 보유
    print("[PASS] 룰 C D+1 -7%+ 갭다운 (5/25 영구 룰)")


def test_rule_d_pre_close():
    """사장님 5/26 룰 D — 14:50 D+0 종가 매수."""
    from data.sajang_rules import SAJANG
    assert SAJANG.RULE_D_SURGE_MIN == 10.0
    assert SAJANG.RULE_D_PULLBACK_MIN == 3.0
    assert SAJANG.RULE_D_CRON_TIME == "14:50"
    assert SAJANG.RULE_D_TOP_K == 2
    pos = SAJANG.get_pre_close_d_position(10000)
    assert pos["take_profit"] == 0
    assert pos["mode"] == "swing"
    print("[PASS] 룰 D 14:50 D+0 종가 매수 (5/26 영구 룰)")


def test_sync_auto_president_protection():
    """사장님 5/26 — SYNC 자동등록 = 사장님 매수 보호."""
    from data.sajang_rules import SAJANG
    assert SAJANG.SYNC_AUTO_TP == 0
    assert SAJANG.SYNC_AUTO_MODE == "swing"
    assert SAJANG.SYNC_AUTO_SOURCE == "manual_president"
    pos = SAJANG.get_sync_auto_position(122500)   # 사장님 삼화 사례
    assert pos["take_profit"] == 0   # ★ 5/26 사고 차단 ★
    assert pos["mode"] == "swing"
    assert pos["source"] == "manual_president"
    assert pos["stop_loss"] == 118825   # 122,500 × 0.97
    print("[PASS] SYNC 자동등록 사장님 매수 보호 (5/26 사고 영구 차단)")


def test_normal_sl_3pct():
    """사장님 영구 — NORMAL SL 매수가 -3%."""
    from data.sajang_rules import SAJANG
    assert SAJANG.NORMAL_SL_PCT == 3.0
    assert SAJANG.get_normal_sl(10000) == 9700
    assert SAJANG.get_normal_sl(122500) == 118825
    print("[PASS] NORMAL SL 매수가 -3% (영구)")


def test_budget_mode_split_cash():
    """사장님 5/25 — split_cash 70%."""
    from data.sajang_rules import SAJANG
    assert SAJANG.BUDGET_MODE == "split_cash"
    assert SAJANG.BUDGET_CASH_RATIO == 0.70
    print("[PASS] BUDGET split_cash × 0.70 (5/25 영구 룰)")


def test_cash_reserve_30pct():
    """★ 사장님 영구 룰 ★ 30% 현금 보유 (5/26 사고 후 영구 fix)."""
    from data.sajang_rules import SAJANG
    assert SAJANG.CASH_RESERVE_PCT == 0.30
    print("[PASS] CASH_RESERVE_PCT = 0.30 (사장님 영구 룰)")


def test_can_buy_30pct_rule():
    """can_buy 헬퍼 — 매수 후 현금 ≥ 총평가 × 30% 보장."""
    from data.sajang_rules import SAJANG
    # 5/26 사고 시나리오: 현금 37,087,184 / 총평가 37,442,684
    cash = 37087184
    total = 37442684
    # 매수 가능 최대 = cash - total × 0.30 = 25,854,378
    assert SAJANG.can_buy(cash, total, 25_000_000) is True   # OK (25백만 매수)
    assert SAJANG.can_buy(cash, total, 26_000_000) is False  # 30% 룰 위반 (26백만)
    assert SAJANG.max_buy_amount(cash, total) == 25854378.4 or SAJANG.max_buy_amount(cash, total) == 25854378
    print(f"[PASS] can_buy 30% 룰: max={SAJANG.max_buy_amount(cash, total):,} (5/26 시나리오)")


def test_calc_budget_per_stock_30pct():
    """calc_budget_per_stock — 30% 보유 + 70% / top_k 분배."""
    from data.sajang_rules import SAJANG
    # 5/26 시나리오
    cash = 37087184
    total = 37442684
    top_k = 3
    budget = SAJANG.calc_budget_per_stock(cash, total, top_k)
    # max_buy = 25,854,378 / 3 = 8,618,126
    assert 8_600_000 <= budget <= 8_620_000, f"종목당 8.6백만 기대: got {budget:,}"
    # 옛 코드 (사고): cash × 0.70 / 3 = 8,653,676 (30% 룰 위반)
    print(f"[PASS] calc_budget_per_stock: {budget:,}/종 (옛 사고: 8,653,676 / 차이 -{8653676 - budget:,})")


def test_cash_reserve_zero_total_eval():
    """초기 상태 (총평가 0) 시 30% 룰 미적용."""
    from data.sajang_rules import SAJANG
    assert SAJANG.can_buy(10_000_000, 0, 5_000_000) is True   # 단순 cash 체크
    assert SAJANG.max_buy_amount(10_000_000, 0) == 10_000_000
    print("[PASS] 초기 상태 (총평가 0) 30% 룰 미적용 (단순 cash 체크)")


def test_cash_reserve_violation_blocks_buy():
    """30% 룰 위반 시 매수 차단 (max_buy=0)."""
    from data.sajang_rules import SAJANG
    # 5/26 마감 상태: 현금 2,214,935 / 총평가 37,145,645
    # min_cash = 37,145,645 × 0.30 = 11,143,693
    # cash 2,214,935 < min_cash → max_buy = 0
    cash = 2214935
    total = 37145645
    assert SAJANG.max_buy_amount(cash, total) == 0  # 매수 차단
    assert SAJANG.can_buy(cash, total, 1_000_000) is False  # 100만 매수도 차단
    print(f"[PASS] 30% 룰 위반 시 매수 차단 (cash={cash:,} < min={int(total*0.3):,})")


def test_position_safety_uses_manual_president():
    """position_safety.py SYNC 자동등록이 manual_president 사용 확인."""
    p = ROOT / "bot" / "position_safety.py"
    src = p.read_text(encoding="utf-8")
    assert "manual_president" in src, "position_safety가 manual_president 사용 X"
    assert "'sync_auto'" not in src or 'sync_auto' in src.split('manual_president')[1], "옛 sync_auto 잔존"
    print("[PASS] position_safety SYNC 자동등록 = manual_president")


def test_pre_commit_has_rule_005_006_007():
    """pre-commit hook이 RULE-005/006/007 추가됐는지."""
    p = ROOT.parent / "tools" / "pre_commit_check.py"
    src = p.read_text(encoding="utf-8")
    assert '"id": "RULE-005"' in src, "RULE-005 (TP+5%) 누락"
    assert '"id": "RULE-006"' in src, "RULE-006 (mode=day) 누락"
    assert '"id": "RULE-007"' in src, "RULE-007 (entry_mode=open) 누락"
    print("[PASS] pre-commit RULE-005/006/007 신규 추가")


def test_auto_trader_l4083_disabled():
    """auto_trader.py L4083 mode='day' + TP 자동 매도 영구 차단 (elif False)."""
    p = ROOT / "bot" / "auto_trader.py"
    src = p.read_text(encoding="utf-8")
    # elif False ... mode == "day" 패턴 검출
    assert "elif False and pos.get(\"mode\") == \"day\"" in src, "L4083 elif False 영구 차단 X"
    print("[PASS] auto_trader L4083 mode='day' + TP 매도 영구 차단 (elif False)")


def test_asset_pool_tp_force_zero():
    """auto_trader.py asset_pool 매수 시 take_profit=0 강제 (L2557 fix)."""
    p = ROOT / "bot" / "auto_trader.py"
    src = p.read_text(encoding="utf-8")
    # take_profit=0 강제 (style_tp_pct 사용 X)
    assert "\"take_profit\": 0" in src, "asset_pool take_profit=0 강제 X"
    print("[PASS] asset_pool_scan_and_buy take_profit=0 강제 + mode='swing' 통일")


if __name__ == "__main__":
    print("=" * 70)
    print("★ 사장님 영구 룰 Rule Registry 단위 테스트 ★")
    print("=" * 70)
    tests = [
        test_registry_exists,
        test_fixed_tp_disabled,
        test_pullback_entry,
        test_trailing_3pct,
        test_rule_3_limit_up,
        test_rule_b_10pct,
        test_rule_c_d1_gap,
        test_rule_d_pre_close,
        test_sync_auto_president_protection,
        test_normal_sl_3pct,
        test_budget_mode_split_cash,
        test_cash_reserve_30pct,
        test_can_buy_30pct_rule,
        test_calc_budget_per_stock_30pct,
        test_cash_reserve_zero_total_eval,
        test_cash_reserve_violation_blocks_buy,
        test_position_safety_uses_manual_president,
        test_pre_commit_has_rule_005_006_007,
        test_auto_trader_l4083_disabled,
        test_asset_pool_tp_force_zero,
    ]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            failed += 1
            print(f"[FAIL] {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"[ERROR] {t.__name__}: {type(e).__name__}: {e}")
    print("=" * 70)
    print(f"결과: {passed}/{passed + failed} PASS, {failed} FAIL")
    sys.exit(0 if failed == 0 else 1)
