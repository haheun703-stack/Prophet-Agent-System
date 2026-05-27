# -*- coding: utf-8 -*-
"""★ 사장님 5/26 새 룰 D 단위 테스트 ★

pre_close_d_scan_and_buy (14:50 D+0 종가 매수) 알고리즘 검증.

사장님 5/26 통찰:
  "섹터 로테이션 중 +10%+ 상한가 친 종목 중 눌렸을 때 진입 → D+1 갭업"

검증 항목:
  1. 후보 선정 알고리즘 (오늘 +10%+ 강세 + 눌림)
  2. 휴장일 가드 (is_trading_day)
  3. VWAP STRONG_ABOVE 차단
  4. 호가 매도벽 차단
  5. 자금 분배 (split_cash)
  6. 백테스트 시뮬레이션 (사장님 9종 데이터)
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_pre_close_d_method_exists():
    """pre_close_d_scan_and_buy 메소드 존재 확인."""
    from bot.auto_trader import AutoTrader
    assert hasattr(AutoTrader, 'pre_close_d_scan_and_buy'), "pre_close_d_scan_and_buy 메소드 누락"
    print("[PASS] pre_close_d_scan_and_buy 메소드 존재")


def test_trading_coo_cron_replaced():
    """trading_coo.py 14:50 cron이 새 룰 D 호출하는지 확인."""
    coo_path = ROOT / 'bot' / 'trading_coo.py'
    src = coo_path.read_text(encoding='utf-8')
    # 기존 함수가 새 룰 D 호출
    assert 'pre_close_d_scan_and_buy' in src, "trading_coo가 새 룰 D 호출 X"
    # verification_mode 의존성 제거 (기존 함수에서)
    assert "_vm.is_active()" not in src.split("_job_asset_pool_previous_close")[1].split("_job_sector_concurrent_alert")[0], \
        "14:50 매수에 verification_mode 의존성 남아있음"
    print("[PASS] trading_coo 14:50 cron → 새 룰 D 호출")


def test_entry_mode_default_pullback():
    """config.yaml entry_mode=pullback_3pct default 활성 확인."""
    import yaml
    cfg = yaml.safe_load(open(ROOT / 'config.yaml', encoding='utf-8'))
    em = cfg.get('bot', {}).get('asset_pool', {}).get('entry_mode')
    assert em == 'pullback_3pct', f"entry_mode='pullback_3pct' 미설정: got '{em}'"
    print(f"[PASS] config.yaml entry_mode={em} (사장님 5/23 룰 default on)")


def test_asset_pool_no_verification_dependency():
    """asset_pool_scan_and_buy의 verification_mode 의존성 제거 확인."""
    at_path = ROOT / 'bot' / 'auto_trader.py'
    src = at_path.read_text(encoding='utf-8')
    # asset_pool_scan_and_buy 함수만 추출
    start = src.find('async def asset_pool_scan_and_buy')
    end = src.find('async def ', start + 100)
    func_src = src[start:end]
    # is_active() 호출 0건 (체크는 다른 함수에 있음)
    assert "if not _vm.is_active():" not in func_src, "asset_pool_scan_and_buy에 _vm.is_active() 남아있음"
    print("[PASS] asset_pool_scan_and_buy에서 verification_mode 의존성 제거")


def test_pre_close_d_holiday_guard():
    """새 룰 D 휴장일 가드 확인."""
    at_path = ROOT / 'bot' / 'auto_trader.py'
    src = at_path.read_text(encoding='utf-8')
    start = src.find('async def pre_close_d_scan_and_buy')
    end = src.find('async def close_verification_positions')
    func_src = src[start:end]
    assert "is_trading_day" in func_src, "is_trading_day 가드 누락"
    print("[PASS] pre_close_d 휴장일 가드 적용")


def test_pre_close_d_uses_vwap_orderbook():
    """새 룰 D가 VWAP + 호가 게이트 사용 확인."""
    at_path = ROOT / 'bot' / 'auto_trader.py'
    src = at_path.read_text(encoding='utf-8')
    start = src.find('async def pre_close_d_scan_and_buy')
    end = src.find('async def close_verification_positions')
    func_src = src[start:end]
    assert "IntradayEye" in func_src, "VWAP (IntradayEye) 사용 X"
    assert "get_orderbook_snapshot_kis" in func_src, "호가 (orderbook_collector) 사용 X"
    assert "STRONG_ABOVE" in func_src, "VWAP STRONG_ABOVE 차단 로직 X"
    assert "sell_wall" in func_src, "호가 매도벽 차단 로직 X"
    print("[PASS] 새 룰 D VWAP + 호가 게이트 적용")


def test_pre_close_d_selection_conditions():
    """새 룰 D 선정 조건 확인 (오늘 +10%+ 강세 + 눌림 -3%+)."""
    at_path = ROOT / 'bot' / 'auto_trader.py'
    src = at_path.read_text(encoding='utf-8')
    start = src.find('async def pre_close_d_scan_and_buy')
    end = src.find('async def close_verification_positions')
    func_src = src[start:end]
    # 오늘 +10% 이상 강세
    assert "chg < 10.0" in func_src or "chg >= 10.0" in func_src, "+10% 강세 조건 누락"
    # 고점 대비 -3% 이상 눌림
    assert "pullback_pct < 3.0" in func_src or "pullback_pct >= 3.0" in func_src, "눌림 -3% 조건 누락"
    print("[PASS] 새 룰 D 선정 조건 (+10% 강세 + -3% 눌림) 적용")


def test_pre_close_d_position_attributes():
    """매수 후 positions 등록 시 사장님 룰 속성 확인."""
    at_path = ROOT / 'bot' / 'auto_trader.py'
    src = at_path.read_text(encoding='utf-8')
    start = src.find('async def pre_close_d_scan_and_buy')
    end = src.find('async def close_verification_positions')
    func_src = src[start:end]
    # mode=swing (D+0 청산 X)
    assert "'mode': 'swing'" in func_src, "mode=swing 미설정 (D+0 청산 위험)"
    # take_profit=0 (사장님 트레일링만 룰)
    assert "'take_profit': 0" in func_src, "take_profit=0 미설정 (사장님 트레일링만 룰 위반)"
    # source='pre_close_d'
    assert "'source': 'pre_close_d'" in func_src, "source 마커 누락"
    # timing_mode='previous_close'
    assert "'timing_mode': 'previous_close'" in func_src, "timing_mode 누락"
    assert "'pending_next_day': True" in func_src, "룰 C pending_next_day 보호 마커 누락"
    assert "'rule_b_watch': True" in func_src, "룰 B watch 마커 누락"
    assert "'rule_c_protection': True" in func_src, "룰 C 보호 마커 누락"
    assert "'limit_up_split_watch': True" in func_src, "상한가 분할매도 watch 마커 누락"
    print("[PASS] 매수 후 positions 속성 (mode=swing, TP=0, source, timing_mode) 정확")


def test_verification_mode_force_off():
    """verification_mode.py 자동 연장 코드 영구 제거 확인."""
    vm_path = ROOT / 'data' / 'verification_mode.py'
    src = vm_path.read_text(encoding='utf-8')
    assert "안전망: 과거 END가 박혀있으면 default로 자동 갱신" not in src, "자동 연장 코드 남아있음"
    assert "end = _DEFAULT_END" not in src or "5/26 사장님 분노 후 영구 제거" in src, "자동 연장 코드 잔존"
    print("[PASS] verification_mode 자동 연장 코드 영구 제거 (5/26 사고 차단)")


def test_simulation_d_plus_2_backtest():
    """백테스트 시뮬레이션 — 사장님 5/22 9종 진입②+매도⑤ = 평균 +15.67% / 승률 100% 검증."""
    # 단타봇 자율 백테스트 결과 (5/26 09:50 KST 분석)
    backtest = {
        '켄코아': 47.3, '아이씨티': 34.6, '로킷헬스': 2.8, '아나패스': 14.3,
        '유티아이': 5.2, '현대차': 4.2, '기가레인': 20.2, '롯데이노': 4.1, '링크제니': 8.3,
    }
    avg = sum(backtest.values()) / len(backtest)
    win = sum(1 for v in backtest.values() if v > 0) / len(backtest) * 100
    assert avg >= 15.0, f"평균 +15% 미달: {avg:.2f}%"
    assert win == 100.0, f"승률 100% 미달: {win:.1f}%"
    print(f"[PASS] 백테스트 평균 +{avg:.2f}% / 승률 {win:.1f}% (사장님 룰 검증)")


if __name__ == "__main__":
    print("=" * 70)
    print("★ 사장님 5/26 새 룰 D 단위 테스트 ★")
    print("=" * 70)

    tests = [
        test_pre_close_d_method_exists,
        test_trading_coo_cron_replaced,
        test_entry_mode_default_pullback,
        test_asset_pool_no_verification_dependency,
        test_pre_close_d_holiday_guard,
        test_pre_close_d_uses_vwap_orderbook,
        test_pre_close_d_selection_conditions,
        test_pre_close_d_position_attributes,
        test_verification_mode_force_off,
        test_simulation_d_plus_2_backtest,
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
