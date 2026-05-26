# -*- coding: utf-8 -*-
"""★ 사장님 영구 룰 단일 진실 (Single Source of Truth) ★

5/26 사고 후 단타봇 박사 미달 자성 → Rule Registry 신설.

사장님 영구 룰이 코드 곳곳에 분산 + 옛 코드와 공존 + default off 패턴 반복.
5/26 D-Day에 5건 사고 발생 (검증모드 / VWAP/호가 / pullback default / 트레일링 옛 / SYNC TP+5%).
모든 사고의 공통 원인 = 사장님 영구 룰이 단일 진실 X.

★ 사용 규칙 ★:
1. 모든 매수/매도 코드는 이 파일 import 의무
2. default 값 부적절 패턴 차단 (사장님 룰 영구 활성)
3. 변경은 사장님 직접 승인만 (단타봇 자율 변경 X)

★ 영구 메모리 ★:
  - [feedback_trailing_only_tp] (5/21) — 트레일링만 / 고정 TP 폐기
  - [project_pullback_entry_5_23] (5/23) — -3% 눌림 매수
  - [feedback_3tier_review_rule] (5/25) — 3-Tier 검수 의무
  - [project_sajang_rules_registry_5_26] (5/26) — 이 파일 제정
"""
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class SajangRules:
    """사장님 영구 룰 — 변경 시 사장님 직접 승인 의무 (frozen=True)."""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/21 영구 룰 — 트레일링만 (고정 TP 폐기)
    # 영구 메모리: feedback_trailing_only_tp
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    FIXED_TP_DISABLED: bool = True             # 모든 매수 시 take_profit=0 강제
    FIXED_TP_FORCE_ZERO: int = 0               # 사장님 매수/단타봇 매수 모두 TP=0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/23 영구 룰 — -3% 눌림 매수
    # 영구 메모리: project_pullback_entry_5_23
    # 백테스트: 시가 매수 -6.95% / 눌림 매수 +9.19% (차이 +15.14%p)
    # 5/26 단타봇 자율 검증: 추가 +3.7%p 우세
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ENTRY_MODE_DEFAULT: str = "pullback_3pct"  # default 'open' (시가) 금지
    PULLBACK_ENTRY_PCT: float = 3.0            # 시가 -3% 눌림 도달 시 진입

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/25 영구 룰 1 — 트레일링 -3% 일관
    # 영구 메모리: feedback_trailing_only_tp + 5/25 사장님 명령
    # "오르는거 최종 올랐는데 힘없이 떨어질때 -3% 되면 팔고 나오자"
    # 폐기: 옛 multi-zone -7% / -10% / -12% / -15%
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    TRAILING_PCT: float = 3.0                  # 모든 zone 고점 -3% 일관
    TRAILING_ACTIVATION_PCT: float = 3.0       # +3% 도달 시 활성
    TRAILING_BREAK_EVEN_AT_ACTIVATION: bool = True  # +3% 초과 시 본전 확보

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/25 영구 룰 3 — 상한가 +25%+ 분할 매도
    # 영구 메모리: project_5_24_sunday_13_15_complete
    # limit_up_split_sell 모듈로 분리: 절반 +29% 매도 + 절반 D+1 이월
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    LIMIT_UP_SPLIT_THRESHOLD: float = 25.0     # +25%+ 도달 시 분할 매도 활성
    LIMIT_UP_SELL_PCT: float = 29.0            # 절반 매도 시점 +29%
    LIMIT_UP_HOLD_RATIO: float = 0.5           # 절반 D+1 이월

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/25 영구 룰 B — 15:26 +10%+ 분할 매도
    # 영구 메모리: project_5_25_rule_b_15_26 (단타봇 자율 5/25 신설)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    RULE_B_THRESHOLD: float = 10.0             # asset_pool 매수 종목 +10%+ 시 분할 매도
    RULE_B_CRON_TIME: str = "15:26"            # KST 15:26 발동

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/25 영구 룰 C — D+1 갭다운 보호망
    # 영구 메모리: project_5_25_rule_c_d1_protect (단타봇 자율 5/25 신설)
    # 5/26 검증: 로킷헬스 -17.8% 즉시 매도 정답 / 링크제니 -4.6% 보유 정답
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    D1_GAP_SELL_THRESHOLD: float = -7.0        # D+1 시초 -7%+ 갭다운 즉시 매도
    RULE_C_CRON_TIME: str = "09:01"            # KST 09:01 발동 (시초가 1분 안정화 후)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/26 영구 룰 D — 14:50 D+0 종가 매수 (다음날 갭업 노림)
    # 영구 메모리: project_5_26_rule_d (단타봇 자율 + 사장님 통찰)
    # 백테스트: D+0 종가 매수 +22.29% / 시가 매수 +19.32% 대비 +2.97%p 우세
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    RULE_D_SURGE_MIN: float = 10.0             # 오늘 +10%+ 강세 종목만
    RULE_D_PULLBACK_MIN: float = 3.0           # 고점 -3% 이상 눌림 종목만
    RULE_D_CRON_TIME: str = "14:50"            # KST 14:50 발동
    RULE_D_TOP_K: int = 2                      # 상위 2종 선정

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 5/26 영구 룰 — SYNC 자동등록 사장님 매수 보호
    # 영구 메모리: feedback_president_manual_sell_backup (5/22)
    # 5/26 사고: 삼화콘덴서 47주 자동 매도 (TP+5% 잔존) → 영구 차단
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    SYNC_AUTO_TP: int = 0                      # SYNC 자동등록 시 TP=0 (트레일링만)
    SYNC_AUTO_MODE: str = "swing"              # mode='day' (D+0 청산) 금지
    SYNC_AUTO_SOURCE: str = "manual_president" # 사장님 직접 매수 표시 (단타봇 트레일링 핸들링 OK)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 영구 NORMAL SL — 매수가 -3% 안전망
    # 영구 메모리: feedback_account_first_principle (5/21)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    NORMAL_SL_PCT: float = 3.0                 # 매수가 -3% (위급 안전망)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ 자금 분배 — 사장님 5/25 split_cash 룰
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    BUDGET_MODE: str = "split_cash"            # 가용현금 × 0.70 / top_k
    BUDGET_CASH_RATIO: float = 0.70            # 가용현금의 70%

    @classmethod
    def get_take_profit(cls, buy_price: float) -> int:
        """사장님 영구 룰 — 모든 매수 시 TP=0 강제 (트레일링만)."""
        return cls.FIXED_TP_FORCE_ZERO

    @classmethod
    def get_normal_sl(cls, buy_price: float) -> int:
        """사장님 영구 룰 — 매수가 -3% NORMAL SL (안전망)."""
        return int(buy_price * (1 - cls.NORMAL_SL_PCT / 100))

    @classmethod
    def get_trailing_sl(cls, high_watermark: float) -> int:
        """사장님 영구 룰 — 고점 -3% 트레일링 SL."""
        return int(high_watermark * (1 - cls.TRAILING_PCT / 100))

    @classmethod
    def is_trailing_activated(cls, pnl_pct: float) -> bool:
        """사장님 영구 룰 — +3% 도달 시 트레일링 활성."""
        return pnl_pct >= cls.TRAILING_ACTIVATION_PCT

    @classmethod
    def is_limit_up_split_triggered(cls, pnl_pct: float) -> bool:
        """사장님 영구 룰 3 — +25%+ 상한가 분할 매도 발동."""
        return pnl_pct >= cls.LIMIT_UP_SPLIT_THRESHOLD

    @classmethod
    def is_rule_b_triggered(cls, pnl_pct: float) -> bool:
        """사장님 영구 룰 B — 15:26 +10%+ 분할 매도 발동."""
        return pnl_pct >= cls.RULE_B_THRESHOLD

    @classmethod
    def is_rule_c_triggered(cls, gap_pct: float) -> bool:
        """사장님 영구 룰 C — D+1 갭다운 -7%+ 즉시 매도."""
        return gap_pct <= cls.D1_GAP_SELL_THRESHOLD

    @classmethod
    def get_pre_close_d_position(cls, buy_price: float) -> dict:
        """사장님 영구 룰 D — 14:50 D+0 종가 매수 시 position 속성."""
        return {
            "take_profit": cls.FIXED_TP_FORCE_ZERO,
            "mode": cls.SYNC_AUTO_MODE,  # swing
            "stop_loss": cls.get_normal_sl(buy_price),
        }

    @classmethod
    def get_sync_auto_position(cls, buy_price: float) -> dict:
        """★ 5/26 사장님 매수 보호 룰 ★ SYNC 자동등록 시 안전 속성."""
        return {
            "take_profit": cls.SYNC_AUTO_TP,         # 0 (트레일링만)
            "mode": cls.SYNC_AUTO_MODE,              # swing (D+0 청산 X)
            "source": cls.SYNC_AUTO_SOURCE,          # manual_president (단타봇 트레일링 핸들링 OK)
            "stop_loss": cls.get_normal_sl(buy_price),
        }

    @classmethod
    def list_all_rules(cls) -> List[str]:
        """모든 룰 리스트 (단타봇 self-audit 용)."""
        return [
            f"FIXED_TP_DISABLED={cls.FIXED_TP_DISABLED}",
            f"ENTRY_MODE_DEFAULT='{cls.ENTRY_MODE_DEFAULT}'",
            f"TRAILING_PCT={cls.TRAILING_PCT}",
            f"TRAILING_ACTIVATION_PCT={cls.TRAILING_ACTIVATION_PCT}",
            f"LIMIT_UP_SPLIT_THRESHOLD={cls.LIMIT_UP_SPLIT_THRESHOLD}",
            f"RULE_B_THRESHOLD={cls.RULE_B_THRESHOLD}",
            f"D1_GAP_SELL_THRESHOLD={cls.D1_GAP_SELL_THRESHOLD}",
            f"RULE_D_SURGE_MIN={cls.RULE_D_SURGE_MIN}",
            f"SYNC_AUTO_TP={cls.SYNC_AUTO_TP}",
            f"SYNC_AUTO_MODE='{cls.SYNC_AUTO_MODE}'",
            f"NORMAL_SL_PCT={cls.NORMAL_SL_PCT}",
            f"BUDGET_CASH_RATIO={cls.BUDGET_CASH_RATIO}",
        ]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Global singleton (모든 코드에서 import)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SAJANG = SajangRules()


if __name__ == "__main__":
    print("=" * 70)
    print("★ 사장님 영구 룰 단일 진실 ★")
    print("=" * 70)
    print()
    print("[모든 룰]")
    for rule in SAJANG.list_all_rules():
        print(f"  - {rule}")
    print()
    print("[헬퍼 함수 검증]")
    print(f"  매수가 10,000원:")
    print(f"    - TP = {SAJANG.get_take_profit(10000)} (영구 0)")
    print(f"    - NORMAL SL = {SAJANG.get_normal_sl(10000)} (매수가 -3%)")
    print(f"    - 트레일링 SL (고점 12,000) = {SAJANG.get_trailing_sl(12000)} (고점 -3%)")
    print(f"    - 트레일링 활성 (pnl +3%) = {SAJANG.is_trailing_activated(3.0)}")
    print(f"    - 룰 3 발동 (pnl +25%) = {SAJANG.is_limit_up_split_triggered(25.0)}")
    print(f"    - 룰 B 발동 (pnl +10%) = {SAJANG.is_rule_b_triggered(10.0)}")
    print(f"    - 룰 C 발동 (갭 -7%) = {SAJANG.is_rule_c_triggered(-7.0)}")
    print(f"    - 룰 D position = {SAJANG.get_pre_close_d_position(10000)}")
    print(f"    - SYNC 자동등록 position = {SAJANG.get_sync_auto_position(10000)}")
