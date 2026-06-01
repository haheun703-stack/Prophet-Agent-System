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
    # ★ 5/27 08:15 사장님 분노 fix ★ "내가 산게 뭔데 니가 다 샀는데 진짜 니가 다 진행을 해야지"
    # 사고: SYNC_AUTO_SOURCE='manual_president' 추정 라벨 = 단타봇 매수를 사장님 책임으로 떠넘김 잘못
    # Fix: 'sync_auto_unknown' = 출처 불명 명시 → 단타봇 매매일지 추적 의무 + 사장님 confirm 후 정확 source 결정
    SYNC_AUTO_SOURCE: str = "sync_auto_unknown" # ★ 출처 불명 (추정 라벨 금지 — 5/27 사장님 명령)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★★★ 5/27 사장님 영구 룰 — 모닝 자동 매수 영구 차단 ★★★
    # 사장님 5/27 09:50 분노 명령: "9시 장시작하자 마자 매수를 했잖아"
    # 사고: _morning_swing() (08:55) + _check_entry_watch() (30초 루프) →
    #       08:57 진입 조건 충족 → execute_pending_auto_buys() → safe_buy() KIS 매수
    #       HD한국조선해양/한화오션/HL만도 각 1주 시장가 매수
    # Fix: execute_pending_auto_buys() 시작에서 영구 차단 (MORNING_AUTO_BUY_DISABLED=True)
    # 영향: 모든 _pending_auto_buys 큐 자동 SKIP / 사장님 명령 없이 매수 X
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    MORNING_AUTO_BUY_DISABLED: bool = True     # ★ 모닝 자동 매수 영구 차단 (5/27 사장님 명령) ★

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★★★ 5/27 사장님 명령 — 자동매매 OFF (전체) ★★★
    # 사장님 5/27 11:10 명령: "일단 자동매매 OFF"
    # 사고: 5/27 사장님 분노 (08:57 자동 매수 + 09:38 자동 매도 + 매매일지 누락 + mojito 버그)
    # Fix: 모든 자동 매수 + 자동 매도 cron 영구 차단 (단일 플래그)
    # 영향: 09:15/14:50 매수 / 09:01 룰C / 15:26 룰B / hard_kill / 트레일링 / JARVIS 모두 OFF
    # 사장님 텔레그램 명령 시에만 매수/매도 가능
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    AUTO_TRADE_DISABLED: bool = True           # ★ 자동매매 전체 OFF (5/27 사장님 명령) ★

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

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★★★ 사장님 영구 룰 — 30% 현금 보유 (5/26 사고 후 영구 fix) ★★★
    # 사장님 명령 여러 번 "30% 현금 보유 + 70% 분배"
    # 단타봇 사고: budget_mode='split_cash' × 0.70 / N회 매수 누적 = 결과 100% 사용
    # 5/26 마감 현금 = 6.0% (사장님 30% 룰 위반)
    # 영구 fix: 매수 전 can_buy() 호출 의무 / 매수 후 현금 ≥ 총평가 × 30% 보장
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    CASH_RESERVE_PCT: float = 0.30             # ★ 항상 총평가 × 30% 현금 보유 강제 ★

    # ── 선정 스타일 파라미터 (4단 재배선, 5/31) — 주문룰 아님, backtest 보정 전 잠정 ──
    THESIS_GATE_STRONG_NORM: float = 0.6   # 명분 버킷 강(强) 정규화 임계
    THESIS_GATE_MIN_STRONG: int = 2        # {재료·수급·기술위치} 강 최소 개수
    THESIS_MIN_CANDIDATES: int = 3         # 이하면 합산 degrade(안전밸브)
    KKI_EXPLOSIVE: float = 70.0            # 끼 등급 컷 (score_kki 0~100) — 데이터 보정 대상
    KKI_HUNTABLE: float = 50.0
    KKI_MODERATE: float = 30.0
    KKI_MIN_LIQUIDITY_억: float = 50.0     # 거래대금 미만 = 끼 무효(못 사는 끼)

    # ── 워치리스트(휴면→첫급등 연속자) 선정 임계 (6/1 확정·검증값) — 주문룰 아님, shadow ──
    # 근거: surge_filter_reentry_5_31(db5cd76) STOP_REENTER 리프트 +1.9~2.4%p 전regime / dormant_first_surge(20f89a7)
    WL_PRE_DAYS: int = 20                  # 휴면 판정 lookback
    WL_DORMANT_MAX: float = 0.10           # 직전 PRE일 최대 일간상승 < 10% = 휴면
    WL_BASE_POS: float = 0.60              # 기지(20일 range 내 전일 위치) 상한
    WL_SURGE_MIN: float = 0.15             # 첫급등 최소
    WL_CS_MIN: float = 0.80                # 종가강도(끝까지 밀어) — 연속자 1번 신호
    WL_VOL_MIN: float = 5.0                # 거래량 MA20 배수
    WL_OVERHEAT: float = 0.30              # 과열회피(이상=blow-off 소진 차단)
    WL_LIQ_FLOOR_억: float = 30.0          # 거래대금 하한(못 사는 끼 제외)

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
    def kki_grade(cls, kki_score: float) -> str:
        """끼 점수(0~100) → 등급. 4단 Stage1 + 재진입 자격게이트 공용 단일진실."""
        if kki_score >= cls.KKI_EXPLOSIVE:
            return "EXPLOSIVE"
        if kki_score >= cls.KKI_HUNTABLE:
            return "HUNTABLE"
        if kki_score >= cls.KKI_MODERATE:
            return "MODERATE"
        return "SLUGGISH"

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
    def can_buy(cls, cash: float, total_eval: float, buy_amount: float) -> bool:
        """★ 사장님 30% 현금 보유 영구 룰 ★ 매수 후 현금 ≥ 총평가 × 30% 보장.

        Args:
            cash: 현재 가용 현금
            total_eval: 총평가액 (현금 + 보유 종목 평가)
            buy_amount: 매수 시도 금액

        Returns:
            True = 매수 가능 / False = 30% 룰 위반 (매수 차단)
        """
        if total_eval <= 0:
            # 평가액 0 (초기 상태) = 단순 cash 체크 (30% 룰 미적용)
            return cash >= buy_amount
        cash_after = cash - buy_amount
        min_cash = total_eval * cls.CASH_RESERVE_PCT
        return cash_after >= min_cash

    @classmethod
    def max_buy_amount(cls, cash: float, total_eval: float) -> int:
        """★ 사장님 30% 현금 보유 영구 룰 ★ 매수 가능 최대 금액.

        Returns:
            max(0, cash - total_eval × 0.30) = 30% 보유 후 사용 가능 현금
        """
        if total_eval <= 0:
            return int(cash)
        min_cash = total_eval * cls.CASH_RESERVE_PCT
        return max(0, int(cash - min_cash))

    @classmethod
    def calc_budget_per_stock(cls, cash: float, total_eval: float, top_k: int) -> int:
        """★ 사장님 30% 현금 보유 + 70% 분배 통합 룰 ★

        매수 가능 금액 = 가용현금 - 총평가 × 30%
        종목당 분배 = 매수 가능 금액 / top_k

        예시 (오늘 5/26 사고 시나리오):
          cash=37,087,184 / total=37,442,684 / top_k=3
          → max_buy = 37,087,184 - 37,442,684 × 0.30 = 25,854,378원
          → 종목당 = 25,854,378 / 3 = 8,618,126원
          → 매수 후 현금 = 37,087,184 - 25,854,378 = 11,232,806원 (30% 정확 보유 ✅)
        """
        max_buy = cls.max_buy_amount(cash, total_eval)
        if top_k <= 0:
            return 0
        return max_buy // top_k

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
            f"CASH_RESERVE_PCT={cls.CASH_RESERVE_PCT}  ★ 30% 현금 보유 영구 룰 ★",
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
