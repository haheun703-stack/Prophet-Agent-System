"""
DrawdownShield v1.0 — 연패 기반 리스크 자동 축소

Prop Firm 철학:
  - 연패가 올 때 리스크를 줄여서 계좌를 보호
  - 새 고점(equity peak) 갱신 시 리스크 복원
  - 연패 2회마다 한 단계씩 리스크 축소

TIERS (기본):
  0~1연패: 50,000원/건
  2~3연패: 25,000원/건
  4~5연패: 15,000원/건
  6+연패:  10,000원/건

수학적 근거:
  Fixed 2:1 TP + SL 0.6R 기준
    - 승: +2.0R × risk, 패: -0.6R × risk
    - 27% 승률: 기대값 = 0.27×2.0 - 0.73×0.6 = +0.10R
    - 6연패 확률: 0.73^6 = 15.1% → 드물지 않음
    - DrawdownShield로 6연패 시 최대 낙폭 -175K (vs -300K without)
"""

import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class DrawdownShield:
    """연패 기반 리스크 자동 축소 모듈"""

    # 리스크 단계: 연패 횟수에 따라 축소
    tiers: List[int] = field(default_factory=lambda: [50_000, 25_000, 15_000, 10_000])

    # 상태
    peak_equity: float = 0.0
    current_equity: float = 0.0
    losses_from_peak: int = 0      # peak 이후 연속 손실 횟수
    total_trades: int = 0
    total_wins: int = 0
    total_losses: int = 0

    # 이력
    tier_history: List[int] = field(default_factory=list)

    def update(self, pnl: float) -> int:
        """거래 결과 반영 → 현재 리스크 반환

        Args:
            pnl: 이번 거래 손익 (원화)

        Returns:
            다음 거래에 적용할 리스크 (원화)
        """
        self.current_equity += pnl
        self.total_trades += 1

        if pnl > 0:
            self.total_wins += 1
        elif pnl < 0:
            self.total_losses += 1

        # 새 고점 → 연패 카운터 리셋
        if self.current_equity > self.peak_equity:
            old_losses = self.losses_from_peak
            self.peak_equity = self.current_equity
            self.losses_from_peak = 0
            if old_losses > 0:
                logger.info(
                    f"[DrawdownShield] 새 고점! {self.peak_equity:+,.0f}원 "
                    f"(연패 {old_losses}회 → 리셋)"
                )
        elif pnl < 0:
            self.losses_from_peak += 1
            logger.info(
                f"[DrawdownShield] 연패 {self.losses_from_peak}회 "
                f"(equity: {self.current_equity:+,.0f}원, "
                f"peak: {self.peak_equity:+,.0f}원)"
            )

        risk = self.current_risk
        self.tier_history.append(risk)
        return risk

    @property
    def current_risk(self) -> int:
        """현재 연패 횟수에 따른 리스크 금액"""
        idx = min(self.losses_from_peak // 2, len(self.tiers) - 1)
        return self.tiers[idx]

    @property
    def current_tier(self) -> int:
        """현재 티어 인덱스 (0=최대 리스크, 3=최소)"""
        return min(self.losses_from_peak // 2, len(self.tiers) - 1)

    @property
    def drawdown(self) -> float:
        """현재 낙폭 (peak 대비)"""
        return self.current_equity - self.peak_equity

    @property
    def win_rate(self) -> float:
        """전체 승률"""
        if self.total_trades == 0:
            return 0.0
        return self.total_wins / self.total_trades * 100

    def summary(self) -> str:
        """현재 상태 요약"""
        return (
            f"DrawdownShield: equity={self.current_equity:+,.0f}원 "
            f"peak={self.peak_equity:+,.0f}원 "
            f"DD={self.drawdown:+,.0f}원 "
            f"연패={self.losses_from_peak} "
            f"리스크={self.current_risk:,}원/건 "
            f"({self.total_wins}W/{self.total_losses}L "
            f"WR:{self.win_rate:.1f}%)"
        )

    def reset(self):
        """완전 리셋"""
        self.peak_equity = 0.0
        self.current_equity = 0.0
        self.losses_from_peak = 0
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.tier_history = []
