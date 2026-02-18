"""
그룹 순환매 전략 (Group Rotation Strategy)

핵심 논리:
  1. 섹터 ETF가 상승 추세일 때 (MA 위)
  2. 구성종목 중 ETF 대비 후행하는 종목 포착
  3. 후행주 매수 → 캐치업 수익 확보
  4. ETF 추세 이탈 시 전량 청산

시간축: 일봉 기반, 보유기간 수일~수주
Body Hunter (5분봉 단타)와 완전 분리된 전략

그룹 예시:
  - 현대차그룹: KODEX 현대차그룹플러스(371160) + 현대차/기아/모비스 등
  - 삼성그룹:  KODEX 삼성그룹(102780) + 삼성전자/삼성물산/삼성SDI 등
  - 2차전지:   KODEX 2차전지산업(305720) + 에코프로/LG화학/포스코퓨처엠 등
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── 데이터 구조 ──────────────────────────────────────────────

class GroupTrend(Enum):
    BULLISH = "BULLISH"     # ETF > MA → 매수 허용
    BEARISH = "BEARISH"     # ETF < MA → 매수 금지, 기존 포지션 청산
    UNKNOWN = "UNKNOWN"


@dataclass
class GroupMember:
    code: str
    name: str


@dataclass
class GroupDef:
    """그룹 정의 (config에서 로드)"""
    group_id: str
    name: str
    etf_code: str
    members: List[GroupMember]


@dataclass
class RotationSignal:
    """순환매 매매 신호"""
    group_id: str
    group_name: str
    etf_code: str
    etf_trend: GroupTrend
    etf_price: float
    etf_ma: float
    picks: List[Dict]       # [{"code", "name", "gap_pct", "return_pct", "reason"}]
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class RotationPosition:
    """순환매 보유 포지션"""
    group_id: str
    code: str
    name: str
    entry_price: float
    entry_date: str
    shares: int
    invested: float         # 투자금
    sl_price: float         # 개별 손절가
    tp_price: float         # 개별 익절가
    hold_days: int = 0
    current_price: float = 0.0

    @property
    def pnl_pct(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) / self.entry_price * 100

    @property
    def pnl_amount(self) -> float:
        return (self.current_price - self.entry_price) * self.shares


# ── 메인 전략 ────────────────────────────────────────────────

class GroupRotationStrategy:
    """그룹 순환매 전략 엔진"""

    def __init__(self, config: dict):
        """
        Args:
            config: config.yaml의 strategies.group_rotation 섹션
        """
        self.enabled = config.get("enabled", True)

        # 그룹 정의 로드
        self.groups: Dict[str, GroupDef] = {}
        for gid, gconf in config.get("groups", {}).items():
            members = [GroupMember(m["code"], m["name"]) for m in gconf.get("members", [])]
            self.groups[gid] = GroupDef(
                group_id=gid,
                name=gconf["name"],
                etf_code=gconf["etf"],
                members=members,
            )

        # 진입 파라미터
        entry = config.get("entry", {})
        self.etf_ma_period = entry.get("etf_ma_period", 20)
        self.etf_above_ma = entry.get("etf_above_ma", True)
        self.lookback_days = entry.get("lookback_days", 20)
        self.max_picks = entry.get("max_picks", 2)
        self.min_gap_pct = entry.get("min_gap_pct", 5.0)
        self.min_volume_ratio = entry.get("min_volume_ratio", 0.8)

        # 청산 파라미터
        exit_cfg = config.get("exit", {})
        self.etf_below_ma = exit_cfg.get("etf_below_ma", True)
        self.individual_sl_pct = exit_cfg.get("individual_sl_pct", 5.0)
        self.individual_tp_pct = exit_cfg.get("individual_tp_pct", 15.0)
        self.max_hold_days = exit_cfg.get("max_hold_days", 20)

        # 리스크
        self.risk_per_group = config.get("risk_per_group", 500_000)
        self.total_risk_cap = config.get("total_risk_cap", 1_000_000)

        # 포지션 관리
        self.positions: Dict[str, RotationPosition] = {}   # code → position

        logger.info(
            f"[GroupRotation] {len(self.groups)}그룹 로드 "
            f"(MA{self.etf_ma_period}, gap>{self.min_gap_pct}%, "
            f"SL:{self.individual_sl_pct}%, TP:{self.individual_tp_pct}%)"
        )

    # ── 추세 판단 ────────────────────────────────────────────

    def check_etf_trend(self, etf_df: pd.DataFrame) -> Tuple[GroupTrend, float, float]:
        """ETF 일봉으로 추세 판단

        Args:
            etf_df: ETF 일봉 DataFrame (date index, OHLCV)

        Returns:
            (trend, current_price, ma_value)
        """
        if etf_df is None or len(etf_df) < self.etf_ma_period:
            return GroupTrend.UNKNOWN, 0.0, 0.0

        ma = etf_df["close"].rolling(self.etf_ma_period).mean()
        current_price = float(etf_df["close"].iloc[-1])
        ma_value = float(ma.iloc[-1])

        if np.isnan(ma_value):
            return GroupTrend.UNKNOWN, current_price, 0.0

        if current_price > ma_value:
            return GroupTrend.BULLISH, current_price, ma_value
        else:
            return GroupTrend.BEARISH, current_price, ma_value

    # ── 후행주 선별 ──────────────────────────────────────────

    def find_laggards(
        self,
        group: GroupDef,
        etf_df: pd.DataFrame,
        member_dfs: Dict[str, pd.DataFrame],
    ) -> List[Dict]:
        """ETF 대비 후행하는 종목 찾기

        Args:
            group: 그룹 정의
            etf_df: ETF 일봉
            member_dfs: {종목코드: 일봉 DataFrame}

        Returns:
            후행주 리스트 (gap 큰 순서)
            [{"code", "name", "etf_return", "stock_return", "gap_pct", "volume_ok", "reason"}]
        """
        if len(etf_df) < self.lookback_days:
            return []

        # ETF 수익률
        etf_close_now = float(etf_df["close"].iloc[-1])
        etf_close_past = float(etf_df["close"].iloc[-self.lookback_days])
        etf_return = (etf_close_now - etf_close_past) / etf_close_past * 100

        candidates = []
        for member in group.members:
            df = member_dfs.get(member.code)
            if df is None or len(df) < self.lookback_days:
                continue

            # 종목 수익률
            stk_close_now = float(df["close"].iloc[-1])
            stk_close_past = float(df["close"].iloc[-self.lookback_days])
            stk_return = (stk_close_now - stk_close_past) / stk_close_past * 100

            # 갭 = ETF 수익률 - 종목 수익률 (양수 = 종목이 후행)
            gap = etf_return - stk_return

            # 거래량 확인
            vol_avg = float(df["volume"].rolling(20).mean().iloc[-1])
            vol_now = float(df["volume"].iloc[-1])
            vol_ratio = vol_now / vol_avg if vol_avg > 0 else 0.0
            volume_ok = vol_ratio >= self.min_volume_ratio

            # 이미 보유 중인 종목 제외
            if member.code in self.positions:
                continue

            if gap >= self.min_gap_pct and volume_ok:
                candidates.append({
                    "code": member.code,
                    "name": member.name,
                    "etf_return": round(etf_return, 2),
                    "stock_return": round(stk_return, 2),
                    "gap_pct": round(gap, 2),
                    "volume_ratio": round(vol_ratio, 2),
                    "reason": (
                        f"ETF {etf_return:+.1f}% vs {member.name} {stk_return:+.1f}% "
                        f"(갭 {gap:.1f}%, 거래량 {vol_ratio:.1f}x)"
                    ),
                })

        # 갭 큰 순서로 정렬 → 최대 max_picks개
        candidates.sort(key=lambda x: x["gap_pct"], reverse=True)
        return candidates[:self.max_picks]

    # ── 메인 스캔 ────────────────────────────────────────────

    def scan(
        self,
        daily_data: Dict[str, pd.DataFrame],
    ) -> List[RotationSignal]:
        """전체 그룹 스캔 → 매매 신호 생성

        Args:
            daily_data: {종목코드: 일봉 DataFrame} — ETF 포함

        Returns:
            그룹별 RotationSignal 리스트
        """
        if not self.enabled:
            return []

        signals = []

        for gid, group in self.groups.items():
            etf_df = daily_data.get(group.etf_code)
            if etf_df is None:
                logger.warning(f"[GroupRotation] {group.name} ETF({group.etf_code}) 데이터 없음")
                continue

            # 1. ETF 추세 확인
            trend, etf_price, etf_ma = self.check_etf_trend(etf_df)

            if trend != GroupTrend.BULLISH:
                signals.append(RotationSignal(
                    group_id=gid,
                    group_name=group.name,
                    etf_code=group.etf_code,
                    etf_trend=trend,
                    etf_price=etf_price,
                    etf_ma=etf_ma,
                    picks=[],
                ))
                continue

            # 2. 후행주 찾기
            member_dfs = {m.code: daily_data.get(m.code) for m in group.members}
            member_dfs = {k: v for k, v in member_dfs.items() if v is not None}

            picks = self.find_laggards(group, etf_df, member_dfs)

            signals.append(RotationSignal(
                group_id=gid,
                group_name=group.name,
                etf_code=group.etf_code,
                etf_trend=trend,
                etf_price=etf_price,
                etf_ma=etf_ma,
                picks=picks,
            ))

            if picks:
                logger.info(
                    f"[GroupRotation] {group.name}: "
                    f"ETF {trend.value} (가격:{etf_price:,.0f} MA{self.etf_ma_period}:{etf_ma:,.0f}) "
                    f"후행주 {len(picks)}개 감지"
                )

        return signals

    # ── 포지션 관리 ──────────────────────────────────────────

    def enter_position(
        self,
        group_id: str,
        code: str,
        name: str,
        price: float,
        date_str: str,
        capital: float = None,
    ) -> Optional[RotationPosition]:
        """포지션 진입

        Args:
            capital: 투입 자금 (None이면 risk_per_group / max_picks)
        """
        if code in self.positions:
            logger.warning(f"[GroupRotation] {code} 이미 보유중")
            return None

        # 그룹별 투자한도 확인
        group_invested = sum(
            p.invested for p in self.positions.values() if p.group_id == group_id
        )
        if group_invested >= self.risk_per_group:
            logger.warning(f"[GroupRotation] {group_id} 그룹 한도 초과 ({group_invested:,.0f})")
            return None

        # 전체 한도 확인
        total_invested = sum(p.invested for p in self.positions.values())
        if total_invested >= self.total_risk_cap:
            logger.warning(f"[GroupRotation] 전체 한도 초과 ({total_invested:,.0f})")
            return None

        if capital is None:
            capital = self.risk_per_group / max(self.max_picks, 1)

        shares = int(capital / price) if price > 0 else 0
        if shares <= 0:
            return None

        invested = shares * price
        sl_price = price * (1 - self.individual_sl_pct / 100)
        tp_price = price * (1 + self.individual_tp_pct / 100)

        pos = RotationPosition(
            group_id=group_id,
            code=code,
            name=name,
            entry_price=price,
            entry_date=date_str,
            shares=shares,
            invested=invested,
            sl_price=sl_price,
            tp_price=tp_price,
            current_price=price,
        )
        self.positions[code] = pos

        logger.info(
            f"[GroupRotation] 진입 {name}({code}) "
            f"@{price:,.0f} x{shares}주 = {invested:,.0f}원 "
            f"(SL:{sl_price:,.0f} TP:{tp_price:,.0f})"
        )
        return pos

    def check_exits(
        self,
        daily_data: Dict[str, pd.DataFrame],
        current_date: str = None,
    ) -> List[Dict]:
        """보유 포지션 청산 조건 체크

        Returns:
            [{"code", "name", "reason", "exit_price", "pnl_pct", "pnl_amount", "hold_days"}]
        """
        exits = []

        for code, pos in list(self.positions.items()):
            df = daily_data.get(code)
            if df is None:
                continue

            pos.current_price = float(df["close"].iloc[-1])
            pos.hold_days += 1

            exit_reason = None

            # 1. 개별 손절
            if pos.current_price <= pos.sl_price:
                exit_reason = f"손절 ({pos.pnl_pct:+.1f}%)"

            # 2. 개별 익절
            elif pos.current_price >= pos.tp_price:
                exit_reason = f"익절 ({pos.pnl_pct:+.1f}%)"

            # 3. 최대 보유일 초과
            elif pos.hold_days >= self.max_hold_days:
                exit_reason = f"보유한도 {self.max_hold_days}일"

            # 4. ETF 추세 이탈 (그룹 전체 청산)
            if exit_reason is None and self.etf_below_ma:
                group = self.groups.get(pos.group_id)
                if group:
                    etf_df = daily_data.get(group.etf_code)
                    if etf_df is not None:
                        trend, _, _ = self.check_etf_trend(etf_df)
                        if trend == GroupTrend.BEARISH:
                            exit_reason = f"ETF 추세이탈 ({group.name})"

            if exit_reason:
                exits.append({
                    "code": code,
                    "name": pos.name,
                    "group_id": pos.group_id,
                    "reason": exit_reason,
                    "entry_price": pos.entry_price,
                    "exit_price": pos.current_price,
                    "shares": pos.shares,
                    "pnl_pct": round(pos.pnl_pct, 2),
                    "pnl_amount": round(pos.pnl_amount),
                    "hold_days": pos.hold_days,
                })
                del self.positions[code]

                logger.info(
                    f"[GroupRotation] 청산 {pos.name}({code}) "
                    f"@{pos.current_price:,.0f} ({exit_reason}) "
                    f"PnL: {pos.pnl_pct:+.1f}% = {pos.pnl_amount:+,.0f}원 "
                    f"({pos.hold_days}일 보유)"
                )

        return exits

    # ── 요약 ─────────────────────────────────────────────────

    def summary(self) -> str:
        """현재 상태 요약"""
        total_invested = sum(p.invested for p in self.positions.values())
        total_pnl = sum(p.pnl_amount for p in self.positions.values())

        lines = [
            f"[GroupRotation] 보유: {len(self.positions)}종목 "
            f"투자: {total_invested:,.0f}원 평가PnL: {total_pnl:+,.0f}원"
        ]

        for code, pos in self.positions.items():
            lines.append(
                f"  {pos.name}({code}) @{pos.entry_price:,.0f} → "
                f"{pos.current_price:,.0f} ({pos.pnl_pct:+.1f}%) "
                f"{pos.hold_days}일"
            )

        return "\n".join(lines)

    def reset(self):
        """포지션 초기화"""
        self.positions.clear()
