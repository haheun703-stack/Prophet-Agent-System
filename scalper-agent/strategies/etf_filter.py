"""
ETF 방향 감지기
- KODEX200 / KODEX레버리지 기준
- 장 방향(롱/숏/중립) 판단
- 개별종목 선정 기준 제공
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


class MarketDirection(Enum):
    LONG    = "LONG"
    SHORT   = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class ETFSignal:
    direction:     MarketDirection
    etf_ticker:    str
    etf_change:    float   # 첫봉 등락률 (%)
    etf_volume:    float   # 거래량 비율 (현재/평균)
    strength:      float   # 방향 강도 0~1
    reason:        str


class ETFDirectionFilter:
    """
    9:05 첫봉 기준으로 당일 시장 방향 결정

    KODEX200    (069500) - 기본 방향
    KODEX레버리지 (122630) - 강도 확인용
    """

    ETF_TICKERS = {
        "069500": "KODEX200",
        "122630": "KODEX레버리지",
    }

    def __init__(
        self,
        primary:     str   = "069500",
        confirm:     str   = "122630",
        min_change:  float = 0.15,
        min_volume:  float = 0.8,
    ):
        self.primary    = primary
        self.confirm    = confirm
        self.min_change = min_change
        self.min_volume = min_volume

    def judge(
        self,
        primary_candle:  pd.Series,
        confirm_candle:  Optional[pd.Series] = None,
        avg_volume:      Optional[float]     = None,
    ) -> ETFSignal:
        """첫봉 분석 -> 당일 방향 결정"""
        o = primary_candle["open"]
        c = primary_candle["close"]
        v = primary_candle["volume"]

        change = (c - o) / o * 100
        vol_ratio = v / avg_volume if avg_volume else 1.0

        confirm_agree = True
        if confirm_candle is not None:
            cc = confirm_candle["close"]
            co = confirm_candle["open"]
            confirm_change = (cc - co) / co * 100
            confirm_agree = (change > 0) == (confirm_change > 0)

        if change >= self.min_change and vol_ratio >= self.min_volume and confirm_agree:
            direction = MarketDirection.LONG
            strength  = min(1.0, abs(change) / 0.5 * 0.5 + vol_ratio * 0.5)
            reason    = f"상승 {change:+.2f}% | 거래량비율 {vol_ratio:.1f}x"

        elif change <= -self.min_change and vol_ratio >= self.min_volume and confirm_agree:
            direction = MarketDirection.SHORT
            strength  = min(1.0, abs(change) / 0.5 * 0.5 + vol_ratio * 0.5)
            reason    = f"하락 {change:+.2f}% | 거래량비율 {vol_ratio:.1f}x"

        else:
            direction = MarketDirection.NEUTRAL
            strength  = 0.0
            reason    = f"방향 불명확 {change:+.2f}% (min:{self.min_change}%)"

        return ETFSignal(
            direction  = direction,
            etf_ticker = self.primary,
            etf_change = change,
            etf_volume = vol_ratio,
            strength   = strength,
            reason     = reason,
        )

    def judge_from_daily(self, df: pd.DataFrame, date_str: str = None) -> ETFSignal:
        """일봉 데이터에서 방향 판단 (백테스트용)"""
        if date_str:
            row = df[df["Date"] == date_str]
            if row.empty:
                return ETFSignal(MarketDirection.NEUTRAL, self.primary, 0, 0, 0, "데이터없음")
            row = row.iloc[0]
        else:
            row = df.iloc[-1]

        candle = pd.Series({
            "open": row["Open"], "close": row["Close"], "volume": row["Volume"]
        })
        avg_vol = df["Volume"].rolling(20).mean().iloc[-1] if len(df) >= 20 else row["Volume"]
        return self.judge(candle, avg_volume=avg_vol)
