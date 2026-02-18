"""
상대강도 스캐너
- ETF 대비 강한 개별종목 추출
- 당일 수급 집중 종목 = 몸통이 선명하게 나오는 종목
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

from strategies.etf_filter import MarketDirection

logger = logging.getLogger(__name__)


@dataclass
class StockCandidate:
    """당일 몸통 후보 종목"""
    ticker:         str
    name:           str
    change_pct:     float
    etf_change:     float
    relative_str:   float
    volume_ratio:   float
    current_price:  float
    first_high:     float
    first_low:      float
    first_mid:      float
    score:          float
    direction:      str

    def __str__(self):
        return (
            f"{self.ticker}({self.name}) "
            f"등락:{self.change_pct:+.2f}% "
            f"상대강도:{self.relative_str:.1f}x "
            f"거래량:{self.volume_ratio:.1f}x "
            f"점수:{self.score:.1f}"
        )


class RelativeStrengthScanner:
    """
    ETF 대비 상대강도 상위 종목 추출

    선정 로직:
      1. ETF 방향과 동일한 방향
      2. 등락률 = ETF x 2배 이상
      3. 거래량 = 평소 대비 1.5배 이상
      4. 주가 5,000~500,000원 (유동성)
    """

    def __init__(
        self,
        min_relative_str:  float = 2.0,
        min_volume_ratio:  float = 1.5,
        min_price:         float = 5_000,
        max_price:         float = 500_000,
        top_n:             int   = 5,
    ):
        self.min_relative_str = min_relative_str
        self.min_volume_ratio = min_volume_ratio
        self.min_price        = min_price
        self.max_price        = max_price
        self.top_n            = top_n

    def scan(
        self,
        etf_change:    float,
        market_dir:    MarketDirection,
        stock_data:    Dict[str, pd.Series],
        avg_volumes:   Optional[Dict[str, float]] = None,
        stock_names:   Optional[Dict[str, str]]   = None,
    ) -> List[StockCandidate]:
        """전 종목 스캔 -> 조건 필터 -> 상위 N개 반환"""
        if market_dir == MarketDirection.NEUTRAL:
            logger.info("중립장 - 스캔 스킵")
            return []

        if abs(etf_change) < 0.01:
            return []

        direction = "LONG" if market_dir == MarketDirection.LONG else "SHORT"
        candidates = []

        for ticker, candle in stock_data.items():
            try:
                o = candle["open"]
                c = candle["close"]
                h = candle["high"]
                l = candle["low"]
                v = candle["volume"]

                if not (self.min_price <= c <= self.max_price):
                    continue
                if o == 0:
                    continue

                change_pct = (c - o) / o * 100

                # 방향 일치 확인
                if direction == "LONG" and change_pct <= 0:
                    continue
                if direction == "SHORT" and change_pct >= 0:
                    continue

                # 상대강도
                relative_str = abs(change_pct) / abs(etf_change) if abs(etf_change) > 0 else 0
                if relative_str < self.min_relative_str:
                    continue

                # 거래량
                avg_vol = (avg_volumes or {}).get(ticker, v)
                vol_ratio = v / avg_vol if avg_vol > 0 else 1.0
                if vol_ratio < self.min_volume_ratio:
                    continue

                mid = (h + l) / 2
                score = relative_str * 0.4 + vol_ratio * 0.4 + abs(change_pct) * 0.2
                name = (stock_names or {}).get(ticker, ticker)

                candidates.append(StockCandidate(
                    ticker=ticker, name=name,
                    change_pct=change_pct, etf_change=etf_change,
                    relative_str=relative_str, volume_ratio=vol_ratio,
                    current_price=c, first_high=h, first_low=l, first_mid=mid,
                    score=score, direction=direction,
                ))
            except (KeyError, ZeroDivisionError):
                continue

        candidates.sort(key=lambda x: x.score, reverse=True)
        selected = candidates[:self.top_n]

        if selected:
            logger.info(f"[{direction}] {len(selected)}종목 선정:")
            for c in selected:
                logger.info(f"  {c}")

        return selected

    @staticmethod
    def generate_sample_stock_data(
        etf_change: float, n_stocks: int = 20, seed: int = 42
    ) -> Dict[str, pd.Series]:
        """백테스트용 샘플 데이터 생성"""
        rng = np.random.default_rng(seed)
        tickers = [
            "005930", "000660", "035420", "005380", "000270",
            "051910", "006400", "035720", "068270", "028260",
            "066570", "003550", "096770", "017670", "030200",
            "055550", "105560", "086790", "032830", "010950",
        ][:n_stocks]

        data = {}
        for t in tickers:
            base = rng.uniform(30000, 150000)
            # 일부 종목은 ETF보다 강하게
            mult = rng.choice([0.3, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0], p=[0.1, 0.1, 0.2, 0.2, 0.15, 0.15, 0.1])
            change = etf_change * mult / 100
            o = base
            c = base * (1 + change)
            h = max(o, c) * (1 + abs(rng.normal(0, 0.005)))
            l = min(o, c) * (1 - abs(rng.normal(0, 0.005)))
            v = int(rng.uniform(500_000, 5_000_000))
            data[t] = pd.Series({"open": o, "high": h, "low": l, "close": c, "volume": v})

        return data
