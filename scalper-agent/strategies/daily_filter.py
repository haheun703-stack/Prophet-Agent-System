"""
일봉 필터 (Daily Chart Filter)
- 5분봉 진입 전, 일봉에서 "몸통이 나올 수 있는 종목"인지 사전 검증
- MA 정배열, RSI, ADX, 외국인/기관 수급 등 복합 판단

철학:
  일봉 = "이 종목 몸통이 시작되겠다" 판단
  5분봉 = "지금 타라" 타이밍 결정
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class DailyScore:
    """일봉 기반 종목 점수"""
    code:             str
    name:             str
    total_score:      float    # 100점 만점
    ma_alignment:     float    # MA 정배열 (25점)
    rsi_score:        float    # RSI 적정구간 (15점)
    adx_score:        float    # ADX 추세 강도 (20점)
    flow_score:       float    # 외국인/기관 수급 (20점)
    volume_score:     float    # 거래량 추세 (10점)
    bollinger_score:  float    # 볼린저 위치 (10점)
    price:            float    # 현재가
    change_pct:       float    # 당일 등락률
    grade:            str      # S/A/B/C

    def __str__(self):
        return (
            f"{self.code}({self.name}) "
            f"[{self.grade}] {self.total_score:.0f}점 "
            f"MA:{self.ma_alignment:.0f} RSI:{self.rsi_score:.0f} "
            f"ADX:{self.adx_score:.0f} 수급:{self.flow_score:.0f}"
        )


class DailyFilter:
    """
    일봉 사전 필터

    역할: scanner.py가 당일 상대강도로 종목을 찾기 전에
          일봉 기준으로 "몸통이 나올 가능성이 높은 종목"을 사전 선별

    점수 체계 (100점):
      - MA 정배열 (25점): MA5 > MA20 > MA60 순서
      - ADX 추세강도 (20점): 25 이상이면 추세 존재
      - 외국인/기관 수급 (20점): 연속 순매수
      - RSI 적정구간 (15점): 40~70 구간이 최적
      - 거래량 추세 (10점): 최근 거래량 증가 추세
      - 볼린저 위치 (10점): 상단 근처 = 모멘텀

    필터 조건:
      - 최소 점수 50점 이상
      - 가격: 2,000원 이상
      - 거래대금: 10억원 이상 (유동성)
    """

    def __init__(
        self,
        min_score:        float = 50.0,
        min_price:        float = 2_000,
        min_trade_value:  float = 1_000_000_000,  # 10억
        lookback_days:    int   = 5,               # 수급 확인 기간
    ):
        self.min_score       = min_score
        self.min_price       = min_price
        self.min_trade_value = min_trade_value
        self.lookback_days   = lookback_days

    def score_stock(self, df: pd.DataFrame, code: str, name: str = "") -> Optional[DailyScore]:
        """
        종목 일봉 DataFrame으로 점수 산출

        Args:
            df: CSVLoader.load()로 가져온 DataFrame (소문자 컬럼)
            code: 종목코드
            name: 종목명
        Returns:
            DailyScore 또는 None (조건 미달)
        """
        if df is None or len(df) < 60:
            return None

        last = df.iloc[-1]
        price = float(last.get("close", 0))

        # 기본 필터
        if price < self.min_price:
            return None

        volume = float(last.get("volume", 0))
        trade_value = price * volume
        if trade_value < self.min_trade_value:
            return None

        # 등락률
        prev_close = float(df.iloc[-2]["close"]) if len(df) >= 2 else price
        change_pct = (price - prev_close) / prev_close * 100 if prev_close > 0 else 0

        # 각 항목 점수 계산
        ma_score      = self._score_ma_alignment(df)
        rsi_score     = self._score_rsi(df)
        adx_score     = self._score_adx(df)
        flow_score    = self._score_flow(df)
        volume_score  = self._score_volume_trend(df)
        boll_score    = self._score_bollinger(df)

        total = ma_score + rsi_score + adx_score + flow_score + volume_score + boll_score

        # 등급
        if total >= 80:
            grade = "S"
        elif total >= 65:
            grade = "A"
        elif total >= 50:
            grade = "B"
        else:
            grade = "C"

        return DailyScore(
            code=code, name=name, total_score=total,
            ma_alignment=ma_score, rsi_score=rsi_score,
            adx_score=adx_score, flow_score=flow_score,
            volume_score=volume_score, bollinger_score=boll_score,
            price=price, change_pct=change_pct, grade=grade,
        )

    def filter_universe(
        self,
        stock_data: Dict[str, pd.DataFrame],
        names: Dict[str, str] = None,
        min_grade: str = "B",
    ) -> List[DailyScore]:
        """
        전체 종목 필터링 → 등급 이상만 반환

        Args:
            stock_data: {code: DataFrame}
            names: {code: name}
            min_grade: 최소 등급 (S > A > B > C)
        """
        grade_order = {"S": 4, "A": 3, "B": 2, "C": 1}
        min_grade_val = grade_order.get(min_grade, 2)

        results = []
        for code, df in stock_data.items():
            name = (names or {}).get(code, code)
            score = self.score_stock(df, code, name)
            if score and grade_order.get(score.grade, 0) >= min_grade_val:
                results.append(score)

        results.sort(key=lambda x: x.total_score, reverse=True)

        if results:
            logger.info(f"일봉필터: {len(stock_data)}종목 → {len(results)}종목 통과 (최소 {min_grade}등급)")

        return results

    # ─── 점수 계산 세부 함수 ───

    def _score_ma_alignment(self, df: pd.DataFrame) -> float:
        """MA 정배열 점수 (25점 만점)"""
        score = 0.0
        last = df.iloc[-1]

        ma5  = float(last.get("ma5", 0))
        ma20 = float(last.get("ma20", 0))
        ma60 = float(last.get("ma60", 0))
        price = float(last["close"])

        if ma5 == 0 or ma20 == 0 or ma60 == 0:
            # MA 데이터 없으면 직접 계산
            ma5  = float(df["close"].tail(5).mean())
            ma20 = float(df["close"].tail(20).mean())
            ma60 = float(df["close"].tail(60).mean())

        # 완전 정배열: 가격 > MA5 > MA20 > MA60
        if price > ma5 > ma20 > ma60:
            score = 25.0
        # 부분 정배열
        elif price > ma20 > ma60:
            score = 18.0
        elif ma5 > ma20 > ma60:
            score = 15.0
        elif price > ma20:
            score = 10.0
        elif price > ma60:
            score = 5.0

        return score

    def _score_rsi(self, df: pd.DataFrame) -> float:
        """RSI 점수 (15점 만점) - 40~70이 최적"""
        score = 0.0
        last = df.iloc[-1]

        rsi = float(last.get("rsi", 0))
        if rsi == 0:
            return 7.0  # 데이터 없으면 중립

        if 45 <= rsi <= 65:
            score = 15.0   # 최적 구간
        elif 40 <= rsi <= 70:
            score = 12.0   # 양호
        elif 35 <= rsi <= 75:
            score = 8.0    # 보통
        elif rsi > 75:
            score = 3.0    # 과매수 위험
        elif rsi < 35:
            score = 2.0    # 과매도 (반등 가능하지만 불확실)

        return score

    def _score_adx(self, df: pd.DataFrame) -> float:
        """ADX 점수 (20점 만점) - 추세 강도"""
        score = 0.0
        last = df.iloc[-1]

        adx = float(last.get("adx", 0))
        if adx == 0:
            return 5.0  # 데이터 없으면 최소

        if adx >= 40:
            score = 20.0   # 매우 강한 추세
        elif adx >= 30:
            score = 16.0   # 강한 추세
        elif adx >= 25:
            score = 12.0   # 추세 존재
        elif adx >= 20:
            score = 8.0    # 약한 추세
        else:
            score = 3.0    # 추세 없음 (횡보)

        return score

    def _score_flow(self, df: pd.DataFrame) -> float:
        """외국인/기관 수급 점수 (20점 만점)"""
        score = 0.0

        # 최근 N일 수급 확인
        recent = df.tail(self.lookback_days)

        foreign = recent.get("foreign_net")
        inst = recent.get("inst_net")

        if foreign is None and inst is None:
            return 5.0  # 데이터 없으면 최소

        foreign_sum = float(foreign.sum()) if foreign is not None else 0
        inst_sum = float(inst.sum()) if inst is not None else 0

        foreign_positive_days = int((foreign > 0).sum()) if foreign is not None else 0
        inst_positive_days = int((inst > 0).sum()) if inst is not None else 0

        # 외국인 점수 (10점)
        if foreign_positive_days >= 4 and foreign_sum > 0:
            score += 10.0   # 연속 순매수
        elif foreign_positive_days >= 3 and foreign_sum > 0:
            score += 7.0
        elif foreign_sum > 0:
            score += 4.0
        elif foreign_sum < 0:
            score += 1.0    # 순매도

        # 기관 점수 (10점)
        if inst_positive_days >= 4 and inst_sum > 0:
            score += 10.0
        elif inst_positive_days >= 3 and inst_sum > 0:
            score += 7.0
        elif inst_sum > 0:
            score += 4.0
        elif inst_sum < 0:
            score += 1.0

        return score

    def _score_volume_trend(self, df: pd.DataFrame) -> float:
        """거래량 추세 점수 (10점 만점)"""
        score = 0.0

        if len(df) < 20:
            return 5.0

        recent_vol = float(df["volume"].tail(5).mean())
        avg_vol = float(df["volume"].tail(20).mean())

        if avg_vol == 0:
            return 5.0

        vol_ratio = recent_vol / avg_vol

        if vol_ratio >= 2.0:
            score = 10.0   # 거래량 급증
        elif vol_ratio >= 1.5:
            score = 8.0    # 거래량 증가
        elif vol_ratio >= 1.2:
            score = 6.0    # 소폭 증가
        elif vol_ratio >= 0.8:
            score = 4.0    # 보통
        else:
            score = 2.0    # 거래량 감소

        return score

    def _score_bollinger(self, df: pd.DataFrame) -> float:
        """볼린저밴드 위치 점수 (10점 만점)"""
        score = 0.0
        last = df.iloc[-1]

        upper = float(last.get("upper_band", 0))
        lower = float(last.get("lower_band", 0))
        price = float(last["close"])
        ma20  = float(last.get("ma20", 0))

        if upper == 0 or lower == 0 or upper == lower:
            return 5.0  # 데이터 없음

        # 볼린저 밴드 내 위치 (0~1)
        position = (price - lower) / (upper - lower)

        if 0.6 <= position <= 0.85:
            score = 10.0   # 상단 근처 = 상승 모멘텀
        elif 0.45 <= position <= 0.95:
            score = 7.0    # 중상단
        elif 0.3 <= position <= 0.6:
            score = 5.0    # 중간
        elif position > 0.95:
            score = 3.0    # 밴드 이탈 = 과열 위험
        else:
            score = 2.0    # 하단 = 하락 추세

        return score
