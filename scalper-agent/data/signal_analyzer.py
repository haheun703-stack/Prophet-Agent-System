# -*- coding: utf-8 -*-
"""
1D~4D 일간 시그널 분석기 + 기록 시스템
========================================
미래에셋증권우 교훈: 조정 vs 하락을 사전에 판단하려면
다차원 시그널을 매일 기록하고 변곡점을 포착해야 한다.

1D: 가격 구조 (봉 형태, 꼬리 비율, 캔들 패턴)
2D: 거래량 에너지 (거래량비, 소진/스파이크, 다이버전스)
3D: 수급 흐름 (기관/외인 순매수, 공매도) — SupplyAnalyzer 재사용
4D: 모멘텀 (RSI, MACD, 볼린저, MA배열) — SupplyAnalyzer 재사용

저장: data_store/signals/YYYYMMDD.csv (일간 전종목 레코드)
      data_store/signals/{code}_history.csv (종목별 히스토리)

사용법:
  from data.signal_analyzer import SignalAnalyzer
  sa = SignalAnalyzer()
  record = sa.analyze_stock("005930", "2025-03-03")  # 특정일
  sa.record_daily(codes)                              # 오늘 전종목
  sa.backfill(codes, "20250301", "20260219")          # 백필
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

from data.indicator_calc import IndicatorCalc as IC

logger = logging.getLogger("BH.Signal")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
SHORT_DIR = DATA_DIR / "short"
SIGNAL_DIR = DATA_DIR / "signals"
SIGNAL_HISTORY_DIR = SIGNAL_DIR / "history"

# 시그널 레코드 컬럼 정의
SIGNAL_COLUMNS = [
    # 기본
    "date", "code", "name", "close", "change_pct",
    # 1D 가격구조
    "body_pct", "upper_wick_pct", "lower_wick_pct",
    "candle_type", "candle_pattern",
    "ma5", "ma20", "ma60", "ma_status",
    # 2D 거래량에너지
    "volume", "vol_ratio_ma20", "vol_trend",
    "vol_exhaustion", "vol_price_div",
    # 3D 수급흐름
    "inst_net_5d", "foreign_net_5d", "smart_net_5d",
    "short_change_5d", "supply_grade", "supply_score",
    # 4D 모멘텀
    "rsi14", "macd_hist", "macd_cross", "bb_position",
    "momentum_signal", "momentum_score",
    # 종합
    "signal", "confidence", "key_signal",
]


def _ensure_dirs():
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    SIGNAL_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


class SignalAnalyzer:
    """1D~4D 통합 일간 시그널 분석기"""

    def __init__(self):
        self._daily_cache: Dict[str, pd.DataFrame] = {}
        self._flow_cache: Dict[str, pd.DataFrame] = {}
        self._short_cache: Dict[str, pd.DataFrame] = {}

    def _load_daily(self, code: str) -> Optional[pd.DataFrame]:
        if code in self._daily_cache:
            return self._daily_cache[code]
        path = DAILY_DIR / f"{code}.csv"
        if not path.exists():
            return None
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        col_map = {"시가": "open", "고가": "high", "저가": "low",
                    "종가": "close", "거래량": "volume", "등락률": "change_pct"}
        df.rename(columns=col_map, inplace=True)
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        self._daily_cache[code] = df
        return df

    def _load_flow(self, code: str) -> Optional[pd.DataFrame]:
        if code in self._flow_cache:
            return self._flow_cache[code]
        path = FLOW_DIR / f"{code}_investor.csv"
        if not path.exists():
            return None
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        self._flow_cache[code] = df
        return df

    def _load_short(self, code: str) -> Optional[pd.DataFrame]:
        if code in self._short_cache:
            return self._short_cache[code]
        path = SHORT_DIR / f"{code}_short_bal.csv"
        if not path.exists():
            return None
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        self._short_cache[code] = df
        return df

    # ================================================================
    #  1D: 가격 구조 분석
    # ================================================================

    def _analyze_1d(self, day_df: pd.DataFrame, as_of: pd.Timestamp) -> dict:
        """1D 가격구조: 봉 형태 + 이평선 배열"""
        df = day_df[day_df.index <= as_of]
        if len(df) < 60:
            return self._empty_1d()

        row = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else row
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])

        # 봉 몸통/꼬리 비율
        total_range = h - l if h != l else 1
        body = abs(c - o)
        body_pct = round(body / total_range * 100, 1)

        if c >= o:  # 양봉
            upper_wick = h - c
            lower_wick = o - l
        else:  # 음봉
            upper_wick = h - o
            lower_wick = c - l

        upper_wick_pct = round(upper_wick / total_range * 100, 1)
        lower_wick_pct = round(lower_wick / total_range * 100, 1)

        # 캔들 타입
        if body_pct < 10:
            candle_type = "doji"
        elif c > o:
            candle_type = "bullish"
        else:
            candle_type = "bearish"

        # 캔들 패턴 판정
        candle_pattern = self._detect_pattern(df)

        # 이평선
        close = df["close"].astype(float)
        ma5 = float(IC.sma(close, 5).iloc[-1])
        ma20 = float(IC.sma(close, 20).iloc[-1])
        ma60 = float(IC.sma(close, 60).iloc[-1])

        if c > ma5 > ma20 > ma60:
            ma_status = "perfect_bull"
        elif ma5 > ma20 > ma60:
            ma_status = "bull"
        elif c > ma20:
            ma_status = "neutral_up"
        elif c > ma60:
            ma_status = "neutral_down"
        else:
            ma_status = "bear"

        change_pct = round((c - float(prev["close"])) / float(prev["close"]) * 100, 2) if float(prev["close"]) > 0 else 0

        return {
            "close": int(c),
            "change_pct": change_pct,
            "body_pct": body_pct,
            "upper_wick_pct": upper_wick_pct,
            "lower_wick_pct": lower_wick_pct,
            "candle_type": candle_type,
            "candle_pattern": candle_pattern,
            "ma5": int(ma5),
            "ma20": int(ma20),
            "ma60": int(ma60),
            "ma_status": ma_status,
        }

    def _detect_pattern(self, df: pd.DataFrame) -> str:
        """최근 2~3봉으로 캔들패턴 판정"""
        if len(df) < 3:
            return "none"

        r0 = df.iloc[-1]  # 오늘
        r1 = df.iloc[-2]  # 어제
        r2 = df.iloc[-3]  # 그제

        o0, c0 = float(r0["open"]), float(r0["close"])
        h0, l0 = float(r0["high"]), float(r0["low"])
        o1, c1 = float(r1["open"]), float(r1["close"])
        h1, l1 = float(r1["high"]), float(r1["low"])
        range0 = h0 - l0 if h0 != l0 else 1
        range1 = h1 - l1 if h1 != l1 else 1
        body0 = abs(c0 - o0)
        body1 = abs(c1 - o1)

        # 장악형 (Engulfing)
        if c1 < o1 and c0 > o0 and c0 > o1 and o0 < c1:
            return "bullish_engulfing"
        if c1 > o1 and c0 < o0 and c0 < o1 and o0 > c1:
            return "bearish_engulfing"

        # 망치형 (Hammer) — 하락 후 긴 아래꼬리
        lower_wick0 = (min(o0, c0) - l0)
        if lower_wick0 > body0 * 2 and (h0 - max(o0, c0)) < body0 * 0.3:
            if c1 < o1:  # 이전 음봉 후
                return "hammer"

        # 역망치 (Inverted Hammer)
        upper_wick0 = h0 - max(o0, c0)
        if upper_wick0 > body0 * 2 and (min(o0, c0) - l0) < body0 * 0.3:
            if c1 < o1:
                return "inverted_hammer"

        # 샛별형 (Morning Star) — 3봉 패턴
        o2, c2 = float(r2["open"]), float(r2["close"])
        body2 = abs(c2 - o2)
        if c2 < o2 and body1 < body2 * 0.3 and c0 > o0 and c0 > (o2 + c2) / 2:
            return "morning_star"

        # 석별형 (Evening Star)
        if c2 > o2 and body1 < body2 * 0.3 and c0 < o0 and c0 < (o2 + c2) / 2:
            return "evening_star"

        # 도지
        if body0 / range0 < 0.1:
            return "doji"

        return "none"

    def _empty_1d(self) -> dict:
        return {
            "close": 0, "change_pct": 0,
            "body_pct": 0, "upper_wick_pct": 0, "lower_wick_pct": 0,
            "candle_type": "unknown", "candle_pattern": "none",
            "ma5": 0, "ma20": 0, "ma60": 0, "ma_status": "unknown",
        }

    # ================================================================
    #  2D: 거래량 에너지 분석
    # ================================================================

    def _analyze_2d(self, day_df: pd.DataFrame, as_of: pd.Timestamp) -> dict:
        """2D 거래량에너지: 비율, 소진, 다이버전스"""
        df = day_df[day_df.index <= as_of]
        if len(df) < 20:
            return self._empty_2d()

        close = df["close"].astype(float)
        volume = df["volume"].astype(float)
        cur_vol = float(volume.iloc[-1])
        cur_close = float(close.iloc[-1])

        # 거래량/MA20 비율
        vol_ma20 = float(volume.rolling(20).mean().iloc[-1])
        vol_ratio = round(cur_vol / vol_ma20, 2) if vol_ma20 > 0 else 0

        # 거래량 추세 (최근 5일 평균 vs 이전 5일 평균)
        vol_recent5 = float(volume.iloc[-5:].mean())
        vol_prev5 = float(volume.iloc[-10:-5].mean()) if len(df) >= 10 else vol_recent5
        if vol_prev5 > 0:
            vol_change = (vol_recent5 - vol_prev5) / vol_prev5
        else:
            vol_change = 0

        if vol_ratio >= 3.0:
            vol_trend = "spike"
        elif vol_change > 0.3:
            vol_trend = "increasing"
        elif vol_change < -0.3:
            vol_trend = "decreasing"
        else:
            vol_trend = "stable"

        # 매물소진 시그널: 3일 연속 거래량 < MA20 * 0.5 + 가격 횡보
        recent3_vol = volume.iloc[-3:]
        if all(recent3_vol < vol_ma20 * 0.5):
            price_range_3d = (close.iloc[-3:].max() - close.iloc[-3:].min()) / cur_close * 100
            vol_exhaustion = "yes" if price_range_3d < 3 else "possible"
        else:
            vol_exhaustion = "no"

        # 거래량-가격 다이버전스
        # 가격 상승 + 거래량 감소 = bearish divergence
        # 가격 하락 + 거래량 감소 = seller exhaustion (bullish)
        price_chg_5d = (cur_close - float(close.iloc[-5])) / float(close.iloc[-5]) * 100 if len(df) >= 5 else 0
        if price_chg_5d > 2 and vol_change < -0.2:
            vol_price_div = "bearish_div"
        elif price_chg_5d < -2 and vol_change < -0.3:
            vol_price_div = "seller_exhaust"
        elif price_chg_5d < -2 and vol_change > 0.3:
            vol_price_div = "panic_sell"
        elif price_chg_5d > 2 and vol_change > 0.3:
            vol_price_div = "strong_buy"
        else:
            vol_price_div = "neutral"

        return {
            "volume": int(cur_vol),
            "vol_ratio_ma20": vol_ratio,
            "vol_trend": vol_trend,
            "vol_exhaustion": vol_exhaustion,
            "vol_price_div": vol_price_div,
        }

    def _empty_2d(self) -> dict:
        return {
            "volume": 0, "vol_ratio_ma20": 0, "vol_trend": "unknown",
            "vol_exhaustion": "unknown", "vol_price_div": "unknown",
        }

    # ================================================================
    #  3D: 수급 흐름 분석
    # ================================================================

    def _analyze_3d(self, code: str, as_of: pd.Timestamp) -> dict:
        """3D 수급: 기관/외인 순매수, 공매도"""
        flow_df = self._load_flow(code)
        short_df = self._load_short(code)

        inst_net_5d = 0.0
        foreign_net_5d = 0.0
        smart_net_5d = 0.0
        short_change_5d = 0.0

        if flow_df is not None and len(flow_df) >= 5:
            fdf = flow_df[flow_df.index <= as_of]
            if len(fdf) >= 5:
                if "기관_금액" in fdf.columns:
                    inst_net_5d = round(float(fdf["기관_금액"].iloc[-5:].sum()) / 1e8, 1)
                if "외국인_금액" in fdf.columns:
                    foreign_net_5d = round(float(fdf["외국인_금액"].iloc[-5:].sum()) / 1e8, 1)
                smart_net_5d = round(inst_net_5d + foreign_net_5d, 1)

        if short_df is not None and "비중" in short_df.columns:
            sdf = short_df[short_df.index <= as_of]
            if len(sdf) >= 5:
                short_change_5d = round(float(sdf["비중"].iloc[-1]) - float(sdf["비중"].iloc[-5]), 3)

        # 수급 등급 (간이)
        score = 0
        if inst_net_5d > 50:
            score += 30
        elif inst_net_5d > 10:
            score += 20
        elif inst_net_5d > 0:
            score += 10

        if foreign_net_5d > 50:
            score += 30
        elif foreign_net_5d > 10:
            score += 20
        elif foreign_net_5d > 0:
            score += 10

        if short_change_5d < -0.1:
            score += 20
        elif short_change_5d < 0:
            score += 10

        if score >= 65:
            grade = "A+"
        elif score >= 50:
            grade = "A"
        elif score >= 35:
            grade = "B"
        elif score >= 20:
            grade = "C"
        else:
            grade = "D"

        return {
            "inst_net_5d": inst_net_5d,
            "foreign_net_5d": foreign_net_5d,
            "smart_net_5d": smart_net_5d,
            "short_change_5d": short_change_5d,
            "supply_grade": grade,
            "supply_score": score,
        }

    # ================================================================
    #  4D: 모멘텀 분석
    # ================================================================

    def _analyze_4d(self, day_df: pd.DataFrame, as_of: pd.Timestamp) -> dict:
        """4D 모멘텀: RSI, MACD, 볼린저, MA교차"""
        df = day_df[day_df.index <= as_of]
        if len(df) < 60:
            return self._empty_4d()

        close = df["close"].astype(float)
        price = float(close.iloc[-1])

        # RSI 14
        rsi_s = IC.rsi(close, 14)
        rsi14 = round(float(rsi_s.iloc[-1]), 1) if not pd.isna(rsi_s.iloc[-1]) else 50.0

        # MACD
        macd_line, signal_line, hist = IC.macd(close)
        macd_hist = round(float(hist.iloc[-1]), 1) if not pd.isna(hist.iloc[-1]) else 0
        hist_prev = float(hist.iloc[-2]) if not pd.isna(hist.iloc[-2]) else 0

        # MACD 크로스
        macd_now = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else 0
        macd_prev = float(macd_line.iloc[-2]) if not pd.isna(macd_line.iloc[-2]) else 0
        sig_now = float(signal_line.iloc[-1]) if not pd.isna(signal_line.iloc[-1]) else 0
        sig_prev = float(signal_line.iloc[-2]) if not pd.isna(signal_line.iloc[-2]) else 0

        if macd_prev <= sig_prev and macd_now > sig_now:
            macd_cross = "golden_cross"
        elif macd_prev >= sig_prev and macd_now < sig_now:
            macd_cross = "dead_cross"
        else:
            macd_cross = "none"

        # 볼린저 위치
        upper, middle, lower = IC.bollinger_bands(close)
        u = float(upper.iloc[-1]) if not pd.isna(upper.iloc[-1]) else price
        l = float(lower.iloc[-1]) if not pd.isna(lower.iloc[-1]) else price
        bb_position = round((price - l) / (u - l), 3) if u != l else 0.5

        # 모멘텀 종합
        mom_score = 0
        # RSI
        if 45 <= rsi14 <= 65:
            mom_score += 25
        elif 35 <= rsi14 <= 75:
            mom_score += 15
        else:
            mom_score += 5

        # MACD
        if macd_now > 0 and macd_hist > hist_prev:
            mom_score += 25
        elif macd_now > 0:
            mom_score += 15
        elif macd_hist > hist_prev:
            mom_score += 10
        else:
            mom_score += 3

        # BB
        if 0.5 <= bb_position <= 0.85:
            mom_score += 25
        elif 0.3 <= bb_position <= 0.95:
            mom_score += 15
        else:
            mom_score += 5

        # MA cross
        if macd_cross == "golden_cross":
            mom_score += 25
        elif macd_cross == "dead_cross":
            mom_score += 0
        else:
            mom_score += 10

        if mom_score >= 75:
            momentum_signal = "strong_bullish"
        elif mom_score >= 55:
            momentum_signal = "bullish"
        elif mom_score >= 40:
            momentum_signal = "neutral"
        elif mom_score >= 25:
            momentum_signal = "bearish"
        else:
            momentum_signal = "strong_bearish"

        return {
            "rsi14": rsi14,
            "macd_hist": macd_hist,
            "macd_cross": macd_cross,
            "bb_position": bb_position,
            "momentum_signal": momentum_signal,
            "momentum_score": mom_score,
        }

    def _empty_4d(self) -> dict:
        return {
            "rsi14": 50, "macd_hist": 0, "macd_cross": "none",
            "bb_position": 0.5, "momentum_signal": "unknown", "momentum_score": 0,
        }

    # ================================================================
    #  종합 시그널 판정
    # ================================================================

    def _judge_signal(self, d1: dict, d2: dict, d3: dict, d4: dict) -> dict:
        """1D~4D 결과를 종합하여 최종 시그널 판정"""
        score = 0
        signals = []

        # 1D 가산
        if d1["candle_pattern"] in ("bullish_engulfing", "morning_star", "hammer"):
            score += 15
            signals.append(f"1D:{d1['candle_pattern']}")
        elif d1["candle_pattern"] in ("bearish_engulfing", "evening_star"):
            score -= 10
            signals.append(f"1D:{d1['candle_pattern']}")

        if d1["ma_status"] in ("perfect_bull", "bull"):
            score += 10
        elif d1["ma_status"] == "bear":
            score -= 10

        # 2D 가산
        if d2["vol_price_div"] == "seller_exhaust":
            score += 15
            signals.append("2D:seller_exhaust")
        elif d2["vol_price_div"] == "strong_buy":
            score += 10
            signals.append("2D:vol+price_up")
        elif d2["vol_price_div"] == "panic_sell":
            score -= 15
            signals.append("2D:panic_sell")

        if d2["vol_exhaustion"] == "yes":
            score += 10
            signals.append("2D:vol_dry")

        if d2["vol_trend"] == "spike":
            score += 5

        # 3D 가산
        if d3["supply_grade"] in ("A+", "A"):
            score += 15
        elif d3["supply_grade"] == "B":
            score += 5
        elif d3["supply_grade"] == "D":
            score -= 10

        if d3["smart_net_5d"] > 50:
            signals.append(f"3D:smart+{d3['smart_net_5d']:.0f}억")
        elif d3["smart_net_5d"] < -50:
            signals.append(f"3D:smart{d3['smart_net_5d']:.0f}억")

        # 4D 가산
        if d4["momentum_signal"] in ("strong_bullish", "bullish"):
            score += 10
        elif d4["momentum_signal"] in ("strong_bearish", "bearish"):
            score -= 10

        if d4["macd_cross"] == "golden_cross":
            score += 10
            signals.append("4D:MACD_GC")
        elif d4["macd_cross"] == "dead_cross":
            score -= 10
            signals.append("4D:MACD_DC")

        # 정규화 (0~100)
        confidence = max(0, min(100, score + 50))

        if confidence >= 75:
            signal = "STRONG_BUY"
        elif confidence >= 60:
            signal = "BUY"
        elif confidence >= 45:
            signal = "HOLD"
        elif confidence >= 30:
            signal = "CAUTION"
        else:
            signal = "SELL"

        key_signal = " | ".join(signals[:3]) if signals else "none"

        return {
            "signal": signal,
            "confidence": confidence,
            "key_signal": key_signal,
        }

    # ================================================================
    #  통합 분석
    # ================================================================

    def analyze_stock(self, code: str, as_of: str = None, name: str = "") -> Optional[dict]:
        """종목 1D~4D 통합 분석 → dict 레코드

        Args:
            code: 종목코드
            as_of: 기준일 (YYYYMMDD or YYYY-MM-DD), None이면 최신
            name: 종목명
        """
        day_df = self._load_daily(code)
        if day_df is None or len(day_df) < 60:
            return None

        if as_of:
            ts = pd.Timestamp(as_of)
        else:
            ts = day_df.index[-1]

        # 기준일이 데이터 범위 밖이면 None
        if ts > day_df.index[-1] or ts < day_df.index[0]:
            return None

        date_str = ts.strftime("%Y-%m-%d")

        d1 = self._analyze_1d(day_df, ts)
        d2 = self._analyze_2d(day_df, ts)
        d3 = self._analyze_3d(code, ts)
        d4 = self._analyze_4d(day_df, ts)
        judge = self._judge_signal(d1, d2, d3, d4)

        record = {"date": date_str, "code": code, "name": name}
        record.update(d1)
        record.update(d2)
        record.update(d3)
        record.update(d4)
        record.update(judge)

        return record

    # ================================================================
    #  일간 기록
    # ================================================================

    def record_daily(self, codes: list, names: dict = None, as_of: str = None) -> int:
        """전종목 일간 시그널 기록 → CSV 저장

        Args:
            codes: 종목코드 리스트
            names: {code: name} 매핑
            as_of: 기준일 (None이면 최신 거래일)

        Returns: 기록 성공 종목 수
        """
        _ensure_dirs()
        if names is None:
            names = {}

        records = []
        for i, code in enumerate(codes):
            name = names.get(code, "")
            rec = self.analyze_stock(code, as_of=as_of, name=name)
            if rec:
                records.append(rec)

            if (i + 1) % 50 == 0:
                logger.info(f"시그널 분석 진행: {i+1}/{len(codes)} ({len(records)}성공)")

        if not records:
            logger.warning("시그널 기록: 레코드 없음")
            return 0

        df = pd.DataFrame(records)

        # 일간 파일 저장
        date_str = records[0]["date"].replace("-", "")
        daily_path = SIGNAL_DIR / f"{date_str}.csv"
        df.to_csv(daily_path, index=False, encoding="utf-8-sig")
        logger.info(f"일간 시그널 저장: {daily_path} ({len(records)}종목)")

        # 종목별 히스토리 append
        for rec in records:
            code = rec["code"]
            hist_path = SIGNAL_HISTORY_DIR / f"{code}.csv"
            row_df = pd.DataFrame([rec])
            if hist_path.exists():
                existing = pd.read_csv(hist_path, dtype=str)
                # 같은 날짜 중복 제거
                existing = existing[existing["date"] != rec["date"]]
                combined = pd.concat([existing, row_df], ignore_index=True)
                combined.to_csv(hist_path, index=False, encoding="utf-8-sig")
            else:
                row_df.to_csv(hist_path, index=False, encoding="utf-8-sig")

        return len(records)

    # ================================================================
    #  백필
    # ================================================================

    def backfill(self, codes: list, start_date: str, end_date: str,
                 names: dict = None) -> dict:
        """과거 날짜별 시그널 백필

        Args:
            codes: 종목코드 리스트
            start_date: 시작일 YYYYMMDD
            end_date: 종료일 YYYYMMDD
            names: {code: name}

        Returns: {date_str: record_count}
        """
        _ensure_dirs()
        if names is None:
            names = {}

        # 거래일 목록 추출 (첫 번째 종목의 일봉 인덱스 사용)
        trading_days = []
        for code in codes:
            day_df = self._load_daily(code)
            if day_df is not None and len(day_df) > 60:
                start_ts = pd.Timestamp(start_date)
                end_ts = pd.Timestamp(end_date)
                mask = (day_df.index >= start_ts) & (day_df.index <= end_ts)
                trading_days = sorted(day_df.index[mask].tolist())
                break

        if not trading_days:
            logger.warning("백필: 거래일 없음")
            return {}

        logger.info(f"백필 시작: {len(codes)}종목 x {len(trading_days)}거래일")
        print(f"\n  백필: {len(codes)}종목 x {len(trading_days)}거래일"
              f" ({trading_days[0].strftime('%Y-%m-%d')} ~ {trading_days[-1].strftime('%Y-%m-%d')})")

        results = {}
        for di, day in enumerate(trading_days):
            date_str = day.strftime("%Y%m%d")
            daily_path = SIGNAL_DIR / f"{date_str}.csv"

            # 이미 존재하면 스킵
            if daily_path.exists():
                existing = pd.read_csv(daily_path)
                if len(existing) > len(codes) * 0.5:
                    results[date_str] = len(existing)
                    continue

            records = []
            for code in codes:
                name = names.get(code, "")
                rec = self.analyze_stock(code, as_of=date_str, name=name)
                if rec:
                    records.append(rec)

            if records:
                df = pd.DataFrame(records)
                df.to_csv(daily_path, index=False, encoding="utf-8-sig")

                # 종목별 히스토리에도 추가
                for rec in records:
                    code = rec["code"]
                    hist_path = SIGNAL_HISTORY_DIR / f"{code}.csv"
                    row_df = pd.DataFrame([rec])
                    if hist_path.exists():
                        existing = pd.read_csv(hist_path, dtype=str)
                        existing = existing[existing["date"] != rec["date"]]
                        combined = pd.concat([existing, row_df], ignore_index=True)
                        combined.to_csv(hist_path, index=False, encoding="utf-8-sig")
                    else:
                        row_df.to_csv(hist_path, index=False, encoding="utf-8-sig")

                results[date_str] = len(records)

            if (di + 1) % 10 == 0:
                print(f"  백필 진행: {di+1}/{len(trading_days)}일 ({len(records)}종목)", flush=True)

        total_records = sum(results.values())
        print(f"\n  백필 완료: {len(results)}거래일, 총 {total_records:,}레코드")
        logger.info(f"백필 완료: {len(results)}거래일, {total_records:,}레코드")

        return results

    # ================================================================
    #  리포트 (텔레그램/콘솔용)
    # ================================================================

    def format_report(self, record: dict) -> str:
        """단일 종목 시그널 리포트 텍스트"""
        lines = []
        name = record.get("name", record["code"])
        lines.append(f"[{name}({record['code']})] {record['date']}")
        lines.append(f"  종가: {record['close']:,}원 ({record['change_pct']:+.2f}%)")
        lines.append("")

        # 1D
        lines.append(f"[1D 가격구조]")
        lines.append(f"  봉: {record['candle_type']} | 몸통:{record['body_pct']}% "
                     f"윗꼬리:{record['upper_wick_pct']}% 아랫꼬리:{record['lower_wick_pct']}%")
        lines.append(f"  패턴: {record['candle_pattern']}")
        lines.append(f"  MA: {record['ma_status']} (5:{record['ma5']:,} 20:{record['ma20']:,} 60:{record['ma60']:,})")
        lines.append("")

        # 2D
        lines.append(f"[2D 거래량에너지]")
        lines.append(f"  거래량: {record['volume']:,} ({record['vol_ratio_ma20']:.1f}x MA20)")
        lines.append(f"  추세: {record['vol_trend']} | 소진: {record['vol_exhaustion']}")
        lines.append(f"  가격-거래량: {record['vol_price_div']}")
        lines.append("")

        # 3D
        lines.append(f"[3D 수급흐름]")
        lines.append(f"  기관: {record['inst_net_5d']:+.1f}억 | 외인: {record['foreign_net_5d']:+.1f}억"
                     f" | 스마트: {record['smart_net_5d']:+.1f}억")
        lines.append(f"  공매도 변화: {record['short_change_5d']:+.3f}% | 등급: {record['supply_grade']}({record['supply_score']})")
        lines.append("")

        # 4D
        lines.append(f"[4D 모멘텀]")
        lines.append(f"  RSI: {record['rsi14']:.1f} | MACD히스토: {record['macd_hist']:.1f}"
                     f" | MACD교차: {record['macd_cross']}")
        lines.append(f"  볼린저: {record['bb_position']:.1%} | 신호: {record['momentum_signal']}({record['momentum_score']})")
        lines.append("")

        # 종합
        lines.append(f"[종합 판정]")
        lines.append(f"  {record['signal']} (신뢰도: {record['confidence']})")
        if record["key_signal"] != "none":
            lines.append(f"  핵심: {record['key_signal']}")

        return "\n".join(lines)

    def format_daily_summary(self, date: str = None) -> str:
        """일간 시그널 요약 (BUY/STRONG_BUY 종목만)"""
        if date is None:
            # 최신 파일 찾기
            csvs = sorted(SIGNAL_DIR.glob("2*.csv"), reverse=True)
            if not csvs:
                return "시그널 기록 없음"
            path = csvs[0]
        else:
            path = SIGNAL_DIR / f"{date.replace('-', '')}.csv"

        if not path.exists():
            return f"{date} 시그널 기록 없음"

        df = pd.read_csv(path)
        date_str = df["date"].iloc[0] if len(df) > 0 else date

        lines = [f"[일간 시그널 요약] {date_str}", f"총 {len(df)}종목 분석", ""]

        # BUY 이상
        buys = df[df["signal"].isin(["STRONG_BUY", "BUY"])]
        if len(buys) > 0:
            lines.append(f"STRONG_BUY/BUY: {len(buys)}종목")
            for _, r in buys.iterrows():
                name = r.get("name", r["code"])
                lines.append(f"  {r['signal']} {name}({r['code']}) "
                            f"{r['close']:,.0f}원 ({r['change_pct']:+.2f}%) "
                            f"| {r.get('key_signal', '')}")
        else:
            lines.append("BUY 시그널 없음")

        # CAUTION/SELL
        cautions = df[df["signal"].isin(["CAUTION", "SELL"])]
        if len(cautions) > 0:
            lines.append(f"\nCAUTION/SELL: {len(cautions)}종목")
            for _, r in cautions.head(5).iterrows():
                name = r.get("name", r["code"])
                lines.append(f"  {r['signal']} {name}({r['code']}) "
                            f"{r['close']:,.0f}원 ({r['change_pct']:+.2f}%)")

        # 시그널 분포
        signal_counts = df["signal"].value_counts().to_dict()
        lines.append(f"\n분포: {' / '.join(f'{k}:{v}' for k, v in signal_counts.items())}")

        return "\n".join(lines)


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import sys
    import io
    sys.path.insert(0, str(BASE_DIR))
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    from data.universe_builder import get_universe_dict

    UNIVERSE = get_universe_dict()
    exclude = {"069500", "371160", "102780", "305720"}
    codes = [c for c in UNIVERSE.keys() if c not in exclude]
    names = {c: UNIVERSE[c][0] for c in codes if c in UNIVERSE}

    sa = SignalAnalyzer()

    if "--backfill" in sys.argv:
        # 전종목 백필: python -m data.signal_analyzer --backfill 20250301 20260219
        start = sys.argv[sys.argv.index("--backfill") + 1] if len(sys.argv) > sys.argv.index("--backfill") + 1 else "20250301"
        end = sys.argv[sys.argv.index("--backfill") + 2] if len(sys.argv) > sys.argv.index("--backfill") + 2 else datetime.now().strftime("%Y%m%d")
        sa.backfill(codes, start, end, names=names)

    elif "--stock" in sys.argv:
        # 개별 종목: python -m data.signal_analyzer --stock 005930
        code = sys.argv[sys.argv.index("--stock") + 1]
        name = names.get(code, code)
        rec = sa.analyze_stock(code, name=name)
        if rec:
            print(sa.format_report(rec))
        else:
            print(f"{code}: 데이터 부족")

    elif "--summary" in sys.argv:
        # 일간 요약: python -m data.signal_analyzer --summary
        print(sa.format_daily_summary())

    else:
        # 오늘 전종목 기록
        count = sa.record_daily(codes, names=names)
        print(f"\n기록 완료: {count}종목")
        print(sa.format_daily_summary())
