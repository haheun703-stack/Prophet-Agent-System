"""
통합 수급 분석기 — 3D 수급 점수 + 4D 모멘텀 (디스크법)

3D: 정적 수급 점수 (0~100) + 몸통/꼬리 판정
4D: 동적 수급 모멘텀 — 디스크 부피의 팽창/수축 속도

핵심 원리 (디스크법 비유):
  3D = 수급 등급 (디스크의 반지름 = 현재 두께)
  4D = 수급 가속도 (반지름의 변화율 = 팽창 중인가 수축 중인가)

  A+ ACC = 두껍고 더 팽창 중 -> 최우선 진입
  A  DEC = 두껍지만 수축 중 -> 함정 (SK텔레콤 패턴)
  B  ACC = 얇지만 팽창 중 -> 관찰 (DB손해보험 패턴)

사용법:
  from data.supply_analyzer import SupplyAnalyzer
  analyzer = SupplyAnalyzer()
  score = analyzer.analyze("005930")          # 3D
  momentum = analyzer.analyze_4d("005930")    # 4D
  full = analyzer.analyze_full("005930")      # 3D + 4D
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"
SHORT_DIR = DATA_DIR / "short"
DAILY_DIR = DATA_DIR / "daily"


@dataclass
class SupplyScore:
    """종목별 수급 분석 결과"""
    code: str
    date: str

    # 개별 점수 (각 0~25점, 합산 0~100)
    institutional_score: float     # 기관 순매수 추세 (0~25)
    foreign_score: float           # 외국인 순매수 추세 (0~25)
    short_score: float             # 공매도 감소 추세 (0~25)
    price_score: float             # 가격 모멘텀 (0~25)

    # 원시 데이터
    inst_net_5d: float = 0         # 기관 5일 누적 순매수 (억원)
    foreign_net_5d: float = 0      # 외국인 5일 누적 순매수 (억원)
    foreign_exhaustion: float = 0  # 외국인 소진율 (%)
    short_balance_pct: float = 0   # 공매도 비중 (%)
    short_change_5d: float = 0     # 공매도 5일 변화 (%)
    price_change_5d: float = 0     # 가격 5일 변화 (%)
    price_change_20d: float = 0    # 가격 20일 변화 (%)

    @property
    def total_score(self) -> float:
        return self.institutional_score + self.foreign_score + self.short_score + self.price_score

    @property
    def grade(self) -> str:
        s = self.total_score
        if s >= 80:
            return "A+"   # 몸통 진행 중 — 매수 적극
        elif s >= 65:
            return "A"    # 몸통 가능성 높음
        elif s >= 50:
            return "B"    # 중립~약 상승
        elif s >= 35:
            return "C"    # 주의 — 꼬리 가능
        else:
            return "D"    # 꼬리/하락 — 회피

    @property
    def is_body(self) -> bool:
        """몸통 구간 판정 (A 이상)"""
        return self.total_score >= 65

    @property
    def is_tail(self) -> bool:
        """꼬리 구간 판정 (C 이하)"""
        return self.total_score < 35

    def __str__(self):
        return (
            f"{self.code} [{self.grade}] {self.total_score:.0f}점 "
            f"(기관:{self.institutional_score:.0f} 외인:{self.foreign_score:.0f} "
            f"공매도:{self.short_score:.0f} 가격:{self.price_score:.0f}) "
            f"| 기관5일:{self.inst_net_5d:+.1f}억 외인5일:{self.foreign_net_5d:+.1f}억 "
            f"소진율:{self.foreign_exhaustion:.1f}% 공매도:{self.short_balance_pct:.2f}%"
        )


@dataclass
class SupplyMomentum:
    """4D 수급 모멘텀 — 디스크 부피의 변화율"""
    code: str
    date: str

    # ── 기관 연속 매수 (S-Oil 8일 연속 = 스텔스 매집) ──
    inst_streak: int = 0             # 기관 연속 순매수일 (양수=매수, 음수=매도)
    inst_streak_amount: float = 0    # 연속 기간 누적 금액 (억원)

    # ── 외인 소진율 변곡점 (2차 미분) ──
    foreign_exh_delta: float = 0     # 소진율 1차 미분 (5일 변화)
    foreign_exh_accel: float = 0     # 소진율 2차 미분 (가속도)
    foreign_inflection: str = ""     # UP_TURN / DOWN_TURN / NONE

    # ── 개인 역지표 (개인 매도 + 기관외인 매수 = 불리시) ──
    retail_contrarian: bool = False  # 개인 역지표 발동 여부
    retail_net_5d: float = 0         # 개인 5일 순매수 (억원)
    smart_net_5d: float = 0          # 기관+외인 5일 순매수 (억원)

    # ── 수급 가속도 (이번주 vs 지난주) ──
    supply_accel: float = 0          # (최근5일 수급 - 이전5일 수급) / |이전5일| * 100
    supply_trend: str = ""           # ACCELERATING / STEADY / DECELERATING

    # ── 종합 4D 점수 (0~100) ──
    momentum_score: float = 0

    @property
    def signal(self) -> str:
        """4D 모멘텀 신호"""
        if self.momentum_score >= 70:
            return "ACC"     # ACCELERATING — 디스크 팽창 중
        elif self.momentum_score >= 40:
            return "STEADY"  # STEADY — 유지
        else:
            return "DEC"     # DECELERATING — 디스크 수축 중

    def __str__(self):
        return (
            f"{self.code} [4D:{self.signal}] {self.momentum_score:.0f}점 "
            f"기관연속:{self.inst_streak:+d}일({self.inst_streak_amount:+.0f}억) "
            f"외인변곡:{self.foreign_inflection}({self.foreign_exh_accel:+.3f}) "
            f"역지표:{'O' if self.retail_contrarian else 'X'} "
            f"가속도:{self.supply_accel:+.1f}%"
        )


@dataclass
class SupplyStability:
    """5D 사냥 에너지 — 디스크의 운동에너지 (5% 몸통 사냥 적합도)

    3D = 반지름 (등급), 4D = 팽창속도 (모멘텀), 5D = 에너지 (사냥 적합도)

    핵심 철학: 안 움직이는 주식은 쓸모없다. 움직이면서 돈이 들어오는 걸 잡아라!
      변동성 높고 + 스마트머니 밀고 + 유동성 있고 + 신호 명확 = 최적 사냥감
    """
    code: str
    date: str

    # 개별 점수 (각 0~25점, 합산 0~100)
    volatility_score: float      # 스윙 잠재력 (ATR% 높을수록 좋음!)
    liquidity_score: float       # 유동성 (진입/퇴출 가능)
    intensity_score: float       # 스마트머니 강도 (기관 매수 / 거래대금 비율)
    alignment_score: float       # 신호 일치도 (4D 구성요소 일치 수)

    # 원시 데이터
    atr_pct: float = 0.0              # 20일 ATR / 종가 (%)
    avg_trading_value: float = 0.0    # 20일 평균 거래대금 (억원)
    smart_money_ratio: float = 0.0    # 기관5일순매수 / 거래대금 비율 (%)
    signal_count: int = 0             # 일치 신호 수 (0~4)

    @property
    def stability_score(self) -> float:
        """총 에너지 점수 (0~100) — 호환성을 위해 이름 유지"""
        return self.volatility_score + self.liquidity_score + self.intensity_score + self.alignment_score

    @property
    def stability_grade(self) -> str:
        """에너지 등급 — 호환성을 위해 이름 유지"""
        s = self.stability_score
        if s >= 80:
            return "EXPLOSIVE"   # 폭발적 — 5% 사냥 최적
        elif s >= 60:
            return "HUNTABLE"    # 사냥감 — 추적 가치
        elif s >= 40:
            return "MODERATE"    # 보통
        else:
            return "SLUGGISH"    # 둔감 — 움직임 부족

    def __str__(self):
        return (
            f"{self.code} [5D:{self.stability_grade}] {self.stability_score:.0f}점 "
            f"(스윙:{self.volatility_score:.0f} 유동:{self.liquidity_score:.0f} "
            f"강도:{self.intensity_score:.0f} 일치:{self.alignment_score:.0f}) "
            f"| ATR%:{self.atr_pct:.2f}% 거래대금:{self.avg_trading_value:.0f}억 "
            f"SM비율:{self.smart_money_ratio:.1f}% 신호:{self.signal_count}/4"
        )


@dataclass
class TechHealth:
    """6D 기술건강도 — 일봉 기술적 상태 (가격 기반 지표)

    3D~5D = 수급 기반, 6D = 가격/기술 기반
    MA정배열 + RSI구간 + MACD방향 + 볼린저위치 + 거래량추세 + MA교차
    """
    code: str

    # 개별 점수
    ma_score: float       # MA 정배열 (25점)
    rsi_score: float      # RSI 적정구간 (20점)
    macd_score: float     # MACD 방향 (20점)
    bb_score: float       # 볼린저 위치 (15점)
    volume_score: float   # 거래량 추세 (10점)
    cross_score: float    # MA 교차 (10점)

    # 원시 값
    rsi_value: float = 0.0
    bb_position: float = 0.0
    vol_ratio: float = 0.0
    ma_status: str = ""

    @property
    def tech_score(self) -> float:
        return self.ma_score + self.rsi_score + self.macd_score + self.bb_score + self.volume_score + self.cross_score

    @property
    def tech_grade(self) -> str:
        s = self.tech_score
        if s >= 80:
            return "S"
        elif s >= 65:
            return "A"
        elif s >= 50:
            return "B"
        else:
            return "C"

    @property
    def rsi_zone(self) -> str:
        if self.rsi_value >= 70:
            return "과매수"
        elif self.rsi_value <= 30:
            return "과매도"
        return "적정"

    def __str__(self):
        return (
            f"[6D:{self.tech_grade}] {self.tech_score:.0f}점 "
            f"MA:{self.ma_status} RSI:{self.rsi_value:.0f}({self.rsi_zone}) "
            f"MACD:{self.macd_score:.0f} BB:{self.bb_position:.0%}"
        )


@dataclass
class SupplyFull:
    """3D + 4D + 5D 통합 수급 판정"""
    score: SupplyScore                          # 3D 정적 등급
    momentum: SupplyMomentum                    # 4D 동적 모멘텀
    stability: Optional[SupplyStability] = None  # 5D 안정성
    valuation_warning: Optional[str] = None      # 밸류에이션 경고 (적자/고PER/저PBR)
    per: float = 0.0
    pbr: float = 0.0
    tech_health: Optional['TechHealth'] = None   # 6D 기술건강도
    news_score: float = 0.0                       # 뉴스 감성점수 (-10~+10)
    news_summary: str = ""                        # 뉴스 한줄 요약

    @property
    def grade_3d(self) -> str:
        return self.score.grade

    @property
    def signal_4d(self) -> str:
        return self.momentum.signal

    @property
    def stability_grade(self) -> str:
        if self.stability is None:
            return "UNKNOWN"
        return self.stability.stability_grade

    @property
    def action(self) -> str:
        """3D + 4D 조합 진입 판정"""
        g = self.score.grade
        s = self.momentum.signal

        # A+ + ACC = 최우선
        if g == "A+" and s == "ACC":
            return "STRONG_BUY"
        # A+/A + ACC = 우선
        if g in ("A+", "A") and s == "ACC":
            return "BUY"
        # A+/A + STEADY = 일반 진입
        if g in ("A+", "A") and s == "STEADY":
            return "ENTER"
        # A + DEC = 함정 가능성 (SK텔레콤 패턴)
        if g in ("A+", "A") and s == "DEC":
            return "CAUTION"
        # B + ACC = 관찰 (DB손해보험 패턴)
        if g == "B" and s == "ACC":
            return "WATCH"
        # 나머지
        return "SKIP"

    @property
    def risk_label(self) -> str:
        """action + 5D 에너지 조합 사냥 라벨"""
        action = self.action
        if self.stability is None:
            label = action
        else:
            grade = self.stability.stability_grade
            if grade == "EXPLOSIVE":
                label = f"EXPLOSIVE {action}"   # 폭발적 사냥감!
            elif grade == "HUNTABLE":
                label = f"HUNT {action}"        # 추적 가치
            elif grade == "SLUGGISH":
                label = f"SLOW {action}"        # 느림보
            else:
                label = action  # MODERATE → 접두사 없음

        # 밸류에이션 경고 태그 (점수 불변, 라벨만)
        if self.valuation_warning:
            label += f" [{self.valuation_warning}]"

        # 6D 기술건강도 태그
        if self.tech_health:
            tg = self.tech_health.tech_grade
            if tg in ("S", "A"):
                label += f" [기술{tg}]"
            elif tg == "C":
                label += " [기술약]"
        return label

    @property
    def disk_thickness(self) -> str:
        """디스크 부피 비유 — 두께 판정"""
        a = self.action
        if a == "STRONG_BUY":
            return "THICK_EXPANDING"     # 두껍고 팽창 중
        elif a == "BUY":
            return "THICK_GROWING"       # 두껍고 성장 중
        elif a == "ENTER":
            return "THICK_STABLE"        # 두껍고 안정
        elif a == "CAUTION":
            return "THICK_SHRINKING"     # 두껍지만 수축 중 (함정!)
        elif a == "WATCH":
            return "THIN_EXPANDING"      # 얇지만 팽창 중
        else:
            return "HOLLOW"              # 속 비어있음

    def __str__(self):
        stab = ""
        if self.stability:
            stab = f" [5D:{self.stability_grade} {self.stability.stability_score:.0f}]"
        tech = ""
        if self.tech_health:
            tech = f" [6D:{self.tech_health.tech_grade} {self.tech_health.tech_score:.0f}]"
        return (
            f"{self.score.code} "
            f"[3D:{self.grade_3d} {self.score.total_score:.0f}] "
            f"[4D:{self.signal_4d} {self.momentum.momentum_score:.0f}]"
            f"{stab}{tech} "
            f"=> {self.risk_label} ({self.disk_thickness})"
        )


class SupplyAnalyzer:
    """통합 수급 분석기 — 3D 정적 + 4D 동적"""

    def __init__(self):
        self._cache_investor: Dict[str, pd.DataFrame] = {}
        self._cache_foreign: Dict[str, pd.DataFrame] = {}
        self._cache_short: Dict[str, pd.DataFrame] = {}
        self._cache_daily: Dict[str, pd.DataFrame] = {}

    def _load(self, code: str):
        """캐시된 CSV 데이터 로드"""
        if code not in self._cache_investor:
            path = FLOW_DIR / f"{code}_investor.csv"
            if path.exists():
                self._cache_investor[code] = pd.read_csv(path, index_col=0, parse_dates=True)

        if code not in self._cache_foreign:
            path = FLOW_DIR / f"{code}_foreign_exh.csv"
            if path.exists():
                self._cache_foreign[code] = pd.read_csv(path, index_col=0, parse_dates=True)

        if code not in self._cache_short:
            path = SHORT_DIR / f"{code}_short_bal.csv"
            if path.exists():
                self._cache_short[code] = pd.read_csv(path, index_col=0, parse_dates=True)

        if code not in self._cache_daily:
            path = DAILY_DIR / f"{code}.csv"
            if path.exists():
                df = pd.read_csv(path, index_col=0, parse_dates=True)
                # pykrx 한글 컬럼 → 영문 컬럼 매핑
                col_map = {"시가": "open", "고가": "high", "저가": "low",
                           "종가": "close", "거래량": "volume", "등락률": "change_pct"}
                df.rename(columns=col_map, inplace=True)
                self._cache_daily[code] = df

    def analyze(self, code: str, as_of: str = None) -> Optional[SupplyScore]:
        """종목 수급 분석

        Args:
            code: 종목코드
            as_of: 기준일 (None이면 최신)

        Returns:
            SupplyScore 또는 None
        """
        self._load(code)

        inv_df = self._cache_investor.get(code)
        for_df = self._cache_foreign.get(code)
        sht_df = self._cache_short.get(code)
        day_df = self._cache_daily.get(code)

        if inv_df is None or len(inv_df) < 5:
            return None

        # 기준일 슬라이싱
        if as_of:
            ts = pd.Timestamp(as_of)
            inv_df = inv_df[inv_df.index <= ts]
            if for_df is not None:
                for_df = for_df[for_df.index <= ts]
            if sht_df is not None:
                sht_df = sht_df[sht_df.index <= ts]
            if day_df is not None:
                day_df = day_df[day_df.index <= ts]

        if len(inv_df) < 5:
            return None

        date_str = str(inv_df.index[-1].date())

        # ── 1. 기관 점수 (0~25) ─────────────────────────
        inst_score, inst_net_5d = self._score_institutional(inv_df)

        # ── 2. 외국인 점수 (0~25) ────────────────────────
        foreign_score, foreign_net_5d, foreign_exh = self._score_foreign(inv_df, for_df)

        # ── 3. 공매도 점수 (0~25) ────────────────────────
        short_score, short_pct, short_chg = self._score_short(sht_df)

        # ── 4. 가격 모멘텀 점수 (0~25) ───────────────────
        price_score, pchg_5d, pchg_20d = self._score_price(day_df)

        return SupplyScore(
            code=code,
            date=date_str,
            institutional_score=inst_score,
            foreign_score=foreign_score,
            short_score=short_score,
            price_score=price_score,
            inst_net_5d=inst_net_5d,
            foreign_net_5d=foreign_net_5d,
            foreign_exhaustion=foreign_exh,
            short_balance_pct=short_pct,
            short_change_5d=short_chg,
            price_change_5d=pchg_5d,
            price_change_20d=pchg_20d,
        )

    def _score_institutional(self, inv_df: pd.DataFrame) -> tuple:
        """기관 순매수 점수"""
        col = "기관_금액"
        if col not in inv_df.columns:
            return 0.0, 0.0

        # 최근 5일 누적 (억원 단위)
        net_5d = float(inv_df[col].iloc[-5:].sum()) / 1e8

        # 최근 5일 중 순매수일 비율
        buy_days = (inv_df[col].iloc[-5:] > 0).sum()

        # 20일 대비 최근 5일 강도
        net_20d = float(inv_df[col].iloc[-20:].sum()) / 1e8 if len(inv_df) >= 20 else net_5d

        # 점수 산출
        score = 0.0

        # 순매수 규모 (0~15)
        if net_5d > 100:
            score += 15
        elif net_5d > 50:
            score += 12
        elif net_5d > 10:
            score += 8
        elif net_5d > 0:
            score += 4

        # 매수 연속성 (0~5)
        score += buy_days

        # 20일 추세 가속 (0~5)
        if net_20d != 0 and net_5d > 0:
            accel = net_5d / max(abs(net_20d), 1) * 4
            score += min(5, max(0, accel))

        return min(25, score), net_5d

    def _score_foreign(self, inv_df, for_df) -> tuple:
        """외국인 순매수 + 소진율 점수"""
        col = "외국인_금액"
        net_5d = 0.0
        foreign_exh = 0.0

        if col in inv_df.columns:
            net_5d = float(inv_df[col].iloc[-5:].sum()) / 1e8

        if for_df is not None and len(for_df) > 0 and "소진율" in for_df.columns:
            foreign_exh = float(for_df["소진율"].iloc[-1])

        score = 0.0

        # 순매수 규모 (0~12)
        if net_5d > 200:
            score += 12
        elif net_5d > 50:
            score += 9
        elif net_5d > 10:
            score += 6
        elif net_5d > 0:
            score += 3

        # 소진율 변화 (0~8) — 증가 추세면 가산
        if for_df is not None and len(for_df) >= 5 and "소진율" in for_df.columns:
            exh_now = float(for_df["소진율"].iloc[-1])
            exh_5d_ago = float(for_df["소진율"].iloc[-5])
            exh_delta = exh_now - exh_5d_ago
            if exh_delta > 0.5:
                score += 8
            elif exh_delta > 0.2:
                score += 5
            elif exh_delta > 0:
                score += 2

        # 매수 연속성 (0~5)
        if col in inv_df.columns:
            buy_days = (inv_df[col].iloc[-5:] > 0).sum()
            score += buy_days

        return min(25, score), net_5d, foreign_exh

    def _score_short(self, sht_df) -> tuple:
        """공매도 점수 (잔고 감소 = 긍정)"""
        if sht_df is None or len(sht_df) < 5 or "비중" not in sht_df.columns:
            return 12.5, 0.0, 0.0  # 데이터 없으면 중립

        short_pct = float(sht_df["비중"].iloc[-1])
        short_5d_ago = float(sht_df["비중"].iloc[-5]) if len(sht_df) >= 5 else short_pct
        short_chg = short_pct - short_5d_ago

        score = 0.0

        # 공매도 비중 자체 (낮을수록 좋음, 0~10)
        if short_pct < 0.5:
            score += 10
        elif short_pct < 1.0:
            score += 7
        elif short_pct < 2.0:
            score += 4
        elif short_pct < 5.0:
            score += 2

        # 공매도 감소 추세 (0~10) — 숏커버 = 상승 연료
        if short_chg < -0.5:
            score += 10      # 급감 — 숏커버 진행
        elif short_chg < -0.1:
            score += 7
        elif short_chg < 0:
            score += 4
        elif short_chg < 0.1:
            score += 2        # 보합
        # 증가면 0점

        # 절대 잔고가 높은데 가격도 오르면 숏스퀴즈 가능 (0~5)
        if short_pct > 2.0 and short_chg < 0:
            score += 5

        return min(25, score), short_pct, short_chg

    def _score_price(self, day_df) -> tuple:
        """가격 모멘텀 점수"""
        if day_df is None or len(day_df) < 20:
            return 12.5, 0.0, 0.0

        close = day_df["close"]
        pchg_5d = (float(close.iloc[-1]) - float(close.iloc[-5])) / float(close.iloc[-5]) * 100
        pchg_20d = (float(close.iloc[-1]) - float(close.iloc[-20])) / float(close.iloc[-20]) * 100

        score = 0.0

        # 5일 모멘텀 (0~12)
        if pchg_5d > 5:
            score += 12
        elif pchg_5d > 2:
            score += 9
        elif pchg_5d > 0:
            score += 5
        elif pchg_5d > -2:
            score += 2

        # 20일 모멘텀 (0~8)
        if pchg_20d > 10:
            score += 8
        elif pchg_20d > 5:
            score += 6
        elif pchg_20d > 0:
            score += 3

        # 20일선 위에 있는지 (0~5)
        ma20 = float(close.rolling(20).mean().iloc[-1])
        if float(close.iloc[-1]) > ma20:
            score += 5

        return min(25, score), pchg_5d, pchg_20d

    # ── 4D 모멘텀 분석 ─────────────────────────────────

    def analyze_4d(self, code: str, as_of: str = None) -> Optional[SupplyMomentum]:
        """4D 수급 모멘텀 분석 — 디스크 부피의 변화율"""
        self._load(code)

        inv_df = self._cache_investor.get(code)
        for_df = self._cache_foreign.get(code)

        if inv_df is None or len(inv_df) < 10:
            return None

        # 기준일 슬라이싱
        if as_of:
            ts = pd.Timestamp(as_of)
            inv_df = inv_df[inv_df.index <= ts]
            if for_df is not None:
                for_df = for_df[for_df.index <= ts]

        if len(inv_df) < 10:
            return None

        date_str = str(inv_df.index[-1].date())
        total_4d = 0.0

        # ── 1. 기관 연속 매수일 (0~30점) ──────────────
        streak, streak_amt = self._calc_inst_streak(inv_df)

        streak_score = 0.0
        abs_streak = abs(streak)
        if abs_streak >= 8:
            streak_score = 30     # S-Oil급 8일 연속
        elif abs_streak >= 5:
            streak_score = 22
        elif abs_streak >= 3:
            streak_score = 14
        elif abs_streak >= 2:
            streak_score = 8
        # 매도 연속이면 감점
        if streak < 0:
            streak_score = -streak_score * 0.5
        total_4d += streak_score

        # ── 2. 외인 소진율 변곡점 (0~25점) ────────────
        exh_delta, exh_accel, inflection = self._calc_foreign_inflection(for_df)

        inflection_score = 0.0
        if inflection == "UP_TURN":
            # 2차 미분 양수 = 하락→상승 전환 (SK이노 패턴)
            inflection_score = min(25, abs(exh_accel) * 50 + 10)
        elif inflection == "DOWN_TURN":
            # 2차 미분 음수 = 상승→하락 전환 (SK텔레콤 패턴)
            inflection_score = max(-15, -(abs(exh_accel) * 50 + 5))
        else:
            # 변곡 없음 — 방향에 따라 소폭 가감
            if exh_delta > 0:
                inflection_score = min(10, exh_delta * 20)
            else:
                inflection_score = max(-10, exh_delta * 20)
        total_4d += inflection_score

        # ── 3. 개인 역지표 (0~20점) ───────────────────
        retail_contra, retail_net, smart_net = self._calc_retail_contrarian(inv_df)

        contra_score = 0.0
        if retail_contra:
            # 개인 매도 + 스마트머니 매수 = 강한 불리시
            intensity = min(abs(retail_net), abs(smart_net))
            if intensity > 100:
                contra_score = 20
            elif intensity > 50:
                contra_score = 15
            elif intensity > 10:
                contra_score = 10
            else:
                contra_score = 5
        total_4d += contra_score

        # ── 4. 수급 가속도 (0~25점) ───────────────────
        accel_pct, trend = self._calc_supply_acceleration(inv_df)

        accel_score = 0.0
        if trend == "ACCELERATING":
            accel_score = min(25, accel_pct / 4)   # 100% 가속 = 25점
        elif trend == "STEADY":
            accel_score = 10
        else:
            accel_score = max(0, 5 + accel_pct / 10)  # 감속도 약간 점수
        total_4d += accel_score

        # 총점 정규화 (0~100)
        momentum_score = max(0, min(100, total_4d))

        return SupplyMomentum(
            code=code,
            date=date_str,
            inst_streak=streak,
            inst_streak_amount=streak_amt,
            foreign_exh_delta=exh_delta,
            foreign_exh_accel=exh_accel,
            foreign_inflection=inflection,
            retail_contrarian=retail_contra,
            retail_net_5d=retail_net,
            smart_net_5d=smart_net,
            supply_accel=accel_pct,
            supply_trend=trend,
            momentum_score=momentum_score,
        )

    # ============================================================
    #  5D 안정성 분석 — 디스크 밀도
    # ============================================================

    def analyze_5d(self, code: str, as_of: str = None,
                   momentum: 'SupplyMomentum' = None) -> Optional[SupplyStability]:
        """5D 사냥 에너지 분석 — 같은 BUY라도 사냥 적합도 차이를 구분

        Args:
            momentum: 4D 모멘텀 결과 (신호 일치도 계산에 필요)
        """
        self._load(code)

        day_df = self._cache_daily.get(code)
        inv_df = self._cache_investor.get(code)

        if day_df is None or len(day_df) < 20:
            return None

        if as_of:
            ts = pd.Timestamp(as_of)
            day_df = day_df[day_df.index <= ts]
            if inv_df is not None:
                inv_df = inv_df[inv_df.index <= ts]

        if len(day_df) < 20:
            return None

        date_str = str(day_df.index[-1].date())

        # 1) 스윙 잠재력 (ATR% 높을수록 좋음)
        vol_score, atr_pct = self._score_volatility(day_df)
        # 2) 유동성 (거래대금)
        liq_score, avg_value = self._score_liquidity(day_df)
        # 3) 스마트머니 강도 (기관매수/거래대금 비율)
        int_score, sm_ratio = self._score_intensity(inv_df, avg_value)
        # 4) 신호 일치도 (4D 구성요소 일치 수)
        if momentum is not None:
            ali_score, sig_count = self._score_alignment(momentum)
        else:
            ali_score, sig_count = 12.5, 0  # 기본값

        return SupplyStability(
            code=code,
            date=date_str,
            volatility_score=vol_score,
            liquidity_score=liq_score,
            intensity_score=int_score,
            alignment_score=ali_score,
            atr_pct=atr_pct,
            avg_trading_value=avg_value,
            smart_money_ratio=sm_ratio,
            signal_count=sig_count,
        )

    def _score_volatility(self, day_df: pd.DataFrame) -> Tuple[float, float]:
        """스윙 잠재력 — 20일 ATR% (높을수록 5% 사냥 가능 = 높은 점수!)"""
        recent = day_df.iloc[-20:]
        high = recent["high"]
        low = recent["low"]
        close = recent["close"]

        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr_20 = float(tr.mean())
        current_price = float(close.iloc[-1])

        if current_price <= 0:
            return 12.5, 0.0

        atr_pct = (atr_20 / current_price) * 100

        # 높을수록 좋음! (기존과 반대)
        if atr_pct > 6.0:
            score = 25.0     # 에코프로급 — 하루 5%+ 가능
        elif atr_pct > 4.5:
            score = 20.0     # 중형주 변동성
        elif atr_pct > 3.5:
            score = 15.0     # 대형주 변동성
        elif atr_pct > 2.0:
            score = 10.0     # 방어주 수준
        else:
            score = 5.0      # 삼전/은행주 — 5%는 안 나옴

        return score, atr_pct

    def _score_liquidity(self, day_df: pd.DataFrame) -> Tuple[float, float]:
        """유동성 — 20일 평균 거래대금 (억원, 진입/퇴출 가능해야 함)"""
        recent = day_df.iloc[-20:]
        trading_value = recent["close"] * recent["volume"]
        avg_value = float(trading_value.mean()) / 1e8  # 억원

        if avg_value > 1000:
            score = 25.0
        elif avg_value > 500:
            score = 20.0
        elif avg_value > 200:
            score = 15.0
        elif avg_value > 50:
            score = 10.0
        else:
            score = 5.0

        return score, avg_value

    def _score_intensity(self, inv_df: Optional[pd.DataFrame],
                         avg_trading_value: float) -> Tuple[float, float]:
        """스마트머니 강도 — 기관5일순매수 / 거래대금 비율

        기관이 거래의 몇 %를 차지하는가 → 비율 높을수록 강한 의지
        """
        if inv_df is None or len(inv_df) < 5 or avg_trading_value <= 0:
            return 12.5, 0.0

        col = "기관_금액"
        if col not in inv_df.columns:
            return 12.5, 0.0

        inst_5d = float(inv_df[col].iloc[-5:].sum()) / 1e8  # 억원
        ratio = abs(inst_5d) / (avg_trading_value * 5) * 100  # %

        # 매수 방향일 때만 높은 점수
        if inst_5d > 0:
            if ratio > 10:
                score = 25.0     # 거래의 10%+ 기관 매수 = 강한 의지
            elif ratio > 5:
                score = 20.0
            elif ratio > 2:
                score = 15.0
            elif ratio > 0.5:
                score = 10.0
            else:
                score = 5.0
        else:
            # 기관이 매도 중이면 낮은 점수
            score = max(0, 5 - ratio)

        return score, ratio if inst_5d > 0 else -ratio

    def _score_alignment(self, momentum: 'SupplyMomentum') -> Tuple[float, int]:
        """신호 일치도 — 4D 구성요소가 몇 개나 같은 방향인가

        기관매수 + 외인유입 + 개인매도(역지표) + 가속 = 4개 모두 일치 → 명확한 신호
        """
        count = 0
        if momentum.inst_streak > 0:
            count += 1
        if momentum.foreign_inflection == "UP_TURN":
            count += 1
        if momentum.retail_contrarian:
            count += 1
        if momentum.supply_accel > 0:
            count += 1

        score = count * 6.25  # 4개 만점 = 25

        return score, count

    # ============================================================
    #  6D 기술건강도 — 일봉 기술 지표 기반
    # ============================================================

    def analyze_6d(self, code: str, day_df: pd.DataFrame = None) -> Optional[TechHealth]:
        """6D 기술건강도 — 일봉 OHLCV에서 기술 지표 계산

        MA정배열(25) + RSI(20) + MACD(20) + 볼린저(15) + 거래량(10) + MA교차(10) = 100점
        """
        if day_df is None:
            self._load(code)
            day_df = self._cache_daily.get(code)

        if day_df is None or len(day_df) < 60:
            return None

        from data.indicator_calc import IndicatorCalc as IC

        close = day_df["close"].astype(float)
        volume = day_df["volume"].astype(float)
        price = float(close.iloc[-1])

        # ── 1. MA 정배열 (25점) ──
        ma5 = float(IC.sma(close, 5).iloc[-1])
        ma20 = float(IC.sma(close, 20).iloc[-1])
        ma60 = float(IC.sma(close, 60).iloc[-1])

        if price > ma5 > ma20 > ma60:
            ma_score, ma_status = 25, "정배열"
        elif price > ma20 > ma60:
            ma_score, ma_status = 18, "부분정배열"
        elif ma5 > ma20 > ma60:
            ma_score, ma_status = 15, "부분정배열"
        elif price > ma20:
            ma_score, ma_status = 10, "중립"
        elif price > ma60:
            ma_score, ma_status = 5, "하락"
        else:
            ma_score, ma_status = 0, "역배열"

        # ── 2. RSI 적정구간 (20점) ──
        rsi_series = IC.rsi(close, 14)
        rsi = float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else 50.0

        if 45 <= rsi <= 65:
            rsi_score = 20      # 최적 구간
        elif 40 <= rsi <= 70:
            rsi_score = 15      # 양호
        elif 35 <= rsi <= 75:
            rsi_score = 10      # 보통
        elif rsi > 75:
            rsi_score = 3       # 과매수 위험
        else:
            rsi_score = 2       # 과매도

        # ── 3. MACD 방향 (20점) ──
        macd_line, signal_line, hist = IC.macd(close)
        hist_now = float(hist.iloc[-1]) if not pd.isna(hist.iloc[-1]) else 0
        hist_prev = float(hist.iloc[-2]) if not pd.isna(hist.iloc[-2]) else 0
        macd_val = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else 0

        if macd_val > 0 and hist_now > hist_prev:
            macd_score = 20     # 상승 + 히스토그램 확대
        elif macd_val > 0:
            macd_score = 14     # 상승 유지
        elif hist_now > hist_prev:
            macd_score = 10     # 하락이지만 수렴 중 (골든크로스 임박)
        elif hist_now > 0:
            macd_score = 8      # 히스토그램 양수
        else:
            macd_score = 3      # 하락 + 확대

        # ── 4. 볼린저 위치 (15점) ──
        upper, middle, lower = IC.bollinger_bands(close)
        u = float(upper.iloc[-1]) if not pd.isna(upper.iloc[-1]) else price
        l = float(lower.iloc[-1]) if not pd.isna(lower.iloc[-1]) else price
        bb_pos = (price - l) / (u - l) if u != l else 0.5

        if 0.6 <= bb_pos <= 0.85:
            bb_score = 15       # 상승 모멘텀 구간
        elif 0.45 <= bb_pos <= 0.95:
            bb_score = 10       # 중상단
        elif 0.3 <= bb_pos <= 0.6:
            bb_score = 7        # 중간
        elif bb_pos > 0.95:
            bb_score = 3        # 과열 위험
        else:
            bb_score = 2        # 하단

        # ── 5. 거래량 추세 (10점) ──
        vol_r = float(IC.volume_ratio(volume).iloc[-1]) if not pd.isna(IC.volume_ratio(volume).iloc[-1]) else 1.0

        if vol_r >= 2.0:
            vol_score = 10      # 거래량 급증
        elif vol_r >= 1.5:
            vol_score = 8
        elif vol_r >= 1.2:
            vol_score = 6
        elif vol_r >= 0.8:
            vol_score = 4
        else:
            vol_score = 2       # 거래량 감소

        # ── 6. MA 교차 신호 (10점) ──
        fast_ma = IC.sma(close, 5)
        slow_ma = IC.sma(close, 20)
        cross = IC.ma_crossover_signal(fast_ma, slow_ma)

        if cross == "golden_cross":
            cross_score = 10    # 골든크로스!
        elif cross == "dead_cross":
            cross_score = 0     # 데드크로스
        else:
            cross_score = 5 if ma_score >= 15 else 3  # 정배열이면 보너스

        return TechHealth(
            code=code,
            ma_score=ma_score, rsi_score=rsi_score,
            macd_score=macd_score, bb_score=bb_score,
            volume_score=vol_score, cross_score=cross_score,
            rsi_value=rsi, bb_position=bb_pos,
            vol_ratio=vol_r, ma_status=ma_status,
        )

    # ============================================================
    #  3D + 4D + 5D + 6D 통합 분석
    # ============================================================

    def analyze_full(self, code: str, as_of: str = None,
                     with_news: bool = False, name: str = "") -> Optional[SupplyFull]:
        """3D + 4D + 5D + 6D 통합 분석

        Args:
            with_news: True면 네이버증권+Grok 뉴스 분석 포함 (개별분석용)
            name: 종목명 (뉴스 분석시 필요)
        """
        score = self.analyze(code, as_of)
        momentum = self.analyze_4d(code, as_of)

        if score is None:
            return None
        if momentum is None:
            # 4D 데이터 부족 시 STEADY 기본값
            momentum = SupplyMomentum(code=code, date=score.date, momentum_score=50)

        # 5D 사냥 에너지 (momentum 전달 → 신호 일치도 계산)
        stability = self.analyze_5d(code, as_of, momentum=momentum)

        # 6D 기술건강도 (일봉 기술 지표)
        tech = self.analyze_6d(code)

        # 밸류에이션 경고 (universe.json에서 PER/PBR 조회)
        val_warning = None
        per_val = 0.0
        pbr_val = 0.0
        try:
            from data.universe_builder import get_valuation
            val = get_valuation(code)
            per_val = val["per"]
            pbr_val = val["pbr"]
            val_warning = val["warning"]
        except Exception:
            pass

        # 뉴스 가산점 (개별 분석 시만 — 배치 스캔에서는 느려서 생략)
        news_score = 0.0
        news_summary = ""
        if with_news:
            try:
                from data.news_collector import NewsCollector
                nc = NewsCollector()
                news = nc.get_news_score(code, name, use_grok=True)
                news_score = news["score"]
                news_summary = news.get("summary", "")
            except Exception as e:
                logger.debug(f"뉴스 수집 실패 {code}: {e}")

        return SupplyFull(
            score=score, momentum=momentum, stability=stability,
            valuation_warning=val_warning, per=per_val, pbr=pbr_val,
            tech_health=tech,
            news_score=news_score, news_summary=news_summary,
        )

    # ── 4D 내부 계산 함수들 ──────────────────────────

    def _calc_inst_streak(self, inv_df: pd.DataFrame) -> Tuple[int, float]:
        """기관 연속 매수/매도일 계산

        Returns: (streak_days, cumulative_amount_억원)
            streak > 0: 연속 매수
            streak < 0: 연속 매도
        """
        col = "기관_금액"
        if col not in inv_df.columns:
            return 0, 0.0

        vals = inv_df[col].iloc[-20:]  # 최근 20일
        streak = 0
        amount = 0.0

        if len(vals) == 0:
            return 0, 0.0

        # 마지막 날 기준으로 방향 결정
        last_sign = 1 if vals.iloc[-1] > 0 else -1

        for i in range(len(vals) - 1, -1, -1):
            v = float(vals.iloc[i])
            if (v > 0 and last_sign > 0) or (v < 0 and last_sign < 0):
                streak += last_sign
                amount += v / 1e8
            else:
                break

        return streak, amount

    def _calc_foreign_inflection(self, for_df) -> Tuple[float, float, str]:
        """외인 소진율 변곡점 계산 (2차 미분)

        Returns: (delta_1st, accel_2nd, inflection_type)
        """
        if for_df is None or len(for_df) < 10 or "소진율" not in for_df.columns:
            return 0.0, 0.0, "NONE"

        exh = for_df["소진율"].iloc[-10:]

        # 1차 미분: 최근 5일 변화
        delta_recent = float(exh.iloc[-1]) - float(exh.iloc[-5])
        # 이전 5일 변화
        delta_prev = float(exh.iloc[-5]) - float(exh.iloc[-10])

        # 2차 미분: 가속도
        accel = delta_recent - delta_prev

        # 변곡점 판정
        if delta_prev <= 0 and delta_recent > 0 and accel > 0.05:
            inflection = "UP_TURN"      # 하락→상승 전환
        elif delta_prev >= 0 and delta_recent < 0 and accel < -0.05:
            inflection = "DOWN_TURN"    # 상승→하락 전환
        else:
            inflection = "NONE"

        return delta_recent, accel, inflection

    def _calc_retail_contrarian(self, inv_df: pd.DataFrame) -> Tuple[bool, float, float]:
        """개인 역지표: 개인 매도 + 기관/외인 매수 = 불리시

        Returns: (is_contrarian, retail_net_억원, smart_net_억원)
        """
        retail_col = "개인_금액"
        inst_col = "기관_금액"
        foreign_col = "외국인_금액"

        retail_net = 0.0
        smart_net = 0.0

        if retail_col in inv_df.columns:
            retail_net = float(inv_df[retail_col].iloc[-5:].sum()) / 1e8

        inst_net = 0.0
        if inst_col in inv_df.columns:
            inst_net = float(inv_df[inst_col].iloc[-5:].sum()) / 1e8

        for_net = 0.0
        if foreign_col in inv_df.columns:
            for_net = float(inv_df[foreign_col].iloc[-5:].sum()) / 1e8

        smart_net = inst_net + for_net

        # 개인 역지표: 개인 순매도 AND 기관+외인 순매수
        is_contrarian = (retail_net < -5) and (smart_net > 5)

        return is_contrarian, retail_net, smart_net

    def _calc_supply_acceleration(self, inv_df: pd.DataFrame) -> Tuple[float, str]:
        """수급 가속도: 최근 5일 vs 이전 5일 기관+외인 순매수 비교

        Returns: (acceleration_pct, trend_label)
        """
        inst_col = "기관_금액"
        foreign_col = "외국인_금액"

        if len(inv_df) < 10:
            return 0.0, "STEADY"

        recent_5d = 0.0
        prev_5d = 0.0

        for col in [inst_col, foreign_col]:
            if col in inv_df.columns:
                recent_5d += float(inv_df[col].iloc[-5:].sum()) / 1e8
                prev_5d += float(inv_df[col].iloc[-10:-5].sum()) / 1e8

        # 가속도 계산
        if abs(prev_5d) < 1:
            # 이전 5일 거의 0이면, 최근 5일 자체가 가속도
            if recent_5d > 10:
                return 100.0, "ACCELERATING"
            elif recent_5d > 0:
                return 50.0, "STEADY"
            else:
                return -50.0, "DECELERATING"

        accel_pct = (recent_5d - prev_5d) / abs(prev_5d) * 100

        if accel_pct > 30:
            trend = "ACCELERATING"
        elif accel_pct > -30:
            trend = "STEADY"
        else:
            trend = "DECELERATING"

        return accel_pct, trend

    # ── 배치 분석 ─────────────────────────────────────

    def scan_all(self, codes: list, as_of: str = None) -> list:
        """전 종목 수급 스캔 -> 점수순 정렬"""
        results = []
        for code in codes:
            score = self.analyze(code, as_of)
            if score:
                results.append(score)
        results.sort(key=lambda x: x.total_score, reverse=True)
        return results

    def scan_all_full(self, codes: list, as_of: str = None) -> List[SupplyFull]:
        """전 종목 3D+4D+5D 통합 스캔 -> action 우선순위 정렬"""
        # 밸류에이션 일괄 로드 (JSON 1회만 읽기)
        try:
            from data.universe_builder import get_valuation_warnings
            val_map = get_valuation_warnings(codes)
        except Exception:
            val_map = {}

        results = []
        for code in codes:
            full = self.analyze_full(code, as_of)
            if full:
                # 일괄 로드된 밸류에이션 덮어쓰기 (analyze_full의 개별 조회보다 효율적)
                if code in val_map:
                    v = val_map[code]
                    full.per = v["per"]
                    full.pbr = v["pbr"]
                    full.valuation_warning = v["warning"]
                results.append(full)

        # 정렬: action 우선순위 -> 3D 점수 -> 5D 안정성 -> 4D 점수
        action_order = {"STRONG_BUY": 0, "BUY": 1, "ENTER": 2, "CAUTION": 3, "WATCH": 4, "SKIP": 5}
        results.sort(key=lambda x: (
            action_order.get(x.action, 9),
            -x.score.total_score,
            -(x.stability.stability_score if x.stability else 0),
            -x.momentum.momentum_score,
        ))
        return results

    def print_dashboard(self, scores: list):
        """수급 대시보드 출력 (3D only)"""
        print(f"\n{'='*80}")
        print(f"  수급 분석 대시보드 ({scores[0].date if scores else '?'})")
        print(f"{'='*80}")
        print(f"  {'종목':>8}  {'등급':>3}  {'총점':>4}  {'기관':>4} {'외인':>4} {'공매':>4} {'가격':>4}"
              f"  {'기관5일':>8} {'외인5일':>8} {'소진율':>6} {'공매비중':>6}")
        print(f"  {'-'*74}")

        for s in scores:
            icon = "+" if s.is_body else ("-" if s.is_tail else "~")
            print(
                f"  {icon}{s.code:>6}  {s.grade:>3}  {s.total_score:>4.0f}  "
                f"{s.institutional_score:>4.0f} {s.foreign_score:>4.0f} "
                f"{s.short_score:>4.0f} {s.price_score:>4.0f}  "
                f"{s.inst_net_5d:>+8.1f} {s.foreign_net_5d:>+8.1f} "
                f"{s.foreign_exhaustion:>6.1f} {s.short_balance_pct:>6.2f}"
            )

        body_count = sum(1 for s in scores if s.is_body)
        tail_count = sum(1 for s in scores if s.is_tail)
        print(f"\n  몸통({body_count}) / 중립({len(scores)-body_count-tail_count}) / 꼬리({tail_count})")
        print(f"{'='*80}")

    def print_dashboard_4d(self, fulls: List[SupplyFull]):
        """3D + 4D 통합 대시보드 (디스크법 시각화)"""
        if not fulls:
            print("  데이터 없음")
            return

        print(f"\n{'='*95}")
        print(f"  3D+4D 수급 대시보드 ({fulls[0].score.date})")
        print(f"  디스크법: 3D=반지름(등급), 4D=팽창속도(모멘텀)")
        print(f"{'='*95}")
        print(f"  {'종목':>8}  {'3D':>3} {'3점':>3}  {'4D':>6} {'4점':>3}"
              f"  {'판정':>10}  {'기관연속':>8} {'외인변곡':>8} {'역지표':>4} {'가속도':>8}")
        print(f"  {'-'*89}")

        for f in fulls:
            s = f.score
            m = f.momentum

            # 디스크 두께 시각화
            action = f.action
            if action == "STRONG_BUY":
                bar = "||||||||"
            elif action == "BUY":
                bar = "||||||"
            elif action == "ENTER":
                bar = "||||"
            elif action == "CAUTION":
                bar = "!!??"
            elif action == "WATCH":
                bar = "..||"
            else:
                bar = "    "

            contra = "O" if m.retail_contrarian else "-"

            print(
                f"  {bar} {s.code:>6}  {s.grade:>3} {s.total_score:>3.0f}  "
                f"{m.signal:>6} {m.momentum_score:>3.0f}  "
                f"{action:>10}  "
                f"{m.inst_streak:>+4d}일{m.inst_streak_amount:>+5.0f}억  "
                f"{m.foreign_inflection:>8}  "
                f"{contra:>4}  "
                f"{m.supply_accel:>+7.1f}%"
            )

        print(f"  {'-'*89}")

        # 액션별 요약
        actions = {}
        for f in fulls:
            a = f.action
            actions[a] = actions.get(a, 0) + 1

        summary = " / ".join(f"{k}({v})" for k, v in sorted(actions.items()))
        print(f"  {summary}")
        print(f"{'='*95}")

    def print_dashboard_5d(self, fulls: List[SupplyFull]):
        """3D + 4D + 5D 통합 대시보드 (디스크법 + 사냥 에너지 시각화)"""
        if not fulls:
            print("  데이터 없음")
            return

        print(f"\n{'='*120}")
        print(f"  3D+4D+5D 수급 대시보드 ({fulls[0].score.date})")
        print(f"  디스크법: 3D=반지름(등급), 4D=팽창속도(모멘텀), 5D=에너지(사냥적합도)")
        print(f"{'='*120}")
        print(f"  {'종목':>8}  {'3D':>3} {'3점':>3}  {'4D':>6} {'4점':>3}"
              f"  {'5D':>9} {'5점':>3}"
              f"  {'판정':>20}  {'ATR%':>5} {'거래대금':>7} {'SM강도':>6} {'신호':>4}")
        print(f"  {'-'*114}")

        for f in fulls:
            s = f.score
            m = f.momentum

            action = f.action
            if action == "STRONG_BUY":
                bar = "||||||||"
            elif action == "BUY":
                bar = "||||||"
            elif action == "ENTER":
                bar = "||||"
            elif action == "CAUTION":
                bar = "!!??"
            elif action == "WATCH":
                bar = "..||"
            else:
                bar = "    "

            # 5D 에너지 아이콘
            sg = f.stability_grade
            if sg == "EXPLOSIVE":
                icon = "[***]"   # 폭발적
            elif sg == "HUNTABLE":
                icon = " [**]"   # 사냥감
            elif sg == "MODERATE":
                icon = "  [*]"   # 보통
            elif sg == "SLUGGISH":
                icon = "  [.]"   # 둔감
            else:
                icon = "  [?]"

            stab_score = f.stability.stability_score if f.stability else 0
            atr_pct = f.stability.atr_pct if f.stability else 0
            avg_val = f.stability.avg_trading_value if f.stability else 0
            sm_ratio = f.stability.smart_money_ratio if f.stability else 0
            sig_count = f.stability.signal_count if f.stability else 0

            label = f.risk_label

            print(
                f"  {bar} {s.code:>6}  {s.grade:>3} {s.total_score:>3.0f}  "
                f"{m.signal:>6} {m.momentum_score:>3.0f}  "
                f"{icon} {stab_score:>3.0f}  "
                f"{label:>20}  "
                f"{atr_pct:>5.2f} {avg_val:>7.0f} "
                f"{sm_ratio:>+6.1f}% {sig_count:>2}/4"
            )

        print(f"  {'-'*114}")

        # 액션별 요약
        actions = {}
        for f in fulls:
            actions[f.action] = actions.get(f.action, 0) + 1
        print(f"  {' / '.join(f'{k}({v})' for k, v in sorted(actions.items()))}")

        # 5D 에너지 요약
        energy = {}
        for f in fulls:
            g = f.stability_grade
            energy[g] = energy.get(g, 0) + 1
        energy_order = ["EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH", "UNKNOWN"]
        energy_parts = [f"{g}({energy[g]})" for g in energy_order if g in energy]
        print(f"  에너지: {' / '.join(energy_parts)}")
        print(f"{'='*120}")


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    from data.kis_collector import UNIVERSE
    codes = [c for c in UNIVERSE.keys() if c != "069500"
             and not c.startswith("371") and not c.startswith("102") and not c.startswith("305")]

    mode = sys.argv[1] if len(sys.argv) > 1 else "5d"

    analyzer = SupplyAnalyzer()

    if mode == "3d":
        scores = analyzer.scan_all(codes)
        analyzer.print_dashboard(scores)
    elif mode == "4d":
        fulls = analyzer.scan_all_full(codes)
        analyzer.print_dashboard_4d(fulls)
    elif mode == "5d":
        fulls = analyzer.scan_all_full(codes)
        analyzer.print_dashboard_5d(fulls)
    else:
        # 개별 종목 상세
        code = mode
        full = analyzer.analyze_full(code)
        if full:
            print(f"\n  {full}")
            print(f"  3D: {full.score}")
            print(f"  4D: {full.momentum}")
            if full.stability:
                print(f"  5D: {full.stability}")
        else:
            print(f"  {code}: 데이터 없음")
