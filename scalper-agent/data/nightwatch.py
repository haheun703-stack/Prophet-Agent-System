# -*- coding: utf-8 -*-
"""
NIGHTWATCH - 3단 예측 체인 야간매매 신호 엔진
═══════════════════════════════════════════════
미국장 개장 7시간 전에 방향을 읽는다.

[1단] 아시안 리스크 스코어 (09:00~15:30 KST)
  - AUD/JPY: 위험자산 온도계
  - CNH: 차이나머니 방향
  - KOSPI 외국인 순매매

[2단] 유럽 오픈 스코어 (16:00~16:30 KST) ★핵심★
  - DAX 첫 30분 등락률
  - EUR/USD: 달러 수요
  - HYG: 크레딧 스프레드

[3단] 괴리 감지 (Divergence Detection)
  - HYG vs ES 방향 불일치
  - VIX 레벨 + 급변
  - 채권(TNX) vs 금(GC) 방향 불일치

최종: -10 ~ +10 점수 → 5단계 신호
  🟢🟢 강매수 / 🟢 매수 / 🟡 관망 / 🔴 금지 / 💀 패닉

사용:
  python -m data.nightwatch           # 전체 실행
  python -m data.nightwatch --test    # 테스트 (캐시된 데이터)
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
REPORT_PATH = DATA_DIR / "nightwatch_report.json"

# ═══════════════════════════════════════════════════
#  yfinance 지연 import (설치 안 되어도 에러 안 남)
# ═══════════════════════════════════════════════════
_yf = None

def _get_yf():
    global _yf
    if _yf is None:
        try:
            import yfinance as yf
            _yf = yf
        except ImportError:
            logger.error("yfinance 미설치. pip install yfinance")
            raise
    return _yf


# ═══════════════════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════════════════
@dataclass
class IndicatorData:
    """단일 지표 데이터"""
    name: str
    value: Optional[float] = None
    change_pct: Optional[float] = None
    change_abs: Optional[float] = None
    signal: str = "⬜"  # 🟢 🟡 🔴 ⬜
    error: str = ""


@dataclass
class NightwatchReport:
    """NIGHTWATCH 최종 리포트"""
    timestamp: str = ""
    # 1단: 아시안 리스크
    asian_score: float = 0.0
    asian_detail: Dict = field(default_factory=dict)
    # 2단: 유럽 오픈
    europe_score: float = 0.0
    europe_detail: Dict = field(default_factory=dict)
    # 3단: 괴리 감지
    divergence_score: float = 0.0
    divergences: List[str] = field(default_factory=list)
    # 최종
    total_score: float = 0.0
    signal: str = "🟡"
    signal_text: str = "관망"
    recommended_sectors: List[str] = field(default_factory=list)
    # JARVIS 섹터 매핑
    nxt_targets: List[Dict] = field(default_factory=list)
    macro_conditions: Dict = field(default_factory=dict)
    selection_reason: str = ""
    # 원본 데이터
    raw_indicators: Dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════
#  JARVIS SECTOR MAP v1.0
#  "대장이 움직이면 소부장이 따라온다. 순서를 아는 자가 이긴다."
# ═══════════════════════════════════════════════════
JARVIS_SECTORS = {
    "semiconductor": {
        "name": "💾 반도체",
        "us": ["NVDA", "TSMC", "AMD", "AVGO", "ASML", "MU"],
        "kr_tier1": [
            {"code": "000660", "name": "SK하이닉스"},
            {"code": "005930", "name": "삼성전자"},
        ],
        "kr_tier2": [
            {"code": "009150", "name": "삼성전기"},
            {"code": "403870", "name": "HPSP"},
            {"code": "240810", "name": "원익IPS"},
            {"code": "042700", "name": "한미반도체"},
            {"code": "039030", "name": "이오테크닉스"},
            {"code": "005290", "name": "동진쎄미켐"},
        ],
        "relay": "NVDA수주→SK하이닉스/삼성전자→한미반도체→HPSP/원익IPS→ISC",
    },
    "software_ai": {
        "name": "💻 소프트웨어·AI",
        "us": ["MSFT", "GOOGL", "META", "CRM", "NOW", "PLTR"],
        "kr_tier1": [
            {"code": "035420", "name": "NAVER"},
            {"code": "035720", "name": "카카오"},
            {"code": "259960", "name": "크래프톤"},
        ],
        "kr_tier2": [
            {"code": "018260", "name": "삼성SDS"},
            {"code": "017670", "name": "SK텔레콤"},
        ],
        "relay": "MSFT/GOOGL AI투자→NAVER/카카오 AI전환→삼성SDS 인프라",
    },
    "bio": {
        "name": "💊 바이오·헬스케어",
        "us": ["LLY", "NVO", "ABBV", "JNJ", "MRK"],
        "kr_tier1": [
            {"code": "207940", "name": "삼성바이오로직스"},
            {"code": "068270", "name": "셀트리온"},
            {"code": "128940", "name": "한미약품"},
        ],
        "kr_tier2": [
            {"code": "000100", "name": "유한양행"},
            {"code": "196170", "name": "알테오젠"},
            {"code": "028300", "name": "HLB"},
        ],
        "relay": "LLY/NVO GLP-1폭증→한미약품→삼성바이오CMO→알테오젠",
    },
    "power_infra": {
        "name": "⚡ 전력·에너지인프라",
        "us": ["GEV", "NEE", "ETN", "VRT", "CEG"],
        "kr_tier1": [
            {"code": "267260", "name": "HD현대일렉트릭"},
            {"code": "010120", "name": "LS일렉트릭"},
            {"code": "298040", "name": "효성중공업"},
        ],
        "kr_tier2": [
            {"code": "229640", "name": "LS전선아시아"},
            {"code": "006260", "name": "LS"},
            {"code": "042670", "name": "두산에너빌리티"},
        ],
        "relay": "AI DC착공→GEV/ETN/VRT→HD현대일렉/효성중공업→LS전선→두산에너빌리티",
    },
    "oil_resource": {
        "name": "🛢 원유·자원",
        "us": ["XOM", "CVX", "COP", "SLB", "LNG"],
        "kr_tier1": [
            {"code": "010950", "name": "S-Oil"},
            {"code": "096770", "name": "SK이노베이션"},
        ],
        "kr_tier2": [
            {"code": "028050", "name": "삼성엔지니어링"},
            {"code": "036460", "name": "한국가스공사"},
        ],
        "relay": "WTI$80+→XOM/CVX증산→SLB서비스→삼성엔지니어링→S-Oil마진개선",
    },
    "space_defense": {
        "name": "🚀 우주·항공·방산",
        "us": ["LMT", "NOC", "BA", "RKLB", "KTOS"],
        "kr_tier1": [
            {"code": "012450", "name": "한화에어로스페이스"},
            {"code": "047810", "name": "한국항공우주"},
            {"code": "079550", "name": "LIG넥스원"},
        ],
        "kr_tier2": [
            {"code": "064350", "name": "현대로템"},
            {"code": "003570", "name": "SNT다이내믹스"},
            {"code": "103140", "name": "풍산"},
            {"code": "099320", "name": "쎄트렉아이"},
        ],
        "relay": "지정학긴장→LMT/NOC→한화에어로/KAI→LIG넥스원→현대로템/SNT/풍산",
    },
    "entertainment": {
        "name": "🎭 엔터테인먼트",
        "us": ["NFLX", "DIS", "SPOT"],
        "kr_tier1": [
            {"code": "352820", "name": "하이브"},
            {"code": "041510", "name": "SM엔터"},
            {"code": "035900", "name": "JYP엔터"},
        ],
        "kr_tier2": [
            {"code": "253450", "name": "스튜디오드래곤"},
            {"code": "122870", "name": "YG엔터"},
            {"code": "035760", "name": "CJ ENM"},
        ],
        "relay": "NFLX K-콘텐츠발주→스튜디오드래곤/CJ ENM→하이브/SM/YG 글로벌투어",
    },
    "securities": {
        "name": "📈 증권·금융",
        "us": ["GS", "MS", "JPM", "BX"],
        "kr_tier1": [
            {"code": "006800", "name": "미래에셋증권"},
            {"code": "039490", "name": "키움증권"},
            {"code": "016360", "name": "삼성증권"},
        ],
        "kr_tier2": [
            {"code": "071050", "name": "한국금융지주"},
        ],
        "relay": "금리인하→채권랠리→증권운용이익/IPO시장→키움거래대금",
    },
    "reits": {
        "name": "🏢 리츠·부동산",
        "us": ["AMT", "PLD", "EQIX", "DLR", "O"],
        "kr_tier1": [
            {"code": "395400", "name": "SK리츠"},
            {"code": "088980", "name": "맥쿼리인프라"},
        ],
        "kr_tier2": [
            {"code": "365550", "name": "ESR켄달스퀘어리츠"},
            {"code": "334890", "name": "이지스밸류리츠"},
        ],
        "relay": "금리인하→배당매력→EQIX/DLR AI DC→ESR켄달스퀘어",
    },
    "shipbuilding": {
        "name": "🚢 조선",
        "us": ["HII", "GD"],
        "kr_tier1": [
            {"code": "009540", "name": "HD한국조선해양"},
            {"code": "042660", "name": "한화오션"},
        ],
        "kr_tier2": [
            {"code": "010620", "name": "HD현대미포"},
            {"code": "329180", "name": "HD현대마린솔루션"},
        ],
        "relay": "LNG발주/지정학→HD한국조선해양/한화오션→HD현대미포→마린솔루션",
    },
    "battery_ev": {
        "name": "🔋 2차전지·EV",
        "us": ["TSLA", "PANW", "ALB", "LAC"],
        "kr_tier1": [
            {"code": "373220", "name": "LG에너지솔루션"},
            {"code": "006400", "name": "삼성SDI"},
        ],
        "kr_tier2": [
            {"code": "051910", "name": "LG화학"},
            {"code": "247540", "name": "에코프로비엠"},
            {"code": "086520", "name": "에코프로"},
        ],
        "relay": "TSLA수주→LG에너지솔루션/삼성SDI→에코프로비엠/에코프로→LG화학",
    },
    # 특수 섹터
    "inverse": {
        "name": "📉 인버스(헤지)",
        "us": [],
        "kr_tier1": [
            {"code": "252670", "name": "KODEX 200선물인버스2X"},
            {"code": "114800", "name": "KODEX 인버스"},
        ],
        "kr_tier2": [],
        "relay": "하락시그널→인버스ETF",
    },
}


# ═══════════════════════════════════════════════════
#  yfinance 유틸리티
# ═══════════════════════════════════════════════════
def _fetch_ticker(symbol: str, period: str = "5d") -> Optional[IndicatorData]:
    """yfinance에서 티커 데이터 가져오기 (최근 2일 비교)"""
    yf = _get_yf()
    name = symbol.replace("=X", "").replace("=F", "").replace("^", "")
    try:
        data = yf.Ticker(symbol).history(period=period)
        if data is None or len(data) < 2:
            return IndicatorData(name=name, error="데이터 부족")

        prev = float(data["Close"].iloc[-2])
        curr = float(data["Close"].iloc[-1])

        if prev == 0:
            return IndicatorData(name=name, value=curr, error="전일가 0")

        pct = ((curr - prev) / prev) * 100
        abs_chg = curr - prev

        return IndicatorData(
            name=name,
            value=round(curr, 4),
            change_pct=round(pct, 3),
            change_abs=round(abs_chg, 4),
        )
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] {symbol} 수집 실패: {e}")
        return IndicatorData(name=name, error=str(e))


def _fetch_intraday_change(symbol: str, minutes: int = 30) -> Optional[IndicatorData]:
    """장중 첫 N분 등락률 (유럽 오픈 30분용)"""
    yf = _get_yf()
    name = symbol.replace("=X", "").replace("=F", "").replace("^", "")
    try:
        data = yf.Ticker(symbol).history(period="1d", interval="5m")
        if data is None or len(data) < 2:
            return IndicatorData(name=name, error="인트라데이 데이터 부족")

        # 첫 N분 = 첫 (N/5)개 봉
        n_bars = max(1, minutes // 5)
        if len(data) < n_bars + 1:
            n_bars = len(data) - 1

        open_price = float(data["Open"].iloc[0])
        close_price = float(data["Close"].iloc[min(n_bars, len(data) - 1)])

        if open_price == 0:
            return IndicatorData(name=name, error="시가 0")

        pct = ((close_price - open_price) / open_price) * 100
        return IndicatorData(
            name=name,
            value=round(close_price, 2),
            change_pct=round(pct, 3),
        )
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] {symbol} 인트라데이 실패: {e}")
        return IndicatorData(name=name, error=str(e))


# ═══════════════════════════════════════════════════
#  [1단] 아시안 리스크 스코어
# ═══════════════════════════════════════════════════
def collect_asian_risk() -> Tuple[float, Dict]:
    """
    아시안 세션 위험자산 온도 측정
    AUD/JPY + CNH + KOSPI 외인 수급
    Returns: (score, detail_dict)
    """
    score = 0.0
    detail = {}

    # --- AUD/JPY: 핵심 리스크 바로미터 ---
    audjpy = _fetch_ticker("AUDJPY=X")
    if audjpy and audjpy.change_pct is not None:
        pct = audjpy.change_pct
        if pct > 0.3:
            audjpy.signal = "🟢"
            score += 2.0
        elif pct > 0.1:
            audjpy.signal = "🟢"
            score += 1.0
        elif pct < -0.3:
            audjpy.signal = "🔴"
            score -= 2.0
        elif pct < -0.1:
            audjpy.signal = "🔴"
            score -= 1.0
        else:
            audjpy.signal = "🟡"
    detail["AUD/JPY"] = asdict(audjpy) if audjpy else {}

    # --- CNH (위안화): 차이나머니 ---
    # CNY=X = 1USD = X위안 → 하락 = 위안화 강세 = 긍정
    cnh = _fetch_ticker("CNY=X")
    if cnh and cnh.change_pct is not None:
        pct = cnh.change_pct
        if pct < -0.2:  # 위안화 강세 (CNY=X 하락)
            cnh.signal = "🟢"
            score += 1.5
        elif pct > 0.2:  # 위안화 약세 (CNY=X 상승)
            cnh.signal = "🔴"
            score -= 1.5
        else:
            cnh.signal = "🟡"
    detail["CNH"] = asdict(cnh) if cnh else {}

    # --- KOSPI 외국인 순매매 (기존 모듈 재활용) ---
    try:
        from data.nationality_signal import score_nationality
        # KODEX200 (069500) 대표로 시장 전체 외인 동향 파악
        raw_score = score_nationality("069500")
        # score_nationality가 tuple 반환할 수 있음 → float 추출
        nat_score = float(raw_score[0]) if isinstance(raw_score, (tuple, list)) else float(raw_score)
        foreign_signal = "🟢" if nat_score > 0 else ("🔴" if nat_score < 0 else "🟡")
        score += nat_score * 0.5  # 가중치 0.5
        detail["KOSPI_외인"] = {"score": nat_score, "signal": foreign_signal}
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] 외인 수급 실패: {e}")
        detail["KOSPI_외인"] = {"score": 0, "signal": "⬜", "error": str(e)}

    return round(score, 1), detail


# ═══════════════════════════════════════════════════
#  [2단] 유럽 오픈 스코어 ★핵심★
# ═══════════════════════════════════════════════════
def collect_europe_open() -> Tuple[float, Dict]:
    """
    유럽장 오픈 첫 30분 방향 측정
    DAX + EUR/USD + HYG
    Returns: (score, detail_dict)
    """
    score = 0.0
    detail = {}

    # --- DAX 첫 30분: 유럽 기관의 첫 포지션 ★ ---
    dax = _fetch_intraday_change("^GDAXI", minutes=30)
    if dax and dax.change_pct is not None:
        pct = dax.change_pct
        if pct > 0.5:
            dax.signal = "🟢"
            score += 3.0  # DAX 가중치 최대
        elif pct > 0.2:
            dax.signal = "🟢"
            score += 1.5
        elif pct < -0.5:
            dax.signal = "🔴"
            score -= 3.0
        elif pct < -0.2:
            dax.signal = "🔴"
            score -= 1.5
        else:
            dax.signal = "🟡"
    detail["DAX_30min"] = asdict(dax) if dax else {}

    # --- EUR/USD: 달러 약세 = 위험자산 우호 ---
    eurusd = _fetch_ticker("EURUSD=X")
    if eurusd and eurusd.change_pct is not None:
        pct = eurusd.change_pct
        if pct > 0.3:  # 유로 강세 = 달러 약세
            eurusd.signal = "🟢"
            score += 1.0
        elif pct < -0.3:  # 유로 약세 = 달러 강세
            eurusd.signal = "🔴"
            score -= 1.0
        else:
            eurusd.signal = "🟡"
    detail["EUR/USD"] = asdict(eurusd) if eurusd else {}

    # --- HYG: 크레딧 스프레드 (가장 빠른 스트레스 감지) ---
    hyg = _fetch_ticker("HYG")
    if hyg and hyg.change_pct is not None:
        pct = hyg.change_pct
        if pct > 0.3:
            hyg.signal = "🟢"
            score += 1.5
        elif pct < -0.3:
            hyg.signal = "🔴"
            score -= 2.0  # HYG 하락 경고
        else:
            hyg.signal = "🟡"
    detail["HYG"] = asdict(hyg) if hyg else {}

    return round(score, 1), detail


# ═══════════════════════════════════════════════════
#  [3단] 괴리 감지 (Divergence Detection)
# ═══════════════════════════════════════════════════
def detect_divergence() -> Tuple[float, List[str], Dict]:
    """
    시장 간 괴리 감지 - 진짜 엣지
    Returns: (penalty_score, divergence_list, raw_data)
    """
    penalty = 0.0
    divergences = []
    raw = {}

    # --- VIX 레벨 ---
    vix = _fetch_ticker("^VIX")
    if vix and vix.value is not None:
        raw["VIX"] = asdict(vix)
        if vix.value >= 30:
            vix.signal = "💀"
            penalty -= 3.0
            divergences.append(f"💀 VIX {vix.value:.1f} - 패닉 구간")
        elif vix.value >= 25:
            vix.signal = "🔴"
            penalty -= 2.0
            divergences.append(f"🔴 VIX {vix.value:.1f} - 공포 구간")
        elif vix.value >= 20:
            vix.signal = "🟡"
            penalty -= 0.5
        else:
            vix.signal = "🟢"
            penalty += 0.5

    # --- ES 선물 (S&P500) ---
    es = _fetch_ticker("ES=F")
    raw["ES"] = asdict(es) if es else {}

    # --- 괴리 1: HYG 하락 + ES 안정 = 숨은 스트레스 ---
    hyg = _fetch_ticker("HYG")
    raw["HYG_div"] = asdict(hyg) if hyg else {}
    if (hyg and hyg.change_pct is not None and
            es and es.change_pct is not None):
        if hyg.change_pct < -0.3 and es.change_pct > -0.2:
            penalty -= 2.0
            divergences.append(
                f"⚠️ HYG {hyg.change_pct:+.2f}% vs ES {es.change_pct:+.2f}%"
                f" - 숨은 크레딧 스트레스"
            )

    # --- 괴리 2: 채권 상승(금리 하락) + 금 하락 = 위험선호 전환 ---
    tnx = _fetch_ticker("^TNX")
    gold = _fetch_ticker("GC=F")
    raw["TNX"] = asdict(tnx) if tnx else {}
    raw["GOLD"] = asdict(gold) if gold else {}
    if (tnx and tnx.change_abs is not None and
            gold and gold.change_pct is not None):
        if tnx.change_abs < -0.03 and gold.change_pct < -0.3:
            penalty += 1.5
            divergences.append(
                f"💡 TNX {tnx.change_abs:+.3f} + Gold {gold.change_pct:+.2f}%"
                f" - 스마트머니 위험선호 전환"
            )
        elif tnx.change_abs > 0.05 and gold.change_pct > 0.3:
            penalty -= 1.5
            divergences.append(
                f"🔴 TNX {tnx.change_abs:+.3f} + Gold {gold.change_pct:+.2f}%"
                f" - 안전자산 쏠림"
            )

    # --- 괴리 3: 원/달러 급등 ---
    usdkrw = _fetch_ticker("KRW=X")
    raw["USDKRW"] = asdict(usdkrw) if usdkrw else {}
    if usdkrw and usdkrw.change_pct is not None:
        if usdkrw.change_pct > 0.5:  # 원화 약세 급등
            penalty -= 1.0
            divergences.append(
                f"🔴 원/달러 {usdkrw.change_pct:+.2f}% - 원화 급락"
            )
        elif usdkrw.change_pct < -0.3:  # 원화 강세
            penalty += 0.5

    return round(penalty, 1), divergences, raw


# ═══════════════════════════════════════════════════
#  [4단] 매크로 조건 감지 (JARVIS 섹터 매핑용)
# ═══════════════════════════════════════════════════
def collect_macro_conditions(
    asian_detail: Dict,
    raw_indicators: Dict,
) -> Tuple[Dict, Dict]:
    """
    추가 매크로 지표 수집 + 조건 판별
    NQ(나스닥), CL(원유), HG(구리) 추가 수집 → 기존 데이터와 결합
    Returns: (conditions_dict, additional_raw_data)
    """
    conditions = {}
    additional_raw = {}

    # --- NQ: 나스닥 100 선물 (미국 기술주 방향) ---
    nq = _fetch_ticker("NQ=F")
    additional_raw["NQ"] = asdict(nq) if nq else {}
    if nq and nq.change_pct is not None:
        conditions["nasdaq_up"] = nq.change_pct > 0.5
        conditions["nasdaq_down"] = nq.change_pct < -0.5
        conditions["nasdaq_pct"] = nq.change_pct

    # --- CL: WTI 원유 선물 ---
    cl = _fetch_ticker("CL=F")
    additional_raw["CL"] = asdict(cl) if cl else {}
    if cl and cl.change_pct is not None:
        conditions["oil_up"] = cl.change_pct > 1.5
        conditions["oil_down"] = cl.change_pct < -1.5
        conditions["oil_pct"] = cl.change_pct

    # --- HG: 구리 선물 (지정학 감지용) ---
    hg = _fetch_ticker("HG=F")
    additional_raw["HG"] = asdict(hg) if hg else {}

    # --- TNX: 금리 방향 (detect_divergence에서 이미 수집) ---
    tnx = raw_indicators.get("TNX", {})
    if tnx.get("change_abs") is not None:
        conditions["rate_down"] = tnx["change_abs"] < -0.04
        conditions["rate_up"] = tnx["change_abs"] > 0.04

    # --- CNH: 위안화 강세 (collect_asian_risk에서 이미 수집) ---
    cnh = asian_detail.get("CNH", {})
    if cnh.get("change_pct") is not None:
        # CNY=X 하락 = 위안화 강세 = 긍정
        conditions["cnh_strong"] = cnh["change_pct"] < -0.15

    # --- 지정학 긴장: AUD/JPY 하락 + 구리 상승 ---
    audjpy = asian_detail.get("AUD/JPY", {})
    if (audjpy.get("change_pct") is not None and
            hg and hg.change_pct is not None):
        conditions["geopolitical"] = (
            audjpy["change_pct"] < -0.2 and hg.change_pct > 0.3
        )
    # AUD/JPY만 급락해도 지정학 의심
    elif audjpy.get("change_pct") is not None and audjpy["change_pct"] < -0.4:
        conditions["geopolitical"] = True

    # 활성 조건 텍스트 생성
    active = []
    if conditions.get("nasdaq_up"):
        active.append(f"나스닥↑ {conditions.get('nasdaq_pct', 0):+.1f}%")
    if conditions.get("nasdaq_down"):
        active.append(f"나스닥↓ {conditions.get('nasdaq_pct', 0):+.1f}%")
    if conditions.get("rate_down"):
        active.append("금리↓")
    if conditions.get("rate_up"):
        active.append("금리↑")
    if conditions.get("oil_up"):
        active.append(f"유가↑ {conditions.get('oil_pct', 0):+.1f}%")
    if conditions.get("cnh_strong"):
        active.append("CNH강세")
    if conditions.get("geopolitical"):
        active.append("지정학 긴장")
    conditions["active_text"] = active

    return conditions, additional_raw


# ═══════════════════════════════════════════════════
#  JARVIS 섹터 선정 + NXT 대상 종목 매칭
# ═══════════════════════════════════════════════════
def select_sectors_and_targets(
    total_score: float,
    macro: Dict,
    max_tier: int = 1,
) -> Tuple[List[str], List[Dict], str]:
    """
    NIGHTWATCH 점수 + 매크로 조건 → 섹터 우선순위 → NXT 대상 종목
    PDF p.16 NIGHTWATCH 섹터 매핑 테이블 구현

    Returns: (sector_display_names, nxt_targets, selection_reason)
    """
    selected_keys = []
    reason = ""

    # ── 강매수 (5+) ──
    if total_score >= 5:
        if macro.get("nasdaq_up"):
            selected_keys = ["semiconductor", "software_ai"]
            reason = "🟢🟢 + 나스닥↑"
        elif macro.get("rate_down"):
            selected_keys = ["reits", "securities", "power_infra"]
            reason = "🟢🟢 + 금리↓"
        elif macro.get("oil_up"):
            selected_keys = ["oil_resource", "shipbuilding"]
            reason = "🟢🟢 + 유가↑"
        else:
            selected_keys = ["semiconductor", "power_infra"]
            reason = "🟢🟢 기본"

    # ── 매수 (2~4.9) ──
    elif total_score >= 2:
        if macro.get("nasdaq_up"):
            selected_keys = ["semiconductor", "battery_ev"]
            reason = "🟢 + 나스닥↑"
        elif macro.get("cnh_strong"):
            selected_keys = ["semiconductor", "entertainment"]
            reason = "🟢 + CNH강세"
        elif macro.get("oil_up"):
            selected_keys = ["oil_resource", "shipbuilding"]
            reason = "🟢 + 유가↑"
        elif macro.get("geopolitical"):
            selected_keys = ["space_defense", "shipbuilding"]
            reason = "🟢 + 지정학 긴장"
        elif macro.get("rate_down"):
            selected_keys = ["reits", "securities"]
            reason = "🟢 + 금리↓"
        else:
            selected_keys = ["semiconductor", "power_infra"]
            reason = "🟢 기본"

    # ── 극단 패닉만 인버스 (-5 이하) ──
    # 백테스트: score<-2 인버스 적중 41% → 대부분 손실
    # 수정: -5 이하 극단적일 때만 인버스 (그 외는 관망=미진입)
    elif total_score < -5:
        selected_keys = ["inverse"]
        reason = "💀 극단 하락 → 인버스 헤지"

    # ── 관망 (-1.9 ~ 1.9) ──
    else:
        return [], [], "🟡 관망 - 진입 없음"

    # 섹터 키 → 종목 목록 변환
    nxt_targets = []
    sector_names = []

    for priority, key in enumerate(selected_keys, 1):
        sector = JARVIS_SECTORS.get(key)
        if not sector:
            continue
        sector_names.append(f"{sector['name']} ({priority}순위)")

        # Tier 1
        for stock in sector.get("kr_tier1", []):
            nxt_targets.append({
                "code": stock["code"],
                "name": stock["name"],
                "sector": sector["name"],
                "sector_key": key,
                "tier": 1,
                "priority": priority,
            })

        # Tier 2 (max_tier >= 2 일 때만)
        if max_tier >= 2:
            for stock in sector.get("kr_tier2", []):
                nxt_targets.append({
                    "code": stock["code"],
                    "name": stock["name"],
                    "sector": sector["name"],
                    "sector_key": key,
                    "tier": 2,
                    "priority": priority,
                })

    # 정렬: 1순위 섹터 Tier1 → 2순위 섹터 Tier1 → ...
    nxt_targets.sort(key=lambda x: (x["priority"], x["tier"]))

    return sector_names, nxt_targets, reason


# ═══════════════════════════════════════════════════
#  최종 점수 계산
# ═══════════════════════════════════════════════════
def calculate_nightwatch_score(
    asian_score: float,
    asian_detail: Dict,
    europe_score: float,
    europe_detail: Dict,
    div_score: float,
    divergences: List[str],
    raw_indicators: Dict,
    macro_conditions: Optional[Dict] = None,
) -> NightwatchReport:
    """3단 체인 + 매크로 조건 종합 → JARVIS 섹터 매핑 → 최종 리포트"""
    total = asian_score + europe_score + div_score
    total = max(-10.0, min(10.0, total))

    # 신호 변환 (v2: 관망 확대, 금지/패닉 축소 - 백테스트 44%→개선)
    if total >= 5:
        signal, text = "🟢🟢", "강한 매수"
    elif total >= 2:
        signal, text = "🟢", "매수 고려"
    elif total >= -2:
        signal, text = "🟡", "관망"       # was -1 → -2 (하방 관망 확대)
    elif total >= -5:
        signal, text = "🔴", "진입 금지"  # was -4 → -5
    else:
        signal, text = "💀", "전체 포지션 점검"

    # JARVIS 섹터 매핑 (매크로 조건 기반)
    macro = macro_conditions or {}
    sector_names, nxt_targets, selection_reason = select_sectors_and_targets(
        total_score=total,
        macro=macro,
        max_tier=2,  # 리포트에 Tier2까지 포함 (매수 시 auto_trader에서 Tier1 필터)
    )

    return NightwatchReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        asian_score=asian_score,
        asian_detail=asian_detail,
        europe_score=europe_score,
        europe_detail=europe_detail,
        divergence_score=div_score,
        divergences=divergences,
        total_score=round(total, 1),
        signal=signal,
        signal_text=text,
        recommended_sectors=sector_names,
        nxt_targets=nxt_targets,
        macro_conditions=macro,
        selection_reason=selection_reason,
        raw_indicators=raw_indicators,
    )


# ═══════════════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════════════
def run_nightwatch() -> NightwatchReport:
    """NIGHTWATCH 전체 실행 (3단 체인 + JARVIS 섹터 매핑)"""
    logger.info("[NIGHTWATCH] 실행 시작")

    # 1단: 아시안 리스크
    logger.info("[NIGHTWATCH] [1단] 아시안 리스크 수집...")
    asian_score, asian_detail = collect_asian_risk()
    logger.info(f"[NIGHTWATCH] 아시안 스코어: {asian_score:+.1f}")

    # 2단: 유럽 오픈
    logger.info("[NIGHTWATCH] [2단] 유럽 오픈 수집...")
    europe_score, europe_detail = collect_europe_open()
    logger.info(f"[NIGHTWATCH] 유럽 스코어: {europe_score:+.1f}")

    # 3단: 괴리 감지
    logger.info("[NIGHTWATCH] [3단] 괴리 감지...")
    div_score, divergences, raw = detect_divergence()
    logger.info(f"[NIGHTWATCH] 괴리 스코어: {div_score:+.1f}, 감지: {len(divergences)}건")

    # 4단: 매크로 조건 수집 (JARVIS 섹터 매핑용)
    logger.info("[NIGHTWATCH] [4단] 매크로 조건 수집 (NQ/CL/HG)...")
    macro_conditions, macro_raw = collect_macro_conditions(asian_detail, raw)
    raw.update(macro_raw)  # 추가 지표 병합
    active = macro_conditions.get("active_text", [])
    logger.info(f"[NIGHTWATCH] 매크로: {', '.join(active) if active else '특이사항 없음'}")

    # 종합 (JARVIS 섹터 매핑 포함)
    report = calculate_nightwatch_score(
        asian_score, asian_detail,
        europe_score, europe_detail,
        div_score, divergences, raw,
        macro_conditions=macro_conditions,
    )

    # 저장
    save_nightwatch_report(report)
    logger.info(f"[NIGHTWATCH] 완료 | {report.signal} {report.signal_text} | "
                f"점수: {report.total_score:+.1f} | "
                f"섹터: {report.selection_reason} | "
                f"NXT 대상: {len(report.nxt_targets)}종목")

    return report


# ═══════════════════════════════════════════════════
#  저장 / 로드
# ═══════════════════════════════════════════════════
def save_nightwatch_report(report: NightwatchReport):
    """리포트 JSON 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2)
    logger.info(f"[NIGHTWATCH] 저장: {REPORT_PATH}")


def load_nightwatch_report() -> Optional[NightwatchReport]:
    """저장된 리포트 로드"""
    if not REPORT_PATH.exists():
        return None
    try:
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return NightwatchReport(**data)
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] 로드 실패: {e}")
        return None


# ═══════════════════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════════════════
def format_nightwatch_report(report: NightwatchReport) -> str:
    """텔레그램 발송용 메시지 포맷 (JARVIS 섹터 매핑 포함)"""

    def _sig(d: dict) -> str:
        return d.get("signal", "⬜")

    def _pct(d: dict) -> str:
        v = d.get("change_pct")
        return f"{v:+.2f}%" if v is not None else "N/A"

    def _val(d: dict) -> str:
        v = d.get("value")
        return f"{v}" if v is not None else "N/A"

    ad = report.asian_detail
    ed = report.europe_detail
    ri = report.raw_indicators
    mc = report.macro_conditions

    lines = [
        f"NIGHTWATCH | {report.timestamp}",
        "=" * 34,
        "",
        "[1단] 아시안 리스크  ({:+.1f})".format(report.asian_score),
        f"  {_sig(ad.get('AUD/JPY', {}))} AUD/JPY: {_pct(ad.get('AUD/JPY', {}))}",
        f"  {_sig(ad.get('CNH', {}))} CNH: {_val(ad.get('CNH', {}))} ({_pct(ad.get('CNH', {}))})",
        f"  {ad.get('KOSPI_외인', {}).get('signal', '⬜')} 외인: {ad.get('KOSPI_외인', {}).get('score', 0):+.1f}",
        "",
        "[2단] 유럽 오픈  ({:+.1f})  ★".format(report.europe_score),
        f"  {_sig(ed.get('DAX_30min', {}))} DAX 30분: {_pct(ed.get('DAX_30min', {}))}",
        f"  {_sig(ed.get('EUR/USD', {}))} EUR/USD: {_pct(ed.get('EUR/USD', {}))}",
        f"  {_sig(ed.get('HYG', {}))} HYG: {_pct(ed.get('HYG', {}))}",
        "",
        "[3단] 괴리 감지  ({:+.1f})".format(report.divergence_score),
    ]

    # VIX
    vix_d = ri.get("VIX", {})
    lines.append(f"  {vix_d.get('signal', '⬜')} VIX: {_val(vix_d)}")

    # 괴리 항목
    if report.divergences:
        for dv in report.divergences:
            lines.append(f"  {dv}")
    else:
        lines.append("  괴리 없음")

    # 매크로 조건
    lines.append("")
    lines.append("[4단] 매크로 조건")
    nq_d = ri.get("NQ", {})
    cl_d = ri.get("CL", {})
    if nq_d:
        lines.append(f"  나스닥(NQ): {_pct(nq_d)}")
    if cl_d:
        lines.append(f"  원유(CL): {_pct(cl_d)}")
    active_text = mc.get("active_text", [])
    if active_text:
        lines.append(f"  활성: {' | '.join(active_text)}")
    else:
        lines.append("  특이사항 없음")

    lines.extend([
        "",
        "=" * 34,
        f"종합: {report.total_score:+.1f} / 10",
        f"{report.signal} {report.signal_text}",
    ])

    # JARVIS 섹터 매핑 결과
    if report.selection_reason:
        lines.append(f"판단: {report.selection_reason}")

    lines.append("")

    if report.recommended_sectors:
        lines.append("JARVIS 섹터:")
        for s in report.recommended_sectors:
            lines.append(f"  {s}")

    # NXT 매수 대상 종목
    if report.nxt_targets:
        lines.append("")
        lines.append("NXT 매수 대상:")
        shown = set()
        for t in report.nxt_targets:
            key = t["code"]
            if key in shown:
                continue
            shown.add(key)
            tier_mark = "★" if t["tier"] == 1 else "☆"
            lines.append(
                f"  {tier_mark} {t['name']}({t['code']}) "
                f"[{t['sector']}]"
            )
            if len(shown) >= 8:  # 최대 8종목까지 표시
                remaining = len(report.nxt_targets) - len(shown)
                if remaining > 0:
                    lines.append(f"  ... 외 {remaining}종목")
                break
    elif report.total_score >= 2:
        lines.append("NXT 대상: 없음")

    lines.append("=" * 34)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    report = run_nightwatch()
    msg = format_nightwatch_report(report)
    print()
    print(msg)
