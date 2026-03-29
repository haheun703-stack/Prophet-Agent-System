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
HISTORY_PATH = DATA_DIR / "learning" / "nxt_history.json"
COMMODITY_HISTORY_PATH = DATA_DIR / "learning" / "commodity_history.json"

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
    # 4.5단: 한국장 강도 (NXT 캘리브레이션 v2)
    korea_strength: float = 0.0
    korea_detail: Dict = field(default_factory=dict)
    # 최종
    total_score: float = 0.0
    signal: str = "🟡"
    signal_text: str = "관망"
    recommended_sectors: List[str] = field(default_factory=list)
    # JARVIS 섹터 매핑
    nxt_targets: List[Dict] = field(default_factory=list)
    macro_conditions: Dict = field(default_factory=dict)
    selection_reason: str = ""
    # 진입 재개 시그널 (전쟁 장기화 감시)
    reentry_signals: Dict = field(default_factory=dict)
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
            {"code": "034020", "name": "두산에너빌리티"},
        ],
        "relay": "AI DC착공→GEV/ETN/VRT→HD현대일렉/효성중공업→LS전선→두산에너빌리티",
    },
    "oil_resource": {
        "name": "🛢 원유·자원",
        "us": ["XOM", "CVX", "COP", "SLB", "LNG", "HAL"],
        "kr_tier1": [
            {"code": "010950", "name": "S-Oil"},
            {"code": "096770", "name": "SK이노베이션"},
            {"code": "078930", "name": "GS"},                   # GS칼텍스 모회사
        ],
        "kr_tier2": [
            # 석유화학 (원유 가격 수혜)
            {"code": "006650", "name": "대한유화"},              # NCC 기초원료
            {"code": "011170", "name": "롯데케미칼"},            # 석유화학 대형
            {"code": "009830", "name": "한화솔루션"},            # 케미칼 사업부
            {"code": "014530", "name": "극동유화"},              # 석유화학
            # EPC / 인프라
            {"code": "028050", "name": "삼성엔지니어링"},        # 정유/LNG 플랜트 EPC
            {"code": "047050", "name": "포스코인터내셔널"},      # 원유/가스 E&P + 트레이딩
        ],
        "relay": "WTI$80+→정유(S-Oil/GS/SK이노)마진→삼성엔지니어링EPC→대한유화NCC→롯데케미칼/한화솔루션",
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
        "name": "🚢 조선·LNG선",
        "us": ["HII", "GD"],
        "kr_tier1": [
            {"code": "009540", "name": "HD한국조선해양"},
            {"code": "042660", "name": "한화오션"},
            {"code": "010140", "name": "삼성중공업"},             # LNG선 수주잔고 최대
        ],
        "kr_tier2": [
            {"code": "010620", "name": "HD현대미포"},
            {"code": "329180", "name": "HD현대마린솔루션"},
            {"code": "034020", "name": "두산에너빌리티"},          # 가스터빈+LNG 인프라 재건
            {"code": "028050", "name": "삼성엔지니어링"},          # LNG 플랜트 EPC
        ],
        "relay": "LNG공급차질/NG급등→LNG선발주→HD한국조선/한화오션/삼성중공업→미포→두산에너빌리티(터빈)→삼성엔지니어링(EPC)",
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
    # ── 원자재 릴레이 섹터 ──
    "natural_gas": {
        "name": "🔥 천연가스·LNG",
        "us": ["LNG", "AR", "EQT", "RRC", "SWN", "TELL"],
        "kr_tier1": [
            {"code": "036460", "name": "한국가스공사"},          # LNG 수입/공급 독점
            {"code": "018670", "name": "SK가스"},                # LPG/LNG 유통
            {"code": "004690", "name": "삼천리"},                # 도시가스 + LNG 발전
        ],
        "kr_tier2": [
            # 유통/도시가스 밸류체인
            {"code": "017940", "name": "E1"},                    # LPG/에너지 유통
            {"code": "006120", "name": "SK디스커버리"},           # SK가스 모회사
            {"code": "017390", "name": "서울가스"},
            {"code": "034590", "name": "인천도시가스"},
            {"code": "267290", "name": "경동도시가스"},
            {"code": "117580", "name": "대성에너지"},
            {"code": "016710", "name": "대성홀딩스"},            # 대성에너지 모회사
            {"code": "015360", "name": "예스코홀딩스"},           # 도시가스
            # LNG 소부장 (설비/부품) ★핵심★
            {"code": "017960", "name": "한국카본"},              # LNG 보냉재 (세계1위급)
            {"code": "033500", "name": "동성화인텍"},            # LNG 단열재
            {"code": "014620", "name": "성광벤드"},              # LNG/조선 배관 피팅
            {"code": "023160", "name": "태광"},                  # LNG 밸브/피팅
            # EPC / 트레이딩
            {"code": "028050", "name": "삼성엔지니어링"},        # LNG 플랜트 EPC
            {"code": "047050", "name": "포스코인터내셔널"},      # LNG 트레이딩/개발
        ],
        "relay": "DC전력수요→NG가격→가스공사/SK가스/삼천리→도시가스유통→한국카본/동성화인텍(보냉재)→성광벤드/태광(배관)",
    },
    "precious_metals": {
        "name": "✨ 귀금속(금·은)",
        "us": ["GLD", "SLV", "NEM", "AEM", "WPM", "PAAS"],
        "kr_tier1": [
            {"code": "010130", "name": "고려아연"},              # 세계 최대 아연/은 제련
            {"code": "000670", "name": "영풍"},                  # 고려아연 모회사
        ],
        "kr_tier2": [
            {"code": "103140", "name": "풍산"},                  # 조폐공사 금/은 동전 원료
            {"code": "005810", "name": "풍산홀딩스"},
            {"code": "036560", "name": "영풍정밀"},              # 영풍 자회사 (비철금속 가공)
            {"code": "018470", "name": "조일알미늄"},            # 비철 가공
        ],
        "relay": "인플레+스태그→금급등→은2배추종→고려아연(은부산물)/영풍→풍산(금속가공)→영풍정밀",
    },
    "industrial_metals": {
        "name": "🏗 산업금속(구리·비철)",
        "us": ["FCX", "SCCO", "TECK", "BHP", "RIO"],
        "kr_tier1": [
            {"code": "103140", "name": "풍산"},                  # 구리 가공 세계 3위
            {"code": "010130", "name": "고려아연"},              # 아연/납/동 제련
        ],
        "kr_tier2": [
            # 구리 밸류체인
            {"code": "006260", "name": "LS"},                    # 구리 가공/전선 지주사
            {"code": "229640", "name": "LS전선아시아"},           # 전력 케이블 (구리 대량소비)
            {"code": "005810", "name": "풍산홀딩스"},
            {"code": "025820", "name": "이구산업"},              # 동합금 가공
            # 알미늄/비철
            {"code": "018470", "name": "조일알미늄"},
            {"code": "008350", "name": "남선알미늄"},
            {"code": "006110", "name": "삼아알미늄"},
        ],
        "relay": "전기화+재생에너지→구리수요→풍산/LS(구리가공)→LS전선(케이블)→이구산업(동합금)→고려아연(제련)→알미늄",
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
    # ── 원자재 ETF (릴레이 연동) ──
    "commodity_etf_gold": {
        "name": "🥇 금 ETF",
        "us": ["GLD", "IAU"],
        "kr_tier1": [
            {"code": "132030", "name": "KODEX 골드선물(H)", "is_etf": True},
            {"code": "411060", "name": "ACE KRX금현물", "is_etf": True},
        ],
        "kr_tier2": [],
        "relay": "금가격상승→골드ETF",
    },
    "commodity_etf_oil": {
        "name": "🛢 원유 ETF",
        "us": ["USO", "BNO"],
        "kr_tier1": [
            {"code": "261220", "name": "KODEX WTI원유선물(H)", "is_etf": True},
            {"code": "130680", "name": "TIGER 원유선물Enhanced(H)", "is_etf": True},
        ],
        "kr_tier2": [],
        "relay": "원유가격상승→원유ETF",
    },
    "commodity_etf_metals": {
        "name": "🏗 금속 ETF",
        "us": ["COPX", "SLV"],
        "kr_tier1": [
            {"code": "139320", "name": "TIGER 금속선물(H)", "is_etf": True},
        ],
        "kr_tier2": [],
        "relay": "구리/은상승→금속ETF",
    },
}

# ── KRX 업종명 → JARVIS 섹터 키 매핑 (섹터 모멘텀 연동) ──
KRX_TO_JARVIS = {
    "전기전자": ["semiconductor"],
    "금융": ["securities"],
    "제약": ["bio"],
    "의약품": ["bio"],
    "운송장비": ["shipbuilding", "space_defense"],
    "기계장비": ["power_infra"],
    "화학": ["battery_ev"],
    "금속": ["industrial_metals", "precious_metals"],
    "전기가스": ["power_infra", "natural_gas"],
    "서비스": ["entertainment"],
    "건설": ["reits"],
    "통신": ["software_ai"],
    "운수창고": ["shipbuilding"],
    "철강": ["industrial_metals"],
}

SECTOR_MOMENTUM_PATH = DATA_DIR / "sector_momentum.json"


def _get_hot_sector_targets(max_sectors: int = 3) -> Tuple[List[str], List[Dict], str]:
    """섹터 모멘텀에서 HOT/WARMING 섹터 → JARVIS 종목 + top movers 반환

    Returns: (jarvis_keys, direct_targets, reason_str)
      - jarvis_keys: JARVIS_SECTORS 매핑된 키 목록
      - direct_targets: JARVIS 미매핑 HOT 섹터 top movers (직접 주입)
      - reason_str: 사유 문자열
    """
    if not SECTOR_MOMENTUM_PATH.exists():
        return [], [], ""
    try:
        data = json.loads(SECTOR_MOMENTUM_PATH.read_text("utf-8"))
    except Exception:
        return [], [], ""

    hot_sectors = [
        s for s in data.get("sectors", [])
        if s.get("phase") in ("HOT", "WARMING")
    ]
    if not hot_sectors:
        return [], [], ""

    hot_sectors.sort(key=lambda s: s.get("boost_score", 0), reverse=True)
    hot_sectors = hot_sectors[:max_sectors]

    jarvis_keys = []
    direct_targets = []
    sector_names = []

    for s in hot_sectors:
        krx_name = s.get("sector", "")
        phase = s.get("phase", "")
        boost = s.get("boost_score", 0)
        sector_names.append(f"{krx_name}({phase})")

        # JARVIS 매핑 시도
        mapped = KRX_TO_JARVIS.get(krx_name, [])
        for key in mapped:
            if key not in jarvis_keys:
                jarvis_keys.append(key)

        # JARVIS 미매핑 OR top movers 직접 주입 (상위 3개)
        for m in s.get("top_movers", [])[:3]:
            code = m.get("code", "")
            if code and not any(t.get("code") == code for t in direct_targets):
                direct_targets.append({
                    "code": code,
                    "name": m.get("name", code),
                    "sector": krx_name,
                    "sector_key": "sector_momentum",
                    "tier": 1,
                    "priority": len(jarvis_keys) + 1,
                    "chg_1d": m.get("chg_1d", 0),
                    "boost_score": boost,
                })

    reason = "🔥 " + ", ".join(sector_names)
    return jarvis_keys, direct_targets, reason


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
            penalty -= 2.5  # NXT-03: was -3.0 → -2.5
            divergences.append(f"💀 VIX {vix.value:.1f} - 패닉 구간")
        elif vix.value >= 25:
            vix.signal = "🔴"
            penalty -= 1.5  # NXT-03: was -2.0 → -1.5 (25는 "불안"이지 "공포"가 아님)
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
            penalty -= 1.5  # NXT-03: was -2.0 → -1.5
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
            penalty -= 1.0  # NXT-03: was -1.5 → -1.0
            divergences.append(
                f"🔴 TNX {tnx.change_abs:+.3f} + Gold {gold.change_pct:+.2f}%"
                f" - 안전자산 쏠림"
            )

    # --- 괴리 3: 원/달러 급등 ---
    usdkrw = _fetch_ticker("KRW=X")
    raw["USDKRW"] = asdict(usdkrw) if usdkrw else {}
    if usdkrw and usdkrw.change_pct is not None:
        if usdkrw.change_pct > 0.5:  # 원화 약세 급등
            penalty -= 0.7  # NXT-03: was -1.0 → -0.7
            divergences.append(
                f"🔴 원/달러 {usdkrw.change_pct:+.2f}% - 원화 급락"
            )
        elif usdkrw.change_pct < -0.3:  # 원화 강세
            penalty += 0.5

    return round(penalty, 1), divergences, raw


# ═══════════════════════════════════════════════════
#  [4.5단] 한국장 강도 (NXT 캘리브레이션 v2)
#  매크로 공포만으로 판단하지 않고, 한국 시장 실제 상태 반영
#  "시장이 안 좋아도 오를 놈은 올라" — 이걸 수치화
# ═══════════════════════════════════════════════════
def _calc_korea_strength() -> Tuple[float, Dict]:
    """
    한국장 자체 강도 판단
    범위: -3.0 ~ +5.0

    매크로가 무서워도 한국이 강하면 양수 → 공포 상쇄
    매크로가 좋아도 한국이 약하면 음수 → 낙관 견제
    """
    score = 0.0
    detail = {}

    # ── (1) 기관/외인 순매수 (pykrx) ──
    try:
        from pykrx import stock as pykrx_stock
        from datetime import date, timedelta as td

        today = date.today()
        week_ago = (today - td(days=10)).strftime("%Y%m%d")
        today_str = today.strftime("%Y%m%d")

        # 기관/외인 매매동향 (KOSPI)
        tv_df = pykrx_stock.get_market_trading_value_by_date(
            week_ago, today_str, "KOSPI"
        )
        if tv_df is not None and len(tv_df) >= 1:
            latest = tv_df.iloc[-1]

            # 기관 순매수 (원 → 억원)
            inst_raw = latest.get("기관합계", 0)
            inst_buy = inst_raw / 1_0000_0000 if abs(inst_raw) > 1000 else inst_raw
            if inst_buy >= 3000:
                score += 1.5
            elif inst_buy >= 1000:
                score += 0.8
            elif inst_buy <= -3000:
                score -= 1.0
            elif inst_buy <= -1000:
                score -= 0.5
            detail["기관순매수"] = f"{inst_buy:+,.0f}억"

            # 외인 순매수
            for_raw = latest.get("외국인합계", 0)
            for_buy = for_raw / 1_0000_0000 if abs(for_raw) > 1000 else for_raw
            if for_buy >= 2000:
                score += 1.0
            elif for_buy >= 500:
                score += 0.5
            elif for_buy <= -2000:
                score -= 0.7
            elif for_buy <= -500:
                score -= 0.3
            detail["외인순매수"] = f"{for_buy:+,.0f}억"

    except Exception as e:
        logger.warning(f"[KR 강도] 기관/외인 데이터 실패: {e}")

    # ── (3) 코스피 기술적 위치 + (5) 거래대금 활력 ──
    try:
        from pykrx import stock as pykrx_stock
        from datetime import date, timedelta as td

        today = date.today()
        start_90 = (today - td(days=120)).strftime("%Y%m%d")
        today_str = today.strftime("%Y%m%d")

        idx_df = pykrx_stock.get_index_ohlcv(start_90, today_str, "1001")
        if idx_df is not None and len(idx_df) >= 60:
            closes = idx_df["종가"]
            current = closes.iloc[-1]
            ma5 = closes.iloc[-5:].mean()
            ma20 = closes.iloc[-20:].mean()
            ma60 = closes.iloc[-60:].mean()

            if current > ma20 and current > ma5:
                score += 1.0
                detail["기술적위치"] = "5+20일선 위 (상승추세)"
            elif current > ma20:
                score += 0.5
                detail["기술적위치"] = "20일선 위"
            elif current < ma60:
                score -= 1.0
                detail["기술적위치"] = "60일선 아래 (약세)"
            elif current < ma20:
                score -= 0.5
                detail["기술적위치"] = "20일선 아래"
            else:
                detail["기술적위치"] = "중립"

            # 거래대금 활력
            if "거래대금" in idx_df.columns and len(idx_df) >= 20:
                tv_today = idx_df["거래대금"].iloc[-1]
                tv_avg20 = idx_df["거래대금"].iloc[-20:].mean()
                if tv_avg20 > 0:
                    tv_ratio = tv_today / tv_avg20
                    if tv_ratio >= 1.5:
                        score += 1.0
                    elif tv_ratio >= 1.0:
                        score += 0.3
                    elif tv_ratio < 0.7:
                        score -= 0.5
                    detail["거래대금활력"] = f"{tv_ratio:.2f}x"

    except Exception as e:
        logger.warning(f"[KR 강도] 코스피 기술적 데이터 실패: {e}")

    # ── (4) 전일 추천 적중률 (봇 성과 기반) ──
    try:
        insights_path = DATA_DIR / "learning" / "insights.json"
        if insights_path.exists():
            insights = json.loads(insights_path.read_text("utf-8"))
            hit_rate = insights.get("overall_hit_rate", 50)
            if hit_rate >= 70:
                score += 0.5
            elif hit_rate < 50:
                score -= 0.3
            detail["봇적중률"] = f"{hit_rate:.0f}%"
    except Exception:
        pass

    final = max(-3.0, min(5.0, round(score, 1)))
    detail["score"] = final
    logger.info(f"[KR 강도] 한국장 강도: {final:+.1f} | {detail}")
    return final, detail


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
    if hg and hg.change_pct is not None:
        conditions["copper_up"] = hg.change_pct > 1.0
        conditions["copper_pct"] = hg.change_pct

    # --- NG: 천연가스 선물 (DC전력 수요 바로미터) ---
    ng = _fetch_ticker("NG=F")
    additional_raw["NG"] = asdict(ng) if ng else {}
    if ng and ng.change_pct is not None:
        conditions["ng_up"] = ng.change_pct > 2.0
        conditions["ng_down"] = ng.change_pct < -2.0
        conditions["ng_pct"] = ng.change_pct
        conditions["ng_price"] = ng.value  # 원가 대비 위치 판단용

    # --- SI: 은 선물 (금 릴레이 추종) ---
    si = _fetch_ticker("SI=F")
    additional_raw["SI"] = asdict(si) if si else {}
    if si and si.change_pct is not None:
        conditions["silver_up"] = si.change_pct > 1.0
        conditions["silver_pct"] = si.change_pct

    # --- 원자재 릴레이 감지: 금→은→구리 순서 ---
    gold_data = raw_indicators.get("GOLD", {})
    gold_pct = gold_data.get("change_pct", 0) or 0
    silver_pct = conditions.get("silver_pct", 0) or 0
    copper_pct = conditions.get("copper_pct", 0) or 0
    # 은이 금보다 더 많이 오르면 릴레이 진행 중
    if silver_pct > 0.5 and silver_pct > gold_pct:
        conditions["commodity_relay"] = "silver"  # 은 단계
    elif copper_pct > 0.5 and copper_pct > gold_pct:
        conditions["commodity_relay"] = "copper"  # 구리 단계
    elif gold_pct > 0.5:
        conditions["commodity_relay"] = "gold"    # 금 선행 단계

    # --- TNX: 금리 방향 (detect_divergence에서 이미 수집) ---
    tnx = raw_indicators.get("TNX", {})
    if tnx.get("change_abs") is not None:
        conditions["rate_down"] = tnx["change_abs"] < -0.04
        conditions["rate_up"] = tnx["change_abs"] > 0.04

    # BOND-01: 채권 환경 세분화 (수준/추세/크레딧 콤보)
    try:
        from data.bond_yield_signal import analyze_bond_environment
        bond_env = analyze_bond_environment(raw_indicators, fetch_trend=False)
        if bond_env.us10y_value > 0:
            conditions["bond_verdict"] = bond_env.verdict
            conditions["bond_score"] = bond_env.composite_score
            conditions["us10y_value"] = bond_env.us10y_value
            conditions["us10y_level"] = bond_env.level_class
    except Exception:
        pass

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
    # 채권 환경 verdict가 있으면 더 풍부한 텍스트 사용
    bond_v = conditions.get("bond_verdict")
    if bond_v and bond_v != "NEUTRAL":
        bond_kr = {"DOVISH": "금리완화", "HAWKISH": "금리긴축",
                   "TIGHTENING_SHOCK": "금리쇼크"}
        us10y_val = conditions.get("us10y_value", 0)
        active.append(f"{bond_kr.get(bond_v, bond_v)}({us10y_val:.1f}%)")
    elif conditions.get("rate_down"):
        active.append("금리↓")
    elif conditions.get("rate_up"):
        active.append("금리↑")
    if conditions.get("oil_up"):
        active.append(f"유가↑ {conditions.get('oil_pct', 0):+.1f}%")
    if conditions.get("cnh_strong"):
        active.append("CNH강세")
    if conditions.get("geopolitical"):
        active.append("지정학 긴장")
    if conditions.get("ng_up"):
        active.append(f"천연가스↑ {conditions.get('ng_pct', 0):+.1f}%")
    if conditions.get("silver_up"):
        active.append(f"은↑ {conditions.get('silver_pct', 0):+.1f}%")
    if conditions.get("copper_up"):
        active.append(f"구리↑ {conditions.get('copper_pct', 0):+.1f}%")
    relay = conditions.get("commodity_relay")
    if relay:
        relay_map = {"gold": "금선행", "silver": "금→은 릴레이", "copper": "금→은→구리 릴레이"}
        active.append(f"원자재릴레이({relay_map.get(relay, relay)})")
    conditions["active_text"] = active

    return conditions, additional_raw


# ═══════════════════════════════════════════════════
#  [5단] 진입 재개 시그널 감시 (WAR GATE)
#  "전쟁 장기화 국면 — 3가지 조건 중 하나라도 충족 시 알림"
#  1. WTI $95 이하 복귀
#  2. VIX 30 이하 안정화
#  3. KOSPI 4,800~5,000 패닉 바닥 터치
# ═══════════════════════════════════════════════════
REENTRY_THRESHOLDS = {
    "wti_safe": 95.0,       # WTI 이 이하로 내려오면 매수 재개 가능
    "vix_calm": 30.0,       # VIX 이 이하면 공포 완화
    "kospi_bottom_low": 4800,   # 패닉 바닥 하단
    "kospi_bottom_high": 5000,  # 패닉 바닥 상단
    "min_signals": 2,       # 최소 2개 이상 충족 시에만 재개 (1개론 부족)
}


def check_reentry_signals(raw_indicators: Dict, macro_conditions: Dict) -> Dict:
    """
    전쟁 장기화 국면 진입 재개 시그널 체크
    Returns: {
        "war_gate_active": True,   # 전쟁 게이트 발동 중
        "wti": {"value": 100.2, "safe": False, "status": "..."},
        "vix": {"value": 32.1, "safe": False, "status": "..."},
        "kospi": {"value": 5180, "bottom": False, "status": "..."},
        "reentry_ok": False,       # min_signals개 이상 충족 시 True
        "summary": "..."
    }
    """
    result = {
        "war_gate_active": True,
        "wti": {"value": None, "safe": False, "status": "데이터 없음"},
        "vix": {"value": None, "safe": False, "status": "데이터 없음"},
        "kospi": {"value": None, "bottom": False, "status": "데이터 없음"},
        "reentry_ok": False,
        "signals_met": [],
        "summary": "",
    }

    # --- WTI 원유 ---
    cl_data = raw_indicators.get("CL", {})
    wti_price = cl_data.get("value")
    if wti_price is not None:
        safe = wti_price <= REENTRY_THRESHOLDS["wti_safe"]
        result["wti"] = {
            "value": wti_price,
            "safe": safe,
            "threshold": REENTRY_THRESHOLDS["wti_safe"],
            "status": f"${wti_price:.1f} {'<= $95 안전' if safe else '> $95 위험'}",
        }
        if safe:
            result["signals_met"].append(f"WTI ${wti_price:.1f} <= $95")

    # --- VIX ---
    vix_data = raw_indicators.get("VIX", {})
    vix_val = vix_data.get("value")
    if vix_val is not None:
        calm = vix_val <= REENTRY_THRESHOLDS["vix_calm"]
        result["vix"] = {
            "value": vix_val,
            "safe": calm,
            "threshold": REENTRY_THRESHOLDS["vix_calm"],
            "status": f"VIX {vix_val:.1f} {'<= 30 안정' if calm else '> 30 공포'}",
        }
        if calm:
            result["signals_met"].append(f"VIX {vix_val:.1f} <= 30")

    # --- KOSPI (^KS11) ---
    try:
        kospi = _fetch_ticker("^KS11")
        if kospi and kospi.value is not None:
            kv = kospi.value
            lo = REENTRY_THRESHOLDS["kospi_bottom_low"]
            hi = REENTRY_THRESHOLDS["kospi_bottom_high"]
            is_bottom = lo <= kv <= hi
            result["kospi"] = {
                "value": kv,
                "bottom": is_bottom,
                "threshold": f"{lo}~{hi}",
                "status": f"KOSPI {kv:.0f} {'패닉바닥 도달!' if is_bottom else ('아직 높음' if kv > hi else '이미 하회')}",
            }
            if is_bottom:
                result["signals_met"].append(f"KOSPI {kv:.0f} 패닉바닥({lo}~{hi})")
            elif kv < lo:
                # 바닥 이하 = 이미 더 빠짐 → 역발상 기회일 수도
                result["signals_met"].append(f"KOSPI {kv:.0f} < {lo} 극단 패닉")
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] KOSPI 수집 실패: {e}")

    # --- 종합 ---
    min_req = REENTRY_THRESHOLDS["min_signals"]
    met_count = len(result["signals_met"])
    result["reentry_ok"] = met_count >= min_req

    statuses = []
    if result["wti"]["value"] is not None:
        statuses.append(result["wti"]["status"])
    if result["vix"]["value"] is not None:
        statuses.append(result["vix"]["status"])
    if result["kospi"]["value"] is not None:
        statuses.append(result["kospi"]["status"])

    if result["reentry_ok"]:
        result["summary"] = f"진입 재개 ({met_count}/{min_req} 충족): " + " / ".join(result["signals_met"])
    elif met_count > 0:
        result["summary"] = (
            f"부분 충족 ({met_count}/{min_req}): "
            + " / ".join(result["signals_met"])
            + " | 나머지: " + " | ".join(statuses)
        )
    else:
        result["summary"] = f"진입 대기 (0/{min_req}): " + " | ".join(statuses)

    return result


# ═══════════════════════════════════════════════════
#  NXT 개별 수급 필터 헬퍼
# ═══════════════════════════════════════════════════
def _load_tv_scanner_data() -> Dict[str, Dict]:
    """tv_scanner.json → {code: {tv_ratio, pattern, score, change_pct}}"""
    path = DATA_DIR / "tv_scanner.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text("utf-8"))
        signals = data.get("signals", [])
        return {item["code"]: item for item in signals if "code" in item}
    except Exception:
        return {}


def _load_investor_flow(code: str, days: int = 3) -> Dict:
    """flow/{code}_investor.csv → 최근 N일 외인/기관 순매수 방향"""
    path = DATA_DIR / "flow" / f"{code}_investor.csv"
    if not path.exists():
        return {"foreign_buy_days": 0, "inst_buy_days": 0}
    try:
        import csv
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                rows.append(row)
        # 최근 N일 (마지막 N행)
        recent = rows[-days:] if len(rows) >= days else rows
        # 컬럼 인덱스: 0=date, 1=기관_금액, 4=외국인_금액
        foreign_idx = None
        inst_idx = None
        for i, h in enumerate(header):
            if "외국인" in h and "금액" in h:
                foreign_idx = i
            elif "기관" in h and "금액" in h:
                inst_idx = i
        foreign_buy = 0
        inst_buy = 0
        for row in recent:
            try:
                if foreign_idx is not None and row[foreign_idx].strip():
                    val = float(row[foreign_idx])
                    if val > 0:
                        foreign_buy += 1
            except (ValueError, IndexError):
                pass
            try:
                if inst_idx is not None and row[inst_idx].strip():
                    val = float(row[inst_idx])
                    if val > 0:
                        inst_buy += 1
            except (ValueError, IndexError):
                pass
        return {"foreign_buy_days": foreign_buy, "inst_buy_days": inst_buy}
    except Exception:
        return {"foreign_buy_days": 0, "inst_buy_days": 0}


def _score_individual_supply(targets: List[Dict]) -> List[Dict]:
    """개별 종목 수급 점수 산출 (0~100). ETF는 면제(100점)."""
    tv_data = _load_tv_scanner_data()
    for t in targets:
        if t.get("is_etf"):
            t["supply_score"] = 100
            continue
        code = t["code"]
        score = 0

        # TV 거래대금 비율 (35점)
        tv = tv_data.get(code, {})
        tv_ratio = tv.get("tv_ratio", 0) or 0
        if tv_ratio >= 2.0:
            score += 35
        elif tv_ratio >= 1.5:
            score += 25
        elif tv_ratio >= 1.0:
            score += 15

        # TV 패턴 보너스 (15점)
        pattern = tv.get("pattern", "")
        if pattern == "EXPLOSION":
            score += 15
        elif "QUIET" in pattern:
            score += 10
        elif "GRADUAL" in pattern:
            score += 5

        # 등락률 양봉 (10점)
        if (tv.get("change_pct", 0) or 0) > 0:
            score += 10

        # 투자자 수급 — 외국인 (25점)
        flow = _load_investor_flow(code)
        foreign_days = flow.get("foreign_buy_days", 0)
        if foreign_days >= 3:
            score += 25
        elif foreign_days >= 2:
            score += 15
        elif foreign_days >= 1:
            score += 8

        # 투자자 수급 — 기관 (15점)
        inst_days = flow.get("inst_buy_days", 0)
        if inst_days >= 3:
            score += 15
        elif inst_days >= 2:
            score += 10
        elif inst_days >= 1:
            score += 5

        t["supply_score"] = score
    return targets


# ═══════════════════════════════════════════════════
#  JARVIS 섹터 선정 + NXT 대상 종목 매칭
# ═══════════════════════════════════════════════════
def select_sectors_and_targets(
    total_score: float,
    macro: Dict,
    max_tier: int = 1,
    korea_strength: float = 0.0,  # NXT-02: 한국장 강도 전달
) -> Tuple[List[str], List[Dict], str]:
    """
    NIGHTWATCH 점수 + 매크로 조건 → 섹터 우선순위 → NXT 대상 종목
    PDF p.16 NIGHTWATCH 섹터 매핑 테이블 구현

    Returns: (sector_display_names, nxt_targets, selection_reason)
    """
    selected_keys = []
    reason = ""
    hot_direct = []  # 섹터 모멘텀 HOT top movers (관망 시 직접 주입)

    # ── 원자재 릴레이 보조 섹터 (점수 무관, 조건 충족 시 추가) ──
    commodity_addon = []
    commodity_reason = ""
    relay = macro.get("commodity_relay")
    if relay == "silver":
        commodity_addon = ["precious_metals"]
        commodity_reason = " + 금→은릴레이"
    elif relay == "copper":
        commodity_addon = ["industrial_metals"]
        commodity_reason = " + 금→은→구리릴레이"
    # [G2] 은↑/구리↑ 독립 addon (릴레이 아니어도 단독 급등 시)
    if macro.get("silver_up") and "precious_metals" not in commodity_addon:
        commodity_addon.append("precious_metals")
        commodity_reason += " + 은↑"
    if macro.get("copper_up") and "industrial_metals" not in commodity_addon:
        commodity_addon.append("industrial_metals")
        commodity_reason += " + 구리↑"
    # [G10] 구리↑ → 2차전지 (EV 배터리 = 구리 대량 소비)
    if macro.get("copper_up") and "battery_ev" not in commodity_addon:
        commodity_addon.append("battery_ev")
    if macro.get("ng_up"):
        commodity_addon.append("natural_gas")
        commodity_addon.append("shipbuilding")  # LNG 공급차질 → 조선 수혜
        commodity_reason += " + 천연가스↑+LNG선"
    # [G5] 유가↑ commodity_addon (NG처럼 점수 무관 추가)
    if macro.get("oil_up"):
        if "oil_resource" not in commodity_addon:
            commodity_addon.append("oil_resource")
        if "shipbuilding" not in commodity_addon:
            commodity_addon.append("shipbuilding")  # 탱커 수요
        commodity_reason += " + 유가↑"

    # ── 원자재 ETF 릴레이 자동 추가 ──
    _etf_addon_map = {
        "precious_metals": "commodity_etf_gold",
        "oil_resource": "commodity_etf_oil",
        "industrial_metals": "commodity_etf_metals",
    }
    for stock_key, etf_key in _etf_addon_map.items():
        if stock_key in commodity_addon and etf_key not in commodity_addon:
            commodity_addon.append(etf_key)

    # ── 강매수 (5+) ──
    if total_score >= 5:
        if macro.get("nasdaq_up"):
            selected_keys = ["semiconductor", "software_ai", "power_infra"]  # [G9] AI DC 전력수요
            reason = "🟢🟢 + 나스닥↑"
        elif macro.get("rate_down"):
            selected_keys = ["reits", "securities", "power_infra", "bio"]  # [G1] 바이오=금리인하 수혜
            reason = "🟢🟢 + 금리↓"
        elif macro.get("oil_up"):
            selected_keys = ["oil_resource", "shipbuilding", "natural_gas"]
            reason = "🟢🟢 + 유가↑ + LNG"
        elif macro.get("ng_up"):
            selected_keys = ["natural_gas", "shipbuilding", "oil_resource"]
            reason = "🟢🟢 + 천연가스↑+LNG선"
        elif macro.get("geopolitical"):
            selected_keys = ["space_defense", "shipbuilding", "oil_resource", "natural_gas"]  # [G4] 지정학→에너지
            reason = "🟢🟢 + 지정학 긴장"
        else:
            selected_keys = ["semiconductor", "power_infra"]
            reason = "🟢🟢 기본"

    # ── 매수 (2~4.9) ──
    elif total_score >= 2:
        if macro.get("nasdaq_up"):
            selected_keys = ["semiconductor", "battery_ev", "power_infra"]  # [G9] AI DC 전력
            reason = "🟢 + 나스닥↑"
        elif macro.get("cnh_strong"):
            selected_keys = ["semiconductor", "entertainment", "battery_ev"]  # CNH강세→EV수출
            reason = "🟢 + CNH강세"
        elif macro.get("oil_up"):
            selected_keys = ["oil_resource", "shipbuilding"]
            reason = "🟢 + 유가↑"
        elif macro.get("ng_up"):
            selected_keys = ["natural_gas", "shipbuilding", "oil_resource"]
            reason = "🟢 + 천연가스↑+LNG선"
        elif macro.get("geopolitical"):
            selected_keys = ["space_defense", "shipbuilding", "oil_resource"]  # [G4] 지정학→에너지
            reason = "🟢 + 지정학 긴장"
        elif macro.get("rate_down"):
            selected_keys = ["reits", "securities", "bio"]  # [G1] 바이오=금리인하 수혜
            reason = "🟢 + 금리↓"
        else:
            selected_keys = ["semiconductor", "power_infra"]
            reason = "🟢 기본"

    # ── NXT-02: 인버스 추천 강화 — "진짜 패닉"만 인버스 ──
    # 매크로만 나빠도 한국장이 강하면 인버스 안 함
    # VIX 30+ AND 한국 약세일 때만 인버스 (VIX 25~30은 관망)
    elif total_score < -5:
        # 한국장 강도가 +1.0 이상이면 → 인버스 안 함 (한국은 강함)
        if korea_strength >= 1.0:
            return [], [], f"🟡 매크로 불안하나 한국장 강세({korea_strength:+.1f}) → 관망"
        # raw VIX 확인: 30 미만이면서 한국이 약하지 않으면 → 진입금지(관망)
        vix_val = macro.get("raw_vix", 0)
        if vix_val < 30 and korea_strength > -1.0:
            return [], [], f"🔴 VIX {vix_val:.0f} + 한국({korea_strength:+.1f}) → 인버스 아닌 관망"
        # 진짜 패닉: VIX 30+ OR 한국도 약세(-1.0 이하)
        selected_keys = ["inverse"]
        reason = f"💀 극단 하락 + 한국 약세({korea_strength:+.1f}) → 인버스 헤지"

    # ── 관망 (-1.9 ~ 1.9) → 섹터 모멘텀 HOT 확인 ──
    else:
        hot_keys, hot_direct, hot_reason = _get_hot_sector_targets(max_sectors=3)
        if hot_keys or hot_direct:
            selected_keys = hot_keys if hot_keys else []
            reason = f"🟡→🔥 관망 + {hot_reason}"
        else:
            return [], [], "🟡 관망 - 진입 없음"

    # 원자재 릴레이 보조 섹터 병합 (중복 제거, 점수 무관, 별도 if)
    if commodity_addon:
        for key in commodity_addon:
            if key not in selected_keys:
                selected_keys.append(key)
        reason += commodity_reason

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
                "is_etf": stock.get("is_etf", False),
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
                    "is_etf": stock.get("is_etf", False),
                })

    # HOT 섹터 top movers 직접 주입 (JARVIS 미매핑 종목)
    if hot_direct:
        existing_codes = {t["code"] for t in nxt_targets}
        for dt in hot_direct:
            if dt["code"] not in existing_codes:
                nxt_targets.append(dt)
                existing_codes.add(dt["code"])

    # ── 개별 수급 필터 ──
    pre_count = len(nxt_targets)
    nxt_targets = _score_individual_supply(nxt_targets)

    # ETF 분리 (필터 면제)
    etf_targets = [t for t in nxt_targets if t.get("is_etf")]
    stock_targets = [t for t in nxt_targets if not t.get("is_etf")]

    # 최소 임계값 필터
    min_score_t1, min_score_t2 = 20, 30
    filtered = [
        t for t in stock_targets
        if t.get("supply_score", 0) >= (min_score_t1 if t.get("tier") == 1 else min_score_t2)
    ]

    # Fallback: 필터 후 주식 0개 → 임계값 낮춰 재시도 (최소 1개 확보)
    if not filtered and stock_targets:
        fallback_min = 10
        filtered = [t for t in stock_targets if t.get("supply_score", 0) >= fallback_min]
        if not filtered:
            # 최후 수단: supply_score 상위 3개
            filtered = sorted(stock_targets, key=lambda x: -x.get("supply_score", 0))[:3]
        logger.info(f"[NXT-SUPPLY] Fallback 적용: 임계값 {fallback_min}으로 완화 → {len(filtered)}개")

    # 섹터당 상위 2개 선별
    from collections import defaultdict
    sector_groups = defaultdict(list)
    for t in filtered:
        sector_groups[t.get("sector_key", "unknown")].append(t)
    final_stocks = []
    for _key, stocks in sector_groups.items():
        stocks.sort(key=lambda x: -x.get("supply_score", 0))
        final_stocks.extend(stocks[:2])

    nxt_targets = etf_targets + final_stocks

    # 정렬: 1순위 섹터 Tier1 → supply_score 내림차순
    nxt_targets.sort(key=lambda x: (x.get("priority", 99), x.get("tier", 99), -x.get("supply_score", 0)))

    logger.info(f"[NXT-SUPPLY] 필터 전 {pre_count}개 → 필터 후 {len(nxt_targets)}개")
    for t in nxt_targets[:10]:
        logger.info(f"  {t['name']} (supply={t.get('supply_score', 0)}, tier={t.get('tier')}, {t.get('sector_key')})")

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
    korea_strength: float = 0.0,       # NXT-01: 한국장 강도
    korea_detail: Optional[Dict] = None,  # NXT-01: 한국장 상세
) -> NightwatchReport:
    """4단 체인 + 한국장 강도 + 매크로 조건 종합 → JARVIS 섹터 매핑 → 최종 리포트"""
    # NXT-01: 4단 공식 — 매크로 공포 + 한국 실제 강도
    total = asian_score + europe_score + div_score + korea_strength

    # OPT-02: 옵션 심리 보정 (VIX 의존도 감소)
    opt_adj = 0.0
    try:
        from data.options_signal import get_nxt_options_adjustment
        opt_adj, opt_detail = get_nxt_options_adjustment()
        if abs(opt_adj) > 0:
            total += opt_adj
            logger.info(f"[NXT] 옵션심리 보정: {opt_adj:+.1f} ({opt_detail})")
    except Exception:
        pass

    # NQ(나스닥) 직접 보정 — 나스닥 등락이 NXT 갭업 핵심 드라이버
    nq_data = raw_indicators.get("NQ", {})
    nq_pct = nq_data.get("change_pct", 0) or 0
    nq_adj = 0.0
    if nq_pct >= 1.5:
        nq_adj = +2.0
    elif nq_pct >= 0.8:
        nq_adj = +1.0
    elif nq_pct >= 0.3:
        nq_adj = +0.5
    elif nq_pct <= -1.5:
        nq_adj = -2.0
    elif nq_pct <= -0.8:
        nq_adj = -1.0
    elif nq_pct <= -0.3:
        nq_adj = -0.5
    if abs(nq_adj) > 0:
        total += nq_adj
        logger.info(f"[NXT] NQ 직접 보정: {nq_adj:+.1f} (NQ {nq_pct:+.2f}%)")

    # BOND-01: 채권금리 방향 보정 (±1.5)
    bond_adj = 0.0
    try:
        from data.bond_yield_signal import get_nxt_bond_adjustment
        bond_adj, bond_detail = get_nxt_bond_adjustment(raw_indicators)
        if abs(bond_adj) > 0:
            total += bond_adj
            logger.info(f"[NXT] 채권금리 보정: {bond_adj:+.1f} ({bond_detail})")
    except Exception:
        pass

    total = max(-10.0, min(10.0, total))

    # NXT-04: 시그널 구간 재조정 (한국장 강도 반영)
    if total >= 6:
        signal, text = "🟢🟢", "강한 매수"
    elif total >= 3:
        signal, text = "🟢", "매수 고려"
    elif total >= -1:
        signal, text = "🟡", "관망"
    elif total >= -4:
        signal, text = "🔴", "진입 금지"
    elif total >= -7:
        signal, text = "💀", "전체 포지션 점검"
    else:
        signal, text = "☠️", "인버스 강력"

    # JARVIS 섹터 매핑 (매크로 조건 기반 + 한국장 강도)
    macro = macro_conditions or {}
    # raw VIX를 macro에 전달 (NXT-02 인버스 판정용)
    vix_raw = raw_indicators.get("VIX", {})
    if isinstance(vix_raw, dict) and "value" in vix_raw:
        macro["raw_vix"] = vix_raw["value"]

    sector_names, nxt_targets, selection_reason = select_sectors_and_targets(
        total_score=total,
        macro=macro,
        max_tier=2,
        korea_strength=korea_strength,  # NXT-02: 인버스 판정에 활용
    )

    return NightwatchReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        asian_score=asian_score,
        asian_detail=asian_detail,
        europe_score=europe_score,
        europe_detail=europe_detail,
        divergence_score=div_score,
        divergences=divergences,
        korea_strength=korea_strength,
        korea_detail=korea_detail or {},
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
#  [NXT 조기 알림] 사전 수집 + 캐시
# ═══════════════════════════════════════════════════
NXT_EARLY_DATA_PATH = DATA_DIR / "nxt_early_data.json"


def collect_nxt_early_data() -> dict:
    """NXT stages 1~4 사전 수집 (G6 C4E에서 C3 일봉수집과 독립 병렬 실행).

    stages 1(아시안), 2(유럽), 3(괴리) 병렬 → stage 4(매크로) 순차.
    결과를 nxt_early_data.json에 캐시하여 16:35 run_nightwatch()에서 재사용.
    """
    import concurrent.futures
    logger.info("[NXT-EARLY] 사전 데이터 수집 시작")

    results = {}

    # stages 1~3 병렬 (yfinance 네트워크 I/O — GIL 영향 적음)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=3, thread_name_prefix="nxt_early"
    ) as ex:
        f1 = ex.submit(collect_asian_risk)
        f2 = ex.submit(collect_europe_open)
        f3 = ex.submit(detect_divergence)

        try:
            asian_score, asian_detail = f1.result(timeout=90)
            results["asian_score"] = asian_score
            results["asian_detail"] = asian_detail
        except Exception as e:
            logger.warning(f"[NXT-EARLY] 1단 실패: {e}")
            results["asian_score"] = 0.0
            results["asian_detail"] = {}

        try:
            europe_score, europe_detail = f2.result(timeout=90)
            results["europe_score"] = europe_score
            results["europe_detail"] = europe_detail
        except Exception as e:
            logger.warning(f"[NXT-EARLY] 2단 실패: {e}")
            results["europe_score"] = 0.0
            results["europe_detail"] = {}

        try:
            div_score, divergences, raw = f3.result(timeout=90)
            results["div_score"] = div_score
            results["divergences"] = divergences
            results["raw"] = raw
        except Exception as e:
            logger.warning(f"[NXT-EARLY] 3단 실패: {e}")
            results["div_score"] = 0.0
            results["divergences"] = []
            results["raw"] = {}

    # stage 4: macro (stage 3 raw 필요 → 순차)
    try:
        macro_conditions, macro_raw = collect_macro_conditions(
            results.get("asian_detail", {}),
            results.get("raw", {}),
        )
        results["raw"].update(macro_raw)
        results["macro_conditions"] = macro_conditions
    except Exception as e:
        logger.warning(f"[NXT-EARLY] 4단 실패: {e}")
        results["macro_conditions"] = {}

    results["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 원자적 저장
    try:
        tmp = NXT_EARLY_DATA_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(NXT_EARLY_DATA_PATH)
        logger.info(f"[NXT-EARLY] 수집 완료 → {NXT_EARLY_DATA_PATH.name}")
    except Exception as e:
        logger.error(f"[NXT-EARLY] 저장 실패: {e}")

    return results


def _load_early_data_if_fresh(max_age_hours: float = 3.0) -> Optional[dict]:
    """nxt_early_data.json 로드 — max_age_hours 이내 데이터만 유효."""
    if not NXT_EARLY_DATA_PATH.exists():
        return None
    try:
        data = json.loads(NXT_EARLY_DATA_PATH.read_text("utf-8"))
        collected_at = data.get("collected_at", "")
        if not collected_at:
            return None
        dt = datetime.strptime(collected_at, "%Y-%m-%d %H:%M:%S")
        age_hours = (datetime.now() - dt).total_seconds() / 3600
        if age_hours > max_age_hours:
            logger.info(f"[NXT-EARLY] 캐시 만료 ({age_hours:.1f}h > {max_age_hours}h)")
            return None
        return data
    except Exception:
        return None


def format_nxt_pre_alert(early_data: dict) -> str:
    """사전 수집 데이터만으로 NXT 예비 알림 메시지 생성 (NASDAQ 방향 강조)."""
    asian_score = early_data.get("asian_score", 0.0)
    europe_score = early_data.get("europe_score", 0.0)
    div_score = early_data.get("div_score", 0.0)
    macro = early_data.get("macro_conditions", {})
    raw = early_data.get("raw", {})
    ad = early_data.get("asian_detail", {})
    ed = early_data.get("europe_detail", {})

    # 예비 총점 (4.5단 한국장 강도 미포함)
    partial_total = asian_score + europe_score + div_score
    partial_total = max(-10.0, min(10.0, partial_total))

    # NQ 방향 강조
    nq_pct = macro.get("nasdaq_pct", None)
    nq_line = ""
    nq_verdict = ""
    if nq_pct is not None:
        if nq_pct >= 1.0:
            nq_verdict = "NXT 고확신 진입 후보"
        elif nq_pct >= 0.5:
            nq_verdict = "NXT 진입 고려"
        elif nq_pct <= -1.0:
            nq_verdict = "NXT 진입 자제"
        else:
            nq_verdict = "중립"
        nq_line = f"NASDAQ: {nq_pct:+.2f}%"

    # 예비 판정
    if partial_total >= 4:
        pre_signal = "매수 유력"
    elif partial_total >= 1.5:
        pre_signal = "매수 고려"
    elif partial_total >= -1:
        pre_signal = "관망"
    else:
        pre_signal = "진입 자제"

    vix_val = raw.get("VIX", {}).get("value", "N/A")
    dax_pct = ed.get("DAX_30min", {}).get("change_pct", 0) or 0
    audjpy_pct = ad.get("AUD/JPY", {}).get("change_pct", 0) or 0

    ts = early_data.get("collected_at", "")[:16]
    lines = [
        f"[NXT 예비 알림] {ts}",
        "=" * 30,
        f"예비 점수: {partial_total:+.1f} (4.5단 미포함)",
        f"예비 판단: {pre_signal}",
        "",
    ]
    if nq_line:
        lines.append(f"[핵심] {nq_line}")
        if nq_verdict:
            lines.append(f"  => {nq_verdict}")
        lines.append("")
    lines.extend([
        f"DAX 30분: {dax_pct:+.2f}%",
        f"VIX: {vix_val}",
        f"AUD/JPY: {audjpy_pct:+.2f}%",
        f"유럽: {europe_score:+.1f} | 아시안: {asian_score:+.1f} | 괴리: {div_score:+.1f}",
        "",
        "* 16:35 최종 판단 후 NXT 대상 종목 확정",
        "* 한국장 강도 반영 시 점수 변동 가능",
        "=" * 30,
    ])
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════════════
def run_nightwatch() -> NightwatchReport:
    """NIGHTWATCH 전체 실행 (3단 체인 + JARVIS 섹터 매핑).

    nxt_early_data.json 캐시가 신선하면 stages 1~4를 스킵하여
    16:35 실행 시 5분 이내 완료.
    """
    logger.info("[NIGHTWATCH] 실행 시작")

    # ── 사전 수집 캐시 확인 (C4E에서 15:40~16:00에 수집된 경우) ──
    early_data = _load_early_data_if_fresh()

    if early_data:
        logger.info("[NIGHTWATCH] 사전 수집 캐시 사용 (stages 1~4 스킵)")
        asian_score = early_data.get("asian_score", 0.0)
        asian_detail = early_data.get("asian_detail", {})
        europe_score = early_data.get("europe_score", 0.0)
        europe_detail = early_data.get("europe_detail", {})
        div_score = early_data.get("div_score", 0.0)
        divergences = early_data.get("divergences", [])
        raw = early_data.get("raw", {})
        macro_conditions = early_data.get("macro_conditions", {})
    else:
        # 캐시 MISS → 기존 순차 실행 (fallback)
        logger.info("[NIGHTWATCH] [1단] 아시안 리스크 수집...")
        asian_score, asian_detail = collect_asian_risk()
        logger.info(f"[NIGHTWATCH] 아시안 스코어: {asian_score:+.1f}")

        logger.info("[NIGHTWATCH] [2단] 유럽 오픈 수집...")
        europe_score, europe_detail = collect_europe_open()
        logger.info(f"[NIGHTWATCH] 유럽 스코어: {europe_score:+.1f}")

        logger.info("[NIGHTWATCH] [3단] 괴리 감지...")
        div_score, divergences, raw = detect_divergence()
        logger.info(f"[NIGHTWATCH] 괴리 스코어: {div_score:+.1f}, 감지: {len(divergences)}건")

        logger.info("[NIGHTWATCH] [4단] 매크로 조건 수집 (NQ/CL/HG)...")
        macro_conditions, macro_raw = collect_macro_conditions(asian_detail, raw)
        raw.update(macro_raw)

    active = macro_conditions.get("active_text", [])
    logger.info(f"[NIGHTWATCH] 매크로: {', '.join(active) if active else '특이사항 없음'}")

    # 4.5단: 한국장 강도 (NXT 캘리브레이션 v2)
    logger.info("[NIGHTWATCH] [4.5단] 한국장 강도 측정...")
    kr_strength, kr_detail = _calc_korea_strength()
    logger.info(f"[NIGHTWATCH] 한국장 강도: {kr_strength:+.1f}")

    # 5단: 진입 재개 시그널 감시 (WAR GATE)
    logger.info("[NIGHTWATCH] [5단] 진입 재개 시그널 감시 (WTI/VIX/KOSPI)...")
    reentry = check_reentry_signals(raw, macro_conditions)
    logger.info(f"[NIGHTWATCH] 재개시그널: {reentry['summary']}")

    # 종합 (JARVIS 섹터 매핑 포함 + 한국장 강도)
    report = calculate_nightwatch_score(
        asian_score, asian_detail,
        europe_score, europe_detail,
        div_score, divergences, raw,
        macro_conditions=macro_conditions,
        korea_strength=kr_strength,
        korea_detail=kr_detail,
    )
    report.reentry_signals = reentry

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
    """리포트 JSON 저장 + 히스토리 누적"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    report_dict = asdict(report)

    # 최신 리포트 저장 (atomic write)
    tmp = REPORT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, ensure_ascii=False, indent=2)
    tmp.replace(REPORT_PATH)

    # 히스토리 누적 (90일 보관)
    _append_history(report_dict)
    # 원자재 가격 히스토리 누적
    _append_commodity_history(report_dict)
    logger.info(f"[NIGHTWATCH] 저장: {REPORT_PATH} + 히스토리 + 원자재 누적")


def _append_history(report_dict: dict):
    """nxt_history.json에 날짜별 리포트 누적 (90일)"""
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history = []
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text("utf-8"))
        except (json.JSONDecodeError, Exception):
            history = []

    report_date = report_dict.get("timestamp", "")[:10]  # "2026-03-18"

    # 슬림 버전 저장 (raw_indicators 등 대형 필드 제외)
    slim = {
        "date": report_date,
        "timestamp": report_dict.get("timestamp", ""),
        "total_score": report_dict.get("total_score", 0),
        "signal": report_dict.get("signal", ""),
        "signal_text": report_dict.get("signal_text", ""),
        "asian_score": report_dict.get("asian_score", 0),
        "europe_score": report_dict.get("europe_score", 0),
        "divergence_score": report_dict.get("divergence_score", 0),
        "divergences": report_dict.get("divergences", []),
        "recommended_sectors": report_dict.get("recommended_sectors", []),
        "nxt_targets": [
            {"code": t["code"], "name": t["name"], "sector": t.get("sector", ""),
             "tier": t.get("tier", 2)}
            for t in report_dict.get("nxt_targets", [])
        ],
        "selection_reason": report_dict.get("selection_reason", ""),
        "korea_strength": report_dict.get("korea_strength", 0),
    }

    # 같은 날짜 중복 교체
    history = [h for h in history if h.get("date") != report_date]
    history.append(slim)

    # 90일만 보관
    if len(history) > 90:
        history = history[-90:]

    tmp = HISTORY_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    tmp.replace(HISTORY_PATH)


def _append_commodity_history(report_dict: dict):
    """commodity_history.json에 원자재 일별 가격/변동률 누적 (90일)"""
    COMMODITY_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history = []
    if COMMODITY_HISTORY_PATH.exists():
        try:
            history = json.loads(COMMODITY_HISTORY_PATH.read_text("utf-8"))
        except (json.JSONDecodeError, Exception):
            history = []

    report_date = report_dict.get("timestamp", "")[:10]
    ri = report_dict.get("raw_indicators", {})
    mc = report_dict.get("macro_conditions", {})

    # 원자재 5종 가격/변동률 추출
    commodities = {}
    for key, label in [("GOLD", "gold"), ("CL", "oil"), ("HG", "copper"),
                        ("NG", "ng"), ("SI", "silver")]:
        data = ri.get(key, {})
        if data.get("value") is not None:
            commodities[label] = {
                "price": data.get("value"),
                "change_pct": data.get("change_pct", 0),
            }

    if not commodities:
        return  # 데이터 없으면 스킵

    entry = {
        "date": report_date,
        "commodities": commodities,
        "relay": mc.get("commodity_relay", None),
        "active_signals": [k for k in ["ng_up", "silver_up", "copper_up", "oil_up"]
                           if mc.get(k)],
    }

    # 같은 날짜 교체
    history = [h for h in history if h.get("date") != report_date]
    history.append(entry)
    if len(history) > 90:
        history = history[-90:]

    tmp = COMMODITY_HISTORY_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    tmp.replace(COMMODITY_HISTORY_PATH)


def load_commodity_history(days: int = 30) -> list:
    """최근 N일 원자재 히스토리 로드"""
    if not COMMODITY_HISTORY_PATH.exists():
        return []
    try:
        history = json.loads(COMMODITY_HISTORY_PATH.read_text("utf-8"))
        return history[-days:]
    except Exception:
        return []


def get_commodity_trend(days: int = 5) -> dict:
    """최근 N일 원자재 추세 분석 → 추천 파이프라인에서 사용"""
    history = load_commodity_history(days)
    if len(history) < 2:
        return {}

    trend = {}
    for commodity in ["gold", "oil", "copper", "ng", "silver"]:
        prices = []
        for h in history:
            c = h.get("commodities", {}).get(commodity, {})
            if c.get("price") is not None:
                prices.append(c["price"])

        if len(prices) < 2:
            continue

        first, last = prices[0], prices[-1]
        pct_change = ((last - first) / first) * 100 if first else 0
        # 추세: 3일 연속 상승/하락 감지
        consecutive_up = all(prices[i] > prices[i - 1] for i in range(1, len(prices)))
        consecutive_down = all(prices[i] < prices[i - 1] for i in range(1, len(prices)))

        direction = "UP" if consecutive_up else ("DOWN" if consecutive_down else "FLAT")

        trend[commodity] = {
            "price": last,
            "change_pct": round(pct_change, 2),
            "direction": direction,
            "days": len(prices),
        }

    # 릴레이 단계 판단 (최근 기록 기준)
    latest = history[-1] if history else {}
    trend["relay"] = latest.get("relay")
    trend["active_signals"] = latest.get("active_signals", [])

    return trend


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
#  시간외 선취매 (Pre-Dawn Buy)
#  "추천 TOP picks → 장마감 시간외 종가매매 → 다음날 갭업 수익"
# ═══════════════════════════════════════════════════
PREDAWN_POSITIONS_PATH = DATA_DIR / "predawn_positions.json"


def select_predawn_targets(
    min_score: float = 65.0,
    max_targets: int = 3,
) -> List[Dict]:
    """
    recommendation.json에서 시간외 선취매 대상 선별 (v2: 완화 필터)

    Tier 1 (확신): AAA/AA + FORCE_BUY + 75점+ + HIGH → 무조건 선취매
    Tier 2 (유력): A이상 + FORCE_BUY/BUY + 65점+ + HIGH/MED → 후보
    Tier 3 (모멘텀): BBB이상 + BUY + 70점+ + HOT섹터 부스트 → 후보

    Tier 1은 max_targets 외 추가 슬롯, Tier 2/3은 max_targets 내 점수순

    Returns: [{code, name, close, total_score, grade, signal_type,
               entry, sl, tp, sources, tv_ratio, tv_pattern, predawn_tier}]
    """
    rec_path = DATA_DIR / "recommendation.json"
    if not rec_path.exists():
        logger.warning("[PREDAWN] recommendation.json 없음")
        return []

    try:
        with open(rec_path, "r", encoding="utf-8") as f:
            rec = json.load(f)
    except Exception as e:
        logger.error(f"[PREDAWN] recommendation.json 로드 실패: {e}")
        return []

    # evening 분석 결과만 사용 (Stage 1)
    stage = rec.get("stage", "")
    if stage not in ("evening", "cross_checked", "us_check"):
        logger.info(f"[PREDAWN] stage={stage} — 저녁분석 아님, 스킵")
        return []

    stocks = rec.get("stocks", [])
    if not stocks:
        logger.info("[PREDAWN] 추천 종목 없음")
        return []

    # HOT 섹터 종목 코드 (sector_momentum 연동)
    hot_codes = set()
    if SECTOR_MOMENTUM_PATH.exists():
        try:
            sm_data = json.loads(SECTOR_MOMENTUM_PATH.read_text("utf-8"))
            for sec in sm_data.get("sectors", []):
                if sec.get("phase") in ("HOT", "WARMING"):
                    for m in sec.get("top_movers", []):
                        hot_codes.add(m.get("code", ""))
        except Exception:
            pass

    tier1 = []
    tier2 = []
    tier3 = []

    for s in stocks:
        grade = s.get("grade", "")
        sig = s.get("signal_type", "")
        score = s.get("total_score", 0)
        conf = s.get("confidence", "")
        code = s.get("code", "")

        base = {
            "code": code,
            "name": s.get("name", ""),
            "close": s.get("close", 0),
            "total_score": score,
            "grade": grade,
            "signal_type": sig,
            "entry": s.get("entry", 0),
            "sl": s.get("sl", 0),
            "tp": s.get("tp", 0),
            "sources": s.get("sources", []),
            "tv_ratio": s.get("tv_ratio", 1.0),
            "tv_pattern": s.get("tv_pattern", "NORMAL"),
        }

        # Tier 1: 확신 — AAA/AA + FORCE_BUY + 75점+ + HIGH
        if grade in ("AAA", "AA") and sig == "FORCE_BUY" and score >= 75 and conf == "HIGH":
            base["predawn_tier"] = 1
            tier1.append(base)
        # Tier 2: 유력 — A이상 + FORCE_BUY/BUY + 65점+ + HIGH/MED
        elif grade in ("AAA", "AA", "A") and sig in ("FORCE_BUY", "BUY") and score >= 65 and conf in ("HIGH", "MED"):
            base["predawn_tier"] = 2
            tier2.append(base)
        # Tier 3: 모멘텀 — BBB이상 + BUY + 70점+ + HOT섹터 종목
        elif grade in ("AAA", "AA", "A", "BBB") and sig in ("FORCE_BUY", "BUY") and score >= 70 and code in hot_codes:
            base["predawn_tier"] = 3
            tier3.append(base)

    # Tier 1은 무조건 포함 + Tier 2/3은 점수순으로 max_targets 채우기
    targets = tier1[:]
    remaining = max_targets - len(tier1)
    if remaining > 0:
        pool = sorted(tier2 + tier3, key=lambda x: x["total_score"], reverse=True)
        targets.extend(pool[:remaining])

    # 최종 정렬: 티어 → 점수
    targets.sort(key=lambda x: (x.get("predawn_tier", 9), -x["total_score"]))

    if targets:
        names = ", ".join(f"{t['name']}(T{t.get('predawn_tier',0)},{t['total_score']:.0f})" for t in targets)
        logger.info(f"[PREDAWN] 선취매 대상 {len(targets)}종목: {names}")
    else:
        logger.info(f"[PREDAWN] 조건 충족 종목 없음 (A+ BUY/FORCE_BUY {min_score}+)")

    return targets


def save_predawn_positions(positions: Dict):
    """선취매 포지션 저장 (atomic write)"""
    PREDAWN_POSITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PREDAWN_POSITIONS_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    tmp.replace(PREDAWN_POSITIONS_PATH)


def load_predawn_positions() -> Dict:
    """선취매 포지션 로드"""
    if not PREDAWN_POSITIONS_PATH.exists():
        return {}
    try:
        with open(PREDAWN_POSITIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def clear_predawn_positions():
    """선취매 포지션 초기화 (정규장 진입 후)"""
    if PREDAWN_POSITIONS_PATH.exists():
        PREDAWN_POSITIONS_PATH.unlink()
        logger.info("[PREDAWN] 선취매 포지션 파일 삭제 (정규 포지션 전환 완료)")


def format_predawn_alert(targets: List[Dict], budget: int = 0, per_stock: int = 0) -> str:
    """선취매 텔레그램 알림 메시지 포맷"""
    lines = [
        "=" * 34,
        "시간외 선취매 추천",
        "=" * 34,
        "",
    ]

    for i, t in enumerate(targets, 1):
        sl_pct = ((t["sl"] - t["close"]) / t["close"] * 100) if t["close"] > 0 and t["sl"] > 0 else 0
        tp_pct = ((t["tp"] - t["close"]) / t["close"] * 100) if t["close"] > 0 and t["tp"] > 0 else 0
        sources_str = ", ".join(t.get("sources", [])[:3])

        lines.append(
            f"[{i}] {t['name']}({t['code']}) {t['grade']}\n"
            f"   종가: {t['close']:,}원 | 점수: {t['total_score']:.0f}\n"
            f"   SL: {t['sl']:,}원({sl_pct:+.1f}%) → TP: {t['tp']:,}원({tp_pct:+.1f}%)\n"
            f"   TV: {t['tv_ratio']:.1f}x ({t['tv_pattern']})\n"
            f"   소스: {sources_str}"
        )
        lines.append("")

    if budget > 0:
        lines.append(f"예산: {budget:,}원 | 종목당: {per_stock:,}원")

    lines.append("")
    lines.append("시간외 종가매매: 18:00까지 매수 가능")
    lines.append("=" * 34)

    return "\n".join(lines)


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
    ng_d = ri.get("NG", {})
    si_d = ri.get("SI", {})
    hg_d = ri.get("HG", {})
    if ng_d:
        lines.append(f"  천연가스(NG): {_pct(ng_d)} (${_val(ng_d)})")
    if si_d:
        lines.append(f"  은(SI): {_pct(si_d)}")
    if hg_d:
        lines.append(f"  구리(HG): {_pct(hg_d)}")
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

    # [5단] 진입 재개 시그널 (WAR GATE)
    re = report.reentry_signals
    if re:
        lines.append("[5단] 진입 재개 감시 (WAR GATE)")
        wti = re.get("wti", {})
        vix = re.get("vix", {})
        kospi = re.get("kospi", {})
        if wti.get("value") is not None:
            icon = "O" if wti["safe"] else "X"
            lines.append(f"  [{icon}] WTI: {wti['status']}")
        if vix.get("value") is not None:
            icon = "O" if vix["safe"] else "X"
            lines.append(f"  [{icon}] VIX: {vix['status']}")
        if kospi.get("value") is not None:
            icon = "O" if kospi["bottom"] else "X"
            lines.append(f"  [{icon}] KOSPI: {kospi['status']}")
        met = re.get("signals_met", [])
        min_req = REENTRY_THRESHOLDS["min_signals"]
        if re.get("reentry_ok"):
            lines.append("")
            lines.append(f">> 진입 재개! ({len(met)}/{min_req} 충족) <<")
            for s in met:
                lines.append(f"  >> {s}")
        elif met:
            lines.append(f"  ! 부분충족 {len(met)}/{min_req}: {', '.join(met)}")
            lines.append("  -> 아직 진입 대기 (현금 유지)")
        else:
            lines.append(f"  -> 진입 대기 (0/{min_req} 충족, 현금 유지)")
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
            supply = t.get("supply_score")
            supply_str = f" 수급{supply}" if supply is not None else ""
            lines.append(
                f"  {tier_mark} {t['name']}({t['code']}) "
                f"[{t['sector']}]{supply_str}"
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
