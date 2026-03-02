"""
섹터 릴레이 에이전트 (Sector Relay Agent)
- 12개 섹터 순환 감지
- 섹터별 모멘텀/거래량/브레드쓰 분석
- HOT/WARMING/RELAY/COOLING/COLD 상태 판정
- 릴레이 후보: 대장주 올랐는데 소부장 안 오른 종목 감지
"""

from dataclasses import dataclass, field
from typing import Optional
from pykrx import stock
import logging

logger = logging.getLogger(__name__)

# ── 12개 섹터 정의 ─────────────────────────────────────────
SECTORS = {
    "defense": {
        "name": "방산/국방",
        "leaders": [
            ("012450", "한화에어로스페이스"),
            ("079550", "LIG넥스원"),
            ("064350", "현대로템"),
            ("047810", "한국항공우주"),
        ],
        "mid": [
            ("272210", "한화시스템"),
            ("042660", "한화오션"),
            ("103140", "풍산"),
        ],
        "sobujan": [
            ("077970", "STX엔진"),
            ("003570", "SNT다이내믹스"),
            ("065450", "빅텍"),
            ("005870", "휴니드"),
            ("024740", "한일단조"),
            ("214430", "아이쓰리시스템"),
            ("218410", "RFHIC"),
            ("274090", "켄코아에어로스페이스"),
        ],
    },
    "semiconductor": {
        "name": "반도체/AI",
        "leaders": [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
        ],
        "mid": [
            ("042700", "한미반도체"),
            ("058470", "리노공업"),
            ("402340", "SK스퀘어"),
        ],
        "sobujan": [
            ("240810", "원익IPS"),
            ("095340", "ISC"),
            ("357780", "솔브레인"),
        ],
    },
    "battery": {
        "name": "2차전지/EV",
        "leaders": [
            ("006400", "삼성SDI"),
            ("373220", "LG에너지솔루션"),
        ],
        "mid": [
            ("051910", "LG화학"),
            ("247540", "에코프로비엠"),
            ("003670", "포스코퓨처엠"),
        ],
        "sobujan": [
            ("086520", "에코프로"),
        ],
    },
    "bio": {
        "name": "바이오/제약",
        "leaders": [
            ("207940", "삼성바이오로직스"),
            ("068270", "셀트리온"),
        ],
        "mid": [
            ("000100", "유한양행"),
            ("128940", "한미약품"),
        ],
        "sobujan": [
            ("326030", "SK바이오팜"),
        ],
    },
    "auto": {
        "name": "자동차",
        "leaders": [
            ("005380", "현대차"),
            ("000270", "기아"),
        ],
        "mid": [
            ("012330", "현대모비스"),
        ],
        "sobujan": [
            ("204320", "만도"),
            ("018880", "한온시스템"),
        ],
    },
    "construction": {
        "name": "건설",
        "leaders": [
            ("000720", "현대건설"),
            ("375500", "DL이앤씨"),
        ],
        "mid": [
            ("006360", "GS건설"),
        ],
        "sobujan": [
            ("294870", "HDC현대산업개발"),
        ],
    },
    "oil_energy": {
        "name": "정유/에너지",
        "leaders": [
            ("096770", "SK이노베이션"),
            ("010950", "S-Oil"),
        ],
        "mid": [
            ("015760", "한국전력"),
            ("034020", "두산에너빌리티"),
        ],
        "sobujan": [
            ("036460", "한국가스공사"),
        ],
    },
    "cosmetics": {
        "name": "화장품/뷰티",
        "leaders": [
            ("090430", "아모레퍼시픽"),
            ("051900", "LG생활건강"),
        ],
        "mid": [
            ("192820", "코스맥스"),
            ("161890", "한국콜마"),
        ],
        "sobujan": [
            ("237880", "클리오"),
        ],
    },
    "shipbuilding": {
        "name": "조선",
        "leaders": [
            ("329180", "HD현대중공업"),
            ("010140", "삼성중공업"),
        ],
        "mid": [
            ("009540", "HD한국조선해양"),
            ("042660", "한화오션"),
        ],
        "sobujan": [],
    },
    "it_platform": {
        "name": "IT/플랫폼",
        "leaders": [
            ("035420", "NAVER"),
            ("035720", "카카오"),
        ],
        "mid": [
            ("259960", "크래프톤"),
            ("036570", "엔씨소프트"),
        ],
        "sobujan": [
            ("251270", "넷마블"),
        ],
    },
    "finance": {
        "name": "금융",
        "leaders": [
            ("105560", "KB금융"),
            ("055550", "신한지주"),
        ],
        "mid": [
            ("086790", "하나금융지주"),
            ("316140", "우리금융지주"),
        ],
        "sobujan": [
            ("000810", "삼성화재"),
        ],
    },
    "steel": {
        "name": "철강/소재",
        "leaders": [
            ("005490", "POSCO홀딩스"),
        ],
        "mid": [
            ("004020", "현대제철"),
            ("010130", "고려아연"),
        ],
        "sobujan": [],
    },
}


@dataclass
class StockMomentum:
    code: str
    name: str
    tier: str  # leader / mid / sobujan
    close: int = 0
    change_1d: float = 0.0
    change_5d: float = 0.0
    change_20d: float = 0.0
    vol_ratio: float = 0.0  # 당일거래량 / 5일평균


@dataclass
class SectorStatus:
    sector_id: str
    sector_name: str
    status: str  # HOT / WARMING / RELAY / COOLING / COLD
    momentum_5d: float = 0.0
    vol_ratio: float = 0.0
    breadth: float = 0.0  # 상승 종목 비율
    leader_avg_5d: float = 0.0
    sobujan_avg_5d: float = 0.0
    relay_gap: float = 0.0  # leader - sobujan (클수록 릴레이 기회)
    stocks: list = field(default_factory=list)
    relay_candidates: list = field(default_factory=list)  # 릴레이 후보


def _fetch_momentum(code: str, name: str, tier: str) -> Optional[StockMomentum]:
    """종목 하나의 모멘텀 데이터 조회"""
    try:
        from datetime import datetime, timedelta
        end = datetime.now()
        start = end - timedelta(days=45)
        start_s = start.strftime("%Y%m%d")
        end_s = end.strftime("%Y%m%d")

        df = stock.get_market_ohlcv(start_s, end_s, code)
        if df is None or len(df) < 5:
            return None

        close = int(df.iloc[-1]["종가"])
        if close == 0:
            return None

        # 등락률 계산
        c1d = (close / int(df.iloc[-2]["종가"]) - 1) * 100 if len(df) >= 2 else 0
        c5d = (close / int(df.iloc[-5]["종가"]) - 1) * 100 if len(df) >= 5 else 0
        c20d = (close / int(df.iloc[-20]["종가"]) - 1) * 100 if len(df) >= 20 else 0

        # 거래량 비율
        vol = int(df.iloc[-1]["거래량"])
        avg_vol = int(df["거래량"].iloc[-6:-1].mean()) if len(df) >= 6 else vol
        vr = vol / avg_vol if avg_vol > 0 else 0

        return StockMomentum(
            code=code, name=name, tier=tier,
            close=close, change_1d=c1d, change_5d=c5d, change_20d=c20d,
            vol_ratio=vr,
        )
    except Exception as e:
        logger.warning(f"[SectorRelay] {name}({code}) fetch error: {e}")
        return None


def _classify_status(
    momentum_5d: float, vol_ratio: float, breadth: float, relay_gap: float
) -> str:
    """섹터 상태 판정"""
    if momentum_5d >= 3.0 and vol_ratio >= 1.5 and breadth >= 0.6:
        return "HOT"
    if momentum_5d >= 1.0 and (vol_ratio >= 1.3 or breadth >= 0.5):
        return "WARMING"
    if relay_gap >= 3.0 and momentum_5d >= 0.5:
        return "RELAY"
    if momentum_5d <= -2.0:
        return "COOLING"
    return "COLD"


def scan_sector(sector_id: str) -> Optional[SectorStatus]:
    """단일 섹터 분석"""
    sec = SECTORS.get(sector_id)
    if not sec:
        return None

    all_stocks = []
    for tier_name, tier_list in [("leader", sec["leaders"]), ("mid", sec["mid"]), ("sobujan", sec["sobujan"])]:
        for code, name in tier_list:
            m = _fetch_momentum(code, name, tier_name)
            if m:
                all_stocks.append(m)

    if not all_stocks:
        return None

    # 통계 계산
    momentum_5d = sum(s.change_5d for s in all_stocks) / len(all_stocks)
    vol_ratio = sum(s.vol_ratio for s in all_stocks) / len(all_stocks)
    breadth = sum(1 for s in all_stocks if s.change_1d > 0) / len(all_stocks)

    leaders = [s for s in all_stocks if s.tier == "leader"]
    sobujan = [s for s in all_stocks if s.tier == "sobujan"]

    leader_avg = sum(s.change_5d for s in leaders) / len(leaders) if leaders else 0
    sobujan_avg = sum(s.change_5d for s in sobujan) / len(sobujan) if sobujan else 0
    relay_gap = leader_avg - sobujan_avg

    # 릴레이 후보: 리더 5D > 2% 인데 소부장 5D < 0%
    relay_candidates = []
    if leader_avg >= 2.0:
        for s in sobujan:
            if s.change_5d < leader_avg * 0.3:  # 리더 대비 30% 미만 수익
                relay_candidates.append(s)

    status = _classify_status(momentum_5d, vol_ratio, breadth, relay_gap)

    return SectorStatus(
        sector_id=sector_id,
        sector_name=sec["name"],
        status=status,
        momentum_5d=round(momentum_5d, 2),
        vol_ratio=round(vol_ratio, 2),
        breadth=round(breadth, 2),
        leader_avg_5d=round(leader_avg, 2),
        sobujan_avg_5d=round(sobujan_avg, 2),
        relay_gap=round(relay_gap, 2),
        stocks=sorted(all_stocks, key=lambda x: x.change_5d, reverse=True),
        relay_candidates=relay_candidates,
    )


def scan_all_sectors() -> list[SectorStatus]:
    """전 섹터 스캔 (HOT/WARMING/RELAY 우선 정렬)"""
    results = []
    for sid in SECTORS:
        ss = scan_sector(sid)
        if ss:
            results.append(ss)

    # 정렬: HOT > WARMING > RELAY > COLD > COOLING
    priority = {"HOT": 0, "WARMING": 1, "RELAY": 2, "COLD": 3, "COOLING": 4}
    results.sort(key=lambda x: (priority.get(x.status, 9), -x.momentum_5d))
    return results


def format_sector_report(results: list[SectorStatus]) -> str:
    """텔레그램용 섹터 리포트"""
    status_emoji = {"HOT": "🔥", "WARMING": "🌡️", "RELAY": "🔄", "COOLING": "❄️", "COLD": "⬜"}

    lines = ["📊 섹터 릴레이 스캔", ""]

    for ss in results:
        emoji = status_emoji.get(ss.status, "⬜")
        lines.append(
            f"{emoji} {ss.sector_name} [{ss.status}]"
            f"  5D:{ss.momentum_5d:+.1f}%  거래:{ss.vol_ratio:.1f}x  상승:{ss.breadth:.0%}"
        )

        if ss.status == "RELAY" and ss.relay_candidates:
            lines.append(f"   🔄 릴레이 후보 (리더{ss.leader_avg_5d:+.1f}% → 소부장{ss.sobujan_avg_5d:+.1f}%):")
            for rc in ss.relay_candidates[:3]:
                lines.append(f"      · {rc.name} {rc.close:,}원 5D:{rc.change_5d:+.1f}% 거래:{rc.vol_ratio:.1f}x")

        if ss.status in ("HOT", "WARMING"):
            top3 = ss.stocks[:3]
            for s in top3:
                lines.append(f"   ▸ {s.name}({s.tier}) {s.close:,}원 5D:{s.change_5d:+.1f}%")

    return "\n".join(lines)


# ── CLI 테스트 ────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        sid = sys.argv[1]
        result = scan_sector(sid)
        if result:
            print(format_sector_report([result]))
        else:
            print(f"섹터 '{sid}' 없음. 가능: {list(SECTORS.keys())}")
    else:
        print("전 섹터 스캔 시작...")
        results = scan_all_sectors()
        print(format_sector_report(results))
