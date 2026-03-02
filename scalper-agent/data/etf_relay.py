"""
ETF 순환 에이전트 (ETF Relay Agent)
- 주요 테마 ETF 모멘텀 추적
- ETF 상승 → 하위 구성종목 후행 감지
- ETF가 주도하고 종목이 따라오는 패턴
"""

from dataclasses import dataclass, field
from typing import Optional
from pykrx import stock
import logging

logger = logging.getLogger(__name__)

# ── 주요 테마 ETF + 구성종목 ──────────────────────────
ETFS = {
    "defense": {
        "name": "K방산",
        "etf": ("449450", "PLUS K방산"),
        "constituents": [
            ("047810", "한국항공우주"),
            ("012450", "한화에어로스페이스"),
            ("042660", "한화오션"),
            ("064350", "현대로템"),
            ("272210", "한화시스템"),
            ("079550", "LIG넥스원"),
            ("103140", "풍산"),
            ("077970", "STX엔진"),
            ("003570", "SNT다이내믹스"),
        ],
    },
    "semiconductor": {
        "name": "반도체",
        "etf": ("091160", "KODEX 반도체"),
        "constituents": [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
            ("042700", "한미반도체"),
            ("058470", "리노공업"),
            ("402340", "SK스퀘어"),
            ("240810", "원익IPS"),
        ],
    },
    "battery": {
        "name": "2차전지",
        "etf": ("305720", "KODEX 2차전지산업"),
        "constituents": [
            ("006400", "삼성SDI"),
            ("373220", "LG에너지솔루션"),
            ("051910", "LG화학"),
            ("247540", "에코프로비엠"),
            ("003670", "포스코퓨처엠"),
            ("086520", "에코프로"),
        ],
    },
    "bio": {
        "name": "바이오",
        "etf": ("244580", "KODEX 바이오"),
        "constituents": [
            ("207940", "삼성바이오로직스"),
            ("068270", "셀트리온"),
            ("000100", "유한양행"),
            ("128940", "한미약품"),
            ("326030", "SK바이오팜"),
        ],
    },
    "kospi200": {
        "name": "KOSPI200",
        "etf": ("069500", "KODEX 200"),
        "constituents": [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
            ("005380", "현대차"),
            ("000270", "기아"),
            ("207940", "삼성바이오로직스"),
        ],
    },
    "auto": {
        "name": "자동차",
        "etf": ("091180", "KODEX 자동차"),
        "constituents": [
            ("005380", "현대차"),
            ("000270", "기아"),
            ("012330", "현대모비스"),
            ("204320", "만도"),
            ("018880", "한온시스템"),
        ],
    },
    "construction": {
        "name": "건설",
        "etf": ("117700", "KODEX 건설"),
        "constituents": [
            ("000720", "현대건설"),
            ("375500", "DL이앤씨"),
            ("006360", "GS건설"),
            ("294870", "HDC현대산업개발"),
        ],
    },
    "shipbuilding": {
        "name": "조선",
        "etf": ("139230", "KODEX 조선"),
        "constituents": [
            ("329180", "HD현대중공업"),
            ("010140", "삼성중공업"),
            ("009540", "HD한국조선해양"),
            ("042660", "한화오션"),
        ],
    },
}


@dataclass
class ETFMomentum:
    code: str
    name: str
    close: int = 0
    change_1d: float = 0.0
    change_5d: float = 0.0
    vol_ratio: float = 0.0


@dataclass
class ConstituentGap:
    code: str
    name: str
    close: int = 0
    change_5d: float = 0.0
    gap_vs_etf: float = 0.0  # ETF 5D - 종목 5D (양수=종목 후행)
    vol_ratio: float = 0.0


@dataclass
class ETFStatus:
    theme_id: str
    theme_name: str
    status: str  # LEADING / DIVERGING / ALIGNED / COLD
    etf: Optional[ETFMomentum] = None
    etf_5d: float = 0.0
    constituents_avg_5d: float = 0.0
    divergence: float = 0.0  # ETF - 구성종목 평균
    laggards: list = field(default_factory=list)  # 후행 종목


def _fetch_etf(code: str, name: str) -> Optional[ETFMomentum]:
    """ETF 모멘텀 조회"""
    try:
        from datetime import datetime, timedelta
        end = datetime.now()
        start = end - timedelta(days=45)
        df = stock.get_market_ohlcv(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), code)
        if df is None or len(df) < 5:
            return None

        close = int(df.iloc[-1]["종가"])
        if close == 0:
            return None

        c1d = (close / int(df.iloc[-2]["종가"]) - 1) * 100 if len(df) >= 2 else 0
        c5d = (close / int(df.iloc[-5]["종가"]) - 1) * 100 if len(df) >= 5 else 0
        vol = int(df.iloc[-1]["거래량"])
        avg_vol = int(df["거래량"].iloc[-6:-1].mean()) if len(df) >= 6 else vol
        vr = vol / avg_vol if avg_vol > 0 else 0

        return ETFMomentum(code=code, name=name, close=close, change_1d=c1d, change_5d=c5d, vol_ratio=vr)
    except Exception as e:
        logger.warning(f"[ETFRelay] ETF {name}({code}) error: {e}")
        return None


def _fetch_constituent(code: str, name: str, etf_5d: float) -> Optional[ConstituentGap]:
    """구성종목 후행 갭 계산"""
    try:
        from datetime import datetime, timedelta
        end = datetime.now()
        start = end - timedelta(days=45)
        df = stock.get_market_ohlcv(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), code)
        if df is None or len(df) < 5:
            return None

        close = int(df.iloc[-1]["종가"])
        if close == 0:
            return None

        c5d = (close / int(df.iloc[-5]["종가"]) - 1) * 100 if len(df) >= 5 else 0
        vol = int(df.iloc[-1]["거래량"])
        avg_vol = int(df["거래량"].iloc[-6:-1].mean()) if len(df) >= 6 else vol
        vr = vol / avg_vol if avg_vol > 0 else 0

        return ConstituentGap(
            code=code, name=name, close=close,
            change_5d=round(c5d, 2),
            gap_vs_etf=round(etf_5d - c5d, 2),
            vol_ratio=round(vr, 2),
        )
    except Exception as e:
        logger.warning(f"[ETFRelay] {name}({code}) error: {e}")
        return None


def scan_etf_theme(theme_id: str) -> Optional[ETFStatus]:
    """단일 ETF 테마 분석"""
    theme = ETFS.get(theme_id)
    if not theme:
        return None

    ecode, ename = theme["etf"]
    etf_m = _fetch_etf(ecode, ename)
    if not etf_m:
        return None

    etf_5d = etf_m.change_5d

    # 구성종목 분석
    constituents = []
    for code, name in theme["constituents"]:
        cg = _fetch_constituent(code, name, etf_5d)
        if cg:
            constituents.append(cg)

    if not constituents:
        return None

    avg_5d = sum(c.change_5d for c in constituents) / len(constituents)
    divergence = etf_5d - avg_5d

    # 후행 종목: ETF보다 2%p 이상 덜 오른 종목
    laggards = [c for c in constituents if c.gap_vs_etf >= 2.0]
    laggards.sort(key=lambda x: x.gap_vs_etf, reverse=True)

    # 상태 판정
    if etf_5d >= 2.0 and divergence >= 2.0:
        status = "LEADING"  # ETF 주도, 종목 후행
    elif etf_5d >= 2.0 and divergence < 1.0:
        status = "ALIGNED"  # ETF와 종목 동반 상승
    elif etf_5d >= 1.0 and divergence >= 1.5:
        status = "DIVERGING"  # ETF 소폭 상승, 종목 후행
    else:
        status = "COLD"

    return ETFStatus(
        theme_id=theme_id,
        theme_name=theme["name"],
        status=status,
        etf=etf_m,
        etf_5d=round(etf_5d, 2),
        constituents_avg_5d=round(avg_5d, 2),
        divergence=round(divergence, 2),
        laggards=laggards,
    )


def scan_all_etfs() -> list[ETFStatus]:
    """전 ETF 테마 스캔"""
    results = []
    for tid in ETFS:
        es = scan_etf_theme(tid)
        if es:
            results.append(es)

    priority = {"LEADING": 0, "DIVERGING": 1, "ALIGNED": 2, "COLD": 3}
    results.sort(key=lambda x: (priority.get(x.status, 9), -x.etf_5d))
    return results


def format_etf_report(results: list[ETFStatus]) -> str:
    """텔레그램용 ETF 리포트"""
    status_emoji = {"LEADING": "🚀", "DIVERGING": "📊", "ALIGNED": "✅", "COLD": "⬜"}

    lines = ["📈 ETF 순환 스캔", ""]

    for es in results:
        emoji = status_emoji.get(es.status, "⬜")
        etf_str = f"{es.etf.name} 5D:{es.etf_5d:+.1f}%" if es.etf else "N/A"
        lines.append(
            f"{emoji} {es.theme_name} [{es.status}]"
            f"  {etf_str}  종목평균:{es.constituents_avg_5d:+.1f}%  괴리:{es.divergence:+.1f}%p"
        )

        if es.status in ("LEADING", "DIVERGING") and es.laggards:
            lines.append(f"   📉 후행 종목 (ETF보다 느린 놈들):")
            for lg in es.laggards[:4]:
                lines.append(
                    f"      · {lg.name} {lg.close:,}원"
                    f" 5D:{lg.change_5d:+.1f}% 갭:{lg.gap_vs_etf:+.1f}%p"
                )

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        tid = sys.argv[1]
        result = scan_etf_theme(tid)
        if result:
            print(format_etf_report([result]))
        else:
            print(f"테마 '{tid}' 없음. 가능: {list(ETFS.keys())}")
    else:
        print("전 ETF 테마 스캔 시작...")
        results = scan_all_etfs()
        print(format_etf_report(results))
