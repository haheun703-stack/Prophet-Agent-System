# -*- coding: utf-8 -*-
"""
SpaceX 관련주 일일 모니터링 모듈
=================================
매일 장마감 후 SpaceX 테마주 수급·기술·뉴스를 종합 분석하여
data_store/spacex_report.json 에 저장.

수집 항목:
  1. OHLCV + 등락률 + 거래량 비율 (vs 20일 평균)
  2. 투자자별 수급 (외인/기관/개인)
  3. 외인 소진율 변화
  4. 국적별 수급 상위 (있는 경우)
  5. 기술적 시그널 (EMA/RSI/MACD)
  6. SpaceX 뉴스 키워드 스캔 (네이버)

사용법:
  from data.spacex_watchlist import generate_spacex_report
  report = generate_spacex_report()
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger("SpaceXWatch")

SCRIPT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = SCRIPT_DIR / "data_store"

# ── SpaceX 관련주 유니버스 ──────────────────────────────────
SPACEX_STOCKS = {
    # Tier 1: 직접 공급/투자 관계
    "347700": {"name": "스피어", "tier": 1, "reason": "Tier1 특수합금 공급 (아시아 유일), 10년 장기계약"},
    "295310": {"name": "에이치브이엠", "tier": 1, "reason": "SpaceX 우주발사체용 첨단금속 납품"},
    "100790": {"name": "미래에셋벤처투자", "tier": 1, "reason": "SpaceX 직접투자 $2.78억 (국내 유일 상장사)"},
    "417010": {"name": "나노팀", "tier": 1, "reason": "SpaceX 갭필러(열관리 소재) 2차 공급"},
    "274090": {"name": "켄코아에어로스페이스", "tier": 1, "reason": "NASA/SpaceX 주요 고객사, 특수 소재 공급"},

    # Tier 2: 우주항공 직접 관련
    "462350": {"name": "이노스페이스", "tier": 2, "reason": "한국판 SpaceX, 재사용 발사체 개발"},
    "474170": {"name": "루미르", "tier": 2, "reason": "SpaceX 통해 자체 위성 발사 예정"},
    "225190": {"name": "LK삼양", "tier": 2, "reason": "SpaceX 위성 탑재 심우주항법 별추적기 개발"},
    "001430": {"name": "세아베스틸지주", "tier": 2, "reason": "자회사 통해 SpaceX 특수합금 공급 추진"},
    "488900": {"name": "비츠로넥스텍", "tier": 2, "reason": "항공우주/방산 특수 소재·부품"},

    # Tier 3: 위성통신/간접 수혜
    "211270": {"name": "AP위성", "tier": 3, "reason": "인공위성·위성통신단말기 개발 (SpaceX 대장주)"},
    "189300": {"name": "인텔리안테크", "tier": 3, "reason": "스타링크 위성통신 지상안테나 공급"},
    "272210": {"name": "한화시스템", "tier": 3, "reason": "위성통신·방산 기술, 스타링크 협력 가능성"},
    "361390": {"name": "제노코", "tier": 3, "reason": "위성통신부품 제조"},
    "289930": {"name": "웨이비스", "tier": 3, "reason": "GaN RF 반도체, 인공위성/우주항공 공급"},
    "478340": {"name": "나라스페이스테크놀로지", "tier": 3, "reason": "누리호 개발 참여, 발사체 부품"},
    "340360": {"name": "다보링크", "tier": 3, "reason": "위성통신 관련 테마 동반 움직임"},
}


def _load_ohlcv(code: str, days: int = 60) -> pd.DataFrame:
    """data_store/daily/{code}.csv 에서 최근 N일 OHLCV 로드"""
    csv_path = DATA_DIR / "daily" / f"{code}.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        if df.empty:
            return df
        col_map = {}
        for c in df.columns:
            cl = c.lower().strip()
            if cl in ("시가", "open"):
                col_map[c] = "open"
            elif cl in ("고가", "high"):
                col_map[c] = "high"
            elif cl in ("저가", "low"):
                col_map[c] = "low"
            elif cl in ("종가", "close"):
                col_map[c] = "close"
            elif cl in ("거래량", "volume"):
                col_map[c] = "volume"
        df = df.rename(columns=col_map)
        df = df.sort_index()
        return df.tail(days)
    except Exception as e:
        logger.debug(f"OHLCV 로드 실패 {code}: {e}")
        return pd.DataFrame()


def _load_investor_flow(code: str) -> dict:
    """최근 투자자별 수급 (외인/기관/개인)"""
    # 파일명 패턴: {code}_investor.csv
    csv_path = DATA_DIR / "flow" / f"{code}_investor.csv"
    if not csv_path.exists():
        return {}
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        if df.empty:
            return {}
        df = df.sort_index()
        latest = df.iloc[-1]
        result = {}
        # 컬럼: 외국인_수량, 기관_수량, 개인_수량
        for col in df.columns:
            cl = col.strip()
            if "외국인" in cl and "수량" in cl:
                result["foreign"] = int(latest[col]) if pd.notna(latest[col]) else 0
            elif "기관" in cl and "수량" in cl:
                result["institution"] = int(latest[col]) if pd.notna(latest[col]) else 0
            elif "개인" in cl and "수량" in cl:
                result["retail"] = int(latest[col]) if pd.notna(latest[col]) else 0
        # 5일 합계
        if len(df) >= 5:
            last5 = df.tail(5)
            for col in df.columns:
                cl = col.strip()
                if "외국인" in cl and "수량" in cl:
                    result["foreign_5d"] = int(last5[col].sum())
                elif "기관" in cl and "수량" in cl:
                    result["institution_5d"] = int(last5[col].sum())
        result["date"] = str(df.index[-1].date()) if hasattr(df.index[-1], 'date') else str(df.index[-1])
        return result
    except Exception as e:
        logger.debug(f"투자자 수급 로드 실패 {code}: {e}")
        return {}


def _load_foreign_exhaustion(code: str) -> dict:
    """외인 소진율"""
    # 파일명 패턴: {code}_foreign_exh.csv
    csv_path = DATA_DIR / "flow" / f"{code}_foreign_exh.csv"
    if not csv_path.exists():
        return {}
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        if df.empty:
            return {}
        df = df.sort_index()
        latest = df.iloc[-1]
        result = {"date": str(df.index[-1].date()) if hasattr(df.index[-1], 'date') else str(df.index[-1])}
        # 컬럼: 소진율, 보유수량, 한도소진율
        if "소진율" in df.columns:
            result["rate"] = round(float(latest["소진율"]), 2) if pd.notna(latest["소진율"]) else 0
        if "보유수량" in df.columns:
            result["holding"] = int(latest["보유수량"]) if pd.notna(latest["보유수량"]) else 0
        # 5일 전 소진율 (변화 추적)
        if len(df) >= 5 and "소진율" in df.columns:
            prev = df.iloc[-5]
            result["rate_5d_ago"] = round(float(prev["소진율"]), 2) if pd.notna(prev["소진율"]) else 0
        return result
    except Exception as e:
        logger.debug(f"외인소진 로드 실패 {code}: {e}")
        return {}


def _calc_technicals(df: pd.DataFrame) -> dict:
    """기술적 지표 계산 (EMA/RSI/MACD)"""
    if df.empty or "close" not in df.columns or len(df) < 20:
        return {}
    close = df["close"].astype(float)
    result = {}

    # EMA
    ema5 = close.ewm(span=5, adjust=False).mean().iloc[-1]
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    ema60 = close.ewm(span=60, adjust=False).mean().iloc[-1] if len(close) >= 60 else None
    result["ema5"] = round(ema5, 1)
    result["ema20"] = round(ema20, 1)
    if ema60:
        result["ema60"] = round(ema60, 1)
    result["ema_trend"] = "UP" if ema5 > ema20 else "DOWN"

    # RSI (14일)
    if len(close) >= 15:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta.clip(upper=0))
        avg_gain = gain.rolling(14).mean().iloc[-1]
        avg_loss = loss.rolling(14).mean().iloc[-1]
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            result["rsi"] = round(100 - (100 / (1 + rs)), 1)
        else:
            result["rsi"] = 100.0

    # MACD
    if len(close) >= 26:
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        result["macd"] = round(macd_line.iloc[-1], 1)
        result["macd_signal"] = round(signal_line.iloc[-1], 1)
        result["macd_cross"] = "GOLDEN" if macd_line.iloc[-1] > signal_line.iloc[-1] else "DEAD"

    return result


def _scan_spacex_news() -> list:
    """네이버 SpaceX 뉴스 스캔 (최근 기사 5건)"""
    try:
        import requests
        from urllib.parse import quote
        keywords = ["스페이스X", "SpaceX", "스타쉽", "Starship"]
        articles = []
        for kw in keywords[:2]:
            url = f"https://search.naver.com/search.naver?where=news&query={quote(kw)}&sort=1"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            # 간단 파싱 (제목+링크)
            import re
            # news_tit 클래스에서 제목 추출
            titles = re.findall(r'class="news_tit"[^>]*title="([^"]+)"', resp.text)
            for t in titles[:3]:
                if t not in [a.get("title") for a in articles]:
                    articles.append({"title": t, "keyword": kw})
            if len(articles) >= 5:
                break
        return articles[:5]
    except Exception as e:
        logger.debug(f"뉴스 스캔 실패: {e}")
        return []


def generate_spacex_report() -> dict:
    """SpaceX 관련주 일일 리포트 생성 → data_store/spacex_report.json"""
    logger.info(f"SpaceX 관련주 모니터링 시작: {len(SPACEX_STOCKS)}종목")
    t0 = __import__("time").time()

    report = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_stocks": len(SPACEX_STOCKS),
        "stocks": [],
        "summary": {},
        "news": [],
    }

    gainers = []
    losers = []
    foreign_buy = []
    foreign_sell = []

    for code, meta in SPACEX_STOCKS.items():
        stock_data = {
            "code": code,
            "name": meta["name"],
            "tier": meta["tier"],
            "reason": meta["reason"],
        }

        # OHLCV
        df = _load_ohlcv(code, 60)
        if not df.empty and "close" in df.columns:
            latest = df.iloc[-1]
            stock_data["close"] = int(latest["close"])
            stock_data["volume"] = int(latest.get("volume", 0))

            # 등락률
            if len(df) >= 2:
                prev_close = float(df.iloc[-2]["close"])
                if prev_close > 0:
                    chg = round((float(latest["close"]) - prev_close) / prev_close * 100, 2)
                    stock_data["change_pct"] = chg
                    if chg > 0:
                        gainers.append((meta["name"], chg))
                    elif chg < 0:
                        losers.append((meta["name"], chg))

            # 거래량 비율 (vs 20일 평균)
            if len(df) >= 20 and "volume" in df.columns:
                avg_vol = df["volume"].tail(20).mean()
                if avg_vol > 0:
                    stock_data["vol_ratio"] = round(float(latest.get("volume", 0)) / avg_vol, 2)

            # 5일 수익률
            if len(df) >= 5:
                close_5d = float(df.iloc[-5]["close"])
                if close_5d > 0:
                    stock_data["return_5d"] = round((float(latest["close"]) - close_5d) / close_5d * 100, 2)

            # 기술적 지표
            tech = _calc_technicals(df)
            if tech:
                stock_data["technicals"] = tech
        else:
            stock_data["close"] = 0
            stock_data["volume"] = 0

        # 투자자별 수급
        flow = _load_investor_flow(code)
        if flow:
            stock_data["investor_flow"] = flow
            fg = flow.get("foreign", 0)
            if fg > 0:
                foreign_buy.append((meta["name"], fg))
            elif fg < 0:
                foreign_sell.append((meta["name"], fg))

        # 외인 소진율
        exh = _load_foreign_exhaustion(code)
        if exh:
            stock_data["foreign_exhaustion"] = exh

        report["stocks"].append(stock_data)

    # 정렬: Tier → 등락률 순
    report["stocks"].sort(key=lambda x: (x["tier"], -x.get("change_pct", 0)))

    # Summary
    gainers.sort(key=lambda x: x[1], reverse=True)
    losers.sort(key=lambda x: x[1])
    foreign_buy.sort(key=lambda x: x[1], reverse=True)
    foreign_sell.sort(key=lambda x: x[1])

    report["summary"] = {
        "top_gainers": [{"name": n, "pct": p} for n, p in gainers[:5]],
        "top_losers": [{"name": n, "pct": p} for n, p in losers[:5]],
        "foreign_top_buy": [{"name": n, "shares": s} for n, s in foreign_buy[:5]],
        "foreign_top_sell": [{"name": n, "shares": s} for n, s in foreign_sell[:5]],
        "avg_change": round(sum(s.get("change_pct", 0) for s in report["stocks"]) / max(len(report["stocks"]), 1), 2),
    }

    # 뉴스 스캔
    report["news"] = _scan_spacex_news()

    # 저장
    out_path = DATA_DIR / "spacex_report.json"
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"SpaceX 리포트 저장: {out_path}")
    except Exception as e:
        logger.error(f"SpaceX 리포트 저장 실패: {e}")

    elapsed = int(__import__("time").time() - t0)
    logger.info(f"SpaceX 모니터링 완료: {len(report['stocks'])}종목 / {elapsed}초")
    return report


def get_spacex_codes() -> list:
    """SpaceX 관련주 종목코드 리스트 반환 (collect_all 연동용)"""
    return list(SPACEX_STOCKS.keys())


def format_spacex_summary(report: dict = None) -> str:
    """SpaceX 리포트를 텍스트 요약으로 포맷팅"""
    if report is None:
        rpt_path = DATA_DIR / "spacex_report.json"
        if not rpt_path.exists():
            return "SpaceX 리포트 없음"
        with open(rpt_path, "r", encoding="utf-8") as f:
            report = json.load(f)

    lines = []
    lines.append(f"=== SpaceX 관련주 일일 리포트 ({report.get('timestamp', '')}) ===")
    lines.append("")

    summary = report.get("summary", {})
    avg = summary.get("avg_change", 0)
    lines.append(f"테마 평균 등락: {avg:+.2f}%")
    lines.append("")

    # 상승 TOP
    if summary.get("top_gainers"):
        lines.append("상승 TOP:")
        for g in summary["top_gainers"]:
            lines.append(f"  {g['name']}: {g['pct']:+.2f}%")

    # 하락 TOP
    if summary.get("top_losers"):
        lines.append("하락 TOP:")
        for g in summary["top_losers"]:
            lines.append(f"  {g['name']}: {g['pct']:+.2f}%")

    lines.append("")

    # 외인 매수/매도 TOP
    if summary.get("foreign_top_buy"):
        lines.append("외인 순매수:")
        for g in summary["foreign_top_buy"]:
            lines.append(f"  {g['name']}: +{g['shares']:,}주")
    if summary.get("foreign_top_sell"):
        lines.append("외인 순매도:")
        for g in summary["foreign_top_sell"]:
            lines.append(f"  {g['name']}: {g['shares']:,}주")

    lines.append("")

    # Tier 1 상세
    lines.append("--- Tier 1 (직접 공급/투자) ---")
    for s in report.get("stocks", []):
        if s.get("tier") != 1:
            continue
        chg = s.get("change_pct", 0)
        vol_r = s.get("vol_ratio", 0)
        tech = s.get("technicals", {})
        ema = tech.get("ema_trend", "?")
        rsi = tech.get("rsi", 0)
        macd = tech.get("macd_cross", "?")
        lines.append(f"  {s['name']}({s['code']}): {s.get('close',0):,}원 ({chg:+.2f}%) "
                      f"| Vol {vol_r:.1f}x | EMA:{ema} RSI:{rsi} MACD:{macd}")

    # 뉴스
    news = report.get("news", [])
    if news:
        lines.append("")
        lines.append("--- SpaceX 뉴스 ---")
        for n in news:
            lines.append(f"  [{n.get('keyword','')}] {n['title']}")

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = generate_spacex_report()
    print(format_spacex_summary(report))
