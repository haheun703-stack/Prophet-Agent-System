#!/usr/bin/env python3
"""
수익률 스캐너 (Market Performance Scanner)
- 한국 개별주 TOP N (로컬 CSV)
- 한국 ETF TOP N (pykrx)
- 글로벌 자산: 원자재/크립토/지수 (yfinance)

Usage:
    python data/market_scanner.py                         # 기본 (1년, TOP 10)
    python data/market_scanner.py --period 1y --top 10
    python data/market_scanner.py --period 6m --top 20 --min-tv 100
    python data/market_scanner.py --category stocks --test
    python data/market_scanner.py --period ytd --category global
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BH.Scanner")

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = BASE_DIR / "data_store"
DAILY_DIR = DATA_STORE / "daily"
UNIVERSE_PATH = DATA_STORE / "universe.json"

# ── 텔레그램 ──
from dotenv import load_dotenv
load_dotenv(BASE_DIR.parent / ".env")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")


def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT:
        print(text)
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        # 텔레그램 4096자 제한 → 분할
        for i in range(0, len(text), 4000):
            requests.post(url, json={"chat_id": TG_CHAT, "text": text[i:i+4000]}, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")
        print(text)


# ── period 매핑 ──
PERIOD_MAP = {
    "1m": {"days": 21, "label": "1개월", "yf": "1mo"},
    "3m": {"days": 63, "label": "3개월", "yf": "3mo"},
    "6m": {"days": 126, "label": "6개월", "yf": "6mo"},
    "1y": {"days": 252, "label": "1년", "yf": "1y"},
    "2y": {"days": 504, "label": "2년", "yf": "2y"},
    "ytd": {"days": None, "label": "연초대비(YTD)", "yf": "ytd"},
}


# ═══════════════════════════════════════════════
# 1. 한국 개별주 스캔 (로컬 CSV)
# ═══════════════════════════════════════════════

def scan_korean_stocks(period_days: int = 252, top_n: int = 10,
                       min_tv: float = 10.0) -> list[dict]:
    """로컬 CSV에서 전종목 1년 수익률 계산

    Args:
        period_days: 비교 기간 (거래일 수)
        top_n: 상위 N개
        min_tv: 최소 일평균 거래대금 (억원)
    """
    logger.info(f"[개별주] 스캔 시작 (기간: {period_days}일, 거래대금 {min_tv}억+)")

    # 유니버스 로드
    if not UNIVERSE_PATH.exists():
        logger.error("universe.json 없음")
        return []

    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        universe = json.load(f)

    results = []
    errors = 0

    for code, info in universe.items():
        csv_path = DAILY_DIR / f"{code}.csv"
        if not csv_path.exists():
            errors += 1
            continue

        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            if len(df) < period_days + 5:
                continue

            # period_days 전 종가 vs 최근 종가
            price_now = df["종가"].iloc[-1]
            price_before = df["종가"].iloc[-period_days]

            if price_before <= 0 or price_now <= 0:
                continue

            return_pct = (price_now / price_before - 1) * 100

            # 일평균 거래대금 (최근 period_days 기간)
            recent = df.iloc[-period_days:]
            trading_values = recent["종가"] * recent["거래량"]
            avg_tv = trading_values.mean() / 1e8  # 억원

            if avg_tv < min_tv:
                continue

            results.append({
                "code": code,
                "name": info.get("name", code),
                "market": info.get("market", ""),
                "sector": info.get("sector", ""),
                "price_before": int(price_before),
                "price_now": int(price_now),
                "return_pct": round(return_pct, 1),
                "avg_tv": round(avg_tv, 0),
            })
        except Exception:
            errors += 1

    # 수익률 내림차순
    results.sort(key=lambda x: x["return_pct"], reverse=True)
    logger.info(f"[개별주] 완료: {len(results)}종목 (에러 {errors}건)")
    return results[:top_n]


# ═══════════════════════════════════════════════
# 2. 한국 ETF 스캔 (yfinance)
# ═══════════════════════════════════════════════

# 주요 한국 ETF 유니버스 (코드: 이름)
# 레버리지/인버스/일반 전부 포함, 인버스는 스캔시 자동 제외
_ETF_UNIVERSE = {
    # ── 시장 대표 ──
    "069500": "KODEX 200",
    "229200": "KODEX 코스닥150",
    "122630": "KODEX 레버리지",
    "233740": "KODEX 코스닥150레버리지",
    "252670": "KODEX 200선물인버스2X",
    "251340": "KODEX 코스닥150선물인버스",
    "114800": "KODEX 인버스",
    "102110": "TIGER 200",
    "091220": "TIGER 반도체",
    # ── 반도체/AI ──
    "091160": "KODEX 반도체",
    "395170": "KODEX K-반도체MoM",
    "469150": "ACE AI반도체포커스",
    "442580": "PLUS 글로벌HBM반도체",
    "396500": "TIGER 반도체TOP10",
    "462010": "TIGER 반도체TOP10레버리지",
    "091230": "TIGER 반도체핵심기술",
    "456600": "ACE 엔비디아채권혼합블룸버그",
    # ── IT/2차전지 ──
    "098560": "TIGER 200IT",
    "305540": "TIGER 2차전지테마",
    "373220": "KODEX 2차전지레버리지",
    "364980": "KODEX 2차전지산업",
    "371460": "TIGER 차이나전기차SOLACTIVE",
    # ── 방산/우주 ──
    "449450": "PLUS K방산",
    "463250": "TIGER K방산&우주",
    "490480": "SOL K방산",
    # "464520": "KODEX K방산TOP10",  # yfinance 미지원
    "421320": "PLUS 우주항공&UAM",
    # ── 조선/원전/전력 ──
    "266370": "KODEX 조선해양",
    "434730": "HANARO 원자력iSelect",
    "466950": "ACE 원전밸류체인액티브",
    "457990": "PLUS 태양광&ESS",
    # "487790": "KODEX K-전력인프라",  # yfinance 미지원
    # ── 건설/인프라 ──
    "117700": "KODEX 건설",
    "139230": "TIGER 200건설",
    # ── 바이오/헬스케어 ──
    "244580": "KODEX 바이오",
    "227540": "TIGER 200헬스케어",
    "462900": "ACE 글로벌바이오테크TOP10",
    # ── 금융/은행/증권 ──
    "091170": "KODEX 은행",
    "102970": "KODEX 증권",
    "140700": "KODEX 보험",
    # ── 소비/유통 ──
    "266410": "KODEX 경기소비재",
    # ── 로봇 ──
    "396520": "TIGER K로보틱스&AI",
    "463050": "ACE AI&로봇",
    # "458740": "KODEX K-로봇액티브",  # yfinance 미지원
    # ── 금/원자재 ──
    "132030": "KODEX 골드선물(H)",
    "411060": "ACE KRX금현물",
    "473640": "HANARO 글로벌금채굴기업",
    "139320": "TIGER 금속선물(H)",
    # ── 해외지수 ──
    "143850": "TIGER 미국S&P500",
    "133690": "TIGER 미국나스닥100",
    "381170": "TIGER 미국테크TOP10INDXX",
    "371160": "TIGER 차이나CSI300",
    "192090": "TIGER 차이나CSI300레버리지",
    "453810": "TIGER 차이나과창판50",
    "195930": "TIGER 유로스톡스50(합성H)",
    "241390": "TIGER 미국MSCI리츠(합성H)",
    "238720": "TIGER 일본니케이225",
    "261220": "KODEX WTI원유선물(H)",
    "130680": "TIGER 원유선물Enhanced(H)",
    # ── 배당/가치 ──
    "211560": "TIGER 배당성장",
    "104530": "KODEX 고배당",
    "161510": "TIGER 배당프리미엄",
    "337140": "KODEX 고배당커버드콜",
    # ── 채권 ──
    "148070": "KOSEF 국고채10년",
    "114260": "KODEX 국고채3년",
    "304660": "KODEX 미국채울트라30년선물(H)",
    "453850": "ACE 미국30년국채엔화노출(합성H)",
    # ── 기타 테마 ──
    "364690": "KODEX K-게임",
    "314700": "TIGER 글로벌리튬&2차전지SOLACTIVE",
    "401470": "TIGER AI코리아그로스",
    "394670": "TIGER 머신러닝&AI",
}


def scan_korean_etfs(period_days: int = 252, top_n: int = 10) -> list[dict]:
    """yfinance로 주요 ETF 수익률 계산"""
    logger.info(f"[ETF] 스캔 시작 (기간: {period_days}일, {len(_ETF_UNIVERSE)}개 ETF)")

    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance 미설치")
        return []

    # yfinance 티커 생성 (KRX ETF는 .KS suffix)
    yf_tickers = [f"{code}.KS" for code in _ETF_UNIVERSE.keys()]

    # period 매핑
    if period_days <= 30:
        yf_period = "3mo"
    elif period_days <= 70:
        yf_period = "6mo"
    elif period_days <= 260:
        yf_period = "1y"
    else:
        yf_period = "2y"

    try:
        data = yf.download(yf_tickers, period=yf_period, progress=False, threads=True)
        if data.empty:
            logger.error("ETF yfinance 데이터 없음")
            return []
    except Exception as e:
        logger.error(f"ETF yfinance 다운로드 실패: {e}")
        return []

    close = data["Close"] if "Close" in data.columns.get_level_values(0) else data

    results = []
    for code, name in _ETF_UNIVERSE.items():
        yf_ticker = f"{code}.KS"

        # 인버스 ETF 제외
        if "인버스" in name or "inverse" in name.lower():
            continue

        try:
            if yf_ticker not in close.columns:
                continue

            series = close[yf_ticker].dropna()
            min_days = int(period_days * 0.85)  # 85% 이상 데이터 있으면 OK
            if len(series) < min_days:
                continue

            price_now = float(series.iloc[-1])
            # 가능한 한 period_days 전 데이터, 없으면 가장 오래된 것
            idx = min(period_days, len(series) - 1)
            price_before = float(series.iloc[-idx])

            if price_before <= 0 or price_now <= 0:
                continue

            return_pct = (price_now / price_before - 1) * 100

            is_leverage = "레버리지" in name or "2X" in name.upper()

            results.append({
                "code": code,
                "name": name,
                "return_pct": round(return_pct, 1),
                "leverage": is_leverage,
                "price_before": int(price_before),
                "price_now": int(price_now),
            })
        except Exception:
            continue

    results.sort(key=lambda x: x["return_pct"], reverse=True)
    logger.info(f"[ETF] 완료: {len(results)}개 ETF 조회 성공")
    return results[:top_n]


# ═══════════════════════════════════════════════
# 3. 글로벌 자산 스캔 (yfinance)
# ═══════════════════════════════════════════════

GLOBAL_ASSETS = {
    # 원자재
    "GC=F":    {"name": "금(Gold)",     "category": "원자재"},
    "SI=F":    {"name": "은(Silver)",    "category": "원자재"},
    "CL=F":    {"name": "원유(WTI)",     "category": "원자재"},
    "HG=F":    {"name": "구리(Copper)",  "category": "원자재"},
    "NG=F":    {"name": "천연가스",      "category": "원자재"},
    # 크립토
    "BTC-USD":  {"name": "비트코인",     "category": "크립토"},
    "ETH-USD":  {"name": "이더리움",     "category": "크립토"},
    "SOL-USD":  {"name": "솔라나",       "category": "크립토"},
    "XRP-USD":  {"name": "리플",         "category": "크립토"},
    "BNB-USD":  {"name": "BNB",          "category": "크립토"},
    "DOGE-USD": {"name": "도지코인",     "category": "크립토"},
    "ADA-USD":  {"name": "카르다노",     "category": "크립토"},
    # 글로벌 지수
    "^KS11":   {"name": "KOSPI",        "category": "지수"},
    "^KQ11":   {"name": "KOSDAQ",       "category": "지수"},
    "^GSPC":   {"name": "S&P500",       "category": "지수"},
    "^IXIC":   {"name": "NASDAQ",       "category": "지수"},
    "^DJI":    {"name": "다우존스",      "category": "지수"},
    "^HSI":    {"name": "항셍",          "category": "지수"},
    "^N225":   {"name": "니케이225",     "category": "지수"},
}


def scan_global_assets(yf_period: str = "1y") -> list[dict]:
    """yfinance로 글로벌 자산 수익률 계산"""
    logger.info(f"[글로벌] 스캔 시작 (period={yf_period})")

    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance 미설치")
        return []

    tickers = list(GLOBAL_ASSETS.keys())

    try:
        data = yf.download(tickers, period=yf_period, progress=False, threads=True)
        if data.empty:
            logger.error("yfinance 데이터 없음")
            return []
    except Exception as e:
        logger.error(f"yfinance 다운로드 실패: {e}")
        return []

    results = []
    close = data["Close"] if "Close" in data.columns.get_level_values(0) else data

    for ticker, info in GLOBAL_ASSETS.items():
        try:
            if ticker not in close.columns:
                continue

            series = close[ticker].dropna()
            if len(series) < 10:
                continue

            price_before = series.iloc[0]
            price_now = series.iloc[-1]

            if price_before <= 0 or price_now <= 0:
                continue

            return_pct = (price_now / price_before - 1) * 100

            results.append({
                "ticker": ticker,
                "name": info["name"],
                "category": info["category"],
                "price_before": round(float(price_before), 2),
                "price_now": round(float(price_now), 2),
                "return_pct": round(float(return_pct), 1),
            })
        except Exception:
            continue

    results.sort(key=lambda x: x["return_pct"], reverse=True)
    logger.info(f"[글로벌] 완료: {len(results)}개 자산")
    return results


# ═══════════════════════════════════════════════
# 4. 보고서 생성
# ═══════════════════════════════════════════════

def _fmt_num(n: float) -> str:
    """숫자 포맷: +1,234.5%"""
    sign = "+" if n > 0 else ""
    return f"{sign}{n:,.1f}%"


def _fmt_price(p: float, is_global: bool = False) -> str:
    """가격 포맷"""
    if is_global:
        if p >= 1000:
            return f"${p:,.0f}"
        elif p >= 1:
            return f"${p:,.2f}"
        else:
            return f"${p:.4f}"
    else:
        return f"{int(p):,}원"


def generate_report(stocks: list, etfs: list, globals_: list,
                    period_label: str, args) -> str:
    """3가지 카테고리 종합 보고서"""
    lines = []
    lines.append("=" * 50)
    lines.append(f"  수익률 스캐너 -- {period_label} 기준 ({datetime.now().strftime('%Y-%m-%d')})")
    lines.append("=" * 50)

    # ── 1. 한국 개별주 ──
    if stocks:
        lines.append("")
        lines.append(f"### 1. 한국 개별주 TOP {len(stocks)} (거래대금 {args.min_tv:.0f}억+)")
        lines.append("")
        lines.append(f"{'순위':>4} | {'종목':<16} | {'수익률':>10} | {'일평균거래대금':>12} | {'시장':<6}")
        lines.append("-" * 65)
        for i, s in enumerate(stocks, 1):
            lines.append(
                f"{i:>4} | {s['name']:<16} | {_fmt_num(s['return_pct']):>10} | "
                f"{s['avg_tv']:>10,.0f}억 | {s['market']:<6}"
            )

    # ── 2. 한국 ETF ──
    if etfs:
        lines.append("")
        lines.append(f"### 2. 한국 ETF TOP {len(etfs)}")
        lines.append("")
        lines.append(f"{'순위':>4} | {'ETF':<30} | {'수익률':>10} | {'구분':<8}")
        lines.append("-" * 65)
        for i, e in enumerate(etfs, 1):
            tag = "레버리지" if e.get("leverage") else "일반"
            lines.append(
                f"{i:>4} | {e['name']:<30} | {_fmt_num(e['return_pct']):>10} | {tag:<8}"
            )

    # ── 3. 글로벌 자산 ──
    if globals_:
        lines.append("")
        lines.append("### 3. 글로벌 자산 (원자재/크립토/지수)")
        lines.append("")
        lines.append(f"{'순위':>4} | {'자산':<14} | {'수익률':>10} | {'현재가':>14} | {'카테고리':<6}")
        lines.append("-" * 65)
        for i, g in enumerate(globals_, 1):
            lines.append(
                f"{i:>4} | {g['name']:<14} | {_fmt_num(g['return_pct']):>10} | "
                f"{_fmt_price(g['price_now'], True):>14} | {g['category']:<6}"
            )

    # ── 4. 자산군 비교 요약 ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("  자산군 비교 요약")
    lines.append("=" * 50)

    if stocks:
        lines.append(f"  개별주 TOP1: {stocks[0]['name']} {_fmt_num(stocks[0]['return_pct'])}")
    if etfs:
        lines.append(f"  ETF TOP1:    {etfs[0]['name']} {_fmt_num(etfs[0]['return_pct'])}")

    # 카테고리별 최고
    for cat in ["원자재", "크립토", "지수"]:
        cat_items = [g for g in globals_ if g["category"] == cat]
        if cat_items:
            best = cat_items[0]  # 이미 정렬됨
            lines.append(f"  {cat} TOP1: {best['name']} {_fmt_num(best['return_pct'])}")

    lines.append("")
    return "\n".join(lines)


# ═══════════════════════════════════════════════
# 5. 메인
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="수익률 스캐너")
    parser.add_argument("--period", default="1y",
                        choices=["1m", "3m", "6m", "1y", "2y", "ytd"],
                        help="비교 기간 (기본: 1y)")
    parser.add_argument("--top", type=int, default=10,
                        help="상위 N개 (기본: 10)")
    parser.add_argument("--min-tv", type=float, default=10,
                        help="최소 일평균 거래대금 억원 (기본: 10)")
    parser.add_argument("--test", action="store_true",
                        help="텔레그램 미발송")
    parser.add_argument("--category", default="all",
                        choices=["stocks", "etf", "global", "all"],
                        help="카테고리 선택 (기본: all)")
    args = parser.parse_args()

    if args.test:
        global TG_TOKEN
        TG_TOKEN = None

    period_info = PERIOD_MAP[args.period]
    period_label = period_info["label"]
    yf_period = period_info["yf"]

    # period_days 계산 (ytd는 동적)
    if args.period == "ytd":
        jan1 = datetime(datetime.now().year, 1, 1)
        cal_days = (datetime.now() - jan1).days
        period_days = max(int(cal_days * 0.7), 10)  # 대략 거래일
    else:
        period_days = period_info["days"]

    logger.info(f"수익률 스캐너 시작 (기간: {period_label}, TOP {args.top}, 거래대금 {args.min_tv}억+)")

    stocks, etfs, globals_ = [], [], []

    # 카테고리별 실행
    if args.category in ("stocks", "all"):
        stocks = scan_korean_stocks(period_days, args.top, args.min_tv)

    if args.category in ("etf", "all"):
        etfs = scan_korean_etfs(period_days, args.top)

    if args.category in ("global", "all"):
        globals_ = scan_global_assets(yf_period)

    # 보고서 생성
    report = generate_report(stocks, etfs, globals_, period_label, args)

    # 출력
    print(report)

    # 텔레그램 발송
    if TG_TOKEN:
        tg_send(report)
        logger.info("텔레그램 발송 완료")

    logger.info("수익률 스캐너 완료")


if __name__ == "__main__":
    main()
