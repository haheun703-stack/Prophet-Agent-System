#!/usr/bin/env python3
"""
중장기 모멘텀 스크리너 (Momentum Screener)
==========================================
주 1회 실행, 시총 200억+ 전종목 대상으로 "강한 상승 모멘텀" 종목을 발굴.
Body Hunter(단타)와 별개의 중장기 관심 종목 리스트 제공.

5팩터 스코어링:
  F1. 3M 수익률 (30)   — 중기 추세 강도
  F2. 1M 수익률 (20)   — 단기 가속도
  F3. 거래대금 증가율 (25) — 관심 유입 감지
  F4. 추세 안정성 (15)  — MA20/MA60 위 여부
  F5. 신고가 근접도 (10) — 돌파 가능성

등급:
  S (슈퍼 모멘텀) : score >= 80, 3M +200%+
  A (강한 모멘텀) : score >= 65, 3M +100%+
  B (성장 모멘텀) : score >= 50, 3M +50%+

Usage:
    python data/momentum_screener.py --test                    # 기본 실행 (텔레그램 미발송)
    python data/momentum_screener.py --rebuild-universe --test # 유니버스 재빌드 + 실행
    python data/momentum_screener.py --min-cap 500 --test      # 시총 500억+ 필터
    python data/momentum_screener.py --min-return 100 --top 30 # 3M +100%+ 상위 30개
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = BASE_DIR / "data_store"
DAILY_DIR = DATA_STORE / "daily"
UNIVERSE_PATH = DATA_STORE / "universe.json"
MOMENTUM_UNIVERSE_PATH = DATA_STORE / "universe_momentum.json"
MOMENTUM_RESULT_PATH = DATA_STORE / "momentum_latest.json"

load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BH.MomentumScreener")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")


def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT:
        print(text)
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        for i in range(0, len(text), 4000):
            requests.post(
                url, json={"chat_id": TG_CHAT, "text": text[i : i + 4000]}, timeout=10
            )
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")
        print(text)


# ═══════════════════════════════════════════════════
#  1. 확장 유니버스 빌드 (네이버 금융 크롤링)
# ═══════════════════════════════════════════════════


def build_momentum_universe(min_cap: int = 200) -> dict:
    """시총 min_cap 억+ 전종목 유니버스 생성 (네이버 금융)

    기존 build_universe(pykrx) 대비:
    - 종목명 개별 조회 없어 10x 빠름
    - 인코딩 이슈 없음 (Windows 안전)
    - 캐시: universe_momentum.json (주 1회 갱신)
    """
    from bs4 import BeautifulSoup

    print(f"\n  모멘텀 유니버스 빌드 - 시총 {min_cap:,}억원+")
    print("=" * 60)

    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    exclude_keywords = ["스팩", "SPAC", "리츠"]
    universe = {}

    for sosok, market_name in [(0, "KOSPI"), (1, "KOSDAQ")]:
        page = 1
        last_page = 1
        reached_min = False

        while page <= last_page and not reached_min:
            url = (
                f"https://finance.naver.com/sise/sise_market_sum.naver"
                f"?sosok={sosok}&page={page}"
            )
            try:
                resp = requests.get(url, headers=hdrs, timeout=10)
                resp.encoding = "euc-kr"
                soup = BeautifulSoup(resp.text, "html.parser")

                if page == 1:
                    pgn = soup.select(".pgRR a")
                    if pgn:
                        last_page = int(pgn[0]["href"].split("page=")[1])
                    print(f"  {market_name}: {last_page}페이지 스캔...")

                rows = soup.select("table.type_2 tr")
                for tr in rows:
                    tds = tr.select("td")
                    if len(tds) < 10:
                        continue

                    link = tds[1].select_one("a")
                    if not link:
                        continue
                    name = link.text.strip()
                    href = link.get("href", "")
                    if "code=" not in href:
                        continue
                    code = href.split("code=")[1][:6]

                    cap_text = tds[6].text.strip().replace(",", "")
                    if not cap_text or not cap_text.isdigit():
                        continue
                    cap_v = int(cap_text)

                    if cap_v < min_cap:
                        reached_min = True
                        break

                    vol_text = tds[9].text.strip().replace(",", "")
                    vol = int(vol_text) if vol_text.isdigit() else 0

                    skip = False
                    for kw in exclude_keywords:
                        if kw in name:
                            skip = True
                            break
                    if skip:
                        continue

                    suffix = ".KS" if market_name == "KOSPI" else ".KQ"
                    universe[code] = {
                        "name": name,
                        "market": market_name,
                        "suffix": suffix,
                        "cap": cap_v,
                        "volume": vol,
                    }
            except Exception as e:
                print(f"  {market_name} p{page} 오류: {e}")

            page += 1
            time.sleep(0.3)

        mkt_cnt = sum(1 for v in universe.values() if v["market"] == market_name)
        print(f"  {market_name}: {mkt_cnt}종목")

    # 기존 universe.json 에서 섹터 정보 보강
    if UNIVERSE_PATH.exists():
        with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
            main_uni = json.load(f)
        for code, info in universe.items():
            if code in main_uni:
                info["sector"] = main_uni[code].get("sector", "기타")
            else:
                info["sector"] = "기타"

    universe = dict(sorted(universe.items(), key=lambda x: -x[1]["cap"]))

    print(f"\n  총 유니버스: {len(universe)}종목")

    DATA_STORE.mkdir(parents=True, exist_ok=True)
    with open(MOMENTUM_UNIVERSE_PATH, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    print(f"  저장: {MOMENTUM_UNIVERSE_PATH}")

    return universe


def load_momentum_universe() -> dict:
    """캐시된 모멘텀 유니버스 로드"""
    if MOMENTUM_UNIVERSE_PATH.exists():
        with open(MOMENTUM_UNIVERSE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ═══════════════════════════════════════════════════
#  2. OHLCV 데이터 조회 (하이브리드)
# ═══════════════════════════════════════════════════


def _batch_download_yfinance(
    codes_with_suffix: list, days: int = 180
) -> dict[str, pd.DataFrame]:
    """yfinance 배치 다운로드 (로컬 CSV 없는 종목용)

    Args:
        codes_with_suffix: [(code, suffix), ...]
    Returns:
        {code: DataFrame}   — 컬럼: 시가/고가/저가/종가/거래량
    """
    import yfinance as yf

    if not codes_with_suffix:
        return {}

    results = {}
    batch_size = 50
    period = "1y" if days > 126 else "6mo"
    total = len(codes_with_suffix)

    for i in range(0, total, batch_size):
        batch = codes_with_suffix[i : i + batch_size]
        tickers = [f"{code}{suffix}" for code, suffix in batch]
        ticker_str = " ".join(tickers)

        try:
            data = yf.download(
                ticker_str, period=period, progress=False, timeout=30
            )

            if data is None or data.empty:
                continue

            for code, suffix in batch:
                yf_ticker = f"{code}{suffix}"
                try:
                    if len(batch) == 1:
                        df = data.copy()
                    else:
                        # yfinance multi-ticker: MultiIndex columns
                        if isinstance(data.columns, pd.MultiIndex):
                            # (Price, Ticker) 형태
                            cols_for_ticker = [
                                c for c in data.columns if yf_ticker in str(c)
                            ]
                            if not cols_for_ticker:
                                continue
                            df = data[cols_for_ticker].copy()
                            df.columns = df.columns.droplevel(-1)
                        else:
                            continue

                    df = df.dropna(how="all")
                    if len(df) < int(days * 0.5):
                        continue

                    # 컬럼 표준화
                    col_map = {}
                    for col in df.columns:
                        cl = str(col).lower()
                        if "open" in cl:
                            col_map[col] = "시가"
                        elif "high" in cl:
                            col_map[col] = "고가"
                        elif "low" in cl:
                            col_map[col] = "저가"
                        elif "close" in cl and "adj" not in cl:
                            col_map[col] = "종가"
                        elif "volume" in cl:
                            col_map[col] = "거래량"
                    df = df.rename(columns=col_map)
                    if "종가" in df.columns:
                        results[code] = df.tail(days + 20)
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"yfinance batch {i}~{i + batch_size} 실패: {e}")

        if i + batch_size < total:
            time.sleep(0.5)

    return results


# ═══════════════════════════════════════════════════
#  3. 모멘텀 스코어링 (5팩터)
# ═══════════════════════════════════════════════════


def _score_stock(df: pd.DataFrame) -> Optional[dict]:
    """5팩터 모멘텀 스코어 계산

    Returns dict or None (필터 미달)
    """
    if "종가" not in df.columns or "거래량" not in df.columns:
        return None

    close = df["종가"].dropna()
    volume = df["거래량"].dropna()

    if len(close) < 40:
        return None

    price_now = float(close.iloc[-1])
    if price_now <= 0:
        return None

    # ── F1: 3M 수익률 (30점) ──
    idx_3m = min(63, len(close) - 1)
    price_3m = float(close.iloc[-(idx_3m + 1)])
    return_3m = (price_now / price_3m - 1) * 100 if price_3m > 0 else 0

    # ── F2: 1M 수익률 (20점) ──
    idx_1m = min(21, len(close) - 1)
    price_1m = float(close.iloc[-(idx_1m + 1)])
    return_1m = (price_now / price_1m - 1) * 100 if price_1m > 0 else 0

    # ── F3: 거래대금 증가율 (25점) ──
    tv = close * volume
    recent_20 = tv.iloc[-20:]
    if len(tv) >= 80:
        prior_60 = tv.iloc[-80:-20]
    else:
        prior_60 = tv.iloc[: max(1, len(tv) - 20)]

    avg_recent = recent_20.mean()
    avg_prior = prior_60.mean()
    tv_growth = avg_recent / avg_prior if avg_prior > 0 else 1.0
    avg_tv = avg_recent / 1e8  # 억원

    # ── F4: 추세 안정성 (15점) ──
    ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else price_now
    ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else ma20
    trend_ok = 1 if (price_now > ma20 and price_now > ma60) else 0

    # ── F5: 신고가 근접도 (10점) ──
    high_max = float(close.max())
    high_pct = (price_now / high_max) * 100 if high_max > 0 else 0

    # ═══ 노이즈 필터 ═══

    # 양봉 비율: 최근 60일 중 40% 미만이면 추세 아님
    n_tail = min(60, len(close))
    recent_tail = close.iloc[-n_tail:]
    if len(recent_tail) >= 10:
        changes = recent_tail.diff().dropna()
        pos_ratio = (changes > 0).sum() / len(changes)
        if pos_ratio < 0.40:
            return None

    # ═══ 스코어 ═══
    f1 = min(30, max(0, return_3m * 0.10))  # +300% → 30
    f2 = min(20, max(0, return_1m * 0.40))  # +50% → 20
    f3 = min(25, max(0, (tv_growth - 1) * 10))  # 3.5x → 25
    f4 = 15.0 * trend_ok
    f5 = min(10, max(0, (high_pct - 70) / 3))  # 70%→0, 100%→10

    score = f1 + f2 + f3 + f4 + f5

    # ═══ 등급 ═══
    if score >= 80 and return_3m >= 200:
        grade = "S"
    elif score >= 65 and return_3m >= 100:
        grade = "A"
    elif score >= 50 and return_3m >= 50:
        grade = "B"
    else:
        grade = None

    return {
        "score": round(score, 1),
        "grade": grade,
        "return_3m": round(return_3m, 1),
        "return_1m": round(return_1m, 1),
        "tv_growth": round(tv_growth, 2),
        "avg_tv": round(avg_tv, 1),
        "trend_ok": trend_ok,
        "high_pct": round(high_pct, 1),
        "price_now": int(price_now),
        "factors": {
            "f1_return3m": round(f1, 1),
            "f2_return1m": round(f2, 1),
            "f3_tv_growth": round(f3, 1),
            "f4_trend": round(f4, 1),
            "f5_high_prox": round(f5, 1),
        },
    }


# ═══════════════════════════════════════════════════
#  4. 메인 스크리너
# ═══════════════════════════════════════════════════


def run_screener(
    min_cap: int = 200,
    min_return_3m: float = 50,
    min_tv: float = 5,
    top_n: int = 20,
    rebuild_universe: bool = False,
) -> tuple[list[dict], int]:
    """중장기 모멘텀 스크리너 실행

    Returns:
        (results, universe_size)
    """
    # 1. 유니버스
    if rebuild_universe or not MOMENTUM_UNIVERSE_PATH.exists():
        universe = build_momentum_universe(min_cap)
    else:
        universe = load_momentum_universe()
        universe = {k: v for k, v in universe.items() if v.get("cap", 0) >= min_cap}

    uni_size = len(universe)
    logger.info(f"유니버스: {uni_size}종목 (시총 {min_cap}억+)")

    # 2. 로컬 CSV vs yfinance 분리
    local_codes = []
    remote_codes = []

    for code, info in universe.items():
        csv_path = DAILY_DIR / f"{code}.csv"
        if csv_path.exists():
            local_codes.append(code)
        else:
            remote_codes.append((code, info.get("suffix", ".KS")))

    logger.info(f"  로컬 CSV: {len(local_codes)} | yfinance: {len(remote_codes)}")

    # 3. 로컬 CSV 스캔
    results = []
    t0 = time.time()

    for code in local_codes:
        try:
            df = pd.read_csv(
                DAILY_DIR / f"{code}.csv", index_col=0, parse_dates=True
            )
            scored = _score_stock(df)
            if (
                scored
                and scored["grade"]
                and scored["return_3m"] >= min_return_3m
                and scored["avg_tv"] >= min_tv
            ):
                info = universe[code]
                results.append(
                    {
                        "code": code,
                        "name": info.get("name", code),
                        "market": info.get("market", ""),
                        "cap": info.get("cap", 0),
                        "sector": info.get("sector", "기타"),
                        **scored,
                    }
                )
        except Exception:
            continue

    t1 = time.time()
    logger.info(f"  로컬 스캔: {len(results)}종목 통과 ({t1 - t0:.1f}초)")

    # 4. yfinance 배치 (로컬 CSV 없는 종목)
    if remote_codes:
        logger.info(f"  yfinance 배치 다운로드: {len(remote_codes)}종목...")
        yf_data = _batch_download_yfinance(remote_codes, days=180)

        yf_pass = 0
        for code, df in yf_data.items():
            try:
                scored = _score_stock(df)
                if (
                    scored
                    and scored["grade"]
                    and scored["return_3m"] >= min_return_3m
                    and scored["avg_tv"] >= min_tv
                ):
                    info = universe.get(code, {})
                    results.append(
                        {
                            "code": code,
                            "name": info.get("name", code),
                            "market": info.get("market", ""),
                            "cap": info.get("cap", 0),
                            "sector": info.get("sector", "기타"),
                            **scored,
                        }
                    )
                    yf_pass += 1
            except Exception:
                continue

        t2 = time.time()
        logger.info(
            f"  yfinance 스캔: {len(yf_data)}종목 다운로드, {yf_pass}종목 통과 ({t2 - t1:.1f}초)"
        )

    # 5. 정렬 (스코어 → 3M 수익률)
    results.sort(key=lambda x: (-x["score"], -x["return_3m"]))

    # 6. 저장
    save_data = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "universe_size": uni_size,
        "local_count": len(local_codes),
        "remote_count": len(remote_codes),
        "min_cap": min_cap,
        "min_return_3m": min_return_3m,
        "min_tv": min_tv,
        "total_pass": len(results),
        "results": results[: top_n * 2],
    }
    DATA_STORE.mkdir(parents=True, exist_ok=True)
    with open(MOMENTUM_RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)

    return results[:top_n], uni_size


# ═══════════════════════════════════════════════════
#  5. 텔레그램 리포트
# ═══════════════════════════════════════════════════


def _fmt_cap(n: float) -> str:
    """시총 포맷 (억→조)"""
    if abs(n) >= 10000:
        return f"{n / 10000:.1f}조"
    return f"{n:,.0f}억"


def format_report(results: list, universe_size: int, min_cap: int) -> str:
    today = datetime.now().strftime("%Y-%m-%d")

    lines = [
        "=" * 44,
        "  중장기 모멘텀 스크리너 (주간 리포트)",
        f"  {today} | 시총 {min_cap}억+ | {universe_size:,}종목 스캔",
        "=" * 44,
        "",
    ]

    grades = {"S": [], "A": [], "B": []}
    for r in results:
        g = r.get("grade", "B")
        if g in grades:
            grades[g].append(r)

    grade_labels = {
        "S": ("슈퍼 모멘텀", "테마 대장주 후보"),
        "A": ("강한 모멘텀", "초기~중기 급등주"),
        "B": ("성장 모멘텀", "상승 초기 종목"),
    }

    rank = 1
    for g in ["S", "A", "B"]:
        items = grades[g]
        if not items:
            continue

        label, desc = grade_labels[g]
        lines.append(f"[{g}등급] {label} ({len(items)}종목) - {desc}")

        for r in items:
            tv_str = (
                f"TV {r['tv_growth']:.1f}x"
                if r["tv_growth"] >= 1.5
                else f"TV {r['avg_tv']:.0f}억"
            )
            cap_str = _fmt_cap(r["cap"])
            lines.append(
                f"  {rank:>2}. {r['name'][:8]:<8}"
                f" 3M{r['return_3m']:>+7.0f}%"
                f" 1M{r['return_1m']:>+5.0f}%"
                f" {tv_str}"
                f"  시총{cap_str}"
            )
            rank += 1

        lines.append("")

    if not any(grades.values()):
        lines.append("  조건 충족 종목 없음")
        lines.append("")

    lines.append("=" * 44)
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  6. CLI
# ═══════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="중장기 모멘텀 스크리너")
    parser.add_argument(
        "--min-cap", type=int, default=200, help="최소 시총 (억, 기본 200)"
    )
    parser.add_argument(
        "--min-return", type=float, default=50, help="최소 3M 수익률 (%%, 기본 50)"
    )
    parser.add_argument(
        "--min-tv", type=float, default=5, help="최소 일평균 거래대금 (억, 기본 5)"
    )
    parser.add_argument("--top", type=int, default=20, help="상위 N개 (기본 20)")
    parser.add_argument(
        "--rebuild-universe", action="store_true", help="유니버스 재빌드"
    )
    parser.add_argument("--test", action="store_true", help="텔레그램 미발송")
    args = parser.parse_args()

    if args.test:
        global TG_TOKEN
        TG_TOKEN = None

    results, uni_size = run_screener(
        min_cap=args.min_cap,
        min_return_3m=args.min_return,
        min_tv=args.min_tv,
        top_n=args.top,
        rebuild_universe=args.rebuild_universe,
    )

    report = format_report(results, uni_size, args.min_cap)
    tg_send(report)

    logger.info(f"완료: {len(results)}종목 발굴")


if __name__ == "__main__":
    main()
