# -*- coding: utf-8 -*-
"""
ETF Universe Scanner — Body Hunter v2.3 일봉 적용
===================================================
pykrx로 전체 ETF 유니버스에서:
  1. 거래대금 필터 (유동성 확보)
  2. 레버리지/인버스/채권 제외 (순수 섹터 ETF만)
  3. NAV 대비 괴리율 계산
  4. v2.3 Body Formation 조건 스캔 (일봉 버전)
  5. 상위 종목 랭킹 출력

사용법:
  python etf_scanner.py                  # 기본 스캔
  python etf_scanner.py --top 30         # 상위 30개 출력
  python etf_scanner.py --min-vol 200    # 거래대금 200억 이상만
  python etf_scanner.py --telegram       # 텔레그램 전송

일봉 Body Hunter v2.3 해석:
  - FCR = 전일 고/저가 범위
  - Breakout = 당일 종가가 전일 고가 이탈 + 거래량 서지
  - SL = FCR고가 - (FCR범위 * 0.6)
  - TP = 진입가 + (리스크 * 2.0)
"""

import sys
import io
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from pykrx import stock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ETFScanner")


# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
# 레버리지/인버스/채권/머니마켓 키워드 (제외 대상)
EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X",
    "채권", "국고채", "통안채", "회사채", "종합채", "단기", "특수채",
    "CD금리", "KOFR", "머니마켓", "단기자금",
    "선물", "옵션", "VIX", "변동성",
    "합성", "스왑",
]

# v2.3 Body Hunter 파라미터 (일봉 버전)
V23_PARAMS = {
    "vol_surge_min": 1.3,      # 거래량 서지 배수
    "vol_avg_window": 5,       # 거래량 평균 기간
    "sl_ratio": 0.6,           # SL 비율
    "tp_rr": 2.0,              # TP Risk:Reward
    "choppy_max": 3,           # 박스권 최대 실패 횟수
    "lookback_days": 60,       # 조회 기간 (영업일)
    "breakout_window": 5,      # 최근 N일 내 이탈 확인
}


def _find_last_trading_date() -> str:
    """최근 거래일 찾기 (오늘 장중이면 전일)"""
    today = datetime.now()
    for delta in range(0, 7):
        dt = today - timedelta(days=delta)
        dt_str = dt.strftime("%Y%m%d")
        try:
            df = stock.get_etf_ohlcv_by_ticker(dt_str)
            if len(df) > 0:
                return dt_str
        except Exception:
            continue
    return (today - timedelta(days=1)).strftime("%Y%m%d")


def get_etf_universe(date: str, min_volume_bill: float = 100) -> pd.DataFrame:
    """거래대금 + 키워드 필터링된 ETF 유니버스 반환

    Args:
        date: 기준일 YYYYMMDD
        min_volume_bill: 최소 거래대금 (억원)

    Returns:
        DataFrame [ticker, name, close, NAV, nav_gap_pct, volume, trading_value_bill]
    """
    logger.info(f"전체 ETF 목록 조회 ({date})...")
    all_tickers = stock.get_etf_ticker_list(date)
    logger.info(f"전체 ETF: {len(all_tickers)}개")

    # 당일 OHLCV + NAV 일괄 조회
    df_all = stock.get_etf_ohlcv_by_ticker(date)
    if df_all.empty:
        logger.error("ETF OHLCV 데이터 없음")
        return pd.DataFrame()

    results = []
    excluded_kw = 0
    excluded_vol = 0

    for ticker in all_tickers:
        name = stock.get_etf_ticker_name(ticker)

        # 키워드 필터
        if any(kw in name for kw in EXCLUDE_KEYWORDS):
            excluded_kw += 1
            continue

        # OHLCV 데이터 존재 확인
        if ticker not in df_all.index:
            continue

        row = df_all.loc[ticker]
        close = row.get("종가", 0)
        nav = row.get("NAV", 0)
        volume = row.get("거래량", 0)
        trading_value = row.get("거래대금", 0)

        if close == 0 or volume == 0:
            continue

        trading_value_bill = trading_value / 1e8

        # 거래대금 필터
        if trading_value_bill < min_volume_bill:
            excluded_vol += 1
            continue

        # NAV 괴리율
        nav_gap_pct = (close - nav) / nav * 100 if nav > 0 else 0

        results.append({
            "ticker": ticker,
            "name": name,
            "close": close,
            "NAV": nav,
            "nav_gap_pct": nav_gap_pct,
            "volume": volume,
            "trading_value_bill": trading_value_bill,
        })

    df = pd.DataFrame(results)
    logger.info(
        f"필터 결과: {len(df)}개 통과 "
        f"(키워드 제외={excluded_kw}, 거래대금 미달={excluded_vol})"
    )
    return df


def scan_body_formation(ticker: str, end_date: str,
                        params: dict = None) -> Optional[Dict]:
    """일봉 기반 Body Hunter v2.3 스캔

    전일 고/저가 = FCR 범위
    당일 종가 > 전일 고가 + 거래량 서지 = LONG 이탈
    당일 종가 < 전일 저가 + 거래량 서지 = SHORT 이탈

    Returns:
        dict with signal info or None
    """
    p = params or V23_PARAMS
    lookback = p["lookback_days"]

    # 60영업일 전 날짜 계산 (여유 있게 100일 전)
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=int(lookback * 1.8))
    start_str = start_dt.strftime("%Y%m%d")

    try:
        df = stock.get_etf_ohlcv_by_date(start_str, end_date, ticker)
    except Exception as e:
        logger.debug(f"[{ticker}] OHLCV 조회 실패: {e}")
        return None

    if df is None or len(df) < 20:
        return None

    # 컬럼 정리
    df = df.rename(columns={
        "시가": "open", "고가": "high", "저가": "low",
        "종가": "close", "거래량": "volume", "거래대금": "value",
    })

    # FCR = 전일 범위
    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)
    df["prev_range"] = df["prev_high"] - df["prev_low"]

    # 거래량 평균
    avg_w = p["vol_avg_window"]
    df["avg_vol"] = df["volume"].rolling(avg_w).mean()
    df["vol_ratio"] = df["volume"] / df["avg_vol"]

    # 이탈 조건
    df["breakout_long"] = (df["close"] > df["prev_high"]) & \
                          (df["vol_ratio"] >= p["vol_surge_min"])
    df["breakout_short"] = (df["close"] < df["prev_low"]) & \
                           (df["vol_ratio"] >= p["vol_surge_min"])

    # 꼬리만 닿고 실패 = choppy 카운트 (최근 N일)
    df["wick_fail_long"] = (df["high"] > df["prev_high"]) & \
                           (df["close"] <= df["prev_high"])
    df["wick_fail_short"] = (df["low"] < df["prev_low"]) & \
                            (df["close"] >= df["prev_low"])

    # 최근 breakout_window 일 확인
    bw = p["breakout_window"]
    recent = df.iloc[-bw:]

    if recent.empty:
        return None

    # 최근 LONG 이탈 찾기
    long_signals = recent[recent["breakout_long"]]
    short_signals = recent[recent["breakout_short"]]

    signal = None

    if not long_signals.empty:
        last = long_signals.iloc[-1]
        signal_date = long_signals.index[-1]

        # 이 이탈 전 choppy 횟수
        pre_signal = df.loc[:signal_date].iloc[-(bw + 5):-1]
        choppy = pre_signal["wick_fail_long"].sum()

        if choppy < p["choppy_max"]:
            entry = last["close"]
            fcr_high = last["prev_high"]
            fcr_range = last["prev_range"]
            sl = fcr_high - fcr_range * p["sl_ratio"]
            risk = entry - sl
            tp = entry + risk * p["tp_rr"] if risk > 0 else entry

            signal = {
                "direction": "LONG",
                "signal_date": signal_date,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "risk": risk,
                "fcr_range": fcr_range,
                "vol_ratio": last["vol_ratio"],
                "choppy_count": int(choppy),
            }

    if not short_signals.empty and signal is None:
        last = short_signals.iloc[-1]
        signal_date = short_signals.index[-1]

        pre_signal = df.loc[:signal_date].iloc[-(bw + 5):-1]
        choppy = pre_signal["wick_fail_short"].sum()

        if choppy < p["choppy_max"]:
            entry = last["close"]
            fcr_low = last["prev_low"]
            fcr_range = last["prev_range"]
            sl = fcr_low + fcr_range * p["sl_ratio"]
            risk = sl - entry
            tp = entry - risk * p["tp_rr"] if risk > 0 else entry

            signal = {
                "direction": "SHORT",
                "signal_date": signal_date,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "risk": risk,
                "fcr_range": fcr_range,
                "vol_ratio": last["vol_ratio"],
                "choppy_count": int(choppy),
            }

    if signal is None:
        return None

    # 추세 강도: 20일 이평선 대비 위치
    ma20 = df["close"].rolling(20).mean()
    if not ma20.empty and pd.notna(ma20.iloc[-1]):
        last_close = df["close"].iloc[-1]
        signal["ma20_gap_pct"] = (last_close - ma20.iloc[-1]) / ma20.iloc[-1] * 100
    else:
        signal["ma20_gap_pct"] = 0

    # 최근 5일 수익률
    if len(df) >= 6:
        signal["return_5d"] = (df["close"].iloc[-1] / df["close"].iloc[-6] - 1) * 100
    else:
        signal["return_5d"] = 0

    return signal


def compute_ranking_score(row: dict) -> float:
    """종합 랭킹 점수 계산 (0~100)

    구성:
      - Body Signal (40점): 이탈 발생 + 거래량 서지 강도
      - NAV 괴리율 (20점): 시장가 < NAV = 저평가 기회
      - 추세 강도 (20점): MA20 대비 위치
      - 거래대금 (10점): 유동성
      - 최근 모멘텀 (10점): 5일 수익률
    """
    score = 0

    # 1) Body Signal (40점)
    if row.get("has_signal"):
        score += 20  # 이탈 존재 기본점
        vr = row.get("vol_ratio", 1.0)
        score += min(15, (vr - 1.0) * 15)  # 거래량 서지 강도
        choppy = row.get("choppy_count", 0)
        score += max(0, 5 - choppy * 2)  # choppy 적을수록 가산

    # 2) NAV 괴리율 (20점): 음수 = 시장가 < NAV = 저평가
    nav_gap = row.get("nav_gap_pct", 0)
    if nav_gap < -1.0:
        score += 20  # 1% 이상 할인
    elif nav_gap < -0.5:
        score += 15
    elif nav_gap < -0.2:
        score += 10
    elif nav_gap < 0:
        score += 5
    # 프리미엄(양수)은 0점

    # 3) 추세 강도 (20점)
    ma_gap = row.get("ma20_gap_pct", 0)
    if row.get("direction") == "LONG":
        # LONG: MA20 위일수록 유리
        if ma_gap > 5:
            score += 20
        elif ma_gap > 2:
            score += 15
        elif ma_gap > 0:
            score += 10
        else:
            score += 5
    elif row.get("direction") == "SHORT":
        # SHORT: MA20 아래일수록 유리
        if ma_gap < -5:
            score += 20
        elif ma_gap < -2:
            score += 15
        elif ma_gap < 0:
            score += 10
        else:
            score += 5

    # 4) 거래대금 (10점)
    tv = row.get("trading_value_bill", 0)
    if tv > 1000:
        score += 10
    elif tv > 500:
        score += 8
    elif tv > 200:
        score += 6
    elif tv > 100:
        score += 4
    else:
        score += 2

    # 5) 최근 모멘텀 (10점)
    ret5 = row.get("return_5d", 0)
    if row.get("direction") == "LONG":
        score += min(10, max(0, ret5 * 2))
    elif row.get("direction") == "SHORT":
        score += min(10, max(0, -ret5 * 2))

    return round(score, 1)


def run_scan(end_date: str = None, min_volume_bill: float = 100,
             top_n: int = 20) -> pd.DataFrame:
    """ETF 유니버스 전체 스캔

    Returns:
        랭킹 정렬된 DataFrame
    """
    if end_date is None:
        end_date = _find_last_trading_date()

    logger.info(f"기준일: {end_date}")

    # 1단계: ETF 유니버스 필터
    universe = get_etf_universe(end_date, min_volume_bill)
    if universe.empty:
        logger.warning("스캔 대상 ETF 없음")
        return pd.DataFrame()

    logger.info(f"Body Hunter v2.3 스캔 시작 ({len(universe)}개)...")

    # 2단계: 각 ETF에 Body Formation 스캔
    results = []
    scanned = 0

    for _, etf in universe.iterrows():
        ticker = etf["ticker"]
        name = etf["name"]
        scanned += 1

        if scanned % 20 == 0:
            logger.info(f"  스캔 진행: {scanned}/{len(universe)}...")

        signal = scan_body_formation(ticker, end_date)

        row = {
            "ticker": ticker,
            "name": name,
            "close": etf["close"],
            "NAV": etf["NAV"],
            "nav_gap_pct": etf["nav_gap_pct"],
            "trading_value_bill": etf["trading_value_bill"],
            "has_signal": signal is not None,
        }

        if signal:
            row.update({
                "direction": signal["direction"],
                "signal_date": signal["signal_date"],
                "entry": signal["entry"],
                "sl": signal["sl"],
                "tp": signal["tp"],
                "risk": signal["risk"],
                "vol_ratio": signal["vol_ratio"],
                "choppy_count": signal["choppy_count"],
                "ma20_gap_pct": signal["ma20_gap_pct"],
                "return_5d": signal["return_5d"],
            })
        else:
            row.update({
                "direction": "-",
                "signal_date": None,
                "entry": 0, "sl": 0, "tp": 0, "risk": 0,
                "vol_ratio": 0, "choppy_count": 0,
                "ma20_gap_pct": 0, "return_5d": 0,
            })

        row["score"] = compute_ranking_score(row)
        results.append(row)

    df = pd.DataFrame(results)
    df = df.sort_values("score", ascending=False)

    # 시그널 있는 것만 상위 출력
    with_signal = df[df["has_signal"]].head(top_n)
    logger.info(f"스캔 완료: {len(df)}개 중 {len(df[df['has_signal']])}개 시그널 발생")

    return df


def format_report(df: pd.DataFrame, top_n: int = 20) -> str:
    """결과를 텍스트 리포트로 포맷"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    with_signal = df[df["has_signal"]].head(top_n)
    total_signal = len(df[df["has_signal"]])
    total_scanned = len(df)

    lines.append(f"{'='*55}")
    lines.append(f"  ETF Universe Scanner — Body Hunter v2.3")
    lines.append(f"  {now}")
    lines.append(f"  스캔: {total_scanned}개 ETF | 시그널: {total_signal}개")
    lines.append(f"{'='*55}")

    if with_signal.empty:
        lines.append("\n  시그널 없음 — 관망")
        return "\n".join(lines)

    # LONG / SHORT 분리
    for direction in ["LONG", "SHORT"]:
        subset = with_signal[with_signal["direction"] == direction]
        if subset.empty:
            continue

        emoji = "+" if direction == "LONG" else "-"
        lines.append(f"\n  [{direction}] 이탈 시그널 ({len(subset)}개)")
        lines.append(f"  {'─'*50}")

        for rank, (_, row) in enumerate(subset.iterrows(), 1):
            sig_date = row["signal_date"]
            if hasattr(sig_date, "strftime"):
                sig_date_str = sig_date.strftime("%m/%d")
            else:
                sig_date_str = str(sig_date)[:5]

            lines.append(
                f"  {rank:>2}. {row['name'][:20]:<20s} "
                f"[{row['ticker']}] "
                f"점수={row['score']:>5.1f}"
            )
            lines.append(
                f"      종가={row['close']:>10,.0f}  "
                f"NAV괴리={row['nav_gap_pct']:+.3f}%  "
                f"거래대금={row['trading_value_bill']:>6,.0f}억"
            )
            lines.append(
                f"      이탈일={sig_date_str}  "
                f"거래량={row['vol_ratio']:.2f}x  "
                f"MA20={row['ma20_gap_pct']:+.1f}%  "
                f"5D수익={row['return_5d']:+.1f}%"
            )

            if row["entry"] > 0:
                risk_pct = row["risk"] / row["entry"] * 100 if row["entry"] > 0 else 0
                lines.append(
                    f"      진입={row['entry']:>10,.0f}  "
                    f"SL={row['sl']:>10,.0f}  "
                    f"TP={row['tp']:>10,.0f}  "
                    f"리스크={risk_pct:.2f}%"
                )
            lines.append("")

    # NAV 저평가 TOP 5 (시그널 무관)
    lines.append(f"\n  [NAV 저평가 TOP 5] (시그널 무관)")
    lines.append(f"  {'─'*50}")
    nav_sorted = df.nsmallest(5, "nav_gap_pct")
    for _, row in nav_sorted.iterrows():
        sig = "O" if row["has_signal"] else "-"
        lines.append(
            f"  {row['name'][:20]:<20s} [{row['ticker']}]  "
            f"괴리={row['nav_gap_pct']:+.3f}%  "
            f"거래대금={row['trading_value_bill']:>6,.0f}억  "
            f"시그널={sig}"
        )

    lines.append(f"\n{'='*55}")
    return "\n".join(lines)


def send_telegram(text: str):
    """텔레그램으로 결과 전송"""
    try:
        from dotenv import load_dotenv
        import os
        load_dotenv(ROOT.parent / ".env")

        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            logger.warning("텔레그램 설정 없음")
            return

        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        # 4096자 제한 → 분할 전송
        chunks = []
        current = ""
        for line in text.split("\n"):
            if len(current) + len(line) + 1 > 4000:
                chunks.append(current)
                current = line
            else:
                current += "\n" + line if current else line
        if current:
            chunks.append(current)

        for chunk in chunks:
            requests.post(url, json={
                "chat_id": chat_id,
                "text": f"```\n{chunk}\n```",
                "parse_mode": "Markdown",
            }, timeout=10)

        logger.info(f"텔레그램 전송 완료 ({len(chunks)}건)")

    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description="ETF Universe Scanner")
    parser.add_argument("--date", type=str, default=None,
                        help="기준일 YYYYMMDD (기본: 최근 거래일)")
    parser.add_argument("--min-vol", type=float, default=100,
                        help="최소 거래대금 (억원, 기본=100)")
    parser.add_argument("--top", type=int, default=20,
                        help="상위 N개 출력 (기본=20)")
    parser.add_argument("--telegram", action="store_true",
                        help="텔레그램으로 결과 전송")
    parser.add_argument("--save", action="store_true",
                        help="CSV로 결과 저장")
    args = parser.parse_args()

    df = run_scan(
        end_date=args.date,
        min_volume_bill=args.min_vol,
        top_n=args.top,
    )

    if df.empty:
        print("스캔 결과 없음")
        return

    report = format_report(df, top_n=args.top)
    print(report)

    if args.save:
        save_dir = ROOT / "results"
        save_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        save_path = save_dir / f"etf_scan_{date_str}.csv"
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"\n  CSV 저장: {save_path}")

    if args.telegram:
        send_telegram(report)


if __name__ == "__main__":
    main()
