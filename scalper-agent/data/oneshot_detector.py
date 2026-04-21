# -*- coding: utf-8 -*-
"""
원샷 쌍매수 잠복 감지기 (One-Shot Dual-Buy Stealth Detector)
=============================================================
최근 N일 내 외인+기관 동시 대량매수(원샷)가 터졌지만
주가가 아직 크게 움직이지 ��은 "잠복" 종목 포착.

패턴: 쌍매수 300억+ 폭탄 → 며칠 눌림 → 급등
실전: 대주전자재료 4/15 +419억 쌍매수 → 4/20 -4.4% 잠복 → 4/21 +14%

데이터: data_store/investor_daily.db (퀀트봇 심볼릭링크)
      + data_store/flow/{code}_investor.csv (종가 참조)
출력:   data_store/oneshot_stealth.json
"""

import csv
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.OneshotDetector")

DATA_DIR = Path(__file__).parent.parent / "data_store"
DB_PATH = DATA_DIR / "investor_daily.db"
FLOW_DIR = DATA_DIR / "flow"
OUTPUT_PATH = DATA_DIR / "oneshot_stealth.json"

# ── 설정 ──
LOOKBACK_DAYS = 7          # 최근 N거래일 내 쌍매수 탐색
MIN_DUAL_BUY = 200         # 최소 쌍매수 합산 (억원)
STEALTH_RANGE = (-7, 7)    # 잠복 판정 범위 (%)
GONE_THRESHOLD = 7         # 이 이상이면 "이미 출발"


def scan_oneshot_stealth(
    lookback: int = LOOKBACK_DAYS,
    min_dual_buy: int = MIN_DUAL_BUY,
    top_n: int = 30,
) -> dict:
    """원샷 쌍매수 잠복 스캔.

    Returns:
        {
            "timestamp": "...",
            "lookback_days": 7,
            "min_dual_buy": 200,
            "stealth": [...],    # 잠복 (쌍매수 후 변화 ±7%)
            "gone": [...],       # 이미 출발 (+7%+)
            "failed": [...],     # ���패 (-7%~)
            "summary": {
                "total_signals": int,
                "stealth_count": int,
                "gone_count": int,
                "failed_count": int,
            }
        }
    """
    if not DB_PATH.exists():
        logger.error(f"investor_daily.db 없음: {DB_PATH}")
        return {"error": "investor_daily.db not found"}

    conn = sqlite3.connect(str(DB_PATH))

    # 1) 최근 거래일 목록 조회
    cur = conn.execute(
        "SELECT DISTINCT date FROM investor_daily "
        "ORDER BY date DESC LIMIT ?",
        (lookback + 1,)  # +1: 최신일은 제외 (당일은 아직 미완)
    )
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        conn.close()
        return {"error": "insufficient dates"}

    latest_date = dates[0]         # 가��� 최근 거래일 (종가 기준)
    latest_date_fmt = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"
    search_dates = dates[1:]       # 쌍매수 탐색 범위 (당일 제외)
    start_date = search_dates[-1]  # 가장 오래된 탐색일

    logger.info(
        f"[원���] 기준일={latest_date}, 탐색={start_date}~{search_dates[0]} "
        f"({len(search_dates)}일), 최소={min_dual_buy}억"
    )

    # 2) 쌍매수 시그널 조회 (외인+기관 동시 순매수, 합산 min_dual_buy억+)
    cur = conn.execute("""
        SELECT a.date, a.ticker, a.name,
               a.net_val as frgn_val,
               b.net_val as inst_val
        FROM investor_daily a
        JOIN investor_daily b ON a.ticker=b.ticker AND a.date=b.date
        WHERE a.investor='외국인' AND b.investor='기관합계'
          AND a.date >= ? AND a.date <= ?
          AND a.net_val > 0 AND b.net_val > 0
          AND (a.net_val + b.net_val) >= ?
        ORDER BY (a.net_val + b.net_val) DESC
    """, (start_date, search_dates[0], min_dual_buy * 1e8))

    signals = cur.fetchall()
    conn.close()

    # 3) 종목별 최대 쌍매수일만 추출
    best = {}
    for date, ticker, name, frgn_val, inst_val in signals:
        total = frgn_val + inst_val
        if ticker not in best or total > best[ticker]["total_val"]:
            best[ticker] = {
                "date": date,
                "name": name,
                "frgn": round(frgn_val / 1e8, 1),
                "inst": round(inst_val / 1e8, 1),
                "total": round(total / 1e8, 1),
                "total_val": total,
            }

    # 4) 쌍매수일 종가 vs 기준일 종가 비교
    stealth = []
    gone = []
    failed = []

    for ticker, sig in best.items():
        csv_path = FLOW_DIR / f"{ticker}_investor.csv"
        if not csv_path.exists():
            continue

        try:
            with open(csv_path, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            continue

        # 날짜 포맷 변환 (DB: 20260415 → CSV: 2026-04-15)
        sig_date = sig["date"]
        sig_date_fmt = f"{sig_date[:4]}-{sig_date[4:6]}-{sig_date[6:]}"

        signal_close = None
        latest_close = None

        for r in rows:
            rd = r.get("date", "")
            if rd == sig_date_fmt:
                try:
                    signal_close = int(r.get("종가", 0))
                except (ValueError, TypeError):
                    pass
            if rd == latest_date_fmt:
                try:
                    latest_close = int(r.get("종가", 0))
                except (ValueError, TypeError):
                    pass

        if not signal_close or not latest_close or signal_close <= 0:
            continue

        chg_pct = (latest_close - signal_close) / signal_close * 100

        item = {
            "ticker": ticker,
            "name": sig["name"],
            "signal_date": sig_date_fmt,
            "frgn_buy": sig["frgn"],
            "inst_buy": sig["inst"],
            "dual_total": sig["total"],
            "signal_close": signal_close,
            "latest_close": latest_close,
            "latest_date": latest_date_fmt,
            "chg_pct": round(chg_pct, 1),
        }

        if STEALTH_RANGE[0] <= chg_pct <= STEALTH_RANGE[1]:
            stealth.append(item)
        elif chg_pct > GONE_THRESHOLD:
            gone.append(item)
        else:
            failed.append(item)

    # 5) 정렬: 잠복은 쌍매수 금액 내림차순
    stealth.sort(key=lambda x: x["dual_total"], reverse=True)
    gone.sort(key=lambda x: x["chg_pct"], reverse=True)
    failed.sort(key=lambda x: x["chg_pct"])

    # top_n 제한
    stealth = stealth[:top_n]

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_date": latest_date_fmt,
        "lookback_days": lookback,
        "min_dual_buy": min_dual_buy,
        "stealth": stealth,
        "gone": gone,
        "failed": failed,
        "summary": {
            "total_signals": len(best),
            "stealth_count": len(stealth),
            "gone_count": len(gone),
            "failed_count": len(failed),
        },
    }

    # 6) JSON 저장
    try:
        OUTPUT_PATH.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"[원샷] 저장 완료: 잠복 {len(stealth)}건")
    except Exception as e:
        logger.error(f"[원샷] JSON 저장 실패: {e}")

    return result


# ── 텔���그램 포맷 ──

def format_oneshot_alert(scan: dict) -> str:
    """텔레그램 알림 포맷"""
    stealth = scan.get("stealth", [])
    summary = scan.get("summary", {})

    lines = [
        "💣 원샷 쌍매수 잠복 리포트",
        f"탐색: 최근 {scan.get('lookback_days', 7)}일 | "
        f"최소 {scan.get('min_dual_buy', 200)}억 | "
        f"기준일: {scan.get('latest_date', '')}",
        f"잠복 {summary.get('stealth_count', 0)} / "
        f"출발 {summary.get('gone_count', 0)} / "
        f"실패 {summary.get('failed_count', 0)}",
        "",
    ]

    if stealth:
        lines.append("▸ 잠복 종목 (��매수 후 아직 ±7%)")
        for i, s in enumerate(stealth[:15], 1):
            lines.append(
                f"  {i}. {s['name']} ({s['ticker']})"
            )
            lines.append(
                f"     {s['signal_date']} 외인+{s['frgn_buy']:.0f} "
                f"기관+{s['inst_buy']:.0f} = +{s['dual_total']:.0f}억"
            )
            lines.append(
                f"     {s['signal_close']:,} → {s['latest_close']:,} "
                f"({s['chg_pct']:+.1f}%)"
            )
    else:
        lines.append("  (잠복 종목 없음)")

    lines.append("")
    lines.append(f"🕐 {scan.get('timestamp', '')}")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = scan_oneshot_stealth()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
    print()
    print(format_oneshot_alert(result))
