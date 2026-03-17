# -*- coding: utf-8 -*-
"""
Daily Market Learner — Body Hunter 일간 학습 엔진
=================================================
매일 장마감 후 (15:40~) 실행하여:
1. 아침 추천 vs 실제 수익률 검증
2. 놓친 급등주 역추적 분석
3. 시그널별 적중률 누적 DB
4. 텔레그램 학습 리포트 전송

실행: python data/daily_learner.py
     python data/daily_learner.py --quick   # 추천 검증만
"""

import os
import sys
import json
import csv
import logging
import requests
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("BH.DailyLearner")

# ── 텔레그램 ──
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")

STORE_DIR = BASE_DIR / "data_store"
LEARNING_DIR = STORE_DIR / "learning"
LEARNING_DIR.mkdir(parents=True, exist_ok=True)


def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT:
        print(text)
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT, "text": text}, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


# ═══════════════════════════════════════════
#  1. 추천 적중률 검증
# ═══════════════════════════════════════════
def verify_recommendations(today: str) -> dict:
    """아침 추천종목의 당일 수익률 검증

    Returns:
        {
            total: int, hits: int, hit_rate: float,
            details: [{code, name, entry, close, pnl, hit, sources}],
            by_source: {source: {total, hits, rate}}
        }
    """
    rec_path = STORE_DIR / "recommendation.json"
    if not rec_path.exists():
        logger.warning("recommendation.json 없음 — 검증 스킵")
        return {"total": 0, "hits": 0, "hit_rate": 0, "details": [], "by_source": {}}

    rec = json.loads(rec_path.read_text("utf-8"))
    rec_date = rec.get("timestamp", "")[:10]

    # 오늘 추천이 아니면 스킵
    if rec_date != today:
        logger.warning(f"추천 날짜({rec_date}) ≠ 오늘({today}) — 검증 스킵")
        return {"total": 0, "hits": 0, "hit_rate": 0, "details": [], "by_source": {}}

    stocks = rec.get("stocks", [])
    # 추천 TOP 8만 검증 (war_relay 제외)
    top_stocks = [s for s in stocks if not s.get("code", "").startswith("war_")][:8]

    from bot.kis_trader import KISTrader
    import time
    trader = KISTrader()

    details = []
    by_source = defaultdict(lambda: {"total": 0, "hits": 0})

    for s in top_stocks:
        code = s["code"]
        name = s["name"]
        entry = s.get("entry", s.get("close", 0))
        sources = s.get("sources", [])

        # 당일 종가 조회
        resp = trader.fetch_price(code)
        time.sleep(0.15)

        if not resp.get("success"):
            continue

        close = resp["current_price"]
        if entry <= 0 or close <= 0:
            continue

        pnl = round((close - entry) / entry * 100, 2)
        hit = pnl > 0

        details.append({
            "code": code,
            "name": name,
            "entry": entry,
            "close": close,
            "pnl": pnl,
            "hit": hit,
            "total_score": s.get("total_score", 0),
            "sources": sources,
        })

        # 소스별 적중률
        for src in sources:
            src_key = src.split(":")[0] if ":" in src else src
            by_source[src_key]["total"] += 1
            if hit:
                by_source[src_key]["hits"] += 1

    total = len(details)
    hits = sum(1 for d in details if d["hit"])
    hit_rate = round(hits / total * 100, 1) if total > 0 else 0

    # 소스별 적중률 계산
    for k, v in by_source.items():
        v["rate"] = round(v["hits"] / v["total"] * 100, 1) if v["total"] > 0 else 0

    logger.info(f"추천 검증: {hits}/{total} = {hit_rate}%")
    return {
        "total": total,
        "hits": hits,
        "hit_rate": hit_rate,
        "details": details,
        "by_source": dict(by_source),
    }


# ═══════════════════════════════════════════
#  2. 놓친 급등주 역추적
# ═══════════════════════════════════════════
def find_missed_gainers(today: str, threshold: float = 3.0) -> list:
    """당일 +3% 이상 급등했는데 추천에 없던 종목 찾기

    Returns: [{code, name, change_rate, volume, reason_missed}]
    """
    from data.kis_collector import UNIVERSE

    # 오늘 추천 종목 코드 set
    rec_path = STORE_DIR / "recommendation.json"
    rec_codes = set()
    if rec_path.exists():
        rec = json.loads(rec_path.read_text("utf-8"))
        rec_codes = {s["code"] for s in rec.get("stocks", [])}

    # daily CSV에서 오늘 등락률 확인
    missed = []
    daily_dir = STORE_DIR / "daily"
    if not daily_dir.exists():
        logger.warning("daily 디렉토리 없음")
        return missed

    checked = 0
    for csv_file in daily_dir.glob("*.csv"):
        code = csv_file.stem
        if code not in UNIVERSE:
            continue

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    continue
                last = rows[-1]
                last_date = last.get("날짜", "")
                if last_date != today:
                    continue

                chg = float(last.get("등락률", 0))
                vol = int(last.get("거래량", 0))
                close = int(float(last.get("종가", 0)))
                checked += 1

                if chg >= threshold and code not in rec_codes:
                    name = UNIVERSE.get(code, [code])[0]
                    missed.append({
                        "code": code,
                        "name": name,
                        "change_rate": round(chg, 2),
                        "close": close,
                        "volume": vol,
                    })
        except Exception:
            continue

    # 등락률 내림차순
    missed.sort(key=lambda x: x["change_rate"], reverse=True)
    logger.info(f"유니버스 {checked}종목 중 +{threshold}% 급등: {len(missed)}개 (추천 누락)")
    return missed[:30]  # TOP 30까지


# ═══════════════════════════════════════════
#  3. 시그널 적중률 누적 DB
# ═══════════════════════════════════════════
SIGNAL_LOG_PATH = LEARNING_DIR / "signal_log.json"
ACCURACY_CSV_PATH = LEARNING_DIR / "accuracy_history.csv"


def load_signal_log() -> list:
    if SIGNAL_LOG_PATH.exists():
        return json.loads(SIGNAL_LOG_PATH.read_text("utf-8"))
    return []


def save_signal_log(log: list):
    SIGNAL_LOG_PATH.write_text(
        json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def append_daily_record(today: str, verify_result: dict, missed: list):
    """일간 학습 기록 추가"""
    log = load_signal_log()

    # 중복 방지
    if any(r["date"] == today for r in log):
        log = [r for r in log if r["date"] != today]

    record = {
        "date": today,
        "total_recommended": verify_result["total"],
        "hits": verify_result["hits"],
        "hit_rate": verify_result["hit_rate"],
        "details": verify_result["details"],
        "by_source": verify_result["by_source"],
        "missed_gainers_count": len(missed),
        "top_missed": missed[:10],
    }

    log.append(record)

    # rolling 30일만 유지
    if len(log) > 30:
        log = log[-30:]

    save_signal_log(log)
    logger.info(f"학습 DB 저장: {today} (총 {len(log)}일)")

    # accuracy CSV 갱신
    _update_accuracy_csv(log)

    return record


def _update_accuracy_csv(log: list):
    """날짜별 적중률 CSV 갱신"""
    with open(ACCURACY_CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["날짜", "추천수", "적중수", "적중률", "놓친급등주수"])
        for r in log:
            writer.writerow([
                r["date"],
                r["total_recommended"],
                r["hits"],
                r["hit_rate"],
                r["missed_gainers_count"],
            ])


# ═══════════════════════════════════════════
#  4. Rolling 적중률 계산
# ═══════════════════════════════════════════
def calc_rolling_accuracy(days: int = 20) -> dict:
    """최근 N일 rolling 적중률

    Returns:
        {
            period: int, total: int, hits: int, rate: float,
            by_source: {src: {total, hits, rate}},
            trend: "improving" | "declining" | "stable"
        }
    """
    log = load_signal_log()
    recent = log[-days:] if len(log) >= days else log

    if not recent:
        return {"period": 0, "total": 0, "hits": 0, "rate": 0, "by_source": {}, "trend": "stable"}

    total = sum(r["total_recommended"] for r in recent)
    hits = sum(r["hits"] for r in recent)
    rate = round(hits / total * 100, 1) if total > 0 else 0

    # 소스별 누적
    by_source = defaultdict(lambda: {"total": 0, "hits": 0})
    for r in recent:
        for src, stats in r.get("by_source", {}).items():
            by_source[src]["total"] += stats["total"]
            by_source[src]["hits"] += stats["hits"]
    for v in by_source.values():
        v["rate"] = round(v["hits"] / v["total"] * 100, 1) if v["total"] > 0 else 0

    # 트렌드 판단 (최근 5일 vs 이전 5일)
    trend = "stable"
    if len(recent) >= 10:
        first_half = recent[:len(recent)//2]
        second_half = recent[len(recent)//2:]
        r1 = sum(r["hits"] for r in first_half) / max(1, sum(r["total_recommended"] for r in first_half))
        r2 = sum(r["hits"] for r in second_half) / max(1, sum(r["total_recommended"] for r in second_half))
        if r2 - r1 > 0.05:
            trend = "improving"
        elif r1 - r2 > 0.05:
            trend = "declining"

    return {
        "period": len(recent),
        "total": total,
        "hits": hits,
        "rate": rate,
        "by_source": dict(by_source),
        "trend": trend,
    }


# ═══════════════════════════════════════════
#  5. 텔레그램 학습 리포트
# ═══════════════════════════════════════════
def format_learning_report(
    today: str,
    verify: dict,
    missed: list,
    rolling: dict,
) -> str:
    """텔레그램 학습 리포트 포맷"""
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📚 Body Hunter 일간 학습",
        f"📅 {today}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # ── 추천 적중률 ──
    if verify["total"] > 0:
        emoji = "🎯" if verify["hit_rate"] >= 60 else "📊"
        lines.append(f"\n{emoji} 추천 적중률: {verify['hits']}/{verify['total']} ({verify['hit_rate']}%)")
        lines.append("────────────────────────")

        # 종목별 결과
        for d in sorted(verify["details"], key=lambda x: x["pnl"], reverse=True):
            icon = "✅" if d["hit"] else "❌"
            lines.append(f"  {icon} {d['name']}({d['code']}) {d['pnl']:+.1f}%")

        # 소스별 적중률
        if verify["by_source"]:
            lines.append("\n  [소스별 적중률]")
            for src, s in sorted(verify["by_source"].items(), key=lambda x: x[1]["rate"], reverse=True):
                lines.append(f"    {src}: {s['hits']}/{s['total']} ({s['rate']}%)")
    else:
        lines.append("\n📊 오늘 추천 데이터 없음")

    # ── 놓친 급등주 ──
    if missed:
        lines.append(f"\n🔍 놓친 급등주 TOP 10 (+3%↑)")
        lines.append("────────────────────────")
        for m in missed[:10]:
            lines.append(f"  ⚡ {m['name']}({m['code']}) +{m['change_rate']:.1f}%")
        if len(missed) > 10:
            lines.append(f"  ... 외 {len(missed)-10}개")

    # ── Rolling 적중률 ──
    if rolling["period"] > 0:
        trend_icon = {"improving": "📈", "declining": "📉", "stable": "➡️"}
        lines.append(f"\n{trend_icon.get(rolling['trend'], '➡️')} 최근 {rolling['period']}일 누적: {rolling['rate']}% ({rolling['trend']})")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("Body Hunter | Daily Learner")

    return "\n".join(lines)


# ═══════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════
def run(quick: bool = False):
    """일간 학습 실행

    Args:
        quick: True면 추천 검증만 (놓친 급등주 스킵)
    """
    today = date.today().strftime("%Y-%m-%d")
    logger.info(f"=== Daily Learner 시작 ({today}) ===")

    # 1. 추천 적중률 검증
    logger.info("[Phase 1] 추천 적중률 검증...")
    verify = verify_recommendations(today)

    # 2. 놓친 급등주
    missed = []
    if not quick:
        logger.info("[Phase 2] 놓친 급등주 역추적...")
        missed = find_missed_gainers(today)

    # 3. DB 저장
    logger.info("[Phase 3] 학습 DB 저장...")
    append_daily_record(today, verify, missed)

    # 4. Rolling 적중률
    rolling = calc_rolling_accuracy(20)

    # 5. 텔레그램 리포트
    logger.info("[Phase 5] 텔레그램 리포트 전송...")
    report = format_learning_report(today, verify, missed, rolling)
    tg_send(report)

    logger.info("=== Daily Learner 완료 ===")
    return {
        "date": today,
        "verify": verify,
        "missed": missed,
        "rolling": rolling,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Body Hunter 일간 학습 엔진")
    parser.add_argument("--quick", action="store_true", help="추천 검증만 (급등주 분석 스킵)")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (텔레그램 전송 안함)")
    args = parser.parse_args()

    if args.test:
        TG_TOKEN = None  # 텔레그램 비활성화
        print("[TEST MODE] 텔레그램 전송 OFF")

    run(quick=args.quick)
