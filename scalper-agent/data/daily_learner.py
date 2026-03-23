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
from data.trading_calendar import is_trading_day, next_trading_day, last_trading_day
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

        if not resp or not resp.get("success"):
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
                # CSV 인덱스 컬럼명: pandas to_csv → 빈 문자열("") 키
                last_date = last.get("", "") or last.get("날짜", "") or last.get("date", "")
                if last_date != today:
                    continue

                # CSV 컬럼: 한글(날짜/종가/거래량/등락률) 또는 영문(date/close/volume)
                close_raw = last.get("종가") or last.get("close", 0)
                vol_raw = last.get("거래량") or last.get("volume", 0)
                chg_raw = last.get("등락률")
                close = int(float(close_raw)) if close_raw else 0
                vol = int(float(vol_raw)) if vol_raw else 0
                # 등락률 없으면 전일 대비 계산
                if chg_raw is not None and chg_raw != "":
                    chg = float(chg_raw)
                elif len(rows) >= 2:
                    prev_close = float(rows[-2].get("종가") or rows[-2].get("close", 0) or 0)
                    chg = ((close / prev_close) - 1) * 100 if prev_close > 0 else 0
                else:
                    chg = 0
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
    tmp = SIGNAL_LOG_PATH.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp.replace(SIGNAL_LOG_PATH)


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
#  4.5 NXT Nightwatch 성과 검증
# ═══════════════════════════════════════════
NXT_LOG_PATH = LEARNING_DIR / "nxt_signal_log.json"
NXT_HISTORY_PATH = STORE_DIR / "learning" / "nxt_history.json"


def _load_nxt_log() -> list:
    if NXT_LOG_PATH.exists():
        try:
            return json.loads(NXT_LOG_PATH.read_text("utf-8"))
        except (json.JSONDecodeError, Exception):
            pass
    return []


def _save_nxt_log(log: list):
    tmp = NXT_LOG_PATH.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp.replace(NXT_LOG_PATH)


def verify_nxt_signals(today: str) -> Optional[dict]:
    """어제 NXT 시그널의 오늘 수익률 검증

    어제 nightwatch가 추천한 tier1 종목들의 당일 등락률을 측정.
    Returns: {signal_date, signal, score, targets: [{code, name, return_pct}],
             avg_return, hit_count, total, hit_rate} or None
    """
    # 히스토리에서 어제 리포트 찾기
    if not NXT_HISTORY_PATH.exists():
        logger.info("[NXT] 히스토리 없음 — 검증 스킵")
        return None

    try:
        history = json.loads(NXT_HISTORY_PATH.read_text("utf-8"))
    except (json.JSONDecodeError, Exception):
        return None

    if not history:
        return None

    # 어제 날짜 계산
    from datetime import timedelta
    today_dt = datetime.strptime(today, "%Y-%m-%d").date()
    # 주말/공휴일 고려: 직전 거래일 리포트
    if not is_trading_day(today_dt):
        return None
    yesterday = last_trading_day(today_dt).strftime("%Y-%m-%d")

    # 어제 리포트 찾기
    yesterday_report = None
    for h in history:
        if h.get("date") == yesterday:
            yesterday_report = h
            break

    if not yesterday_report:
        logger.info(f"[NXT] {yesterday} 리포트 없음 — 검증 스킵")
        return None

    # tier1 종목의 오늘 수익률 확인
    targets = yesterday_report.get("nxt_targets", [])
    tier1 = [t for t in targets if t.get("tier") == 1]
    if not tier1:
        logger.info(f"[NXT] {yesterday} tier1 종목 없음")
        return None

    # KIS API로 당일 수익률 조회
    sys.path.insert(0, str(BASE_DIR))
    from bot.kis_trader import KISTrader
    import time as _time
    trader = KISTrader()

    results = []
    for t in tier1[:5]:  # 최대 5종목
        code = t["code"]
        name = t["name"]
        try:
            resp = trader.fetch_price(code)
            _time.sleep(0.15)
            if not resp or not resp.get("success"):
                continue
            change_pct = resp.get("change_rate", 0)
            results.append({
                "code": code,
                "name": name,
                "sector": t.get("sector", ""),
                "return_pct": round(change_pct, 2),
                "hit": change_pct > 0,
            })
        except Exception:
            continue

    if not results:
        logger.info("[NXT] 수익률 조회 실패")
        return None

    total = len(results)
    hits = sum(1 for r in results if r["hit"])
    avg_ret = round(sum(r["return_pct"] for r in results) / total, 2)
    hit_rate = round(hits / total * 100, 1)

    # NXT-07: korea_strength + 정규추천 수익률 비교
    korea_str = yesterday_report.get("korea_strength", 0)

    # 정규 추천 당일 평균 수익률 (signal_log에서)
    rec_avg = 0.0
    try:
        sl_path = STORE_DIR / "learning" / "signal_log.json"
        if sl_path.exists():
            sl = json.loads(sl_path.read_text("utf-8"))
            for entry in reversed(sl):
                if entry.get("verify_date") == today:
                    rec_avg = entry.get("avg_return", 0)
                    break
    except Exception:
        pass

    # 더 나은 행동 판정
    signal_text = yesterday_report.get("signal_text", "")
    is_inverse = "인버스" in signal_text or "포지션 점검" in signal_text
    if is_inverse and rec_avg > avg_ret:
        better_action = "정규추천"
    elif not is_inverse and avg_ret > rec_avg:
        better_action = "NXT"
    else:
        better_action = "NXT" if avg_ret > 0 else "관망"

    record = {
        "signal_date": yesterday,
        "verify_date": today,
        "signal": yesterday_report.get("signal", ""),
        "signal_text": signal_text,
        "total_score": yesterday_report.get("total_score", 0),
        "korea_strength": korea_str,
        "recommended_sectors": yesterday_report.get("recommended_sectors", []),
        "targets": results,
        "total": total,
        "hit_count": hits,
        "hit_rate": hit_rate,
        "avg_return": avg_ret,
        "rec_avg_return": rec_avg,
        "better_action": better_action,
    }

    # NXT 로그에 누적
    log = _load_nxt_log()
    log = [r for r in log if r.get("signal_date") != yesterday]  # 중복 방지
    log.append(record)
    if len(log) > 90:
        log = log[-90:]
    _save_nxt_log(log)

    logger.info(f"[NXT] {yesterday} 검증: {hits}/{total} ({hit_rate}%) 평균 {avg_ret:+.2f}%")
    return record


def calc_nxt_rolling(days: int = 30) -> dict:
    """NXT 시그널 Rolling 적중률

    Returns: {period, total_days, total_stocks, hits, hit_rate,
              avg_return, by_signal: {signal: {count, avg_return, hit_rate}},
              best, worst}
    """
    log = _load_nxt_log()
    if not log:
        return {"period": 0, "total_days": 0, "total_stocks": 0,
                "hits": 0, "hit_rate": 0, "avg_return": 0,
                "by_signal": {}, "best": None, "worst": None}

    recent = log[-days:]

    all_targets = []
    by_signal = defaultdict(lambda: {"count": 0, "return_sum": 0, "hits": 0})

    for rec in recent:
        sig = rec.get("signal_text", "")
        for t in rec.get("targets", []):
            all_targets.append(t)
            by_signal[sig]["count"] += 1
            by_signal[sig]["return_sum"] += t["return_pct"]
            if t["hit"]:
                by_signal[sig]["hits"] += 1

    total_stocks = len(all_targets)
    hits = sum(1 for t in all_targets if t["hit"])
    avg_ret = round(sum(t["return_pct"] for t in all_targets) / total_stocks, 2) if total_stocks else 0
    hit_rate = round(hits / total_stocks * 100, 1) if total_stocks else 0

    # 시그널별 통계
    by_signal_out = {}
    for sig, stats in by_signal.items():
        cnt = stats["count"]
        by_signal_out[sig] = {
            "count": cnt,
            "avg_return": round(stats["return_sum"] / cnt, 2) if cnt else 0,
            "hit_rate": round(stats["hits"] / cnt * 100, 1) if cnt else 0,
        }

    # 최고/최저 종목
    best = max(all_targets, key=lambda t: t["return_pct"]) if all_targets else None
    worst = min(all_targets, key=lambda t: t["return_pct"]) if all_targets else None

    # NXT-07: 인버스 적중률 + better_action 통계
    inverse_days = [r for r in recent if "인버스" in r.get("signal_text", "") or "점검" in r.get("signal_text", "")]
    inverse_correct = sum(1 for r in inverse_days if r.get("avg_return", 0) > 0)
    inverse_hit_rate = round(inverse_correct / len(inverse_days) * 100, 1) if inverse_days else 0

    better_nxt = sum(1 for r in recent if r.get("better_action") == "NXT")
    better_rec = sum(1 for r in recent if r.get("better_action") == "정규추천")

    return {
        "period": len(recent),
        "total_days": len(recent),
        "total_stocks": total_stocks,
        "hits": hits,
        "hit_rate": hit_rate,
        "avg_return": avg_ret,
        "by_signal": by_signal_out,
        "best": best,
        "worst": worst,
        "inverse_days": len(inverse_days),
        "inverse_hit_rate": inverse_hit_rate,
        "better_nxt": better_nxt,
        "better_rec": better_rec,
    }


# ═══════════════════════════════════════════
#  5. 텔레그램 학습 리포트
# ═══════════════════════════════════════════
def format_learning_report(
    today: str,
    verify: dict,
    missed: list,
    rolling: dict,
    insights: Optional[dict] = None,
    nxt_verify: Optional[dict] = None,
    nxt_rolling: Optional[dict] = None,
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

    # ── 브레인 업데이트 (인사이트) ──
    if insights and insights.get("source_weights"):
        lines.append(f"\n🧠 브레인 업데이트 (D+{insights.get('score_adj_applied', 0)})")
        lines.append("────────────────────────")

        # 소스 가중치 TOP
        sw = insights["source_weights"]
        boosted = [(s, w) for s, w in sw.items() if w["weight"] > 1.0]
        nerfed = [(s, w) for s, w in sw.items() if w["weight"] < 1.0]

        if boosted:
            boosted.sort(key=lambda x: x[1]["weight"], reverse=True)
            lines.append("  신뢰 UP:")
            for src, w in boosted[:3]:
                lines.append(f"    📈 {src} x{w['weight']:.2f} ({w['hit_rate']}%적중)")

        if nerfed:
            nerfed.sort(key=lambda x: x[1]["weight"])
            lines.append("  신뢰 DOWN:")
            for src, w in nerfed[:3]:
                lines.append(f"    📉 {src} x{w['weight']:.2f} ({w['hit_rate']}%적중)")

        # 섹터 부스트
        sb = insights.get("sector_boost", {})
        if sb:
            lines.append("  누락 섹터 부스트:")
            for sector, b in sorted(sb.items(), key=lambda x: x[1]["boost"], reverse=True)[:3]:
                lines.append(f"    ⚡ {sector} +{b['boost']:.0f}점 ({b['miss_count']}회 누락)")

        # 패턴 인사이트 핵심만
        pi = insights.get("pattern_insights", [])
        notable = [p for p in pi if p.get("type") == "tv_pattern" and p.get("sample", 0) >= 3]
        if notable:
            lines.append("  TV 패턴 학습:")
            for p in notable[:2]:
                lines.append(f"    🔬 {p['pattern']}: {p['hit_rate']}% (n={p['sample']})")

    # ── NXT Nightwatch 성과 ──
    if nxt_verify and nxt_verify.get("total", 0) > 0:
        nv = nxt_verify
        sig_icon = nv.get("signal", "🟡")
        kr_str = nv.get("korea_strength", 0)
        kr_tag = f" | 한국장{kr_str:+.1f}" if kr_str else ""
        lines.append(f"\n{sig_icon} NXT 시그널 검증 ({nv['signal_date']})")
        lines.append("────────────────────────")
        lines.append(f"  신호: {nv.get('signal_text', '')} (점수 {nv.get('total_score', 0):+.1f}{kr_tag})")
        for t in nv.get("targets", []):
            icon = "✅" if t["hit"] else "❌"
            lines.append(f"  {icon} {t['name']}({t['code']}) {t['return_pct']:+.1f}%")
        lines.append(f"  적중: {nv['hit_count']}/{nv['total']} ({nv['hit_rate']}%) | 평균 {nv['avg_return']:+.2f}%")
        # NXT-07: NXT vs 정규추천 비교
        rec_avg = nv.get("rec_avg_return", 0)
        better = nv.get("better_action", "")
        if rec_avg:
            lines.append(f"  정규추천 평균: {rec_avg:+.1f}% | 더 나은 선택: {better}")

    if nxt_rolling and nxt_rolling.get("total_days", 0) > 0:
        nr = nxt_rolling
        lines.append(f"\n📡 NXT {nr['period']}일 누적: {nr['hit_rate']}% ({nr['hits']}/{nr['total_stocks']}) 평균 {nr['avg_return']:+.2f}%")
        if nr.get("by_signal"):
            for sig, stats in sorted(nr["by_signal"].items(), key=lambda x: x[1]["avg_return"], reverse=True):
                lines.append(f"    {sig}: {stats['hit_rate']}% (n={stats['count']}) 평균{stats['avg_return']:+.1f}%")
        # NXT-07: 인버스 적중률 + NXT vs 정규추천 비교
        inv_days = nr.get("inverse_days", 0)
        if inv_days > 0:
            lines.append(f"  인버스 추천: {inv_days}일, 적중률 {nr.get('inverse_hit_rate', 0)}%")
        better_nxt = nr.get("better_nxt", 0)
        better_rec = nr.get("better_rec", 0)
        if better_nxt + better_rec > 0:
            lines.append(f"  NXT승 {better_nxt}일 vs 정규추천승 {better_rec}일")
        if nr.get("best"):
            lines.append(f"  최고: {nr['best']['name']} {nr['best']['return_pct']:+.1f}%")
        if nr.get("worst"):
            lines.append(f"  최저: {nr['worst']['name']} {nr['worst']['return_pct']:+.1f}%")

    # ── 내일 이벤트 리스크 ──
    try:
        from data.event_calendar import get_event_risk_for_recommendation
        _today_dt = datetime.strptime(today, "%Y-%m-%d").date()
        _tmr = next_trading_day(_today_dt)
        tmr_risk = get_event_risk_for_recommendation(_tmr)
        if tmr_risk.get("events"):
            _ri = {"EXTREME": "💀", "HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(tmr_risk["risk_level"], "")
            ev_names = [e["name"] for e in tmr_risk["events"] if e.get("impact") == "HIGH"][:3]
            lines.append(f"\n📅 내일({_tmr}) {_ri}{tmr_risk['risk_level']} {' '.join(ev_names)}")
    except Exception:
        pass

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("Body Hunter | Daily Learner")

    return "\n".join(lines)


# ═══════════════════════════════════════════
#  6. 브레인 업데이트 (피드백 루프 핵심)
# ═══════════════════════════════════════════
INSIGHTS_PATH = LEARNING_DIR / "insights.json"


def load_insights() -> dict:
    """현재 인사이트 로드 (morning_recommendation.py에서도 호출)"""
    if INSIGHTS_PATH.exists():
        return json.loads(INSIGHTS_PATH.read_text("utf-8"))
    return {
        "updated": "",
        "source_weights": {},
        "sector_boost": {},
        "pattern_insights": [],
        "score_adj_applied": 0,
    }


def generate_insights() -> dict:
    """누적 학습 데이터에서 인사이트 추출 → insights.json 저장

    이 파일이 다음 날 morning_recommendation.py에 로드되어
    스코어링에 반영됩니다. (피드백 루프)
    """
    log = load_signal_log()
    if len(log) < 3:
        logger.info("학습 데이터 3일 미만 — 인사이트 생성 스킵 (데이터 축적 필요)")
        return load_insights()

    insights = load_insights()
    today = date.today().strftime("%Y-%m-%d")
    insights["updated"] = today

    # ─────────────────────────────────────
    # A. 소스별 가중치 자동조정
    # ─────────────────────────────────────
    # 최근 전체 로그에서 소스별 적중률 집계
    src_stats = defaultdict(lambda: {"total": 0, "hits": 0, "pnl_sum": 0.0})
    for rec in log:
        for src, stats in rec.get("by_source", {}).items():
            src_key = src.split(":")[0] if ":" in src else src
            src_stats[src_key]["total"] += stats["total"]
            src_stats[src_key]["hits"] += stats["hits"]
        # 종목별 PNL도 소스에 매핑
        for det in rec.get("details", []):
            for src in det.get("sources", []):
                src_key = src.split(":")[0] if ":" in src else src
                if src_key in src_stats:
                    src_stats[src_key]["pnl_sum"] += det.get("pnl", 0)

    source_weights = {}
    for src, st in src_stats.items():
        if st["total"] < 3:  # 샘플 3건 미만은 판단 불가
            continue
        rate = st["hits"] / st["total"]
        avg_pnl = st["pnl_sum"] / st["total"]

        # 가중치 공식: 적중률 60%+ → 부스트, 40%- → 페널티
        if rate >= 0.70:
            weight = 1.3   # 강한 부스트
            reason = f"적중률 {rate*100:.0f}% (평균PNL {avg_pnl:+.1f}%) — 강한 신뢰"
        elif rate >= 0.55:
            weight = 1.15  # 약한 부스트
            reason = f"적중률 {rate*100:.0f}% — 양호"
        elif rate >= 0.40:
            weight = 1.0   # 기본값
            reason = f"적중률 {rate*100:.0f}% — 보통"
        elif rate >= 0.25:
            weight = 0.85  # 약한 페널티
            reason = f"적중률 {rate*100:.0f}% — 부진"
        else:
            weight = 0.70  # 강한 페널티
            reason = f"적중률 {rate*100:.0f}% — 매우 부진"

        source_weights[src] = {
            "weight": weight,
            "hit_rate": round(rate * 100, 1),
            "avg_pnl": round(avg_pnl, 2),
            "sample": st["total"],
            "reason": reason,
        }

    insights["source_weights"] = source_weights
    logger.info(f"[Brain] 소스 가중치 업데이트: {len(source_weights)}개 소스")
    for src, w in sorted(source_weights.items(), key=lambda x: x[1]["weight"], reverse=True):
        logger.info(f"  {src}: x{w['weight']:.2f} ({w['reason']})")

    # ─────────────────────────────────────
    # B. 놓친 급등주 섹터 패턴 분석
    # ─────────────────────────────────────
    # 반복적으로 놓치는 섹터 = 스캐너 사각지대 → 다음 날 부스트
    sector_miss = defaultdict(lambda: {"count": 0, "avg_gain": 0, "gains": []})

    try:
        from data.kis_collector import UNIVERSE
        # 간단 섹터 매핑 (종목명 키워드 기반)
        SECTOR_KEYWORDS = {
            "반도체": ["하이닉스", "삼성전자", "한미반도체", "리노공업", "ISC", "원익", "테스"],
            "2차전지": ["에코프로", "포스코퓨처", "엘앤에프", "LG에너지", "삼성SDI"],
            "방산": ["한화에어로", "현대로템", "LIG넥스원", "한국항공", "풍산"],
            "건설": ["삼성E&A", "현대건설", "GS건설", "DL이앤씨", "대우건설"],
            "전력": ["한전", "HD현대일렉", "일진전기", "제룡전기", "LS일렉"],
            "바이오": ["삼바", "셀트리온", "알테오젠", "리가켐"],
            "금융": ["KB금융", "신한지주", "하나금융", "BNK"],
            "자동차": ["현대차", "기아", "현대모비스"],
            "조선": ["HD한국조선", "삼성중공업", "한화오션"],
            "AI/SW": ["네이버", "카카오", "더존비즈온"],
        }

        for rec in log:
            for m in rec.get("top_missed", []):
                name = m.get("name", "")
                gain = m.get("change_rate", 0)
                sector_found = "기타"
                for sector, keywords in SECTOR_KEYWORDS.items():
                    if any(kw in name for kw in keywords):
                        sector_found = sector
                        break
                sector_miss[sector_found]["count"] += 1
                sector_miss[sector_found]["gains"].append(gain)
    except ImportError:
        logger.warning("UNIVERSE 임포트 실패 — 섹터 분석 스킵")

    sector_boost = {}
    for sector, info in sector_miss.items():
        if info["count"] >= 3 and sector != "기타":  # 3회 이상 누락
            avg_gain = sum(info["gains"]) / len(info["gains"])
            # 누적 누락 횟수에 비례하여 부스트 (최대 +15점)
            boost = min(info["count"] * 2.5, 15)
            sector_boost[sector] = {
                "boost": round(boost, 1),
                "miss_count": info["count"],
                "avg_gain": round(avg_gain, 1),
                "reason": f"{info['count']}회 누락, 평균 +{avg_gain:.1f}% 상승",
            }

    insights["sector_boost"] = sector_boost
    if sector_boost:
        logger.info(f"[Brain] 누락 섹터 부스트: {len(sector_boost)}개 섹터")
        for s, b in sorted(sector_boost.items(), key=lambda x: x[1]["boost"], reverse=True):
            logger.info(f"  {s}: +{b['boost']:.1f}점 ({b['reason']})")

    # ─────────────────────────────────────
    # C. 패턴 인사이트 (고적중 + 저적중 패턴)
    # ─────────────────────────────────────
    pattern_insights = []

    # C-1. TV 패턴별 적중률
    tv_patterns = defaultdict(lambda: {"total": 0, "hits": 0, "pnl_sum": 0})
    for rec in log:
        for det in rec.get("details", []):
            for src in det.get("sources", []):
                if src.startswith("tv:"):
                    pat = src.replace("tv:", "")
                    tv_patterns[pat]["total"] += 1
                    if det.get("hit"):
                        tv_patterns[pat]["hits"] += 1
                    tv_patterns[pat]["pnl_sum"] += det.get("pnl", 0)

    for pat, st in tv_patterns.items():
        if st["total"] >= 2:
            rate = st["hits"] / st["total"] * 100
            avg = st["pnl_sum"] / st["total"]
            pattern_insights.append({
                "type": "tv_pattern",
                "pattern": pat,
                "hit_rate": round(rate, 1),
                "avg_pnl": round(avg, 2),
                "sample": st["total"],
                "note": f"TV {pat}: {rate:.0f}% 적중 (avg {avg:+.1f}%)",
            })

    # C-2. 점수대별 적중률 (high score 종목이 정말 더 잘 맞는가?)
    score_buckets = {"high(80+)": {"t": 0, "h": 0}, "mid(50-80)": {"t": 0, "h": 0}, "low(<50)": {"t": 0, "h": 0}}
    for rec in log:
        for det in rec.get("details", []):
            sc = det.get("total_score", 0)
            if sc >= 80:
                bucket = "high(80+)"
            elif sc >= 50:
                bucket = "mid(50-80)"
            else:
                bucket = "low(<50)"
            score_buckets[bucket]["t"] += 1
            if det.get("hit"):
                score_buckets[bucket]["h"] += 1

    for bucket, st in score_buckets.items():
        if st["t"] >= 3:
            rate = st["h"] / st["t"] * 100
            pattern_insights.append({
                "type": "score_band",
                "pattern": bucket,
                "hit_rate": round(rate, 1),
                "sample": st["t"],
                "note": f"점수 {bucket}: {rate:.0f}% 적중 (n={st['t']})",
            })

    # C-3. 놓친 급등주의 공통 특성 메모
    total_missed = sum(rec.get("missed_gainers_count", 0) for rec in log)
    total_recommended = sum(rec.get("total_recommended", 0) for rec in log)
    if total_recommended > 0:
        miss_ratio = total_missed / max(1, total_recommended)
        pattern_insights.append({
            "type": "coverage",
            "pattern": "miss_ratio",
            "value": round(miss_ratio, 1),
            "note": f"추천 {total_recommended}건 대비 놓친 급등주 {total_missed}건 (비율 {miss_ratio:.1f}x)",
        })

    insights["pattern_insights"] = pattern_insights
    insights["score_adj_applied"] = len(log)  # 몇 일치 데이터로 생성했는지

    # ── 저장 (atomic write) ──
    tmp_path = INSIGHTS_PATH.with_suffix(".tmp")
    try:
        tmp_path.write_text(
            json.dumps(insights, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp_path.replace(INSIGHTS_PATH)
    except Exception as e:
        logger.error(f"[Brain] insights.json 저장 실패: {e}")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    logger.info(f"[Brain] insights.json 저장 완료 ({len(pattern_insights)}개 패턴)")
    return insights


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

    # 4.5 NXT Nightwatch 성과 검증
    logger.info("[Phase 4.5] NXT 시그널 검증...")
    nxt_verify = None
    nxt_rolling = None
    try:
        nxt_verify = verify_nxt_signals(today)
        nxt_rolling = calc_nxt_rolling(30)
        if nxt_verify:
            logger.info(f"NXT 검증: {nxt_verify['hit_count']}/{nxt_verify['total']} ({nxt_verify['hit_rate']}%)")
    except Exception as e:
        logger.error(f"NXT 검증 실패: {e}")

    # 5. 브레인 업데이트 (피드백 루프 핵심!)
    logger.info("[Phase 5] 브레인 업데이트 — 인사이트 추출...")
    insights = generate_insights()

    # 5.5 Trade Learner — score_bucket_returns 업데이트
    try:
        from data.trade_learner import update_bucket_returns
        update_bucket_returns()
        logger.info("[Phase 5.5] Trade Learner — score_bucket 업데이트 완료")
    except Exception as e:
        logger.warning(f"Trade Learner 업데이트 실패 (무시): {e}")

    # 6. 텔레그램 리포트
    logger.info("[Phase 6] 텔레그램 리포트 전송...")
    report = format_learning_report(today, verify, missed, rolling, insights,
                                     nxt_verify=nxt_verify, nxt_rolling=nxt_rolling)
    tg_send(report)

    # FIX-08: BRAIN 자기 학습 — 성과 추적
    brain_perf = {}
    try:
        brain_perf = brain_performance_check()
    except Exception as e:
        logger.warning(f"[BRAIN Perf] 실패 (무시): {e}")

    # Phase 7: 시장 일지 기록 (market_journal 피드백 루프)
    try:
        from data.market_journal import record_daily as _journal_record
        journal_entry = _journal_record(today, verify)
        logger.info(f"[Phase 7] 시장 일지 기록 완료: {journal_entry.get('date', today)}")
    except Exception as e:
        logger.warning(f"[MarketJournal] 시장 일지 기록 실패 (무시): {e}")

    # Phase 8: 이벤트 캘린더 내일 리스크 (다음날 이벤트 미리 확인)
    try:
        from data.event_calendar import get_event_risk_for_recommendation
        tomorrow = next_trading_day(datetime.strptime(today, "%Y-%m-%d").date())
        tmr_risk = get_event_risk_for_recommendation(tomorrow)
        if tmr_risk.get("events"):
            logger.info(
                f"[Phase 8] 내일({tomorrow}) 이벤트: {tmr_risk['risk_level']} "
                f"({len(tmr_risk['events'])}건, confluence:{tmr_risk['confluence_score']:.0f})"
            )
    except Exception as e:
        logger.warning(f"[EventCalendar] 실패 (무시): {e}")

    logger.info("=== Daily Learner 완료 ===")
    return {
        "date": today,
        "verify": verify,
        "missed": missed,
        "rolling": rolling,
        "insights": insights,
        "nxt_verify": nxt_verify,
        "nxt_rolling": nxt_rolling,
        "brain_perf": brain_perf,
    }


# ═══════════════════════════════════════
#  FIX-08: BRAIN 자기 학습 — 성과 추적
# ═══════════════════════════════════════

def brain_performance_check() -> dict:
    """BRAIN 판단 vs 실제 시장 결과를 기록 → brain_performance.json (30일 롤링)

    적중 판정:
      - 공격/표준 + 코스피↑ → correct
      - 방어/관망 + 코스피↓ → correct
      - 공격 + 코스피 -2% 이상 → bad_call
      - 관망 + 코스피 +2% 이상 → missed_opportunity
    """
    PERF_PATH = STORE_DIR / "learning" / "brain_performance.json"

    # 현재 brain_report 로드
    brain_path = STORE_DIR / "brain_report.json"
    if not brain_path.exists():
        logger.info("[BRAIN Perf] brain_report.json 없음 — 스킵")
        return {}

    try:
        brain = json.loads(brain_path.read_text("utf-8"))
    except Exception:
        return {}

    brain_date = brain.get("date", "")
    pct = brain.get("position_size_pct", 70)
    verdict = brain.get("overall_verdict", "")

    # 오늘 코스피 등락률 (pykrx)
    try:
        from pykrx import stock
        today_str = date.today().strftime("%Y%m%d")
        yesterday = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        df = stock.get_index_ohlcv(yesterday, today_str, "1001")
        if df is not None and len(df) >= 2:
            kospi_today = df["종가"].iloc[-1]
            kospi_prev = df["종가"].iloc[-2]
            kospi_chg = (kospi_today / kospi_prev - 1) * 100
        else:
            kospi_chg = 0.0
    except Exception as e:
        logger.warning(f"[BRAIN Perf] 코스피 조회 실패: {e}")
        kospi_chg = 0.0

    # 추천 종목 평균 수익률 (signal_log에서)
    log = load_signal_log()
    rec_avg = 0.0
    if log:
        latest = log[-1]
        details = latest.get("details", [])
        if details:
            pnls = [d.get("pnl", 0) for d in details]
            rec_avg = sum(pnls) / len(pnls) if pnls else 0.0

    # 적중 판정
    label_map = {100: "공격", 70: "표준", 50: "방어", 30: "최소", 0: "관망"}
    label = label_map.get(pct, "표준")

    if pct >= 70 and kospi_chg > 0:
        correct = True
    elif pct <= 50 and kospi_chg < 0:
        correct = True
    else:
        correct = False

    bad_call = pct >= 70 and kospi_chg <= -2.0
    missed = pct <= 30 and kospi_chg >= 2.0

    entry = {
        "date": date.today().isoformat(),
        "brain_date": brain_date,
        "verdict": f"{label}{pct}%",
        "score_detail": brain.get("position_size_reason", ""),
        "kospi_change": round(kospi_chg, 2),
        "rec_avg_return": round(rec_avg, 2),
        "correct": correct,
        "bad_call": bad_call,
        "missed_opportunity": missed,
    }

    # 기존 파일 로드 + 추가 (30일 롤링)
    perf_list = []
    if PERF_PATH.exists():
        try:
            perf_list = json.loads(PERF_PATH.read_text("utf-8"))
        except Exception:
            perf_list = []

    # 중복 방지
    if perf_list and perf_list[-1].get("date") == entry["date"]:
        perf_list[-1] = entry  # 덮어쓰기
    else:
        perf_list.append(entry)

    # 30일 롤링
    perf_list = perf_list[-30:]

    PERF_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PERF_PATH, "w", encoding="utf-8") as f:
        json.dump(perf_list, f, ensure_ascii=False, indent=2)

    # 적중률 계산
    recent_10 = perf_list[-10:]
    if recent_10:
        hit_rate = sum(1 for p in recent_10 if p.get("correct")) / len(recent_10) * 100
    else:
        hit_rate = 50.0

    logger.info(
        f"[BRAIN Perf] {label}{pct}% | 코스피{kospi_chg:+.1f}% | "
        f"{'적중' if correct else '빗나감'} | 10일 적중률 {hit_rate:.0f}%"
    )

    return {
        "entry": entry,
        "hit_rate_10d": hit_rate,
        "total_records": len(perf_list),
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Body Hunter 일간 학습 엔진")
    parser.add_argument("--quick", action="store_true", help="추천 검증만 (급등주 분석 스킵)")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (텔레그램 전송 안함)")
    args = parser.parse_args()

    if args.test:
        TG_TOKEN = None  # noqa: F841 — 텔레그램 비활성화 (모듈 변수 덮어쓰기)
        print("[TEST MODE] 텔레그램 전송 OFF")

    run(quick=args.quick)
