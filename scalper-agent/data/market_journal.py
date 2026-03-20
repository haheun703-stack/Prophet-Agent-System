# -*- coding: utf-8 -*-
"""
Market Journal — 시장 일기 + 패턴 학습 + 피드백 루프
====================================================
1단: 매일 기록 (JRN-01~03)
  - 급등/급락 TOP10, 대형주, 섹터 동향
  - 원인 자동 태깅 (RELAY, TV_EXPLOSION, SECTOR_RALLY, ...)
  - "못 산 이유" 분석 (RR_REJECT, ENTRY_FAIL, ...)

2단: 패턴 축적 + 보고 (JRN-04~09)
  - patterns.json에 source별 적중률/수익률 누적
  - 주간/월간 보고 생성

3단: 피드백 루프 (JRN-10)
  - 패턴 성과 → R:R 임계값 조정 제안
  - 사용자 "적용" → rr_overrides.json → trade_object RRGate 반영

Usage:
    python data/market_journal.py --test-daily     # 오늘 일일 저널 생성
    python data/market_journal.py --test-patterns   # signal_log에서 패턴 소급
"""

import json
import csv
import logging
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

logger = logging.getLogger("BH.MarketJournal")

BASE_DIR = Path(__file__).resolve().parent.parent
STORE_DIR = BASE_DIR / "data_store"
LEARNING_DIR = STORE_DIR / "learning"
JOURNAL_DIR = LEARNING_DIR / "journal"
DAILY_DIR = JOURNAL_DIR / "daily"
WEEKLY_DIR = JOURNAL_DIR / "weekly"
MONTHLY_DIR = JOURNAL_DIR / "monthly"
PATTERNS_PATH = JOURNAL_DIR / "patterns.json"
RR_OVERRIDES_PATH = JOURNAL_DIR / "rr_overrides.json"


def _ensure_dirs():
    for d in [JOURNAL_DIR, DAILY_DIR, WEEKLY_DIR, MONTHLY_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# ════════════════════════════════════════════
# JRN-01: 일일 시장 기록
# ════════════════════════════════════════════

def _load_universe() -> dict:
    """universe.json → {code: {name, sector, cap_억}}"""
    upath = STORE_DIR / "universe.json"
    if not upath.exists():
        return {}
    try:
        data = json.loads(upath.read_text("utf-8"))
        return data
    except Exception:
        return {}


def _scan_all_stocks(today: str) -> List[dict]:
    """전체 종목 당일 등락률 스캔 (daily CSV 기반)

    find_missed_gainers() 변형 — threshold 없이 전체 + 섹터 태깅
    """
    try:
        from data.kis_collector import UNIVERSE
    except Exception:
        UNIVERSE = {}

    universe_info = _load_universe()

    daily_dir = STORE_DIR / "daily"
    if not daily_dir.exists():
        return []

    results = []
    for csv_file in daily_dir.glob("*.csv"):
        code = csv_file.stem
        if UNIVERSE and code not in UNIVERSE:
            continue

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows:
                    continue
                last = rows[-1]

                last_date = last.get("", "") or last.get("날짜", "") or last.get("date", "")
                if last_date != today:
                    continue

                close_raw = last.get("종가") or last.get("close", 0)
                vol_raw = last.get("거래량") or last.get("volume", 0)
                chg_raw = last.get("등락률")

                close = int(float(close_raw)) if close_raw else 0
                vol = int(float(vol_raw)) if vol_raw else 0

                if chg_raw is not None and chg_raw != "":
                    chg = float(chg_raw)
                elif len(rows) >= 2:
                    prev_close = float(rows[-2].get("종가") or rows[-2].get("close", 0) or 0)
                    chg = ((close / prev_close) - 1) * 100 if prev_close > 0 else 0
                else:
                    chg = 0

                # 5일 수익률 (최근 5봉)
                recent_5d = 0
                if len(rows) >= 6:
                    p5 = float(rows[-6].get("종가") or rows[-6].get("close", 0) or 0)
                    if p5 > 0:
                        recent_5d = round((close / p5 - 1) * 100, 2)

                # 20일 평균 거래량 대비 비율
                vol_ratio = 1.0
                if len(rows) >= 21:
                    avg_vol = sum(
                        int(float(r.get("거래량") or r.get("volume", 0) or 0))
                        for r in rows[-21:-1]
                    ) / 20
                    vol_ratio = round(vol / avg_vol, 2) if avg_vol > 0 else 1.0

                # 유니버스에서 이름+섹터
                uni = universe_info.get(code, {})
                name = uni.get("name", "") if isinstance(uni, dict) else ""
                if not name and UNIVERSE:
                    uv = UNIVERSE.get(code, [code])
                    name = uv[0] if isinstance(uv, (list, tuple)) else code
                sector = uni.get("sector", "") if isinstance(uni, dict) else ""
                cap = uni.get("cap_억", 0) if isinstance(uni, dict) else 0

                results.append({
                    "code": code,
                    "name": name,
                    "sector": sector,
                    "cap": cap,
                    "close": close,
                    "change_rate": round(chg, 2),
                    "volume": vol,
                    "volume_ratio": vol_ratio,
                    "recent_5d_gain": recent_5d,
                })
        except Exception:
            continue

    return results


def _get_large_cap_moves(stocks: List[dict], top_n: int = 15) -> List[dict]:
    """시총 TOP N 대형주 동향"""
    by_cap = sorted(stocks, key=lambda x: x.get("cap", 0), reverse=True)
    return [
        {"code": s["code"], "name": s["name"], "change_rate": s["change_rate"],
         "sector": s["sector"]}
        for s in by_cap[:top_n] if s.get("cap", 0) > 0
    ]


def _get_sector_summary(stocks: List[dict]) -> Dict[str, dict]:
    """섹터별 평균 등락률 + 대장주"""
    sectors = defaultdict(lambda: {"changes": [], "stocks": []})
    for s in stocks:
        sec = s.get("sector", "")
        if not sec:
            continue
        sectors[sec]["changes"].append(s["change_rate"])
        sectors[sec]["stocks"].append(s)

    result = {}
    for sec, data in sectors.items():
        if not data["changes"]:
            continue
        avg = round(sum(data["changes"]) / len(data["changes"]), 2)
        top = max(data["stocks"], key=lambda x: x["change_rate"])
        result[sec] = {
            "avg_change": avg,
            "count": len(data["changes"]),
            "top_stock": top["name"],
            "top_change": top["change_rate"],
        }

    return dict(sorted(result.items(), key=lambda x: x[1]["avg_change"], reverse=True))


# ════════════════════════════════════════════
# JRN-02: 원인 자동 태깅
# ════════════════════════════════════════════

CAUSE_LABELS = {
    "SECTOR_RALLY": "섹터 상승",
    "RELAY": "릴레이",
    "TV_EXPLOSION": "거래대금 폭발",
    "INST_BUY": "기관 매수",
    "FOREIGN_BUY": "외국인 매수",
    "MOMENTUM": "모멘텀",
    "BREAKOUT": "기술적 돌파",
    "REBOUND": "낙폭 반등",
    "SECTOR_SELLOFF": "섹터 하락",
    "PROFIT_TAKING": "차익실현",
    "FOREIGN_SELL": "외국인 매도",
    "INST_SELL": "기관 매도",
    "BREAKDOWN": "기술적 이탈",
}


def tag_cause(stock: dict, sector_summary: dict, rec_data: dict) -> List[str]:
    """급등/급락 원인 자동 태깅"""
    tags = []
    code = stock["code"]
    sector = stock.get("sector", "")
    change = stock["change_rate"]

    sec_avg = sector_summary.get(sector, {}).get("avg_change", 0)

    # 섹터 동조
    if change > 3 and sec_avg > 2:
        tags.append("SECTOR_RALLY")
    elif change < -3 and sec_avg < -2:
        tags.append("SECTOR_SELLOFF")

    # 우리 추천에 있었으면 sources 분석
    rec = rec_data.get(code)
    if rec:
        sources = rec.get("sources", [])
        src_lower = [s.lower() for s in sources]
        if any("relay" in s for s in src_lower):
            tags.append("RELAY")
        if any("tv:" in s for s in src_lower) or any("tv_cluster" in s for s in src_lower):
            tags.append("TV_EXPLOSION")
        if any("7secret" in s for s in src_lower):
            if change > 0:
                tags.append("FOREIGN_BUY")
            else:
                tags.append("FOREIGN_SELL")
        if any("bargain" in s for s in src_lower):
            tags.append("REBOUND")

    # 거래량 폭발
    if stock.get("volume_ratio", 0) >= 3.0 and "TV_EXPLOSION" not in tags:
        tags.append("TV_EXPLOSION")

    # 차익실현 (최근 5일 +10% 후 오늘 -3%)
    if change < -3 and stock.get("recent_5d_gain", 0) > 10:
        tags.append("PROFIT_TAKING")

    # 낙폭 반등 (최근 5일 -10% 후 오늘 +3%)
    if change > 3 and stock.get("recent_5d_gain", 0) < -10:
        tags.append("REBOUND")

    return tags if tags else ["UNKNOWN"]


# ════════════════════════════════════════════
# JRN-03: "못 산 이유" 분석
# ════════════════════════════════════════════

def _load_rec_data() -> dict:
    """recommendation.json → {code: {name, sources, total_score, ...}}"""
    rec_path = STORE_DIR / "recommendation.json"
    if not rec_path.exists():
        return {}
    try:
        rec = json.loads(rec_path.read_text("utf-8"))
        return {s["code"]: s for s in rec.get("stocks", [])}
    except Exception:
        return {}


def _load_trade_objects() -> dict:
    """trade_objects.json → {code: {rr_verdict, ...}}"""
    to_path = STORE_DIR / "trade_objects.json"
    if not to_path.exists():
        return {}
    try:
        data = json.loads(to_path.read_text("utf-8"))
        return {t["code"]: t for t in data.get("trades", [])}
    except Exception:
        return {}


def analyze_missed(today: str, verify_result: dict, all_stocks: List[dict]) -> List[dict]:
    """추천했는데 못 산 종목 + 추천 안 했는데 급등한 종목"""
    rec_data = _load_rec_data()
    trade_objects = _load_trade_objects()
    missed = []

    # A. 추천했는데 적중 못한 + 못 산 종목 분석
    bought_codes = set()
    # positions.json에서 실제 매수 확인
    pos_path = STORE_DIR / "positions.json"
    if pos_path.exists():
        try:
            bought_codes = set(json.loads(pos_path.read_text("utf-8")).keys())
        except Exception:
            pass

    for code, rec in rec_data.items():
        if code in bought_codes:
            continue  # 실제로 샀으면 스킵

        # 못 산 이유 분류
        to = trade_objects.get(code, {})
        if to.get("rr_verdict") == "REJECT":
            reason = "RR_REJECT"
        elif to.get("rr_verdict") == "MARGINAL_PASS":
            reason = "RR_MARGINAL"
        else:
            reason = "ENTRY_FAIL"  # 6조건 미충족 또는 슬롯 부족

        # 실제 등락률
        actual = 0
        for s in all_stocks:
            if s["code"] == code:
                actual = s["change_rate"]
                break

        missed.append({
            "code": code,
            "name": rec.get("name", code),
            "type": "REC_NOT_BOUGHT",
            "reason": reason,
            "score": rec.get("total_score", 0),
            "actual_change": actual,
        })

    # B. 추천 안 했는데 +5% 이상 급등
    for s in all_stocks:
        if s["change_rate"] >= 5.0 and s["code"] not in rec_data:
            # 유니버스에 있는지 체크
            uni = _load_universe()
            in_uni = s["code"] in uni
            reason = "LOW_SCORE" if in_uni else "OUT_OF_UNIVERSE"

            missed.append({
                "code": s["code"],
                "name": s["name"],
                "type": "NOT_RECOMMENDED",
                "reason": reason,
                "actual_change": s["change_rate"],
            })
            if len(missed) > 30:
                break

    return missed


# ════════════════════════════════════════════
# JRN-04~05: 일일 기록 + 패턴 누적
# ════════════════════════════════════════════

def _normalize_source(src: str) -> str:
    """소스 문자열 → 패턴 키 정규화

    "sector_hot:건설" → "sector_hot"
    "tv:EXPLOSION(82)" → "tv:EXPLOSION"
    "7SECRET(BUY:+12.7)" → "7SECRET"
    "brain(+9.7)" → skip (brain은 패턴 아님)
    "relay" → "relay"
    """
    s = src.lower().strip()
    if s.startswith("brain"):
        return ""  # brain 조정은 패턴이 아님
    if s.startswith("tv_cluster"):
        return "tv_cluster"
    if s.startswith("tv:"):
        # tv:EXPLOSION(82) → tv:EXPLOSION
        part = src[3:].split("(")[0].strip().upper()
        return f"tv:{part}" if part else "tv"
    if s.startswith("7secret"):
        return "7SECRET"
    if ":" in s:
        return s.split(":")[0]
    if "(" in s:
        return s.split("(")[0]
    return s


def load_patterns() -> dict:
    """patterns.json 로드"""
    if PATTERNS_PATH.exists():
        try:
            return json.loads(PATTERNS_PATH.read_text("utf-8"))
        except Exception:
            pass
    return {}


def save_patterns(patterns: dict):
    """patterns.json 저장"""
    _ensure_dirs()
    PATTERNS_PATH.write_text(json.dumps(patterns, ensure_ascii=False, indent=2), encoding="utf-8")


def update_patterns(verify_result: dict, patterns: dict) -> dict:
    """추천 결과 → 패턴별 통계 누적"""
    today = date.today().strftime("%Y-%m-%d")

    for detail in verify_result.get("details", []):
        sources = detail.get("sources", [])
        pnl = detail.get("pnl", 0)
        hit = detail.get("hit", False)

        for src in sources:
            key = _normalize_source(src)
            if not key:
                continue

            if key not in patterns:
                patterns[key] = {
                    "total": 0, "hits": 0, "pnl_sum": 0.0,
                    "history": [],
                }

            p = patterns[key]
            p["total"] += 1
            if hit:
                p["hits"] += 1
            p["pnl_sum"] = round(p.get("pnl_sum", 0) + pnl, 2)
            p["history"].append({
                "date": today,
                "code": detail["code"],
                "pnl": pnl,
            })
            # 최근 60건만 유지
            p["history"] = p["history"][-60:]

    # 파생 통계
    for key, p in patterns.items():
        total = p.get("total", 0)
        p["hit_rate"] = round(p["hits"] / total * 100, 1) if total > 0 else 0
        p["avg_pnl"] = round(p["pnl_sum"] / total, 2) if total > 0 else 0
        # 최근 20건 트렌드
        recent = p.get("history", [])[-20:]
        if len(recent) >= 5:
            p["recent_hit_rate"] = round(
                sum(1 for h in recent if h.get("pnl", 0) > 0) / len(recent) * 100, 1
            )

    return patterns


def record_daily(today: str, verify_result: dict = None) -> dict:
    """일일 시장 저널 엔트리 생성 + 저장"""
    _ensure_dirs()
    logger.info(f"[MarketJournal] 일일 기록 시작 ({today})")

    # 1. 전체 종목 스캔
    all_stocks = _scan_all_stocks(today)
    if not all_stocks:
        logger.warning("[MarketJournal] 당일 데이터 없음 — 스킵")
        return {}

    # 2. TOP 10 급등/급락
    gainers = sorted(all_stocks, key=lambda x: x["change_rate"], reverse=True)[:10]
    losers = sorted(all_stocks, key=lambda x: x["change_rate"])[:10]

    # 3. 대형주 동향
    large_caps = _get_large_cap_moves(all_stocks)

    # 4. 섹터 동향
    sector_summary = _get_sector_summary(all_stocks)

    # 5. 원인 태깅
    rec_data = _load_rec_data()
    for g in gainers:
        g["causes"] = tag_cause(g, sector_summary, rec_data)
    for l in losers:
        l["causes"] = tag_cause(l, sector_summary, rec_data)

    # 6. 못 산 이유
    missed = analyze_missed(today, verify_result or {}, all_stocks)

    # 7. 매크로 레짐
    brain = {}
    brain_path = STORE_DIR / "brain_report.json"
    if brain_path.exists():
        try:
            brain = json.loads(brain_path.read_text("utf-8"))
        except Exception:
            pass

    # 8. 추천 성과 요약
    rec_count = verify_result.get("total", 0) if verify_result else 0
    rec_hits = verify_result.get("hits", 0) if verify_result else 0
    rec_hit_rate = verify_result.get("hit_rate", 0) if verify_result else 0

    # 9. 패턴 업데이트
    patterns = load_patterns()
    if verify_result:
        patterns = update_patterns(verify_result, patterns)
        save_patterns(patterns)

    # 10. 공매도 잔고 섹션
    short_selling = {}
    try:
        from data.short_analyzer import get_short_journal_data
        short_selling = get_short_journal_data(
            [s.get("code", "") for s in all_stocks[:500]] if all_stocks else None
        )
    except Exception as e:
        logger.debug(f"[MarketJournal] 공매도 데이터 수집 실패 (무시): {e}")

    journal_entry = {
        "date": today,
        "regime": brain.get("regime", "NORMAL"),
        "macro_summary": brain.get("overall_verdict", ""),
        "total_stocks_scanned": len(all_stocks),
        "gainers_top10": gainers,
        "losers_top10": losers,
        "large_cap_moves": large_caps,
        "sector_summary": sector_summary,
        "rec_count": rec_count,
        "rec_hits": rec_hits,
        "rec_hit_rate": rec_hit_rate,
        "rec_results": verify_result.get("details", []) if verify_result else [],
        "missed_analysis": missed,
        "short_selling": short_selling,
    }

    # 저장
    daily_path = DAILY_DIR / f"{today}.json"
    daily_path.write_text(json.dumps(journal_entry, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[MarketJournal] 일일 저널 저장: {daily_path}")
    logger.info(f"  스캔: {len(all_stocks)}종목 | 급등TOP: {gainers[0]['name']}(+{gainers[0]['change_rate']}%) | "
                f"추천: {rec_count}건 적중 {rec_hits}건({rec_hit_rate}%)")

    return journal_entry


# ════════════════════════════════════════════
# JRN-06: 주간 보고
# ════════════════════════════════════════════

def _load_journals(days: int = 7) -> List[dict]:
    """최근 N일 일일 저널 로드"""
    journals = []
    for i in range(days):
        d = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        path = DAILY_DIR / f"{d}.json"
        if path.exists():
            try:
                journals.append(json.loads(path.read_text("utf-8")))
            except Exception:
                continue
    return list(reversed(journals))  # 오래된 것부터


def generate_weekly_report() -> dict:
    """최근 7일 저널 → 주간 보고"""
    journals = _load_journals(7)
    if not journals:
        return {"error": "주간 데이터 없음"}

    patterns = load_patterns()

    # 기본 통계
    total_rec = sum(j.get("rec_count", 0) for j in journals)
    total_hits = sum(j.get("rec_hits", 0) for j in journals)
    hit_rate = round(total_hits / total_rec * 100, 1) if total_rec > 0 else 0

    # 최고/최악 종목
    all_details = []
    for j in journals:
        all_details.extend(j.get("rec_results", []))
    best = max(all_details, key=lambda x: x.get("pnl", 0)) if all_details else {}
    worst = min(all_details, key=lambda x: x.get("pnl", 0)) if all_details else {}

    # 패턴 TOP/WORST
    sorted_patterns = sorted(
        [(k, v) for k, v in patterns.items() if v.get("total", 0) >= 3],
        key=lambda x: x[1].get("hit_rate", 0),
        reverse=True,
    )
    top_patterns = sorted_patterns[:5]
    worst_patterns = sorted_patterns[-3:] if len(sorted_patterns) > 3 else []

    # 섹터 흐름
    sector_flow = defaultdict(list)
    for j in journals:
        for sec, info in j.get("sector_summary", {}).items():
            sector_flow[sec].append(info.get("avg_change", 0))

    hot_sectors = {
        sec: round(sum(changes) / len(changes), 2)
        for sec, changes in sector_flow.items()
        if len(changes) >= 3 and sum(changes) / len(changes) > 1.0
    }

    # 못 산 이유 통계
    miss_reasons = defaultdict(int)
    for j in journals:
        for m in j.get("missed_analysis", []):
            miss_reasons[m.get("reason", "UNKNOWN")] += 1

    today = date.today()
    week_num = today.isocalendar()[1]
    report = {
        "period": f"{today.year}-W{week_num:02d}",
        "trading_days": len(journals),
        "total_recommended": total_rec,
        "total_hits": total_hits,
        "hit_rate": hit_rate,
        "best_pick": {"name": best.get("name", ""), "pnl": best.get("pnl", 0)} if best else {},
        "worst_pick": {"name": worst.get("name", ""), "pnl": worst.get("pnl", 0)} if worst else {},
        "top_patterns": [
            {"pattern": k, "hit_rate": v["hit_rate"], "total": v["total"], "avg_pnl": v["avg_pnl"]}
            for k, v in top_patterns
        ],
        "worst_patterns": [
            {"pattern": k, "hit_rate": v["hit_rate"], "total": v["total"], "avg_pnl": v["avg_pnl"]}
            for k, v in worst_patterns
        ],
        "hot_sectors": hot_sectors,
        "miss_reasons": dict(miss_reasons),
    }

    # 저장
    _ensure_dirs()
    path = WEEKLY_DIR / f"{report['period']}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[MarketJournal] 주간 보고 저장: {path}")
    return report


# ════════════════════════════════════════════
# JRN-07~09: 월간 보고 + 제안 생성
# ════════════════════════════════════════════

def _generate_proposals(patterns: dict) -> List[dict]:
    """패턴 성과 → R:R 임계값 조정 제안"""
    proposals = []
    for key, p in patterns.items():
        total = p.get("total", 0)
        if total < 10:
            continue  # 샘플 부족

        hr = p.get("hit_rate", 0)
        avg_pnl = p.get("avg_pnl", 0)

        # R:R 완화 제안: 적중률 70%+ AND 평균 수익 1.5%+
        if hr >= 70 and avg_pnl >= 1.5:
            proposals.append({
                "type": "RR_LOWER",
                "pattern": key,
                "current_rr": 1.5,
                "proposed_rr": 1.3,
                "reason": f"{key} 적중률 {hr}%, 평균 +{avg_pnl}% → R:R 1.3 완화",
                "evidence": {"total": total, "hits": p["hits"], "avg_pnl": avg_pnl},
            })
        # R:R 강화 제안: 적중률 40% 미만 AND 평균 손실
        elif hr < 40 and avg_pnl < 0:
            proposals.append({
                "type": "RR_RAISE",
                "pattern": key,
                "current_rr": 1.5,
                "proposed_rr": 2.0,
                "reason": f"{key} 적중률 {hr}%, 평균 {avg_pnl}% → R:R 2.0 강화",
                "evidence": {"total": total, "hits": p["hits"], "avg_pnl": avg_pnl},
            })

    return proposals


def generate_monthly_report() -> dict:
    """최근 30일 저널 → 월간 보고 + 제안"""
    journals = _load_journals(31)
    if not journals:
        return {"error": "월간 데이터 없음"}

    patterns = load_patterns()

    total_rec = sum(j.get("rec_count", 0) for j in journals)
    total_hits = sum(j.get("rec_hits", 0) for j in journals)
    hit_rate = round(total_hits / total_rec * 100, 1) if total_rec > 0 else 0

    # 주차별 적중률 트렌드
    week_rates = []
    for i in range(0, len(journals), 5):
        chunk = journals[i:i+5]
        wr = sum(j.get("rec_count", 0) for j in chunk)
        wh = sum(j.get("rec_hits", 0) for j in chunk)
        week_rates.append(round(wh / wr * 100, 1) if wr > 0 else 0)

    # 패턴 전체 성과
    pattern_stats = []
    for key, p in sorted(patterns.items(), key=lambda x: x[1].get("total", 0), reverse=True):
        if p.get("total", 0) >= 3:
            pattern_stats.append({
                "pattern": key,
                "total": p["total"],
                "hits": p["hits"],
                "hit_rate": p["hit_rate"],
                "avg_pnl": p["avg_pnl"],
            })

    # 제안 생성
    proposals = _generate_proposals(patterns)

    # 못 산 이유 통계
    miss_reasons = defaultdict(int)
    for j in journals:
        for m in j.get("missed_analysis", []):
            miss_reasons[m.get("reason", "UNKNOWN")] += 1

    today = date.today()
    report = {
        "period": today.strftime("%Y-%m"),
        "trading_days": len(journals),
        "total_recommended": total_rec,
        "total_hits": total_hits,
        "hit_rate": hit_rate,
        "weekly_trend": week_rates,
        "pattern_performance": pattern_stats,
        "parameter_proposals": proposals,
        "miss_reasons": dict(miss_reasons),
    }

    _ensure_dirs()
    path = MONTHLY_DIR / f"{report['period']}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[MarketJournal] 월간 보고 저장: {path}")
    return report


# ════════════════════════════════════════════
# JRN-10: 피드백 루프 — R:R override 적용
# ════════════════════════════════════════════

def load_rr_overrides() -> dict:
    """rr_overrides.json 로드"""
    if RR_OVERRIDES_PATH.exists():
        try:
            return json.loads(RR_OVERRIDES_PATH.read_text("utf-8"))
        except Exception:
            pass
    return {"updated": "", "overrides": {}}


def apply_proposals(proposal_indices: List[int], proposals: List[dict]) -> dict:
    """제안 적용 → rr_overrides.json 업데이트

    Args:
        proposal_indices: 적용할 제안 번호 (1-based)
        proposals: _generate_proposals() 결과
    """
    overrides_data = load_rr_overrides()
    overrides = overrides_data.get("overrides", {})
    today = date.today().strftime("%Y-%m-%d")
    applied = []

    for idx in proposal_indices:
        if 1 <= idx <= len(proposals):
            p = proposals[idx - 1]
            pattern = p["pattern"]
            if p["type"] == "RR_LOWER":
                overrides[pattern] = {
                    "rr_min": p["proposed_rr"],
                    "reason": p["reason"],
                    "applied": today,
                }
            elif p["type"] == "RR_RAISE":
                overrides[pattern] = {
                    "rr_min": p["proposed_rr"],
                    "reason": p["reason"],
                    "applied": today,
                }
            applied.append(f"[{idx}] {pattern}: R:R → {p['proposed_rr']}")

    overrides_data["updated"] = today
    overrides_data["overrides"] = overrides

    _ensure_dirs()
    RR_OVERRIDES_PATH.write_text(
        json.dumps(overrides_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"[MarketJournal] R:R override 적용: {applied}")
    return {"applied": applied, "overrides": overrides}


# ════════════════════════════════════════════
# 텔레그램 포맷터
# ════════════════════════════════════════════

def format_daily_journal(journal: dict) -> str:
    """일일 저널 → 텔레그램 메시지"""
    if not journal or journal.get("error"):
        return "일일 저널 데이터가 없습니다."

    d = journal["date"]
    regime = journal.get("regime", "NORMAL")
    lines = [f"━━ 시장 일지 {d} ━━━━━━"]
    lines.append(f"레짐: {regime}")
    lines.append(f"스캔: {journal.get('total_stocks_scanned', 0)}종목")

    # 급등 TOP 5
    lines.append("")
    lines.append("📊 급등 TOP 5")
    for i, g in enumerate(journal.get("gainers_top10", [])[:5], 1):
        causes = ", ".join(g.get("causes", []))
        lines.append(f"  {i}. {g['name']} +{g['change_rate']}% [{causes}]")

    # 급락 TOP 5
    lines.append("")
    lines.append("📉 급락 TOP 5")
    for i, l in enumerate(journal.get("losers_top10", [])[:5], 1):
        causes = ", ".join(l.get("causes", []))
        lines.append(f"  {i}. {l['name']} {l['change_rate']}% [{causes}]")

    # 섹터 TOP 5
    lines.append("")
    lines.append("🏭 섹터 동향")
    sectors = list(journal.get("sector_summary", {}).items())[:5]
    sec_parts = []
    for sec, info in sectors:
        sec_parts.append(f"{sec} {info['avg_change']:+.1f}%")
    lines.append("  " + " | ".join(sec_parts))

    # 추천 성과
    lines.append("")
    lines.append("📋 우리 추천 성과")
    rc = journal.get("rec_count", 0)
    rh = journal.get("rec_hits", 0)
    rr = journal.get("rec_hit_rate", 0)
    lines.append(f"  추천 {rc}건 → 적중 {rh}건 ({rr}%)")

    # 못 산 이유 요약
    miss = journal.get("missed_analysis", [])
    if miss:
        reasons = defaultdict(int)
        for m in miss:
            if m.get("type") == "REC_NOT_BOUGHT":
                reasons[m["reason"]] += 1
        if reasons:
            parts = [f"{r} {c}건" for r, c in reasons.items()]
            lines.append(f"  못 산 이유: {', '.join(parts)}")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def format_weekly_report(report: dict) -> str:
    """주간 보고 → 텔레그램 메시지"""
    if not report or report.get("error"):
        return "주간 데이터가 부족합니다."

    lines = [f"━━ 주간 보고 {report['period']} ━━━━━━━━"]
    lines.append(f"거래일 {report['trading_days']}일 | 추천 {report['total_recommended']}건 "
                 f"→ 적중 {report['total_hits']}건 ({report['hit_rate']}%)")

    # 패턴 TOP
    if report.get("top_patterns"):
        lines.append("")
        lines.append("📊 패턴 적중률 TOP")
        for i, p in enumerate(report["top_patterns"][:5], 1):
            lines.append(f"  {i}. {p['pattern']}: {p['hit_rate']}% ({p['total']}건, avg {p['avg_pnl']:+.1f}%)")

    # 하위 패턴
    if report.get("worst_patterns"):
        lines.append("")
        lines.append("📉 하위 패턴")
        for p in report["worst_patterns"]:
            lines.append(f"  {p['pattern']}: {p['hit_rate']}% ({p['total']}건, avg {p['avg_pnl']:+.1f}%)")

    # HOT 섹터
    if report.get("hot_sectors"):
        lines.append("")
        lines.append("🔄 HOT 섹터 (주간 평균 +1%+)")
        for sec, avg in sorted(report["hot_sectors"].items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"  {sec} avg {avg:+.1f}%")

    # 못 산 이유
    if report.get("miss_reasons"):
        lines.append("")
        lines.append("💡 못 산 이유 통계")
        for r, c in sorted(report["miss_reasons"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  {r}: {c}건")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def format_monthly_report(report: dict) -> str:
    """월간 보고 → 텔레그램 메시지"""
    if not report or report.get("error"):
        return "월간 데이터가 부족합니다."

    lines = [f"━━ 월간 보고 {report['period']} ━━━━━━━━━"]
    lines.append(f"거래일 {report['trading_days']}일 | 추천 {report['total_recommended']}건 "
                 f"→ 적중 {report['total_hits']}건 ({report['hit_rate']}%)")

    # 학습 곡선
    trend = report.get("weekly_trend", [])
    if trend:
        parts = [f"{i+1}주차 {r}%" for i, r in enumerate(trend)]
        lines.append("")
        lines.append("📈 학습 곡선")
        lines.append("  " + " → ".join(parts))

    # 패턴 전체
    if report.get("pattern_performance"):
        lines.append("")
        lines.append("📊 패턴 성과 전체")
        for p in report["pattern_performance"][:10]:
            lines.append(f"  {p['pattern']}: {p['hit_rate']}% ({p['total']}건, avg {p['avg_pnl']:+.1f}%)")

    # 제안
    proposals = report.get("parameter_proposals", [])
    if proposals:
        lines.append("")
        lines.append("━━ 파라미터 조정 제안 ━━━━━━━━")
        for i, p in enumerate(proposals, 1):
            arrow = "↓" if p["type"] == "RR_LOWER" else "↑"
            lines.append(f"  [{i}] {p['pattern']} R:R {p['current_rr']}→{p['proposed_rr']} {arrow} ({p['reason']})")
        lines.append("")
        lines.append("적용하려면: \"적용 1 2\" 또는 \"적용 전체\"")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ════════════════════════════════════════════
# 소급 패턴 생성 (signal_log.json에서)
# ════════════════════════════════════════════

def bootstrap_patterns_from_signal_log() -> dict:
    """기존 signal_log.json에서 패턴 소급 생성"""
    sl_path = LEARNING_DIR / "signal_log.json"
    if not sl_path.exists():
        logger.warning("signal_log.json 없음 — 소급 불가")
        return {}

    try:
        log = json.loads(sl_path.read_text("utf-8"))
    except Exception as e:
        logger.error(f"signal_log 파싱 실패: {e}")
        return {}

    patterns = {}
    for entry in log:
        entry_date = entry.get("date", "")
        for detail in entry.get("details", []):
            sources = detail.get("sources", [])
            pnl = detail.get("pnl", 0)
            hit = detail.get("hit", False)

            for src in sources:
                key = _normalize_source(src)
                if not key:
                    continue
                if key not in patterns:
                    patterns[key] = {"total": 0, "hits": 0, "pnl_sum": 0.0, "history": []}
                p = patterns[key]
                p["total"] += 1
                if hit:
                    p["hits"] += 1
                p["pnl_sum"] = round(p["pnl_sum"] + pnl, 2)
                p["history"].append({"date": entry_date, "code": detail["code"], "pnl": pnl})
                p["history"] = p["history"][-60:]

    # 파생 통계
    for key, p in patterns.items():
        total = p["total"]
        p["hit_rate"] = round(p["hits"] / total * 100, 1) if total > 0 else 0
        p["avg_pnl"] = round(p["pnl_sum"] / total, 2) if total > 0 else 0
        recent = p["history"][-20:]
        if len(recent) >= 5:
            p["recent_hit_rate"] = round(
                sum(1 for h in recent if h.get("pnl", 0) > 0) / len(recent) * 100, 1
            )

    save_patterns(patterns)
    logger.info(f"[MarketJournal] 소급 패턴 생성: {len(patterns)}개 패턴")
    return patterns


# ════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════

def _test_daily():
    """테스트: 오늘 일일 저널 생성"""
    today = date.today().strftime("%Y-%m-%d")

    # verify 결과 로드 시도
    verify = {"total": 0, "hits": 0, "hit_rate": 0, "details": []}
    sl_path = LEARNING_DIR / "signal_log.json"
    if sl_path.exists():
        try:
            log = json.loads(sl_path.read_text("utf-8"))
            if log and log[-1].get("date") == today:
                verify = log[-1]
        except Exception:
            pass

    journal = record_daily(today, verify)
    if journal:
        print(format_daily_journal(journal))
    else:
        print("일일 저널 생성 실패 (당일 데이터 없음)")


def _test_patterns():
    """테스트: signal_log에서 패턴 소급"""
    patterns = bootstrap_patterns_from_signal_log()
    if not patterns:
        print("패턴 소급 실패 (signal_log 없음)")
        return

    print(f"{'=' * 50}")
    print(f"  패턴 소급 결과: {len(patterns)}개 패턴")
    print(f"{'=' * 50}")

    for key, p in sorted(patterns.items(), key=lambda x: x[1]["total"], reverse=True):
        print(f"  {key:20s} | {p['total']:3d}건 | 적중 {p['hit_rate']:5.1f}% | avg {p['avg_pnl']:+.2f}%")

    # 제안 생성
    proposals = _generate_proposals(patterns)
    if proposals:
        print(f"\n{'─' * 50}")
        print("  파라미터 조정 제안:")
        for i, p in enumerate(proposals, 1):
            print(f"  [{i}] {p['reason']}")


if __name__ == "__main__":
    sys.path.insert(0, str(BASE_DIR))
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test-daily" in sys.argv:
        _test_daily()
    elif "--test-patterns" in sys.argv:
        _test_patterns()
    elif "--weekly" in sys.argv:
        report = generate_weekly_report()
        print(format_weekly_report(report))
    elif "--monthly" in sys.argv:
        report = generate_monthly_report()
        print(format_monthly_report(report))
    else:
        print("사용법:")
        print("  python data/market_journal.py --test-daily    # 오늘 일일 저널")
        print("  python data/market_journal.py --test-patterns # signal_log 소급")
        print("  python data/market_journal.py --weekly        # 주간 보고")
        print("  python data/market_journal.py --monthly       # 월간 보고")
