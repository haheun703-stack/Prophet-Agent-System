# -*- coding: utf-8 -*-
"""
추천 적중률 추적기 (Recommendation Tracker)
=============================================
매일 추천 결과를 스냅샷으로 저장하고, T+1~T+5 실제 수익률을 추적.
어떤 팩터가 실제 수익에 기여하는지 분석할 수 있는 데이터 축적.

사용법:
  python -m data.recommendation_tracker save       # 오늘 추천 스냅샷 저장
  python -m data.recommendation_tracker update      # 과거 추천의 T+N 수익률 업데이트
  python -m data.recommendation_tracker report      # 적중률 리포트
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger("BH.Tracker")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
TRACKER_PATH = DATA_DIR / "recommendation_history.json"


def _load_history() -> dict:
    """추적 히스토리 로드"""
    if TRACKER_PATH.exists():
        try:
            with open(TRACKER_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_history(history: dict):
    """추적 히스토리 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(TRACKER_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def save_recommendation_snapshot(report=None) -> int:
    """오늘 추천 결과를 히스토리에 스냅샷 저장

    Args:
        report: RecommendationReport 객체 (None이면 recommendation.json에서 로드)

    Returns: 저장된 종목 수
    """
    if report is None:
        from data.morning_recommendation import load_recommendation
        report = load_recommendation()
        if report is None:
            logger.warning("추천 데이터 없음 — 스냅샷 저장 생략")
            return 0

    today = datetime.now().strftime("%Y-%m-%d")
    history = _load_history()

    # 이미 오늘 스냅샷 있으면 덮어쓰기
    snapshot = {
        "timestamp": report.timestamp,
        "market_health": report.market_health,
        "market_change": getattr(report, "market_change", 0),
        "stocks": [],
    }

    for s in report.stocks:
        stock_data = {
            "code": s.code,
            "name": s.name,
            "close_at_rec": s.close,
            "total_score": s.total_score,
            "confidence": s.confidence,
            "sources": s.sources,
            "entry": s.entry,
            "sl": s.sl,
            "tp": s.tp,
            "today_chg": getattr(s, "today_chg", 0),
            "relative_str": getattr(s, "relative_str", 0),
            # 팩터별 점수 (나중에 기여도 분석용)
            "factors": {
                "relay": s.relay_score,
                "premove": s.premove_score,
                "tech": s.tech_score,
                "news": s.news_score,
                "nationality": s.nationality_score,
                "news_penalty": getattr(s, "news_penalty", 0),
                "obv_penalty": getattr(s, "obv_penalty", 0),
                "relative_penalty": getattr(s, "relative_penalty", 0),
            },
            # T+N 수익률 (나중에 update_returns()로 채움)
            "T1": None,
            "T2": None,
            "T3": None,
            "T5": None,
        }
        snapshot["stocks"].append(stock_data)

    history[today] = snapshot
    _save_history(history)
    logger.info(f"추천 스냅샷 저장: {today} ({len(snapshot['stocks'])}종목)")
    return len(snapshot["stocks"])


def update_returns() -> dict:
    """과거 추천의 T+N 수익률을 현재 가격 기준으로 업데이트

    Returns: {"updated": N, "dates_checked": M}
    """
    from pykrx import stock

    history = _load_history()
    if not history:
        return {"updated": 0, "dates_checked": 0}

    today = datetime.now()
    today_s = today.strftime("%Y-%m-%d")
    updated_count = 0
    dates_checked = 0

    for rec_date, snapshot in history.items():
        rec_dt = datetime.strptime(rec_date, "%Y-%m-%d")
        days_since = (today - rec_dt).days

        if days_since < 1:
            continue  # 오늘 추천은 아직 T+1 없음

        dates_checked += 1

        for stock_data in snapshot["stocks"]:
            code = stock_data["code"]
            close_at_rec = stock_data.get("close_at_rec", 0)
            if close_at_rec <= 0:
                continue

            # 각 T+N 날짜의 종가 가져오기
            for t_label, t_days in [("T1", 1), ("T2", 2), ("T3", 3), ("T5", 5)]:
                if stock_data.get(t_label) is not None:
                    continue  # 이미 채워짐

                if days_since < t_days:
                    continue  # 아직 해당 날짜 안 됨

                target_date = rec_dt + timedelta(days=t_days)
                # 주말 건너뛰기 (단순 보정)
                while target_date.weekday() >= 5:
                    target_date += timedelta(days=1)

                target_s = target_date.strftime("%Y%m%d")

                try:
                    df = stock.get_market_ohlcv(target_s, target_s, code)
                    if df is not None and len(df) > 0:
                        close_tn = int(df["종가"].iloc[-1])
                        ret = round((close_tn / close_at_rec - 1) * 100, 2)
                        stock_data[t_label] = ret
                        updated_count += 1
                except Exception:
                    pass  # 데이터 없으면 다음에 재시도

    _save_history(history)
    logger.info(f"수익률 업데이트: {updated_count}건 (체크한 날짜: {dates_checked})")
    return {"updated": updated_count, "dates_checked": dates_checked}


def get_hit_rate_report(days: int = 30) -> str:
    """적중률 리포트 생성

    Args:
        days: 최근 N일 데이터만 분석

    Returns: 텔레그램 형식 리포트
    """
    history = _load_history()
    if not history:
        return "추천 히스토리 없음"

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    total_recs = 0
    hit_t1 = 0  # T+1 양수
    hit_t3 = 0  # T+3 양수
    hit_t5 = 0  # T+5 양수
    t1_returns = []
    t3_returns = []
    t5_returns = []

    # 팩터별 수익률 집계
    factor_returns = {}  # factor_name -> [returns]

    for rec_date, snapshot in sorted(history.items()):
        if rec_date < cutoff:
            continue

        for sd in snapshot["stocks"]:
            total_recs += 1

            if sd.get("T1") is not None:
                t1_returns.append(sd["T1"])
                if sd["T1"] > 0:
                    hit_t1 += 1

            if sd.get("T3") is not None:
                t3_returns.append(sd["T3"])
                if sd["T3"] > 0:
                    hit_t3 += 1

            if sd.get("T5") is not None:
                t5_returns.append(sd["T5"])
                if sd["T5"] > 0:
                    hit_t5 += 1

                # 팩터별 기여도 (T5 수익률과 연관)
                factors = sd.get("factors", {})
                for fname, fval in factors.items():
                    if fval != 0:
                        if fname not in factor_returns:
                            factor_returns[fname] = {"positive": [], "negative": []}
                        if sd["T5"] > 0:
                            factor_returns[fname]["positive"].append(sd["T5"])
                        else:
                            factor_returns[fname]["negative"].append(sd["T5"])

    lines = [
        f"📊 추천 적중률 리포트 (최근 {days}일)",
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"총 추천: {total_recs}종목 ({len(history)}일)",
        "",
    ]

    # T+N 적중률
    def _rate(hits, total):
        return f"{hits}/{total} ({hits/total*100:.0f}%)" if total > 0 else "데이터없음"

    def _avg(arr):
        return f"{sum(arr)/len(arr):+.2f}%" if arr else "N/A"

    lines.append(f"T+1: {_rate(hit_t1, len(t1_returns))} | 평균 {_avg(t1_returns)}")
    lines.append(f"T+3: {_rate(hit_t3, len(t3_returns))} | 평균 {_avg(t3_returns)}")
    lines.append(f"T+5: {_rate(hit_t5, len(t5_returns))} | 평균 {_avg(t5_returns)}")

    # 팩터별 승률
    if factor_returns:
        lines.append("")
        lines.append("📈 팩터별 T+5 승률:")
        for fname, data in sorted(factor_returns.items()):
            pos = len(data["positive"])
            neg = len(data["negative"])
            total = pos + neg
            if total > 0:
                rate = pos / total * 100
                avg_ret = sum(data["positive"] + data["negative"]) / total
                lines.append(f"  {fname}: {pos}/{total} ({rate:.0f}%) avg {avg_ret:+.1f}%")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def format_daily_result(rec_date: str) -> str:
    """특정 날짜의 추천 결과 + 수익률 포맷"""
    history = _load_history()
    snapshot = history.get(rec_date)
    if not snapshot:
        return f"{rec_date} 추천 데이터 없음"

    lines = [
        f"📋 {rec_date} 추천 결과",
        f"시장: {snapshot.get('market_change', 0):+.1f}%",
        "",
    ]

    for sd in snapshot["stocks"]:
        ret_parts = []
        for t in ["T1", "T2", "T3", "T5"]:
            val = sd.get(t)
            if val is not None:
                icon = "✅" if val > 0 else "❌"
                ret_parts.append(f"{t}:{icon}{val:+.1f}%")
            else:
                ret_parts.append(f"{t}:⏳")

        lines.append(f"{sd['name']}({sd['code']}) 점수:{sd['total_score']:.0f}")
        lines.append(f"  {' | '.join(ret_parts)}")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    if len(sys.argv) < 2:
        print("사용법: python -m data.recommendation_tracker [save|update|report]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "save":
        n = save_recommendation_snapshot()
        print(f"저장 완료: {n}종목")
    elif cmd == "update":
        result = update_returns()
        print(f"업데이트: {result}")
    elif cmd == "report":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        print(get_hit_rate_report(days))
    else:
        print(f"알 수 없는 명령: {cmd}")
