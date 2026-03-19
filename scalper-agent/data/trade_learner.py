# -*- coding: utf-8 -*-
"""
Trade Learner — 트레이드 단위 P&L 학습 + 콜드스타트 부트스트랩
================================================================
기존 daily_learner.py의 "종목 적중 여부" → "트레이드가 돈을 벌었냐"로 전환.

Usage:
    python data/trade_learner.py --bootstrap   # 콜드스타트: score_bucket_returns 초기화
    python data/trade_learner.py --stats       # 현재 트레이드 통계 출력
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("BH.TradeLearner")

_STORE = Path(__file__).resolve().parent.parent / "data_store"
_LEARN_DIR = _STORE / "learning"
_DAILY_DIR = _STORE / "daily"

BUCKET_PATH = _LEARN_DIR / "score_bucket_returns.json"
TRADE_LOG_PATH = _LEARN_DIR / "trade_log.json"
SIGNAL_LOG_PATH = _LEARN_DIR / "signal_log.json"

# ─── 보수적 기본값 (데이터 부족 시) ─────────────────────
DEFAULT_BUCKETS = {
    "100_plus":  {"avg_1d": 2.0, "avg_3d": 3.5, "avg_5d": 5.0, "hit_rate": 0.65, "n": 0},
    "80_99":     {"avg_1d": 1.5, "avg_3d": 2.5, "avg_5d": 3.5, "hit_rate": 0.58, "n": 0},
    "60_79":     {"avg_1d": 0.8, "avg_3d": 1.5, "avg_5d": 2.0, "hit_rate": 0.50, "n": 0},
    "40_59":     {"avg_1d": 0.3, "avg_3d": 0.5, "avg_5d": 0.8, "hit_rate": 0.42, "n": 0},
    "under_40":  {"avg_1d": 0.0, "avg_3d": 0.0, "avg_5d": 0.0, "hit_rate": 0.30, "n": 0},
}


def _score_to_bucket(score: float) -> str:
    if score >= 100:
        return "100_plus"
    elif score >= 80:
        return "80_99"
    elif score >= 60:
        return "60_79"
    elif score >= 40:
        return "40_59"
    return "under_40"


# ─── Score Bucket Returns 관리 ─────────────────────────

def load_bucket_returns() -> dict:
    """score_bucket_returns.json 로드 (없으면 기본값)"""
    if BUCKET_PATH.exists():
        try:
            with open(BUCKET_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"updated": "", "sample_size": 0, "buckets": dict(DEFAULT_BUCKETS)}


def save_bucket_returns(data: dict):
    _LEARN_DIR.mkdir(parents=True, exist_ok=True)
    data["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    tmp = BUCKET_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(BUCKET_PATH)


def get_expected_return(score: float, hold_days: int = 3) -> float:
    """점수대별 기대수익률 반환"""
    data = load_bucket_returns()
    bucket = _score_to_bucket(score)
    bk = data.get("buckets", DEFAULT_BUCKETS).get(bucket, DEFAULT_BUCKETS["60_79"])

    if hold_days <= 1:
        return bk.get("avg_1d", 0.5)
    elif hold_days <= 3:
        return bk.get("avg_3d", 1.0)
    return bk.get("avg_5d", 1.5)


# ─── 트레이드 로그 관리 ────────────────────────────────

def load_trade_log() -> list:
    if TRADE_LOG_PATH.exists():
        try:
            with open(TRADE_LOG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_trade_log(log: list):
    _LEARN_DIR.mkdir(parents=True, exist_ok=True)
    # 최근 200건만 유지
    log = log[-200:]
    tmp = TRADE_LOG_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    tmp.replace(TRADE_LOG_PATH)


def record_trade_close(trade_id: str, code: str, name: str,
                       entry_price: int, exit_price: int,
                       total_score: float, sources: list,
                       close_reason: str, hold_days: int,
                       expected_return: float, rr_ratio: float):
    """트레이드 종료 시 기록"""
    actual_pnl = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0

    record = {
        "trade_id": trade_id,
        "code": code,
        "name": name,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "total_score": total_score,
        "sources": sources,
        "close_reason": close_reason,
        "hold_days": hold_days,
        "expected_return": round(expected_return, 2),
        "actual_pnl": round(actual_pnl, 2),
        "prediction_error": round(actual_pnl - expected_return, 2),
        "rr_ratio": round(rr_ratio, 2),
        "category": _categorize(close_reason, actual_pnl),
    }

    log = load_trade_log()
    log.append(record)
    save_trade_log(log)
    return record


def _categorize(reason: str, pnl: float) -> str:
    if reason == "TARGET":
        return "PLAN_WORKED"
    elif reason == "STOP":
        return "WRONG_THESIS"
    elif reason == "TIME":
        return "DEAD_MONEY"
    elif reason == "GUARDIAN":
        return "CHANGED_CONDITION"
    elif pnl > 0:
        return "EARLY_WIN"
    return "EARLY_LOSS"


# ─── Bucket Returns 업데이트 (장마감 후) ───────────────

def update_bucket_returns(closed_trades: list = None):
    """종료된 트레이드로 bucket returns 테이블 업데이트

    Args:
        closed_trades: 종료 트레이드 리스트. None이면 오늘 기록된 trade_log에서 자동 로드
    """
    if closed_trades is None:
        # 오늘 기록된 트레이드 자동 로드
        today = datetime.now().strftime("%Y-%m-%d")
        all_log = load_trade_log()
        closed_trades = [t for t in all_log if t.get("date") == today]

    if not closed_trades:
        return

    data = load_bucket_returns()
    buckets = data.get("buckets", dict(DEFAULT_BUCKETS))

    for t in closed_trades:
        bucket = _score_to_bucket(t.get("total_score", 50))
        bk = buckets.get(bucket, dict(DEFAULT_BUCKETS.get(bucket, {})))

        old_n = bk.get("n", 0)
        pnl = t.get("actual_pnl", 0)
        hold = t.get("hold_days", 3)

        # 이동 평균 업데이트 (EMA-like)
        new_n = old_n + 1
        alpha = 1.0 / new_n

        # hold_days에 따라 해당 구간 업데이트
        if hold <= 1:
            key = "avg_1d"
        elif hold <= 3:
            key = "avg_3d"
        else:
            key = "avg_5d"

        old_val = bk.get(key, 1.0)
        bk[key] = round(old_val * (1 - alpha) + pnl * alpha, 3)

        # 적중률 업데이트
        old_hr = bk.get("hit_rate", 0.5)
        hit = 1.0 if pnl > 0 else 0.0
        bk["hit_rate"] = round(old_hr * (1 - alpha) + hit * alpha, 3)
        bk["n"] = new_n

        buckets[bucket] = bk

    data["buckets"] = buckets
    data["sample_size"] = sum(b.get("n", 0) for b in buckets.values())
    save_bucket_returns(data)
    logger.info(f"[TradeLearner] Bucket returns 업데이트: {len(closed_trades)}건")


# ─── 트레이드 통계 ─────────────────────────────────────

def calc_trade_stats() -> dict:
    """축적된 트레이드 로그에서 통계 추출"""
    log = load_trade_log()
    if not log:
        return {"total": 0, "message": "데이터 없음"}

    total = len(log)
    wins = [t for t in log if t.get("actual_pnl", 0) > 0]
    losses = [t for t in log if t.get("actual_pnl", 0) <= 0]

    avg_win = sum(t["actual_pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["actual_pnl"] for t in losses) / len(losses) if losses else 0

    by_category = {}
    for t in log:
        cat = t.get("category", "UNKNOWN")
        if cat not in by_category:
            by_category[cat] = {"count": 0, "avg_pnl": 0, "total_pnl": 0}
        by_category[cat]["count"] += 1
        by_category[cat]["total_pnl"] += t.get("actual_pnl", 0)
    for cat in by_category:
        n = by_category[cat]["count"]
        by_category[cat]["avg_pnl"] = round(by_category[cat]["total_pnl"] / n, 2) if n else 0

    by_rr = {"strong": [], "good": [], "marginal": [], "reject": []}
    for t in log:
        rr = t.get("rr_ratio", 0)
        if rr >= 2.0:
            by_rr["strong"].append(t["actual_pnl"])
        elif rr >= 1.5:
            by_rr["good"].append(t["actual_pnl"])
        elif rr >= 1.0:
            by_rr["marginal"].append(t["actual_pnl"])
        else:
            by_rr["reject"].append(t["actual_pnl"])

    rr_stats = {}
    for k, v in by_rr.items():
        if v:
            rr_stats[k] = {"count": len(v), "avg_pnl": round(sum(v)/len(v), 2),
                           "win_rate": round(sum(1 for x in v if x > 0)/len(v)*100, 1)}

    return {
        "total": total,
        "win_rate": round(len(wins) / total * 100, 1),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "by_category": by_category,
        "by_rr_band": rr_stats,
    }


# ─── 콜드스타트 부트스트랩 ─────────────────────────────

def bootstrap_from_signal_log() -> dict:
    """signal_log.json + daily CSV에서 초기 bucket returns 생성"""
    if not SIGNAL_LOG_PATH.exists():
        logger.warning("signal_log.json 없음 → 기본값 사용")
        return {"updated": "", "sample_size": 0, "buckets": dict(DEFAULT_BUCKETS)}

    with open(SIGNAL_LOG_PATH, "r", encoding="utf-8") as f:
        signal_log = json.load(f)

    # 모든 details에서 {code, entry, close, pnl, total_score} 추출
    all_trades = []
    for day in signal_log:
        details = day.get("details", [])
        date_str = day.get("date", "")
        for d in details:
            if d.get("entry") and d.get("close") and d.get("total_score"):
                all_trades.append({
                    "date": date_str,
                    "code": d["code"],
                    "name": d.get("name", ""),
                    "entry": d["entry"],
                    "close": d["close"],
                    "pnl_1d": d.get("pnl", 0),
                    "total_score": d["total_score"],
                    "sources": d.get("sources", []),
                })

    logger.info(f"[Bootstrap] signal_log에서 {len(all_trades)}건 추출")

    # N일 후 수익률 계산 (daily CSV에서)
    for t in all_trades:
        code = t["code"]
        csv_path = _DAILY_DIR / f"{code}.csv"
        if not csv_path.exists():
            continue

        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
            # 컬럼명 정규화
            col_map = {}
            for c in df.columns:
                cl = c.strip()
                if cl in ("날짜", "date", "Date"):
                    col_map[c] = "date"
                elif cl in ("종가", "close", "Close"):
                    col_map[c] = "close"
            df = df.rename(columns=col_map)

            if "date" not in df.columns or "close" not in df.columns:
                continue

            df["date"] = df["date"].astype(str).str.strip()
            # 추천일 찾기
            rec_date = t["date"]
            idx_list = df.index[df["date"] == rec_date].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]
            entry = t["entry"]

            # 3일 후, 5일 후 수익률
            if idx + 3 < len(df):
                c3 = float(df.iloc[idx + 3]["close"])
                t["pnl_3d"] = round((c3 - entry) / entry * 100, 2)
            if idx + 5 < len(df):
                c5 = float(df.iloc[idx + 5]["close"])
                t["pnl_5d"] = round((c5 - entry) / entry * 100, 2)

        except Exception as e:
            logger.debug(f"CSV 로드 실패 {code}: {e}")
            continue

    # 버킷별 집계
    buckets = dict(DEFAULT_BUCKETS)
    for bk_name in buckets:
        bucket_trades = [t for t in all_trades if _score_to_bucket(t["total_score"]) == bk_name]
        if not bucket_trades:
            continue

        n = len(bucket_trades)
        avg_1d = sum(t.get("pnl_1d", 0) for t in bucket_trades) / n
        pnl_3d = [t["pnl_3d"] for t in bucket_trades if "pnl_3d" in t]
        pnl_5d = [t["pnl_5d"] for t in bucket_trades if "pnl_5d" in t]

        avg_3d = sum(pnl_3d) / len(pnl_3d) if pnl_3d else avg_1d * 2
        avg_5d = sum(pnl_5d) / len(pnl_5d) if pnl_5d else avg_1d * 3
        hit_rate = sum(1 for t in bucket_trades if t.get("pnl_1d", 0) > 0) / n

        # 기본값과 블렌딩 (데이터 적으면 기본값 비중 높게)
        confidence = min(1.0, n / 20)  # 20개 이상이면 100% 실데이터
        defaults = DEFAULT_BUCKETS[bk_name]

        buckets[bk_name] = {
            "avg_1d": round(avg_1d * confidence + defaults["avg_1d"] * (1 - confidence), 3),
            "avg_3d": round(avg_3d * confidence + defaults["avg_3d"] * (1 - confidence), 3),
            "avg_5d": round(avg_5d * confidence + defaults["avg_5d"] * (1 - confidence), 3),
            "hit_rate": round(hit_rate * confidence + defaults["hit_rate"] * (1 - confidence), 3),
            "n": n,
        }

    total_n = sum(b["n"] for b in buckets.values())
    result = {"updated": "", "sample_size": total_n, "buckets": buckets}
    save_bucket_returns(result)
    logger.info(f"[Bootstrap] 완료: {total_n}건, 버킷 {len([b for b in buckets.values() if b['n'] > 0])}개 실데이터")
    return result


# ─── CLI ───────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--bootstrap" in sys.argv:
        print("=" * 50)
        print("Trade Learner — 콜드스타트 부트스트랩")
        print("=" * 50)
        result = bootstrap_from_signal_log()
        print(f"\n총 표본: {result['sample_size']}건")
        print("\n점수 구간별 기대수익률:")
        for bk_name, bk in result["buckets"].items():
            marker = "*" if bk["n"] > 0 else " "
            print(f"  {marker} {bk_name:12s}: "
                  f"1D {bk['avg_1d']:+5.2f}% | "
                  f"3D {bk['avg_3d']:+5.2f}% | "
                  f"5D {bk['avg_5d']:+5.2f}% | "
                  f"적중 {bk['hit_rate']*100:4.1f}% | "
                  f"n={bk['n']}")
        print(f"\n저장: {BUCKET_PATH}")

    elif "--stats" in sys.argv:
        print("=" * 50)
        print("Trade Learner — 트레이드 통계")
        print("=" * 50)
        stats = calc_trade_stats()
        if stats["total"] == 0:
            print("아직 트레이드 기록 없음")
            return
        print(f"총 트레이드: {stats['total']}건")
        print(f"승률: {stats['win_rate']}%")
        print(f"평균 수익(승): +{stats['avg_win']}%")
        print(f"평균 손실(패): {stats['avg_loss']}%")
        print(f"\n카테고리별:")
        for cat, info in stats["by_category"].items():
            print(f"  {cat}: {info['count']}건, avg {info['avg_pnl']:+.2f}%")
        print(f"\nR:R 밴드별:")
        for band, info in stats.get("by_rr_band", {}).items():
            print(f"  {band}: {info['count']}건, avg {info['avg_pnl']:+.2f}%, 승률 {info['win_rate']}%")

    else:
        print("Usage:")
        print("  python data/trade_learner.py --bootstrap   # 콜드스타트 부트스트랩")
        print("  python data/trade_learner.py --stats       # 트레이드 통계")


if __name__ == "__main__":
    main()
