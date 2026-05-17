# -*- coding: utf-8 -*-
"""ETF Step C-3 + C-4 통합 — TOP 20 ETF 비중 일괄 수집 + 종목 점수화 → TOP 30 주도주.

흐름:
  1) leader_picks_<date>.json (Step B 결과) → TOP 20 ETF ticker + score 추출
  2) 네이버 etfAnalysis API 일괄 호출 → 각 ETF의 비중 TOP 10
  3) 종목별 leader_score 합산: Σ(ETF_score × weight% × rank_factor)
     - rank_factor: TOP 3 = 1.2x, TOP 5 = 1.1x, 그 외 1.0x
  4) TOP 30 주도주 산출 + 다중 ETF 포함 종목 식별

산출:
  data_store/etf_backtest/holdings_<date>/{ticker}.json       — 원자료
  data_store/etf_backtest/leader_stocks_<date>.json           — TOP 30
"""
import sys
import io
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scalper-agent"))

from utils.etf_holdings_collector import collect_holdings_bulk  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ETFLeaderC34")

# ── 경로 ───────────────────────────────────────────────
DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"

# ── 점수화 파라미터 ─────────────────────────────────────
RANK_FACTOR_TOP3 = 1.2
RANK_FACTOR_TOP5 = 1.1
RANK_FACTOR_OTHER = 1.0
TOP_STOCKS_N = 30


def load_leader_picks(date_str: str) -> dict:
    """Step B 산출 leader_picks_<date>.json 로드."""
    path = DATA_DIR / f"leader_picks_{date_str}.json"
    if not path.exists():
        raise FileNotFoundError(f"leader_picks 파일 없음: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def rank_factor(seq: int) -> float:
    """ETF 내 순위(seq)별 가중치 보너스."""
    if seq <= 3:
        return RANK_FACTOR_TOP3
    if seq <= 5:
        return RANK_FACTOR_TOP5
    return RANK_FACTOR_OTHER


def score_stocks(top_picks: list, holdings_by_ticker: dict) -> list:
    """종목별 leader_score 합산 + 포함 ETF 추적.

    Args:
        top_picks: [{ticker, name, score, ...}]  Step B의 TOP 20 ETF
        holdings_by_ticker: {etf_ticker: collector 결과}

    Returns:
        [{stock_ticker, stock_name, leader_score, etf_count, top_weight,
          top_etf_name, included_etfs: [{etf_ticker, etf_name, etf_score,
          weight_pct, seq}]}]
    """
    stock_scores: dict = defaultdict(lambda: {
        "stock_ticker": "",
        "stock_name": "",
        "leader_score": 0.0,
        "etf_count": 0,
        "top_weight": 0.0,
        "top_etf_name": "",
        "top_etf_score": 0,
        "included_etfs": [],
    })

    for etf in top_picks:
        etf_ticker = etf["ticker"]
        etf_name = etf["name"]
        etf_score = etf.get("score", 0)

        data = holdings_by_ticker.get(etf_ticker)
        if not data or not data.get("holdings"):
            continue

        for h in data["holdings"]:
            stock_ticker = h["ticker"]
            stock_name = h["name"]
            weight = h["weight_pct"]
            seq = h["seq"]

            contribution = etf_score * (weight / 100.0) * rank_factor(seq)

            s = stock_scores[stock_ticker]
            s["stock_ticker"] = stock_ticker
            s["stock_name"] = stock_name
            s["leader_score"] += contribution
            s["etf_count"] += 1
            s["included_etfs"].append({
                "etf_ticker": etf_ticker,
                "etf_name": etf_name,
                "etf_score": etf_score,
                "weight_pct": weight,
                "seq": seq,
                "contribution": round(contribution, 3),
            })
            # 최대 비중 ETF 추적
            if weight > s["top_weight"]:
                s["top_weight"] = weight
                s["top_etf_name"] = etf_name
                s["top_etf_score"] = etf_score

    # 정렬: leader_score 내림차순
    ranked = sorted(stock_scores.values(), key=lambda x: x["leader_score"], reverse=True)
    # 소수점 정리
    for s in ranked:
        s["leader_score"] = round(s["leader_score"], 3)
        s["top_weight"] = round(s["top_weight"], 2)
    return ranked


def print_report(top_stocks: list, top_picks: list, success_count: int, total_etfs: int):
    """결과 콘솔 리포트."""
    print(f"\n{'='*80}")
    print(f"  📊 ETF Step C-3+4 — 종목 점수화 결과")
    print(f"{'='*80}")
    print(f"  ETF 비중 수집: {success_count}/{total_etfs}개 성공")
    print(f"  추출 종목 수: {len(top_stocks)}개 (TOP {TOP_STOCKS_N} 표시)")
    print()
    print(f"  {'순위':<4}{'종목코드':<8}{'종목명':<22}{'점수':>8}{'ETF수':>5}{'TOP비중':>8}  {'최대보유 ETF':<28}")
    print(f"  {'-'*78}")
    for i, s in enumerate(top_stocks[:TOP_STOCKS_N], 1):
        print(
            f"  {i:<4}{s['stock_ticker']:<8}{s['stock_name'][:20]:<22}"
            f"{s['leader_score']:>8.2f}{s['etf_count']:>5}{s['top_weight']:>7.2f}%  "
            f"{s['top_etf_name'][:26]:<28}"
        )

    # 다중 ETF 포함 종목 (집중도 분석)
    multi = [s for s in top_stocks if s["etf_count"] >= 3]
    print(f"\n  [다중 ETF (3개 이상) 포함 종목 {len(multi)}개 — 강한 주도주 후보]")
    for s in multi[:10]:
        etfs = ", ".join(e["etf_name"][:12] for e in s["included_etfs"][:5])
        print(f"    [{s['stock_ticker']}] {s['stock_name']:<18s} {s['etf_count']}개: {etfs}")


def main(base_date: str = "20260515"):
    started = datetime.now()
    print(f"\n{'='*80}")
    print(f"  ETF Step C — TOP 20 ETF 비중 일괄 수집 + 종목 점수화")
    print(f"  기준일: {base_date}")
    print(f"{'='*80}\n")

    # 1) Step B 결과 로드
    leader = load_leader_picks(base_date)
    top_picks = leader.get("top_picks", [])
    logger.info(f"Step B TOP 20 ETF 로드: {len(top_picks)}개")

    # 2) TOP 20 ETF ticker 추출 → 일괄 수집
    tickers = [p["ticker"] for p in top_picks]
    holdings_dir = DATA_DIR / f"holdings_{base_date}"

    logger.info(f"네이버 etfAnalysis 일괄 호출 ({len(tickers)}개, sleep 0.3s)...")
    results = collect_holdings_bulk(tickers, sleep_sec=0.3, save_dir=holdings_dir)
    success_count = sum(1 for v in results.values() if v is not None and v.get("holdings"))
    logger.info(f"수집 완료: {success_count}/{len(tickers)} 성공 (저장 {holdings_dir})")

    # 3) 종목 점수화
    ranked = score_stocks(top_picks, results)
    logger.info(f"종목 점수화: {len(ranked)}개 종목 산출")

    # 4) TOP 30 저장
    elapsed = round((datetime.now() - started).total_seconds(), 1)
    now_iso = datetime.now(KST).isoformat() if KST else datetime.now().isoformat()
    final = {
        "executed_at": now_iso,
        "elapsed_sec": elapsed,
        "base_date": base_date,
        "source": "naver_m_stock_etfAnalysis",
        "parameters": {
            "top_etfs_used": len(top_picks),
            "etfs_succeeded": success_count,
            "rank_factor_top3": RANK_FACTOR_TOP3,
            "rank_factor_top5": RANK_FACTOR_TOP5,
            "top_stocks_n": TOP_STOCKS_N,
        },
        "stats": {
            "total_stocks_found": len(ranked),
            "top_stocks_count": min(TOP_STOCKS_N, len(ranked)),
            "multi_etf_stocks_3plus": sum(1 for s in ranked if s["etf_count"] >= 3),
        },
        "top_stocks": ranked[:TOP_STOCKS_N],
        "all_stocks": ranked,
    }

    out_path = DATA_DIR / f"leader_stocks_{base_date}.json"
    out_path.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    print_report(final["top_stocks"], top_picks, success_count, len(tickers))
    print(f"\n  💾 저장: {out_path}")
    print(f"  ⏱  소요: {elapsed}초")

    return final


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "20260515"
    main(date)
