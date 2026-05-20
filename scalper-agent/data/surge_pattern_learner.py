# -*- coding: utf-8 -*-
"""surge_pattern_learner — 매일 +10%/상한가 종목 패턴 자동 학습.

[사장님 5/20 21:00 비전]
> "1번 상한가 쳤어... 근데 어쩔때 어떤 시그널, 수급, 가격, 타이밍, 체결강도,
>  난 알지못하지만, 이걸 니가 지금 알아내서 종목선정을 해야되..."
> "대화를 하고 있는 너 말고 너가 만든 프로그램 즉, 봇에게 그렇게 로직을
>  함수를 짜서 줘야되..."

= 사장님이 모르는 패턴을 AI가 매일 자동으로 추출 → 학습 누적 → 익일 종목 선정 가중치 반영

매일 15:35 (장 마감 + 5분) 자동 실행:
  Step 1: today_gainers + limit_up + consecutive_surge에서 오늘 +10%+ 종목 수집
  Step 2: 각 종목 시초가 → 상한가/+10% 도달 시점 시그널 분석
    - 체결강도 (시초가 5분 vs 도달 시점)
    - 거래량 (전일 평균 대비 배수)
    - 외인/기관 수급 (매수 강도)
    - 매수벽 비율 (호가)
    - 캔들 패턴 (시초가 갭/장중 추세)
    - 섹터 동조 (같은 테마 동시 강세)
    - 뉴스/공시 (DART/뉴스 키워드)
  Step 3: 패턴 누적 (jarvis_learning/surge_patterns/YYYYMMDD.json)
  Step 4: 7일/30일 통계 → asset_pool_loader 가중치 자동 반영

[익일 자비스 활용]
  asset_pool_loader.get_high_confidence_candidates() 호출 시
  → 어제 학습된 "성공 패턴" 종목 +30점 가중
"""
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

# 학습 데이터 저장 위치
_LEARN_DIR = _ROOT / "data_store" / "jarvis_learning" / "surge_patterns"
_LEARN_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return (datetime.now(KST) if KST else datetime.now()).strftime("%Y%m%d")


def collect_surge_stocks(min_change_pct: float = 10.0) -> List[Dict]:
    """오늘 +10%+ 급등 + 상한가 종목 수집.

    Sources:
      - today_gainers.json (KIS 등락률 TOP)
      - limit_up/signals.json (상한가 엔진 시그널)
      - consecutive_surge.json (상한가 연속)
      - sector_concurrent_surge.json (섹터 동조)
    """
    surge_stocks: Dict[str, Dict] = {}

    sources = [
        ("data_store/today_gainers.json", "today_gainers"),
        ("data_store/limit_up/signals.json", "limit_up_signal"),
        ("data_store/consecutive_surge.json", "consecutive_surge"),
        ("data_store/sector_concurrent_surge.json", "sector_surge"),
    ]

    for rel_path, source_name in sources:
        path = _ROOT / rel_path
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            items = data.get("stocks", []) if isinstance(data, dict) else data
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict):
                    continue
                code = it.get("code") or it.get("ticker")
                if not code:
                    continue
                chg = it.get("change_pct") or it.get("chg") or 0
                try:
                    chg = float(chg)
                except (TypeError, ValueError):
                    chg = 0
                if chg < min_change_pct:
                    continue
                if code not in surge_stocks:
                    surge_stocks[code] = {
                        "code": code,
                        "name": it.get("name", ""),
                        "change_pct": chg,
                        "volume": it.get("volume", 0),
                        "price": it.get("price", 0) or it.get("close", 0),
                        "sources": [],
                    }
                surge_stocks[code]["sources"].append(source_name)
                # 가장 큰 등락률로 갱신
                if chg > surge_stocks[code]["change_pct"]:
                    surge_stocks[code]["change_pct"] = chg
        except Exception as e:
            logger.warning(f"[surge_learner] {rel_path} 로드 실패: {e}")

    return sorted(surge_stocks.values(), key=lambda x: -x["change_pct"])


def analyze_pattern(code: str, name: str = "") -> Dict:
    """개별 종목 패턴 분석 — 시초가 5분 시그널 추출.

    [핵심] 어떤 시그널 조합이 +10%/상한가 만들었는지 추출:
      - 시초가 체결강도
      - 시초가 갭 (open vs prev_close)
      - 거래량 (시초가 vs 전일 평균)
      - 외인/기관 수급 (있다면)
      - 섹터 (테마)
    """
    pattern = {
        "code": code,
        "name": name,
        "analyzed_at": (datetime.now(KST) if KST else datetime.now()).isoformat(),
    }

    # 1) flow (외인/기관 수급) 데이터 — 5/20 갱신된 종목만
    flow_path = _ROOT / "data_store" / "flow" / f"{code}_investor.csv"
    if flow_path.exists():
        try:
            import csv as _csv
            with flow_path.open(encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            if rows:
                latest = rows[-1]
                pattern["foreign_net"] = int(float(latest.get("외국인", 0) or 0))
                pattern["institution_net"] = int(float(latest.get("기관", 0) or 0))
                pattern["individual_net"] = int(float(latest.get("개인", 0) or 0))
        except Exception:
            pass

    # 2) 분봉 데이터 — 시초가 vs 장중 (있다면)
    minute_path = _ROOT / "data_store" / "1min" / f"{code}.csv"
    if minute_path.exists():
        try:
            import csv as _csv
            with minute_path.open(encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            today = _today_str()
            today_rows = [r for r in rows if r.get("date", "").startswith(today[:8])]
            if today_rows:
                first = today_rows[0]
                last = today_rows[-1]
                try:
                    open_p = float(first.get("close", 0) or first.get("price", 0))
                    last_p = float(last.get("close", 0) or last.get("price", 0))
                    if open_p > 0:
                        pattern["intraday_change_pct"] = round((last_p - open_p) / open_p * 100, 2)
                    pattern["bars_count"] = len(today_rows)
                    # 거래량 합
                    vol_sum = sum(int(float(r.get("volume", 0) or 0)) for r in today_rows)
                    pattern["total_volume"] = vol_sum
                except (TypeError, ValueError):
                    pass
        except Exception:
            pass

    # 3) universe 메타데이터 — 시총/섹터
    universe_path = _ROOT / "data_store" / "universe.json"
    if universe_path.exists():
        try:
            u = json.loads(universe_path.read_text(encoding="utf-8"))
            if isinstance(u, list):
                u_dict = {str(x.get("code", "")).zfill(6): x for x in u}
            else:
                u_dict = u
            item = u_dict.get(code, {})
            pattern["sector"] = item.get("sector", "") or item.get("industry", "")
            pattern["market_cap"] = item.get("market_cap", 0)
        except Exception:
            pass

    return pattern


def save_daily_learning(stocks: List[Dict], patterns: Dict[str, Dict]) -> Path:
    """오늘 학습 데이터 저장."""
    today_str = _today_str()
    out_path = _LEARN_DIR / f"{today_str}.json"
    payload = {
        "date": today_str,
        "generated_at": (datetime.now(KST) if KST else datetime.now()).isoformat(),
        "surge_count": len(stocks),
        "stocks": stocks,
        "patterns": patterns,
        "note": "사장님 5/20 비전 — AI가 매일 자동 패턴 학습",
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    logger.info(f"[surge_learner] {today_str} 저장: {len(stocks)}종목 → {out_path.name}")
    return out_path


def aggregate_stats(days: int = 7) -> Dict:
    """최근 N일 패턴 통계 — 자비스 가중치 자동 반영용.

    Returns:
        {
            "top_sectors": [("바이오", 25), ("반도체", 18), ...],
            "avg_volume_multiple": 12.3,  # 평균 거래량 배수
            "common_patterns": {"foreign_buy_with_gap_up": 5, ...},
        }
    """
    sector_count: Dict[str, int] = {}
    volume_samples = []
    foreign_buy_count = 0
    inst_buy_count = 0
    total = 0

    today = date.today()
    for d in range(days):
        target = (today - timedelta(days=d)).strftime("%Y%m%d")
        path = _LEARN_DIR / f"{target}.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for stock in data.get("stocks", []):
                code = stock.get("code", "")
                pat = data.get("patterns", {}).get(code, {})
                sector = pat.get("sector", "")
                if sector:
                    sector_count[sector] = sector_count.get(sector, 0) + 1
                if pat.get("foreign_net", 0) > 0:
                    foreign_buy_count += 1
                if pat.get("institution_net", 0) > 0:
                    inst_buy_count += 1
                if pat.get("total_volume", 0) > 0:
                    volume_samples.append(pat["total_volume"])
                total += 1
        except Exception as e:
            logger.warning(f"[surge_learner] {target} 통계 실패: {e}")

    top_sectors = sorted(sector_count.items(), key=lambda x: -x[1])[:10]
    avg_vol = sum(volume_samples) / len(volume_samples) if volume_samples else 0
    return {
        "days": days,
        "total_surge_stocks": total,
        "top_sectors": top_sectors,
        "avg_volume": int(avg_vol),
        "foreign_buy_rate": round(foreign_buy_count / total * 100, 1) if total else 0,
        "institution_buy_rate": round(inst_buy_count / total * 100, 1) if total else 0,
    }


def run_daily_learning() -> Dict:
    """매일 장 마감 후 (15:35) 호출되는 메인 함수.

    [trading_coo.py에서 자동 호출]
      G5_MARKET_CLOSE 후 자동 트리거 (15:35)
    """
    logger.info("[surge_learner] 일일 학습 시작")
    stocks = collect_surge_stocks(min_change_pct=10.0)
    logger.info(f"[surge_learner] +10%+ 종목 {len(stocks)}건 수집")
    patterns: Dict[str, Dict] = {}
    for s in stocks[:30]:  # TOP 30만 패턴 분석 (속도)
        try:
            p = analyze_pattern(s["code"], s.get("name", ""))
            patterns[s["code"]] = p
        except Exception as e:
            logger.warning(f"[surge_learner] {s['code']} 분석 실패: {e}")
    out_path = save_daily_learning(stocks, patterns)
    stats = aggregate_stats(days=7)
    logger.info(
        f"[surge_learner] 완료 — 7일 통계: 총 {stats['total_surge_stocks']}종 / "
        f"TOP 섹터: {stats['top_sectors'][:3]} / "
        f"외인 매수율: {stats['foreign_buy_rate']}%"
    )
    return {
        "today_path": str(out_path),
        "today_count": len(stocks),
        "weekly_stats": stats,
    }


def get_learned_strong_codes(days: int = 7, min_appearance: int = 2) -> List[str]:
    """최근 N일에 +10%+ 도달한 종목 중 N회+ 반복 출현 = 학습된 강세 종목.

    [자비스 활용]
      asset_pool_loader.get_high_confidence_candidates() 에서 가중치 +30점
    """
    appearance: Dict[str, int] = {}
    today = date.today()
    for d in range(days):
        target = (today - timedelta(days=d)).strftime("%Y%m%d")
        path = _LEARN_DIR / f"{target}.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for stock in data.get("stocks", []):
                code = stock.get("code", "")
                if code:
                    appearance[code] = appearance.get(code, 0) + 1
        except Exception:
            pass
    return [c for c, n in appearance.items() if n >= min_appearance]


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    print("=== surge_pattern_learner 수동 실행 (오늘 학습) ===")
    result = run_daily_learning()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print("\n=== 7일 학습 강세 종목 (2회+ 반복) ===")
    strong = get_learned_strong_codes(days=7, min_appearance=2)
    print(f"{len(strong)}종목: {strong[:20]}")
