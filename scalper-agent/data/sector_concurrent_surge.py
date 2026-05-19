# -*- coding: utf-8 -*-
"""섹터 동조 카운터 (Sector Concurrent Surge Detector)
=====================================================
같은 섹터에서 5개+ 종목이 동시에 +10%+ 급등 시 정책/이벤트 신호로 판단.

5/18 발견 패턴:
  - 증권주 8개 동시 +20~30% (SK/한화/NH/미래/유진/현대차/다올/유안타)
  - 바이오 7개 동시 +15~30%
  - 반도체장비 3개

이런 패턴은 개별 종목이 아니라 **섹터/테마 전체 호재** 신호.
사전 포착 후 사장님 즉시 알림 + 섹터 전체 풀 매수 후보 등록.

사용:
  python -X utf8 data/sector_concurrent_surge.py            # 실시간 KIS 스캔
  python -X utf8 data/sector_concurrent_surge.py --file PATH # JSON 파일 분석 (백테스트)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_store"
UNIVERSE_PATH = DATA_DIR / "universe.json"

logger = logging.getLogger("BH.SectorSurge")

# ── 임계 ──
MIN_SURGE_PCT = 10.0      # +10%+ 만 카운트
MIN_GROUP_SIZE = 4         # 같은 섹터 4개+ 동시 = 신호 (보수: 5개)
DEFAULT_TOP_N = 50         # 등락률 TOP N 스캔


def _load_universe() -> dict:
    if not UNIVERSE_PATH.exists():
        logger.warning("universe.json 없음")
        return {}
    return json.loads(UNIVERSE_PATH.read_text("utf-8"))


def detect_from_list(items: list[dict], min_group: int = MIN_GROUP_SIZE) -> dict:
    """등락률 TOP 리스트에서 섹터 동조 감지.

    Args:
        items: [{"code","name","sector","change"(or change_rate),...}, ...]
        min_group: 그룹 최소 크기

    Returns:
        {
            "timestamp": str,
            "total_surge": int (10%+ 종목 수),
            "groups": [
                {"sector": str, "count": int, "members": [...]}
            ],
            "alert_groups": [...]  # min_group 이상
        }
    """
    surge = []
    for x in items:
        ch = x.get("change", x.get("change_rate", 0))
        if ch >= MIN_SURGE_PCT:
            surge.append(x)

    by_sector: dict[str, list] = defaultdict(list)
    for x in surge:
        sec = x.get("sector", "") or "기타"
        by_sector[sec].append({
            "code": x.get("code", ""),
            "name": x.get("name", ""),
            "change": x.get("change", x.get("change_rate", 0)),
            "cap": x.get("cap", x.get("market_cap", 0)),
        })

    groups = sorted(
        [{"sector": s, "count": len(m), "members": m} for s, m in by_sector.items()],
        key=lambda x: -x["count"],
    )
    alert_groups = [g for g in groups if g["count"] >= min_group]

    return {
        "timestamp": datetime.now().isoformat(),
        "total_surge": len(surge),
        "min_group_threshold": min_group,
        "groups": groups,
        "alert_groups": alert_groups,
    }


def detect_live(top_n: int = DEFAULT_TOP_N, min_group: int = MIN_GROUP_SIZE) -> dict:
    """KIS API 실시간 스캔 + 유니버스 섹터 매핑."""
    from bot.kis_trader import KISTrader
    trader = KISTrader()
    flu = trader.fetch_ranking_fluctuation(market="J", top_n=top_n)

    universe = _load_universe()
    for x in flu:
        info = universe.get(x["code"], {})
        x["sector"] = info.get("sector", "")
        x["cap"] = info.get("cap_억", 0)

    result = detect_from_list(flu, min_group=min_group)

    out_path = DATA_DIR / "sector_concurrent_surge.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def print_report(result: dict):
    print()
    print("=" * 80)
    print(f"섹터 동조 카운터 — {result['timestamp']}")
    print("=" * 80)
    print(f"총 +{MIN_SURGE_PCT:.0f}%+ 종목: {result['total_surge']}건")
    print(f"동조 임계: {result['min_group_threshold']}개+")

    alerts = result.get("alert_groups", [])
    if alerts:
        print()
        print(f"🚨 동조 그룹 감지: {len(alerts)}건")
        for g in alerts:
            print(f"\n  ★ {g['sector']} 섹터 — {g['count']}개 동시 +10%+")
            for m in g["members"]:
                print(f"      {m['name']:<14} ({m['code']}) +{m['change']:.2f}% | 시총 {m['cap']:,}억")
    else:
        print(f"\n   동조 그룹 없음 (모든 섹터 < {result['min_group_threshold']}개)")

    print()
    print("📊 섹터별 전체 분포:")
    for g in result["groups"][:10]:
        print(f"   {g['sector']:<14} {g['count']}건")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="JSON 파일 입력 (백테스트)")
    parser.add_argument("--min-group", type=int, default=MIN_GROUP_SIZE)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    args = parser.parse_args()

    if args.file:
        items = json.loads(Path(args.file).read_text("utf-8"))
        if isinstance(items, dict):
            # snap_*.json 포맷
            items = items.get("fluctuation_top", items.get("data", []))
        result = detect_from_list(items, min_group=args.min_group)
    else:
        result = detect_live(top_n=args.top_n, min_group=args.min_group)

    print_report(result)
