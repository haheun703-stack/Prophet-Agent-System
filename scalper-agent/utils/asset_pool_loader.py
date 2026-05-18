# -*- coding: utf-8 -*-
"""4종 자산 통합 로더 (5/18 사장님 두 번 지적 후 작성)

사장님 지적 사항:
  1. "우리가 만든 상한가 엔진이 있잖아" (5/18 15:30)
  2. "NXT 야간, 매매포인트, 상한가 엔진, 매집합류 시그널 다 활용" (5/18 15:35)

4종 카테고리 자산 통합:
  ① 상한가 엔진 (trigger 6건 + watchlist 17건)
  ② NXT 야간 (nxt_top5_picks + nxt_eligible)
  ③ 매매포인트 (premium_levels 통합 — 종목별 진입가 자동 계산)
  ④ 매집합류 (massive_dual_buy + oneshot_stealth + accumulation_radar + foreign_accumulation + nationality_signal)

5/19 D-1 + 5/20 D-Day 자비스 v3.0 매수 후보 풀의 핵심.

5/18 활용 결과 (참고):
  - 미활용으로 +47.7만원 기회 놓침
  - 가온전선 +13.44% (상한가 watchlist) / HD현대에너지 +9.50% (상한가 trigger)
  - 제주반도체 +12.64% (어제 massive_dual_buy)
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.AssetPool")

DATA_DIR = _ROOT / "data_store"


def _load_json(filename: str) -> Optional[Dict]:
    """data_store/ JSON 파일 안전 로드."""
    path = DATA_DIR / filename
    if not path.exists():
        logger.debug(f"파일 없음: {filename}")
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"로드 실패 {filename}: {e}")
        return None


# ─────────────────────────────────────────────────────
# 카테고리 ① 상한가 엔진
# ─────────────────────────────────────────────────────
def load_limit_up_triggers() -> List[Dict]:
    """상한가 엔진 트리거 (즉시 진입가 + 목표가 계산된 종목)."""
    data = _load_json("limit_up/signals.json")
    if not data:
        return []
    triggers = data if isinstance(data, list) else data.get("triggers", [])
    return triggers


def load_limit_up_watchlist() -> List[Dict]:
    """상한가 엔진 감시풀 (눌림목 진입 대기 종목)."""
    data = _load_json("limit_up/watchlist.json")
    if not data:
        return []
    return data if isinstance(data, list) else data.get("watchlist", [])


# ─────────────────────────────────────────────────────
# 카테고리 ② NXT 야간
# ─────────────────────────────────────────────────────
def load_nxt_top5_picks() -> Dict:
    """NXT 야간 TOP 5 픽."""
    return _load_json("nxt_top5_picks.json") or {}


def load_nxt_eligible() -> List[str]:
    """NXT 매매 가능 종목 코드 리스트."""
    data = _load_json("nxt_eligible.json")
    if not data:
        return []
    if isinstance(data, dict):
        stocks = data.get("stocks", [])
        if isinstance(stocks, list):
            return [s if isinstance(s, str) else s.get("code", "") for s in stocks if s]
    return []


# ─────────────────────────────────────────────────────
# 카테고리 ③ 매매포인트 (premium_levels)
# ─────────────────────────────────────────────────────
def get_premium_levels(code: str, name: str = "", current_price: int = 0) -> Optional[Dict]:
    """종목별 정밀 매매 레벨 (현재가, 손절가, 목표가)."""
    try:
        from strategies.premium_levels import format_telegram_levels
        line = format_telegram_levels(code, name, current_price)
        return {"code": code, "name": name, "levels_line": line} if line else None
    except Exception as e:
        logger.debug(f"premium_levels 실패 {code}: {e}")
        return None


# ─────────────────────────────────────────────────────
# 카테고리 ④ 매집합류 시그널
# ─────────────────────────────────────────────────────
def load_massive_dual_buy() -> List[Dict]:
    """외인+기관 대량 쌍매수 종목."""
    data = _load_json("massive_dual_buy.json")
    if not data:
        return []
    return data.get("stocks", []) if isinstance(data, dict) else data


def load_oneshot_stealth() -> List[Dict]:
    """원샷 쌍매수 잠복 종목."""
    data = _load_json("oneshot_stealth.json")
    if not data:
        return []
    return data.get("stealth", []) if isinstance(data, dict) else data


def load_foreign_accumulation() -> List[Dict]:
    """외인 매집 종목."""
    data = _load_json("foreign_accumulation.json")
    if not data:
        return []
    return data.get("candidates", []) if isinstance(data, dict) else data


def load_accumulation_radar() -> List[Dict]:
    """매집 레이더 종목."""
    data = _load_json("accumulation_radar.json")
    if not data:
        return []
    return data.get("stocks", []) if isinstance(data, dict) else data


# ─────────────────────────────────────────────────────
# 통합 매수 후보 풀
# ─────────────────────────────────────────────────────
def collect_all_candidates() -> Dict[str, List[Dict]]:
    """4종 카테고리 모든 자산의 매수 후보 종목 통합.

    Returns:
        {
            "limit_up_triggers": [...],     # ① 즉시 진입가
            "limit_up_watchlist": [...],    # ② 눌림목 대기
            "nxt_top5": {...},              # NXT TOP 5
            "nxt_eligible": [...],          # NXT 가능 종목
            "massive_dual_buy": [...],      # 외인+기관 쌍매수
            "oneshot_stealth": [...],       # 원샷 잠복
            "foreign_accumulation": [...],  # 외인 매집
            "accumulation_radar": [...],    # 매집 레이더
            "total_unique_codes": int,      # 중복 제거 종목 수
        }
    """
    result = {
        "limit_up_triggers": load_limit_up_triggers(),
        "limit_up_watchlist": load_limit_up_watchlist(),
        "nxt_top5": load_nxt_top5_picks(),
        "nxt_eligible": load_nxt_eligible(),
        "massive_dual_buy": load_massive_dual_buy(),
        "oneshot_stealth": load_oneshot_stealth(),
        "foreign_accumulation": load_foreign_accumulation(),
        "accumulation_radar": load_accumulation_radar(),
    }

    # 중복 제거 종목 코드 집계
    all_codes: Set[str] = set()
    for key, items in result.items():
        if key == "nxt_top5":
            continue
        for item in items:
            if isinstance(item, dict):
                code = item.get("code") or item.get("ticker")
                if code:
                    all_codes.add(code)
            elif isinstance(item, str):
                all_codes.add(item)
    result["total_unique_codes"] = len(all_codes)
    return result


def get_candidate_source_map() -> Dict[str, List[str]]:
    """각 종목이 어떤 자산 소스에서 발굴됐는지 매핑.

    Returns:
        {code: [source1, source2, ...]}
        예: {"080220": ["massive_dual_buy", "accumulation_radar"]}
    """
    source_map: Dict[str, List[str]] = {}

    def _add(items: List, source: str):
        for item in items:
            code = None
            if isinstance(item, dict):
                code = item.get("code") or item.get("ticker")
            elif isinstance(item, str):
                code = item
            if code:
                source_map.setdefault(code, []).append(source)

    _add(load_limit_up_triggers(), "limit_up_trigger")
    _add(load_limit_up_watchlist(), "limit_up_watchlist")
    _add(load_nxt_eligible(), "nxt_eligible")
    _add(load_massive_dual_buy(), "massive_dual_buy")
    _add(load_oneshot_stealth(), "oneshot_stealth")
    _add(load_foreign_accumulation(), "foreign_accumulation")
    _add(load_accumulation_radar(), "accumulation_radar")

    return source_map


def get_high_confidence_candidates() -> List[str]:
    """다중 자산 소스에서 동시 발견된 고신뢰 종목 (2개+ 소스)."""
    source_map = get_candidate_source_map()
    return [code for code, sources in source_map.items() if len(sources) >= 2]


if __name__ == "__main__":
    print("=== asset_pool_loader 자가 진단 ===")
    print()
    pools = collect_all_candidates()
    print(f"📊 카테고리별 종목 수:")
    print(f"  ① 상한가 trigger: {len(pools['limit_up_triggers'])}건")
    print(f"  ② 상한가 watchlist: {len(pools['limit_up_watchlist'])}건")
    print(f"  ③ NXT eligible: {len(pools['nxt_eligible'])}종목")
    print(f"  ④ massive_dual_buy: {len(pools['massive_dual_buy'])}건")
    print(f"  ⑤ oneshot_stealth: {len(pools['oneshot_stealth'])}건")
    print(f"  ⑥ foreign_accumulation: {len(pools['foreign_accumulation'])}건")
    print(f"  ⑦ accumulation_radar: {len(pools['accumulation_radar'])}건")
    print(f"  ★ 중복 제거 총 종목 수: {pools['total_unique_codes']}")
    print()

    high_conf = get_high_confidence_candidates()
    print(f"🎯 고신뢰 종목 (2개+ 자산 일치): {len(high_conf)}종목")
    source_map = get_candidate_source_map()
    for code in high_conf[:15]:
        print(f"  {code} ← {source_map[code]}")
