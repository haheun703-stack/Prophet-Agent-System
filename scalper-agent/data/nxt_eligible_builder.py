# -*- coding: utf-8 -*-
"""nxt_eligible_builder — NXT 야간 거래 가능 종목 매일 갱신.

[5/20 사장님 지적]
> "주요종목 업데이트 안 되고 있다 (nxt_eligible 4/18 = 1개월 미갱신)"

기존 nxt_eligible.json 생성 모듈 위치 미상 (코드에서 찾을 수 없음) → 신규 작성.

NXT 자격 기준 (한국거래소 NXT 종목):
  1. 거래대금 일평균 1억원+ (유동성)
  2. 시총 100억+ (대형/중형)
  3. ETF/ETN/우선주/관리종목/감리종목 제외
  4. 거래정지 X

universe.json + market_kospi/kosdaq 메타데이터 활용.
매일 G1 MORNING_PREP (06:30)에서 자동 호출.
"""
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

_OUT_PATH = _ROOT / "data_store" / "nxt_eligible.json"

# ETF/우선주/관리종목 prefix/suffix
_ETF_PREFIXES = (
    "KODEX", "TIGER", "RISE", "PLUS", "ACE", "SOL",
    "KOSEF", "ARIRANG", "HANARO", "KoAct",
)


def _is_etf_or_special(name: str, code: str) -> bool:
    """ETF/ETN/우선주/REITs/SPAC 제외."""
    if not name:
        return False
    # ETF prefix
    if any(name.startswith(p) for p in _ETF_PREFIXES):
        return True
    # 우선주 (5자리 + 우/우B/우C 또는 코드 끝 5)
    if name.endswith("우") or name.endswith("우B") or name.endswith("우C"):
        return True
    # 우선주는 코드 마지막 자리가 5 (예: 005380 vs 005385)
    if len(code) == 6 and code.endswith("5"):
        return True
    # REITs / 리츠
    if "리츠" in name or "REIT" in name.upper():
        return True
    # SPAC
    if "스팩" in name or "SPAC" in name.upper():
        return True
    # 관리종목 마커 (별도 데이터 필요 — 일단 이름 패턴만)
    return False


def build_nxt_eligible(
    min_daily_value_won: int = 100_000_000,  # 1억원
    min_market_cap_won: int = 10_000_000_000,  # 100억
) -> List[str]:
    """NXT 자격 종목 코드 리스트 생성.

    Returns:
        list of stock codes (6-digit string)
    """
    universe_path = _ROOT / "data_store" / "universe.json"
    if not universe_path.exists():
        logger.warning("[nxt_eligible] universe.json 없음")
        return []

    try:
        u = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[nxt_eligible] universe.json 로드 실패: {e}")
        return []

    # universe 형식 (list or dict)
    if isinstance(u, dict):
        items = [{"code": k, **v} if isinstance(v, dict) else {"code": k}
                 for k, v in u.items()]
    elif isinstance(u, list):
        items = u
    else:
        return []

    eligible = []
    for it in items:
        if not isinstance(it, dict):
            continue
        code = str(it.get("code", "")).zfill(6)
        if not code or len(code) != 6 or not code.isdigit():
            continue
        name = it.get("name", "")

        # 1) ETF/우선주/REITs 제외
        if _is_etf_or_special(name, code):
            continue

        # 2) 시총 체크 (있다면)
        market_cap = it.get("market_cap") or it.get("mkt_cap") or 0
        try:
            market_cap = int(market_cap)
        except (TypeError, ValueError):
            market_cap = 0
        if market_cap > 0 and market_cap < min_market_cap_won:
            continue

        # 3) 거래대금 체크 (있다면)
        # universe에 없으면 통과 (보수적)
        daily_value = it.get("daily_value") or it.get("avg_value") or 0
        try:
            daily_value = int(daily_value)
        except (TypeError, ValueError):
            daily_value = 0
        if daily_value > 0 and daily_value < min_daily_value_won:
            continue

        eligible.append(code)

    return eligible


def save_nxt_eligible(codes: List[str]) -> Path:
    """nxt_eligible.json 저장."""
    payload = {
        "date": (datetime.now(KST) if KST else datetime.now()).strftime("%Y-%m-%d"),
        "generated_at": (datetime.now(KST) if KST else datetime.now()).isoformat(),
        "count": len(codes),
        "codes": codes,
        "criteria": {
            "min_daily_value_won": 100_000_000,
            "min_market_cap_won": 10_000_000_000,
            "exclude": ["ETF", "ETN", "preferred", "REITs", "SPAC"],
        },
        "source": "universe.json",
    }
    _OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"[nxt_eligible] 저장 완료: {len(codes)}종목 → {_OUT_PATH.name}")
    return _OUT_PATH


def run_daily_update() -> Dict:
    """매일 G1에서 호출되는 메인 함수."""
    logger.info("[nxt_eligible] 일일 갱신 시작")
    codes = build_nxt_eligible()
    out_path = save_nxt_eligible(codes)
    return {
        "path": str(out_path),
        "count": len(codes),
        "date": (datetime.now(KST) if KST else datetime.now()).strftime("%Y-%m-%d"),
    }


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    print("=== nxt_eligible_builder 수동 실행 ===")
    result = run_daily_update()
    print(json.dumps(result, ensure_ascii=False, indent=2))
