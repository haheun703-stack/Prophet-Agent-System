# -*- coding: utf-8 -*-
"""brain_state.json 생성 — 큰형(quant_bot_advisory) → 단타봇 자산배분.

[5/20 정보봇 막내 지적]
> "❌ BRAIN 자산 배분 (큰형 advisory 반영): brain_state.json 파일 없음"
> "큰형 advisory 5건 정상 적재됨에도 단타봇이 brain_state 미생성"

[5/19 사고 회피]
> "큰형 5/19 advisory 미도착, brain_state.json 부재 → 매크로 신호 무시"

이 모듈이 매일 06:30 G1 MORNING_PREP에서 호출됨.
quant_bot_advisory 최신 row → regime/market_strength_avg → 자산배분 % 계산 → brain_state.json 저장.
"""
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

_BRAIN_PATH = _ROOT / "data_store" / "brain_state.json"


def _allocation_for_regime(regime: str, strength: float = 100.0) -> int:
    """regime + 큰형 강력포착 평균 체결강도 → 자산배분 % (0~100)."""
    regime = (regime or "NEUTRAL").upper()
    if regime == "PANIC":
        return 0
    if regime == "BEARISH":
        return 20 if strength < 80 else 30
    if regime == "CAUTION":
        return 40 if strength < 90 else 50
    if regime == "BULLISH":
        return 90 if strength > 110 else 80
    # NEUTRAL
    return 60 if strength > 100 else 50


def build_brain_state(verbose: bool = False) -> Dict:
    """큰형 advisory 최신 → brain_state.json 생성.

    Returns: brain_state dict (저장 후 반환). 실패 시 fallback dict.
    """
    today = date.today().isoformat()
    now_iso = datetime.now(KST).isoformat() if KST else datetime.now().isoformat()

    # 큰형 advisory 조회
    rows = []
    try:
        # 5/21 fix: supabase_sql 모듈은 `query` (다건) / `query_one` (단건) 만 export.
        # 기존의 `query_all`은 존재하지 않아 ImportError로 항상 fallback path만 탔음
        # → brain_state.json이 한 번도 생성되지 않음 (DataIntegrity stale 원인)
        from utils.supabase_sql import query as query_all
        try:
            rows = query_all(
                "SELECT * FROM quant_bot_advisory WHERE advisory_date = %s "
                "ORDER BY created_at DESC LIMIT 10",
                (today,),
            )
        except Exception as e_q:
            logger.warning(f"[brain_state] 오늘 advisory 조회 실패: {e_q}")
        # fallback — 어제 + 오늘 합쳐 최신 5건
        if not rows:
            try:
                rows = query_all(
                    "SELECT * FROM quant_bot_advisory "
                    "ORDER BY created_at DESC LIMIT 5"
                )
            except Exception as e_f:
                logger.warning(f"[brain_state] fallback 조회 실패: {e_f}")
    except Exception as e:
        logger.warning(f"[brain_state] Supabase 모듈 로드 실패: {e}")

    # regime + market_strength_avg 추출
    regime = "NEUTRAL"
    market_strength_avg = 100.0
    latest_at = None
    if rows:
        latest = rows[0]
        regime = (latest.get("regime") or "NEUTRAL").upper()
        # market_strength_avg는 다양한 키 후보
        for k in ("market_strength_avg", "strength_avg", "avg_strength"):
            v = latest.get(k)
            if v is not None:
                try:
                    market_strength_avg = float(v)
                    break
                except (TypeError, ValueError):
                    pass
        latest_at = str(latest.get("created_at", "")) or None

    allocation_pct = _allocation_for_regime(regime, market_strength_avg)

    brain_state = {
        "date": today,
        "generated_at": now_iso,
        "regime": regime,
        "market_strength_avg": round(market_strength_avg, 1),
        "allocation_pct": allocation_pct,
        "advisory_count": len(rows),
        "latest_advisory_at": latest_at,
        "source": "quant_bot_advisory",
        "note": "5/20 막내 지적 후 추가 — 큰형 → 단타봇 자산배분 매핑",
    }

    # 저장 (디렉토리 보장)
    try:
        _BRAIN_PATH.parent.mkdir(parents=True, exist_ok=True)
        _BRAIN_PATH.write_text(
            json.dumps(brain_state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(
            f"[brain_state] 생성 완료: regime={regime} strength={market_strength_avg:.1f} "
            f"allocation={allocation_pct}% (advisory={len(rows)}건)"
        )
    except Exception as e:
        logger.error(f"[brain_state] 파일 저장 실패: {e}")

    if verbose:
        print(json.dumps(brain_state, ensure_ascii=False, indent=2))
    return brain_state


def load_brain_state() -> Optional[Dict]:
    """저장된 brain_state.json 로드. 없으면 None."""
    if not _BRAIN_PATH.exists():
        return None
    try:
        return json.loads(_BRAIN_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[brain_state] 로드 실패: {e}")
        return None


if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    print("=== brain_state.json 수동 생성 ===")
    state = build_brain_state(verbose=True)
    print(f"\n저장 위치: {_BRAIN_PATH}")
