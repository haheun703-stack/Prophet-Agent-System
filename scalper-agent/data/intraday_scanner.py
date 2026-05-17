# -*- coding: utf-8 -*-
"""장중 신규 진입 멀티시그널 스캐너 — 5/17 사장님 결정.

검증 모드 v2: 09:00 morning_rec 13종목 외에 장중 갑자기 체결강도가 올라가는 종목도
1주씩 자동 매수하여 검증. 본격 진입(5/21+)에서는 실전 매매로 확장 가능.

쿠안텍 멀티 시그널 (3개 동시 충족만 진입 — 거짓신호 차단):
  ① 체결강도 폭발: KIS volume-power TOP 30 + strength 150+
  ② 거래량 폭발: KIS volume-rank TOP 30 (당일 거래량 절대 TOP)
  ③ tipping_point 임계: tipping_scan.json score 60+ (사전 매집된 종목)

3개 모두 통과한 종목만 → 즉시 1주 시장가 매수.

안전망:
  - 14:00 이후 신규 진입 OFF (청산 직전 진입 X)
  - 일일 최대 10종목 추가 (자금/관리 폭주 차단)
  - 이미 보유 종목 차단
  - 외인 OVERHEAT/DUMP 종목 차단
  - ETF 차단 (단타 X)
"""
import json
import logging
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Set

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data_store"
TIPPING_PATH = DATA_DIR / "tipping_scan.json"

# ── 시그널 임계 ──────────────────────────────────────
_STRENGTH_THRESHOLD = 150.0   # 체결강도 (volume-power tday_rltv)
_VOLUME_RANK_TOP_N = 30       # 거래량 TOP N
_STRENGTH_RANK_TOP_N = 30     # 체결강도 TOP N
_TIPPING_SCORE_MIN = 60       # tipping_scan score 임계
_INTRADAY_MAX_PER_DAY = 10    # 일일 최대 추가 진입 수
_CUTOFF_TIME = time(14, 0)    # 14:00 이후 신규 진입 OFF (KST)

# ETF prefix (단타 검증 대상 외)
_ETF_PREFIXES = (
    "KODEX", "TIGER", "RISE", "PLUS", "KIWOOM", "KoAct", "TIME",
    "SOL", "HANARO", "KOSEF", "ACE", "ARIRANG", "BNK",
)


def _now_kst() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def _is_intraday_window() -> bool:
    """현재 시각이 장중 신규 진입 허용 시간(09:05 ~ 14:00)인지."""
    now = _now_kst().time()
    return time(9, 5) <= now <= _CUTOFF_TIME


def _load_tipping_scores() -> Dict[str, float]:
    """tipping_scan.json → {code: score} 매핑."""
    if not TIPPING_PATH.exists():
        logger.warning(f"[intraday] tipping_scan.json 없음 — 시그널 1개 누락")
        return {}
    try:
        d = json.loads(TIPPING_PATH.read_text(encoding="utf-8"))
        return {
            item["code"]: float(item.get("score", 0))
            for item in d.get("top_picks", []) or d.get("items", [])
            if item.get("code")
        }
    except (json.JSONDecodeError, KeyError, OSError) as e:
        logger.warning(f"[intraday] tipping_scan 로드 실패: {e}")
        return {}


def _is_etf(name: str) -> bool:
    return any(name.startswith(p) for p in _ETF_PREFIXES)


def scan_intraday_signals(trader, excluded_codes: Set[str] = None) -> List[Dict]:
    """장중 멀티시그널 스캔 → 진입 후보 리스트.

    Args:
        trader: kis_trader 인스턴스 (fetch_ranking_strength/volume 호출용)
        excluded_codes: 이미 보유 중이거나 추천 리스트에 있는 종목 (중복 매수 차단)

    Returns:
        [{"code", "name", "strength", "volume", "tipping_score",
          "current_price", "change_rate", "signal_tags"}, ...]
        3개 시그널 동시 충족 종목만 (최대 _INTRADAY_MAX_PER_DAY)
    """
    if not _is_intraday_window():
        logger.info(f"[intraday] 진입 허용 시간 외 ({_now_kst().strftime('%H:%M')}) — 스캔 스킵")
        return []

    excluded_codes = excluded_codes or set()

    # 1) KIS API 체결강도 TOP N
    try:
        strength_list = trader.fetch_ranking_strength(top_n=_STRENGTH_RANK_TOP_N)
        strength_map = {
            r["code"]: r for r in strength_list
            if r.get("code") and r.get("strength", 0) >= _STRENGTH_THRESHOLD
        }
        logger.info(
            f"[intraday] 체결강도 TOP {_STRENGTH_RANK_TOP_N} 중 임계 {_STRENGTH_THRESHOLD}+ "
            f"통과: {len(strength_map)}종목"
        )
    except Exception as e:
        logger.warning(f"[intraday] 체결강도 조회 실패: {e}")
        return []

    if not strength_map:
        logger.info("[intraday] 체결강도 시그널 통과 종목 없음")
        return []

    # 2) KIS API 거래량 TOP N
    try:
        volume_list = trader.fetch_ranking_volume(top_n=_VOLUME_RANK_TOP_N)
        volume_codes = {r["code"] for r in volume_list if r.get("code")}
        logger.info(f"[intraday] 거래량 TOP {_VOLUME_RANK_TOP_N}: {len(volume_codes)}종목")
    except Exception as e:
        logger.warning(f"[intraday] 거래량 조회 실패: {e}")
        volume_codes = set()

    # 3) tipping_point score
    tipping_scores = _load_tipping_scores()

    # 멀티시그널 합류 (3개 동시 충족)
    candidates = []
    for code, strength_item in strength_map.items():
        # 시그널 ②: 거래량 TOP
        in_volume = code in volume_codes
        # 시그널 ③: tipping score 임계
        tipping_score = tipping_scores.get(code, 0)
        in_tipping = tipping_score >= _TIPPING_SCORE_MIN

        if not (in_volume and in_tipping):
            continue

        # 안전망: ETF/이미 보유/외인 OVERHEAT 차단
        name = strength_item.get("name", "")
        if _is_etf(name):
            continue
        if code in excluded_codes:
            continue

        candidates.append({
            "code": code,
            "name": name,
            "strength": strength_item.get("strength", 0),
            "volume": strength_item.get("volume", 0),
            "current_price": strength_item.get("price", 0),
            "change_rate": strength_item.get("change_rate", 0),
            "tipping_score": tipping_score,
            "signal_tags": (
                f"intraday(strength:{strength_item.get('strength', 0):.0f} "
                f"vol_top:{_VOLUME_RANK_TOP_N} tipping:{tipping_score:.0f}:+11)"
            ),
            "total_score": (
                # 추정 점수 = (체결강도/10) + tipping_score/2 + 11
                min(100, strength_item.get("strength", 0) / 10 + tipping_score / 2 + 11)
            ),
        })

    # 외인 OVERHEAT 차단
    try:
        from tools.flow_intelligence import detect_foreign_dump
        dump_map = detect_foreign_dump()
        before = len(candidates)
        candidates = [
            c for c in candidates
            if dump_map.get(c["code"], {}).get("tag") != "THEME_OVERHEAT"
        ]
        if before > len(candidates):
            logger.info(f"[intraday] 외인 OVERHEAT 차단: {before - len(candidates)}종목 제외")
    except Exception as e:
        logger.debug(f"[intraday] OVERHEAT 검사 실패 (무시): {e}")

    # 점수 내림차순 + 일일 최대 제한
    candidates.sort(key=lambda x: x["total_score"], reverse=True)
    final = candidates[:_INTRADAY_MAX_PER_DAY]
    logger.info(f"[intraday] 멀티시그널 3개 동시 충족: {len(final)}종목 (최대 {_INTRADAY_MAX_PER_DAY})")
    return final
