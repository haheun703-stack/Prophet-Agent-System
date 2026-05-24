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
    """상한가 엔진 트리거 (즉시 진입가 + 목표가 계산된 종목).

    signals.json 구조: {generated_at, count, signals: [...]}
    또는 watchlist.json items 중 status='triggered' 종목.
    """
    triggers = []
    # signals.json 우선
    sig_data = _load_json("limit_up/signals.json")
    if sig_data:
        triggers.extend(sig_data if isinstance(sig_data, list) else sig_data.get("signals", []))
    # watchlist.json에서 status=triggered 종목도 추가
    wl_data = _load_json("limit_up/watchlist.json")
    if wl_data and isinstance(wl_data, dict):
        for item in wl_data.get("items", []):
            if item.get("status") == "triggered":
                # 중복 방지
                if not any(t.get("code") == item.get("code") for t in triggers):
                    triggers.append(item)
    return triggers


def load_limit_up_watchlist() -> List[Dict]:
    """상한가 엔진 감시풀 (눌림목 진입 대기 종목, status='monitoring').

    watchlist.json 구조: {updated_at, count, monitoring, triggered, items: [...]}
    """
    data = _load_json("limit_up/watchlist.json")
    if not data:
        return []
    items = data if isinstance(data, list) else data.get("items", [])
    # monitoring 상태만 (triggered는 load_limit_up_triggers로)
    return [item for item in items if item.get("status") == "monitoring"]


# ─────────────────────────────────────────────────────
# 카테고리 ② NXT 야간
# ─────────────────────────────────────────────────────
def load_nxt_top5_picks() -> Dict:
    """NXT 야간 TOP 5 픽."""
    return _load_json("nxt_top5_picks.json") or {}


def load_nxt_eligible() -> List[str]:
    """NXT 매매 가능 종목 코드 리스트.

    [5/20 fix] nxt_eligible_builder는 'codes' 키로 저장 (신규).
    레거시 데이터는 'stocks' 키 사용 가능 — 양쪽 호환.
    """
    data = _load_json("nxt_eligible.json")
    if not data:
        return []
    if isinstance(data, dict):
        # ★ codes 키 우선 (nxt_eligible_builder), fallback stocks (레거시) ★
        items = data.get("codes") or data.get("stocks") or []
        if isinstance(items, list):
            return [s if isinstance(s, str) else s.get("code", "") for s in items if s]
    elif isinstance(data, list):
        return [s if isinstance(s, str) else s.get("code", "") for s in data if s]
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

    # ★ 5/20 사고 fix #8: 시장 강세 + 테마 통합 추가 ★
    # 5/20 자비스 차단 5종 중 진원생명과학(+18.23%) 미발굴 사고 후
    _add(load_kis_market_top(), "kis_market_top")
    _add(load_short_cover(), "short_cover")
    for stock in load_theme_universe_stocks():
        source_map.setdefault(stock["code"], []).append(f"theme:{stock.get('subtheme','?')}")

    # ★ 5/20 사장님 비전 — 자체 자산 추가 통합 (사장님 지적: "있는데 활용 안 함") ★
    _add(load_sector_concurrent_surge(), "sector_concurrent_surge")
    _add(load_consecutive_surge(), "consecutive_surge")
    _add(load_signals_triggers(), "limit_up_signals")  # signals.json = triggers 역할
    _add(load_missed_gainers(), "missed_gainers_learning")
    _add(load_premium_levels_stocks(), "premium_levels")

    # ★ 5/20 사장님 비전 — 학습 데이터 기반 강세 종목 (가장 중요) ★
    # surge_pattern_learner가 매일 누적한 +10%+ 종목 중 7일 내 2회+ 반복 출현 종목
    try:
        from data.surge_pattern_learner import get_learned_strong_codes
        for code in get_learned_strong_codes(days=7, min_appearance=2):
            source_map.setdefault(code, []).append("learned_strong")
    except Exception:
        pass

    return source_map


# ─────────────────────────────────────────────────────
# ★ 5/20 사장님 비전 — 자체 자산 추가 로더 ★
# ─────────────────────────────────────────────────────
def load_sector_concurrent_surge() -> List[Dict]:
    """섹터 동조 급등 (sector_concurrent_surge.json)."""
    data = _load_json("sector_concurrent_surge.json")
    if not data:
        return []
    result = []
    if isinstance(data, dict):
        for sector, items in data.items():
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict) and it.get("code"):
                        result.append({**it, "sector": sector})
                    elif isinstance(it, str):
                        result.append({"code": it, "sector": sector})
    elif isinstance(data, list):
        result = [x for x in data if isinstance(x, dict) and x.get("code")]
    return result


def load_consecutive_surge() -> List[Dict]:
    """상한가 연속 (consecutive_surge.json)."""
    data = _load_json("consecutive_surge.json")
    if not data:
        return []
    items = data.get("stocks", []) if isinstance(data, dict) else data
    return [x for x in items if isinstance(x, dict) and x.get("code")]


def load_signals_triggers() -> List[Dict]:
    """상한가 엔진 signals.json (triggers 역할)."""
    data = _load_json("limit_up/signals.json")
    if not data:
        return []
    items = data.get("stocks", []) if isinstance(data, dict) else data
    return [x for x in items if isinstance(x, dict) and x.get("code")]


def load_missed_gainers() -> List[str]:
    """놓친 급등 종목 학습 (learning/missed_gainers/)."""
    import os
    learn_dir = DATA_DIR / "learning" / "missed_gainers"
    if not learn_dir.exists():
        return []
    codes = set()
    # 최근 7일 파일만
    try:
        files = sorted(learn_dir.glob("*.json"))[-7:]
        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                items = data.get("stocks", []) if isinstance(data, dict) else data
                for x in items:
                    if isinstance(x, dict) and x.get("code"):
                        codes.add(x["code"])
                    elif isinstance(x, str):
                        codes.add(x)
            except Exception:
                pass
    except Exception:
        pass
    return list(codes)


def load_premium_levels_stocks() -> List[Dict]:
    """매매포인트 premium_levels 종목 리스트."""
    data = _load_json("premium_levels.json")
    if not data:
        return []
    if isinstance(data, dict):
        # {"001440": {...}, ...} 형식 가능
        return [{"code": k, **v} if isinstance(v, dict) else {"code": k}
                for k, v in data.items()]
    elif isinstance(data, list):
        return [x for x in data if isinstance(x, dict) and x.get("code")]
    return []


# ─────────────────────────────────────────────────────
# ★ 5/20 fix #8: 신규 후보 소스 3종 (시장 상한가 + 숏커버 + 테마)
# ─────────────────────────────────────────────────────
def load_kis_market_top(min_change_pct: float = 5.0, min_volume: int = 1_000_000) -> List[Dict]:
    """KIS 등락률 TOP 30 실시간 (4황금 시그널 추격 매수 후보).

    [5/20 학습] +10~20% + 거래량 1,000만주+ 시그널 = 검증된 추격 매수 (이노/빛과전자)
    여기선 +5% + 100만주+ 로 약간 완화 (asset_pool 후보 풀 확장 목적).

    실시간 호출 비용이 있어 캐시 사용 (5분).
    """
    import os, json, time as _t
    from pathlib import Path as _P
    cache_path = _P(__file__).resolve().parent.parent / "data_store" / "kis_market_top_cache.json"
    # 캐시 5분
    if cache_path.exists():
        try:
            age = _t.time() - cache_path.stat().st_mtime
            if age < 300:
                return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    # KIS API 호출
    try:
        import requests
        from dotenv import load_dotenv
        load_dotenv()
        import mojito
        broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"), api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"), mock=False,
        )
        token = broker.access_token
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/ranking/fluctuation"
        hdrs = {"authorization": "Bearer " + token, "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"), "tr_id": "FHPST01700000"}
        params = {"fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20170",
                  "fid_input_iscd": "0000", "fid_rank_sort_cls_code": "0", "fid_input_cnt_1": "0",
                  "fid_prc_cls_code": "1", "fid_input_price_1": "", "fid_input_price_2": "",
                  "fid_vol_cnt": "", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
                  "fid_div_cls_code": "0", "fid_rsfl_rate1": "", "fid_rsfl_rate2": ""}
        r = requests.get(url, headers=hdrs, params=params, timeout=10)
        out = r.json().get("output", [])[:30]
        result = []
        for x in out:
            chg = float(x.get("prdy_ctrt", 0))
            vol = int(x.get("acml_vol", 0))
            # 4황금 시그널 필터: +5% + 100만주+ (느슨한 추격 후보)
            if chg >= min_change_pct and vol >= min_volume:
                result.append({
                    "code": x.get("stck_shrn_iscd", ""),
                    "name": x.get("hts_kor_isnm", ""),
                    "change_pct": chg, "volume": vol,
                    "price": int(x.get("stck_prpr", 0)),
                })
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"[asset_pool] kis_market_top 실패: {e}")
        return []


def load_short_cover(min_ratio: float = 5.0) -> List[Dict]:
    """숏커버 반등 후보 (상환/신규 비율 높은 종목).

    [5/20 학습] 사장님이 5/20 11:00 "숏커버 반등 후보" 텔레그램 알람으로 수동 제공.
    이걸 asset_pool에 통합 → 자비스가 자동 픽업.
    """
    data = _load_json("short_cover_candidates.json")
    if not data:
        return []
    items = data.get("stocks", []) if isinstance(data, dict) else data
    return [x for x in items if isinstance(x, dict) and x.get("ratio", 0) >= min_ratio]


def load_theme_universe_stocks() -> List[Dict]:
    """막내(정보봇) 테마 유니버스 → 종목 평면 리스트.

    [5/20 학습] 사장님 "단타 = 소재" 원칙. 막내가 매일 06:00 theme_universe 생성.
    여기선 단순 평면 리스트로 변환 (서브테마 정보는 subtheme 필드로 보존).
    """
    import os
    path_candidates = [
        "/home/ubuntu/jgis/data_store/theme_universe.json",
        str(BASE_DIR / "theme_universe.json") if 'BASE_DIR' in globals() else None,
    ]
    for p in path_candidates:
        if p and os.path.exists(p):
            try:
                import json
                data = json.loads(open(p, encoding="utf-8").read())
                result = []
                for theme in data.get("themes", []):
                    for sub in theme.get("subthemes", []):
                        for stock in sub.get("stocks", []):
                            result.append({
                                "code": stock.get("code", ""),
                                "name": stock.get("name", ""),
                                "theme": theme.get("theme_id", ""),
                                "subtheme": sub.get("id", ""),
                                "weight": sub.get("weight", 0),
                            })
                return result
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"[asset_pool] theme_universe 로드 실패 {p}: {e}")
    return []


def get_high_confidence_candidates() -> List[str]:
    """다중 자산 소스에서 동시 발견된 고신뢰 종목 (2개+ 소스).

    [5/20 fix] kis_market_top / short_cover / theme:* / learned_strong
    소스 단독으로도 고신뢰 처리.

    [5/20 사장님 비전] learned_strong = surge_pattern_learner 7일 누적 강세 종목
    → 가장 신뢰도 높음 (이미 검증된 패턴).
    """
    source_map = get_candidate_source_map()
    _STRONG_SOLO_SOURCES = ("kis_market_top", "short_cover", "learned_strong",
                            "consecutive_surge", "sector_concurrent_surge")
    result = []
    for code, sources in source_map.items():
        if len(sources) >= 2:
            result.append(code)
            continue
        # 단독 강력 소스 OR theme 단독
        if any(s in _STRONG_SOLO_SOURCES for s in sources):
            result.append(code)
            continue
        if any(s.startswith("theme:") for s in sources):
            result.append(code)
            continue
    return result


def _get_today_change_pct(code: str) -> float:
    """오늘 등락률 — kis_market_top_cache에서 조회."""
    cache = _load_json("kis_market_top_cache.json")
    if not cache:
        return 0.0
    if isinstance(cache, list):
        for x in cache:
            if isinstance(x, dict) and x.get("code") == code:
                try:
                    return float(x.get("change_pct", 0))
                except (TypeError, ValueError):
                    return 0.0
    return 0.0


def _load_continuation_score_map() -> Dict[str, float]:
    """★ 5/24 사장님 영구 룰 ★ continuation_score 종목별 평균 매핑.

    백테스트 검증 (5/24 stock_selection_accuracy_5_24.py):
        score 0-30 zone (n=218):  평균 max_gain +1.57% (거의 무익)
        score 30-50 zone (n=95):  평균 max_gain +21.75%
        score 50-70 zone (n=253): 평균 max_gain +68.67% ★ Sweet Zone ★
        score 70-90+ zone (n=127): 평균 max_gain +44.60% (이미 폭등 후)

    history.json events의 종목별 평균 continuation_score 반환.
    """
    try:
        import json
        from pathlib import Path
        base = Path(__file__).resolve().parent.parent / "data_store"
        hist_path = base / "limit_up" / "history.json"
        if not hist_path.exists():
            return {}
        data = json.loads(hist_path.read_text(encoding="utf-8"))
        events = data.get("events", [])
        scores_by_code: Dict[str, List[float]] = {}
        for e in events:
            code = e.get("code")
            cs = e.get("continuation_score")
            if code and cs is not None:
                scores_by_code.setdefault(code, []).append(float(cs))
        return {c: round(sum(s) / len(s), 1) for c, s in scores_by_code.items() if s}
    except Exception as _e:
        logger.debug(f"[continuation_score] 로드 실패: {_e}")
        return {}


def _load_theme_gain_map(top_n_themes: int = 10,
                          min_events: int = 3,
                          cache_ttl_sec: int = 3600) -> Dict[str, Dict]:
    """★ 5/24 사장님 지적 ★ theme_map.json (302개) 세부 테마 동적 가중치.

    사장님 시장 관점 [feedback_market_view_small_caps_5_24]:
        KIS 표준 22 sector ≠ 단타 관점
        세부 테마 = 원전/전력기기/조선/광통신/로봇/반도체장비/2차전지 등

    로직:
        1. theme_map.json 302개 테마 (code → [theme1, theme2, ...] 역매핑)
        2. history.json events 의 종목 → 테마 매핑
        3. 테마별 평균 max_gain_after 산출
        4. 상위 N개 테마 자동 가중치 (rank별)

    Returns:
        {
            "theme_bonus": {theme_name: bonus_score},
            "code_themes": {code: [theme1, ...]},  # 종목 → 테마 매핑
        }
    """
    import json
    import time as _t
    from pathlib import Path

    if not hasattr(_load_theme_gain_map, "_cache"):
        _load_theme_gain_map._cache = {"ts": 0, "data": {"theme_bonus": {}, "code_themes": {}}}
    cache = _load_theme_gain_map._cache
    if cache["ts"] and (_t.time() - cache["ts"]) < cache_ttl_sec:
        return cache["data"]

    try:
        base = Path(__file__).resolve().parent.parent / "data_store"
        theme_path = base / "theme_map.json"
        hist_path = base / "limit_up" / "history.json"
        if not theme_path.exists() or not hist_path.exists():
            return {"theme_bonus": {}, "code_themes": {}}

        tm = json.loads(theme_path.read_text(encoding="utf-8"))
        # tm["themes"] = {theme_code: {name, codes}}
        themes = tm.get("themes", {})
        code_to_themes: Dict[str, List[str]] = tm.get("code_to_themes", {})

        # 사전 code_to_themes 없으면 역인덱스 구축
        if not code_to_themes and themes:
            for t_code, t_info in themes.items():
                if not isinstance(t_info, dict):
                    continue
                name = t_info.get("name")
                for c in t_info.get("codes", []):
                    code_to_themes.setdefault(c, []).append(name)

        # theme_map.json code_to_themes 구조: {code: [{theme_code, theme_name}, ...]}
        # → 종목코드 → 테마 이름 리스트로 변환
        code_theme_names: Dict[str, List[str]] = {}
        for c, theme_list in code_to_themes.items():
            if not isinstance(theme_list, list):
                continue
            names = []
            for t in theme_list:
                if isinstance(t, dict):
                    n = t.get("theme_name") or t.get("name")
                    if n:
                        names.append(n)
                elif isinstance(t, str):
                    names.append(t)
            if names:
                code_theme_names[c] = names

        # history events → 테마별 gain
        data = json.loads(hist_path.read_text(encoding="utf-8"))
        events = data.get("events", [])
        theme_gains: Dict[str, List[float]] = {}
        for e in events:
            code = e.get("code")
            gain = e.get("max_gain_after")
            if not code or gain is None:
                continue
            for theme_name in code_theme_names.get(code, []):
                theme_gains.setdefault(theme_name, []).append(float(gain))

        # 필터 + 평균 + 정렬
        theme_stats = []
        for name, gains in theme_gains.items():
            if len(gains) < min_events:
                continue
            avg = sum(gains) / len(gains)
            theme_stats.append((name, avg, len(gains)))
        theme_stats.sort(key=lambda x: -x[1])

        # 상위 N개 가중치
        rank_weights = [15, 12, 10, 8, 7, 6, 5, 4, 3, 2]
        theme_bonus: Dict[str, int] = {}
        for i, (name, avg, n) in enumerate(theme_stats[:top_n_themes]):
            theme_bonus[name] = rank_weights[i] if i < len(rank_weights) else 2

        if theme_bonus:
            top3 = list(theme_bonus.items())[:3]
            logger.info(f"[theme_gain] 동적 테마 가중치 (top {len(theme_bonus)}): "
                          + ", ".join(f"{n}(+{w})" for n, w in top3))

        result = {"theme_bonus": theme_bonus, "code_themes": code_theme_names}
        cache["ts"] = int(_t.time())
        cache["data"] = result
        return result
    except Exception as _e:
        logger.debug(f"[theme_gain] 동적 산출 실패: {_e}")
        return {"theme_bonus": {}, "code_themes": {}}


def _detect_group_rotation(top_today_codes: List[str],
                             today_change_map: Dict[str, float],
                             min_surge_count: int = 2,
                             surge_threshold: float = 5.0) -> Dict[str, int]:
    """★ 5/24 사장님 지적 ★ 그룹주 로테이션 감지 → 후발 종목 가중치.

    로직:
        1. 오늘 강세 종목 (today_change >= 5%) 중 그룹주 그룹별 카운트
        2. 같은 그룹 N종 강세 시 → 그 그룹 다른 종목에 가중치 (+10)
        3. 그룹 전체 로테이션 진행 중 → 후발 종목 매수 기회

    Args:
        top_today_codes: 오늘 매수 후보 종목 코드 리스트
        today_change_map: {code: change_pct}
        min_surge_count: 그룹 로테이션 인정 최소 강세 종목 수

    Returns:
        {code: bonus_score} 그룹 로테이션 후발 종목들
    """
    code_to_group = _build_code_to_group_map()

    # 오늘 강세 그룹 카운트
    group_surge: Dict[str, int] = {}
    for code, ch in today_change_map.items():
        if ch >= surge_threshold:
            group = code_to_group.get(code)
            if group:
                group_surge[group] = group_surge.get(group, 0) + 1

    # 로테이션 진행 중 그룹 → 후발 종목 보너스
    bonus: Dict[str, int] = {}
    for group, count in group_surge.items():
        if count < min_surge_count:
            continue
        # 같은 그룹의 다른 종목 (오늘 강세 X)
        for code in GROUP_STOCK_MAP.get(group, []):
            ch = today_change_map.get(code, 0)
            if ch < surge_threshold:   # 아직 안 오른 종목
                bonus[code] = bonus.get(code, 0) + 10
    return bonus


def _continuation_score_adjustment(cs: float) -> int:
    """★ 5/24 백테스트 검증 ★ continuation_score zone별 점수 조정.

    zone 0-30 차단 (218건 = 평균 +1.57% 거의 무익)
    zone 50-70 우선 (253건 = 평균 +68.67% 가장 강력)
    zone 70-90+ 보조 (이미 폭등 — 추가 상승 여력 제한)
    """
    if cs < 30:
        return -40   # 강한 패널티 (실질 차단)
    if cs < 50:
        return 5    # 약한 보너스
    if cs < 70:
        return 30   # ★ Sweet Zone — 강한 보너스 ★
    return 15      # zone 70+ — 보조 보너스


# ★ 5/24 사장님 영구 시장 전망 ★ [feedback_market_view_small_caps_5_24]
# "대형주는 조금씩 올라가고 단타의 연속 = 소부장 + 소형주 + 세부 테마/그룹주 로테이션"
#
# 시총 구간별 단타 우대 (소부장/소형주 핵심)
CAP_BAND_BONUS = {
    "초소형":  -3,    # ~100억 (위험)
    "소형":   +10,   # 100~1000억 ★ 소부장 다수 ★
    "중소형": +8,    # 1000~5000억 ★ 단타 핵심 ★
    "중대형": +3,    # 5000억~1조
    "대형":   +0,    # 1조~5조
    "초대형": -5,    # 5조+ (단타 대상 X)
}


def _classify_cap_band(cap_억: float) -> str:
    """시총 구간 분류 (사장님 5/24 직관 기반)."""
    if cap_억 < 100:
        return "초소형"
    if cap_억 < 1000:
        return "소형"
    if cap_억 < 5000:
        return "중소형"
    if cap_억 < 10000:
        return "중대형"
    if cap_억 < 50000:
        return "대형"
    return "초대형"


# ★ 5/24 사장님 지적 ★ 그룹주 매핑 (로테이션 감지용)
# theme_map.json에 없음 → 수동 정의 (단타봇 자체 학습 데이터)
GROUP_STOCK_MAP = {
    "삼성그룹": ["005930", "005935", "028260", "010140", "207940", "032830",
                "009150", "018260", "006400", "029780", "010620"],
    "SK그룹":  ["034730", "000660", "011790", "017670", "096770", "003600",
                "006120", "001740", "006800", "036570"],
    "LG그룹":  ["003550", "066570", "051900", "051910", "032640", "001120",
                "034220", "108670", "108670", "002270"],
    "현대그룹": ["005380", "005440", "086280", "012330", "011200", "057050",
                "001450", "267250", "010620"],
    "두산":    ["000150", "336260", "241560", "042670", "081000"],
    "한화":    ["000880", "088350", "001460", "009830", "272210"],
    "롯데":    ["004990", "023530", "071840", "011170", "012170"],
    "GS":     ["078930", "001250", "006360"],
    "포스코":  ["005490", "047050", "003670"],
    "신세계":  ["004170", "031430", "035250"],
    "셀트리온": ["068270", "091990"],
    "카카오":  ["035720", "323410", "293490"],
    "네이버":  ["035420"],
}


def _build_code_to_group_map() -> Dict[str, str]:
    """종목코드 → 그룹주 이름 역매핑."""
    result = {}
    for group, codes in GROUP_STOCK_MAP.items():
        for c in codes:
            result[c] = group
    return result


# ★ 5/24 사장님 지적 ★ 섹터 가중치는 매일 변경됨 → 동적 산출 필수
# [feedback_verify_external_knowledge]: 외부/주관 임계값 hard-coded X
# 매일 G7 EVENING_BRAIN history.json 갱신 후 자동 재계산
def _load_sector_gain_map(top_n_sectors: int = 5,
                            min_events: int = 5,
                            cache_ttl_sec: int = 3600) -> Dict[str, int]:
    """★ 5/24 사장님 지적 ★ history.json에서 섹터별 평균 max_gain 자동 산출.

    매일 G7 EVENING_BRAIN의 build_limit_up_history 갱신 직후 호출 → 최신 섹터 강세 반영.

    로직:
        1. history.json events 섹터별 그룹화
        2. min_events (기본 5건) 이상 표본 있는 섹터만 채택
        3. 평균 max_gain_after 계산 → 상위 top_n_sectors (기본 5개)
        4. 가중치 부여:
           rank 1: +12 / rank 2: +10 / rank 3: +8 / rank 4: +6 / rank 5: +4
        5. 결과를 모듈 캐시에 저장 (1시간 TTL)

    Returns:
        {sector_name: bonus_score} 매일 갱신
    """
    import json
    import time as _t
    from pathlib import Path

    # 모듈 캐시 (성능)
    if not hasattr(_load_sector_gain_map, "_cache"):
        _load_sector_gain_map._cache = {"ts": 0, "data": {}}
    cache = _load_sector_gain_map._cache
    if cache["ts"] and (_t.time() - cache["ts"]) < cache_ttl_sec:
        return cache["data"]

    try:
        base = Path(__file__).resolve().parent.parent / "data_store"
        hist_path = base / "limit_up" / "history.json"
        if not hist_path.exists():
            logger.debug("[sector_gain] history.json 없음 — 동적 가중치 스킵")
            return {}
        data = json.loads(hist_path.read_text(encoding="utf-8"))
        events = data.get("events", [])

        sector_gains: Dict[str, List[float]] = {}
        for e in events:
            sec = e.get("sector") or "기타"
            gain = e.get("max_gain_after")
            if gain is None:
                continue
            sector_gains.setdefault(sec, []).append(float(gain))

        # 필터 + 평균 + 정렬
        sector_stats = []
        for sec, gains in sector_gains.items():
            if len(gains) < min_events:
                continue
            avg = sum(gains) / len(gains)
            sector_stats.append((sec, avg, len(gains)))
        sector_stats.sort(key=lambda x: -x[1])

        # 상위 N개 가중치 (rank 따라 차등)
        rank_weights = [12, 10, 8, 6, 4]
        result: Dict[str, int] = {}
        for i, (sec, avg, n) in enumerate(sector_stats[:top_n_sectors]):
            result[sec] = rank_weights[i] if i < len(rank_weights) else 4

        if result:
            top_log = ", ".join(f"{s}(+{result[s]}/{int(sector_gains[s] and sum(sector_gains[s])/len(sector_gains[s])):.0f}%)"
                                  for s in result)
            logger.info(f"[sector_gain] 동적 가중치 (top {len(result)}): {top_log}")

        cache["ts"] = int(_t.time())
        cache["data"] = result
        return result
    except Exception as _e:
        logger.debug(f"[sector_gain] 동적 산출 실패: {_e}")
        return {}


def get_candidate_score_map() -> Dict[str, int]:
    """[5/20 사장님 지적 fix] 점수 = "내일 오를 종목" 기준 (오늘 강세 기준 X).

    핵심 깨달음:
      - 오늘 +25% 상한가 = 내일 갭다운 위험 (X)
      - 오늘 +5~15% + 매수 잔량 = 연속 강세 (O)
      - 오늘 0~5% + 같은 테마 강세 = 눌림목/미반영 (★ 사장님 황금 패턴)

    소스 점수 (기본):
      - learned_strong: +50 (7일 누적 검증)
      - sector_concurrent_surge: +35 (★ 테마 동조 = 가장 안전)
      - theme:diagnostic_kit: +30 (5/20 100% 적중)
      - kis_market_top: +25 → 오늘 등락률에 따라 패널티 적용
      - 기타 소스: +10~25

    ★ 오늘 등락률 패널티/보너스 (사장님 5/20 21:40 지적) ★
      - 오늘 +25%+ (상한가): -50점 (다음날 갭다운 위험)
      - 오늘 +15~25%: -10점 (조정 위험)
      - 오늘 +5~15%: +20점 (연속 강세 후보)
      - 오늘 0~5%: +35점 (★ 미반영 강세 = 가장 안전)
      - 오늘 -5~0%: +25점 (눌림목 매수)
      - 오늘 -5% 이하: -20점 (약세 지속 위험)
    """
    source_map = get_candidate_source_map()
    # ★ 5/21 박사 자율 fix — 5/21 상한가 7종 분석 후 가중치 재조정 ★
    # 어제 시그널 5/7 감지 (71%) but 매수 1/7 = 박사 후보 우선순위 시스템 X
    # 원인: consecutive_surge/massive_dual_buy 가중치 낮음 → TOP 5 탈락
    # 검증: 어제 매수 누락한 4종 모두 오늘 +20%~+30% 상한가
    # 박사 결단: 결과 검증된 시그널 가중치 ↑
    weights = {
        "learned_strong": 50,
        "sector_concurrent_surge": 35,   # ★ 테마 동조 = 가장 안전
        "kis_market_top": 25,            # 오늘 강세 — 등락률 패널티 별도 적용
        "consecutive_surge": 30,         # ★ 5/21 fix: 20→30 (오늘 상한가 2종 검증 — 피델릭스/대원전선우)
        "limit_up_signals": 15,
        "short_cover": 25,
        "limit_up_trigger": 10,          # 상한가 트리거 = 추격 위험 (낮춤)
        "limit_up_watchlist": 18,        # 워치 = 눌림목 가능성 (높임)
        "massive_dual_buy": 35,          # ★ 5/21 fix: 25→35 (오늘 상한가 3종 검증 — LG/제주반도체/가온전선)
        "oneshot_stealth": 20,
        "foreign_accumulation": 20,
        "accumulation_radar": 20,
        "premium_levels": 12,
        "missed_gainers_learning": 15,
        "nxt_eligible": 3,
    }
    # ★ 5/21 박사 자율 — 매크로 패널티 (개선 액션 #1) ★
    # 외인 N일 연속 매도 = 약세장 = 점수 패널티 (5/21 -15일 연속 + 반도체 -1.2조)
    # brain_state + jgis_morning_context에서 외인 streak 가져옴
    macro_penalty = 0
    try:
        import json
        from pathlib import Path
        base = Path(__file__).resolve().parent.parent / "data_store"

        # 외인 streak (음수 = 매도 연속)
        ctx_path = base / "jgis_morning_context.json"
        if ctx_path.exists():
            ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
            f_streak = int(ctx.get("foreign_streak", 0))
            if f_streak <= -10:
                macro_penalty -= 15  # 강한 약세장
            elif f_streak <= -7:
                macro_penalty -= 10  # 중강 약세장
            elif f_streak <= -3:
                macro_penalty -= 5   # 약세장

        # brain_state regime
        brain_path = base / "brain_state.json"
        if brain_path.exists():
            bd = json.loads(brain_path.read_text(encoding="utf-8"))
            regime = bd.get("regime", "NEUTRAL")
            if regime == "PANIC":
                macro_penalty -= 20
            elif regime == "BEARISH":
                macro_penalty -= 10

        if macro_penalty < 0:
            logger.info(f"[score] 매크로 패널티 적용: {macro_penalty}점")
    except Exception as _me:
        logger.debug(f"[score] 매크로 패널티 계산 실패 (무시): {_me}")

    # ★ 5/21 박사 자율 — 섹터 변동성 패널티 (개선 액션 #2 단순 버전) ★
    # ATR 백테스트는 주말 작업. 우선 변동성 큰 섹터 -점 적용.
    # 5/21 로킷헬스케어 -15.4% 사고 = 의료/바이오 변동성 큰 섹터
    HIGH_VOLATILITY_SECTORS = {
        "의약품", "제약", "의료정밀", "바이오", "헬스케어",
        "엔터테인먼트", "게임",  # 변동성 추가
    }
    try:
        universe_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        sector_map = {}
        if universe_path.exists():
            u = json.loads(universe_path.read_text(encoding="utf-8"))
            if isinstance(u, dict):
                for c, info in u.items():
                    if isinstance(info, dict):
                        sector_map[c] = info.get("sector", "")
    except Exception:
        sector_map = {}

    # ★ 5/24 사장님 영구 룰 ★ continuation_score 종목별 평균 (백테스트 검증)
    cs_map = _load_continuation_score_map()

    # ★ 5/24 사장님 지적 ★ 섹터 가중치 동적 산출 (KIS 22 표준 sector)
    sector_gain_map = _load_sector_gain_map(top_n_sectors=5, min_events=5)

    # ★ 5/24 사장님 지적 ★ 세부 테마 가중치 동적 (theme_map.json 302개)
    theme_data = _load_theme_gain_map(top_n_themes=10, min_events=3)
    theme_bonus_map = theme_data.get("theme_bonus", {})
    code_to_themes = theme_data.get("code_themes", {})

    # ★ 5/24 사장님 지적 ★ 그룹주 로테이션 감지 (오늘 등락률 기반)
    # today_change_map 사전 수집
    today_change_map = {}
    for code in source_map:
        today_change_map[code] = _get_today_change_pct(code)
    group_rotation_bonus = _detect_group_rotation(
        list(source_map.keys()), today_change_map,
        min_surge_count=2, surge_threshold=5.0,
    )
    if group_rotation_bonus:
        logger.info(f"[group_rotation] 후발 종목 가중치: {len(group_rotation_bonus)}개")

    # 시총 정보 매핑 (universe.json)
    cap_map: Dict[str, float] = {}
    try:
        from pathlib import Path
        import json as _json
        universe_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        if universe_path.exists():
            u = _json.loads(universe_path.read_text(encoding="utf-8"))
            for c, info in u.items():
                if isinstance(info, dict):
                    cap_map[c] = float(info.get("cap_억", 0))
    except Exception:
        pass

    score_map: Dict[str, int] = {}
    for code, sources in source_map.items():
        score = 0
        for src in sources:
            if src.startswith("theme:"):
                sub = src.split(":", 1)[1]
                if sub == "diagnostic_kit":
                    score += 30
                elif sub == "vaccine":
                    score += 15
                elif sub == "antiviral":
                    score += 0
                else:
                    score += 10
            else:
                score += weights.get(src, 10)
        # 다중 소스 보너스
        if len(sources) > 1:
            score += 10 * (len(sources) - 1)

        # ★ 5/21 박사 자율 — 매크로 패널티 적용 ★
        score += macro_penalty

        # ★ 5/21 박사 자율 — 섹터 변동성 패널티 ★
        sector = sector_map.get(code, "")
        if sector in HIGH_VOLATILITY_SECTORS:
            score -= 5

        # ★ 5/24 백테스트 검증 ★ continuation_score zone 보너스/패널티
        if code in cs_map:
            score += _continuation_score_adjustment(cs_map[code])

        # ★ 5/24 사장님 지적 ★ 섹터 가중치 동적 (매일 갱신 — hardcoded 제거)
        if sector in sector_gain_map:
            score += sector_gain_map[sector]

        # ★ 5/24 사장님 지적 ★ 세부 테마 가중치 (theme_map.json 302개)
        # 종목이 여러 테마에 속할 수 있음 → 가장 강한 테마 보너스만 적용 (중복 방지)
        if theme_bonus_map and code in code_to_themes:
            best_theme_bonus = 0
            for theme_name in code_to_themes[code]:
                if theme_name in theme_bonus_map:
                    best_theme_bonus = max(best_theme_bonus, theme_bonus_map[theme_name])
            score += best_theme_bonus

        # ★ 5/24 사장님 시장 전망 ★ 시총 가중치 (소부장/소형주 우대)
        cap = cap_map.get(code, 0)
        if cap > 0:
            cap_band = _classify_cap_band(cap)
            score += CAP_BAND_BONUS.get(cap_band, 0)

        # ★ 5/24 사장님 지적 ★ 그룹주 로테이션 후발 가중치
        if code in group_rotation_bonus:
            score += group_rotation_bonus[code]

        # ★★★ 5/20 사장님 핵심 지적 — 오늘 등락률 기반 패널티/보너스 ★★★
        # "이게 오늘 상쳤는 종목 아니니? 내일도 오른다는 근거는 어디에서 온거니?"
        today_chg = _get_today_change_pct(code)
        if today_chg >= 25:
            score -= 50      # 상한가 = 다음날 갭다운 위험
        elif today_chg >= 15:
            score -= 10      # 큰 강세 = 조정 위험
        elif today_chg >= 5:
            score += 20      # 연속 강세 후보
        elif today_chg >= 0:
            score += 35      # ★ 미반영 강세 = 가장 안전 (눌림목/테마 동조 후보) ★
        elif today_chg >= -5:
            score += 25      # 눌림목 매수
        else:
            score -= 20      # 약세 지속 위험

        score_map[code] = score
    return score_map


def get_top_candidates(top_k: int = 5) -> List[Dict]:
    """자비스 매수 후보 TOP K (점수순).

    [자비스 활용]
      auto_trader.asset_pool_scan_and_buy() 에서 직접 호출.
      반환된 후보들에 trade_style_decider 적용 → 스타일별 매수.
    """
    score_map = get_candidate_score_map()
    source_map = get_candidate_source_map()
    # 점수순 정렬
    sorted_codes = sorted(score_map.items(), key=lambda x: -x[1])[:top_k]
    return [
        {
            "code": code,
            "score": score,
            "sources": source_map.get(code, []),
        }
        for code, score in sorted_codes
    ]


def get_diversified_candidates(
    limit_up_slots: int = 2,
    nxt_slots: int = 1,
    signal_slots: int = 2,
) -> List[Dict]:
    """★ 5/21 21:30 박사 자율 — 카테고리별 분산 매수 후보 (사장님 전략) ★

    사장님 5/21 21:00 명령:
    "상한가 엔진 2~3종목, nxt에서 강력한거 1종목, 추가로 장중에 추매 1~2종목"

    카테고리별 슬롯 보장:
      - 상한가 엔진: limit_up_watchlist + limit_up_trigger (폭발 대기 패턴)
        · 어제 +25%+ 상한가 친 종목은 -50점 패널티로 자동 제외 (5/20 사장님 영구 룰)
        · 박사 해석 B: "오늘 상한가 가능성" (사장님 5/21 21:30 확정)
      - NXT 강력: nxt_eligible TOP 1 (어젯밤 NXT 시스템 점수)
      - 자비스 시그널: massive_dual_buy + oneshot_stealth + foreign_accumulation 다중

    중복 종목은 한 슬롯에 1번만 (다른 슬롯 재배치).

    Returns:
        [{code, score, sources, category}] 카테고리별 분산된 후보 리스트
    """
    score_map = get_candidate_score_map()
    source_map = get_candidate_source_map()

    # 카테고리별 후보 풀 구성 (점수순)
    LIMIT_UP_SOURCES = {"limit_up_watchlist", "limit_up_trigger", "limit_up_signals"}
    NXT_SOURCES = {"nxt_eligible"}
    SIGNAL_SOURCES = {
        "massive_dual_buy", "oneshot_stealth", "foreign_accumulation",
        "accumulation_radar", "consecutive_surge",
        "sector_concurrent_surge", "learned_strong",
    }

    pool_limit_up = []
    pool_nxt = []
    pool_signal = []

    for code, score in score_map.items():
        sources = source_map.get(code, [])
        src_set = set(sources)
        entry = {"code": code, "score": score, "sources": sources}

        if src_set & LIMIT_UP_SOURCES:
            pool_limit_up.append(entry)
        if src_set & NXT_SOURCES:
            pool_nxt.append(entry)
        if src_set & SIGNAL_SOURCES:
            pool_signal.append(entry)

    # 점수순 정렬
    pool_limit_up.sort(key=lambda x: -x["score"])
    pool_nxt.sort(key=lambda x: -x["score"])
    pool_signal.sort(key=lambda x: -x["score"])

    selected: List[Dict] = []
    selected_codes: Set[str] = set()

    # ① 상한가 엔진 슬롯 (폭발 대기)
    for entry in pool_limit_up:
        if entry["code"] not in selected_codes and len([e for e in selected if e.get("category") == "limit_up"]) < limit_up_slots:
            entry_copy = dict(entry)
            entry_copy["category"] = "limit_up"
            selected.append(entry_copy)
            selected_codes.add(entry["code"])

    # ② NXT 강력 슬롯
    for entry in pool_nxt:
        if entry["code"] not in selected_codes and len([e for e in selected if e.get("category") == "nxt"]) < nxt_slots:
            entry_copy = dict(entry)
            entry_copy["category"] = "nxt"
            selected.append(entry_copy)
            selected_codes.add(entry["code"])

    # ③ 자비스 시그널 슬롯 (massive_dual + oneshot 등 다중 시그널)
    for entry in pool_signal:
        if entry["code"] not in selected_codes and len([e for e in selected if e.get("category") == "signal"]) < signal_slots:
            entry_copy = dict(entry)
            entry_copy["category"] = "signal"
            selected.append(entry_copy)
            selected_codes.add(entry["code"])

    # 부족하면 일반 점수순으로 채움 (graceful fallback)
    expected_total = limit_up_slots + nxt_slots + signal_slots
    if len(selected) < expected_total:
        all_sorted = sorted(score_map.items(), key=lambda x: -x[1])
        for code, score in all_sorted:
            if code not in selected_codes and len(selected) < expected_total:
                selected.append({
                    "code": code,
                    "score": score,
                    "sources": source_map.get(code, []),
                    "category": "fallback",
                })
                selected_codes.add(code)

    logger.info(
        f"[diversified] 분산 매수 후보: "
        f"상한가 {sum(1 for e in selected if e['category']=='limit_up')}/{limit_up_slots} "
        f"NXT {sum(1 for e in selected if e['category']=='nxt')}/{nxt_slots} "
        f"시그널 {sum(1 for e in selected if e['category']=='signal')}/{signal_slots} "
        f"fallback {sum(1 for e in selected if e['category']=='fallback')}"
    )
    return selected


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
