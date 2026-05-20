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
    weights = {
        "learned_strong": 50,
        "sector_concurrent_surge": 35,   # ★ 테마 동조 = 가장 안전
        "kis_market_top": 25,            # 오늘 강세 — 등락률 패널티 별도 적용
        "consecutive_surge": 20,         # 상한가 연속 = 위험 (낮춤)
        "limit_up_signals": 15,
        "short_cover": 25,
        "limit_up_trigger": 10,          # 상한가 트리거 = 추격 위험 (낮춤)
        "limit_up_watchlist": 18,        # 워치 = 눌림목 가능성 (높임)
        "massive_dual_buy": 25,          # 외인+기관 동시 = 강함
        "oneshot_stealth": 20,
        "foreign_accumulation": 20,
        "accumulation_radar": 20,
        "premium_levels": 12,
        "missed_gainers_learning": 15,
        "nxt_eligible": 3,
    }
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
