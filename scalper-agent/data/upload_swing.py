"""
FLOWX VIP 스윙시스템 → Supabase 업로드 모듈
담당 테이블: swing_signals + dashboard_swing
VIP 스윙 추천 + 모델 포트폴리오 + 분석 보고서

3-Source 융합 (v2):
  Source 1: nightwatch_report.json  → 세계 지표 + NXT 종목
  Source 2: macro_baseline.json     → 시장 상태 판정 + 전략
  Source 3: sector_momentum.json    → 섹터별 온도 (HOT/COLD)
  + recommendation.json (기존)      → 있으면 가산, 없어도 페이지 생성

BRAIN 역방향 등급 필터:
  공격 (pct>=80) → A이상, 최대5종목
  표준 (pct>=60) → AA이상, 최대3종목
  방어 (pct>=40) → AAA만, 최대2종목
  관망 (pct<40)  → 개별주 0, ETF/현금 전략+워치리스트
"""
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("flowx_swing")

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"

# ── 등급 서열 (필터링용) ──
_GRADE_ORDER = {"AAA": 0, "AA": 1, "A": 2, "BBB": 3, "BB": 4, "B": 5, "C": 6, "D": 7, "F": 8}


# ── Supabase 클라이언트 (lazy init, upload_short.py와 동일 패턴) ──
_supabase = None


def _get_client():
    global _supabase
    if _supabase is not None:
        return _supabase

    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    logger.info(f"Supabase 연결: {url[:40]}...")
    return _supabase


def _fetch_closing_prices(codes: list) -> dict:
    """NXT 종목 종가 일괄 조회 (pykrx OHLCV).
    Returns: {code: {"close": int, "chg_pct": float}} 또는 빈 dict
    """
    if not codes:
        return {}
    result = {}
    try:
        from pykrx import stock as pykrx_stock
        from data.trading_calendar import last_trading_day
        last_day = last_trading_day()
        d = last_day.strftime("%Y%m%d")
        for code in codes:
            try:
                df = pykrx_stock.get_market_ohlcv(d, d, code)
                if not df.empty:
                    row = df.iloc[-1]
                    result[code] = {
                        "close": int(row["종가"]),
                        "chg_pct": round(float(row.get("등락률", 0)), 2),
                    }
            except Exception:
                pass
        logger.info(f"[NXT] 종가 조회: {len(result)}/{len(codes)}건 성공 ({d})")
    except Exception as e:
        logger.warning(f"[NXT] 종가 조회 실패 (무시): {e}")
    return result


# ═══════════════════════════════════════
#  BRAIN 역방향 등급 필터
# ═══════════════════════════════════════

def _judge_star(score: float, grade: str, rr: float, conviction: str, stock: dict) -> bool:
    """★ 별표 판정 — 진입해도 되는 종목인지 결정

    조건 (하나라도 만족하면 star):
    1. score 60+ AND (grade AAA/AA/A)
    2. score 70+ (등급 무관)
    3. RSI 40~65 AND vol_ratio 1.5+ AND score 50+
    4. conviction HIGH
    """
    if conviction == "HIGH":
        return True
    if score >= 70:
        return True
    if score >= 60 and grade in ("AAA", "AA", "A"):
        return True
    rsi = stock.get("rsi", 50)
    vol = stock.get("vol_ratio", 0)
    if 40 <= rsi <= 65 and vol >= 1.5 and score >= 50:
        return True
    return False


def _get_swing_filter(brain_pct: int) -> tuple:
    """BRAIN 비중에 따른 (min_grade, max_picks, mode_label) 결정"""
    if brain_pct >= 80:
        return "A", 5, "공격"
    elif brain_pct >= 60:
        return "AA", 3, "표준"
    elif brain_pct >= 40:
        return "AAA", 2, "방어"
    else:
        return "NONE", 0, "관망"


def _grade_passes(stock_grade: str, min_grade: str) -> bool:
    """stock_grade가 min_grade 이상인지 확인"""
    if min_grade == "NONE":
        return False
    s_rank = _GRADE_ORDER.get(stock_grade, 99)
    m_rank = _GRADE_ORDER.get(min_grade, 99)
    return s_rank <= m_rank


# ═══════════════════════════════════════
#  메인: 스윙 페이지 데이터 생성
# ═══════════════════════════════════════

def generate_swing_page_data() -> dict:
    """recommendation.json + brain_report.json + trade_objects.json
    → FLOWX 스윙 페이지 데이터 생성"""
    today_str = date.today().strftime("%Y-%m-%d")

    # ── 1) BRAIN 로드 ──
    brain_pct = 0
    brain_verdict = "관망"
    brain_reason = ""
    analysis = {}

    brain_path = STORE_DIR / "brain_report.json"
    if brain_path.exists():
        try:
            brain = json.loads(brain_path.read_text("utf-8"))
            brain_pct = brain.get("position_size_pct", 0)
            brain_verdict_raw = brain.get("overall_verdict", "")

            # verdict 번역
            if brain_pct >= 80:
                brain_verdict = "공격"
            elif brain_pct >= 60:
                brain_verdict = "표준"
            elif brain_pct >= 40:
                brain_verdict = "방어"
            else:
                brain_verdict = "관망"

            brain_reason = brain.get("position_size_reason", brain_verdict_raw[:100])

            # 분석 보고서: Brain 6Phase narrative 추출
            analysis = {
                "macro_summary": _safe_get_narrative(brain, "macro"),
                "commodity_summary": _safe_get_narrative(brain, "commodity"),
                "sector_summary": _safe_get_narrative(brain, "sector"),
                "flow_summary": _safe_get_narrative(brain, "flow"),
                "risk_summary": _safe_get_narrative(brain, "risk"),
            }
        except Exception as e:
            logger.warning(f"brain_report.json 로드 실패: {e}")

    # ── 2) 역방향 필터 결정 ──
    min_grade, max_picks, mode_label = _get_swing_filter(brain_pct)
    logger.info(f"[FLOWX 스윙] BRAIN {brain_pct}% → {mode_label} 모드 (min={min_grade}, max={max_picks})")

    # ── 3) recommendation.json 로드 ──
    rec_path = STORE_DIR / "recommendation.json"
    rec_data = {}
    if rec_path.exists():
        try:
            rec_data = json.loads(rec_path.read_text("utf-8"))
        except Exception as e:
            logger.warning(f"recommendation.json 로드 실패: {e}")

    stocks = rec_data.get("stocks", [])
    etf_recs = rec_data.get("etf_recommendations", [])

    # ── 4) trade_objects.json 로드 ──
    to_map = {}  # code → trade_object dict
    to_path = STORE_DIR / "trade_objects.json"
    if to_path.exists():
        try:
            to_data = json.loads(to_path.read_text("utf-8"))
            for t in to_data.get("trades", []):
                to_map[t.get("code", "")] = t
        except Exception:
            pass

    # ── 4b) universe.json 로드 (sector 매핑용) ──
    universe = {}  # code → {name, sector, ...}
    uni_path = STORE_DIR / "universe.json"
    if uni_path.exists():
        try:
            universe = json.loads(uni_path.read_text("utf-8"))
        except Exception:
            pass

    # ── 5) KRX 주간 종목 (recommendation.json 기반) ──
    # 등급 필터 없이 전부 표시 (star로 구분)
    picks = []
    for s in stocks:
        code = s.get("code", "")
        to = to_map.get(code, {})
        grade = s.get("grade", "")

        # universe에서 sector 조회
        uni_info = universe.get(code, {})
        sector = uni_info.get("sector", "") if isinstance(uni_info, dict) else ""

        score = round(s.get("total_score", s.get("score", 0)), 1)
        rr = round(to.get("rr_ratio", 0), 2)
        conviction = _calc_conviction(s, to)
        entry = s.get("entry", 0)
        tp = s.get("tp", s.get("tp1", 0))
        sl = s.get("sl", 0)

        # ★ 별표 판정: 진입 추천 여부
        star = _judge_star(score, grade, rr, conviction, s)

        # 한국어 레이블
        _확신 = {"HIGH": "확신 높음", "MEDIUM": "보통", "LOW": "낮음"}
        _행동 = "매수" if star else ("관심매수" if score >= 40 else "관찰")

        pick = {
            "code": code,
            "name": s.get("name", ""),
            "category": "KRX",
            "category_label": "주간 매매",
            "star": star,
            "action": _행동,
            "grade": grade,
            "score": score,
            "sector": sector,
            "rr_ratio": rr,
            # 지시서 호환 필드 (entry/sl/tp)
            "entry": int(entry) if entry else 0,
            "sl": int(sl) if sl else 0,
            "tp": int(tp) if tp else 0,
            # 기존 필드 (하위 호환)
            "entry_price": int(entry) if entry else 0,
            "target_price": int(tp) if tp else 0,
            "stop_price": int(sl) if sl else 0,
            "hold_days": to.get("expected_hold_days", s.get("hold_days", 3)),
            "conviction": conviction,
            "conviction_label": _확신.get(conviction, "보통"),
            "reason": _build_catalyst(s),
            "catalyst": _build_catalyst(s),
            "reasons": s.get("reasons", []),
            "close": s.get("close", 0),
            "chg_pct": round(s.get("chg_pct", 0), 1),
            "rsi": round(s.get("rsi", 0), 1),
            "vol_ratio": round(s.get("vol_ratio", 0), 1),
            "source": s.get("source", ""),
            # 피보나치 레벨
            "fib_position": s.get("fib_position", ""),
            "fib_upside_pct": round(s.get("fib_upside_pct", 0), 1),
            "fib_downside_pct": round(s.get("fib_downside_pct", 0), 1),
            "sl_fib": s.get("sl_fib", 0),
            "tp_fib": s.get("tp_fib", 0),
            "fib_adj": round(s.get("fib_adj", 0), 1),
        }
        picks.append(pick)

    # ── 5b) NXT 야간 종목 (nightwatch 기반, 항상 추가) ──
    nxt_data = _load_json("nightwatch_report.json")
    nxt_targets = nxt_data.get("nxt_targets", [])
    nxt_codes = {p["code"] for p in picks}  # KRX와 중복 방지

    # NXT 종목 종가 일괄 조회
    nxt_new_codes = [t.get("code", "") for t in nxt_targets
                     if t.get("code", "") and t.get("code", "") not in nxt_codes]
    nxt_prices = _fetch_closing_prices(nxt_new_codes)

    for t in nxt_targets:
        code = t.get("code", "")
        if code in nxt_codes:
            continue
        nxt_codes.add(code)

        tier = t.get("tier", 3)
        is_etf = t.get("is_etf", False)
        sector_key = t.get("sector_key", t.get("sector", ""))
        name = t.get("name", "")

        # NXT star: Tier1 → star
        star = tier <= 1

        _분류명 = {
            "inverse": "인버스 (하락 대비)",
            "precious_metals": "금/귀금속",
            "oil_resource": "원유/에너지",
            "shipbuilding": "조선/방산",
            "commodity_etf_gold": "금 ETF",
            "commodity_etf_oil": "원유 ETF",
        }

        # 종가 기반 진입가/TP/SL 계산 (NXT: TP+3%, SL-2.5%)
        price_info = nxt_prices.get(code, {})
        close_price = price_info.get("close", 0)
        chg_pct = price_info.get("chg_pct", 0)
        entry = close_price
        tp = int(entry * 1.03) if entry > 0 else 0
        sl = int(entry * 0.975) if entry > 0 else 0
        rr = round((tp - entry) / (entry - sl), 1) if entry > 0 and entry > sl else 0
        # NXT 스코어: supply_score(0~100) 기반
        supply_sc = t.get("supply_score", 0)
        nxt_score = round(supply_sc * 0.7 + (20 if tier <= 1 else 10), 1)

        picks.append({
            "code": code,
            "name": name,
            "category": "NXT",
            "category_label": "야간 매매",
            "star": star,
            "action": "매수" if star else "관심매수",
            "grade": f"Tier{tier}",
            "score": nxt_score,
            "sector": _분류명.get(sector_key, sector_key),
            "rr_ratio": rr,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "entry_price": entry,
            "target_price": tp,
            "stop_price": sl,
            "hold_days": 1,
            "conviction": "HIGH" if tier <= 1 else "MEDIUM",
            "conviction_label": "확신 높음" if tier <= 1 else "보통",
            "reason": _분류명.get(sector_key, "야간 신호"),
            "catalyst": _분류명.get(sector_key, "야간 신호"),
            "reasons": [f"NXT Tier{tier}", _분류명.get(sector_key, "")],
            "close": close_price,
            "chg_pct": round(chg_pct, 2),
            "rsi": 0,
            "vol_ratio": 0,
            "source": "NXT",
            "is_etf": is_etf,
            "supply_score": supply_sc,
            "fib_position": "",
            "fib_upside_pct": 0,
            "fib_downside_pct": 0,
            "sl_fib": 0,
            "tp_fib": 0,
            "fib_adj": 0,
        })

    # picks 정렬: star 먼저, 그 다음 score 순 → 최대 10종목 (지시서 기준)
    picks.sort(key=lambda x: (-int(x.get("star", False)), -x.get("score", 0)))
    picks = picks[:10]

    logger.info(
        f"[FLOWX 스윙] picks: KRX {sum(1 for p in picks if p.get('category')=='KRX')}개 + "
        f"NXT {sum(1 for p in picks if p.get('category')=='NXT')}개 = {len(picks)}개 "
        f"(star {sum(1 for p in picks if p.get('star'))}개)"
    )

    # ── 6) ETF picks (모든 모드에서 표시) ──
    etf_picks = []
    for e in etf_recs:
        if e.get("signal") != "BUY":
            continue
        etf_picks.append({
            "code": e.get("code", ""),
            "name": e.get("name", ""),
            "category": e.get("category", ""),
            "signal": "매수",
            "entry": e.get("entry", 0),
            "sl": e.get("sl", 0),
            "tp": e.get("tp", 0),
            "reason": e.get("reason", ""),
            "holding_days": e.get("holding_days", 5),
        })
    etf_picks = etf_picks[:5]  # 최대 5종목 (지시서 기준)

    # ── 7) 모델 포트폴리오 (학습 데이터 기반) ──
    portfolio = _build_portfolio_stats(brain_pct, len(picks))

    # ── 8) 관망 모드 market_comment + watchlist ──
    if brain_pct < 40:
        market_comment = "방향 불명확 — 현금이 포지션입니다"
        watchlist = _build_watchlist(stocks[:3], universe)
    else:
        market_comment = brain_reason[:200] if brain_reason else ""
        watchlist = []

    result = {
        "date": today_str,
        "brain_verdict": brain_verdict,
        "brain_pct": brain_pct,
        "brain_reason": brain_reason[:300],
        "min_grade_applied": min_grade,
        "market_comment": market_comment,
        "picks": picks,
        "etf_picks": etf_picks,
        "portfolio": portfolio,
        "analysis": analysis,
        "watchlist": watchlist,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ── 9) 3-Source 매크로 융합 (v2) ──
    try:
        result = _enrich_with_macro(result)
    except Exception as e:
        logger.warning(f"[FLOWX 스윙] 매크로 융합 실패 (무시): {e}")

    # 로컬 저장 (디버깅용)
    local_path = STORE_DIR / "flowx_swing.json"
    try:
        tmp = local_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(local_path)
        logger.info(f"[FLOWX 스윙] 로컬 저장: {local_path}")
    except Exception as e:
        logger.warning(f"로컬 저장 실패: {e}")

    logger.info(
        f"[FLOWX 스윙] {result.get('brain_verdict', '?')} 모드 | "
        f"{len(result.get('picks', []))}종목 + {len(result.get('etf_picks', []))} ETF | "
        f"매크로:{result.get('analysis', {}).get('시장상태', '?')}"
    )
    return result


# ═══════════════════════════════════════
#  3-Source 융합: 매크로 + 섹터 + 나이트워치
# ═══════════════════════════════════════

def _load_json(filename: str) -> dict:
    """data_store에서 JSON 안전 로드"""
    path = STORE_DIR / filename
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _build_macro_report() -> dict:
    """세계 경제 지표 + 시장 상태 판정

    macro_baseline.json → 6개 지표 (유가/금/구리/VIX/금리/환율)
    inflation_chain     → 시장 상태 (물가상승+경기둔화 / 비용상승 / 수요둔화 / 안정)
    macro_strategy      → 대응 전략 (비중/손절/보유일/선호·회피 섹터)
    """
    baseline = _load_json("macro_baseline.json")
    if not baseline:
        return {}

    # 방향 한국어 변환
    _방향 = {
        "STRONG_UP": "급등", "UP": "상승", "FLAT": "보합",
        "DOWN": "하락", "STRONG_DOWN": "급락", "보합": "보합",
    }

    # 6개 핵심 지표
    지표목록 = {}
    지표키 = {
        "oil": "유가", "gold": "금", "copper": "구리",
        "vix": "공포지수", "tnx": "미국금리", "usdkrw": "환율",
    }
    for key, 이름 in 지표키.items():
        item = baseline.get(key, {})
        if not item:
            continue
        지표목록[이름] = {
            "값": round(item.get("current", 0), 2),
            "20일평균_차이": round(item.get("dev20_pct", 0), 1),
            "60일평균_차이": round(item.get("dev60_pct", 0), 1),
            "방향": _방향.get(item.get("trend_20d", ""), "보합"),
            "위치": round(item.get("pct_rank_60d", 50), 0),  # 0~100 (100=최고점)
        }

    # 시장 상태 판정 (inflation_chain)
    시장상태 = "안정"
    try:
        from data.macro_strategy import get_current_regime, get_regime_response
        시장상태 = get_current_regime()
        전략 = get_regime_response(시장상태)
    except Exception:
        from dataclasses import dataclass, field
        전략 = None

    # 전략 → 쉬운 한국어
    전략요약 = {}
    if 전략:
        _상태이름 = {
            "스태그플레이션": "물가상승 + 경기둔화 (최고 위험)",
            "비용상승": "원자재 가격 상승 (주의)",
            "수요둔화": "소비 위축 (주의)",
            "안정": "정상 (표준 운영)",
        }
        전략요약 = {
            "상태": 시장상태,
            "설명": _상태이름.get(시장상태, 시장상태),
            "투자비중": 전략.max_position_pct,
            "손절": round(전략.sl_pct * 100, 1),
            "추적손절": round(전략.trailing_pct * 100, 1),
            "최대보유일": 전략.max_hold_days,
            "선호섹터": 전략.preferred_classes,
            "회피섹터": 전략.blacklist_classes,
            "방어ETF": 전략.etf_defensive,
            "추천ETF종류": 전략.etf_preferred_types,
            "안내문": 전략.action_summary,
        }

    return {
        "지표": 지표목록,
        "시장상태": 시장상태,
        "전략": 전략요약,
        "기준시각": baseline.get("timestamp", ""),
    }


def _build_category_picks() -> list:
    """나이트워치 NXT 종목 → 카테고리별 묶음

    인버스/헤지, 금/귀금속, 원유/에너지, 조선/방산, 금ETF, 원유ETF
    """
    nxt = _load_json("nightwatch_report.json")
    targets = nxt.get("nxt_targets", [])
    if not targets:
        return []

    # 섹터키 → 알기 쉬운 분류명
    _분류 = {
        "inverse": "인버스 (시장 하락 대비)",
        "precious_metals": "금/귀금속 (안전자산)",
        "oil_resource": "원유/에너지 (유가 수혜)",
        "shipbuilding": "조선/방산 (지정학 수혜)",
        "commodity_etf_gold": "금 ETF",
        "commodity_etf_oil": "원유 ETF",
    }
    # 섹터키별 우선순위 (낮을수록 먼저)
    _우선순위 = {
        "inverse": 1, "precious_metals": 2, "oil_resource": 3,
        "shipbuilding": 4, "commodity_etf_gold": 5, "commodity_etf_oil": 6,
    }

    # 그룹핑
    groups = {}
    for t in targets:
        sk = t.get("sector_key", "")
        if sk not in _분류:
            continue
        if sk not in groups:
            groups[sk] = {
                "분류": _분류[sk],
                "순위": _우선순위.get(sk, 99),
                "종목": [],
            }
        groups[sk]["종목"].append({
            "코드": t.get("code", ""),
            "이름": t.get("name", ""),
            "등급": f"Tier{t.get('tier', 0)}",
            "ETF": t.get("is_etf", False),
        })

    # 순위순 정렬, Tier1 종목 우선
    result = sorted(groups.values(), key=lambda x: x["순위"])
    for g in result:
        g["종목"] = sorted(g["종목"], key=lambda x: x["등급"])

    return result


def _build_sector_heatmap() -> list:
    """섹터 온도계 — HOT/따뜻/보통/차가움 시각화용

    sector_momentum.json → 22개 섹터 수익률 + 상태
    """
    sm = _load_json("sector_momentum.json")
    sectors = sm.get("sectors", [])
    if not sectors:
        return []

    _상태 = {"HOT": "뜨거움", "WARMING": "따뜻", "NEUTRAL": "보통", "COLD": "차가움"}

    result = []
    for s in sectors[:22]:  # 상위 22개
        top3 = []
        for m in s.get("top_movers", [])[:3]:
            top3.append({
                "이름": m.get("name", ""),
                "등락": round(m.get("chg_1d", 0), 1),
            })
        result.append({
            "섹터": s.get("sector", ""),
            "수익률": round(s.get("avg_return_1d", 0), 2),
            "상태": _상태.get(s.get("phase", "NEUTRAL"), "보통"),
            "상승비율": round(s.get("breadth_1d", 0) * 100),
            "대표종목": top3,
        })

    return result


def _build_action_guide(macro_regime: str, nxt_score: float) -> list:
    """시간대별 매매 안내 (쉬운 한국어)"""
    guide = []

    if nxt_score <= -5:
        # 강한 하락 신호
        guide = [
            {"시간": "09:00~09:15", "행동": "시장 방향 확인 → 인버스/금 ETF 먼저 진입"},
            {"시간": "09:15~09:30", "행동": "원유/에너지주 → 많이 빠졌으면 조금씩 분할 매수"},
            {"시간": "09:30~10:00", "행동": "방어주(통신/제약) 진입 검토"},
            {"시간": "10:00~11:00", "행동": "TV 스캔 결과 확인 후 추가 조정"},
            {"시간": "종일", "행동": "인버스 보유 유지 (보험 역할)"},
        ]
    elif nxt_score <= 0:
        # 약한 하락 / 중립
        guide = [
            {"시간": "09:00~09:15", "행동": "전날 추천종목 갭 확인"},
            {"시간": "09:15~10:00", "행동": "1/3씩 분할 매수 (한 번에 몰빵 금지)"},
            {"시간": "10:00~11:00", "행동": "거래량 터지는 종목 실시간 확인"},
            {"시간": "14:00~15:00", "행동": "당일 손절선 확인 → 다음날 전략 준비"},
        ]
    else:
        # 상승 신호
        guide = [
            {"시간": "09:00~09:15", "행동": "추천종목 1순위부터 순서대로 진입"},
            {"시간": "09:15~10:00", "행동": "모멘텀 강한 종목 추가 진입"},
            {"시간": "10:00~14:00", "행동": "목표가 도달 시 일부 익절"},
            {"시간": "14:30~15:00", "행동": "내일 연속 상승 가능한 종목 보유 유지"},
        ]

    # 매크로 상태별 추가 안내
    if macro_regime == "스태그플레이션":
        guide.append({"시간": "주의사항", "행동": "물가상승+경기둔화 → 3일 안에 청산, 손절 -2%로 타이트하게"})
    elif macro_regime == "비용상승":
        guide.append({"시간": "주의사항", "행동": "원자재 비용 상승 → 에너지/원자재 주 선호, 전기가스/운송 주의"})

    return guide


def _build_warnings(nxt: dict, macro_report: dict) -> list:
    """핵심 경고 목록 (쉬운 한국어)"""
    warnings = []

    # 나이트워치 경고
    reentry = nxt.get("reentry_signals", {})
    if reentry.get("war_gate_active"):
        warnings.append("전쟁 게이트 발동 — 유가+공포지수 위험 수준")

    raw = nxt.get("raw_indicators", {})
    vix = raw.get("VIX", {}).get("value", 0) or 0
    if vix >= 30:
        warnings.append(f"공포지수(VIX) {vix:.1f} — 극도의 불안 구간")
    elif vix >= 25:
        warnings.append(f"공포지수(VIX) {vix:.1f} — 불안 구간")

    oil = raw.get("CL", {}).get("value", 0) or 0
    if oil >= 95:
        warnings.append(f"유가 ${oil:.1f} — $100 돌파 임박, 추가 급등 가능")

    macro = nxt.get("macro_conditions", {})
    nq_pct = macro.get("nasdaq_pct", 0) or 0
    if nq_pct <= -1.5:
        warnings.append(f"나스닥 {nq_pct:+.1f}% 급락 → 월요일 갭하락 가능")

    usdkrw = raw.get("USDKRW", {}).get("value", 0) or 0
    if usdkrw >= 1500:
        warnings.append(f"환율 {usdkrw:,.0f}원 — 원화 약세, 외국인 이탈 주의")

    # 매크로 상태 경고
    시장상태 = macro_report.get("시장상태", "안정")
    if 시장상태 == "스태그플레이션":
        warnings.append("시장 상태: 물가상승+경기둔화 — 최소 비중으로 방어")
    elif 시장상태 == "비용상승":
        warnings.append("시장 상태: 비용 상승 중 — 에너지 외 섹터 주의")

    # 오래된 데이터 경고
    nxt_ts = nxt.get("timestamp", "")
    if nxt_ts:
        try:
            nxt_dt = datetime.strptime(nxt_ts[:19], "%Y-%m-%d %H:%M:%S")
            age_hours = (datetime.now() - nxt_dt).total_seconds() / 3600
            if age_hours > 48:
                warnings.append(f"데이터 {age_hours:.0f}시간 전 기준 — 최신 데이터 아님, 참고만")
        except (ValueError, TypeError):
            pass

    return warnings


def _build_nxt_rationale(nxt: dict) -> dict:
    """채권자경단 v2 데이터 → 프론트 표시용 nxt_rationale JSONB 생성.
    nightwatch_report.json의 bond_vigilante 필드에서 읽음.
    """
    bv = nxt.get("bond_vigilante", {})
    if not bv or not bv.get("signals"):
        return {}

    signals = bv["signals"]
    summary = bv.get("summary", {})

    # 지표별 한국어 매핑
    _이름 = {
        "move": "MOVE 채권공포",
        "vix_term": "VIX 기간구조",
        "cu_au": "구리/금 비율",
        "jpy_carry": "엔 캐리트레이드",
        "vvix": "VVIX 스마트머니",
        "credit_spread": "신용스프레드",
        "btc": "BTC 야간심리",
    }
    _신호색 = {"GREEN": "안전", "YELLOW": "경계", "RED": "위험"}

    indicators = []
    for key, name in _이름.items():
        sig = signals.get(key, {})
        if not sig:
            continue
        signal = sig.get("signal", "YELLOW")
        # 지표별 핵심 수치
        if key == "move":
            detail = f"{sig.get('value', 0)}"
        elif key == "vix_term":
            detail = f"VIX {sig.get('vix', 0)} / VIX3M {sig.get('vix3m', 0)} ({sig.get('structure', '')})"
        elif key == "cu_au":
            detail = f"구리 {sig.get('copper_chg', 0):+.1f}% vs 금 {sig.get('gold_chg', 0):+.1f}%"
        elif key == "jpy_carry":
            detail = f"JPY {sig.get('value', 0)} ({sig.get('carry', '')})"
        elif key == "vvix":
            detail = f"{sig.get('value', 0)} (MA20 대비 {sig.get('vs_ma20_pct', 0):+.1f}%)"
        elif key == "credit_spread":
            detail = f"HYG {sig.get('hyg_chg', 0):+.2f}% vs LQD {sig.get('lqd_chg', 0):+.2f}%"
        elif key == "btc":
            detail = f"${sig.get('value', 0):,.0f} ({sig.get('chg_pct', 0):+.1f}%)"
        else:
            detail = ""

        indicators.append({
            "key": key,
            "name": name,
            "signal": signal,
            "signal_label": _신호색.get(signal, "경계"),
            "detail": detail,
        })

    return {
        "timestamp": bv.get("timestamp", ""),
        "verdict": summary.get("verdict", ""),
        "green": summary.get("green", 0),
        "yellow": summary.get("yellow", 0),
        "red": summary.get("red", 0),
        "total": summary.get("total", 0),
        "indicators": indicators,
    }


def _build_fib_stocks() -> list:
    """bottom_scan.json → 피보나치 눌림목 종목 (프론트 패널용).
    fib_zone별 그룹핑, DEEP 우선, 최대 30종목.
    """
    data = _load_json("bottom_scan.json")
    if not data:
        return []

    # bottom_scan.json은 리스트
    if isinstance(data, dict):
        stocks = data.get("stocks", [])
    else:
        stocks = data

    if not stocks:
        return []

    # fib_zone 정렬 우선순위 (깊은 하락 먼저)
    _zone_order = {"DEEP": 0, "MID": 1, "MILD": 2, "SHALLOW": 3}

    _zone_label = {
        "DEEP": "50%+ 하락 (바닥 매수 구간)",
        "MID": "40~50% 하락 (중간 눌림)",
        "MILD": "30~40% 하락 (1차 눌림)",
        "SHALLOW": "15~30% 하락 (얕은 조정)",
    }

    result = []
    for s in stocks:
        zone = s.get("fib_zone", "SHALLOW")
        price = s.get("price", 0)
        fib_382 = s.get("fib_382", 0)
        fib_500 = s.get("fib_500", 0)
        fib_618 = s.get("fib_618", 0)

        # 현재가 대비 피보나치 레벨 위치 판정
        if price <= fib_382:
            fib_status = "38.2% 아래 (깊은 하락)"
        elif price <= fib_500:
            fib_status = "38.2%~50% 사이"
        elif price <= fib_618:
            fib_status = "50%~61.8% 사이"
        else:
            fib_status = "61.8% 위 (회복 중)"

        result.append({
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "sector": s.get("sector", ""),
            "cap": s.get("cap", 0),
            "price": price,
            "w52h": s.get("w52h", 0),
            "w52l": s.get("w52l", 0),
            "drop": round(s.get("drop", 0), 1),
            "fib_zone": zone,
            "fib_zone_label": _zone_label.get(zone, zone),
            "fib_382": fib_382,
            "fib_500": fib_500,
            "fib_618": fib_618,
            "fib_status": fib_status,
            "target": s.get("target_peace", 0),
            "upside": round(s.get("upside", 0), 1),
            "per": round(s.get("per", 0), 1),
            "pbr": round(s.get("pbr", 0), 2),
            "frgn": round(s.get("frgn", 0), 1),
            "_sort": (_zone_order.get(zone, 9), s.get("drop", 0)),
        })

    # DEEP 우선 + 같은 zone 내에서 drop이 큰 순 (더 많이 빠진 것 먼저)
    result.sort(key=lambda x: (x["_sort"][0], x["_sort"][1]))

    # _sort 제거 + 최대 30종목
    for r in result:
        del r["_sort"]

    return result[:50]


def _enrich_with_macro(result: dict) -> dict:
    """기존 스윙 데이터에 매크로/섹터/카테고리 융합

    brain이 없어도 나이트워치+매크로로 풍성한 페이지 생성
    """
    nxt = _load_json("nightwatch_report.json")

    # 1) 매크로 보고서
    macro_report = _build_macro_report()

    # 2) 카테고리별 추천 (나이트워치 기반)
    카테고리 = _build_category_picks()

    # 3) 섹터 온도계
    섹터온도 = _build_sector_heatmap()

    # 4) 나이트워치 요약
    nxt_score = nxt.get("total_score", 0) or 0
    nxt_summary = {
        "점수": nxt_score,
        "신호": nxt.get("signal", ""),
        "의미": nxt.get("signal_text", ""),
        "판단근거": nxt.get("selection_reason", ""),
        "추천섹터": nxt.get("recommended_sectors", []),
    }

    # 5) 매매 안내
    시장상태 = macro_report.get("시장상태", "안정")
    매매안내 = _build_action_guide(시장상태, nxt_score)

    # 6) 경고
    경고 = _build_warnings(nxt, macro_report)

    # 7) 시장 한줄 요약
    시장요약 = _make_market_summary(nxt, macro_report)

    # analysis 필드: 문자열만! (프론트가 값을 직접 렌더링 → 객체 넣으면 React #31 에러)
    analysis = result.get("analysis", {})
    analysis["시장상태"] = 시장상태
    analysis["시장요약"] = 시장요약
    analysis["경고"] = " | ".join(경고) if 경고 else ""
    analysis["기준시각"] = macro_report.get("기준시각", "")

    # 전략 → 한줄 문자열
    전략 = macro_report.get("전략", {})
    if 전략:
        analysis["전략요약"] = (
            f"{전략.get('설명', '')} | "
            f"투자비중 {전략.get('투자비중', 100)}% | "
            f"손절 -{전략.get('손절', 3.5)}% | "
            f"최대 {전략.get('최대보유일', 5)}일"
        )

    # 나이트워치 → 한줄 문자열
    analysis["나이트워치"] = (
        f"{nxt_summary.get('신호', '')} {nxt_summary.get('의미', '')} "
        f"({nxt_score:+.0f}점) — {nxt_summary.get('판단근거', '')}"
    )

    # 매매안내 → 한줄 문자열
    if 매매안내:
        analysis["매매안내"] = " | ".join(
            f"[{a['시간']}] {a['행동']}" for a in 매매안내
        )

    result["analysis"] = analysis

    # 복잡한 객체는 result 최상위에 별도 저장 (dashboard_swing 전용)
    result["_macro_fusion"] = {
        "매크로_지표": macro_report.get("지표", {}),
        "전략": 전략,
        "카테고리별_추천": 카테고리,
        "섹터_온도계": 섹터온도,
        "나이트워치_상세": nxt_summary,
        "매매안내_상세": 매매안내,
        "경고_목록": 경고,
    }

    # NXT fallback 불필요 — picks는 generate_swing_page_data 섹션 5b에서 항상 추가됨

    # market_comment 보강
    if not result.get("market_comment") or result["market_comment"] == "방향 불명확 — 현금이 포지션입니다":
        result["market_comment"] = 시장요약

    return result


def _make_market_summary(nxt: dict, macro_report: dict) -> str:
    """시장 한줄 요약 생성"""
    parts = []

    시장상태 = macro_report.get("시장상태", "안정")
    전략 = macro_report.get("전략", {})

    nxt_score = nxt.get("total_score", 0) or 0
    nxt_text = nxt.get("signal_text", "")

    # 나이트워치 신호
    if nxt_score <= -5:
        parts.append(f"야간 {nxt_text}({nxt_score:+.0f}점)")
    elif nxt_score >= 5:
        parts.append(f"야간 {nxt_text}({nxt_score:+.0f}점)")

    # 매크로 상태
    if 시장상태 != "안정":
        설명 = 전략.get("설명", 시장상태)
        parts.append(설명)

    # 핵심 지표 변동
    지표 = macro_report.get("지표", {})
    유가 = 지표.get("유가", {})
    if 유가.get("60일평균_차이", 0) > 20:
        parts.append(f"유가 60일 대비 +{유가['60일평균_차이']:.0f}%")

    공포 = 지표.get("공포지수", {})
    if 공포.get("값", 0) >= 25:
        parts.append(f"VIX {공포['값']:.0f}")

    # 투자 안내
    비중 = 전략.get("투자비중", 100)
    if 비중 < 100:
        parts.append(f"투자비중 {비중}%로 축소")

    if parts:
        return " | ".join(parts)
    return "시장 안정 — 표준 전략 유지"


# ═══════════════════════════════════════
#  헬퍼 함수
# ═══════════════════════════════════════

def _safe_get_narrative(brain: dict, phase_key: str) -> str:
    """brain_report에서 phase별 narrative 안전 추출"""
    phase = brain.get(phase_key, {})
    if isinstance(phase, dict):
        return phase.get("narrative", "")
    return ""


def _calc_conviction(stock: dict, trade_obj: dict) -> str:
    """확신도 계산 (HIGH/MEDIUM/LOW)"""
    score = stock.get("total_score", 0)
    confidence = stock.get("confidence", "LOW")
    rr = trade_obj.get("rr_ratio", 0)

    if score >= 90 and confidence == "HIGH" and rr >= 2.0:
        return "HIGH"
    elif score >= 70 and rr >= 1.5:
        return "MEDIUM"
    else:
        return "LOW"


def _build_catalyst(stock: dict) -> str:
    """촉매(catalyst) 문자열 생성"""
    parts = []
    sources = stock.get("sources", [])
    if sources:
        # 상위 3개 소스만
        parts.extend(sources[:3])

    regime = stock.get("regime", "")
    if regime == "MOMENTUM":
        parts.append("모멘텀")

    tv_pattern = stock.get("tv_pattern", "")
    if tv_pattern == "QUIET_ACCUMULATION":
        parts.append("조용한매집")
    elif tv_pattern == "EXPLOSION":
        parts.append("거래대금폭발")

    return " + ".join(parts[:4]) if parts else "기술적 시그널"


def _extract_news_sentiment(stock: dict) -> str:
    """뉴스 감성 추출"""
    news = stock.get("news_detail", "")
    if isinstance(news, dict):
        return news.get("sentiment", "NEUTRAL")
    if isinstance(news, str):
        if "POSITIVE" in news.upper():
            return "POSITIVE"
        elif "NEGATIVE" in news.upper():
            return "NEGATIVE"
    return "NEUTRAL"


def _build_portfolio_stats(brain_pct: int, current_picks: int) -> dict:
    """모델 포트폴리오 통계 (learning 데이터 기반)"""
    # patterns.json에서 적중률
    win_rate = 0
    total_trades = 0
    patterns_path = STORE_DIR / "learning" / "patterns.json"
    if patterns_path.exists():
        try:
            patterns = json.loads(patterns_path.read_text("utf-8"))
            # 전체 적중률 계산
            total_wins = 0
            total_count = 0
            for key, val in patterns.items():
                if isinstance(val, dict) and "count" in val and "wins" in val:
                    total_count += val["count"]
                    total_wins += val["wins"]
            if total_count > 0:
                win_rate = round(total_wins / total_count * 100, 1)
                total_trades = total_count
        except Exception:
            pass

    # ETF 성과
    etf_perf_path = STORE_DIR / "learning" / "etf_performance.json"
    etf_trades = 0
    if etf_perf_path.exists():
        try:
            etf_perf = json.loads(etf_perf_path.read_text("utf-8"))
            etf_trades = len(etf_perf.get("history", []))
        except Exception:
            pass

    return {
        "win_rate": win_rate,
        "total_trades": total_trades + etf_trades,
        "current_picks": current_picks,
        "brain_cash_ratio": max(0, 100 - brain_pct),
        "brain_pct": brain_pct,
    }


def _build_watchlist(top_stocks: list, universe: dict = None) -> list:
    """관망 모드용 워치리스트 (매수 시그널 아님, 반등 감시)"""
    universe = universe or {}
    watchlist = []
    for s in top_stocks:
        code = s.get("code", "")
        name = s.get("name", "")
        entry = s.get("entry", 0)
        tp = s.get("tp", 0)
        uni_info = universe.get(code, {})
        sector = uni_info.get("sector", "") if isinstance(uni_info, dict) else ""

        # 반등 트리거: 진입가 돌파 시
        trigger = f"종가 {entry:,}원 돌파 + 거래량 증가 시 진입 검토" if entry else "수급 전환 확인 시"

        reasons = []
        if s.get("regime") == "MOMENTUM":
            reasons.append("모멘텀 레짐")
        if s.get("nat_power_grade") in ("POWER_BUY", "BUY"):
            reasons.append("외국인 수급 양호")
        if s.get("tv_pattern") in ("QUIET_ACCUMULATION", "EXPLOSION"):
            reasons.append("거래대금 이상")
        if not reasons:
            reasons.append("기술적 반등 대기")

        watchlist.append({
            "code": code,
            "name": name,
            "sector": sector,
            "reason": "반등 감시 — " + ", ".join(reasons),
            "trigger": trigger,
            "grade": s.get("grade", ""),
            "score": round(s.get("total_score", 0), 1),
        })

    return watchlist


# ═══════════════════════════════════════
#  Supabase 업로드
# ═══════════════════════════════════════

def upload_swing_to_supabase(data: dict) -> bool:
    """swing_signals 테이블에 upsert (date 기준)"""
    client = _get_client()
    if not client:
        return False

    try:
        # JSONB 필드는 Python dict/list 그대로 전달 (supabase 클라이언트가 직렬화)
        row = {
            "date": data["date"],
            "brain_verdict": data["brain_verdict"],
            "brain_pct": data["brain_pct"],
            "brain_reason": data.get("brain_reason", ""),
            "min_grade_applied": data["min_grade_applied"],
            "market_comment": data.get("market_comment", ""),
            "picks": data.get("picks", []),
            "etf_picks": data.get("etf_picks", []),
            "portfolio": data.get("portfolio", {}),
            "analysis": data.get("analysis", {}),
            "watchlist": data.get("watchlist", []),
        }

        result = client.table("swing_signals").upsert(
            row, on_conflict="date"
        ).execute()

        logger.info(
            f"[FLOWX 스윙] Supabase 업로드 완료: "
            f"{data['brain_verdict']} 모드, {len(data.get('picks', []))}종목"
        )
        return True

    except Exception as e:
        logger.error(f"[FLOWX 스윙] Supabase 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════
#  dashboard_swing 통합 업로드
# ═══════════════════════════════════════

def _load_nxt_data() -> dict:
    """nightwatch_report.json → NXT 데이터"""
    path = STORE_DIR / "nightwatch_report.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _load_allocation_data() -> dict:
    """brain_allocation.json → 자산배분 + 센서 데이터"""
    path = STORE_DIR / "brain_allocation.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def upload_dashboard_swing(swing_data: dict) -> bool:
    """dashboard_swing 테이블에 통합 upsert
    swing_data + NXT + Brain Allocation + 시장지표 병합"""
    client = _get_client()
    if not client:
        return False

    try:
        nxt = _load_nxt_data()
        alloc = _load_allocation_data()
        alloc_pct = alloc.get("allocation_pct", {})
        raw = nxt.get("raw_indicators", {})
        macro = nxt.get("macro_conditions", {})

        # allocation_pct 미존재 시 position_size_pct에서 파생
        if not alloc_pct:
            pct = alloc.get("position_size_pct", 0)
            regime = alloc.get("effective_regime", "관망")
            cash = 100 - pct
            # 전쟁/방어 시 gold/inverse 비중 확보
            if regime in ("방어", "최소") and pct > 0:
                gold = min(10, pct // 5)
                inverse = min(10, pct // 5)
                swing = pct - gold - inverse
            elif regime == "관망":
                gold, inverse, swing = 0, 0, 0
            else:
                gold = min(5, pct // 10)
                inverse = 0
                swing = pct - gold
            alloc_pct = {
                "bh_swing": max(0, swing),
                "gold_etf": gold,
                "inverse_etf": inverse,
                "group_etf": 0,
                "small_cap": 0,
                "cash": cash,
            }
            logger.info(f"[SWING] allocation 파생: {regime}({pct}%) → "
                        f"swing={swing} gold={gold} inv={inverse} cash={cash}")

        # NXT targets에서 supply_score 포함
        nxt_targets = nxt.get("nxt_targets", [])
        nxt_targets_clean = []
        for t in nxt_targets:
            nxt_targets_clean.append({
                "code": t.get("code", ""),
                "name": t.get("name", ""),
                "sector": t.get("sector", ""),
                "tier": t.get("tier", 0),
                "priority": t.get("priority", 0),
                "supply_score": t.get("supply_score", 0),
                "is_etf": t.get("is_etf", False),
            })

        # 센서 데이터
        cot = alloc.get("cot_smartmoney", {})
        stress = alloc.get("cross_asset_stress", {})
        rotation = alloc.get("rotation", {})
        liquidity = alloc.get("liquidity_cycle", {})

        row = {
            "date": swing_data["date"],
            # BRAIN
            "brain_verdict": swing_data["brain_verdict"],
            "brain_pct": swing_data["brain_pct"],
            "brain_reason": swing_data.get("brain_reason", "")[:300],
            "regime": alloc.get("effective_regime", alloc.get("regime", "NORMAL")),
            "regime_severity": alloc.get("severity", 0),
            "regime_desc": alloc.get("description", ""),
            # 자산 배분 (합계 100% 보정)
            "alloc_swing": alloc_pct.get("bh_swing", 0),
            "alloc_gold_etf": alloc_pct.get("gold_etf", 0),
            "alloc_inverse": alloc_pct.get("inverse_etf", 0),
            "alloc_group_etf": alloc_pct.get("group_etf", 0),
            "alloc_small_cap": alloc_pct.get("small_cap", 0),
            "alloc_cash": alloc_pct.get("cash", 100),
            # 추천 종목
            "picks": swing_data.get("picks", []),
            "etf_picks": swing_data.get("etf_picks", []),
            "watchlist": swing_data.get("watchlist", []),
            # NXT
            "nxt_signal": nxt.get("signal", ""),
            "nxt_signal_text": nxt.get("signal_text", ""),
            "nxt_score": nxt.get("total_score", 0),
            "nxt_reason": nxt.get("selection_reason", ""),
            "nxt_targets": nxt_targets_clean,
            # 시장 지표
            "vix": raw.get("VIX", {}).get("value", 0) or 0,
            "nasdaq_pct": macro.get("nasdaq_pct", 0) or 0,
            "usdkrw": raw.get("USDKRW", {}).get("value", 0) or 0,
            "oil_pct": macro.get("oil_pct", 0) or 0,
            "gold_pct": raw.get("GOLD", {}).get("change_pct", 0) or 0,
            "silver_pct": macro.get("silver_pct", 0) or 0,
            # 분석 (analysis는 문자열만 + _macro_fusion은 별도 JSONB)
            # analysis에는 문자열만 (React #31 방지)
            "analysis": swing_data.get("analysis", {}),
            "portfolio": swing_data.get("portfolio", {}),
            # 센서
            "smart_money_score": cot.get("smart_money_score", 0) or 0,
            "smart_money_signal": cot.get("smart_money_signal", ""),
            "stress_index": stress.get("stress_index", 0) or 0,
            "stress_level": stress.get("stress_level", ""),
            "rotation_signal": rotation.get("rotation_signal", ""),
            "liquidity_score": liquidity.get("liquidity_score", 0) or 0,
            # 메타
            "market_comment": swing_data.get("market_comment", ""),
            # 채권자경단 v2 (NXT 추천 근거)
            "nxt_rationale": _build_nxt_rationale(nxt),
            # 피보나치 눌림목 종목 (퀀트 대시보드 대체)
            "fib_stocks": _build_fib_stocks(),
        }

        # alloc_* 합계 100% 보정
        alloc_sum = (row["alloc_swing"] + row["alloc_gold_etf"] + row["alloc_inverse"]
                     + row["alloc_group_etf"] + row["alloc_small_cap"] + row["alloc_cash"])
        if alloc_sum != 100 and alloc_sum > 0:
            diff = 100 - alloc_sum
            row["alloc_cash"] = max(0, row["alloc_cash"] + diff)
            logger.info(f"[DASHBOARD] alloc 합계 {alloc_sum}→100 보정 (cash {diff:+d})")
        elif alloc_sum == 0:
            row["alloc_cash"] = 100

        client.table("dashboard_swing").upsert(
            row, on_conflict="date"
        ).execute()

        logger.info(
            f"[DASHBOARD] 스윙 업로드 완료: {row['regime']} | "
            f"NXT {row['nxt_signal']} {row['nxt_score']:+.1f} | "
            f"{len(swing_data.get('picks', []))}종목"
        )
        return True

    except Exception as e:
        logger.error(f"[DASHBOARD] 스윙 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════
#  메인 진입점
# ═══════════════════════════════════════

def run_flowx_swing_upload() -> bool:
    """FLOWX 스윙 페이지 데이터 생성 + Supabase 업로드 (swing_signals + dashboard_swing)"""
    try:
        data = generate_swing_page_data()

        # 1) 기존 swing_signals 업로드 (유지)
        uploaded = upload_swing_to_supabase(data)

        # 2) dashboard_swing 통합 업로드 (NXT + Brain Allocation 병합)
        dashboard_ok = upload_dashboard_swing(data)

        return uploaded or dashboard_ok
    except Exception as e:
        logger.error(f"[FLOWX 스윙] 전체 실패: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False
