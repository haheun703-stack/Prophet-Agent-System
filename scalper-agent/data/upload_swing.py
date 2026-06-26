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


# ── Supabase 클라이언트 (shared 모듈) ──
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.supabase_client import get_client as _get_client


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
    brain_raw_pct = 0
    regime_cap_reason = ""
    brain_verdict = "관망"
    brain_reason = ""
    analysis = {}

    brain_path = STORE_DIR / "brain_report.json"
    if brain_path.exists():
        try:
            brain = json.loads(brain_path.read_text("utf-8"))
            brain_pct = brain.get("position_size_pct", 0)
            brain_raw_pct = brain.get("position_size_pct_raw", brain_pct)
            regime_cap_reason = brain.get("regime_cap_reason", "")
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
        _행동 = "포착" if star else ("관심" if score >= 40 else "관찰")

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
            "action": "포착" if star else "관심",
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
        "brain_raw_pct": brain_raw_pct,
        "regime_cap_reason": regime_cap_reason,
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
        pass
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
    """시간대별 매매 안내 (v5.0: NXT 08:00 + 수급 4회 + bomb 15:00)"""
    guide = []

    if nxt_score <= -5:
        # 강한 하락 신호
        guide = [
            {"시간": "08:00~09:00", "행동": "NXT [쌍매수] 종목 소량 진입 (하락장이므로 관망 우선)", "태그": "관망"},
            {"시간": "09:00~09:15", "행동": "시장 방향 확인 → 인버스/금 ETF 검토", "태그": "관망"},
            {"시간": "10:00", "행동": "1차 수급 리포트 확인 — 외인/기관 방향 체크", "태그": "수급확인"},
            {"시간": "11:20", "행동": "2차 수급 리포트 — 매도세 지속 여부 판단", "태그": "수급확인"},
            {"시간": "13:20", "행동": "3차 수급 리포트 — 오후 반등 여부 확인", "태그": "수급확인"},
            {"시간": "14:30", "행동": "4차 수급 리포트 — 최종 흐름 정리", "태그": "수급확인"},
            {"시간": "15:00", "행동": "bomb 매수 알림 — 내일용 수급폭탄 후보 확인", "태그": "내일준비"},
            {"시간": "종일", "행동": "포지션 관리 — 손절 -2% 엄수", "태그": "자동알림"},
        ]
    elif nxt_score <= 0:
        # 약한 하락 / 중립
        guide = [
            {"시간": "08:00~09:00", "행동": "NXT [쌍매수] 종목 1/3 분할 진입", "태그": "NXT진입"},
            {"시간": "09:00~09:15", "행동": "[KRX전용] 종목 갭 확인 후 진입", "태그": "관망/진입"},
            {"시간": "10:00", "행동": "1차 수급 리포트 — 외인/기관 매수 TOP 체크", "태그": "수급확인"},
            {"시간": "11:20", "행동": "2차 수급 리포트 — 수급 변동 확인", "태그": "수급확인"},
            {"시간": "13:20", "행동": "3차 수급 리포트 — 오후장 전환 판단", "태그": "수급확인"},
            {"시간": "14:30", "행동": "4차 수급 리포트 — 마감 전 정리", "태그": "수급확인"},
            {"시간": "15:00", "행동": "bomb 매수 알림 — 내일용 수급폭탄 TOP5", "태그": "내일준비"},
            {"시간": "종일", "행동": "포지션 관리 — 익절 +3% / 손절 -2%", "태그": "자동알림"},
        ]
    else:
        # 상승 신호
        guide = [
            {"시간": "08:00~09:00", "행동": "NXT 수급 강도 TOP → 1순위부터 진입", "태그": "NXT진입"},
            {"시간": "09:00~09:15", "행동": "[KRX전용] 종목 갭 확인 → 순서대로 진입", "태그": "진입"},
            {"시간": "10:00", "행동": "1차 수급 리포트 — 쌍매수 진행 중 종목 추가 진입", "태그": "수급확인"},
            {"시간": "11:20", "행동": "2차 수급 리포트 — 모멘텀 확인, 추가 매수 판단", "태그": "수급확인"},
            {"시간": "13:20", "행동": "3차 수급 리포트 — 목표가 도달 시 일부 익절", "태그": "수급확인"},
            {"시간": "14:30", "행동": "4차 수급 리포트 — 보유/청산 최종 판단", "태그": "수급확인"},
            {"시간": "15:00", "행동": "bomb 매수 알림 — 내일 수급폭탄 TOP5 확인", "태그": "내일준비"},
            {"시간": "종일", "행동": "포지션 관리 — 익절 +3% / 손절 -2%", "태그": "자동알림"},
        ]

    # 매크로 상태별 추가 안내
    if macro_regime == "스태그플레이션":
        guide.append({"시간": "주의사항", "행동": "물가상승+경기둔화 → 3일 안에 청산, 손절 -2%로 타이트하게", "태그": "경고"})
    elif macro_regime == "비용상승":
        guide.append({"시간": "주의사항", "행동": "원자재 비용 상승 → 에너지/원자재 주 선호, 전기가스/운송 주의", "태그": "경고"})

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


def _build_fib_leaders() -> list:
    """시총 상위 30 대형주 피보나치 레벨 (하락률 무관, 무조건 표시).
    fib_leaders.json에서 로드. fib_zone 미존재 시 drop 기반 자동 계산.
    """
    data = _load_json("fib_leaders.json")
    if not data:
        return []
    items = data[:30] if isinstance(data, list) else []
    # fib_zone 보정: 비어있으면 drop 기반 자동 부여
    for s in items:
        if not s.get("fib_zone"):
            drop = abs(s.get("drop", 0))
            if drop >= 50:
                s["fib_zone"] = "DEEP"
            elif drop >= 40:
                s["fib_zone"] = "MID"
            elif drop >= 30:
                s["fib_zone"] = "MILD"
            elif drop >= 15:
                s["fib_zone"] = "SHALLOW"
            else:
                s["fib_zone"] = "NEAR_HIGH"
    return items


def _build_fx_monitor() -> dict:
    """달러-환율 모니터 — DXY/USD-KRW/VIX + 외국인 자금흐름 신호.
    yfinance로 수집, 환율↔KOSPI 상관관계 계산.
    """
    import pandas as pd
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[FX] yfinance 미설치")
        return {}

    result = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")}

    try:
        # ── 1) DXY 달러인덱스 ──
        dxy_tk = yf.Ticker("DX-Y.NYB")
        dxy_df = dxy_tk.history(period="1mo")
        if len(dxy_df) >= 2:
            dxy_val = round(float(dxy_df.iloc[-1]["Close"]), 2)
            dxy_prev = round(float(dxy_df.iloc[-2]["Close"]), 2)
            dxy_ma5 = round(float(dxy_df["Close"].tail(5).mean()), 2)
            dxy_ma20 = round(float(dxy_df["Close"].tail(20).mean()), 2) if len(dxy_df) >= 20 else dxy_ma5
            dxy_chg = round((dxy_val / dxy_prev - 1) * 100, 2) if dxy_prev > 0 else 0

            if dxy_ma5 < dxy_ma20:
                dxy_trend = "약세"
            elif dxy_ma5 > dxy_ma20 * 1.005:
                dxy_trend = "강세"
            else:
                dxy_trend = "횡보"

            result["dxy"] = {
                "value": dxy_val, "prev": dxy_prev, "chg_1d": dxy_chg,
                "ma5": dxy_ma5, "ma20": dxy_ma20, "trend": dxy_trend,
            }

        # ── 2) USD/KRW 환율 ──
        fx_tk = yf.Ticker("KRW=X")
        fx_df = fx_tk.history(period="1mo")
        if len(fx_df) >= 2:
            fx_val = round(float(fx_df.iloc[-1]["Close"]), 1)
            fx_prev = round(float(fx_df.iloc[-2]["Close"]), 1)
            fx_ma5 = round(float(fx_df["Close"].tail(5).mean()), 1)
            fx_ma20 = round(float(fx_df["Close"].tail(20).mean()), 1) if len(fx_df) >= 20 else fx_ma5
            fx_chg = round((fx_val / fx_prev - 1) * 100, 2) if fx_prev > 0 else 0

            if fx_val < fx_ma5:
                fx_trend = "원강세"
            elif fx_val > fx_ma5 * 1.003:
                fx_trend = "원약세"
            else:
                fx_trend = "횡보"

            result["usdkrw"] = {
                "value": fx_val, "prev": fx_prev, "chg_1d": fx_chg,
                "ma5": fx_ma5, "ma20": fx_ma20, "trend": fx_trend,
            }

        # ── 3) VIX 구조 ──
        vix_val, vix3m_val = 0.0, 0.0
        for ticker, key in [("^VIX", "vix"), ("^VIX3M", "vix3m")]:
            try:
                tk = yf.Ticker(ticker)
                df = tk.history(period="5d")
                if len(df) >= 1:
                    val = round(float(df.iloc[-1]["Close"]), 2)
                    if key == "vix":
                        vix_val = val
                    else:
                        vix3m_val = val
            except Exception:
                pass

        if vix_val > 0 and vix3m_val > 0:
            structure = "CONTANGO" if vix_val < vix3m_val else "BACKWARDATION"
            ratio = round(vix_val / vix3m_val, 3)
            result["vix_structure"] = {
                "vix": vix_val, "vix3m": vix3m_val,
                "ratio": ratio, "structure": structure,
                "label": "정상(안전)" if structure == "CONTANGO" else "역전(패닉)",
            }

        # ── 4) 환율↔KOSPI 상관관계 (최근 15일) ──
        try:
            kospi_tk = yf.Ticker("^KS11")
            kospi_df = kospi_tk.history(period="1mo")

            if len(fx_df) >= 5 and len(kospi_df) >= 5:
                # 날짜 기준 매칭
                fx_dict = {}
                for idx, row in fx_df.iterrows():
                    fx_dict[idx.strftime("%Y-%m-%d")] = float(row["Close"])

                matches, total = 0, 0
                prev_fx_v, prev_k_v = None, None

                for idx, row in kospi_df.tail(15).iterrows():
                    dt = idx.strftime("%Y-%m-%d")
                    fx_v = fx_dict.get(dt)
                    k_v = float(row["Close"])

                    if fx_v and prev_fx_v and prev_k_v:
                        fx_up = fx_v > prev_fx_v   # 원약세
                        k_up = k_v > prev_k_v       # KOSPI 상승
                        # 역상관 = 환율↑ & KOSPI↓ 또는 환율↓ & KOSPI↑
                        if fx_up != k_up:
                            matches += 1
                        total += 1

                    if fx_v:
                        prev_fx_v = fx_v
                    prev_k_v = k_v

                corr_pct = round(matches / total * 100) if total > 0 else 0
                result["correlation"] = {
                    "matches": matches, "total": total,
                    "pct": corr_pct,
                    "label": f"최근 {total}일 중 {matches}일 역상관 ({corr_pct}%)",
                }
        except Exception as e:
            logger.warning(f"[FX] 상관관계 계산 실패: {e}")

        # ── 5) 외국인 자금 흐름 (삼성전자 CSV 대용) ──
        try:
            csv_path = STORE_DIR / "flow" / "005930_investor.csv"
            if csv_path.exists():
                inv_df = pd.read_csv(csv_path)
                if len(inv_df) >= 3:
                    last3 = inv_df.tail(3)
                    frgn_today = int(last3.iloc[-1].get("외국인_금액", 0) or 0)
                    frgn_3d = int(last3["외국인_금액"].sum())

                    # 연속 매수/매도 일수
                    streak = 0
                    direction = ""
                    for i in range(len(inv_df) - 1, max(len(inv_df) - 20, -1), -1):
                        val = int(inv_df.iloc[i].get("외국인_금액", 0) or 0)
                        if streak == 0:
                            direction = "매수" if val > 0 else "매도"
                            streak = 1
                        elif (direction == "매수" and val > 0) or (direction == "매도" and val <= 0):
                            streak += 1
                        else:
                            break

                    # 신호 판정
                    if frgn_today > 0 and streak <= 2:
                        signal = "순매수전환"
                        signal_color = "GREEN"
                    elif direction == "매수" and streak >= 3:
                        signal = f"{streak}일연속매수"
                        signal_color = "GREEN"
                    elif direction == "매도" and streak >= 5:
                        signal = f"{streak}일연속매도"
                        signal_color = "RED"
                    elif direction == "매도":
                        signal = f"{streak}일매도중"
                        signal_color = "YELLOW"
                    else:
                        signal = "중립"
                        signal_color = "YELLOW"

                    result["foreign_flow"] = {
                        "proxy": "삼성전자",
                        "today": frgn_today,          # 백만원
                        "today_억": round(frgn_today / 100),
                        "sum_3d": frgn_3d,
                        "sum_3d_억": round(frgn_3d / 100),
                        "streak": streak,
                        "direction": direction,
                        "signal": signal,
                        "signal_color": signal_color,
                    }
        except Exception as e:
            logger.warning(f"[FX] 외국인 흐름 로드 실패: {e}")

        # ── 6) 종합 판정 ──
        dxy_info = result.get("dxy", {})
        fx_info = result.get("usdkrw", {})
        vix_info = result.get("vix_structure", {})
        flow_info = result.get("foreign_flow", {})

        bullish = 0  # 외국인 유입 방향 점수
        bearish = 0

        # DXY 약세 = 원강세 = 외국인 유입
        if dxy_info.get("trend") == "약세":
            bullish += 2
        elif dxy_info.get("trend") == "강세":
            bearish += 2

        # 환율 원강세 = 외국인 유입
        if fx_info.get("trend") == "원강세":
            bullish += 2
        elif fx_info.get("trend") == "원약세":
            bearish += 2

        # VIX CONTANGO = 안전 = 이머징 선호
        if vix_info.get("structure") == "CONTANGO":
            bullish += 1
        elif vix_info.get("structure") == "BACKWARDATION":
            bearish += 2

        # 외국인 흐름
        if flow_info.get("signal_color") == "GREEN":
            bullish += 2
        elif flow_info.get("signal_color") == "RED":
            bearish += 2

        total_score = bullish - bearish
        if total_score >= 4:
            verdict = "외국인 유입 강력"
            verdict_color = "GREEN"
        elif total_score >= 2:
            verdict = "외국인 유입 가능"
            verdict_color = "GREEN"
        elif total_score >= 0:
            verdict = "중립 (관망)"
            verdict_color = "YELLOW"
        elif total_score >= -2:
            verdict = "외국인 유출 우려"
            verdict_color = "YELLOW"
        else:
            verdict = "외국인 유출 경고"
            verdict_color = "RED"

        result["verdict"] = {
            "text": verdict,
            "color": verdict_color,
            "bullish": bullish,
            "bearish": bearish,
            "score": total_score,
        }

        logger.info(f"[FX] 달러-환율 모니터: {verdict} (점수 {total_score}, "
                     f"DXY {dxy_info.get('trend','?')} / FX {fx_info.get('trend','?')} / "
                     f"VIX {vix_info.get('structure','?')})")

    except Exception as e:
        logger.error(f"[FX] 달러-환율 모니터 빌드 실패: {e}")

    return result


def _build_sector_rotation() -> dict:
    """섹터 로테이션 맵 — 피보나치 + 수급 + 모멘텀 기반 자금 흐름 예측.

    bottom_scan.json + fib_leaders.json + 투자자 CSV + pykrx 등락률 →
    섹터별 종합 점수 계산 → 로테이션 단계(선도/추격/대기/후발) 판정.
    """
    import pandas as pd
    from collections import defaultdict

    bs = _load_json("bottom_scan.json")
    fl_raw = _load_json("fib_leaders.json")
    fl = fl_raw if isinstance(fl_raw, list) else []
    if isinstance(bs, dict):
        bs = bs.get("stocks", [])

    if not bs and not fl:
        return {}

    # 전체 종목 합치기 (중복 제거)
    all_stocks = {}
    for s in (bs if isinstance(bs, list) else []) + fl:
        code = s.get("code", "")
        if code and code not in all_stocks:
            all_stocks[code] = s

    if not all_stocks:
        return {}

    # 등락률 + 수급: investor CSV에서 한 번에 추출
    chg_dict = {}
    flow_dict = {}  # code -> {inst_3d, frgn_3d}
    flow_dir = STORE_DIR / "flow"
    for code in all_stocks:
        csv_path = flow_dir / f"{code}_investor.csv"
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                df.columns = [c.strip() for c in df.columns]
                if len(df) >= 2:
                    # 등락률: 종가 기반 전일 대비
                    last_row = df.iloc[-1]
                    prev_row = df.iloc[-2]
                    c_last = float(last_row.get("종가", 0) or 0)
                    c_prev = float(prev_row.get("종가", 0) or 0)
                    if c_prev > 0 and c_last > 0:
                        chg_dict[code] = round((c_last / c_prev - 1) * 100, 2)
                if len(df) >= 3:
                    last3 = df.tail(3)
                    flow_dict[code] = {
                        "inst_3d": int(last3["기관_금액"].sum()),
                        "frgn_3d": int(last3["외국인_금액"].sum()),
                    }
            except Exception:
                pass

    # 섹터별 집계
    sector_agg = defaultdict(lambda: {
        "count": 0, "total_cap": 0,
        "deep": 0, "mid": 0, "mild": 0, "shallow": 0,
        "drop_sum": 0, "upside_sum": 0,
        "chg_list": [], "inst_3d_sum": 0, "frgn_3d_sum": 0,
        "dual_buy_3d": 0, "stocks": [],
    })

    for code, s in all_stocks.items():
        sector = s.get("sector", "기타") or "기타"
        sd = sector_agg[sector]
        sd["count"] += 1
        sd["total_cap"] += s.get("cap", 0)
        sd["drop_sum"] += s.get("drop", 0)
        sd["upside_sum"] += s.get("upside", 0)

        zone = s.get("fib_zone", "")
        if zone == "DEEP": sd["deep"] += 1
        elif zone == "MID": sd["mid"] += 1
        elif zone == "MILD": sd["mild"] += 1
        elif zone == "SHALLOW": sd["shallow"] += 1

        if code in chg_dict:
            sd["chg_list"].append(chg_dict[code])

        fl_data = flow_dict.get(code)
        if fl_data:
            sd["inst_3d_sum"] += fl_data["inst_3d"]
            sd["frgn_3d_sum"] += fl_data["frgn_3d"]
            if fl_data["inst_3d"] > 0 and fl_data["frgn_3d"] > 0:
                sd["dual_buy_3d"] += 1

        sd["stocks"].append(s.get("name", code))

    # 섹터별 점수 계산
    sectors = []
    for sector, sd in sector_agg.items():
        if sd["count"] < 3:
            continue

        avg_drop = round(sd["drop_sum"] / sd["count"], 1)
        avg_upside = round(sd["upside_sum"] / sd["count"], 1)
        avg_chg = round(sum(sd["chg_list"]) / len(sd["chg_list"]), 2) if sd["chg_list"] else 0
        net_flow = sd["inst_3d_sum"] + sd["frgn_3d_sum"]
        net_flow_억 = round(net_flow / 100)

        # 종합 점수
        momentum = round(avg_chg * 10, 1)  # 직전 거래일 모멘텀
        flow_score = round(min(30, max(-30, net_flow / 10000)), 1)  # 수급 (±30 상한)
        dual_bonus = sd["dual_buy_3d"] * 10  # 쌍매수 보너스
        total = round(momentum + flow_score + dual_bonus, 1)

        # 경고 플래그: 모멘텀만 있고 수급 없으면
        warning = ""
        if momentum > 20 and abs(net_flow) < 5000:
            warning = "개인 주도 상승 (수급 미확인)"

        up_count = sum(1 for c in sd["chg_list"] if c > 0)
        down_count = sum(1 for c in sd["chg_list"] if c <= 0)

        cap_조 = round(sd["total_cap"] / 10000, 1) if sd["total_cap"] >= 10000 else 0
        cap_억 = sd["total_cap"] if sd["total_cap"] < 10000 else 0

        sectors.append({
            "sector": sector,
            "count": sd["count"],
            "total_score": total,
            "momentum": momentum,
            "flow_score": flow_score,
            "dual_bonus": dual_bonus,
            "avg_chg": avg_chg,
            "avg_drop": avg_drop,
            "avg_upside": avg_upside,
            "net_flow_억": net_flow_억,
            "dual_buy_3d": sd["dual_buy_3d"],
            "up_count": up_count,
            "down_count": down_count,
            "deep": sd["deep"],
            "mid": sd["mid"],
            "mild": sd["mild"],
            "shallow": sd["shallow"],
            "cap_조": cap_조,
            "cap_억": cap_억,
            "warning": warning,
        })

    # 종합 점수 순 정렬
    sectors.sort(key=lambda x: x["total_score"], reverse=True)

    # 로테이션 단계 판정
    for i, s in enumerate(sectors):
        score = s["total_score"]
        if score >= 50:
            s["stage"] = "선도"
            s["stage_num"] = 1
            s["stage_color"] = "GREEN"
        elif score >= 20:
            s["stage"] = "추격"
            s["stage_num"] = 2
            s["stage_color"] = "GREEN"
        elif score >= 0:
            s["stage"] = "대기"
            s["stage_num"] = 3
            s["stage_color"] = "YELLOW"
        else:
            s["stage"] = "후발"
            s["stage_num"] = 4
            s["stage_color"] = "RED"

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_sectors": len(sectors),
        "total_stocks": len(all_stocks),
        "sectors": sectors,
    }

    logger.info(f"[ROTATION] 섹터 로테이션 맵: {len(sectors)}섹터 "
                f"(선도 {sum(1 for s in sectors if s['stage']=='선도')}, "
                f"추격 {sum(1 for s in sectors if s['stage']=='추격')}, "
                f"대기 {sum(1 for s in sectors if s['stage']=='대기')}, "
                f"후발 {sum(1 for s in sectors if s['stage']=='후발')})")

    return result


def _build_stealth_stocks() -> dict:
    """기관 선매집 탐지 결과 → 대시보드 JSONB.

    stealth_scan.json 로드. 없으면 실시간 스캔 실행.
    잠복(stealth) + 움직임(moving) 상위만 전달.
    """
    scan_path = STORE_DIR / "stealth_scan.json"

    # 캐시 로드 (당일 데이터면 재사용)
    data = _load_json("stealth_scan.json")
    if data:
        ts = data.get("timestamp", "")
        from datetime import date as _d
        today_str = _d.today().strftime("%Y-%m-%d")
        if ts.startswith(today_str):
            return _format_stealth_for_dashboard(data)

    # 캐시 없거나 오래됨 → 실시간 스캔
    try:
        from data.stealth_scanner import scan_stealth_accumulation
        data = scan_stealth_accumulation()
        return _format_stealth_for_dashboard(data)
    except Exception as e:
        logger.warning(f"[선매집] 스캔 실패: {e}")
        # 오래된 캐시라도 반환
        if data:
            return _format_stealth_for_dashboard(data)
        return {}


def _format_stealth_for_dashboard(data: dict) -> dict:
    """스캔 결과 → 대시보드용 JSONB 경량화"""
    stealth = data.get("stealth", [])[:30]
    moving = data.get("moving", [])[:15]

    # 프론트에 필요한 필드만 전달
    def _slim(item):
        return {
            "code": item.get("code", ""),
            "name": item.get("name", ""),
            "sector": item.get("sector", ""),
            "score": item.get("score", 0),
            "pattern": item.get("pattern", ""),
            "dual_buy": item.get("dual_buy", False),
            "inst_avg": item.get("inst_avg", 0),
            "frgn_avg": item.get("frgn_avg", 0),
            "chg_5d": item.get("chg_5d", 0),
            "close": item.get("close", 0),
            "cap": item.get("cap", 0),
            "category": item.get("category", ""),
        }

    return {
        "timestamp": data.get("timestamp", ""),
        "stealth": [_slim(s) for s in stealth],
        "moving": [_slim(m) for m in moving],
        "summary": data.get("summary", {}),
    }


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


def _build_paper_performance() -> dict:
    """★ 6/25 사장님: 단타봇 페이퍼 매매 누적수익률 → 한국스윙 '시장판단&전략' 옆 패널.

    두 기준 나란히 (사장님 6/25 결정 — 미실현 손실 숨기지 않음):
      - 전체자산(equity_return_pct): 현금+보유평가, 미실현 보유손실 포함 (보수적·현실)
      - 청산손익합(realized_sum_pct): 청산(매도)한 거래 손익률 누적 합 (판 것만)
    퀀트봇 upload_quant_dashboard 방식 — 매일 1건 upsert(on_conflict=date).
    record-only · 매매 무접촉 (PaperPortfolio._load만, _save 호출 안 함).
    """
    try:
        from engine.paper_portfolio import PaperPortfolio
        pf = PaperPortfolio()  # _load만 (파일 읽기) — _save 호출 X → 장부 무손상
    except Exception as e:
        logger.warning(f"[PAPER] 포트폴리오 로드 실패(무시): {e}")
        return {}

    try:
        closed = pf.closed_trades or []
        daily_log = pf._load_daily_log()
        today = date.today().strftime("%Y-%m-%d")

        # 기준1: 전체자산 누적 (paper_daily_log 최신 — 미실현 포함)
        equity_return = daily_log[-1].get("total_return_pct", 0.0) if daily_log else 0.0
        try:
            mdd = round(pf._calc_mdd(daily_log), 2)
        except Exception:
            mdd = 0.0

        # 기준2: 청산손익합 (realized only — 청산한 거래 pnl_pct 누적)
        realized_sum = round(sum(t.get("pnl_pct", 0) or 0 for t in closed), 2)
        total = len(closed)
        wins = sum(1 for t in closed if (t.get("pnl_pct", 0) or 0) > 0)
        win_rate = round(wins / total * 100, 1) if total else 0.0
        # 거래당 평균 손익률 — realized_sum(단순합)이 포지션크기 미반영이라 오해되지 않게 병기
        avg_realized = round(realized_sum / total, 2) if total else 0.0

        # 오늘 청산
        today_closed = [t for t in closed if t.get("exit_date") == today]
        today_realized_pnl = sum(int(t.get("pnl_krw", 0) or 0) for t in today_closed)

        # ★ 6/26 사장님: 일일 매매일지 — 오늘 체결(매도=청산 / 매수=오늘 신규보유). 웹봇 한국스윙 렌더용.
        today_trades = []
        for t in today_closed:
            today_trades.append({
                "side": "SELL",
                "code": t.get("code", ""), "name": t.get("name", ""),
                "entry": t.get("entry_price"), "exit": t.get("exit_price"),
                "shares": t.get("shares"),
                "pnl_pct": t.get("pnl_pct"), "pnl_krw": int(t.get("pnl_krw", 0) or 0),
                "reason": t.get("reason", ""), "hold_days": t.get("hold_days"),
                "source": t.get("source", ""),
            })
        for _code, _pos in pf.positions.items():
            if _pos.get("entry_date") == today:
                today_trades.append({
                    "side": "BUY",
                    "code": _code, "name": _pos.get("name", ""),
                    "entry": _pos.get("entry_price"), "exit": None,
                    "shares": _pos.get("shares", _pos.get("qty")),
                    "pnl_pct": None, "pnl_krw": None,
                    "reason": "", "hold_days": None,
                    "source": _pos.get("source", ""),
                })

        # 자산곡선 (최근 30 스냅샷: date + 전체자산 누적%)
        curve = [
            {"date": s.get("date", ""), "equity_pct": round(s.get("total_return_pct", 0) or 0, 2)}
            for s in daily_log[-30:]
        ]

        return {
            "date": today,
            "start_date": pf.start_date,
            "initial_cash": pf.initial_cash,
            # 기준1: 전체자산 (미실현 포함 — 현실)
            "equity_return_pct": round(equity_return, 2),
            "mdd_pct": mdd,
            # 기준2: 청산손익합 (realized — 판 것만)
            "realized_sum_pct": realized_sum,
            "avg_realized_pct": avg_realized,
            "closed_total": total,
            "closed_wins": wins,
            "win_rate": win_rate,
            # 오늘
            "today_closed": len(today_closed),
            "today_realized_pnl": today_realized_pnl,
            "open_positions": len(pf.positions),
            # ★ 6/26: 일일 매매일지 (오늘 체결 매수/매도 내역)
            "today_trades": today_trades,
            # 자산곡선 (전체자산 기준)
            "equity_curve": curve,
            # 정직성 라벨 (사장님 룰: 미실현 숨기지 않음 / 관측 전용)
            "note": "전체자산=미실현 보유손실 포함(현실) · 청산합=판 것만(realized). 봇 OFF·paper 관측 전용·실매수 신호 아님.",
        }
    except Exception as e:
        logger.warning(f"[PAPER] 성과 요약 실패(무시): {e}")
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

        # ★ 6/22 사장님: 테마 relay-aware 매매포인트(theme_relay_shadow 관측) → 한국스윙 '매매포인트' 탭 ★
        try:
            from data.theme_trading_points import build_trading_points
            _trading_points = build_trading_points()
        except Exception as _tp_e:
            logger.warning(f"[DASHBOARD] trading_points 생성 실패(무시): {_tp_e}")
            _trading_points = {}

        row = {
            "date": swing_data["date"],
            # BRAIN
            "brain_verdict": swing_data["brain_verdict"],
            "brain_pct": swing_data["brain_pct"],
            "brain_raw_pct": swing_data.get("brain_raw_pct", swing_data["brain_pct"]),
            "brain_capped_pct": swing_data["brain_pct"],
            "regime_cap_reason": swing_data.get("regime_cap_reason", ""),
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
            # 대형주 피보나치 (시총 상위 30)
            "fib_leaders": _build_fib_leaders(),
            # 달러-환율 모니터 (DXY/환율/VIX/외국인 흐름)
            "fx_monitor": _build_fx_monitor(),
            # 섹터 로테이션 맵 (피보나치+수급+모멘텀)
            "sector_rotation": _build_sector_rotation(),
            # 기관 선매집 탐지 (잠복+움직임 종목)
            "stealth_stocks": _build_stealth_stocks(),
            # ★ 6/22 테마 relay-aware 매매포인트 (강한 테마 주도그룹·초입·바통, 관측·실매수 아님) ★
            "trading_points": _trading_points,
            # ★ 6/25 사장님: 단타봇 페이퍼 누적수익률(전체자산+청산합) → '시장판단&전략' 옆 (record-only) ★
            "paper_performance": _build_paper_performance(),
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

        try:
            client.table("dashboard_swing").upsert(
                row, on_conflict="date"
            ).execute()
        except Exception as upsert_err:
            # 컬럼 미존재 시 해당 필드 제거 후 재시도
            err_str = str(upsert_err)
            if "does not exist" in err_str:
                for col in ["paper_performance", "trading_points", "stealth_stocks", "brain_raw_pct", "brain_capped_pct", "regime_cap_reason"]:
                    if col in err_str and col in row:
                        logger.warning(f"[DASHBOARD] {col} 컬럼 미존재 → 제거 후 재시도")
                        del row[col]
                client.table("dashboard_swing").upsert(
                    row, on_conflict="date"
                ).execute()
            else:
                raise

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
#  피보나치 종가 자동 갱신
# ═══════════════════════════════════════

def refresh_fib_prices():
    """bottom_scan.json + fib_leaders.json 종가 갱신 (매일 C19 전 자동 실행).

    pykrx 개별호출 → CSV 종가 읽기로 전환 (API 호출 0건, 타임아웃 방지).
    data_store/flow/{code}_investor.csv 마지막 행의 종가 사용.
    """
    import csv

    flow_dir = STORE_DIR / "flow"

    # 1) CSV에서 종가 일괄 로드 (코드 → 종가 dict)
    price_map: dict[str, int] = {}
    if flow_dir.exists():
        for csv_path in flow_dir.glob("*_investor.csv"):
            code = csv_path.stem.replace("_investor", "")
            try:
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    last_row = None
                    for row in reader:
                        last_row = row
                    if last_row and "종가" in last_row:
                        price_val = last_row["종가"]
                        if price_val:
                            price_map[code] = int(float(price_val))
            except Exception:
                continue

    if not price_map:
        logger.warning("[FIB] CSV 종가 로드 실패 — flow 데이터 없음")
        return

    logger.info(f"[FIB] CSV 종가 {len(price_map)}종목 로드 완료")

    for filename in ["bottom_scan.json", "fib_leaders.json"]:
        path = STORE_DIR / filename
        if not path.exists():
            continue

        try:
            data = json.loads(path.read_text("utf-8"))
            if not isinstance(data, list) or not data:
                continue

            updated = 0
            for s in data:
                code = s.get("code", "")
                if not code or code not in price_map:
                    continue

                new_price = price_map[code]
                w52h = s.get("w52h", 0)

                s["price"] = new_price
                s["drop"] = round((new_price / w52h - 1) * 100, 2) if w52h > 0 else 0

                # fib_zone 재판정 (bottom_scan만)
                if "fib_zone" in s:
                    drop_abs = abs(s["drop"])
                    if drop_abs >= 50:
                        s["fib_zone"] = "DEEP"
                    elif drop_abs >= 40:
                        s["fib_zone"] = "MID"
                    elif drop_abs >= 30:
                        s["fib_zone"] = "MILD"
                    else:
                        s["fib_zone"] = "SHALLOW"

                    _zone_label = {
                        "DEEP": "50%+ 하락 (바닥 매수 구간)",
                        "MID": "40~50% 하락 (중간 눌림)",
                        "MILD": "30~40% 하락 (1차 눌림)",
                        "SHALLOW": "15~30% 하락 (얕은 조정)",
                    }
                    s["fib_zone_label"] = _zone_label.get(s["fib_zone"], s["fib_zone"])

                # fib_status 재판정
                fib_382 = s.get("fib_382", 0)
                fib_500 = s.get("fib_500", 0)
                fib_618 = s.get("fib_618", 0)
                if fib_382 and new_price <= fib_382:
                    s["fib_status"] = "38.2% 아래 (깊은 하락)"
                elif fib_500 and new_price <= fib_500:
                    s["fib_status"] = "38.2%~50% 사이"
                elif fib_618 and new_price <= fib_618:
                    s["fib_status"] = "50%~61.8% 사이"
                elif w52h and new_price <= w52h * 0.9:
                    s["fib_status"] = "61.8% 위 (회복 중)"
                else:
                    s["fib_status"] = "고점 근접"

                # upside 재계산
                target = s.get("target_peace", s.get("target", 0))
                if target and new_price > 0:
                    s["upside"] = round((target / new_price - 1) * 100, 2)

                updated += 1

            # 저장
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
            tmp.replace(path)
            logger.info(f"[FIB] {filename} 종가 갱신: {updated}/{len(data)}종목 (CSV)")

        except Exception as e:
            logger.warning(f"[FIB] {filename} 갱신 실패: {e}")


# ═══════════════════════════════════════
#  메인 진입점
# ═══════════════════════════════════════

def run_flowx_swing_upload() -> bool:
    """FLOWX 스윙 페이지 데이터 생성 + Supabase 업로드 (swing_signals + dashboard_swing)"""
    try:
        # 0) 피보나치 종가 갱신 (bottom_scan + fib_leaders)
        try:
            refresh_fib_prices()
        except Exception as e:
            logger.warning(f"[FIB] 종가 갱신 실패 (무시): {e}")

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
