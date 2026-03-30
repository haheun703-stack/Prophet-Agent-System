"""
Market Brain — 시장 합성 예측 엔진
6-Phase 순차 알고리즘으로 모든 데이터를 합성하여
시장 예측 + 섹터 전략 + 종목 근거 서술 + 투자 비중 권장 제공

Phase 1: 매크로 방향 판정
Phase 2: 원자재 사이클 판정
Phase 3: 섹터 로테이션 판정
Phase 4: 수급 흐름 판정
Phase 5: 리스크 판정
Phase 6: 종합 판정 + 종목 서술 + 포지션 사이징
"""

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger("market_brain")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
BRAIN_REPORT_PATH = DATA_DIR / "brain_report.json"


# ═══════════════════════════════════════
#  데이터 로더 (안전한 JSON 로드)
# ═══════════════════════════════════════

def _load_json(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text("utf-8"))
    except Exception as e:
        logger.warning(f"로드 실패 {path.name}: {e}")
    return {}


def _load_nightwatch() -> dict:
    return _load_json(DATA_DIR / "nightwatch_report.json")


def _load_sector_history() -> dict:
    return _load_json(DATA_DIR / "sector_history.json")


def _load_tv_scanner() -> dict:
    return _load_json(DATA_DIR / "tv_scanner.json")


def _load_recommendation() -> dict:
    return _load_json(DATA_DIR / "recommendation.json")


def _load_guardian() -> dict:
    return _load_json(DATA_DIR / "learning" / "guardian_latest.json")


def _load_global_events() -> dict:
    return _load_json(DATA_DIR / "global_events.json")


def _load_macro_themes() -> dict:
    return _load_json(DATA_DIR / "macro_themes.json")


def _load_insights() -> dict:
    return _load_json(DATA_DIR / "learning" / "insights.json")


def _load_brain_performance() -> list:
    """FIX-08: brain_performance.json (30일 롤링 리스트)"""
    path = DATA_DIR / "learning" / "brain_performance.json"
    try:
        if path.exists():
            data = json.loads(path.read_text("utf-8"))
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


# ═══════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════

@dataclass
class MacroAssessment:
    direction: str = "NEUTRAL"       # STRONG_BULL / BULL / NEUTRAL / BEAR / STRONG_BEAR
    nxt_total: float = 0.0
    vix: float = 0.0
    usdkrw: float = 0.0
    usdkrw_chg: float = 0.0
    nasdaq_chg: float = 0.0
    tnx_value: float = 0.0
    tnx_chg: float = 0.0
    gold_chg: float = 0.0
    hyg_chg: float = 0.0
    es_chg: float = 0.0
    korea_strength: float = 0.0  # NXT-06: 한국장 강도
    options_pc_level: str = ""   # OPT-03: 옵션 심리 (FEAR/NEUTRAL/GREED)
    options_adj: float = 0.0     # OPT-03: 옵션 심리 보정값
    contradictions: list = field(default_factory=list)
    narrative: str = ""


@dataclass
class CommodityAssessment:
    relay_stage: str = "NONE"
    gold_chg: float = 0.0
    oil_chg: float = 0.0
    ng_chg: float = 0.0
    silver_chg: float = 0.0
    copper_chg: float = 0.0
    active_signals: list = field(default_factory=list)
    beneficiary_sectors: list = field(default_factory=list)
    narrative: str = ""


@dataclass
class SectorAssessment:
    hot_sectors: list = field(default_factory=list)
    next_sectors: list = field(default_factory=list)
    cooling_sectors: list = field(default_factory=list)
    narrative: str = ""


@dataclass
class FlowAssessment:
    dominant_buyer: str = ""
    tv_sector_summary: list = field(default_factory=list)
    nationality_signals: list = field(default_factory=list)
    strong_buy_count: int = 0
    sell_count: int = 0
    flow_zscore_level: str = ""      # STRONG_BUY/BUY/NEUTRAL/CAUTION/SELL/DIVERGENCE
    flow_zscore_detail: str = ""     # "기관Z+2.3 외인Z+1.8 2-sigma 동반 폭발매수"
    flow_zscore_adj: float = 0.0     # ±5.0
    narrative: str = ""


@dataclass
class RiskAssessment:
    risk_level: str = "LOW"
    risk_score: float = 0.0
    vix_risk: str = ""
    currency_risk: str = ""
    credit_risk: str = ""
    rate_risk: str = ""
    positions_at_risk: list = field(default_factory=list)
    upcoming_events: list = field(default_factory=list)
    narrative: str = ""


@dataclass
class StockNarrative:
    code: str = ""
    name: str = ""
    total_score: float = 0.0
    grade: str = ""
    why_narrative: str = ""
    risk_flag: str = ""
    macro_alignment: str = ""


@dataclass
class BrainReport:
    date: str = ""
    generated_at: str = ""
    macro: MacroAssessment = field(default_factory=MacroAssessment)
    commodity: CommodityAssessment = field(default_factory=CommodityAssessment)
    sector: SectorAssessment = field(default_factory=SectorAssessment)
    flow: FlowAssessment = field(default_factory=FlowAssessment)
    risk: RiskAssessment = field(default_factory=RiskAssessment)
    overall_verdict: str = ""
    position_size_pct: int = 70
    position_size_reason: str = ""
    stock_narratives: list = field(default_factory=list)


# ═══════════════════════════════════════
#  Phase 1: 매크로 방향 판정
# ═══════════════════════════════════════

def _phase1_macro(nw: dict) -> MacroAssessment:
    m = MacroAssessment()
    if not nw:
        m.narrative = "NIGHTWATCH 데이터 없음"
        return m

    m.nxt_total = nw.get("total_score", 0)
    ri = nw.get("raw_indicators", {})
    mc = nw.get("macro_conditions", {})

    # 지표 추출
    m.vix = ri.get("VIX", {}).get("value", 0) or 0
    m.usdkrw = ri.get("USDKRW", {}).get("value", 0) or 0
    m.usdkrw_chg = ri.get("USDKRW", {}).get("change_pct", 0) or 0
    m.nasdaq_chg = mc.get("nasdaq_pct", 0) or 0
    m.tnx_value = ri.get("TNX", {}).get("value", 0) or 0
    m.tnx_chg = ri.get("TNX", {}).get("change_abs", 0) or 0
    m.gold_chg = ri.get("GOLD", {}).get("change_pct", 0) or 0
    m.hyg_chg = ri.get("HYG_div", {}).get("change_pct", 0) or ri.get("HYG", {}).get("change_pct", 0) or 0
    m.es_chg = ri.get("ES", {}).get("change_pct", 0) or 0

    # NXT-06: 한국장 강도 추출 (NXT 캘리브레이션 v2)
    m.korea_strength = nw.get("korea_strength", 0)

    # OPT-03: 옵션/선물 심리 반영
    try:
        from data.options_signal import get_brain_options_adjustment
        opt_adj, opt_detail = get_brain_options_adjustment()
        opt_adj = max(-3.0, min(3.0, opt_adj))  # ±3.0 클램핑 (매크로 1단계 이내)
        if abs(opt_adj) > 0:
            m.options_adj = opt_adj
            m.options_pc_level = opt_detail
            m.nxt_total += opt_adj  # BRAIN score에 직접 반영
            logger.info(f"[BRAIN] 옵션심리: {opt_adj:+.1f} ({opt_detail})")
    except Exception:
        pass

    # TIER2: ETF 수급 방어 점수 반영
    try:
        from data.etf_fund_flow import get_etf_flow_defense_score
        etf_defense = get_etf_flow_defense_score()
        etf_defense = max(-5.0, min(5.0, etf_defense))  # ±5.0 클램핑
        if abs(etf_defense) >= 1.0:
            m.nxt_total -= etf_defense  # 양수=하락경고 → BRAIN 점수 차감
            logger.info(f"[BRAIN] ETF수급방어: {-etf_defense:+.1f} (defense={etf_defense:.1f})")
    except Exception:
        pass

    # Step 1A: 기본 방향 (NXT-06: 한국장 강도 포함된 total 기준)
    t = m.nxt_total
    if t >= 7:
        m.direction = "STRONG_BULL"
    elif t >= 3:
        m.direction = "BULL"
    elif t >= -1:
        m.direction = "NEUTRAL"
    elif t >= -4:
        m.direction = "BEAR"
    else:
        m.direction = "STRONG_BEAR"

    # Step 1B: 모순 감지
    # 1) 숏커버 함정: 나스닥↑ + VIX 높음
    if m.nasdaq_chg > 0.3 and m.vix >= 24:
        m.contradictions.append(
            f"나스닥+{m.nasdaq_chg:.1f}% but VIX {m.vix:.1f}↑ → 숏커버 반등, 지속성 의문"
        )

    # 2) 크레딧 스트레스: ES↑ + HYG↓
    if m.es_chg > 0.2 and m.hyg_chg < -0.3:
        m.contradictions.append(
            f"주가↑{m.es_chg:+.1f}% but HYG{m.hyg_chg:+.1f}% → 크레딧 스트레스"
        )

    # 3) 외국인 이탈 압력: 원화 약세
    if m.usdkrw > 1490:
        m.contradictions.append(
            f"원화 {m.usdkrw:.0f}원({m.usdkrw_chg:+.1f}%) → 외국인 이탈 압력"
        )

    # 4) 금리 급등
    if m.tnx_chg > 0.05:
        m.contradictions.append(
            f"금리 +{m.tnx_chg:.3f}%p 급등({m.tnx_value:.2f}%) → 성장주 밸류에이션 압박"
        )

    # 5) 금 급락 + 주가 약세 = 마진콜 주의
    if m.gold_chg < -2.0 and m.nxt_total < 0:
        m.contradictions.append(
            f"금{m.gold_chg:+.1f}% 급락 + 매크로 약세 → 마진콜 청산 주의"
        )

    # Step 1C: 내러티브 생성
    active_text = mc.get("active_text", [])
    parts = []
    if active_text:
        parts.append(" + ".join(active_text))
    if m.contradictions:
        parts.append(". ".join(m.contradictions))
    else:
        dir_kr = {"STRONG_BULL": "강세", "BULL": "약강세", "NEUTRAL": "중립",
                  "BEAR": "약세", "STRONG_BEAR": "강약세"}
        parts.append(f"방향: {dir_kr.get(m.direction, m.direction)}")

    m.narrative = ". ".join(parts) if parts else "매크로 데이터 부족"
    return m


# ── FIX-05: 지수 기술적 분석 헬퍼 ──

def _calc_index_technicals() -> dict:
    """코스피 60일 일봉 → MA/MACD/RSI 계산.
    Returns: {"bull": bool, "bear": bool, "oversold": bool, "overbought": bool, "detail": str}
    실패 시 빈 dict → Phase 6에서 무시됨.
    """
    try:
        from pykrx import stock
        from datetime import timedelta

        end = date.today()
        start = end - timedelta(days=120)  # 60영업일 확보 위해 넉넉히
        df = stock.get_index_ohlcv(
            start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), "1001"  # 코스피
        )
        if df is None or len(df) < 30:
            return {}

        closes = df["종가"].values
        n = len(closes)

        # 이동평균
        ma5 = closes[-5:].mean()
        ma20 = closes[-20:].mean() if n >= 20 else ma5
        ma60 = closes[-60:].mean() if n >= 60 else ma20

        # MACD(12,26,9)
        def _ema(arr, span):
            alpha = 2 / (span + 1)
            out = [arr[0]]
            for v in arr[1:]:
                out.append(alpha * v + (1 - alpha) * out[-1])
            return out

        ema12 = _ema(list(closes), 12)
        ema26 = _ema(list(closes), 26)
        macd_line = [a - b for a, b in zip(ema12, ema26)]
        signal_line = _ema(macd_line, 9)
        macd_hist = macd_line[-1] - signal_line[-1]
        prev_hist = macd_line[-2] - signal_line[-2] if len(macd_line) > 1 else 0

        # RSI(14)
        if n >= 15:
            diffs = [closes[i] - closes[i - 1] for i in range(1, n)]
            gains = [max(d, 0) for d in diffs[-14:]]
            losses = [max(-d, 0) for d in diffs[-14:]]
            avg_gain = sum(gains) / 14
            avg_loss = sum(losses) / 14
            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 50

        cur = closes[-1]
        bull = cur > ma20 and macd_hist > 0 and prev_hist <= 0  # 골든크로스
        bear = cur < ma20 and macd_hist < 0
        oversold = rsi < 30
        overbought = rsi > 70

        parts = []
        parts.append(f"코스피{cur:,.0f}")
        if cur > ma20:
            parts.append(f"20MA↑{(cur/ma20-1)*100:+.1f}%")
        else:
            parts.append(f"20MA↓{(cur/ma20-1)*100:+.1f}%")
        parts.append(f"MACD{'↑' if macd_hist > 0 else '↓'}")
        parts.append(f"RSI{rsi:.0f}")

        return {
            "bull": bull, "bear": bear,
            "oversold": oversold, "overbought": overbought,
            "rsi": rsi, "macd_hist": macd_hist,
            "detail": " ".join(parts),
        }
    except Exception as e:
        logger.warning(f"[FIX-05] 지수 기술적 분석 실패: {e}")
        return {}


# ═══════════════════════════════════════
#  Phase 2: 원자재 사이클 판정
# ═══════════════════════════════════════

def _phase2_commodity(nw: dict) -> CommodityAssessment:
    c = CommodityAssessment()
    if not nw:
        c.narrative = "원자재 데이터 없음"
        return c

    ri = nw.get("raw_indicators", {})
    mc = nw.get("macro_conditions", {})

    c.gold_chg = ri.get("GOLD", {}).get("change_pct", 0) or 0
    c.oil_chg = ri.get("CL", {}).get("change_pct", 0) or 0
    c.copper_chg = ri.get("HG", {}).get("change_pct", 0) or 0
    c.ng_chg = ri.get("NG", {}).get("change_pct", 0) or 0
    c.silver_chg = ri.get("SI", {}).get("change_pct", 0) or 0

    # 릴레이 단계 감지
    relay = mc.get("commodity_relay")
    if relay:
        stage_map = {"gold": "GOLD_UP", "silver": "SILVER_RELAY", "copper": "COPPER_RELAY"}
        c.relay_stage = stage_map.get(relay, relay.upper())
    elif c.gold_chg > 1.5:
        c.relay_stage = "GOLD_UP"
    elif c.gold_chg < -2.0:
        c.relay_stage = "GOLD_DOWN"

    # 활성 시그널
    if mc.get("oil_up") or c.oil_chg > 1.5:
        c.active_signals.append("유가↑")
        c.beneficiary_sectors.append("정유/에너지")
    if mc.get("ng_up") or c.ng_chg > 2.0:
        c.active_signals.append("NG↑")
        c.beneficiary_sectors.append("천연가스/LNG")
    if c.copper_chg > 1.0:
        c.active_signals.append("구리↑")
        c.beneficiary_sectors.append("전력인프라/산업금속")
    if c.silver_chg > 1.0:
        c.active_signals.append("은↑")
        c.beneficiary_sectors.append("귀금속")

    # 내러티브
    parts = []
    icons = {"UP": "▲", "DOWN": "▼"}

    def _fmt(label, chg):
        icon = "▲" if chg > 0.3 else ("▼" if chg < -0.3 else "─")
        return f"{label}{icon}{chg:+.1f}%"

    price_str = " | ".join([
        _fmt("금", c.gold_chg), _fmt("유", c.oil_chg),
        _fmt("구리", c.copper_chg), _fmt("NG", c.ng_chg),
    ])
    parts.append(price_str)

    if c.relay_stage != "NONE":
        relay_kr = {
            "GOLD_UP": "금 상승(안전자산 선호)",
            "GOLD_DOWN": "금 급락(리스크온 or 마진콜)",
            "SILVER_RELAY": "금→은 릴레이 진행",
            "COPPER_RELAY": "금→은→구리 릴레이(경기 회복 신호)",
        }
        parts.append(relay_kr.get(c.relay_stage, c.relay_stage))
    else:
        parts.append("릴레이 미발동")

    if c.beneficiary_sectors:
        parts.append(f"수혜: {', '.join(c.beneficiary_sectors)}")

    c.narrative = ". ".join(parts)
    return c


# ═══════════════════════════════════════
#  Phase 3: 섹터 로테이션 판정
# ═══════════════════════════════════════

def _phase3_sector(history: dict, rotation_detail: list = None) -> SectorAssessment:
    sa = SectorAssessment()

    # rotation_detail(추천 파이프라인의 섹터 로테이션 결과)이 있으면 우선 사용
    if rotation_detail:
        for rd in rotation_detail:
            sector_name = rd.get("sector", "")
            phase = rd.get("phase", "")
            hot_days = rd.get("hot_days", 0)
            momentum = rd.get("momentum", 0)
            breadth = rd.get("breadth", 0)

            if phase in ("EARLY", "MID", "LATE", "HOT"):
                if hot_days <= 2:
                    cycle = "초기"
                elif hot_days <= 5:
                    cycle = "중기"
                else:
                    cycle = "소진주의"

                sa.hot_sectors.append({
                    "name": sector_name,
                    "key": sector_name,
                    "hot_days": hot_days,
                    "cycle_position": cycle,
                    "momentum": momentum,
                    "breadth": breadth,
                })
            elif phase == "STAGING":
                sa.next_sectors.append({
                    "name": sector_name,
                    "key": sector_name,
                    "warming_days": hot_days,
                    "vol_ratio": 0,
                    "breadth": breadth,
                    "momentum": momentum,
                })
            elif phase in ("COOLING", "REVERSAL"):
                sa.cooling_sectors.append({
                    "name": sector_name,
                    "key": sector_name,
                    "momentum": momentum,
                })

    # sector_history.json에서 보완 (rotation_detail이 없거나 빈 경우 fallback)
    if not sa.hot_sectors and not sa.next_sectors and history:
        dates = sorted(history.keys(), reverse=True)
        if not dates:
            sa.narrative = "섹터 데이터 없음"
            return sa

        latest_date = dates[0]
        latest = history[latest_date]
        sector_keys = list(latest.keys())

        for sk in sector_keys:
            sector_data = latest[sk]
            sector_name = sector_data.get("sector_name", sk)
            status = sector_data.get("status", "")
            momentum = sector_data.get("momentum_5d", 0) or 0
            breadth = sector_data.get("breadth", 0) or 0
            vol_ratio = sector_data.get("vol_ratio", 0) or 0

            hot_days = 0
            if status == "HOT":
                for d in dates:
                    s_data = history.get(d, {}).get(sk, {})
                    if s_data.get("status") == "HOT":
                        hot_days += 1
                    else:
                        break

            if status == "HOT":
                if hot_days <= 2:
                    cycle = "초기"
                elif hot_days <= 5:
                    cycle = "중기"
                else:
                    cycle = "소진주의"
                sa.hot_sectors.append({
                    "name": sector_name, "key": sk, "hot_days": hot_days,
                    "cycle_position": cycle, "momentum": momentum, "breadth": breadth,
                })
            elif status == "WARMING":
                warming_days = 0
                for d in dates:
                    s_data = history.get(d, {}).get(sk, {})
                    if s_data.get("status") in ("WARMING", "HOT"):
                        warming_days += 1
                    else:
                        break
                sa.next_sectors.append({
                    "name": sector_name, "key": sk, "warming_days": warming_days,
                    "vol_ratio": vol_ratio, "breadth": breadth, "momentum": momentum,
                })
            elif status == "COOLING":
                sa.cooling_sectors.append({
                    "name": sector_name, "key": sk, "momentum": momentum,
                })

    # FX 분류 태그 (BOND-P1)
    fx_tag = {}
    try:
        from data.fx_sector_signal import get_fx_type
        for hs in sa.hot_sectors:
            ft = get_fx_type(hs["name"])
            if ft in ("EXPORT", "IMPORT"):
                fx_tag[hs["name"]] = "수출" if ft == "EXPORT" else "수입"
    except Exception:
        pass

    # 인플레 분류 태그 (BOND-P3)
    infl_tag = {}
    try:
        from data.inflation_chain import get_inflation_class
        for hs in sa.hot_sectors:
            ic = get_inflation_class(hs["name"])
            if ic in ("VICTIM", "HEDGE"):
                infl_tag[hs["name"]] = "비용피해" if ic == "VICTIM" else "원자재헤지"
    except Exception:
        pass

    # 내러티브 생성
    parts = []
    for hs in sa.hot_sectors:
        tags = []
        fx_t = fx_tag.get(hs["name"], "")
        if fx_t:
            tags.append(fx_t)
        infl_t = infl_tag.get(hs["name"], "")
        if infl_t:
            tags.append(infl_t)
        tag_str = f"/{'/'.join(tags)}" if tags else ""
        parts.append(f"{hs['name']}(HOT {hs['hot_days']}일→{hs['cycle_position']}{tag_str})")
    for ns in sa.next_sectors:
        if ns["momentum"] > 3 or ns["vol_ratio"] > 1.0:
            parts.append(f"{ns['name']}(WARMING→HOT 임박)")
        else:
            parts.append(f"{ns['name']}(WARMING)")

    if not parts:
        parts.append("활성 HOT 섹터 없음")

    # 특수: COLD이지만 momentum 높은 섹터 (반전 조짐)
    if history:
        latest_key = sorted(history.keys())[-1]
        for sk, sd in history.get(latest_key, {}).items():
            if sd.get("status") == "COLD" and (sd.get("momentum_5d", 0) or 0) > 5:
                parts.append(
                    f"{sd.get('sector_name', sk)}: COLD but momentum↑{sd['momentum_5d']:.1f}% → 반전 조짐?"
                )

    sa.narrative = " | ".join(parts) if parts else "데이터 부족"
    return sa


# ── FIX-06: 시장 전체 수급 헬퍼 ──

def _calc_market_flow() -> dict:
    """코스피 투자자별 매매동향 5일 → 기관/외인 수급 판정.
    Returns: {"bull": bool, "bear": bool, "retail_panic": bool, "detail": str}
    """
    try:
        from pykrx import stock
        from datetime import timedelta

        end = date.today()
        start = end - timedelta(days=14)  # 5영업일 확보

        df = stock.get_market_trading_value_by_date(
            start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), "KOSPI"
        )
        if df is None or len(df) < 3:
            return {}

        # 최근 5일 (영업일)
        recent = df.tail(5)
        inst_col = "기관합계" if "기관합계" in recent.columns else None
        foreign_col = "외국인합계" if "외국인합계" in recent.columns else None
        retail_col = "개인" if "개인" in recent.columns else None

        if not inst_col or not foreign_col:
            # 컬럼명 다를 수 있음
            return {}

        inst_5d = recent[inst_col].sum()
        foreign_5d = recent[foreign_col].sum()
        retail_today = recent[retail_col].iloc[-1] if retail_col else 0

        # 연속 매수일 카운트
        inst_consec_buy = 0
        foreign_consec_buy = 0
        for _, row in recent.iloc[::-1].iterrows():
            if row[inst_col] > 0:
                inst_consec_buy += 1
            else:
                break
        for _, row in recent.iloc[::-1].iterrows():
            if row[foreign_col] > 0:
                foreign_consec_buy += 1
            else:
                break

        bull = inst_consec_buy >= 3 and foreign_consec_buy >= 3
        # 기관+외인 동반 순매도 3일
        inst_consec_sell = 0
        foreign_consec_sell = 0
        for _, row in recent.iloc[::-1].iterrows():
            if row[inst_col] < 0:
                inst_consec_sell += 1
            else:
                break
        for _, row in recent.iloc[::-1].iterrows():
            if row[foreign_col] < 0:
                foreign_consec_sell += 1
            else:
                break
        bear = inst_consec_sell >= 3 and foreign_consec_sell >= 3

        # 개인만 순매수 + 기관/외인 매도 = 위험
        retail_panic = (retail_today > 0 and
                        recent[inst_col].iloc[-1] < 0 and
                        recent[foreign_col].iloc[-1] < 0)

        parts = []
        parts.append(f"기관{inst_5d/1e8:+,.0f}억(5D)")
        parts.append(f"외인{foreign_5d/1e8:+,.0f}억(5D)")
        if bull:
            parts.append("동반매수")
        elif bear:
            parts.append("동반매도")
        if retail_panic:
            parts.append("개인쏠림⚠")

        return {
            "bull": bull, "bear": bear, "retail_panic": retail_panic,
            "detail": " ".join(parts),
        }
    except Exception as e:
        logger.warning(f"[FIX-06] 시장 수급 분석 실패: {e}")
        return {}


# ═══════════════════════════════════════
#  Phase 4: 수급 흐름 판정
# ═══════════════════════════════════════

def _phase4_flow(tv: dict, rec: dict) -> FlowAssessment:
    fa = FlowAssessment()

    # TV 스캐너 섹터별 QUIET_ACCUMULATION 집중도
    signals = tv.get("signals", [])
    sector_tv = {}  # sector → {qa: count, explosion: count, total: count}
    for sig in signals:
        sector = sig.get("sector", "기타")
        if sector not in sector_tv:
            sector_tv[sector] = {"qa": 0, "explosion": 0, "total": 0}
        sector_tv[sector]["total"] += 1
        if sig.get("pattern") == "QUIET_ACCUMULATION":
            sector_tv[sector]["qa"] += 1
        elif sig.get("pattern") == "EXPLOSION":
            sector_tv[sector]["explosion"] += 1

    # 상위 섹터 정렬 (QUIET_ACCUMULATION 우선)
    sorted_sectors = sorted(sector_tv.items(),
                            key=lambda x: (x[1]["qa"], x[1]["total"]),
                            reverse=True)
    for sector, counts in sorted_sectors[:5]:
        if counts["total"] >= 2:
            fa.tv_sector_summary.append({
                "sector": sector,
                "qa": counts["qa"],
                "explosion": counts["explosion"],
                "total": counts["total"],
            })

    # recommendation.json에서 flow_signal 분석
    stocks = rec.get("stocks", [])
    buy_types = {"STRONG_BUY": 0, "BUY": 0, "NEUTRAL": 0, "CAUTION": 0, "SELL": 0}
    flow_details = []
    for s in stocks[:8]:
        fs = s.get("flow_signal", "")
        if fs in buy_types:
            buy_types[fs] += 1
        fd = s.get("flow_detail", "")
        if fd:
            flow_details.append(fd)

    fa.strong_buy_count = buy_types["STRONG_BUY"]
    fa.sell_count = buy_types["SELL"] + buy_types["CAUTION"]

    # dominant buyer 결정
    if fa.strong_buy_count >= 3:
        fa.dominant_buyer = "강매수 우위"
    elif fa.strong_buy_count >= 1 and fa.sell_count == 0:
        fa.dominant_buyer = "매수 우위"
    elif fa.sell_count >= 2:
        fa.dominant_buyer = "매도 우위"
    else:
        fa.dominant_buyer = "혼조"

    # 국적 동향 파싱 (nat_power_detail에서)
    for s in stocks[:5]:
        npd = s.get("nat_power_detail", "")
        if npd:
            m = re.search(r"급증:(.+?)(?:\||$)", npd)
            if m:
                fa.nationality_signals.append(
                    f"{s.get('name', '')} — {m.group(1).strip()} 진입"
                )

    # 내러티브
    parts = []
    if fa.tv_sector_summary:
        top_tv = fa.tv_sector_summary[:3]
        tv_parts = []
        for ts in top_tv:
            if ts["qa"] >= 2:
                tv_parts.append(f"{ts['sector']} QUIET_ACCUM {ts['qa']}종목")
            elif ts["explosion"] >= 2:
                tv_parts.append(f"{ts['sector']} EXPLOSION {ts['explosion']}종목")
            else:
                tv_parts.append(f"{ts['sector']} TV {ts['total']}종목")
        parts.append(" | ".join(tv_parts))

    parts.append(f"수급: {fa.dominant_buyer} (STRONG_BUY {fa.strong_buy_count}건)")

    if fa.nationality_signals:
        parts.append(fa.nationality_signals[0])  # 가장 주요 1건만

    # Z-score 통계적 이상치 감지 (flow_zscore 연동)
    try:
        from data.flow_zscore import get_brain_flow_zscore_adj
        z_adj, z_detail = get_brain_flow_zscore_adj()
        if z_adj != 0 or z_detail:
            fa.flow_zscore_adj = z_adj
            fa.flow_zscore_detail = z_detail
            # adj 기반 level 분류
            if z_adj >= 4.0:
                fa.flow_zscore_level = "STRONG_BUY"
            elif z_adj >= 2.0:
                fa.flow_zscore_level = "BUY"
            elif z_adj <= -4.0:
                fa.flow_zscore_level = "SELL"
            elif z_adj <= -2.0:
                fa.flow_zscore_level = "CAUTION"
            elif z_adj == 0 and "괴리" in z_detail:
                fa.flow_zscore_level = "DIVERGENCE"
            else:
                fa.flow_zscore_level = "NEUTRAL"
            parts.append(f"Z-score {fa.flow_zscore_level}({z_adj:+.1f})")
    except Exception as e:
        logger.debug(f"[BRAIN] Flow Z-score 연동 스킵: {e}")

    fa.narrative = ". ".join(parts) if parts else "수급 데이터 부족"
    return fa


# ═══════════════════════════════════════
#  Phase 5: 리스크 판정
# ═══════════════════════════════════════

def _phase5_risk(nw: dict, guardian: dict, events: dict) -> RiskAssessment:
    ra = RiskAssessment()
    ri = nw.get("raw_indicators", {})
    divs = nw.get("divergences", [])

    vix = ri.get("VIX", {}).get("value", 0) or 0
    usdkrw = ri.get("USDKRW", {}).get("value", 0) or 0
    tnx_chg = ri.get("TNX", {}).get("change_abs", 0) or 0
    hyg_chg = ri.get("HYG_div", {}).get("change_pct", 0) or ri.get("HYG", {}).get("change_pct", 0) or 0
    es_chg = ri.get("ES", {}).get("change_pct", 0) or 0

    # VIX 리스크
    if vix < 20:
        ra.vix_risk = "정상"
    elif vix < 25:
        ra.vix_risk = "경계"
        ra.risk_score += 20
    elif vix < 30:
        ra.vix_risk = "공포"
        ra.risk_score += 40
    else:
        ra.vix_risk = "패닉"
        ra.risk_score += 70

    # 환율 리스크
    if usdkrw < 1470:
        ra.currency_risk = "안정"
    elif usdkrw < 1490:
        ra.currency_risk = "주의"
        ra.risk_score += 10
    elif usdkrw < 1510:
        ra.currency_risk = "경고"
        ra.risk_score += 20
    else:
        ra.currency_risk = "위험"
        ra.risk_score += 30

    # 크레딧 리스크
    if es_chg > 0.2 and hyg_chg < -0.3:
        ra.credit_risk = "스트레스"
        ra.risk_score += 25
    else:
        ra.credit_risk = "정상"

    # 금리 리스크
    if tnx_chg > 0.10:
        ra.rate_risk = "급등"
        ra.risk_score += 20
    elif tnx_chg > 0.05:
        ra.rate_risk = "상승"
        ra.risk_score += 10
    else:
        ra.rate_risk = "안정"

    # 포지션 리스크 (guardian)
    for v in guardian.get("verdicts", []):
        action = v.get("action", "")
        if action in ("EXIT", "REDUCE"):
            ra.positions_at_risk.append({
                "name": v.get("name", ""),
                "action": action,
                "risk_score": v.get("risk_score", 0),
            })
            ra.risk_score += 15 if action == "EXIT" else 5

    # 이벤트 리스크
    for cat in ("economic", "earnings", "alerts"):
        for ev in events.get(cat, []):
            ra.upcoming_events.append(ev.get("title", str(ev)))

    # 최종 risk_level
    if ra.risk_score < 30:
        ra.risk_level = "LOW"
    elif ra.risk_score < 55:
        ra.risk_level = "MEDIUM"
    elif ra.risk_score < 75:
        ra.risk_level = "HIGH"
    else:
        ra.risk_level = "EXTREME"

    # 내러티브
    parts = []
    if ra.vix_risk != "정상":
        parts.append(f"VIX {vix:.1f} {ra.vix_risk}")
    if ra.credit_risk == "스트레스":
        parts.append(f"HYG{hyg_chg:+.1f}%+ES{es_chg:+.1f}% 크레딧 스트레스")
    if ra.currency_risk not in ("안정", ""):
        parts.append(f"원화 {usdkrw:.0f} {ra.currency_risk}")
    if ra.rate_risk not in ("안정", ""):
        parts.append(f"금리 {ra.rate_risk}")
    if ra.positions_at_risk:
        risk_names = [f"{p['name']}({p['action']})" for p in ra.positions_at_risk[:3]]
        parts.append(f"포지션: {', '.join(risk_names)}")

    ra.narrative = " | ".join(parts) if parts else "리스크 낮음"
    return ra


# ═══════════════════════════════════════
#  Phase 6: 종합 판정 + 종목 서술
# ═══════════════════════════════════════

def _phase6_synthesis(
    macro: MacroAssessment,
    commodity: CommodityAssessment,
    sector: SectorAssessment,
    flow: FlowAssessment,
    risk: RiskAssessment,
    rec: dict,
    themes: dict,
    insights: dict = None,
    index_tech: dict = None,
    market_flow: dict = None,
    brain_perf: list = None,
    nw: dict = None,
) -> tuple:
    """Returns: (overall_verdict, position_size_pct, position_size_reason, stock_narratives)"""

    # ── 포지션 사이징 ──
    score = 0
    reasons = []

    # 1. 매크로 베이스라인
    macro_pts = {
        "STRONG_BULL": 40, "BULL": 20, "NEUTRAL": 0,
        "BEAR": -20, "STRONG_BEAR": -40,
    }
    pts = macro_pts.get(macro.direction, 0)
    score += pts
    if pts != 0:
        dir_kr = {"STRONG_BULL": "강세", "BULL": "약강세", "NEUTRAL": "중립",
                  "BEAR": "약세", "STRONG_BEAR": "강약세"}
        reasons.append(f"매크로{dir_kr.get(macro.direction, '')}({int(pts):+d})")

    # 2. 리스크 조정
    risk_adj = {"LOW": 0, "MEDIUM": -10, "HIGH": -30, "EXTREME": -50}
    r_pts = risk_adj.get(risk.risk_level, 0)
    score += r_pts
    if r_pts < 0:
        reasons.append(f"리스크{risk.risk_level}({int(r_pts):+d})")

    # 3. 섹터 보너스
    hot_early = [s for s in sector.hot_sectors if s.get("cycle_position") == "초기"]
    hot_mid = [s for s in sector.hot_sectors if s.get("cycle_position") == "중기"]
    if len(hot_early) >= 2:
        score += 15
        reasons.append("섹터초기HOT2개+(+15)")
    elif hot_early:
        score += 8
        reasons.append("섹터초기HOT(+8)")
    elif hot_mid:
        score += 3
        reasons.append("섹터중기HOT(+3)")
    elif not sector.hot_sectors and not sector.next_sectors:
        score -= 10
        reasons.append("활성섹터없음(-10)")

    # 4. 수급 보너스
    qa_sectors = sum(1 for ts in flow.tv_sector_summary if ts.get("qa", 0) >= 2)
    if qa_sectors >= 2:
        score += 10
        reasons.append(f"TV매집클러스터{qa_sectors}개(+10)")
    elif flow.strong_buy_count >= 3:
        score += 5
        reasons.append(f"STRONG_BUY{flow.strong_buy_count}건(+5)")

    # 5. 원자재 모멘텀 (FIX-03)
    n_active = len(commodity.active_signals)
    if n_active >= 2:
        score += 8
        reasons.append(f"원자재활성{n_active}건(+8)")
    elif n_active >= 1:
        score += 3
        reasons.append(f"원자재활성{n_active}건(+3)")
    if commodity.relay_stage not in (None, "NONE", ""):
        score += 5
        reasons.append(f"릴레이{commodity.relay_stage}(+5)")
    # 원자재 전면 하락 (금+유가 동반 하락)
    if commodity.gold_chg < -1.0 and commodity.oil_chg < -1.0:
        score -= 5
        reasons.append("원자재하락(-5)")

    # 6. 모순 페널티
    n_cont = len(macro.contradictions)
    if n_cont > 0:
        score -= n_cont * 5
        reasons.append(f"모순{n_cont}건({int(-n_cont*5):+d})")

    # 7. 지수 기술적 분석 (FIX-05)
    if index_tech:
        if index_tech.get("bull"):
            score += 5
            reasons.append(f"지수MACD골든({index_tech.get('detail', '')})(+5)")
        elif index_tech.get("bear"):
            score -= 10
            reasons.append(f"지수약세({index_tech.get('detail', '')})(-10)")
        if index_tech.get("oversold"):
            score += 3
            reasons.append("RSI과매도(+3)")
        elif index_tech.get("overbought"):
            score -= 5
            reasons.append("RSI과열(-5)")

    # 8. 학습 인사이트 반영 (FIX-04)
    if insights:
        # 전체 시스템 적중률 (source_weights 평균)
        sw = insights.get("source_weights", {})
        if sw:
            hit_rates = [v.get("hit_rate", 50) for v in sw.values()
                         if isinstance(v, dict) and v.get("sample", 0) >= 3]
            if hit_rates:
                avg_hit = sum(hit_rates) / len(hit_rates)
                if avg_hit < 50:
                    score -= 5
                    reasons.append(f"학습적중률{avg_hit:.0f}%↓(-5)")
                elif avg_hit >= 70:
                    score += 3
                    reasons.append(f"학습적중률{avg_hit:.0f}%↑(+3)")

        # 섹터별 부스트 (sector_boost 맵)
        sb = insights.get("sector_boost", {})
        if sb and sector.hot_sectors:
            for hs in sector.hot_sectors:
                sname = hs.get("name", "")
                boost = sb.get(sname, 0)
                if boost > 0:
                    score += 3
                    reasons.append(f"학습섹터{sname}(+3)")
                    break  # 최대 1개만

    # 9. 시장 전체 수급 (FIX-06)
    if market_flow:
        if market_flow.get("bull"):
            score += 8
            reasons.append(f"기관외인동반매수(+8)")
        elif market_flow.get("bear"):
            score -= 8
            reasons.append(f"기관외인동반매도(-8)")
        if market_flow.get("retail_panic"):
            score -= 5
            reasons.append("개인쏠림(-5)")

    # 9.5 수급 Z-score 통계적 이상치 보정
    if flow.flow_zscore_adj != 0:
        z_adj = max(-5.0, min(5.0, flow.flow_zscore_adj))
        score += z_adj
        reasons.append(f"수급Z{flow.flow_zscore_level}({z_adj:+.1f})")

    # 10. BRAIN 자기 학습 적중률 (FIX-08)
    if brain_perf and len(brain_perf) >= 5:
        recent = brain_perf[-10:]  # 최근 10일
        correct_count = sum(1 for r in recent if r.get("correct"))
        hit_rate = correct_count / len(recent) * 100
        if hit_rate < 40:
            score -= 5
            reasons.append(f"BRAIN적중률{hit_rate:.0f}%↓(-5)")
        elif hit_rate > 70:
            score += 3
            reasons.append(f"BRAIN적중률{hit_rate:.0f}%↑(+3)")

    # 11. 채권금리 환경 (BOND-01)
    try:
        from data.bond_yield_signal import get_brain_bond_adjustment
        ri = nw.get("raw_indicators", {}) if isinstance(nw, dict) else {}
        bond_adj, bond_detail = get_brain_bond_adjustment(ri)
        bond_adj = max(-5.0, min(5.0, bond_adj))
        if abs(bond_adj) >= 0.5:
            score += bond_adj
            reasons.append(f"채권{bond_detail}({bond_adj:+.1f})")
    except Exception:
        pass

    # 12. 상대가치 ERP (BOND-P2: 주식 vs 채권 기대수익률)
    try:
        from data.bond_yield_signal import get_erp_brain_adjustment
        ri = nw.get("raw_indicators", {}) if isinstance(nw, dict) else {}
        erp_adj, erp_detail = get_erp_brain_adjustment(ri)
        erp_adj = max(-3.0, min(3.0, erp_adj))
        if abs(erp_adj) >= 0.5:
            score += erp_adj
            reasons.append(f"상대가치{erp_detail}({erp_adj:+.1f})")
    except Exception:
        pass

    # 13. 인플레이션 비용 체인 (BOND-P3: CPI/PPI 대리 → 기업이익 영향)
    try:
        from data.inflation_chain import get_inflation_brain_adjustment
        ri = nw.get("raw_indicators", {}) if isinstance(nw, dict) else {}
        infl_adj, infl_detail = get_inflation_brain_adjustment(ri)
        infl_adj = max(-3.0, min(3.0, infl_adj))
        if abs(infl_adj) >= 0.5:
            score += infl_adj
            reasons.append(f"인플레{infl_detail}({infl_adj:+.1f})")
    except Exception:
        pass

    # 결정
    if score >= 20:
        pct, label = 100, "공격"
    elif score >= 0:
        pct, label = 70, "표준"
    elif score >= -20:
        pct, label = 50, "방어"
    elif score >= -40:
        pct, label = 30, "최소"
    else:
        pct, label = 0, "관망"

    # 14. 매크로 전략 비중 캡 (macro_strategy → 레짐별 상한)
    try:
        from data.macro_strategy import get_budget_cap
        macro_cap_pct, _ = get_budget_cap()
        if macro_cap_pct < pct:
            reasons.append(f"매크로캡{macro_cap_pct}%")
            pct = macro_cap_pct
            label = f"매크로제한({label})"
    except Exception:
        pass

    size_reason = f"{label}({int(round(score)):+d}) — " + " | ".join(reasons)

    # ── 종합 판정 문장 ──
    dir_kr = {"STRONG_BULL": "강세", "BULL": "약강세", "NEUTRAL": "중립",
              "BEAR": "약세", "STRONG_BEAR": "강약세"}
    verdict_parts = []

    # 시장 방향
    verdict_parts.append(
        f"매크로 {dir_kr.get(macro.direction, '')}(NXT {macro.nxt_total:+.0f}점)"
    )

    # 섹터 현황
    if sector.hot_sectors or sector.next_sectors:
        active_names = []
        for hs in sector.hot_sectors:
            active_names.append(hs["name"])
        for ns in sector.next_sectors:
            if ns.get("momentum", 0) > 3:
                active_names.append(ns["name"])
        if active_names:
            verdict_parts[-1] += f"이나 {'/'.join(active_names[:3])} 국지 강세"

    # 전략
    size_kr = {100: "전면 진입", 70: "표준 진입",
               50: "소규모 진입(비중 50%)", 30: "최소 진입(비중 30%)",
               0: "관망 권고"}
    verdict_parts.append(f"{size_kr.get(pct, f'비중 {pct}%')} 권장")

    # VIX 경고
    if macro.vix >= 25:
        verdict_parts.append(f"VIX {macro.vix:.0f}+ 손절 철저")

    overall_verdict = ". ".join(verdict_parts)

    # ── 종목별 서술 ──
    # 섹터 HOT 맵 구축
    sector_ctx = {}
    for hs in sector.hot_sectors:
        sector_ctx[hs["key"]] = hs
    for ns in sector.next_sectors:
        sector_ctx[ns["key"]] = ns

    # macro_themes beneficiary 맵
    theme_map = {}  # code → theme_name
    for t in themes.get("themes", []):
        if t.get("status") == "ACTIVE":
            for b in t.get("beneficiaries", []):
                theme_map[b.get("ticker", "")] = t.get("name", "")

    stock_narratives = []
    for s in rec.get("stocks", [])[:8]:
        sn = _build_stock_narrative(s, sector_ctx, macro, theme_map)
        stock_narratives.append(sn)

    return overall_verdict, pct, size_reason, stock_narratives


def _build_stock_narrative(
    stock: dict,
    sector_ctx: dict,
    macro: MacroAssessment,
    theme_map: dict,
) -> StockNarrative:
    """개별 종목 1행 서술 생성"""
    sn = StockNarrative(
        code=stock.get("code", ""),
        name=stock.get("name", ""),
        total_score=stock.get("total_score", 0),
        grade=stock.get("grade", ""),
    )

    parts = []

    # 1) 섹터 컨텍스트
    sources = stock.get("sources", [])
    for src in sources:
        if src.startswith("sector_hot:"):
            sector_name = src.split(":")[1]
            # sector_ctx에서 hot_days 찾기
            for sk, ctx in sector_ctx.items():
                ctx_name = ctx.get("name", "")
                if (ctx_name == sector_name
                        or sector_name in ctx_name
                        or ctx_name in sector_name
                        or sk.lower() in sector_name.lower().replace("/", "")):
                    hd = ctx.get("hot_days", ctx.get("warming_days", 0))
                    cp = ctx.get("cycle_position", "WARMING")
                    parts.append(f"{sector_name}HOT{hd}일({cp})")
                    break
            else:
                parts.append(f"{sector_name}")
            break
        elif src.startswith("rotation:"):
            rot_info = src.split(":")[1].split("(")
            if len(rot_info) >= 2:
                parts.append(f"로테이션:{rot_info[1].rstrip(')')}")

    # 2) 수급 컨텍스트
    flow_detail = stock.get("flow_detail", "")
    if flow_detail:
        # "🏛기관매집 + 🐉아시아진입" → "기관매집+외국인진입"
        flow_kr = []
        if "기관매집" in flow_detail:
            flow_kr.append("기관매집")
        if "아시아진입" in flow_detail or "미국매수" in flow_detail:
            flow_kr.append("외국인진입")
        if "복합매수" in flow_detail:
            flow_kr.append("복합매수")
        if flow_kr:
            parts.append("+".join(flow_kr))

    # 3) TV 컨텍스트
    tv_pattern = stock.get("tv_pattern", "")
    tv_ratio = stock.get("tv_ratio", 1.0)
    if tv_pattern == "QUIET_ACCUMULATION" and tv_ratio >= 2.0:
        parts.append(f"TV{tv_ratio:.0f}x매집")
    elif tv_pattern == "EXPLOSION":
        parts.append(f"TV{tv_ratio:.0f}x폭발")
    elif tv_pattern == "GRADUAL_BUILDUP" and tv_ratio >= 1.5:
        parts.append(f"TV점진축적")

    # 4) 매크로 정합성
    if macro.direction in ("BEAR", "STRONG_BEAR"):
        # 약세장에서 종목이 추천됨 → 역행
        sn.macro_alignment = "역행(섹터 독립)"
    elif macro.direction in ("BULL", "STRONG_BULL"):
        sn.macro_alignment = "정렬"
    else:
        sn.macro_alignment = "중립"

    # 5) 매크로 테마 정렬
    theme = theme_map.get(stock.get("code", ""))
    if theme:
        parts.append(f"테마:{theme}")

    # 6) 리스크 플래그
    risk_flags = []
    if "헤지이탈" in flow_detail:
        risk_flags.append("헤지이탈")
    nat_detail = stock.get("nationality_detail", "")
    if nat_detail and "외-" in nat_detail:
        # "외-10(3d)" 패턴
        m = re.search(r"외-(\d+)\((\d+)d\)", nat_detail)
        if m and int(m.group(1)) >= 10:
            risk_flags.append(f"외국인{m.group(2)}일 이탈")
    if risk_flags:
        sn.risk_flag = " | ".join(risk_flags)

    # 최종 조합
    why = " + ".join(parts) if parts else f"종합점수 {sn.total_score:.0f}"
    if sn.macro_alignment == "역행(섹터 독립)":
        why += " (매크로 역행이나 섹터 독립 강도)"

    sn.why_narrative = why
    return sn


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_brain_telegram(report: BrainReport) -> str:
    """Brain 리포트를 텔레그램 서술형 메시지로 변환"""
    DIR_EMOJI = {
        "STRONG_BULL": "🟢🟢", "BULL": "🟢", "NEUTRAL": "🟡",
        "BEAR": "🔴", "STRONG_BEAR": "💀",
    }
    DIR_KR = {
        "STRONG_BULL": "강세", "BULL": "약강세", "NEUTRAL": "중립",
        "BEAR": "약세", "STRONG_BEAR": "강약세",
    }
    RISK_EMOJI = {
        "LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴", "EXTREME": "💀",
    }

    m = report.macro
    c = report.commodity
    s = report.sector
    f = report.flow
    r = report.risk

    lines = [
        f"🧠 마켓 브레인 ({report.date})",
        "=" * 32,
    ]

    # 기준선 요약 (있으면 상단에 표시)
    try:
        from data.macro_baseline import get_baseline_summary
        bl_summary = get_baseline_summary()
        if bl_summary and "데이터 없음" not in bl_summary:
            lines.append(bl_summary)
            lines.append("")
    except Exception:
        pass

    # Phase 1: 매크로
    d_emoji = DIR_EMOJI.get(m.direction, "⚪")
    d_kr = DIR_KR.get(m.direction, m.direction)
    kr_str = m.korea_strength
    kr_tag = f" | 한국장{kr_str:+.1f}" if kr_str else ""
    lines.append(f"📊 매크로: {d_emoji} {d_kr} (NXT {m.nxt_total:+.0f}점{kr_tag})")
    if m.narrative:
        lines.append(f"  {m.narrative}")

    lines.append("")

    # Phase 2: 원자재
    lines.append(f"⛏️ 원자재: {c.narrative}")

    lines.append("")

    # Phase 3: 섹터
    lines.append(f"🏗️ 섹터: {s.narrative}")

    lines.append("")

    # Phase 4: 수급
    lines.append(f"💰 수급: {f.narrative}")
    if f.flow_zscore_detail:
        z_emoji = {"STRONG_BUY": "🔴", "BUY": "🟠", "SELL": "🔵",
                   "CAUTION": "🟡", "DIVERGENCE": "⚡", "NEUTRAL": "⚪"}.get(
            f.flow_zscore_level, "⚪")
        lines.append(f"  📊 수급Z: {z_emoji} {f.flow_zscore_detail}")

    lines.append("")

    # Phase 5: 리스크
    r_emoji = RISK_EMOJI.get(r.risk_level, "⚪")
    lines.append(f"⚠️ 리스크: {r_emoji} {r.risk_level} ({r.risk_score:.0f}점)")
    if r.narrative:
        lines.append(f"  {r.narrative}")

    lines.append("")

    # Phase 6: 종합 판정
    lines.append(f"📋 종합: \"{report.overall_verdict}\"")
    lines.append(f"   투자 비중: {report.position_size_pct}% — {report.position_size_reason}")

    # 종목 서술
    if report.stock_narratives:
        lines.append("")
        lines.append("🏆 추천 종목:")
        for i, sn in enumerate(report.stock_narratives[:5], 1):
            grade_str = f"[{sn.grade}]" if sn.grade else ""
            lines.append(
                f"{i}. {sn.name}({sn.code}) {grade_str} {sn.total_score:.0f}점"
            )
            lines.append(f"   \"{sn.why_narrative}\"")
            if sn.risk_flag:
                lines.append(f"   ⚠️ {sn.risk_flag}")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  메인 진입점
# ═══════════════════════════════════════

def generate_brain_report() -> BrainReport:
    """전체 6-Phase 파이프라인 실행 → BrainReport 반환"""
    logger.info("[Market Brain] 합성 시작...")

    # 데이터 로드
    nw = _load_nightwatch()
    history = _load_sector_history()
    tv = _load_tv_scanner()
    rec = _load_recommendation()
    guardian = _load_guardian()
    events = _load_global_events()
    themes = _load_macro_themes()
    insights = _load_insights()  # FIX-04: 학습 데이터 연결
    index_tech = _calc_index_technicals()  # FIX-05: 지수 기술적 분석
    market_flow = _calc_market_flow()  # FIX-06: 시장 전체 수급
    brain_perf = _load_brain_performance()  # FIX-08: BRAIN 자기 학습

    # 6 Phase 실행
    macro = _phase1_macro(nw)
    logger.info(f"  [Phase 1] 매크로: {macro.direction} (NXT {macro.nxt_total:+.0f})")

    commodity = _phase2_commodity(nw)
    logger.info(f"  [Phase 2] 원자재: {commodity.relay_stage}")

    sector = _phase3_sector(history, rotation_detail=rec.get("rotation_detail", []))
    logger.info(f"  [Phase 3] 섹터: HOT {len(sector.hot_sectors)}, NEXT {len(sector.next_sectors)}")

    flow = _phase4_flow(tv, rec)
    logger.info(f"  [Phase 4] 수급: {flow.dominant_buyer}")

    risk = _phase5_risk(nw, guardian, events)
    logger.info(f"  [Phase 5] 리스크: {risk.risk_level} ({risk.risk_score:.0f}점)")

    verdict, pct, reason, narratives = _phase6_synthesis(
        macro, commodity, sector, flow, risk, rec, themes,
        insights, index_tech, market_flow, brain_perf, nw,
    )
    logger.info(f"  [Phase 6] 판정: 비중 {pct}% | {verdict[:60]}")

    report = BrainReport(
        date=str(date.today()),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        macro=macro,
        commodity=commodity,
        sector=sector,
        flow=flow,
        risk=risk,
        overall_verdict=verdict,
        position_size_pct=pct,
        position_size_reason=reason,
        stock_narratives=narratives,
    )

    logger.info("[Market Brain] 합성 완료")
    return report


def save_brain_report(report: BrainReport) -> None:
    """brain_report.json 저장 (atomic write)"""
    BRAIN_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # dataclass → dict (중첩 dataclass도 처리)
    data = asdict(report)

    tmp = BRAIN_REPORT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(BRAIN_REPORT_PATH)
    logger.info(f"[Market Brain] 저장: {BRAIN_REPORT_PATH}")

    # FIX-02: brain_allocation.json도 같이 갱신 (auto_trader 연동)
    _save_brain_allocation(report)


def _save_brain_allocation(report: BrainReport) -> None:
    """brain_allocation.json — auto_trader가 읽는 포맷으로 저장"""
    pct = report.position_size_pct  # 0 / 30 / 50 / 70 / 100

    # 포지션 캡: auto_trader의 max_auto_positions(2)에 곱해지는 비율
    # 관망(0%) → 0, 최소(30%) → 0.5, 방어(50%) → 1.0, 표준/공격 → 1.0
    if pct == 0:
        pos_cap = 0.0
    elif pct <= 30:
        pos_cap = 0.5
    else:
        pos_cap = 1.0

    label_map = {100: "공격", 70: "표준", 50: "방어", 30: "최소", 0: "관망"}
    label = label_map.get(pct, "표준")

    alloc = {
        "timestamp": report.generated_at,
        "effective_regime": label,
        "position_size_pct": pct,
        "cross_signal": {
            "max_positions_cap": pos_cap,
            "mode_kr": label,
        },
        "overall_verdict": report.overall_verdict,
        "source": "market_brain",
    }

    alloc_path = DATA_DIR / "brain_allocation.json"
    tmp = alloc_path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(alloc, f, ensure_ascii=False, indent=2)
    tmp.replace(alloc_path)
    logger.info(f"[Market Brain] allocation 저장: {alloc_path} ({label} {pct}%)")


def load_brain_report() -> BrainReport | None:
    """저장된 brain_report.json → BrainReport 복원"""
    data = _load_json(BRAIN_REPORT_PATH)
    if not data:
        return None

    report = BrainReport(
        date=data.get("date", ""),
        generated_at=data.get("generated_at", ""),
        overall_verdict=data.get("overall_verdict", ""),
        position_size_pct=data.get("position_size_pct", 70),
        position_size_reason=data.get("position_size_reason", ""),
    )

    # Phase 결과 복원
    if data.get("macro"):
        report.macro = MacroAssessment(**{k: v for k, v in data["macro"].items()
                                          if k in MacroAssessment.__dataclass_fields__})
    if data.get("commodity"):
        report.commodity = CommodityAssessment(**{k: v for k, v in data["commodity"].items()
                                                   if k in CommodityAssessment.__dataclass_fields__})
    if data.get("sector"):
        report.sector = SectorAssessment(**{k: v for k, v in data["sector"].items()
                                             if k in SectorAssessment.__dataclass_fields__})
    if data.get("flow"):
        report.flow = FlowAssessment(**{k: v for k, v in data["flow"].items()
                                         if k in FlowAssessment.__dataclass_fields__})
    if data.get("risk"):
        report.risk = RiskAssessment(**{k: v for k, v in data["risk"].items()
                                         if k in RiskAssessment.__dataclass_fields__})

    # 종목 서술 복원
    for sn_data in data.get("stock_narratives", []):
        report.stock_narratives.append(
            StockNarrative(**{k: v for k, v in sn_data.items()
                             if k in StockNarrative.__dataclass_fields__})
        )

    return report


# ═══════════════════════════════════════
#  FIX-07: 장중 긴급 재평가
# ═══════════════════════════════════════

def emergency_reassess() -> dict | None:
    """장중 리스크 급변 시 Phase 5만 재계산 → brain_allocation 갱신.

    트리거 조건 (하나라도 충족 시):
      - VIX 30+ 돌파 (기존 BRAIN 판단 시 VIX < 25)
      - USDKRW 1510+ 돌파
      - S&P 선물(ES) -2% 이상 급락

    Returns:
      {"triggered": True, "new_pct": 30, "reason": "..."} or None (미트리거)
    """
    # 현재 brain_report 로드
    report = load_brain_report()
    if not report:
        return None

    # 실시간 nightwatch 데이터
    nw = _load_nightwatch()
    if not nw:
        return None

    ri = nw.get("raw_indicators", {})
    current_vix = ri.get("VIX", {}).get("value", 0) or 0
    current_usdkrw = ri.get("USDKRW", {}).get("value", 0) or 0
    current_es_chg = ri.get("ES", {}).get("change_pct", 0) or 0

    original_vix = report.macro.vix
    original_pct = report.position_size_pct

    # 트리거 체크
    triggers = []
    if current_vix >= 30 and original_vix < 25:
        triggers.append(f"VIX {original_vix:.0f}→{current_vix:.0f} 패닉")
    if current_usdkrw >= 1510:
        triggers.append(f"원화 {current_usdkrw:.0f}원 위험")
    if current_es_chg <= -2.0:
        triggers.append(f"S&P {current_es_chg:+.1f}% 급락")

    if not triggers:
        return None

    # Phase 5만 재계산
    guardian = _load_guardian()
    events = _load_global_events()
    new_risk = _phase5_risk(nw, guardian, events)

    # 기존 Phase 6 score에서 리스크만 교체
    # 원래 리스크 조정분 복원 → 새 리스크 적용
    old_risk_adj = {"LOW": 0, "MEDIUM": -10, "HIGH": -30, "EXTREME": -50}
    old_pts = old_risk_adj.get(report.risk.risk_level, 0)
    new_pts = old_risk_adj.get(new_risk.risk_level, 0)

    # 원래 score 역산 — 비표준 pct도 보간 지원
    _boundaries = [(100, 20), (70, 0), (50, -20), (30, -40), (0, -41)]
    estimated_old_score = 0
    for i, (pct_hi, sc_hi) in enumerate(_boundaries):
        if original_pct >= pct_hi:
            estimated_old_score = sc_hi
            break
        if i + 1 < len(_boundaries):
            pct_lo, sc_lo = _boundaries[i + 1]
            if original_pct >= pct_lo:
                # 선형 보간
                ratio = (original_pct - pct_lo) / max(1, pct_hi - pct_lo)
                estimated_old_score = sc_lo + (sc_hi - sc_lo) * ratio
                break
    else:
        estimated_old_score = -41
    new_score = estimated_old_score - old_pts + new_pts

    # 새 비중 결정
    if new_score >= 20:
        new_pct = 100
    elif new_score >= 0:
        new_pct = 70
    elif new_score >= -20:
        new_pct = 50
    elif new_score >= -40:
        new_pct = 30
    else:
        new_pct = 0

    # 긴급이므로 원래보다 높아질 수는 없음 (방어 방향만)
    new_pct = min(new_pct, original_pct)

    if new_pct >= original_pct:
        return None  # 악화 안 됐으면 무시

    reason = " + ".join(triggers)
    logger.warning(
        f"[BRAIN 긴급] {reason} → 비중 {original_pct}%→{new_pct}%"
    )

    # brain_report + brain_allocation 갱신
    report.risk = new_risk
    report.position_size_pct = new_pct
    report.position_size_reason = f"긴급({new_pct}) — {reason}"
    report.overall_verdict = f"[긴급 하향] {reason}. {report.overall_verdict}"
    save_brain_report(report)

    return {
        "triggered": True,
        "old_pct": original_pct,
        "new_pct": new_pct,
        "reason": reason,
        "risk_level": new_risk.risk_level,
    }


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        print("=" * 50)
        print("  Market Brain — 테스트 실행")
        print("=" * 50)

        report = generate_brain_report()
        save_brain_report(report)

        print()
        msg = format_brain_telegram(report)
        print(msg)

        print()
        print(f"brain_report.json 저장: {BRAIN_REPORT_PATH}")
    else:
        print("사용법: python data/market_brain.py --test")
