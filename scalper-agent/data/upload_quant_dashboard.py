# -*- coding: utf-8 -*-
"""
FLOWX 퀀트봇 대시보드 통합 업로드
===================================
5개 테이블 일괄 업로드:
  1. quant_sector_flow      — 섹터 기관/외국인 수급
  2. quant_etf_fund_flow     — ETF 투자자별 수급
  3. quant_sector_momentum   — 섹터 모멘텀 (HOT/COLD)
  4. quant_etf_recommendation — ETF 추천
  5. quant_market_brain      — BRAIN 시장판단

빈페이지 방지: 업로드 실패 시 전일 데이터 유지 (upsert 안 함)
스케줄: COO G7 Stage3 C22 (16:45 이후)
"""
import json
import logging
import os
from datetime import date
from pathlib import Path

logger = logging.getLogger("flowx_quant_dash")

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"

# ── Supabase 클라이언트 (shared 모듈) ──
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.supabase_client import get_client as _get_client


def _safe_upsert(table: str, row: dict) -> bool:
    """안전한 upsert (실패 시 전일 데이터 유지)"""
    client = _get_client()
    if not client:
        return False
    try:
        client.table(table).upsert(row, on_conflict="date").execute()
        logger.info(f"[QUANT-DASH] {table} 업로드 완료")
        return True
    except Exception as e:
        logger.error(f"[QUANT-DASH] {table} 업로드 실패 (전일 유지): {e}")
        return False


def _load_json(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _today() -> str:
    return date.today().strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════
#  1. 섹터 기관/외국인 수급
# ═══════════════════════════════════════════════════════

def upload_sector_flow() -> bool:
    """quant_sector_flow 업로드"""
    data = _load_json(STORE_DIR / "sector_flow.json")
    if not data or not data.get("sectors"):
        logger.warning("[QUANT-DASH] sector_flow.json 없음 → 스킵")
        return False

    # 주린이용 한글 필드만 추출
    sectors_clean = []
    for s in data["sectors"]:
        sectors_clean.append({
            "섹터": s.get("alias") or s.get("sector", ""),
            "기관_당일": round(s.get("inst_1d", 0), 1),
            "기관_3일": round(s.get("inst_3d", 0), 1),
            "기관_5일": round(s.get("inst_5d", 0), 1),
            "기관_연속일": s.get("inst_consecutive", 0),
            "외인_당일": round(s.get("foreign_1d", 0), 1),
            "외인_3일": round(s.get("foreign_3d", 0), 1),
            "외인_5일": round(s.get("foreign_5d", 0), 1),
            "외인_연속일": s.get("foreign_consecutive", 0),
            "판단": s.get("agreement", "중립"),
            "설명": s.get("agreement_desc", ""),
            "보정점수": round(s.get("boost_score", 0), 1),
        })

    row = {
        "date": data.get("data_date") or _today(),
        "sectors": sectors_clean,
        "top_inflow": data.get("top_inflow", []),
        "top_outflow": data.get("top_outflow", []),
        "signal": data.get("signal", ""),
        "total_sectors": data.get("total_sectors", 0),
    }
    return _safe_upsert("quant_sector_flow", row)


# ═══════════════════════════════════════════════════════
#  2. ETF 투자자별 수급
# ═══════════════════════════════════════════════════════

def upload_etf_fund_flow() -> bool:
    """quant_etf_fund_flow 업로드"""
    data = _load_json(STORE_DIR / "etf_flow.json")
    if not data or not data.get("items"):
        logger.warning("[QUANT-DASH] etf_flow.json 없음 → 스킵")
        return False

    # 주린이용 한글 필드 추출
    etfs_clean = []
    for e in data["items"]:
        etfs_clean.append({
            "종목코드": e.get("code", ""),
            "ETF명": e.get("name", ""),
            "설명": e.get("alias", ""),
            "분류": e.get("group", ""),
            "기관_당일": round(e.get("inst_1d", 0), 1),
            "기관_3일": round(e.get("inst_3d", 0), 1),
            "기관_연속일": e.get("inst_consecutive", 0),
            "외인_당일": round(e.get("foreign_1d", 0), 1),
            "외인_3일": round(e.get("foreign_3d", 0), 1),
            "외인_연속일": e.get("foreign_consecutive", 0),
            "개인_당일": round(e.get("retail_1d", 0), 1),
            "시그널": e.get("signal", ""),
            "시그널_설명": e.get("signal_desc", ""),
            "강도": e.get("strength", 0),
            "등락률": round(e.get("change_pct", 0), 2),
        })

    row = {
        "date": data.get("data_date") or _today(),
        "etfs": etfs_clean,
        "market_direction": data.get("market_direction", "NEUTRAL"),
        "market_direction_desc": data.get("market_direction_desc", ""),
        "inverse_warning": data.get("inverse_warning", False),
        "hot_sector_etfs": data.get("hot_sector_etfs", []),
        "safe_haven_signal": data.get("safe_haven_signal", "NEUTRAL"),
        "safe_haven_desc": data.get("safe_haven_desc", ""),
        "brain_defense_score": data.get("brain_defense_score", 0),
    }
    return _safe_upsert("quant_etf_fund_flow", row)


# ═══════════════════════════════════════════════════════
#  3. 섹터 모멘텀 (HOT/WARMING/COLD)
# ═══════════════════════════════════════════════════════

def upload_sector_momentum() -> bool:
    """quant_sector_momentum 업로드"""
    data = _load_json(STORE_DIR / "sector_momentum.json")
    if not data or not data.get("sectors"):
        logger.warning("[QUANT-DASH] sector_momentum.json 없음 → 스킵")
        return False

    # 주린이용 한글
    sectors_clean = []
    for s in data["sectors"]:
        sectors_clean.append({
            "섹터": s.get("sector", ""),
            "상태": s.get("phase", "NEUTRAL"),
            "순위": s.get("rank", 0),
            "당일수익률": round(s.get("avg_return_1d", 0), 2),
            "3일수익률": round(s.get("avg_return_3d", 0), 2),
            "5일수익률": round(s.get("avg_return_5d", 0), 2),
            "상승비율": round(s.get("breadth_1d", 0), 2),
            "가속도": round(s.get("acceleration", 0), 2),
            "거래량폭증": round(s.get("volume_surge", 0), 2),
            "부스트": round(s.get("boost_score", 0), 1),
            "주도주": s.get("top_movers", [])[:3],
        })

    row = {
        "date": data.get("timestamp", _today())[:10],
        "market_return_1d": round(data.get("market_return_1d", 0), 2),
        "sectors": sectors_clean,
        "hot_sectors": data.get("hot_sectors", []),
        "cold_sectors": data.get("cold_sectors", []),
        "rotation_signal": data.get("rotation_signal", ""),
    }
    return _safe_upsert("quant_sector_momentum", row)


# ═══════════════════════════════════════════════════════
#  4. ETF 추천
# ═══════════════════════════════════════════════════════

def upload_etf_recommendation() -> bool:
    """quant_etf_recommendation 업로드"""
    # etf_recommendations.json (save_etf_recommendations()가 저장)
    data = _load_json(STORE_DIR / "etf_recommendations.json")

    picks = data.get("recommendations", [])
    if not picks:
        # JSON 없으면 직접 생성 시도
        try:
            from data.etf_recommender import generate_etf_recommendations
            recs = generate_etf_recommendations()
            picks = [r.to_dict() if hasattr(r, 'to_dict') else r for r in recs]
        except Exception as e:
            logger.warning(f"[QUANT-DASH] ETF 추천 생성 실패: {e}")
            picks = []

    # 주린이용 정리
    picks_clean = []
    for p in picks:
        cat_kr = {"directional": "시장방향", "commodity": "원자재", "sector": "섹터테마"}.get(
            p.get("category", ""), p.get("category", "")
        )
        sig_kr = {"BUY": "매수", "SELL": "매도", "HOLD": "보유"}.get(
            p.get("signal", ""), p.get("signal", "")
        )
        conf_kr = {"HIGH": "높음", "MEDIUM": "보통", "LOW": "낮음"}.get(
            p.get("confidence", ""), p.get("confidence", "")
        )
        picks_clean.append({
            "종목코드": p.get("code", ""),
            "ETF명": p.get("name", ""),
            "분류": cat_kr,
            "신호": sig_kr,
            "신뢰도": conf_kr,
            "점수": round(p.get("score", 0), 1),
            "진입가": p.get("entry", 0),
            "손절가": p.get("sl", 0),
            "목표가": p.get("tp", 0),
            "손실률": round(p.get("risk_pct", 0), 2),
            "사유": p.get("reason", ""),
            "보유일": p.get("holding_days", 5),
        })

    has_cat = {p.get("category", "") for p in picks}
    row = {
        "date": _today(),
        "picks": picks_clean,
        "pick_count": len(picks_clean),
        "has_directional": "directional" in has_cat,
        "has_commodity": "commodity" in has_cat,
        "has_sector": "sector" in has_cat,
    }
    return _safe_upsert("quant_etf_recommendation", row)


# ═══════════════════════════════════════════════════════
#  5. BRAIN 시장판단
# ═══════════════════════════════════════════════════════

def upload_market_brain() -> bool:
    """quant_market_brain 업로드"""
    data = _load_json(STORE_DIR / "brain_report.json")
    if not data:
        logger.warning("[QUANT-DASH] brain_report.json 없음 → 스킵")
        return False

    macro = data.get("macro", {})
    commodity = data.get("commodity", {})
    sector = data.get("sector", {})
    flow = data.get("flow", {})
    risk = data.get("risk", {})

    # 종목 서술 주린이용 정리
    narratives_clean = []
    for n in data.get("stock_narratives", []):
        narratives_clean.append({
            "종목코드": n.get("code", ""),
            "종목명": n.get("name", ""),
            "점수": round(n.get("total_score", 0), 1),
            "등급": n.get("grade", ""),
            "분석": n.get("why_narrative", ""),
            "리스크": n.get("risk_flag", ""),
            "매크로연동": n.get("macro_alignment", ""),
        })

    row = {
        "date": data.get("date") or _today(),
        # 종합
        "overall_verdict": data.get("overall_verdict", ""),
        "position_size_pct": data.get("position_size_pct", 70),
        "position_size_reason": data.get("position_size_reason", ""),
        # Phase 1: 매크로
        "macro_direction": macro.get("direction", "NEUTRAL"),
        "macro_narrative": macro.get("narrative", ""),
        "vix": macro.get("vix", 0) or 0,
        "nasdaq_chg": macro.get("nasdaq_chg", 0) or 0,
        "usdkrw": macro.get("usdkrw", 0) or 0,
        "usdkrw_chg": macro.get("usdkrw_chg", 0) or 0,
        "gold_chg": macro.get("gold_chg", 0) or 0,
        # Phase 2: 원자재
        "commodity_relay": commodity.get("relay_stage", "NONE"),
        "commodity_narrative": commodity.get("narrative", ""),
        # Phase 3: 섹터
        "hot_sectors": sector.get("hot_sectors", []),
        "next_sectors": sector.get("next_sectors", []),
        "cooling_sectors": sector.get("cooling_sectors", []),
        "sector_narrative": sector.get("narrative", ""),
        # Phase 4: 수급
        "dominant_buyer": flow.get("dominant_buyer", ""),
        "flow_narrative": flow.get("narrative", ""),
        # Phase 5: 리스크
        "risk_level": risk.get("risk_level", "LOW"),
        "risk_score": risk.get("risk_score", 0) or 0,
        "risk_narrative": risk.get("narrative", ""),
        # Phase 6: 종목 서술
        "stock_narratives": narratives_clean,
    }
    return _safe_upsert("quant_market_brain", row)


# ═══════════════════════════════════════════════════════
#  통합 실행
# ═══════════════════════════════════════════════════════

def run_quant_dashboard_upload() -> dict:
    """5개 테이블 일괄 업로드. 실패해도 나머지 계속 진행."""
    results = {}
    uploads = [
        ("sector_flow", upload_sector_flow),
        ("etf_fund_flow", upload_etf_fund_flow),
        ("sector_momentum", upload_sector_momentum),
        ("etf_recommendation", upload_etf_recommendation),
        ("market_brain", upload_market_brain),
    ]

    for name, func in uploads:
        try:
            ok = func()
            results[name] = "OK" if ok else "SKIP"
        except Exception as e:
            logger.error(f"[QUANT-DASH] {name} 에러: {e}")
            results[name] = f"ERROR: {e}"

    ok_count = sum(1 for v in results.values() if v == "OK")
    total = len(uploads)
    logger.info(f"[QUANT-DASH] 완료: {ok_count}/{total} 성공 — {results}")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    results = run_quant_dashboard_upload()
    for k, v in results.items():
        print(f"  {k}: {v}")
