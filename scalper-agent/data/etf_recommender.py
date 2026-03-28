# -*- coding: utf-8 -*-
"""
ETF 추천 엔진 (ETF Recommender)
================================
3개 소스에서 매매 가능한 ETF 추천을 생성:
  Source 1: crisis_etf_signal → 방향성 ETF (인버스/레버리지)
  Source 2: nightwatch commodity relay → 원자재 ETF (금/원유/금속)
  Source 3: etf_relay + sector_momentum → 섹터 ETF

최대 3개 추천 (카테고리별 1개씩)
entry/SL/TP는 ATR 기반 계산
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.ETFRec")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"


@dataclass
class RecommendedETF:
    """매매 가능 ETF 추천"""
    code: str = ""
    name: str = ""
    category: str = ""          # directional / commodity / sector
    etf_type: str = ""          # index / leverage / inverse / commodity / sector

    # 시그널
    signal: str = "HOLD"        # BUY / SELL / HOLD
    confidence: str = "LOW"     # HIGH / MEDIUM / LOW
    score: float = 0.0          # 0~100

    # 진입 레벨
    entry: int = 0
    sl: int = 0
    tp: int = 0
    risk_pct: float = 0.0       # SL까지 손실률(%)

    # 메타
    reason: str = ""
    holding_days: int = 5       # 최대 보유일 (레버리지/인버스=1~3, 원자재=10, 섹터=5)
    underlying_ticker: str = "" # 기초자산 심볼 (GC=F, CL=F 등)
    underlying_change: float = 0.0  # 기초자산 변동률(%)

    def to_dict(self):
        return asdict(self)


# ═════════════════════════════════════════════════════════════
# Source 1: 방향성 ETF (인버스/레버리지) — crisis_etf_signal
# ═════════════════════════════════════════════════════════════

def _source_directional() -> Optional[RecommendedETF]:
    """위기 ETF 시그널 기반 방향성 ETF 추천"""
    try:
        from strategies.crisis_etf_signal import load_signal
        sig = load_signal()
        if sig is None:
            return None

        # 당일 시그널만 유효
        today_str = datetime.now().strftime("%Y-%m-%d")
        if not sig.timestamp.startswith(today_str):
            logger.info("[ETF-방향성] 시그널 날짜 불일치 → 스킵")
            return None

        if sig.signal == "HOLD":
            return None

        etf_info = sig.recommended_etf
        if not etf_info:
            return None

        code = etf_info.get("code", "")
        name = etf_info.get("name", "")
        mult = etf_info.get("mult", 1)

        # ATR 기반 SL/TP 계산
        entry, sl, tp = _calc_entry_sl_tp(code, mult)

        # 보유일: 레버리지/인버스 = 최대 3일
        hold = 3 if abs(mult) >= 2 else 5

        # 시그널 매핑
        if "INVERSE" in sig.signal:
            signal_type = "BUY"
            etf_type = "inverse"
        elif "LEVERAGE" in sig.signal:
            signal_type = "BUY"
            etf_type = "leverage"
        elif "EXIT" in sig.signal:
            signal_type = "SELL"
            etf_type = "inverse" if "INVERSE" in sig.signal else "leverage"
        else:
            signal_type = "HOLD"
            etf_type = "index"

        # 스코어 계산
        score_map = {"HIGH": 85, "MEDIUM": 65, "LOW": 45}
        score = score_map.get(sig.confidence, 45)

        risk_pct = abs(entry - sl) / entry * 100 if entry > 0 else 0

        rec = RecommendedETF(
            code=code, name=name,
            category="directional", etf_type=etf_type,
            signal=signal_type, confidence=sig.confidence,
            score=score,
            entry=entry, sl=sl, tp=tp,
            risk_pct=round(risk_pct, 2),
            reason=sig.reason,
            holding_days=hold,
            underlying_ticker="KOSPI200",
            underlying_change=sig.kospi_change_1d,
        )
        logger.info(f"[ETF-방향성] {name}({code}) {signal_type} {sig.confidence} | {sig.reason}")
        return rec

    except Exception as e:
        logger.warning(f"[ETF-방향성] 실패: {e}")
        return None


# ═════════════════════════════════════════════════════════════
# Source 2: 원자재 ETF — nightwatch commodity relay
# ═════════════════════════════════════════════════════════════

def _source_commodity() -> Optional[RecommendedETF]:
    """원자재 릴레이 시그널 기반 원자재 ETF 추천"""
    try:
        from data.etf_universe import get_commodity_etfs_for_signal, get_etf_snapshot
        from data.nightwatch import get_commodity_trend

        commodity = get_commodity_trend(5)
        if not commodity:
            return None

        active_sigs = commodity.get("active_signals", [])
        if not active_sigs:
            return None

        # 우선순위: gold > oil > copper/silver
        best = None
        best_score = 0

        for sig_key in ("gold_up", "oil_up", "silver_up", "copper_up"):
            if sig_key not in active_sigs:
                continue

            etfs = get_commodity_etfs_for_signal(sig_key)
            if not etfs:
                continue

            # 첫 번째 ETF 선택
            etf = etfs[0]
            code = etf["code"]

            # 기초자산 변동률
            asset_key = sig_key.replace("_up", "")
            asset_data = commodity.get(asset_key, {})
            change_pct = asset_data.get("change_pct", 0)
            direction = asset_data.get("direction", "FLAT")

            if direction != "UP":
                continue

            # 스코어: 변동률 + 옵션 심리
            score = 50 + min(change_pct * 5, 30)  # 기본 50 + 변동률 보너스

            # 옵션 심리 보너스 (FEAR → 금 선호)
            try:
                from data.options_signal import load_latest_signal
                opt = load_latest_signal()
                if opt and opt.pc_level in ("EXTREME_FEAR", "FEAR") and asset_key == "gold":
                    score += 15  # 공포시 금 ETF 가산
            except Exception:
                pass

            if score > best_score:
                snap = get_etf_snapshot(code)
                if snap and snap["close"] > 0:
                    entry, sl, tp = _calc_entry_sl_tp(code, 1)
                    risk_pct = abs(entry - sl) / entry * 100 if entry > 0 else 0

                    confidence = "HIGH" if score >= 75 else ("MEDIUM" if score >= 60 else "LOW")
                    underlying = etf.get("underlying", "")

                    best = RecommendedETF(
                        code=code, name=etf["name"],
                        category="commodity", etf_type="commodity",
                        signal="BUY", confidence=confidence,
                        score=round(score, 1),
                        entry=entry, sl=sl, tp=tp,
                        risk_pct=round(risk_pct, 2),
                        reason=f"{asset_key}릴레이 {change_pct:+.1f}% {direction}",
                        holding_days=10,
                        underlying_ticker=underlying,
                        underlying_change=change_pct,
                    )
                    best_score = score

        if best:
            logger.info(f"[ETF-원자재] {best.name}({best.code}) {best.confidence} | {best.reason}")
        return best

    except Exception as e:
        logger.warning(f"[ETF-원자재] 실패: {e}")
        return None


# ═════════════════════════════════════════════════════════════
# Source 3: 섹터 ETF — etf_relay + sector_momentum
# ═════════════════════════════════════════════════════════════

def _source_sector() -> Optional[RecommendedETF]:
    """섹터 모멘텀 + ETF 릴레이 기반 섹터 ETF 추천"""
    try:
        from data.etf_universe import get_etf_info, get_etf_snapshot

        best = None
        best_score = 0

        # 1) 섹터 모멘텀 데이터
        sector_hot = {}
        try:
            from data.sector_momentum import load_sector_report
            sr = load_sector_report()
            if sr:
                for s in sr.sectors:
                    if s.phase in ("HOT_EARLY", "HOT_MID"):
                        sector_hot[s.sector_key] = {
                            "phase": s.phase,
                            "score": s.momentum_score,
                        }
        except Exception:
            pass

        # 2) ETF 릴레이 데이터
        etf_leading = {}
        try:
            from data.etf_relay import scan_all_etfs
            relay_results = scan_all_etfs()
            for es in relay_results:
                if es.status in ("LEADING", "DIVERGING"):
                    etf_leading[es.theme_id] = {
                        "status": es.status,
                        "etf_5d": es.etf_5d,
                        "divergence": es.divergence,
                    }
        except Exception:
            pass

        # 3) 섹터 ETF 후보 스코어링
        from data.etf_universe import ETF_UNIVERSE
        sector_etfs = ETF_UNIVERSE.get("sector", {})

        # sector_key → theme_id 매핑
        _sector_to_theme = {
            "space_defense": "defense",
            "semiconductor": "semiconductor",
            "battery_ev": "battery",
            "bio_health": "bio",
            "construction": "construction",
            "shipbuilding": "shipbuilding",
            "auto": "auto",
        }

        for code, info in sector_etfs.items():
            sector_key = info.get("sector_key", "")
            theme_id = _sector_to_theme.get(sector_key, "")

            score = 0
            reasons = []

            # 섹터 모멘텀 점수
            if sector_key in sector_hot:
                hot = sector_hot[sector_key]
                if hot["phase"] == "HOT_EARLY":
                    score += 40
                    reasons.append(f"섹터HOT_EARLY({hot['score']:.0f})")
                elif hot["phase"] == "HOT_MID":
                    score += 25
                    reasons.append(f"섹터HOT_MID({hot['score']:.0f})")

            # ETF 릴레이 점수
            if theme_id in etf_leading:
                lead = etf_leading[theme_id]
                if lead["status"] == "LEADING":
                    score += 30
                    reasons.append(f"ETF주도(5D:{lead['etf_5d']:+.1f}%)")
                elif lead["status"] == "DIVERGING":
                    score += 15
                    reasons.append(f"ETF괴리(5D:{lead['etf_5d']:+.1f}%)")

            # 최소 점수 미달
            if score < 30:
                continue

            snap = get_etf_snapshot(code)
            if not snap or snap["close"] <= 0:
                continue

            # 최근 수익률 보너스
            if snap["change_pct"] > 1.0:
                score += 10
                reasons.append(f"등락{snap['change_pct']:+.1f}%")

            if score > best_score:
                entry, sl, tp = _calc_entry_sl_tp(code, 1)
                risk_pct = abs(entry - sl) / entry * 100 if entry > 0 else 0

                confidence = "HIGH" if score >= 70 else ("MEDIUM" if score >= 50 else "LOW")

                best = RecommendedETF(
                    code=code, name=info["name"],
                    category="sector", etf_type="sector",
                    signal="BUY", confidence=confidence,
                    score=round(score, 1),
                    entry=entry, sl=sl, tp=tp,
                    risk_pct=round(risk_pct, 2),
                    reason=" + ".join(reasons),
                    holding_days=5,
                    underlying_ticker=sector_key,
                    underlying_change=snap.get("change_pct", 0),
                )
                best_score = score

        if best:
            logger.info(f"[ETF-섹터] {best.name}({best.code}) {best.confidence} | {best.reason}")
        return best

    except Exception as e:
        logger.warning(f"[ETF-섹터] 실패: {e}")
        return None


# ═════════════════════════════════════════════════════════════
# Entry/SL/TP 계산 (ATR 기반)
# ═════════════════════════════════════════════════════════════

def _calc_entry_sl_tp(code: str, mult: int = 1) -> tuple[int, int, int]:
    """ATR 기반 entry/SL/TP 계산

    Args:
        code: ETF 코드
        mult: 배율 (양수=롱, 음수=인버스)

    Returns:
        (entry, sl, tp)
    """
    from data.etf_universe import get_etf_snapshot, get_etf_atr

    snap = get_etf_snapshot(code)
    if not snap or snap["close"] <= 0:
        return 0, 0, 0

    close = snap["close"]
    atr = get_etf_atr(code, period=14)

    if not atr or atr <= 0:
        # ATR 없으면 종가 기반 3% 기본값
        atr = close * 0.03

    entry = close

    # 인버스: 방향 반대
    if mult < 0:
        # 인버스 ETF는 시장 하락 시 상승 → SL은 위, TP는 아래가 아님
        # 인버스 ETF 자체는 "롱" 포지션이므로 일반 롱과 동일
        sl = int(entry - atr * 1.5)
        tp = int(entry + atr * 2.5)
    else:
        sl = int(entry - atr * 1.5)
        tp = int(entry + atr * 2.5)

    # 레버리지: ATR 축소 (시간감쇠 고려)
    if abs(mult) >= 2:
        sl = int(entry - atr * 1.0)
        tp = int(entry + atr * 2.0)

    return entry, max(sl, 1), tp


# ═════════════════════════════════════════════════════════════
# 메인 함수
# ═════════════════════════════════════════════════════════════

def generate_etf_recommendations() -> list[RecommendedETF]:
    """3개 소스에서 ETF 추천 생성 (최대 3개, 카테고리별 1개)

    Returns:
        list[RecommendedETF] — BUY 시그널만 반환
    """
    logger.info("[ETF-REC] ETF 추천 생성 시작...")
    recommendations = []

    # Source 1: 방향성 ETF
    directional = _source_directional()
    if directional and directional.signal == "BUY":
        recommendations.append(directional)

    # Source 2: 원자재 ETF
    commodity = _source_commodity()
    if commodity and commodity.signal == "BUY":
        recommendations.append(commodity)

    # Source 3: 섹터 ETF
    sector = _source_sector()
    if sector and sector.signal == "BUY":
        recommendations.append(sector)

    # ── ETF 수급 부스트 (TIER2 Phase 2) ──
    try:
        from data.etf_fund_flow import get_etf_flow_boost
        for r in recommendations:
            boost = get_etf_flow_boost(r.code)
            if abs(boost) >= 1.0:
                r.score += boost
                r.reason += f" | 수급{boost:+.0f}"
                logger.info(f"  [ETF수급부스트] {r.name}: {boost:+.1f}점 → {r.score:.0f}")
    except Exception as e:
        logger.warning(f"[ETF수급부스트] 실패 (무시): {e}")

    # 점수 내림차순 정렬
    recommendations.sort(key=lambda x: x.score, reverse=True)

    logger.info(f"[ETF-REC] 추천 완료: {len(recommendations)}종목")
    for r in recommendations:
        logger.info(
            f"  [{r.category}] {r.name}({r.code}) "
            f"{r.signal} {r.confidence} score={r.score:.0f} "
            f"entry={r.entry:,} SL={r.sl:,} TP={r.tp:,}"
        )

    return recommendations


def format_etf_recommendations(recs: list[RecommendedETF]) -> str:
    """텔레그램용 ETF 추천 포맷"""
    if not recs:
        return ""

    buy_recs = [r for r in recs if r.signal == "BUY"]
    if not buy_recs:
        return ""

    cat_emoji = {
        "directional": "📊",
        "commodity": "⛏️",
        "sector": "🏭",
    }
    cat_label = {
        "directional": "방향성",
        "commodity": "원자재",
        "sector": "섹터",
    }
    conf_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}

    lines = [
        f"━━ ETF 추천 ({len(buy_recs)}/{len(recs)}) ━━",
        "",
    ]

    for r in buy_recs:
        ce = cat_emoji.get(r.category, "📈")
        cl = cat_label.get(r.category, r.category)
        cfe = conf_emoji.get(r.confidence, "⚪")
        lines.append(
            f"{ce} [{cl}] {r.signal} ({r.confidence}) {cfe}"
        )
        lines.append(
            f"  {r.name} ({r.code})"
        )
        lines.append(
            f"  진입:{r.entry:,}  SL:{r.sl:,}  TP:{r.tp:,} | 최대 {r.holding_days}일"
        )
        lines.append(
            f"  사유: {r.reason}"
        )
        lines.append("")

    return "\n".join(lines)


def save_etf_recommendations(recs: list[RecommendedETF]):
    """ETF 추천 저장"""
    path = DATA_DIR / "etf_recommendations.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "timestamp": datetime.now().isoformat(),
        "count": len(recs),
        "recommendations": [r.to_dict() for r in recs],
    }
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
    logger.info(f"[ETF-REC] 저장 완료: {path} ({len(recs)}종목)")


def load_etf_recommendations() -> list[RecommendedETF]:
    """저장된 ETF 추천 로드"""
    path = DATA_DIR / "etf_recommendations.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        recs = []
        for d in data.get("recommendations", []):
            recs.append(RecommendedETF(**{
                k: v for k, v in d.items()
                if k in RecommendedETF.__dataclass_fields__
            }))
        return recs
    except Exception as e:
        logger.warning(f"[ETF-REC] 로드 실패: {e}")
        return []


# ═════════════════════════════════════════════════════════════
# CLI 테스트
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("ETF 추천 엔진 실행 중...")
    recs = generate_etf_recommendations()
    save_etf_recommendations(recs)

    if recs:
        print("\n" + format_etf_recommendations(recs))
    else:
        print("\n추천 ETF 없음 (모든 소스 HOLD)")
