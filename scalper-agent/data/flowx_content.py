# -*- coding: utf-8 -*-
"""
FLOWX VIP 8-Panel Content Generator
=====================================
FLOWX 구독자(월 5만원)를 위한 프리미엄 8패널 콘텐츠 자동 생성.

Panel 1: 추천종목 + 근거 태그    (recommendation.json → 소스 태그 + 등급)
Panel 2: ALL 수급               (외국인/기관/개인 상위 종목 + 숫자)
Panel 3: 추천 근거               (왜 사야하는가 — 5단계 분석 요약)
Panel 4: 국적별 수급             (미국/유럽/싱가포르 국적 상세)
Panel 5: 매집 감지 레이더         (trading_value_scanner 사전 포착)
Panel 6: 릴레이 체인             (섹터 로테이션 다음 종목 예측)
Panel 7: 적중률 대시보드          (패턴별 적중률 투명 공개)
Panel 8: 변곡점 알림             (position_guardian EXIT 경보)

Usage:
  python data/flowx_content.py --test       # 콘솔 출력
  python data/flowx_content.py --upload     # Supabase 업로드
  python data/flowx_content.py --telegram   # 텔레그램 포맷 출력
"""

import json
import logging
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("flowx_content")

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = BASE_DIR / "data_store"
ENV_PATH = BASE_DIR.parent / ".env"

# ── Supabase 클라이언트 (lazy) ──
_supabase = None


def _get_client():
    global _supabase
    if _supabase is not None:
        return _supabase
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정")
        return None
    from supabase import create_client
    _supabase = create_client(url, key)
    return _supabase


def _load_json(filename: str) -> dict:
    """data_store 내 JSON 로드"""
    path = DATA_STORE / filename
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _atomic_save(data: dict, filename: str):
    """atomic JSON 저장"""
    path = DATA_STORE / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ═══════════════════════════════════════════════════
#  Panel 1: 추천종목 + 근거 태그
# ═══════════════════════════════════════════════════

def _panel_recommended_stocks(rec: dict) -> list:
    """추천 종목 + source 태그 + 등급 + 진입/손절/목표가"""
    from data.upload_short import _score_to_grade, _determine_signal_type

    stocks = rec.get("stocks", [])
    result = []
    for s in stocks[:8]:
        grade = _score_to_grade(
            s.get("total_score", 0),
            s.get("confidence", ""),
            s.get("nat_power_grade", ""),
        )
        tv_ratio = s.get("tv_ratio", 1.0) or 1.0
        inst = "기OK" in s.get("nationality_detail", "")
        signal = _determine_signal_type(
            grade, inst, tv_ratio, s.get("tv_pattern", "NORMAL"),
        )

        # 소스 태그 — 어떤 분석에서 추천되었는지
        source_tags = []
        for src in s.get("sources", []):
            tag = src.replace("_", " ").strip()
            if tag and tag not in source_tags:
                source_tags.append(tag)

        result.append({
            "code": s["code"],
            "name": s["name"],
            "score": round(s.get("total_score", 0), 1),
            "grade": grade,
            "signal": signal,
            "confidence": s.get("confidence", ""),
            "regime": s.get("regime", "NORMAL"),
            "entry": s.get("entry", 0),
            "sl": s.get("sl", 0),
            "tp": s.get("tp", 0),
            "source_tags": source_tags[:5],
            "tv_pattern": s.get("tv_pattern", "NORMAL"),
            "tv_ratio": round(tv_ratio, 2),
        })
    return result


# ═══════════════════════════════════════════════════
#  Panel 2: ALL 수급 (외국인/기관/개인)
# ═══════════════════════════════════════════════════

def _panel_all_supply(rec: dict) -> dict:
    """추천종목의 외국인/기관/개인 수급 상위 정리"""
    stocks = rec.get("stocks", [])

    # 종목별 nationality_detail 파싱
    supply_list = []
    for s in stocks[:8]:
        nd = s.get("nationality_detail", "")
        nat_grade = s.get("nat_power_grade", "NEUTRAL")

        # 외국인/기관 분류 파싱
        foreign_net = 0
        inst_net = 0
        individual_net = 0

        # nationality_detail 예: "외+1200|기OK|개-500|싱+800"
        for part in nd.split("|"):
            part = part.strip()
            if part.startswith("외") and ("+" in part or "-" in part):
                try:
                    foreign_net = int(part[1:].replace(",", "").replace("+", ""))
                except (ValueError, IndexError):
                    pass
            elif part.startswith("기") and ("+" in part or "-" in part):
                try:
                    inst_net = int(part[1:].replace(",", "").replace("+", ""))
                except (ValueError, IndexError):
                    pass
            elif part.startswith("개") and ("+" in part or "-" in part):
                try:
                    individual_net = int(part[1:].replace(",", "").replace("+", ""))
                except (ValueError, IndexError):
                    pass

        supply_list.append({
            "code": s["code"],
            "name": s["name"],
            "foreign_net": foreign_net,
            "inst_net": inst_net,
            "individual_net": individual_net,
            "nat_grade": nat_grade,
            "detail": nd,
        })

    # 외국인 매수 상위 정렬
    top_foreign = sorted(supply_list, key=lambda x: x["foreign_net"], reverse=True)
    top_inst = sorted(supply_list, key=lambda x: x["inst_net"], reverse=True)

    return {
        "stocks": supply_list,
        "top_foreign_buy": [s for s in top_foreign if s["foreign_net"] > 0][:5],
        "top_inst_buy": [s for s in top_inst if s["inst_net"] > 0][:5],
    }


# ═══════════════════════════════════════════════════
#  Panel 3: 추천 근거 (왜 사야하는가)
# ═══════════════════════════════════════════════════

def _panel_buy_reasons(rec: dict) -> list:
    """각 추천종목에 대해 '왜 사야하는가' 5단계 분석 요약"""
    stocks = rec.get("stocks", [])
    reasons = []

    for s in stocks[:8]:
        analysis = []

        # 1. 릴레이 교차검증
        relay = s.get("relay_score", 0)
        if relay > 0:
            analysis.append(f"릴레이 교차 {relay:.0f}점")

        # 2. 사전감지 (premove)
        premove = s.get("premove_score", 0)
        if premove > 0:
            analysis.append(f"사전감지 {premove:.0f}점")

        # 3. 기술적 분석
        tech = s.get("tech_detail", "")
        if tech:
            analysis.append(f"기술: {tech[:60]}")

        # 4. 거래대금 패턴
        tv_pattern = s.get("tv_pattern", "NORMAL")
        tv_ratio = s.get("tv_ratio", 1.0)
        if tv_pattern != "NORMAL":
            pattern_name = {
                "QUIET_ACCUMULATION": "조용한 매집",
                "EXPLOSION": "거래대금 폭발",
                "GRADUAL_BUILDUP": "점진적 매집",
            }.get(tv_pattern, tv_pattern)
            analysis.append(f"{pattern_name} (TV {tv_ratio:.1f}x)")

        # 5. 수급 파워
        nat_grade = s.get("nat_power_grade", "")
        nat_detail = s.get("nat_power_detail", "")
        if nat_grade in ("POWER_BUY", "BUY"):
            analysis.append(f"수급 {nat_grade}: {nat_detail[:40]}")

        # 6. 뉴스
        news = s.get("news_detail", "")
        if news and news != "뉴스 없음":
            analysis.append(f"뉴스: {news[:60]}")

        # 7. MOMENTUM 레짐
        regime = s.get("regime", "NORMAL")
        if regime == "MOMENTUM":
            analysis.append(f"모멘텀 레짐 (점수 {s.get('regime_score', 0):.2f})")

        # 8. 교차 등장
        cross = s.get("cross_count", 0)
        if cross >= 3:
            analysis.append(f"교차 {cross}회 등장")

        # 위험요소
        risks = []
        if s.get("news_penalty", 0) > 0:
            risks.append("뉴스 부정적")
        if s.get("obv_penalty", 0) > 0:
            risks.append("OBV 하락")
        if s.get("relative_penalty", 0) > 0:
            risks.append("시장대비 약세")

        reasons.append({
            "code": s["code"],
            "name": s["name"],
            "score": round(s.get("total_score", 0), 1),
            "confidence": s.get("confidence", ""),
            "analysis": analysis,
            "risks": risks,
            "entry": s.get("entry", 0),
            "sl": s.get("sl", 0),
            "tp": s.get("tp", 0),
            "upside_pct": round(
                (s.get("tp", 0) - s.get("entry", 0)) / max(s.get("entry", 1), 1) * 100, 1
            ) if s.get("tp") and s.get("entry") else 0,
        })

    return reasons


# ═══════════════════════════════════════════════════
#  Panel 4: 국적별 수급 (미국/유럽/싱가포르)
# ═══════════════════════════════════════════════════

def _panel_nationality_detail(rec: dict) -> list:
    """추천종목의 국적별 외국인 수급 상세"""
    stocks = rec.get("stocks", [])
    result = []

    for s in stocks[:8]:
        nd = s.get("nationality_detail", "")
        nat_detail = s.get("nat_power_detail", "")

        # nationality_detail 파싱 → 국가별 데이터
        countries = {}
        for part in nd.split("|"):
            part = part.strip()
            if not part:
                continue
            # "싱+800", "미+1200", "영+500" 등
            for prefix, country in [
                ("미", "미국"), ("영", "영국"), ("싱", "싱가포르"),
                ("케", "케이맨"), ("노", "노르웨이"), ("중", "중국"),
                ("홍", "홍콩"), ("일", "일본"), ("대", "대만"),
                ("스", "스위스"), ("호", "호주"), ("프", "프랑스"),
            ]:
                if part.startswith(prefix) and len(part) > 1:
                    try:
                        val = int(part[1:].replace(",", "").replace("+", ""))
                        countries[country] = val
                    except (ValueError, IndexError):
                        pass
                    break

        result.append({
            "code": s["code"],
            "name": s["name"],
            "countries": countries,
            "nat_grade": s.get("nat_power_grade", "NEUTRAL"),
            "flow_signal": s.get("flow_signal", ""),
            "flow_detail": s.get("flow_detail", "")[:80],
        })

    return result


# ═══════════════════════════════════════════════════
#  Panel 5: 매집 감지 레이더
# ═══════════════════════════════════════════════════

def _panel_accumulation_radar() -> list:
    """trading_value_scanner → 매집 패턴 감지 종목"""
    tv_data = _load_json("tv_scanner.json")
    signals = tv_data.get("signals", [])

    if not signals:
        return []

    # 패턴별 분류 + score 상위
    result = []
    for sig in sorted(signals, key=lambda x: x.get("score", 0), reverse=True)[:10]:
        pattern = sig.get("pattern", "NORMAL")
        if pattern == "NORMAL":
            continue

        pattern_label = {
            "QUIET_ACCUMULATION": "조용한 매집",
            "EXPLOSION": "거래대금 폭발",
            "GRADUAL_BUILDUP": "점진적 매집",
        }.get(pattern, pattern)

        result.append({
            "code": sig.get("code", ""),
            "name": sig.get("name", ""),
            "pattern": pattern,
            "pattern_label": pattern_label,
            "score": round(sig.get("score", 0), 1),
            "tv_ratio": round(sig.get("tv_ratio", 0), 2),
            "trading_value": round(sig.get("trading_value", 0), 1),
            "change_pct": round(sig.get("change_pct", 0), 2),
            "detail": sig.get("detail", "")[:80],
            "frgn_joined": sig.get("frgn_joined", False),
            "frgn_amount": round(sig.get("frgn_amount", 0), 1),
        })

    return result


# ═══════════════════════════════════════════════════
#  Panel 6: 릴레이 체인 (섹터 로테이션)
# ═══════════════════════════════════════════════════

def _panel_relay_chain(rec: dict) -> dict:
    """릴레이 요약 + 섹터 로테이션 + TV 클러스터 + 원자재"""
    relay_summary = rec.get("relay_summary", "")
    rotation_signal = rec.get("rotation_signal", "")
    rotation_detail = rec.get("rotation_detail", "")
    tv_cluster = rec.get("tv_cluster_info", "")
    commodity = rec.get("commodity_info", "")

    # HOT 섹터 추출
    hot_sectors = []
    for part in relay_summary.split("|"):
        part = part.strip()
        if "HOT" in part:
            sector = part.replace("HOT:", "").strip()
            if sector:
                hot_sectors.append(sector)

    # 로테이션 신호 파싱
    rotation_sectors = []
    if rotation_detail:
        if isinstance(rotation_detail, list):
            rotation_sectors = [str(x) for x in rotation_detail]
        elif isinstance(rotation_detail, str):
            for part in rotation_detail.split("|"):
                part = part.strip()
                if part:
                    rotation_sectors.append(part)

    # TV 클러스터 요약 (raw list → 간결 텍스트)
    cluster_summary = ""
    if isinstance(tv_cluster, list):
        top_clusters = sorted(tv_cluster, key=lambda x: x.get("count", 0), reverse=True)[:5]
        cluster_parts = [f"{c.get('name', '')}({c.get('count', 0)}종목)" for c in top_clusters]
        cluster_summary = ", ".join(cluster_parts)
    elif isinstance(tv_cluster, str):
        cluster_summary = tv_cluster[:100]

    return {
        "relay_summary": relay_summary,
        "hot_sectors": hot_sectors,
        "rotation_signal": rotation_signal,
        "rotation_sectors": rotation_sectors[:5],
        "tv_cluster": cluster_summary,
        "commodity_info": commodity if isinstance(commodity, str) else "",
    }


# ═══════════════════════════════════════════════════
#  Panel 7: 적중률 대시보드
# ═══════════════════════════════════════════════════

def _panel_accuracy_dashboard() -> dict:
    """patterns.json → 소스별 적중률 투명 공개"""
    patterns = _load_json("learning/journal/patterns.json")
    if not patterns:
        return {"patterns": [], "overall": {}}

    pattern_list = []
    total_trades = 0
    total_hits = 0
    total_pnl = 0

    for key, p in patterns.items():
        total = p.get("total", 0)
        if total == 0:
            continue

        hits = p.get("hits", 0)
        pnl_sum = p.get("pnl_sum", 0)
        hit_rate = round(hits / total * 100, 1) if total > 0 else 0
        avg_pnl = round(pnl_sum / total, 2) if total > 0 else 0

        total_trades += total
        total_hits += hits
        total_pnl += pnl_sum

        pattern_list.append({
            "source": key,
            "total": total,
            "hits": hits,
            "hit_rate": hit_rate,
            "avg_pnl": avg_pnl,
            "pnl_sum": round(pnl_sum, 2),
        })

    # 적중률 높은 순 정렬
    pattern_list.sort(key=lambda x: x["hit_rate"], reverse=True)

    overall_rate = round(total_hits / total_trades * 100, 1) if total_trades > 0 else 0
    overall_pnl = round(total_pnl / total_trades, 2) if total_trades > 0 else 0

    return {
        "patterns": pattern_list,
        "overall": {
            "total_trades": total_trades,
            "total_hits": total_hits,
            "overall_hit_rate": overall_rate,
            "overall_avg_pnl": overall_pnl,
        },
    }


# ═══════════════════════════════════════════════════
#  Panel 8: 변곡점 알림 (Position Guardian)
# ═══════════════════════════════════════════════════

def _panel_inflection_alerts() -> list:
    """guardian_latest.json → 보유종목 변곡점 경보"""
    guardian = _load_json("learning/guardian_latest.json")
    verdicts = guardian.get("verdicts", [])

    if not verdicts:
        return []

    alerts = []
    for v in verdicts:
        action = v.get("action", "HOLD")

        # 위험 시그널 요약
        signal_summary = []
        for sig in v.get("signals", []):
            severity = sig.get("severity", "SAFE")
            if severity in ("WARNING", "DANGER", "CRITICAL"):
                signal_summary.append({
                    "name": sig.get("name", ""),
                    "severity": severity,
                    "score": round(sig.get("score", 0), 1),
                    "detail": sig.get("detail", "")[:60],
                })

        alerts.append({
            "code": v.get("code", ""),
            "name": v.get("name", ""),
            "action": action,
            "risk_score": round(v.get("risk_score", 0), 1),
            "key_reason": v.get("key_reason", "")[:80],
            "pnl_pct": round(v.get("pnl_pct", 0), 2),
            "current_price": v.get("current_price", 0),
            "signals": signal_summary,
        })

    # EXIT → REDUCE → TAKE_PROFIT → HOLD 순
    action_order = {"EXIT": 0, "REDUCE": 1, "TAKE_PROFIT": 2, "HOLD": 3}
    alerts.sort(key=lambda x: action_order.get(x["action"], 9))
    return alerts


# ═══════════════════════════════════════════════════
#  통합 생성
# ═══════════════════════════════════════════════════

def generate_vip_content() -> dict:
    """8패널 VIP 콘텐츠 통합 생성

    Returns:
        {
            "date": "YYYY-MM-DD",
            "generated_at": "...",
            "panel_1_stocks": [...],
            "panel_2_supply": {...},
            "panel_3_reasons": [...],
            "panel_4_nationality": [...],
            "panel_5_accumulation": [...],
            "panel_6_relay": {...},
            "panel_7_accuracy": {...},
            "panel_8_alerts": [...],
        }
    """
    rec = _load_json("recommendation.json")

    content = {
        "date": str(date.today()),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "panel_1_stocks": _panel_recommended_stocks(rec),
        "panel_2_supply": _panel_all_supply(rec),
        "panel_3_reasons": _panel_buy_reasons(rec),
        "panel_4_nationality": _panel_nationality_detail(rec),
        "panel_5_accumulation": _panel_accumulation_radar(),
        "panel_6_relay": _panel_relay_chain(rec),
        "panel_7_accuracy": _panel_accuracy_dashboard(),
        "panel_8_alerts": _panel_inflection_alerts(),
    }

    # 요약 통계
    content["summary"] = {
        "total_stocks": len(content["panel_1_stocks"]),
        "strong_pick": sum(1 for s in content["panel_1_stocks"] if s["signal"] == "STRONG_PICK"),
        "accumulation_detected": len(content["panel_5_accumulation"]),
        "exit_alerts": sum(1 for a in content["panel_8_alerts"] if a["action"] == "EXIT"),
        "reduce_alerts": sum(1 for a in content["panel_8_alerts"] if a["action"] == "REDUCE"),
        "overall_hit_rate": content["panel_7_accuracy"].get("overall", {}).get("overall_hit_rate", 0),
    }

    # 로컬 저장
    _atomic_save(content, "flowx_vip_content.json")
    logger.info(
        f"[FLOWX VIP] 8패널 생성 완료: "
        f"종목={content['summary']['total_stocks']} | "
        f"매집={content['summary']['accumulation_detected']} | "
        f"EXIT={content['summary']['exit_alerts']} | "
        f"적중률={content['summary']['overall_hit_rate']}%"
    )
    return content


# ═══════════════════════════════════════════════════
#  Supabase 업로드
# ═══════════════════════════════════════════════════

def upload_vip_content(content: dict) -> bool:
    """vip_content 테이블에 upsert
    NOTE: Supabase에 vip_content 테이블 미생성 시 로컬 저장만 수행
    """
    client = _get_client()
    if not client:
        return False

    try:
        row = {
            "date": content["date"],
            "generated_at": content["generated_at"],
            "panel_1_stocks": json.dumps(content["panel_1_stocks"], ensure_ascii=False),
            "panel_2_supply": json.dumps(content["panel_2_supply"], ensure_ascii=False),
            "panel_3_reasons": json.dumps(content["panel_3_reasons"], ensure_ascii=False),
            "panel_4_nationality": json.dumps(content["panel_4_nationality"], ensure_ascii=False),
            "panel_5_accumulation": json.dumps(content["panel_5_accumulation"], ensure_ascii=False),
            "panel_6_relay": json.dumps(content["panel_6_relay"], ensure_ascii=False),
            "panel_7_accuracy": json.dumps(content["panel_7_accuracy"], ensure_ascii=False),
            "panel_8_alerts": json.dumps(content["panel_8_alerts"], ensure_ascii=False),
            "summary": json.dumps(content["summary"], ensure_ascii=False),
        }

        client.table("vip_content").upsert(
            [row], on_conflict="date"
        ).execute()

        logger.info(f"[FLOWX VIP] Supabase 업로드 완료: {content['date']}")
        return True
    except Exception as e:
        err_str = str(e)
        if "PGRST205" in err_str or "schema cache" in err_str:
            logger.warning(f"[FLOWX VIP] vip_content 테이블 미존재 — 로컬 저장만 완료")
        else:
            logger.error(f"[FLOWX VIP] 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════════════════

def format_telegram_vip(content: dict) -> str:
    """8패널 VIP 콘텐츠 텔레그램 포맷"""
    lines = []
    lines.append(f"<b>FLOWX VIP 8-Panel {content['date']}</b>")
    lines.append("")

    # ── Panel 1: 추천종목 ──
    p1 = content.get("panel_1_stocks", [])
    if p1:
        lines.append("<b>1. 추천종목</b>")
        for s in p1:
            tags = " ".join(f"#{t}" for t in s["source_tags"][:3])
            lines.append(
                f"  {s['name']} [{s['grade']}] {s['score']}점 "
                f"{s['signal']}"
            )
            if tags:
                lines.append(f"    {tags}")
            lines.append(
                f"    진입 {s['entry']:,} → 목표 {s['tp']:,} "
                f"(손절 {s['sl']:,})"
            )
        lines.append("")

    # ── Panel 2: ALL 수급 ──
    p2 = content.get("panel_2_supply", {})
    top_f = p2.get("top_foreign_buy", [])
    top_i = p2.get("top_inst_buy", [])
    if top_f or top_i:
        lines.append("<b>2. ALL 수급</b>")
        if top_f:
            lines.append("  외국인 매수:")
            for s in top_f[:3]:
                lines.append(f"    {s['name']} +{s['foreign_net']:,}")
        if top_i:
            lines.append("  기관 매수:")
            for s in top_i[:3]:
                lines.append(f"    {s['name']} +{s['inst_net']:,}")
        lines.append("")

    # ── Panel 3: 추천 근거 ──
    p3 = content.get("panel_3_reasons", [])
    if p3:
        lines.append("<b>3. 추천 근거</b>")
        for r in p3[:5]:
            lines.append(f"  {r['name']} ({r['score']}점 {r['confidence']})")
            for a in r["analysis"][:3]:
                lines.append(f"    - {a}")
            if r["risks"]:
                lines.append(f"    ⚠ {', '.join(r['risks'])}")
            if r.get("upside_pct", 0) > 0:
                lines.append(f"    목표 업사이드: +{r['upside_pct']:.1f}%")
        lines.append("")

    # ── Panel 4: 국적별 수급 ──
    p4 = content.get("panel_4_nationality", [])
    if p4:
        lines.append("<b>4. 국적별 수급</b>")
        for n in p4[:5]:
            if not n["countries"]:
                continue
            top3 = sorted(n["countries"].items(), key=lambda x: x[1], reverse=True)[:3]
            country_str = " | ".join(f"{c} {v:+,}" for c, v in top3)
            lines.append(f"  {n['name']} [{n['nat_grade']}]")
            lines.append(f"    {country_str}")
        lines.append("")

    # ── Panel 5: 매집 감지 ──
    p5 = content.get("panel_5_accumulation", [])
    if p5:
        lines.append("<b>5. 매집 감지 레이더</b>")
        for a in p5[:5]:
            lines.append(
                f"  {a['name']} {a['pattern_label']} "
                f"(TV {a['tv_ratio']:.1f}x, 점수 {a['score']})"
            )
        lines.append("")

    # ── Panel 6: 릴레이 ──
    p6 = content.get("panel_6_relay", {})
    if p6.get("hot_sectors"):
        lines.append("<b>6. 릴레이 체인</b>")
        lines.append(f"  HOT: {', '.join(p6['hot_sectors'])}")
        if p6.get("rotation_signal"):
            lines.append(f"  로테이션: {p6['rotation_signal']}")
        if p6.get("commodity_info"):
            lines.append(f"  원자재: {p6['commodity_info'][:60]}")
        if p6.get("tv_cluster"):
            lines.append(f"  클러스터: {p6['tv_cluster'][:60]}")
        lines.append("")

    # ── Panel 7: 적중률 ──
    p7 = content.get("panel_7_accuracy", {})
    overall = p7.get("overall", {})
    patterns = p7.get("patterns", [])
    if patterns:
        lines.append("<b>7. 적중률 대시보드</b>")
        lines.append(
            f"  전체: {overall.get('overall_hit_rate', 0)}% "
            f"({overall.get('total_hits', 0)}/{overall.get('total_trades', 0)}) "
            f"평균 {overall.get('overall_avg_pnl', 0):+.1f}%"
        )
        for p in patterns[:5]:
            lines.append(
                f"  {p['source']}: {p['hit_rate']}% "
                f"({p['hits']}/{p['total']}) {p['avg_pnl']:+.1f}%"
            )
        lines.append("")

    # ── Panel 8: 변곡점 ──
    p8 = content.get("panel_8_alerts", [])
    exit_reduce = [a for a in p8 if a["action"] in ("EXIT", "REDUCE")]
    if exit_reduce:
        lines.append("<b>8. 변곡점 알림</b>")
        for a in exit_reduce[:5]:
            icon = "🚨" if a["action"] == "EXIT" else "⚠️"
            lines.append(
                f"  {icon} {a['name']} → {a['action']} "
                f"(리스크 {a['risk_score']:.0f}, P&L {a['pnl_pct']:+.1f}%)"
            )
            if a.get("key_reason"):
                lines.append(f"    {a['key_reason']}")
        lines.append("")

    # ── 요약 ──
    s = content.get("summary", {})
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(
        f"종목 {s.get('total_stocks', 0)} | "
        f"STRONG_PICK {s.get('strong_pick', 0)} | "
        f"매집 {s.get('accumulation_detected', 0)} | "
        f"적중률 {s.get('overall_hit_rate', 0)}%"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  통합 파이프라인
# ═══════════════════════════════════════════════════

def run_vip_content(upload: bool = True) -> dict:
    """원클릭 파이프라인: 생성 → 저장 → 업로드

    Args:
        upload: Supabase 업로드 여부

    Returns:
        생성된 VIP 콘텐츠 dict
    """
    content = generate_vip_content()
    if upload:
        upload_vip_content(content)
    return content


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

def _print_content(content: dict):
    """콘솔 출력"""
    print(f"\n{'='*70}")
    print(f"  FLOWX VIP 8-Panel Content — {content['date']}")
    print(f"  생성: {content['generated_at']}")
    print(f"{'='*70}")

    # Panel 1
    p1 = content.get("panel_1_stocks", [])
    print(f"\n[Panel 1] 추천종목 ({len(p1)}개)")
    for s in p1:
        tags = ", ".join(s["source_tags"][:3])
        print(f"  {s['name']:>12} [{s['grade']:>3}] {s['score']:>5.1f}점 "
              f"{s['signal']:<10} ({tags})")
        print(f"               진입 {s['entry']:>8,} → 목표 {s['tp']:>8,} (SL {s['sl']:>8,})")

    # Panel 2
    p2 = content.get("panel_2_supply", {})
    top_f = p2.get("top_foreign_buy", [])
    print(f"\n[Panel 2] ALL 수급")
    if top_f:
        print("  외국인 매수 TOP:")
        for s in top_f[:5]:
            print(f"    {s['name']:>12} +{s['foreign_net']:>8,}")

    # Panel 3
    p3 = content.get("panel_3_reasons", [])
    print(f"\n[Panel 3] 추천 근거 ({len(p3)}개)")
    for r in p3[:3]:
        print(f"  {r['name']} ({r['score']}점 {r['confidence']})")
        for a in r["analysis"][:3]:
            print(f"    - {a}")

    # Panel 5
    p5 = content.get("panel_5_accumulation", [])
    print(f"\n[Panel 5] 매집 감지 ({len(p5)}개)")
    for a in p5[:5]:
        print(f"  {a['name']:>12} {a['pattern_label']:<10} "
              f"TV {a['tv_ratio']:.1f}x  점수 {a['score']}")

    # Panel 6
    p6 = content.get("panel_6_relay", {})
    print(f"\n[Panel 6] 릴레이")
    if p6.get("hot_sectors"):
        print(f"  HOT: {', '.join(p6['hot_sectors'])}")
    if p6.get("rotation_signal"):
        print(f"  로테이션: {p6['rotation_signal']}")

    # Panel 7
    p7 = content.get("panel_7_accuracy", {})
    overall = p7.get("overall", {})
    print(f"\n[Panel 7] 적중률")
    print(f"  전체: {overall.get('overall_hit_rate', 0)}% "
          f"({overall.get('total_hits', 0)}/{overall.get('total_trades', 0)})")
    for p in p7.get("patterns", [])[:5]:
        print(f"    {p['source']:>25}: {p['hit_rate']:>5.1f}% "
              f"({p['hits']}/{p['total']}) {p['avg_pnl']:+.1f}%")

    # Panel 8
    p8 = content.get("panel_8_alerts", [])
    exits = [a for a in p8 if a["action"] in ("EXIT", "REDUCE")]
    print(f"\n[Panel 8] 변곡점 ({len(exits)}건)")
    for a in exits[:5]:
        print(f"  {a['action']:>6} {a['name']:>12} "
              f"리스크 {a['risk_score']:.0f} P&L {a['pnl_pct']:+.1f}%")

    # 요약
    s = content.get("summary", {})
    print(f"\n{'='*70}")
    print(f"  종목 {s.get('total_stocks', 0)} | "
          f"STRONG_PICK {s.get('strong_pick', 0)} | "
          f"매집 {s.get('accumulation_detected', 0)} | "
          f"EXIT {s.get('exit_alerts', 0)} | "
          f"적중률 {s.get('overall_hit_rate', 0)}%")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    sys.path.insert(0, str(BASE_DIR))

    import argparse
    parser = argparse.ArgumentParser(description="FLOWX VIP 8-Panel Content")
    parser.add_argument("--test", action="store_true", help="콘솔 출력만")
    parser.add_argument("--upload", action="store_true", help="Supabase 업로드")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 포맷 출력")
    args = parser.parse_args()

    content = generate_vip_content()
    _print_content(content)

    if args.telegram:
        print("\n" + "="*70)
        print("  텔레그램 포맷:")
        print("="*70)
        print(format_telegram_vip(content))

    if args.upload:
        ok = upload_vip_content(content)
        print(f"\nSupabase 업로드: {'성공' if ok else '실패'}")
    elif not args.test and not args.telegram:
        print("[INFO] --upload 또는 --telegram 옵션으로 실행하세요")
