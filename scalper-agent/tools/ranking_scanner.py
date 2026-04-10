# -*- coding: utf-8 -*-
"""
국내종목 순위 실시간 스캐너
============================
KIS API 5개 순위 조회 → 교차 분석 → 상한가/급등/세력주 탐지

1. 등락률 순위 (FHPST01700000) — 급등 종목
2. 거래량 순위 (FHPST01710000) — 거래 폭발
3. 체결강도 상위 (FHPST01680000) — 매수세
4. 상하한가 포착 (FHKST130000C0) — 상한가/근접
5. 외국인/기관 가집계 (FHPTJ04400000) — 수급

v1: 2026-04-10
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("BH.RankScan")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"


def _get_trader():
    """KISTrader 싱글턴 가져오기 (COO에서 이미 생성된 것 재사용)."""
    try:
        from bot.kis_trader import KISTrader
        return KISTrader({})
    except Exception as e:
        logger.error(f"KISTrader 생성 실패: {e}")
        return None


def scan_surge(trader=None, top_n: int = 20) -> dict:
    """급등 + 상한가 근접 교차 스캔.

    Returns: {
        "timestamp": "2026-04-10 13:30",
        "upper_limit": [...],   # 상한가 종목
        "near_limit": [...],    # 8%+ 근접
        "surge_top": [...],     # 등락률 상위
        "crossover": [...],     # 급등 + 체결강도 + 수급 교차 종목
    }
    """
    if not trader:
        trader = _get_trader()
    if not trader:
        return {}

    result = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")}

    # 1) 상한가 종목 (가장 중요)
    try:
        upper = trader.fetch_uplowprice(price_cls="0", div_cls="0")
        result["upper_limit"] = upper[:10]
        logger.info(f"[RANK] 상한가 종목: {len(upper)}개")
    except Exception as e:
        logger.warning(f"[RANK] 상한가 조회 실패: {e}")
        result["upper_limit"] = []

    time.sleep(0.3)

    # 2) 상한가 8% 근접 (추격 매수 후보)
    try:
        near = trader.fetch_uplowprice(price_cls="0", div_cls="6")
        result["near_limit"] = near[:15]
        logger.info(f"[RANK] 상한가 근접(8%+): {len(near)}개")
    except Exception as e:
        logger.warning(f"[RANK] 상한가 근접 조회 실패: {e}")
        result["near_limit"] = []

    time.sleep(0.3)

    # 3) 등락률 상위
    try:
        fluct = trader.fetch_ranking_fluctuation(top_n=top_n)
        result["surge_top"] = fluct
        logger.info(f"[RANK] 등락률 상위: {len(fluct)}개")
    except Exception as e:
        logger.warning(f"[RANK] 등락률 조회 실패: {e}")
        result["surge_top"] = []

    time.sleep(0.3)

    # 4) 체결강도 상위
    try:
        strength = trader.fetch_ranking_strength(top_n=top_n)
        result["strength_top"] = strength
        # 코드→체결강도 맵
        strength_map = {s["code"]: s["strength"] for s in strength if s["code"]}
    except Exception as e:
        logger.warning(f"[RANK] 체결강도 조회 실패: {e}")
        result["strength_top"] = []
        strength_map = {}

    time.sleep(0.3)

    # 5) 거래량 폭발
    try:
        volume = trader.fetch_ranking_volume(top_n=top_n, sort_by="3")
        result["volume_top"] = volume
        volume_codes = {v["code"] for v in volume if v["code"]}
    except Exception as e:
        logger.warning(f"[RANK] 거래량 조회 실패: {e}")
        result["volume_top"] = []
        volume_codes = set()

    # ── 교차 분석: 급등 + 체결강도 100+ + 거래량 상위 ──
    crossover = []
    for s in result.get("surge_top", []):
        code = s.get("code", "")
        if not code:
            continue
        chg = s.get("change_rate", 0)
        str_val = strength_map.get(code, 0)
        in_vol = code in volume_codes

        # 급등(5%+) + 체결강도(100+) = 강한 신호
        # 급등(5%+) + 거래량 상위 = 관심
        score = 0
        tags = []
        if chg >= 15:
            score += 40
            tags.append("폭등")
        elif chg >= 10:
            score += 30
            tags.append("급등")
        elif chg >= 5:
            score += 15
            tags.append("강세")
        else:
            continue  # 5% 미만은 교차 분석 대상 외

        if str_val >= 200:
            score += 30
            tags.append(f"체결{str_val:.0f}")
        elif str_val >= 150:
            score += 20
            tags.append(f"체결{str_val:.0f}")
        elif str_val >= 100:
            score += 10
            tags.append(f"체결{str_val:.0f}")

        if in_vol:
            score += 15
            tags.append("거래폭발")

        # 상한가 근접 종목이면 추가 가점
        near_codes = {n["code"] for n in result.get("near_limit", []) if n.get("code")}
        if code in near_codes:
            score += 20
            tags.append("상한근접")

        upper_codes = {u["code"] for u in result.get("upper_limit", []) if u.get("code")}
        if code in upper_codes:
            score += 30
            tags.append("상한가")

        crossover.append({
            "code": code,
            "name": s.get("name", ""),
            "price": s.get("price", 0),
            "change_rate": chg,
            "strength": str_val,
            "in_volume_top": in_vol,
            "score": score,
            "tags": tags,
        })

    crossover.sort(key=lambda x: x["score"], reverse=True)
    result["crossover"] = crossover[:15]

    # 로컬 저장
    try:
        save_path = DATA_DIR / "ranking_scan.json"
        save_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass

    return result


def scan_institution(trader=None) -> dict:
    """외국인/기관 수급 스캔.

    Returns: {
        "foreign_buy": [...],   # 외국인 순매수 상위
        "foreign_sell": [...],  # 외국인 순매도 상위
        "inst_buy": [...],      # 기관 순매수 상위
    }
    """
    if not trader:
        trader = _get_trader()
    if not trader:
        return {}

    result = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")}

    # 외국인 순매수
    try:
        result["foreign_buy"] = trader.fetch_foreign_inst_total(target="1", sort_cls="0")[:15]
    except Exception as e:
        logger.warning(f"[RANK] 외국인 순매수 조회 실패: {e}")
        result["foreign_buy"] = []

    time.sleep(0.3)

    # 외국인 순매도
    try:
        result["foreign_sell"] = trader.fetch_foreign_inst_total(target="1", sort_cls="1")[:15]
    except Exception as e:
        logger.warning(f"[RANK] 외국인 순매도 조회 실패: {e}")
        result["foreign_sell"] = []

    time.sleep(0.3)

    # 기관 순매수
    try:
        result["inst_buy"] = trader.fetch_foreign_inst_total(target="2", sort_cls="0")[:15]
    except Exception as e:
        logger.warning(f"[RANK] 기관 순매수 조회 실패: {e}")
        result["inst_buy"] = []

    return result


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_surge_telegram(data: dict) -> str:
    """급등/상한가 스캔 결과 텔레그램 메시지."""
    if not data:
        return "순위 데이터 없음"

    ts = data.get("timestamp", "")
    lines = [f"<b>실시간 순위 스캔</b> ({ts})", ""]

    # 상한가
    upper = data.get("upper_limit", [])
    if upper:
        lines.append(f"<b>상한가 ({len(upper)}종목)</b>")
        for u in upper[:5]:
            lines.append(f"  {u['name']} · {u['price']:,}원 · 거래{u['volume']:,}")
        lines.append("")

    # 상한가 근접
    near = data.get("near_limit", [])
    if near:
        lines.append(f"<b>상한가 근접 ({len(near)}종목)</b>")
        for n in near[:5]:
            lines.append(
                f"  {n['name']} · {n['change_rate']:+.1f}% · {n['price']:,}원"
            )
        lines.append("")

    # 교차분석 (핵심)
    cross = data.get("crossover", [])
    if cross:
        lines.append(f"<b>교차 분석 ({len(cross)}종목)</b>")
        for c in cross[:10]:
            tags_str = " ".join(c["tags"])
            str_info = f"체결{c['strength']:.0f}" if c["strength"] > 0 else ""
            lines.append(
                f"  <b>{c['name']}</b> {c['change_rate']:+.1f}% "
                f"{str_info} [{tags_str}] ({c['score']}점)"
            )
        lines.append("")

    # 등락률 TOP 5 (교차에 안 잡힌 것 포함)
    surge = data.get("surge_top", [])
    if surge:
        lines.append(f"<b>등락률 TOP 5</b>")
        for s in surge[:5]:
            lines.append(
                f"  {s['name']} · {s['change_rate']:+.1f}% · {s['price']:,}원"
            )
        lines.append("")

    # 체결강도 TOP 5
    strength = data.get("strength_top", [])
    if strength:
        lines.append(f"<b>체결강도 TOP 5</b>")
        for s in strength[:5]:
            lines.append(
                f"  {s['name']} · 강도{s['strength']:.0f} · {s['change_rate']:+.1f}%"
            )

    return "\n".join(lines)


def format_institution_telegram(data: dict) -> str:
    """외국인/기관 수급 텔레그램 메시지."""
    if not data:
        return "수급 데이터 없음"

    ts = data.get("timestamp", "")
    lines = [f"<b>외국인/기관 가집계</b> ({ts})", ""]

    fb = data.get("foreign_buy", [])
    if fb:
        lines.append("<b>외국인 순매수 TOP 5</b>")
        for f in fb[:5]:
            lines.append(f"  {f['name']} · {f['change_rate']:+.1f}% · {f['price']:,}원")
        lines.append("")

    fs = data.get("foreign_sell", [])
    if fs:
        lines.append("<b>외국인 순매도 TOP 5</b>")
        for f in fs[:5]:
            lines.append(f"  {f['name']} · {f['change_rate']:+.1f}% · {f['price']:,}원")
        lines.append("")

    ib = data.get("inst_buy", [])
    if ib:
        lines.append("<b>기관 순매수 TOP 5</b>")
        for f in ib[:5]:
            lines.append(f"  {f['name']} · {f['change_rate']:+.1f}% · {f['price']:,}원")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import os, sys

    # scalper-agent 디렉토리로 이동 (모듈 임포트용)
    _SCRIPT_DIR = Path(__file__).resolve().parent.parent
    os.chdir(str(_SCRIPT_DIR))
    sys.path.insert(0, str(_SCRIPT_DIR))

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="국내종목 순위 스캐너")
    parser.add_argument("--surge", action="store_true", help="급등/상한가 스캔")
    parser.add_argument("--inst", action="store_true", help="외국인/기관 수급")
    parser.add_argument("--all", action="store_true", help="전체 스캔")
    args = parser.parse_args()

    if args.surge or args.all:
        data = scan_surge()
        print(format_surge_telegram(data).replace("<b>", "").replace("</b>", ""))
        print()

    if args.inst or args.all:
        data = scan_institution()
        print(format_institution_telegram(data).replace("<b>", "").replace("</b>", ""))

    if not (args.surge or args.inst or args.all):
        parser.print_help()
