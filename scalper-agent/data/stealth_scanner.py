# -*- coding: utf-8 -*-
"""
기관 선매집 탐지기 (Stealth Accumulation Scanner)
==================================================
기관/외국인이 연속 매수하고 있지만 주가는 아직 안 움직인 "잠복" 종목 감지.
뉴스 하나에 폭등할 '장전된 스프링' 포착.

데이터: data_store/flow/{code}_investor.csv  (2,500+ 종목)
출력:   data_store/stealth_scan.json

점수 체계 (최대 140점):
  - 연속 매수일수: min(days, 10) * 5  → 최대 50점
  - 일평균 순매수액: 점진적 스케일     → 최대 40점
  - 쌍매수(기관+외인 동시): +30점
  - 잠복 보너스(가격 ±5%): +20점

카테고리:
  - 잠복: 5일 등락률 ±5% 이내 (뉴스 대기 상태)
  - 움직임: 5일 등락률 +5~10% (초기 반응)
  - 이미상승: 5일 등락률 +10%+ (이미 출발)
"""

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.Stealth")

DATA_DIR = Path(__file__).parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"
OUTPUT_PATH = DATA_DIR / "stealth_scan.json"

# 최소 시총 필터 (억원)
MIN_CAP = 3000
# 최소 연속 매수일
MIN_CONSECUTIVE_DAYS = 2
# 최소 일평균 순매수액 (백만원)
MIN_AVG_NET = 30  # 30백만 = 3000만원 (CSV 단위: 백만원)
# 분석 기간 (최근 N 거래일)
LOOKBACK_DAYS = 10
# 5일 등락률 기준 (카테고리 분류)
STEALTH_THRESHOLD = 5.0   # ±5% → 잠복
MOVING_THRESHOLD = 10.0   # +5~10% → 움직임


def scan_stealth_accumulation(min_cap: int = MIN_CAP, top_n: int = 50) -> dict:
    """전종목 기관/외인 선매집 스캔.

    Returns:
        {
            "timestamp": "...",
            "total_scanned": int,
            "stealth": [...],   # 잠복 (가격 ±5%)
            "moving": [...],    # 움직임 (5~10%)
            "surged": [...],    # 이미상승 (10%+)
            "summary": {...}
        }
    """
    # 1) universe 로드
    uni_path = DATA_DIR / "universe.json"
    if not uni_path.exists():
        logger.error("universe.json 없음")
        return {"error": "universe.json missing"}

    universe = json.loads(uni_path.read_text("utf-8"))

    # 시총 필터
    targets = {}
    for code, info in universe.items():
        if not isinstance(info, dict):
            continue
        cap = info.get("cap_억", 0)
        if cap < min_cap:
            continue
        targets[code] = info

    logger.info(f"[선매집] 시총 {min_cap}억+ 종목: {len(targets)}개")

    # 2) 각 종목 flow CSV 분석
    results = []
    scanned = 0

    for code, info in targets.items():
        csv_path = FLOW_DIR / f"{code}_investor.csv"
        if not csv_path.exists():
            continue

        scanned += 1
        result = _analyze_stock(code, info, csv_path)
        if result:
            results.append(result)

    # 3) 카테고리 분류
    stealth = []  # 잠복
    moving = []   # 움직임
    surged = []   # 이미상승

    for r in sorted(results, key=lambda x: x["score"], reverse=True):
        chg5 = r.get("chg_5d", 0)
        if -STEALTH_THRESHOLD <= chg5 <= STEALTH_THRESHOLD:
            r["category"] = "잠복"
            stealth.append(r)
        elif STEALTH_THRESHOLD < chg5 <= MOVING_THRESHOLD:
            r["category"] = "움직임"
            moving.append(r)
        else:
            r["category"] = "이미상승"
            surged.append(r)

    # 상위 N개만
    stealth = stealth[:top_n]
    moving = moving[:20]
    surged = surged[:10]

    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "stealth": stealth,
        "moving": moving,
        "surged": surged,
        "summary": {
            "total_detected": len(results),
            "stealth_count": len(stealth),
            "moving_count": len(moving),
            "surged_count": len(surged),
            "top_stealth": [
                f"{s['name']}({s['score']}점,{s['pattern']})"
                for s in stealth[:5]
            ],
        },
    }

    # 저장
    try:
        tmp = OUTPUT_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUTPUT_PATH)
        logger.info(
            f"[선매집] 스캔 완료: {scanned}종목 → "
            f"잠복 {len(stealth)} / 움직임 {len(moving)} / 이미상승 {len(surged)}"
        )
    except Exception as e:
        logger.warning(f"[선매집] 저장 실패: {e}")

    return output


def _analyze_stock(code: str, info: dict, csv_path: Path) -> dict | None:
    """개별 종목 flow CSV 분석 → 선매집 감지 결과 또는 None"""
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception:
        return None

    if len(rows) < MIN_CONSECUTIVE_DAYS + 1:
        return None

    # 최근 LOOKBACK_DAYS 행만
    recent = rows[-LOOKBACK_DAYS:]

    def safe_float(val, default=0.0):
        try:
            return float(val) if val and str(val).strip() else default
        except (ValueError, TypeError):
            return default

    # 기관/외인 순매수 시계열 추출 (DictReader — 헤더 자동 매핑)
    inst_series = []  # 기관 순매수 (백만원)
    frgn_series = []  # 외인 순매수 (백만원)
    close_series = []  # 종가

    for row in recent:
        # 두 가지 CSV 포맷 대응 (로컬 vs VPS)
        inst = safe_float(row.get("기관_금액", 0))
        frgn = safe_float(row.get("외국인_금액", 0))
        close = safe_float(row.get("종가", 0))
        inst_series.append(inst)
        frgn_series.append(frgn)
        close_series.append(close)

    if len(inst_series) < MIN_CONSECUTIVE_DAYS:
        return None

    # 연속 매수일 계산 (가장 최근부터 역순)
    inst_consec = _count_consecutive_buy(inst_series)
    frgn_consec = _count_consecutive_buy(frgn_series)

    max_consec = max(inst_consec, frgn_consec)
    if max_consec < MIN_CONSECUTIVE_DAYS:
        return None

    # 평균 순매수액 (연속 매수 기간)
    inst_avg = _avg_net_buy(inst_series, inst_consec) if inst_consec >= MIN_CONSECUTIVE_DAYS else 0
    frgn_avg = _avg_net_buy(frgn_series, frgn_consec) if frgn_consec >= MIN_CONSECUTIVE_DAYS else 0

    # 최소 금액 필터
    if inst_avg < MIN_AVG_NET and frgn_avg < MIN_AVG_NET:
        return None

    # 쌍매수 여부
    dual_buy = inst_consec >= MIN_CONSECUTIVE_DAYS and frgn_consec >= MIN_CONSECUTIVE_DAYS

    # 5일 등락률
    chg_5d = 0.0
    if len(close_series) >= 5 and close_series[-5] > 0:
        chg_5d = (close_series[-1] / close_series[-5] - 1) * 100
    elif len(close_series) >= 2 and close_series[0] > 0:
        chg_5d = (close_series[-1] / close_series[0] - 1) * 100

    # 점수 계산
    score = 0

    # (1) 연속일수 점수: min(days, 10) * 5 → 최대 50
    score += min(max_consec, 10) * 5

    # (2) 일평균 순매수액 점수 → 최대 40
    total_avg = inst_avg + frgn_avg  # 백만원
    if total_avg >= 500:     # 5억+
        score += 40
    elif total_avg >= 200:   # 2억+
        score += 30
    elif total_avg >= 100:   # 1억+
        score += 20
    elif total_avg >= MIN_AVG_NET:
        score += 10

    # (3) 쌍매수 보너스 → +30
    if dual_buy:
        score += 30

    # (4) 잠복 보너스 (가격 안 움직임) → +20
    if -STEALTH_THRESHOLD <= chg_5d <= STEALTH_THRESHOLD:
        score += 20

    # 패턴 라벨
    if dual_buy:
        pattern = f"쌍매수{min(inst_consec, frgn_consec)}D"
    elif inst_consec >= frgn_consec:
        pattern = f"기관{inst_consec}D"
    else:
        pattern = f"외인{frgn_consec}D"

    return {
        "code": code,
        "name": info.get("name", ""),
        "sector": info.get("sector", ""),
        "market": info.get("market", ""),
        "cap": info.get("cap_억", 0),
        "score": score,
        "pattern": pattern,
        "dual_buy": dual_buy,
        "inst_consec": inst_consec,
        "frgn_consec": frgn_consec,
        "inst_avg": round(inst_avg),      # 백만원
        "frgn_avg": round(frgn_avg),      # 백만원
        "chg_5d": round(chg_5d, 1),
        "close": int(close_series[-1]) if close_series else 0,
        "per": info.get("per", 0),
        "pbr": info.get("pbr", 0),
    }


def _count_consecutive_buy(series: list) -> int:
    """최근부터 역순으로 연속 매수(양수) 일수 카운트"""
    count = 0
    for val in reversed(series):
        if val > 0:
            count += 1
        else:
            break
    return count


def _avg_net_buy(series: list, consec: int) -> float:
    """연속 매수 기간의 평균 순매수액"""
    if consec <= 0:
        return 0.0
    buy_days = series[-consec:]
    return sum(buy_days) / len(buy_days) if buy_days else 0.0


# ── 텔레그램 포맷 ──

def format_stealth_alert(scan: dict) -> str:
    """텔레그램 알림 포맷"""
    stealth = scan.get("stealth", [])
    moving = scan.get("moving", [])
    summary = scan.get("summary", {})

    lines = [
        "🔍 기관 선매집 탐지 리포트",
        f"스캔: {scan.get('total_scanned', 0)}종목 | "
        f"잠복 {summary.get('stealth_count', 0)} / "
        f"움직임 {summary.get('moving_count', 0)} / "
        f"이미상승 {summary.get('surged_count', 0)}",
        "",
    ]

    if stealth:
        lines.append("━━ 잠복 (가격 ±5%, 스프링 장전) ━━")
        for s in stealth[:15]:
            badge = "🔴" if s["score"] >= 100 else "🟠" if s["score"] >= 70 else "🟡"
            dual = "⚡" if s.get("dual_buy") else ""
            lines.append(
                f"{badge}{dual} {s['name']} [{s['sector']}] "
                f"점수{s['score']} {s['pattern']} "
                f"I+{s['inst_avg']}M/F+{s['frgn_avg']}M "
                f"5D{s['chg_5d']:+.1f}%"
            )
        lines.append("")

    if moving:
        lines.append("━━ 움직임 (+5~10%, 초기 반응) ━━")
        for m in moving[:10]:
            badge = "🔵"
            dual = "⚡" if m.get("dual_buy") else ""
            lines.append(
                f"{badge}{dual} {m['name']} [{m['sector']}] "
                f"점수{m['score']} {m['pattern']} "
                f"5D{m['chg_5d']:+.1f}%"
            )
        lines.append("")

    lines.append(f"🕐 {scan.get('timestamp', '')}")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = scan_stealth_accumulation()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
    print(format_stealth_alert(result))
