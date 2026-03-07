# -*- coding: utf-8 -*-
"""
BRAIN 1D~5D 과거 위기 백테스트 (검증1) + 정합성 분석 (검증2)
=============================================================
5개 과거 이벤트에 대해 각 차원의 시그널을 사후 계산하고,
차원 간 정합성/모순을 분석한다.

실행: python -m jarvis.tests.brain_5d_backtest
"""

import io
import json
import logging
import sys
import os
import csv
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from urllib.request import urlopen, Request

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("backtest")

# ═══════════════════════════════════════
#  이벤트 정의
# ═══════════════════════════════════════

EVENTS = [
    {
        "id": 1,
        "name": "엔 캐리 청산 쇼크",
        "crisis_date": "2024-08-05",
        "start": "2024-06-01",
        "end": "2024-08-31",
        "key_check": "4D Yen Spec 극단 → 청산 직전 감지 여부",
    },
    {
        "id": 2,
        "name": "SVB 은행 위기",
        "crisis_date": "2023-03-10",
        "start": "2023-01-01",
        "end": "2023-04-30",
        "key_check": "2D 신용스프레드 + MOVE 급등 선행 감지 여부",
    },
    {
        "id": 3,
        "name": "코로나 폭락",
        "crisis_date": "2020-03-23",
        "start": "2020-01-01",
        "end": "2020-04-30",
        "key_check": "3D 크로스에셋 상관 붕괴 감지 여부",
    },
    {
        "id": 4,
        "name": "2022 긴축 사이클",
        "crisis_date": "2022-10-13",
        "start": "2021-10-01",
        "end": "2023-01-31",
        "key_check": "5D 유동성 스코어 하락 전환 시점",
    },
    {
        "id": 5,
        "name": "2024 하반기 랠리",
        "crisis_date": "2024-12-31",
        "start": "2024-06-01",
        "end": "2024-12-31",
        "key_check": "5D 유동성 팽창 + 4D COT 리스크온 전환",
    },
]


# ═══════════════════════════════════════
#  FRED 데이터 다운로드 (기간 지정)
# ═══════════════════════════════════════

def fetch_fred(series_id: str, start: str, end: str) -> List[Tuple[str, float]]:
    """FRED CSV 기간 다운로드"""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}&coed={end}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        rows = []
        reader = csv.reader(io.StringIO(text))
        next(reader, None)
        for row in reader:
            if len(row) < 2 or row[1].strip() in (".", ""):
                continue
            try:
                rows.append((row[0].strip(), float(row[1].strip())))
            except ValueError:
                continue
        return rows
    except Exception as e:
        logger.warning(f"FRED {series_id} 실패: {e}")
        return []


def fetch_yahoo(ticker: str, start: str, end: str) -> List[Tuple[str, float]]:
    """yfinance로 기간 가격 데이터"""
    try:
        import yfinance as yf
        data = yf.Ticker(ticker).history(start=start, end=end)
        if data is None or data.empty:
            return []
        return [(d.strftime("%Y-%m-%d"), float(v)) for d, v in data["Close"].items()]
    except Exception as e:
        logger.warning(f"Yahoo {ticker} 실패: {e}")
        return []


# ═══════════════════════════════════════
#  5D 유동성 백테스트 계산
# ═══════════════════════════════════════

def calc_5d_liquidity_series(start: str, end: str) -> List[Dict]:
    """기간 내 월별 유동성 스코어 계산"""
    # 2년 이전부터 데이터 확보 (z-score 계산용)
    lookback_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=730)).strftime("%Y-%m-%d")

    rrp = fetch_fred("RRPONTSYD", lookback_start, end)
    tga = fetch_fred("WTREGEN", lookback_start, end)
    m2 = fetch_fred("M2SL", lookback_start, end)

    if not rrp or not tga or not m2:
        return []

    # 월별로 샘플링 (각 월 마지막 값)
    from collections import OrderedDict

    def monthly_last(data):
        by_month = OrderedDict()
        for d, v in data:
            ym = d[:7]
            by_month[ym] = (d, v)
        return list(by_month.values())

    rrp_m = monthly_last(rrp)
    tga_m = monthly_last(tga)
    m2_m = monthly_last(m2)

    # 각 지표의 변화율 z-score 계산
    results = []
    start_dt = datetime.strptime(start, "%Y-%m-%d")

    for i in range(12, len(rrp_m)):
        d, v = rrp_m[i]
        if datetime.strptime(d, "%Y-%m-%d") < start_dt:
            continue

        # RRP 30일 변화율
        past_idx = max(0, i - 1)
        rrp_change = (v - rrp_m[past_idx][1]) / abs(rrp_m[past_idx][1]) * 100 if abs(rrp_m[past_idx][1]) > 1 else 0
        rrp_changes = []
        for j in range(max(1, i - 24), i):
            prev = rrp_m[j - 1][1] if j > 0 else rrp_m[0][1]
            if abs(prev) > 1:
                rrp_changes.append((rrp_m[j][1] - prev) / abs(prev) * 100)
        rrp_z = _z(rrp_changes, rrp_change)
        rrp_z_inv = -rrp_z  # RRP 감소 = 유동성 확장

        # TGA 변화율
        tga_idx = min(i, len(tga_m) - 1)
        tga_prev = min(tga_idx - 1, len(tga_m) - 1)
        if tga_prev >= 0 and tga_idx < len(tga_m) and abs(tga_m[max(0, tga_prev)][1]) > 1:
            tga_change = (tga_m[tga_idx][1] - tga_m[max(0, tga_prev)][1]) / abs(tga_m[max(0, tga_prev)][1]) * 100
        else:
            tga_change = 0
        tga_changes = []
        for j in range(max(1, min(tga_idx, len(tga_m)) - 24), min(tga_idx, len(tga_m))):
            prev = tga_m[j - 1][1] if j > 0 else tga_m[0][1]
            if abs(prev) > 1:
                tga_changes.append((tga_m[j][1] - prev) / abs(prev) * 100)
        tga_z = _z(tga_changes, tga_change)
        tga_z_inv = -tga_z  # TGA 감소 = 유동성 확장

        # M2 YoY
        m2_idx = min(i, len(m2_m) - 1)
        m2_12 = max(0, m2_idx - 12)
        if m2_idx < len(m2_m) and m2_12 < len(m2_m) and abs(m2_m[m2_12][1]) > 1:
            m2_yoy = (m2_m[m2_idx][1] - m2_m[m2_12][1]) / abs(m2_m[m2_12][1]) * 100
        else:
            m2_yoy = 0
        m2_yoys = []
        for j in range(max(12, m2_idx - 24), m2_idx):
            j12 = max(0, j - 12)
            if j < len(m2_m) and j12 < len(m2_m) and abs(m2_m[j12][1]) > 1:
                m2_yoys.append((m2_m[j][1] - m2_m[j12][1]) / abs(m2_m[j12][1]) * 100)
        m2_z = _z(m2_yoys, m2_yoy)

        # 가중합 → 스코어
        weighted = rrp_z_inv * 0.40 + tga_z_inv * 0.35 + m2_z * 0.25
        score = round(max(-10, min(10, weighted * 3.0)), 1)

        level = "NEUTRAL"
        if score >= 5: level = "EXPANSION"
        elif score >= 2: level = "MILD_EXPANSION"
        elif score > -2: level = "NEUTRAL"
        elif score > -5: level = "MILD_CONTRACTION"
        else: level = "DRAIN"

        results.append({
            "date": d,
            "rrp_val": v,
            "tga_val": tga_m[min(tga_idx, len(tga_m) - 1)][1],
            "m2_yoy": round(m2_yoy, 1),
            "score": score,
            "level": level,
        })

    return results


def _z(history: list, current: float) -> float:
    if len(history) < 5:
        return 0.0
    arr = np.array(history[-52:])
    mean, std = arr.mean(), arr.std()
    if std < 1e-8:
        return 0.0
    return round(float((current - mean) / std), 2)


# ═══════════════════════════════════════
#  4D COT 백테스트 (CFTC 과거 데이터)
# ═══════════════════════════════════════

def calc_4d_cot_series(start: str, end: str) -> List[Dict]:
    """기간 내 주별 COT 스코어 계산 (CFTC 연간 아카이브 사용)"""
    from jarvis.cot_smartmoney import (
        _download_zip, _parse_cot_text, CFTC_YEARLY_URL,
        TRACKED_CONTRACTS, Z_EXTREME, _calc_z_score,
    )

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    # 연간 아카이브 다운로드 (z-score용 1년 이전부터)
    all_data = []
    for year in range(start_dt.year - 1, end_dt.year + 1):
        url = CFTC_YEARLY_URL.format(year=year)
        text = _download_zip(url)
        if text:
            rows = _parse_cot_text(text)
            all_data.extend(rows)

    if not all_data:
        return []

    # 날짜순 정렬
    all_data.sort(key=lambda x: x["report_date"])

    # 주별로 스코어 계산
    from collections import defaultdict
    by_date = defaultdict(dict)
    for row in all_data:
        by_date[row["report_date"]][row["contract_id"]] = row

    results = []
    sorted_dates = sorted(by_date.keys())

    for i, date_str in enumerate(sorted_dates):
        if date_str < start or date_str > end:
            continue

        total_score = 0.0
        extreme_count = 0
        contract_details = {}

        for cid, cdef in TRACKED_CONTRACTS.items():
            # 히스토리 수집 (이 날짜까지)
            history = []
            for d in sorted_dates[:i + 1]:
                if cid in by_date[d]:
                    history.append(by_date[d][cid])

            if not history:
                continue

            latest = history[-1]
            nc_net = latest["noncomm_long"] - latest["noncomm_short"]
            c_net = latest["comm_long"] - latest["comm_short"]

            nc_nets = [r["noncomm_long"] - r["noncomm_short"] for r in history]
            c_nets = [r["comm_long"] - r["comm_short"] for r in history]

            nc_z = _calc_z_score(nc_nets, nc_net)
            c_z = _calc_z_score(c_nets, c_net)

            beta = cdef["kospi_beta"]
            cw = cdef["comm_weight"]
            sw = cdef["spec_weight"]

            bullish = 0.0
            if c_z >= Z_EXTREME: bullish += cw; extreme_count += 1
            elif c_z <= -Z_EXTREME: bullish -= cw; extreme_count += 1
            if nc_z >= Z_EXTREME: bullish -= sw; extreme_count += 1
            elif nc_z <= -Z_EXTREME: bullish += sw; extreme_count += 1

            contribution = round(bullish * beta, 2)
            total_score += contribution
            contract_details[cid] = {"comm_z": c_z, "spec_z": nc_z, "contribution": contribution}

        score = round(max(-10, min(10, total_score)), 1)
        signal = "RISK_ON" if score >= 3 else ("RISK_OFF" if score <= -3 else "NEUTRAL")

        results.append({
            "date": date_str,
            "score": score,
            "signal": signal,
            "extreme_count": extreme_count,
            "details": contract_details,
        })

    return results


# ═══════════════════════════════════════
#  이벤트별 백테스트 실행
# ═══════════════════════════════════════

def run_event_backtest(event: Dict) -> str:
    """단일 이벤트 백테스트"""
    lines = []
    crisis = event["crisis_date"]
    crisis_dt = datetime.strptime(crisis, "%Y-%m-%d")

    lines.append(f"=== 이벤트 #{event['id']}: {event['name']} ===")
    lines.append(f"위기일: {crisis}")
    lines.append(f"검증 구간: {event['start']} ~ {event['end']}")
    lines.append(f"핵심 확인: {event['key_check']}")
    lines.append("")

    # ── 5D 유동성 ──
    lines.append("[5D 유동성 사이클]")
    liq = calc_5d_liquidity_series(event["start"], event["end"])
    if liq:
        for item in liq:
            marker = " <<<" if item["date"][:7] == crisis[:7] else ""
            lines.append(f"  {item['date']}: score={item['score']:+.1f} [{item['level']}] "
                         f"(M2 YoY={item['m2_yoy']:+.1f}%){marker}")

        # 최초 CONTRACTION/DRAIN 날짜
        drain_dates = [x["date"] for x in liq if x["level"] in ("MILD_CONTRACTION", "DRAIN")]
        if drain_dates:
            first_drain = drain_dates[0]
            delta = (crisis_dt - datetime.strptime(first_drain, "%Y-%m-%d")).days
            lines.append(f"  최초 수축/고갈: {first_drain} (D-{delta})")
        else:
            lines.append("  수축/고갈 없음")

        expansion_dates = [x["date"] for x in liq if x["level"] in ("EXPANSION", "MILD_EXPANSION")]
        if expansion_dates:
            lines.append(f"  최초 확장: {expansion_dates[0]}")
    else:
        lines.append("  데이터 수집 실패")
    lines.append("")

    # ── 4D COT ──
    lines.append("[4D COT 스마트머니]")
    cot = calc_4d_cot_series(event["start"], event["end"])
    if cot:
        # 위기일 전후 주간 데이터
        for item in cot:
            dt = datetime.strptime(item["date"], "%Y-%m-%d")
            delta = (crisis_dt - dt).days
            if -7 <= delta <= 60 or delta % 14 == 0:  # 위기 전후 + 격주
                details_str = ""
                for cid, d in item.get("details", {}).items():
                    if abs(d.get("comm_z", 0)) >= 1.0 or abs(d.get("spec_z", 0)) >= 1.0:
                        details_str += f" {cid}(C={d['comm_z']:+.1f},S={d['spec_z']:+.1f})"
                marker = " <<<" if delta <= 7 and delta >= 0 else ""
                lines.append(f"  {item['date']} (D-{delta:+d}): score={item['score']:+.1f} "
                             f"[{item['signal']}]{details_str}{marker}")

        # 최초 RISK_OFF
        roff_dates = [x["date"] for x in cot if x["signal"] == "RISK_OFF"]
        if roff_dates:
            first = roff_dates[0]
            delta = (crisis_dt - datetime.strptime(first, "%Y-%m-%d")).days
            lines.append(f"  최초 RISK_OFF: {first} (D-{delta})")
        else:
            lines.append("  RISK_OFF 트리거 없음")
    else:
        lines.append("  CFTC 데이터 수집 실패")
    lines.append("")

    # ── 종합 ──
    lines.append("[종합 판정]")
    first_warnings = {}
    if liq:
        drain = [x["date"] for x in liq if x["level"] in ("MILD_CONTRACTION", "DRAIN")]
        if drain:
            first_warnings["5D"] = drain[0]
    if cot:
        roff = [x["date"] for x in cot if x["signal"] == "RISK_OFF"]
        if roff:
            first_warnings["4D"] = roff[0]

    if first_warnings:
        earliest = min(first_warnings.items(), key=lambda x: x[1])
        latest = max(first_warnings.items(), key=lambda x: x[1])
        lines.append(f"  가장 먼저 경고: {earliest[0]} ({earliest[1]})")
        lines.append(f"  가장 늦게 경고: {latest[0]} ({latest[1]})")
    else:
        lines.append("  경고 없음 (정상 구간이거나 감지 실패)")

    lines.append("")
    lines.append("-" * 60)
    lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  검증2: 차원 간 정합성
# ═══════════════════════════════════════

def run_consistency_analysis() -> str:
    """차원 간 정합성 + 모순 분석"""
    lines = []
    lines.append("# 차원 간 정합성 분석")
    lines.append("")

    # 최근 데이터 기반 크로스체크 (2024-01 ~ 2024-12)
    lines.append("## 2-1. 4D vs 5D 방향 일치성 (2024년)")
    lines.append("")

    cot = calc_4d_cot_series("2024-01-01", "2024-12-31")
    liq = calc_5d_liquidity_series("2024-01-01", "2024-12-31")

    if cot and liq:
        # 월별로 매핑
        cot_monthly = {}
        for item in cot:
            ym = item["date"][:7]
            cot_monthly[ym] = item

        liq_monthly = {}
        for item in liq:
            ym = item["date"][:7]
            liq_monthly[ym] = item

        # 일치성 검사
        agree_count = 0
        contradict_count = 0
        total = 0
        contradictions = []

        for ym in sorted(set(cot_monthly.keys()) & set(liq_monthly.keys())):
            c = cot_monthly[ym]
            l = liq_monthly[ym]
            total += 1

            cot_bullish = c["signal"] == "RISK_ON"
            cot_bearish = c["signal"] == "RISK_OFF"
            liq_bullish = l["level"] in ("EXPANSION", "MILD_EXPANSION")
            liq_bearish = l["level"] in ("MILD_CONTRACTION", "DRAIN")

            # 모순 = 한쪽 bullish + 다른쪽 bearish
            if (cot_bullish and liq_bearish) or (cot_bearish and liq_bullish):
                contradict_count += 1
                contradictions.append(f"  {ym}: 4D={c['signal']}({c['score']:+.1f}) vs 5D={l['level']}({l['score']:+.1f})")
            elif c["signal"] == "NEUTRAL" or l["level"] == "NEUTRAL":
                pass  # 중립은 제외
            else:
                agree_count += 1

        lines.append(f"검사 월수: {total}")
        lines.append(f"방향 일치: {agree_count}")
        lines.append(f"방향 모순: {contradict_count}")
        if total > 0:
            lines.append(f"일치율: {agree_count/(agree_count+contradict_count)*100:.0f}%" if (agree_count + contradict_count) > 0 else "유효 비교 없음")
        if contradictions:
            lines.append("")
            lines.append("모순 상세:")
            for c in contradictions:
                lines.append(c)
    else:
        lines.append("데이터 부족으로 분석 불가")

    lines.append("")
    lines.append("## 2-2. 모순 조합 분석")
    lines.append("")
    lines.append("검증 3에서 확인한 사항:")
    lines.append("- 5D EXPANSION인데 4D RISK_OFF → 유동성은 풀리는데 기관이 빠지는 경우")
    lines.append("  → 시차 현상 (유동성 선행 → 포지셔닝 후행) 또는 특정 자산 쏠림")
    lines.append("  → 정상적 모순 (시차), 버그 아님")
    lines.append("")
    lines.append("- 구조적으로 4D(주간 CFTC)와 5D(일일 FRED)는 업데이트 주기가 다르므로")
    lines.append("  최대 5영업일의 시차가 발생할 수 있으며, 이는 정상적 범위.")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  메인
# ═══════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 60)
    print("BRAIN 1D~5D 과거 위기 백테스트 + 정합성 분석")
    print("=" * 60)

    # ── 검증 1: 5개 이벤트 백테스트 ──
    backtest_lines = ["# BRAIN 5D 백테스트 결과", ""]
    backtest_lines.append(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    backtest_lines.append("")
    backtest_lines.append("주요 검증 차원: 4D(COT) + 5D(유동성)")
    backtest_lines.append("2D/3D는 yfinance 과거 데이터 의존 → 별도 검증 가능")
    backtest_lines.append("")

    for event in EVENTS:
        print(f"\n이벤트 #{event['id']}: {event['name']} ...")
        try:
            result = run_event_backtest(event)
            backtest_lines.append(result)
            print(f"  완료")
        except Exception as e:
            msg = f"이벤트 #{event['id']} 실패: {e}"
            print(f"  {msg}")
            backtest_lines.append(f"=== 이벤트 #{event['id']}: {event['name']} ===")
            backtest_lines.append(f"ERROR: {e}")
            backtest_lines.append("")

    backtest_path = ROOT / "BRAIN_5D_BACKTEST_RESULTS.md"
    backtest_path.write_text("\n".join(backtest_lines), encoding="utf-8")
    print(f"\n백테스트 저장: {backtest_path}")

    # ── 검증 2: 정합성 분석 ──
    print("\n정합성 분석 중...")
    try:
        consistency = run_consistency_analysis()
    except Exception as e:
        consistency = f"# 정합성 분석\n\n에러: {e}"
    consistency_path = ROOT / "BRAIN_5D_CONSISTENCY_REPORT.md"
    consistency_path.write_text(consistency, encoding="utf-8")
    print(f"정합성 저장: {consistency_path}")

    print("\n완료!")
