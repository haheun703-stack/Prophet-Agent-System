# -*- coding: utf-8 -*-
"""
BRAIN 2D/3D 과거 위기 백테스트
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
5개 위기 이벤트에서 2D(선행지표) + 3D(크로스에셋) 일별 시그널을 역산.
각 차원이 위기 D-몇일에 경보를 발생시켰는지 확인.

이벤트:
  #1 엔 캐리 청산 (2024-08-05)
  #2 SVB 은행 위기 (2023-03-10)
  #3 코로나 폭락  (2020-03-23)
  #4 2022 긴축    (2022-10-13)
  #5 2024 랠리    (2024-12-31) — RISK_ON 이벤트

사용법:
  python -m jarvis.tests.brain_2d3d_backtest
"""

import sys, io, json, logging
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════
#  yfinance 데이터 다운로드 + 캐시
# ═══════════════════════════════════════════════════

CACHE_DIR = Path(__file__).resolve().parent / "yf_cache"

def fetch_daily(symbol: str, start: str, end: str) -> dict:
    """yfinance 일봉 다운로드 (캐시 포함). Returns {date_str: close}"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe_sym = symbol.replace("^", "_").replace("=", "_").replace("/", "_")
    cache_file = CACHE_DIR / f"{safe_sym}_{start}_{end}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))

    import yfinance as yf
    logger.info(f"Downloading {symbol} {start}~{end}")
    data = yf.Ticker(symbol).history(start=start, end=end)
    if data is None or len(data) == 0:
        return {}

    result = {}
    for idx, row in data.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        close = float(row["Close"])
        if not np.isnan(close):
            result[date_str] = close

    cache_file.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return result


# ═══════════════════════════════════════════════════
#  2D 선행지표 일별 계산
# ═══════════════════════════════════════════════════

def calc_2d_daily(start: str, end: str) -> list:
    """2D 선행지표를 일별로 계산.

    Returns list of {date, warning_count, worsening_count, signal, details}
    """
    # 데이터 다운로드 (여유 30일 추가)
    dt_start = datetime.strptime(start, "%Y-%m-%d") - timedelta(days=30)
    fetch_start = dt_start.strftime("%Y-%m-%d")

    hyg = fetch_daily("HYG", fetch_start, end)
    lqd = fetch_daily("LQD", fetch_start, end)
    tnx = fetch_daily("^TNX", fetch_start, end)
    irx = fetch_daily("^IRX", fetch_start, end)
    move_raw = fetch_daily("^MOVE", fetch_start, end)
    tlt = fetch_daily("TLT", fetch_start, end)
    bil = fetch_daily("BIL", fetch_start, end)
    shv = fetch_daily("SHV", fetch_start, end)

    # 공통 날짜 리스트 (분석 구간)
    all_dates = sorted(set(hyg.keys()) | set(tnx.keys()))
    analysis_dates = [d for d in all_dates if start <= d <= end]

    results = []
    for date in analysis_dates:
        # 이 날짜까지의 최근 10일 데이터 수집
        prev_dates = [d for d in all_dates if d <= date][-10:]

        warnings = 0
        worsenings = 0
        details = {}

        # 1. Credit Spread (HYG/LQD)
        spread_vals = []
        for d in prev_dates:
            h = hyg.get(d)
            l = lqd.get(d)
            if h and l and l > 0:
                spread_vals.append(h / l)

        if len(spread_vals) >= 4:
            # 3일 연속 하락 = 스프레드 확대
            diffs = [spread_vals[i+1] - spread_vals[i] for i in range(len(spread_vals)-1)]
            recent_3 = diffs[-3:] if len(diffs) >= 3 else diffs
            is_warning = len(recent_3) == 3 and all(d < 0 for d in recent_3)
            change_3d = spread_vals[-1] - spread_vals[-4] if len(spread_vals) >= 4 else 0
            is_worsening = change_3d < -0.005

            if is_warning:
                warnings += 1
                worsenings += 1
            elif is_worsening:
                worsenings += 1
            details["credit"] = {"val": round(spread_vals[-1], 4), "chg3d": round(change_3d, 4),
                                  "warn": is_warning, "worse": is_worsening}

        # 2. Yield Curve (TNX - IRX)
        curve_vals = []
        for d in prev_dates:
            t = tnx.get(d)
            i = irx.get(d)
            if t is not None and i is not None:
                curve_vals.append(t - i)

        if len(curve_vals) >= 4:
            diffs = [curve_vals[i+1] - curve_vals[i] for i in range(len(curve_vals)-1)]
            recent_3 = diffs[-3:] if len(diffs) >= 3 else diffs
            is_warning = len(recent_3) == 3 and all(d < 0 for d in recent_3)
            change_3d = curve_vals[-1] - curve_vals[-4] if len(curve_vals) >= 4 else 0
            is_inverted = curve_vals[-1] < 0
            is_worsening = change_3d < -0.05 or is_inverted

            if is_warning:
                warnings += 1
                worsenings += 1
            elif is_worsening:
                worsenings += 1
            details["curve"] = {"val": round(curve_vals[-1], 3), "chg3d": round(change_3d, 3),
                                 "warn": is_warning, "inverted": is_inverted}

        # 3. MOVE Index
        move_vals = []
        for d in prev_dates:
            m = move_raw.get(d)
            if m is not None:
                move_vals.append(m)

        if len(move_vals) >= 4:
            diffs = [move_vals[i+1] - move_vals[i] for i in range(len(move_vals)-1)]
            recent_3 = diffs[-3:] if len(diffs) >= 3 else diffs
            is_warning = len(recent_3) == 3 and all(d > 0 for d in recent_3)
            is_worsening = move_vals[-1] > 120

            if is_warning:
                warnings += 1
                worsenings += 1
            elif is_worsening:
                worsenings += 1
            details["move"] = {"val": round(move_vals[-1], 1), "warn": is_warning, "worse": is_worsening}
        else:
            # TLT 폴백
            tlt_vals = []
            for d in prev_dates:
                t = tlt.get(d)
                if t is not None:
                    tlt_vals.append(t)
            if len(tlt_vals) >= 5:
                returns = np.diff(tlt_vals) / np.array(tlt_vals[:-1]) * 100
                vol = float(np.std(returns)) * np.sqrt(252)
                details["move"] = {"val": round(vol, 1), "proxy": True}

        # 4. TED Spread (BIL/SHV)
        ted_vals = []
        for d in prev_dates:
            b = bil.get(d)
            s = shv.get(d)
            if b and s and s > 0:
                ted_vals.append(b / s)

        if len(ted_vals) >= 4:
            diffs = [ted_vals[i+1] - ted_vals[i] for i in range(len(ted_vals)-1)]
            recent_3 = diffs[-3:] if len(diffs) >= 3 else diffs
            is_warning = len(recent_3) == 3 and all(d < 0 for d in recent_3)
            change_3d = ted_vals[-1] - ted_vals[-4] if len(ted_vals) >= 4 else 0
            is_worsening = change_3d < -0.001

            if is_warning:
                warnings += 1
                worsenings += 1
            elif is_worsening:
                worsenings += 1
            details["ted"] = {"val": round(ted_vals[-1], 4), "chg3d": round(change_3d, 4),
                               "warn": is_warning}

        # Signal
        if warnings >= 3:
            signal = "IMMINENT"
        elif warnings >= 2:
            signal = "IMMINENT"
        elif worsenings >= 3:
            signal = "WATCH"
        elif worsenings >= 2:
            signal = "WATCH"
        else:
            signal = "CLEAR"

        results.append({
            "date": date,
            "warning_count": warnings,
            "worsening_count": worsenings,
            "signal": signal,
            "details": details,
        })

    return results


# ═══════════════════════════════════════════════════
#  3D 크로스에셋 스트레스 일별 계산
# ═══════════════════════════════════════════════════

PAIRS_3D = {
    "gold_kospi": {"a": "GC=F", "b": "^KS11", "normal": -0.2, "crisis_thresh": 0.2, "weight": 2.5, "direction": "above"},
    "usd_kospi": {"a": "KRW=X", "b": "^KS11", "normal": -0.25, "crisis_thresh": 0.15, "weight": 2.0, "direction": "above"},
    "bond_kospi": {"a": "^TNX", "b": "^KS11", "normal": 0.1, "crisis_thresh": -0.3, "weight": 2.0, "direction": "below"},
    "oil_kospi": {"a": "CL=F", "b": "^KS11", "normal": 0.3, "crisis_thresh": -0.2, "weight": 1.5, "direction": "below"},
}


def calc_3d_daily(start: str, end: str) -> list:
    """3D 크로스에셋 스트레스를 일별로 계산.

    Returns list of {date, stress_index, stress_level, anomaly_count, pairs}
    """
    dt_start = datetime.strptime(start, "%Y-%m-%d") - timedelta(days=100)  # 60d corr 필요
    fetch_start = dt_start.strftime("%Y-%m-%d")

    # 데이터 다운로드
    price_data = {}
    symbols = set()
    for pdef in PAIRS_3D.values():
        symbols.add(pdef["a"])
        symbols.add(pdef["b"])

    for sym in symbols:
        price_data[sym] = fetch_daily(sym, fetch_start, end)

    # 공통 날짜
    kospi_dates = sorted(price_data.get("^KS11", {}).keys())
    analysis_dates = [d for d in kospi_dates if start <= d <= end]

    results = []
    for date in analysis_dates:
        anomaly_count = 0
        total_stress = 0.0
        pair_results = {}

        for pid, pdef in PAIRS_3D.items():
            prices_a = price_data.get(pdef["a"], {})
            prices_b = price_data.get(pdef["b"], {})

            # date까지의 최근 25일 수익률 (20d corr + 여유)
            prev_dates = [d for d in kospi_dates if d <= date][-25:]

            rets_a = []
            rets_b = []
            prev_a = None
            prev_b = None
            for d in prev_dates:
                a = prices_a.get(d)
                b = prices_b.get(d)
                if a and b and prev_a and prev_b and prev_a > 0 and prev_b > 0:
                    rets_a.append((a - prev_a) / prev_a * 100)
                    rets_b.append((b - prev_b) / prev_b * 100)
                prev_a = a if a else prev_a
                prev_b = b if b else prev_b

            if len(rets_a) < 15:
                pair_results[pid] = {"corr20d": 0.0, "anomaly": False}
                continue

            # 20d rolling correlation
            a_arr = np.array(rets_a[-20:])
            b_arr = np.array(rets_b[-20:])
            corr_mat = np.corrcoef(a_arr, b_arr)
            corr20 = float(corr_mat[0, 1]) if not np.isnan(corr_mat[0, 1]) else 0.0

            # Anomaly check
            if pdef["direction"] == "above":
                is_anomaly = corr20 > pdef["crisis_thresh"]
            else:
                is_anomaly = corr20 < pdef["crisis_thresh"]

            deviation = abs(corr20 - pdef["normal"]) / max(abs(pdef["crisis_thresh"] - pdef["normal"]), 0.1)
            deviation = min(deviation, 2.0)

            contrib = pdef["weight"] * min(deviation, 1.5) if is_anomaly else 0.0

            if is_anomaly:
                anomaly_count += 1
            total_stress += contrib

            pair_results[pid] = {
                "corr20d": round(corr20, 3),
                "anomaly": is_anomaly,
                "deviation": round(deviation, 2),
                "contrib": round(contrib, 2),
            }

        # Amplification
        if anomaly_count >= 3:
            total_stress *= 1.5
        elif anomaly_count >= 2:
            total_stress *= 1.2

        stress_index = round(min(total_stress, 10.0), 1)

        if stress_index >= 7.0:
            level = "CRITICAL"
        elif stress_index >= 5.0:
            level = "HIGH"
        elif stress_index >= 3.0:
            level = "ELEVATED"
        else:
            level = "NORMAL"

        results.append({
            "date": date,
            "stress_index": stress_index,
            "stress_level": level,
            "anomaly_count": anomaly_count,
            "pairs": pair_results,
        })

    return results


# ═══════════════════════════════════════════════════
#  이벤트 백테스트 실행
# ═══════════════════════════════════════════════════

EVENTS = [
    {"name": "엔 캐리 청산 쇼크", "date": "2024-08-05",
     "start": "2024-05-01", "end": "2024-08-31",
     "expect_2d": "신용스프레드+MOVE 선행", "expect_3d": "Gold↔KOSPI 상관 붕괴"},
    {"name": "SVB 은행 위기", "date": "2023-03-10",
     "start": "2022-12-01", "end": "2023-04-30",
     "expect_2d": "신용스프레드 급등 선행", "expect_3d": "다쌍 상관 붕괴"},
    {"name": "코로나 폭락", "date": "2020-03-23",
     "start": "2020-01-01", "end": "2020-04-30",
     "expect_2d": "전지표 IMMINENT", "expect_3d": "4쌍 동시 붕괴 (CRITICAL)"},
    {"name": "2022 긴축 사이클", "date": "2022-10-13",
     "start": "2022-06-01", "end": "2022-12-31",
     "expect_2d": "yield curve 역전 지속", "expect_3d": "Bond↔KOSPI 역상관 심화"},
    {"name": "2024 하반기 랠리", "date": "2024-12-31",
     "start": "2024-08-01", "end": "2024-12-31",
     "expect_2d": "CLEAR (호전)", "expect_3d": "NORMAL (정상 상관 복원)"},
]


def run_backtest():
    """전체 백테스트 실행"""
    report_lines = []
    report_lines.append("# BRAIN 2D/3D 과거 위기 백테스트 결과")
    report_lines.append(f"\n실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("\n주요 검증: 2D(채권 선행지표) + 3D(크로스에셋 상관 붕괴)")
    report_lines.append("각 위기 D-몇일에 경보가 발생했는지 선행일수 확정\n")

    summary = []

    for event in EVENTS:
        print(f"\n{'='*60}")
        print(f"  이벤트: {event['name']} ({event['date']})")
        print(f"{'='*60}")

        crisis_dt = datetime.strptime(event["date"], "%Y-%m-%d")

        report_lines.append(f"{'='*60}")
        report_lines.append(f"=== 이벤트: {event['name']} ===")
        report_lines.append(f"위기일: {event['date']}")
        report_lines.append(f"검증 구간: {event['start']} ~ {event['end']}")
        report_lines.append(f"예상 2D: {event['expect_2d']}")
        report_lines.append(f"예상 3D: {event['expect_3d']}")

        # ── 2D 백테스트 ──
        print("  2D 선행지표 계산 중...")
        data_2d = calc_2d_daily(event["start"], event["end"])

        report_lines.append(f"\n[2D 선행지표]")

        first_watch = None
        first_imminent = None
        max_warnings = 0

        for row in data_2d:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            d_day = (crisis_dt - dt).days

            if row["warning_count"] > max_warnings:
                max_warnings = row["warning_count"]

            if row["signal"] in ("WATCH", "IMMINENT") and not first_watch:
                first_watch = row["date"]

            if row["signal"] == "IMMINENT" and not first_imminent:
                first_imminent = row["date"]

        # 주요 날짜 출력 (위기 전후 2주)
        crisis_window = [r for r in data_2d
                        if abs((datetime.strptime(r["date"], "%Y-%m-%d") - crisis_dt).days) <= 14]

        for row in crisis_window:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            d_day = (crisis_dt - dt).days
            marker = " <<<" if d_day == 0 else ""
            det = row["details"]

            move_str = f"MOVE={det.get('move', {}).get('val', '?')}" if det.get("move") else "MOVE=N/A"
            credit_str = f"Credit={det.get('credit', {}).get('chg3d', '?')}" if det.get("credit") else "Credit=N/A"
            curve_str = f"Curve={det.get('curve', {}).get('val', '?')}" if det.get("curve") else "Curve=N/A"

            line = (f"  {row['date']} D{-d_day:+d}: [{row['signal']:9}]"
                   f" warn={row['warning_count']} worse={row['worsening_count']}"
                   f" | {credit_str} {move_str} {curve_str}{marker}")
            report_lines.append(line)
            print(line)

        # 2D 요약 (위기 전 경보만 유효)
        d_n_watch = (crisis_dt - datetime.strptime(first_watch, "%Y-%m-%d")).days if first_watch else None
        d_n_imminent = (crisis_dt - datetime.strptime(first_imminent, "%Y-%m-%d")).days if first_imminent else None
        if d_n_watch and d_n_watch <= 0:
            d_n_watch = None
        if d_n_imminent and d_n_imminent <= 0:
            d_n_imminent = None

        d_n = d_n_imminent or d_n_watch  # IMMINENT 우선

        if d_n_imminent:
            report_lines.append(f"  최초 IMMINENT: {first_imminent} (D-{d_n_imminent})")
            print(f"  → 최초 IMMINENT: {first_imminent} (D-{d_n_imminent})")
        if d_n_watch:
            report_lines.append(f"  최초 WATCH: {first_watch} (D-{d_n_watch})")
            print(f"  → 최초 WATCH: {first_watch} (D-{d_n_watch})")
        if d_n is None:
            report_lines.append("  위기 전 경보 없음")
            print("  → 2D 위기 전 경보 없음")

        report_lines.append(f"  최대 경고수: {max_warnings}/4")

        # ── 3D 백테스트 ──
        print("  3D 크로스에셋 계산 중...")
        data_3d = calc_3d_daily(event["start"], event["end"])

        report_lines.append(f"\n[3D 크로스에셋 스트레스]")

        first_elevated = None
        first_high = None
        first_critical = None
        max_stress = 0.0
        max_anomaly = 0

        for row in data_3d:
            if row["stress_index"] > max_stress:
                max_stress = row["stress_index"]
            if row["anomaly_count"] > max_anomaly:
                max_anomaly = row["anomaly_count"]

            if row["stress_level"] in ("ELEVATED", "HIGH", "CRITICAL") and not first_elevated:
                first_elevated = row["date"]
            if row["stress_level"] in ("HIGH", "CRITICAL") and not first_high:
                first_high = row["date"]
            if row["stress_level"] == "CRITICAL" and not first_critical:
                first_critical = row["date"]

        # 주요 날짜 출력
        crisis_window_3d = [r for r in data_3d
                           if abs((datetime.strptime(r["date"], "%Y-%m-%d") - crisis_dt).days) <= 14]

        for row in crisis_window_3d:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            d_day = (crisis_dt - dt).days
            marker = " <<<" if d_day == 0 else ""

            pairs_str = " ".join(
                f"{pid}={'⚠' if p.get('anomaly') else '✓'}({p.get('corr20d', 0):+.2f})"
                for pid, p in row["pairs"].items()
            )

            line = (f"  {row['date']} D{-d_day:+d}: [{row['stress_level']:9}]"
                   f" idx={row['stress_index']:4.1f} anom={row['anomaly_count']}/4"
                   f" | {pairs_str}{marker}")
            report_lines.append(line)
            print(line)

        # 3D 요약 (가장 먼저 뜬 레벨 기준 — ELEVATED가 항상 최초)
        # 위기 '전' 경보만 유효 (D>0)
        def d_n_before_crisis(date_str):
            if not date_str:
                return None
            d = (crisis_dt - datetime.strptime(date_str, "%Y-%m-%d")).days
            return d if d > 0 else None

        d_elev = d_n_before_crisis(first_elevated)
        d_high = d_n_before_crisis(first_high)
        d_crit = d_n_before_crisis(first_critical)

        # 최초 경보 = 가장 큰 D-N (= 가장 이른 날짜)
        d_n_3d = max(filter(None, [d_elev, d_high, d_crit]), default=None)

        if d_crit and d_crit > 0:
            report_lines.append(f"  최초 CRITICAL: {first_critical} (D-{d_crit})")
            print(f"  → 최초 CRITICAL: {first_critical} (D-{d_crit})")
        if d_high and d_high > 0:
            report_lines.append(f"  최초 HIGH: {first_high} (D-{d_high})")
            print(f"  → 최초 HIGH: {first_high} (D-{d_high})")
        if d_elev and d_elev > 0:
            report_lines.append(f"  최초 ELEVATED: {first_elevated} (D-{d_elev})")
            print(f"  → 최초 ELEVATED: {first_elevated} (D-{d_elev})")
        if d_n_3d is None:
            report_lines.append("  위기 전 경보 없음")
            print("  → 3D 위기 전 경보 없음")

        report_lines.append(f"  최대 스트레스: {max_stress:.1f}/10, 최대 이상쌍: {max_anomaly}/4")

        # 이벤트 종합
        report_lines.append(f"\n[종합]")
        dims = {}
        if d_n is not None and d_n > 0:
            dims["2D"] = d_n
        if d_n_3d is not None and d_n_3d > 0:
            dims["3D"] = d_n_3d

        if dims:
            fastest = min(dims.items(), key=lambda x: -x[1])
            report_lines.append(f"  가장 먼저 경고: {fastest[0]} (D-{fastest[1]})")
        else:
            report_lines.append("  2D/3D 모두 위기 전 경보 없음")

        # 3D 최고 레벨 (위기 전)
        if d_crit and d_crit > 0:
            d3_best_level = "CRITICAL"
        elif d_high and d_high > 0:
            d3_best_level = "HIGH"
        elif d_elev and d_elev > 0:
            d3_best_level = "ELEVATED"
        else:
            d3_best_level = "NORMAL"

        summary.append({
            "event": event["name"],
            "crisis": event["date"],
            "2d_first": first_watch or first_imminent,
            "2d_d_n": d_n,
            "2d_max_warn": max_warnings,
            "3d_d_n": d_n_3d,
            "3d_best_level": d3_best_level,
            "3d_max_stress": max_stress,
            "3d_max_anomaly": max_anomaly,
        })

        report_lines.append("")

    # ── 종합 요약 테이블 ──
    report_lines.append("\n" + "=" * 60)
    report_lines.append("종합 선행일수 매트릭스")
    report_lines.append("=" * 60)
    report_lines.append(f"\n{'이벤트':20} {'위기일':12} │ {'2D(D-N)':>10} {'2D레벨':>10} │ {'3D(D-N)':>10} {'3D레벨':>10} {'최대스트레스':>10}")
    report_lines.append("─" * 95)

    for s in summary:
        d2 = f"D-{s['2d_d_n']}" if s['2d_d_n'] and s['2d_d_n'] > 0 else "미감지"
        d2_lvl = f"warn={s['2d_max_warn']}/4" if s['2d_max_warn'] else "CLEAR"
        d3 = f"D-{s['3d_d_n']}" if s['3d_d_n'] and s['3d_d_n'] > 0 else "미감지"
        d3_lvl = s.get("3d_best_level", "NORMAL")
        stress = f"{s['3d_max_stress']:.1f}/10"

        report_lines.append(f"  {s['event']:20} {s['crisis']:12} │ {d2:>10} {d2_lvl:>10} │ {d3:>10} {d3_lvl:>10} {stress:>10}")

    report_lines.append("")

    # 보고서 저장
    report_text = "\n".join(report_lines)
    report_path = Path(__file__).resolve().parents[2] / "BRAIN_2D3D_BACKTEST_RESULTS.md"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\n보고서 저장: {report_path}")

    return report_text


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    run_backtest()
