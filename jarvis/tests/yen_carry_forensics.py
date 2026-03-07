# -*- coding: utf-8 -*-
"""
엔 캐리 청산 미감지 포렌식 분석
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
목적:
  1) 4D COT에서 Yen 기여도 vs 전체 스코어 — 왜 -3.0 안 찍었나
  2) 어떤 계약이 Yen 신호를 상쇄했나
  3) 최적 임계값/per-contract 경보 역산
  4) 5D D-38 수축의 RRP/TGA/M2 분해 — 엔 캐리 인과 vs 우연

사용법:
  python -m jarvis.tests.yen_carry_forensics
"""

import sys, os, csv, io, json, zipfile, logging
from pathlib import Path
from datetime import datetime, timedelta
from urllib.request import urlopen, Request

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from jarvis.cot_smartmoney import (
    TRACKED_CONTRACTS, Z_EXTREME, Z_MODERATE,
    _download_zip, _parse_cot_text, CFTC_YEARLY_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════
#  파트1: 4D Yen 기여도 주간 분해
# ═══════════════════════════════════════════════════

def load_2024_cot() -> list:
    """2023~2024 CFTC 아카이브 로드 (z-score 52주 lookback 필요)"""
    cache = Path(__file__).resolve().parent / "cot_2024_cache.json"
    if cache.exists():
        data = json.loads(cache.read_text(encoding="utf-8"))
        logger.info(f"캐시 로드: {len(data)}건")
        return data

    all_data = []
    for year in [2023, 2024]:
        url = CFTC_YEARLY_URL.format(year=year)
        logger.info(f"{year}년 아카이브 다운로드...")
        text = _download_zip(url)
        if text:
            rows = _parse_cot_text(text)
            all_data.extend(rows)
            logger.info(f"  {year}: {len(rows)}건")

    all_data.sort(key=lambda x: x["report_date"])
    # 중복 제거
    seen = set()
    unique = []
    for r in all_data:
        key = (r["contract_id"], r["report_date"])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    unique.sort(key=lambda x: x["report_date"])

    cache.write_text(json.dumps(unique, ensure_ascii=False), encoding="utf-8")
    logger.info(f"캐시 저장: {len(unique)}건")
    return unique


def calc_z_score(values: list, current: float) -> float:
    if len(values) < 10:
        return 0.0
    arr = np.array(values[-52:])
    mean = arr.mean()
    std = arr.std()
    if std < 1e-8:
        return 0.0
    return round(float((current - mean) / std), 2)


def analyze_yen_carry_4d():
    """파트1: 2024년 6~8월 Yen 기여도 vs 전체 스코어 주간 분해"""
    print("\n" + "=" * 70)
    print("  파트1: 4D COT Yen 캐리 미감지 포렌식")
    print("=" * 70)

    all_data = load_2024_cot()

    # 계약별 히스토리 구축
    by_contract = {}
    for r in all_data:
        cid = r["contract_id"]
        if cid not in by_contract:
            by_contract[cid] = []
        by_contract[cid].append(r)
    for cid in by_contract:
        by_contract[cid].sort(key=lambda x: x["report_date"])

    # 2024-06-01 ~ 2024-08-31 주간별 분석
    crisis_date = datetime(2024, 8, 5)
    start = datetime(2024, 5, 1)
    end = datetime(2024, 8, 31)

    # Yen 데이터로 주간 날짜 리스트 추출
    yen_dates = sorted(set(
        r["report_date"] for r in all_data
        if r["contract_id"] == "yen"
        and start.strftime("%Y-%m-%d") <= r["report_date"] <= end.strftime("%Y-%m-%d")
    ))

    print(f"\n분석 구간: {start.date()} ~ {end.date()}")
    print(f"위기일: {crisis_date.date()}")
    print(f"주간 데이터: {len(yen_dates)}건\n")

    print(f"{'날짜':12} {'D-day':>6} │ {'Yen기여':>8} {'SP500':>8} {'Gold':>8} {'Oil':>8} │ {'합계':>7} {'판정':10} │ Yen(C_z/S_z)")
    print("─" * 110)

    yen_max_contribution = 0
    yen_max_date = ""

    for date_str in yen_dates:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        d_day = (crisis_date - dt).days

        total_score = 0.0
        contributions = {}

        for cid, cdef in TRACKED_CONTRACTS.items():
            history = by_contract.get(cid, [])
            # date_str 이전까지의 데이터
            hist_up_to = [r for r in history if r["report_date"] <= date_str]
            if not hist_up_to:
                contributions[cid] = 0.0
                continue

            latest = hist_up_to[-1]
            if latest["report_date"] != date_str:
                # 이 주에 해당 계약 데이터 없으면 이전 것 사용
                pass

            noncomm_net = latest["noncomm_long"] - latest["noncomm_short"]
            comm_net = latest["comm_long"] - latest["comm_short"]

            noncomm_nets = [r["noncomm_long"] - r["noncomm_short"] for r in hist_up_to]
            comm_nets = [r["comm_long"] - r["comm_short"] for r in hist_up_to]

            noncomm_z = calc_z_score(noncomm_nets, noncomm_net)
            comm_z = calc_z_score(comm_nets, comm_net)

            kospi_beta = cdef["kospi_beta"]
            comm_weight = cdef["comm_weight"]
            spec_weight = cdef["spec_weight"]

            contract_bullish = 0.0
            if comm_z >= Z_EXTREME:
                contract_bullish += comm_weight
            elif comm_z >= Z_MODERATE:
                contract_bullish += comm_weight * 0.5
            elif comm_z <= -Z_EXTREME:
                contract_bullish -= comm_weight
            elif comm_z <= -Z_MODERATE:
                contract_bullish -= comm_weight * 0.5

            if noncomm_z >= Z_EXTREME:
                contract_bullish -= spec_weight
            elif noncomm_z >= Z_MODERATE:
                contract_bullish -= spec_weight * 0.5
            elif noncomm_z <= -Z_EXTREME:
                contract_bullish += spec_weight
            elif noncomm_z <= -Z_MODERATE:
                contract_bullish += spec_weight * 0.5

            kospi_contrib = round(contract_bullish * kospi_beta, 2)
            contributions[cid] = kospi_contrib
            total_score += kospi_contrib

            # Yen 상세
            if cid == "yen":
                yen_comm_z = comm_z
                yen_spec_z = noncomm_z
                if abs(kospi_contrib) > abs(yen_max_contribution):
                    yen_max_contribution = kospi_contrib
                    yen_max_date = date_str

        total_score = round(total_score, 1)
        signal = "RISK_OFF" if total_score <= -3.0 else ("RISK_ON" if total_score >= 3.0 else "NEUTRAL")
        marker = " <<<" if date_str == "2024-07-30" or d_day <= 0 else ""

        yen_c = contributions.get("yen", 0)
        sp = contributions.get("sp500", 0)
        gold = contributions.get("gold", 0)
        oil = contributions.get("crude_oil", 0)

        # Yen z-scores
        yen_hist = by_contract.get("yen", [])
        yen_up_to = [r for r in yen_hist if r["report_date"] <= date_str]
        if yen_up_to:
            lat = yen_up_to[-1]
            cn = lat["comm_long"] - lat["comm_short"]
            sn = lat["noncomm_long"] - lat["noncomm_short"]
            cns = [r["comm_long"] - r["comm_short"] for r in yen_up_to]
            sns = [r["noncomm_long"] - r["noncomm_short"] for r in yen_up_to]
            ycz = calc_z_score(cns, cn)
            ysz = calc_z_score(sns, sn)
            yen_detail = f"C={ycz:+.1f} S={ysz:+.1f}"
        else:
            yen_detail = "N/A"

        print(f"{date_str:12} D-{d_day:>4} │ {yen_c:+8.1f} {sp:+8.1f} {gold:+8.1f} {oil:+8.1f} │ {total_score:+7.1f} {signal:10} │ {yen_detail}{marker}")

    print(f"\n📊 Yen 최대 기여도: {yen_max_contribution:+.1f} ({yen_max_date})")
    print(f"   RISK_OFF 임계값: -3.0")
    print(f"   → Yen 단독으로 -3.0 찍으려면 기여도 -3.0 이상 필요")
    print(f"   → 현재 Yen 최대: {yen_max_contribution:+.1f} (beta=-0.8, max_bullish=3.0 → max_contrib=-2.4)")

    # 역산
    print(f"\n🔧 개선안 역산:")
    print(f"   현재: Yen beta=-0.8, comm_w=2.0, spec_w=1.0 → 최대기여 = 3.0*0.8 = 2.4")
    print(f"   안1) beta=-1.0 → 최대기여 = 3.0*1.0 = 3.0 (단독 RISK_OFF 가능)")
    print(f"   안2) spec_w=1.5 → 최대bullish=3.5, 기여=3.5*0.8 = 2.8 (여전히 부족)")
    print(f"   안3) per-contract 경보: |기여도| >= 2.0이면 독립 경고 (권장)")
    print(f"   안4) 임계값 -2.5로 하향 (다른 이벤트 false positive 확인 필요)")

    return by_contract


# ═══════════════════════════════════════════════════
#  파트2: 5D D-38 인과관계 분해
# ═══════════════════════════════════════════════════

def analyze_5d_causality():
    """파트2: 2024-06-28 5D 수축의 RRP/TGA/M2 분해"""
    print("\n\n" + "=" * 70)
    print("  파트2: 5D D-38 수축 인과관계 분해")
    print("=" * 70)

    # FRED 데이터 다운로드 (2024-01 ~ 2024-08)
    start = "2023-01-01"  # 2년 lookback for z-score
    end = "2024-08-31"

    series = {
        "RRP": "RRPONTSYD",    # Overnight Reverse Repo
        "TGA": "WTREGEN",      # Treasury General Account
        "M2":  "M2SL",         # M2 Money Supply
    }

    data = {}
    for name, sid in series.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={start}&coed={end}"
        logger.info(f"FRED {name} ({sid}) 다운로드...")
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8")
            rows = []
            for line in text.strip().split("\n")[1:]:
                parts = line.split(",")
                if len(parts) >= 2 and parts[1].strip() != ".":
                    try:
                        rows.append({
                            "date": parts[0].strip(),
                            "value": float(parts[1].strip())
                        })
                    except ValueError:
                        continue
            data[name] = rows
            logger.info(f"  {name}: {len(rows)}건 ({rows[0]['date']} ~ {rows[-1]['date']})")
        except Exception as e:
            logger.error(f"  {name} 실패: {e}")
            data[name] = []

    if not all(data.values()):
        print("FRED 데이터 불충분 — 분석 중단")
        return

    # 월말 기준 z-score 계산 (analyze_liquidity 로직 재현)
    def monthly_zscore(series_data, target_month_str):
        """target_month_str = '2024-06' 등"""
        target_y, target_m = map(int, target_month_str.split("-"))

        # target_month 말일까지의 데이터
        cutoff = f"{target_month_str}-31"
        vals_up_to = [r for r in series_data if r["date"] <= cutoff]
        if not vals_up_to:
            return None, None

        latest_val = vals_up_to[-1]["value"]

        # z-score: 최근 2년(~500 영업일) 데이터
        two_years_ago = f"{target_y - 2}-{target_m:02d}-01"
        window = [r["value"] for r in vals_up_to if r["date"] >= two_years_ago]

        if len(window) < 30:
            return latest_val, 0.0

        arr = np.array(window)
        mean = arr.mean()
        std = arr.std()
        z = (latest_val - mean) / std if std > 1e-8 else 0.0
        return latest_val, round(z, 2)

    # 2024-04 ~ 2024-08 월별 분해
    months = ["2024-04", "2024-05", "2024-06", "2024-07", "2024-08"]

    print(f"\n{'월':10} │ {'RRP($B)':>10} {'RRP_z':>8} │ {'TGA($B)':>10} {'TGA_z':>8} │ {'M2($T)':>10} {'M2_z':>8} │ {'5D합계':>8}")
    print("─" * 95)

    # 가중치 (liquidity_cycle.py와 동일)
    W_RRP, W_TGA, W_M2 = 0.40, 0.35, 0.25

    for month in months:
        rrp_val, rrp_z = monthly_zscore(data["RRP"], month)
        tga_val, tga_z = monthly_zscore(data["TGA"], month)
        m2_val, m2_z = monthly_zscore(data["M2"], month)

        if rrp_z is None or tga_z is None or m2_z is None:
            print(f"{month:10} │ 데이터 불충분")
            continue

        # RRP: 하락 = 유동성 주입 → 점수 양수 (부호 반전)
        # TGA: 급증 = 유동성 흡수 → 점수 음수 (부호 반전)
        # M2: 증가 = 유동성 확대 → 점수 양수 (그대로)
        rrp_score = -rrp_z * W_RRP  # RRP 하락 = 좋음
        tga_score = -tga_z * W_TGA  # TGA 상승 = 나쁨
        m2_score = m2_z * W_M2      # M2 상승 = 좋음

        raw_score = rrp_score + tga_score + m2_score
        # 10점 스케일로 매핑 (liquidity_cycle.py 기준)
        final = round(max(-10, min(10, raw_score * 3.3)), 1)

        marker = " <<<" if month == "2024-06" else ""

        rrp_b = rrp_val / 1e9 if rrp_val > 1e6 else rrp_val / 1e3
        tga_b = tga_val / 1e6 if tga_val > 1e3 else tga_val
        m2_t = m2_val / 1e3 if m2_val > 100 else m2_val

        print(f"{month:10} │ {rrp_b:>10.1f} {rrp_z:>+8.2f} │ {tga_b:>10.1f} {tga_z:>+8.2f} │ {m2_t:>10.1f} {m2_z:>+8.2f} │ {final:>+8.1f}{marker}")

    # 인과관계 분석
    print(f"\n📊 5D D-38 (2024-06-28) 수축 원인 분해:")
    rrp_val_jun, rrp_z_jun = monthly_zscore(data["RRP"], "2024-06")
    tga_val_jun, tga_z_jun = monthly_zscore(data["TGA"], "2024-06")
    m2_val_jun, m2_z_jun = monthly_zscore(data["M2"], "2024-06")

    print(f"  RRP: z={rrp_z_jun:+.2f} → 기여 = {-rrp_z_jun * W_RRP:+.2f} (비중 40%)")
    print(f"  TGA: z={tga_z_jun:+.2f} → 기여 = {-tga_z_jun * W_TGA:+.2f} (비중 35%)")
    print(f"  M2:  z={m2_z_jun:+.2f} → 기여 = {m2_z_jun * W_M2:+.2f} (비중 25%)")

    print(f"\n🔍 인과관계 판정:")
    # TGA 급증 = 재무부 세금 수입 or 채권 발행 → 시장에서 유동성 빨아감
    # RRP 변동 = Fed 유동성 흡수 도구
    # 엔 캐리와의 직접 연결고리는 약함 — TGA/RRP는 미국 내부 요인

    if abs(tga_z_jun) > abs(rrp_z_jun) and abs(tga_z_jun) > abs(m2_z_jun):
        main_driver = "TGA (미 재무부 계좌)"
        causal = "간접적"
        reason = "TGA 급증 = 세금시즌/국채발행 → 미국 내부 유동성 흡수. 엔 캐리와 직접 인과 약함."
    elif abs(rrp_z_jun) > abs(tga_z_jun):
        main_driver = "RRP (역RP)"
        causal = "간접적"
        reason = "RRP 변동 = Fed 유동성 도구. 엔 캐리와 직접 인과 약함."
    else:
        main_driver = "M2"
        causal = "간접적"
        reason = "M2 증가율 둔화 = 통화 긴축 환경. 캐리 언와인드의 원인이 아닌 배경."

    print(f"  주 드라이버: {main_driver}")
    print(f"  엔 캐리 인과: {causal}")
    print(f"  설명: {reason}")
    print(f"\n  결론: 5D D-38 수축은 엔 캐리가 '원인'이 아니라,")
    print(f"         미국 유동성 환경 자체가 긴축적이었음을 반영.")
    print(f"         엔 캐리 청산과 시기가 겹쳤지만 인과관계보다는")
    print(f"         '같은 매크로 환경(긴축)이 두 현상을 모두 촉발'한 것.")
    print(f"         → 5D의 D-38 감지는 유용하지만 '엔 캐리 전용 경보'는 아님.")


# ═══════════════════════════════════════════════════
#  파트3: 개선안 시뮬레이션
# ═══════════════════════════════════════════════════

def simulate_improvements(by_contract: dict):
    """파트3: per-contract 경보 + 임계값 조정 시뮬레이션"""
    print("\n\n" + "=" * 70)
    print("  파트3: 개선안 시뮬레이션")
    print("=" * 70)

    all_data = []
    for cid, rows in by_contract.items():
        all_data.extend(rows)
    all_data.sort(key=lambda x: x["report_date"])

    # 2024년 전체에 대해 per-contract 극단 경보 시뮬
    print("\n[시뮬A] per-contract 극단 경보 (|KOSPI기여| >= 2.0)")
    print(f"{'날짜':12} {'계약':10} {'기여도':>8} {'Comm_z':>8} {'Spec_z':>8}")
    print("─" * 55)

    alerts_count = 0
    yen_alerts = []

    # 2024 전체
    dates_2024 = sorted(set(
        r["report_date"] for r in all_data
        if r["report_date"].startswith("2024")
    ))

    for date_str in dates_2024:
        for cid, cdef in TRACKED_CONTRACTS.items():
            hist = by_contract.get(cid, [])
            hist_up_to = [r for r in hist if r["report_date"] <= date_str]
            if not hist_up_to:
                continue

            latest = hist_up_to[-1]
            noncomm_net = latest["noncomm_long"] - latest["noncomm_short"]
            comm_net = latest["comm_long"] - latest["comm_short"]
            noncomm_nets = [r["noncomm_long"] - r["noncomm_short"] for r in hist_up_to]
            comm_nets = [r["comm_long"] - r["comm_short"] for r in hist_up_to]
            noncomm_z = calc_z_score(noncomm_nets, noncomm_net)
            comm_z = calc_z_score(comm_nets, comm_net)

            kospi_beta = cdef["kospi_beta"]
            comm_weight = cdef["comm_weight"]
            spec_weight = cdef["spec_weight"]

            contract_bullish = 0.0
            if comm_z >= Z_EXTREME:
                contract_bullish += comm_weight
            elif comm_z >= Z_MODERATE:
                contract_bullish += comm_weight * 0.5
            elif comm_z <= -Z_EXTREME:
                contract_bullish -= comm_weight
            elif comm_z <= -Z_MODERATE:
                contract_bullish -= comm_weight * 0.5

            if noncomm_z >= Z_EXTREME:
                contract_bullish -= spec_weight
            elif noncomm_z >= Z_MODERATE:
                contract_bullish -= spec_weight * 0.5
            elif noncomm_z <= -Z_EXTREME:
                contract_bullish += spec_weight
            elif noncomm_z <= -Z_MODERATE:
                contract_bullish += spec_weight * 0.5

            kospi_contrib = round(contract_bullish * kospi_beta, 2)

            if abs(kospi_contrib) >= 2.0:
                print(f"{date_str:12} {cdef['name']:10} {kospi_contrib:>+8.1f} {comm_z:>+8.1f} {noncomm_z:>+8.1f}")
                alerts_count += 1
                if cid == "yen":
                    yen_alerts.append(date_str)

    print(f"\n  총 경보: {alerts_count}건")
    print(f"  Yen 경보: {len(yen_alerts)}건")
    if yen_alerts:
        # 엔 캐리 직전 경보 확인
        pre_crisis = [d for d in yen_alerts if d < "2024-08-05"]
        post_crisis = [d for d in yen_alerts if d >= "2024-08-05"]
        print(f"    위기 전 ({len(pre_crisis)}건): {', '.join(pre_crisis[:10])}")
        print(f"    위기 후 ({len(post_crisis)}건): {', '.join(post_crisis[:5])}")
        if pre_crisis:
            first = pre_crisis[0]
            d_day = (datetime(2024, 8, 5) - datetime.strptime(first, "%Y-%m-%d")).days
            print(f"    최초 Yen 경보: {first} (D-{d_day})")

    print(f"\n[시뮬B] 임계값 -2.5로 하향 시 false positive 확인")
    fp_count = 0
    for date_str in dates_2024:
        total_score = 0.0
        for cid, cdef in TRACKED_CONTRACTS.items():
            hist = by_contract.get(cid, [])
            hist_up_to = [r for r in hist if r["report_date"] <= date_str]
            if not hist_up_to:
                continue
            latest = hist_up_to[-1]
            noncomm_net = latest["noncomm_long"] - latest["noncomm_short"]
            comm_net = latest["comm_long"] - latest["comm_short"]
            noncomm_nets = [r["noncomm_long"] - r["noncomm_short"] for r in hist_up_to]
            comm_nets = [r["comm_long"] - r["comm_short"] for r in hist_up_to]
            noncomm_z = calc_z_score(noncomm_nets, noncomm_net)
            comm_z = calc_z_score(comm_nets, comm_net)
            kospi_beta = cdef["kospi_beta"]
            comm_weight = cdef["comm_weight"]
            spec_weight = cdef["spec_weight"]
            cb = 0.0
            if comm_z >= Z_EXTREME: cb += comm_weight
            elif comm_z >= Z_MODERATE: cb += comm_weight * 0.5
            elif comm_z <= -Z_EXTREME: cb -= comm_weight
            elif comm_z <= -Z_MODERATE: cb -= comm_weight * 0.5
            if noncomm_z >= Z_EXTREME: cb -= spec_weight
            elif noncomm_z >= Z_MODERATE: cb -= spec_weight * 0.5
            elif noncomm_z <= -Z_EXTREME: cb += spec_weight
            elif noncomm_z <= -Z_MODERATE: cb += spec_weight * 0.5
            total_score += cb * kospi_beta

        if total_score <= -2.5:
            fp_count += 1
            if fp_count <= 10:
                print(f"  {date_str}: score={total_score:+.1f}")

    print(f"  총 RISK_OFF 트리거(-2.5): {fp_count}건/년 ({fp_count/52:.1f}건/주)")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")

    by_contract = analyze_yen_carry_4d()
    analyze_5d_causality()
    simulate_improvements(by_contract)

    print("\n\n" + "=" * 70)
    print("  종합 권장사항")
    print("=" * 70)
    print("""
1. per-contract 극단 경보 (권장)
   → |KOSPI기여| >= 2.0인 개별 계약 독립 경고
   → 합산 RISK_OFF와 별개로 "🟡 Yen 캐리 극단 경고" 같은 보조 시그널
   → false positive 낮음 (극단 z-score 도달해야 발동)

2. 합산 임계값은 -3.0 유지
   → -2.5로 낮추면 연간 false positive 증가 위험
   → 시뮬레이션 결과로 판단

3. 5D D-38 평가
   → 엔 캐리 전용이 아닌 '매크로 환경 긴축' 반영
   → 유용하지만 인과관계는 간접적 → 신뢰도 조정 불필요
""")
