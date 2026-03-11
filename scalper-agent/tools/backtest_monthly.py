"""
1달치 외국인 국적별 수급 → 주가 종합 백테스트
──────────────────────────────────────────────
기간: 2026/02/01 ~ 2026/03/11
대상: 주요 대형주 20종목

KRX 데이터 특성:
  ACC_TRDVOL = 국적별 거래량 (항상 양수, 순매수 아님)
  → "절대값"이 아닌 "상대 변화"로 분석

분석 방법론:
  1. 국가별 평균 거래량 대비 "급증(1.5x+)" vs "저조(<0.5x)" vs "부재(0)"
  2. 급증일 다음날 수익률 vs 저조/부재일 다음날 수익률
  3. 국가 조합 (영국급증+중국급증 동시) 효과
  4. 종목별 민감도

사용:
  cd scalper-agent
  python tools/backtest_monthly.py
"""

import json
import sys
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

# ── 경로 설정 ──
AGENT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(AGENT_DIR))
os.chdir(AGENT_DIR)

from dotenv import load_dotenv
load_dotenv(AGENT_DIR.parent / ".env")

from data.krx_nationality_crawler import (
    _get_valid_session, _fetch_nationality_http, REQUEST_INTERVAL,
)

DATA_STORE = AGENT_DIR / "data_store"
DAILY_DIR = DATA_STORE / "daily"
NAT_DIR = DATA_STORE / "nationality"
NAT_DIR.mkdir(parents=True, exist_ok=True)

# ── 분석 대상 종목 ──
TARGET_STOCKS = {
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "015760": "한국전력",
    "028260": "삼성물산",
    "035420": "NAVER",
    "035720": "카카오",
    "012330": "현대모비스",
    "086280": "현대글로비스",
    "051910": "LG화학",
    "005380": "현대차",
    "000270": "기아",
    "068270": "셀트리온",
    "105560": "KB금융",
    "055550": "신한지주",
    "006400": "삼성SDI",
    "373220": "LG에너지솔루션",
    "196170": "알테오젠",
    "218410": "RFHIC",
    "003670": "포스코퓨처엠",
    "207940": "삼성바이오로직스",
}

# 주요 국가
COUNTRIES = [
    "영국", "미국", "중국", "홍콩", "싱가포르", "일본",
    "노르웨이", "케이맨 제도", "스위스", "프랑스", "오스트레일리아",
]

# 국가 그룹
COUNTRY_GROUPS = {
    "기관": ["영국", "노르웨이", "스위스", "스웨덴", "아일랜드", "룩셈부르크", "덴마크"],
    "아시아": ["중국", "홍콩", "싱가포르", "일본", "대만"],
    "헤지펀드": ["케이맨 제도", "영국령 버진아일랜드", "버뮤다"],
    "북미": ["미국", "캐나다"],
}


def get_trading_days_from_csv(code="005930"):
    """삼성전자 CSV에서 실제 영업일 추출"""
    csv_path = DAILY_DIR / f"{code}.csv"
    df = pd.read_csv(csv_path, encoding="utf-8")
    dates = df["날짜"].tolist()
    filtered = [d for d in dates if "2026-02-01" <= d <= "2026-03-11"]
    return sorted(filtered)


def date_to_krx(date_str):
    return date_str.replace("-", "")


def fetch_nat_data(session, code, krx_date):
    """단일 종목/단일일 국적별 데이터 (캐시 우선)"""
    cache_path = NAT_DIR / f"{code}_{krx_date}.csv"
    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path, encoding="utf-8-sig")
            return dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
        except Exception:
            pass

    for mkt in ["STK", "KSQ"]:
        df = _fetch_nationality_http(session, code, krx_date, krx_date, mkt)
        if df is None:
            session = _get_valid_session()
            if session:
                df = _fetch_nationality_http(session, code, krx_date, krx_date, mkt)
        if df is not None and not df.empty:
            data = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            return data
        time.sleep(REQUEST_INTERVAL)
    return None


def get_price_change(code, date_str):
    csv_path = DAILY_DIR / f"{code}.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        row = df[df["날짜"] == date_str]
        if row.empty:
            return None
        return float(row.iloc[0]["등락률"])
    except Exception:
        return None


# ═══════════════════════════════════════════
# 메인 분석
# ═══════════════════════════════════════════

def run_monthly_backtest():
    print("=" * 70)
    print("  1달치 외국인 국적별 수급 → 주가 종합 백테스트 (v2)")
    print(f"  기간: 2026/02/01 ~ 2026/03/11")
    print(f"  대상: {len(TARGET_STOCKS)}개 종목")
    print("  방법: 상대 거래량 변화 (급증/저조/부재) 기반 분석")
    print("=" * 70)

    # 1) 영업일 산출
    trading_days = get_trading_days_from_csv()
    print(f"\n영업일 수: {len(trading_days)}일 ({trading_days[0]} ~ {trading_days[-1]})")

    # 2) KRX 세션
    print("\nKRX 세션 연결 중...")
    session = _get_valid_session()
    if not session:
        print("ERROR: KRX 세션 생성 실패")
        return

    # 3) 전체 데이터 수집 (캐시 활용)
    all_nat_data = {}  # {code: {date: {country: volume}}}
    total_queries = len(TARGET_STOCKS) * len(trading_days)
    done = 0
    new_fetches = 0

    print(f"\n데이터 로드 중 ({total_queries}건, 캐시 우선)...")
    for code, name in TARGET_STOCKS.items():
        all_nat_data[code] = {}
        for day in trading_days:
            krx_date = date_to_krx(day)
            cache_path = NAT_DIR / f"{code}_{krx_date}.csv"
            if not cache_path.exists():
                new_fetches += 1

            nat = fetch_nat_data(session, code, krx_date)
            if nat:
                all_nat_data[code][day] = nat
            done += 1
            if done % 100 == 0:
                print(f"  ... {done}/{total_queries}")
            time.sleep(REQUEST_INTERVAL * 0.2)

    print(f"  완료 (신규수집: {new_fetches}건)")

    # 4) 종목×국가별 평균 거래량 계산
    # {code: {country: avg_vol}}
    avg_vols = {}
    for code in TARGET_STOCKS:
        avg_vols[code] = {}
        country_totals = defaultdict(list)
        for day in trading_days:
            nat = all_nat_data.get(code, {}).get(day, {})
            for country in COUNTRIES:
                country_totals[country].append(nat.get(country, 0))

        for country, vols in country_totals.items():
            nonzero = [v for v in vols if v > 0]
            avg_vols[code][country] = np.mean(nonzero) if nonzero else 0

    # 5) 분석용 레코드 생성
    # 각 레코드: 종목, 날짜, 국가별 "활동 상태", 다음날 등락률
    records = []
    for code, name in TARGET_STOCKS.items():
        for i, day in enumerate(trading_days):
            if i + 1 >= len(trading_days):
                continue  # 마지막 날은 제외

            next_day = trading_days[i + 1]
            nat = all_nat_data.get(code, {}).get(day, {})
            if not nat:
                continue
            next_chg = get_price_change(code, next_day)
            if next_chg is None:
                continue

            record = {
                "code": code,
                "name": name,
                "buy_date": day,
                "next_date": next_day,
                "next_chg": next_chg,
                "total_foreign": sum(nat.values()),
                "nat": nat,
            }

            # 국가별 활동 상태: 급증/활발/보통/저조/부재
            for country in COUNTRIES:
                vol = nat.get(country, 0)
                avg = avg_vols[code].get(country, 0)
                if avg == 0:
                    ratio = 0
                else:
                    ratio = vol / avg

                record[f"vol_{country}"] = vol
                record[f"ratio_{country}"] = ratio
                # 상태 분류
                if vol == 0:
                    record[f"state_{country}"] = "부재"
                elif ratio >= 2.0:
                    record[f"state_{country}"] = "급증"
                elif ratio >= 1.2:
                    record[f"state_{country}"] = "활발"
                elif ratio >= 0.6:
                    record[f"state_{country}"] = "보통"
                else:
                    record[f"state_{country}"] = "저조"

            # 그룹별 합산 ratio
            for grp_name, grp_countries in COUNTRY_GROUPS.items():
                grp_vol = sum(nat.get(c, 0) for c in grp_countries)
                grp_avg = sum(avg_vols[code].get(c, 0) for c in grp_countries)
                grp_ratio = grp_vol / grp_avg if grp_avg > 0 else 0
                record[f"grp_vol_{grp_name}"] = grp_vol
                record[f"grp_ratio_{grp_name}"] = grp_ratio

            # 전일 대비 변화량 (있으면)
            prev_day = trading_days[i - 1] if i > 0 else None
            if prev_day:
                prev_nat = all_nat_data.get(code, {}).get(prev_day, {})
                for country in COUNTRIES:
                    prev_vol = prev_nat.get(country, 0)
                    curr_vol = nat.get(country, 0)
                    if prev_vol > 0:
                        record[f"dod_{country}"] = (curr_vol - prev_vol) / prev_vol
                    else:
                        record[f"dod_{country}"] = 1.0 if curr_vol > 0 else 0

            records.append(record)

    print(f"\n분석 레코드: {len(records)}건 (종목x일)")
    all_avg = np.mean([r["next_chg"] for r in records])
    all_hit = sum(1 for r in records if r["next_chg"] > 0) / len(records) * 100
    print(f"전체 평균 수익률: {all_avg:+.3f}%, 전체 적중률: {all_hit:.1f}%")

    # ═══════════════════════════════════════════
    # 분석 1: 국가별 급증 시 다음날 수익률
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 1: 국가별 '거래 급증(2x+)' 시 다음날 수익률")
    print("  (어느 나라가 거래 늘리면 다음날 주가가 오르는가?)")
    print("=" * 70)

    country_analysis = {}
    for country in COUNTRIES:
        surge = [r for r in records if r.get(f"state_{country}") == "급증"]
        active = [r for r in records if r.get(f"state_{country}") == "활발"]
        normal = [r for r in records if r.get(f"state_{country}") == "보통"]
        low = [r for r in records if r.get(f"state_{country}") == "저조"]
        absent = [r for r in records if r.get(f"state_{country}") == "부재"]

        if len(surge) < 5:
            continue

        surge_avg = np.mean([r["next_chg"] for r in surge])
        surge_hit = sum(1 for r in surge if r["next_chg"] > 0) / len(surge) * 100

        low_absent = low + absent
        low_avg = np.mean([r["next_chg"] for r in low_absent]) if low_absent else 0
        low_hit = (sum(1 for r in low_absent if r["next_chg"] > 0) / len(low_absent) * 100) if low_absent else 0

        normal_avg = np.mean([r["next_chg"] for r in normal]) if normal else 0

        # 급증 - 저조/부재 스프레드 = 핵심 지표
        spread = surge_avg - low_avg if low_absent else surge_avg - all_avg

        country_analysis[country] = {
            "surge_n": len(surge),
            "surge_avg": surge_avg,
            "surge_hit": surge_hit,
            "active_n": len(active),
            "normal_n": len(normal),
            "low_n": len(low_absent),
            "low_avg": low_avg,
            "low_hit": low_hit,
            "spread": spread,
        }

    sorted_ca = sorted(country_analysis.items(), key=lambda x: x[1]["spread"], reverse=True)

    print(f"\n{'국가':<12} {'급증일':>5} {'급증→수익':>10} {'급증적중':>7} {'저조일':>5} {'저조→수익':>10} {'저조적중':>7} {'스프레드':>8}")
    print("-" * 80)
    for country, st in sorted_ca:
        marker = " ★" if st["spread"] > 0.5 else ""
        print(f"{country:<12} {st['surge_n']:>5} {st['surge_avg']:>+9.2f}% {st['surge_hit']:>6.1f}% "
              f"{st['low_n']:>5} {st['low_avg']:>+9.2f}% {st['low_hit']:>6.1f}% "
              f"{st['spread']:>+7.2f}%{marker}")

    # ═══════════════════════════════════════════
    # 분석 2: 국가 그룹별 급증 효과
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 2: 국가 그룹별 급증 효과")
    print("=" * 70)

    for grp_name in ["기관", "아시아", "헤지펀드", "북미"]:
        ratio_key = f"grp_ratio_{grp_name}"
        surge = [r for r in records if r.get(ratio_key, 0) >= 1.5]
        low = [r for r in records if r.get(ratio_key, 0) < 0.7 and r.get(ratio_key, 0) > 0]

        if not surge:
            continue

        surge_avg = np.mean([r["next_chg"] for r in surge])
        surge_hit = sum(1 for r in surge if r["next_chg"] > 0) / len(surge) * 100
        low_avg = np.mean([r["next_chg"] for r in low]) if low else 0

        print(f"\n  {grp_name} ({', '.join(COUNTRY_GROUPS[grp_name][:3])}...)")
        print(f"    급증(1.5x+) {len(surge)}건 → 다음날 {surge_avg:+.2f}%, 적중률 {surge_hit:.1f}%")
        if low:
            print(f"    저조(<0.7x) {len(low)}건 → 다음날 {low_avg:+.2f}%")
            print(f"    스프레드: {surge_avg - low_avg:+.2f}%p")

    # ═══════════════════════════════════════════
    # 분석 3: 전일 대비 증가 vs 감소 효과
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 3: 전일 대비 증가 vs 감소 (Day-over-Day)")
    print("  (어제보다 오늘 거래가 많아졌을 때 내일 오르는가?)")
    print("=" * 70)

    dod_results = {}
    for country in COUNTRIES:
        dod_key = f"dod_{country}"
        has_dod = [r for r in records if dod_key in r]
        if len(has_dod) < 20:
            continue

        increased = [r for r in has_dod if r[dod_key] > 0.3]  # 30%+ 증가
        decreased = [r for r in has_dod if r[dod_key] < -0.3]  # 30%+ 감소

        if len(increased) < 5 or len(decreased) < 5:
            continue

        inc_avg = np.mean([r["next_chg"] for r in increased])
        inc_hit = sum(1 for r in increased if r["next_chg"] > 0) / len(increased) * 100
        dec_avg = np.mean([r["next_chg"] for r in decreased])
        dec_hit = sum(1 for r in decreased if r["next_chg"] > 0) / len(decreased) * 100

        dod_results[country] = {
            "inc_n": len(increased),
            "inc_avg": inc_avg,
            "inc_hit": inc_hit,
            "dec_n": len(decreased),
            "dec_avg": dec_avg,
            "dec_hit": dec_hit,
            "spread": inc_avg - dec_avg,
        }

    sorted_dod = sorted(dod_results.items(), key=lambda x: x[1]["spread"], reverse=True)
    print(f"\n{'국가':<12} {'증가일':>5} {'증가→수익':>10} {'증가적중':>7} {'감소일':>5} {'감소→수익':>10} {'감소적중':>7} {'스프레드':>8}")
    print("-" * 82)
    for country, st in sorted_dod:
        marker = " ★" if st["spread"] > 0.5 else ""
        print(f"{country:<12} {st['inc_n']:>5} {st['inc_avg']:>+9.2f}% {st['inc_hit']:>6.1f}% "
              f"{st['dec_n']:>5} {st['dec_avg']:>+9.2f}% {st['dec_hit']:>6.1f}% "
              f"{st['spread']:>+7.2f}%{marker}")

    # ═══════════════════════════════════════════
    # 분석 4: 국가 조합 급증 효과
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 4: 국가 조합 동시 급증 → 다음날 수익률")
    print("=" * 70)

    combos = [
        ("영국+중국", ["영국", "중국"]),
        ("영국+미국", ["영국", "미국"]),
        ("영국+케이맨", ["영국", "케이맨 제도"]),
        ("중국+홍콩", ["중국", "홍콩"]),
        ("중국+미국", ["중국", "미국"]),
        ("미국+일본", ["미국", "일본"]),
        ("영국+싱가포르", ["영국", "싱가포르"]),
        ("케이맨+중국", ["케이맨 제도", "중국"]),
        ("기관+아시아(그룹)", None),  # 그룹 기반
        ("기관+헤지(그룹)", None),
    ]

    combo_results = {}
    print(f"\n{'조합':<20} {'동시급증일':>8} {'평균수익':>9} {'적중률':>7} {'vs전체':>8}")
    print("-" * 60)

    for label, country_list in combos:
        if country_list:
            # 개별 국가 기반: 모두 급증(2x+)
            both_surge = [r for r in records
                          if all(r.get(f"state_{c}") == "급증" for c in country_list)]
        else:
            # 그룹 기반
            if "기관+아시아" in label:
                both_surge = [r for r in records
                              if r.get("grp_ratio_기관", 0) >= 1.5
                              and r.get("grp_ratio_아시아", 0) >= 1.5]
            elif "기관+헤지" in label:
                both_surge = [r for r in records
                              if r.get("grp_ratio_기관", 0) >= 1.5
                              and r.get("grp_ratio_헤지펀드", 0) >= 1.5]
            else:
                continue

        if len(both_surge) < 3:
            print(f"  {label:<18} {len(both_surge):>8}  (표본 부족)")
            continue

        avg = np.mean([r["next_chg"] for r in both_surge])
        hit = sum(1 for r in both_surge if r["next_chg"] > 0) / len(both_surge) * 100
        diff = avg - all_avg
        combo_results[label] = {"count": len(both_surge), "avg": avg, "hit": hit}

        marker = " ★★" if diff > 1.0 else (" ★" if diff > 0.3 else "")
        print(f"  {label:<18} {len(both_surge):>8} {avg:>+8.2f}% {hit:>6.1f}% {diff:>+7.2f}%{marker}")

    # ═══════════════════════════════════════════
    # 분석 5: 종목별 수급 민감도
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 5: 종목별 외국인 수급 민감도")
    print("  (어떤 종목이 외국인 활동에 가장 민감하게 반응하는가?)")
    print("=" * 70)

    stock_stats = {}
    for code, name in TARGET_STOCKS.items():
        stock_recs = [r for r in records if r["code"] == code]
        if len(stock_recs) < 10:
            continue

        # 전체 외국인 거래량 상위/하위 4분위
        total_vols = sorted([r["total_foreign"] for r in stock_recs])
        q1 = total_vols[len(total_vols) // 4]
        q3 = total_vols[3 * len(total_vols) // 4]

        heavy = [r for r in stock_recs if r["total_foreign"] >= q3]
        light = [r for r in stock_recs if r["total_foreign"] <= q1]

        if not heavy or not light:
            continue

        heavy_avg = np.mean([r["next_chg"] for r in heavy])
        heavy_hit = sum(1 for r in heavy if r["next_chg"] > 0) / len(heavy) * 100
        light_avg = np.mean([r["next_chg"] for r in light])
        light_hit = sum(1 for r in light if r["next_chg"] > 0) / len(light) * 100

        stock_stats[name] = {
            "code": code,
            "days": len(stock_recs),
            "heavy_n": len(heavy),
            "heavy_avg": heavy_avg,
            "heavy_hit": heavy_hit,
            "light_n": len(light),
            "light_avg": light_avg,
            "light_hit": light_hit,
            "spread": heavy_avg - light_avg,
        }

    sorted_stocks = sorted(stock_stats.items(), key=lambda x: x[1]["spread"], reverse=True)
    print(f"\n{'종목':<14} {'Q4(대량)':>7} {'대량→':>8} {'적중':>6} {'Q1(소량)':>7} {'소량→':>8} {'적중':>6} {'스프레드':>8}")
    print("-" * 78)
    for name, st in sorted_stocks:
        marker = " ★" if st["spread"] > 1.0 else ""
        print(f"{name:<14} {st['heavy_n']:>7} {st['heavy_avg']:>+7.2f}% {st['heavy_hit']:>5.1f}% "
              f"{st['light_n']:>7} {st['light_avg']:>+7.2f}% {st['light_hit']:>5.1f}% "
              f"{st['spread']:>+7.2f}%{marker}")

    # ═══════════════════════════════════════════
    # 분석 6: 상관계수 (거래 변화 vs 수익률)
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  분석 6: 국가별 거래량비율 vs 다음날 수익률 상관계수")
    print("=" * 70)

    changes = np.array([r["next_chg"] for r in records])
    corr_results = {}

    for country in COUNTRIES:
        # ratio 기반 상관
        ratios = np.array([r.get(f"ratio_{country}", 0) for r in records], dtype=float)
        if np.std(ratios) > 0:
            corr = np.corrcoef(ratios, changes)[0, 1]
            corr_results[country] = corr

    # 그룹
    for grp in COUNTRY_GROUPS:
        ratios = np.array([r.get(f"grp_ratio_{grp}", 0) for r in records], dtype=float)
        if np.std(ratios) > 0:
            corr = np.corrcoef(ratios, changes)[0, 1]
            corr_results[f"[{grp}]"] = corr

    sorted_corr = sorted(corr_results.items(), key=lambda x: x[1], reverse=True)
    print(f"\n{'국가/그룹':<18} {'상관계수':>10} {'강도'}")
    print("-" * 55)
    for name, corr in sorted_corr:
        if abs(corr) > 0.15:
            strength = "★★★ 유의미"
        elif abs(corr) > 0.10:
            strength = "★★ 약한 상관"
        elif abs(corr) > 0.05:
            strength = "★ 미약한 상관"
        else:
            strength = "- 무상관"
        sign = "+" if corr > 0 else "-"
        print(f"  {name:<16} {corr:>+9.4f}  {strength}")

    # ═══════════════════════════════════════════
    # 최종 종합 결론
    # ═══════════════════════════════════════════
    print("\n" + "=" * 70)
    print("  최종 종합 보고서")
    print("=" * 70)

    print(f"\n  기본 통계: {len(records)}건 분석, "
          f"전체 평균 {all_avg:+.3f}%, 전체 적중률 {all_hit:.1f}%")

    # 1) 급증 스프레드 1등 국가
    if sorted_ca:
        best = sorted_ca[0]
        print(f"\n  1. 급증 시 가장 강력한 국가: {best[0]}")
        print(f"     급증(2x+) → 다음날 {best[1]['surge_avg']:+.2f}% (적중률 {best[1]['surge_hit']:.0f}%)")
        print(f"     저조/부재 → 다음날 {best[1]['low_avg']:+.2f}%")
        print(f"     스프레드: {best[1]['spread']:+.2f}%p")

    if len(sorted_ca) >= 2:
        second = sorted_ca[1]
        print(f"\n  2. 2위 국가: {second[0]}")
        print(f"     스프레드: {second[1]['spread']:+.2f}%p, "
              f"급증 적중률: {second[1]['surge_hit']:.0f}%")

    if len(sorted_ca) >= 3:
        third = sorted_ca[2]
        print(f"\n  3. 3위 국가: {third[0]}")
        print(f"     스프레드: {third[1]['spread']:+.2f}%p")

    # 3) DoD 1등
    if sorted_dod:
        best_dod = sorted_dod[0]
        print(f"\n  4. 전일 대비 증가 효과 1위: {best_dod[0]}")
        print(f"     거래 30%+ 증가 → 다음날 {best_dod[1]['inc_avg']:+.2f}% "
              f"(적중률 {best_dod[1]['inc_hit']:.0f}%)")
        print(f"     거래 30%+ 감소 → 다음날 {best_dod[1]['dec_avg']:+.2f}%")
        print(f"     스프레드: {best_dod[1]['spread']:+.2f}%p")

    # 4) 최강 조합
    if combo_results:
        best_combo = max(combo_results.items(), key=lambda x: x[1]["avg"])
        print(f"\n  5. 최강 조합: {best_combo[0]}")
        print(f"     동시 급증 시 다음날 {best_combo[1]['avg']:+.2f}% "
              f"(적중률 {best_combo[1]['hit']:.0f}%, {best_combo[1]['count']}건)")

    # 5) 수급 민감 종목
    if sorted_stocks:
        top3 = sorted_stocks[:3]
        print(f"\n  6. 수급에 가장 민감한 종목 TOP 3:")
        for name, st in top3:
            print(f"     {name}: 스프레드 {st['spread']:+.2f}%p "
                  f"(대량→{st['heavy_avg']:+.2f}%, 소량→{st['light_avg']:+.2f}%)")

    # 6) 상관계수 TOP 3
    if sorted_corr:
        print(f"\n  7. 상관계수 TOP 3:")
        for name, corr in sorted_corr[:3]:
            print(f"     {name}: {corr:+.4f}")

    # 실전 가이드
    print("\n" + "-" * 70)
    print("  실전 투자 가이드")
    print("-" * 70)

    if sorted_ca:
        best_country = sorted_ca[0][0]
        print(f"\n  [1] {best_country} 거래량이 평소 2배 이상 급증하면 → 매수 신호")

    if len(sorted_ca) >= 2:
        print(f"  [2] {sorted_ca[1][0]}도 동시 급증 시 → 더 강한 신호")

    if combo_results:
        best_combo_name = max(combo_results.items(), key=lambda x: x[1]["avg"])[0]
        print(f"  [3] {best_combo_name} 동시 급증 = 가장 강력한 진입 시그널")

    if sorted_stocks:
        print(f"  [4] 수급 민감종목 ({sorted_stocks[0][0]}, {sorted_stocks[1][0] if len(sorted_stocks)>1 else ''})")
        print(f"      → 이 종목들은 외국인 수급 변화에 가장 크게 반응")

    worst_country = sorted_ca[-1][0] if sorted_ca else "?"
    print(f"  [5] {worst_country} 거래량 변화는 다음날 주가에 거의 영향 없음 → 무시 가능")

    print(f"\n  ★ 결론: 외국인 수급은 '방향성 참고'(적중률 ~{all_hit:.0f}%)로 유용하며,")
    print(f"    특히 '{best_country if sorted_ca else '?'}'의 급증이 가장 예측력이 높습니다.")


if __name__ == "__main__":
    run_monthly_backtest()
