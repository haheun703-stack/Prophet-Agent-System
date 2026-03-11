"""
외국인 수급 → 다음날 주가 백테스트
─────────────────────────────────────
3/10(월) 외국인 매수 vs 3/11(화) 실제 주가 변동 비교

질문: "수급이 좋으면 무조건 오르나?"
→ 데이터로 검증

사용:
  cd scalper-agent
  python tools/backtest_nationality.py
"""

import json
import sys
import time
import os
from pathlib import Path
from datetime import datetime

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

# ── 설정 ──
BUY_DATE = "20260310"   # 외국인 매수 기준일
CHK_DATE = "2026-03-11" # 주가 확인일 (CSV 포맷)
BUY_DATE_DISP = "3/10(월)"
CHK_DATE_DISP = "3/11(화)"


def load_universe_top(n=30):
    """시가총액 상위 N종목 + WAR TOP 8 + 관심종목"""
    uni_path = DATA_STORE / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    # 시총 순 정렬
    sorted_items = sorted(
        universe.items(),
        key=lambda x: x[1].get("cap_억", 0),
        reverse=True,
    )

    # TOP N
    codes_dict = {}
    for code, info in sorted_items[:n]:
        if code.endswith("5"):  # 우선주 제외
            continue
        codes_dict[code] = info.get("name", code)

    # WAR TOP 8 + 관심종목 추가
    extra = {
        "015760": "한국전력",
        "028260": "삼성물산",
        "035420": "NAVER",
        "035720": "카카오",
        "012330": "현대모비스",
        "086280": "현대글로비스",
        "000660": "SK하이닉스",
        "051910": "LG화학",
        "005930": "삼성전자",
        "068270": "셀트리온",
        "055550": "신한지주",
        "105560": "KB금융",
        "196170": "알테오젠",
        "218410": "RFHIC",
        "003670": "포스코퓨처엠",
        "006400": "삼성SDI",
        "373220": "LG에너지솔루션",
        "000270": "기아",
        "005380": "현대차",
        "207940": "삼성바이오로직스",
    }
    codes_dict.update(extra)

    return codes_dict


def get_nationality_data(session, code, date, name):
    """특정 종목의 특정일 외국인 국적별 데이터 수집"""
    # 캐시 확인
    cache_path = NAT_DIR / f"{code}_{date}.csv"
    if cache_path.exists():
        import pandas as pd
        try:
            df = pd.read_csv(cache_path, encoding="utf-8-sig")
            return dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
        except Exception:
            pass

    # KRX API 호출
    for mkt in ["STK", "KSQ"]:
        df = _fetch_nationality_http(session, code, date, date, mkt)
        if df is None:
            # 세션 재생성
            session = _get_valid_session()
            if session:
                df = _fetch_nationality_http(session, code, date, date, mkt)
        if df is not None and not df.empty:
            data = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            return data
        time.sleep(REQUEST_INTERVAL)

    return None


def get_price_change(code, date_str):
    """일별 CSV에서 특정일 등락률 추출"""
    csv_path = DAILY_DIR / f"{code}.csv"
    if not csv_path.exists():
        return None, None, None

    import pandas as pd
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        row = df[df["날짜"] == date_str]
        if row.empty:
            return None, None, None
        close = int(row.iloc[0]["종가"])
        chg = float(row.iloc[0]["등락률"])
        vol = int(row.iloc[0]["거래량"])
        return close, chg, vol
    except Exception as e:
        return None, None, None


def run_backtest():
    """메인 백테스트"""
    print("=" * 60)
    print(f"  외국인 수급 → 주가 백테스트")
    print(f"  {BUY_DATE_DISP} 외국인 매수 vs {CHK_DATE_DISP} 실제 주가")
    print("=" * 60)

    # 1) 종목 리스트 로드
    codes_dict = load_universe_top(30)
    print(f"\n대상 종목: {len(codes_dict)}개")

    # 2) KRX 세션
    print("\nKRX 세션 연결 중...")
    session = _get_valid_session()
    if not session:
        print("ERROR: KRX 세션 생성 실패")
        return

    # 3) 각 종목 3/10 외국인 데이터 수집 + 3/11 주가 변동
    results = []
    for i, (code, name) in enumerate(codes_dict.items()):
        # 3/10 외국인 데이터
        nat_data = get_nationality_data(session, code, BUY_DATE, name)
        if not nat_data:
            continue

        # 총 외국인 순매수 (전체 합)
        total_foreign = sum(nat_data.values())
        # 주요국 분류
        china_hk = nat_data.get("중국", 0) + nat_data.get("홍콩", 0)
        us = nat_data.get("미국", 0)
        uk = nat_data.get("영국", 0)
        institution = sum(nat_data.get(c, 0) for c in ["영국", "노르웨이", "스위스", "스웨덴", "아일랜드"])
        asia = sum(nat_data.get(c, 0) for c in ["중국", "홍콩", "싱가포르", "일본", "대만"])

        # 3/11 주가 변동
        close, chg, vol = get_price_change(code, CHK_DATE)
        if close is None:
            continue

        results.append({
            "code": code,
            "name": name,
            "total_foreign": total_foreign,
            "china_hk": china_hk,
            "us": us,
            "institution": institution,
            "asia": asia,
            "close_311": close,
            "chg_311": chg,
            "vol_311": vol,
        })

        if (i + 1) % 10 == 0:
            print(f"  ... {i+1}/{len(codes_dict)} 종목 처리")
        time.sleep(REQUEST_INTERVAL * 0.5)

    print(f"\n분석 가능 종목: {len(results)}개")

    if not results:
        print("ERROR: 데이터 없음")
        return

    # 4) 결과 정렬: 외국인 순매수 내림차순
    results.sort(key=lambda x: x["total_foreign"], reverse=True)

    # ── 결과 출력 ──
    print("\n" + "=" * 80)
    print(f"  {BUY_DATE_DISP} 외국인 매수 TOP → {CHK_DATE_DISP} 주가 변동")
    print("=" * 80)
    print(f"{'#':>3} {'종목':<12} {'외국인합(주)':>12} {'중국+홍콩':>10} {'미국':>10} {'3/11등락률':>10} {'판정':>6}")
    print("-" * 80)

    hit_count = 0
    miss_count = 0
    for i, r in enumerate(results):
        chg = r["chg_311"]
        # 판정: 외국인 순매수 양수 → 다음날 상승?
        if r["total_foreign"] > 0:
            verdict = "O적중" if chg > 0 else "X틀림"
            if chg > 0:
                hit_count += 1
            else:
                miss_count += 1
        else:
            verdict = "O적중" if chg <= 0 else "X틀림"
            if chg <= 0:
                hit_count += 1
            else:
                miss_count += 1

        print(f"{i+1:>3} {r['name']:<12} {r['total_foreign']:>12,} {r['china_hk']:>10,} {r['us']:>10,} {chg:>+9.2f}% {verdict:>6}")

    # ── 통계 분석 ──
    total = hit_count + miss_count
    hit_rate = hit_count / total * 100 if total > 0 else 0

    print("\n" + "=" * 60)
    print("  통계 분석")
    print("=" * 60)
    print(f"  적중률: {hit_count}/{total} ({hit_rate:.1f}%)")

    # TOP 10 (외국인 순매수 상위) vs BOTTOM 10
    top10 = results[:10]
    bottom10 = results[-10:]

    top10_avg = sum(r["chg_311"] for r in top10) / len(top10)
    bottom10_avg = sum(r["chg_311"] for r in bottom10) / len(bottom10)
    all_avg = sum(r["chg_311"] for r in results) / len(results)

    print(f"\n  외국인 매수 TOP10 평균 수익률: {top10_avg:+.2f}%")
    print(f"  외국인 매수 BOTTOM10 평균 수익률: {bottom10_avg:+.2f}%")
    print(f"  전체 평균 수익률: {all_avg:+.2f}%")
    print(f"  TOP10 - BOTTOM10 차이: {top10_avg - bottom10_avg:+.2f}%p")

    # 중국+홍콩 기준 TOP5 vs BOTTOM5
    by_china = sorted(results, key=lambda x: x["china_hk"], reverse=True)
    cn_top5 = by_china[:5]
    cn_bot5 = by_china[-5:]
    cn_top5_avg = sum(r["chg_311"] for r in cn_top5) / len(cn_top5)
    cn_bot5_avg = sum(r["chg_311"] for r in cn_bot5) / len(cn_bot5)

    print(f"\n  중국+홍콩 매수 TOP5 평균 수익률: {cn_top5_avg:+.2f}%")
    print(f"  중국+홍콩 매수 BOTTOM5 평균 수익률: {cn_bot5_avg:+.2f}%")

    # 기관(영국+노르웨이 등) 기준
    by_inst = sorted(results, key=lambda x: x["institution"], reverse=True)
    inst_top5 = by_inst[:5]
    inst_bot5 = by_inst[-5:]
    inst_top5_avg = sum(r["chg_311"] for r in inst_top5) / len(inst_top5)
    inst_bot5_avg = sum(r["chg_311"] for r in inst_bot5) / len(inst_bot5)

    print(f"\n  기관국(영국등) 매수 TOP5 평균 수익률: {inst_top5_avg:+.2f}%")
    print(f"  기관국(영국등) 매수 BOTTOM5 평균 수익률: {inst_bot5_avg:+.2f}%")

    # 상관관계 (간단 pearson)
    try:
        import numpy as np
        foreigns = np.array([r["total_foreign"] for r in results], dtype=float)
        changes = np.array([r["chg_311"] for r in results], dtype=float)
        if np.std(foreigns) > 0 and np.std(changes) > 0:
            corr = np.corrcoef(foreigns, changes)[0, 1]
            print(f"\n  상관계수 (외국인매수 vs 주가변동): {corr:.4f}")
            if corr > 0.3:
                print("  → 양의 상관 (외국인 사면 오른다)")
            elif corr < -0.3:
                print("  → 음의 상관 (외국인 사면 오히려 내린다)")
            else:
                print("  → 상관 약함 (직접적 연관 약함)")
    except ImportError:
        pass

    # ── 결론 ──
    print("\n" + "=" * 60)
    print("  결론")
    print("=" * 60)
    if hit_rate >= 65:
        print(f"  ✅ 적중률 {hit_rate:.0f}%: 외국인 수급은 유의미한 선행지표")
    elif hit_rate >= 50:
        print(f"  ⚠️ 적중률 {hit_rate:.0f}%: 외국인 수급만으로는 불충분, 보조지표로 활용")
    else:
        print(f"  ❌ 적중률 {hit_rate:.0f}%: 외국인 수급과 다음날 주가 상관 약함")

    if top10_avg - bottom10_avg > 1.0:
        print(f"  ✅ TOP10-BOTTOM10 차이 {top10_avg - bottom10_avg:+.2f}%p: 수급 좋을수록 수익률 높음")
    elif top10_avg - bottom10_avg > 0:
        print(f"  ⚠️ TOP10-BOTTOM10 차이 {top10_avg - bottom10_avg:+.2f}%p: 약한 양의 효과")
    else:
        print(f"  ❌ TOP10-BOTTOM10 차이 {top10_avg - bottom10_avg:+.2f}%p: 역효과")


if __name__ == "__main__":
    run_backtest()
