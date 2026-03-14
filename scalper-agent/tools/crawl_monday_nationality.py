# -*- coding: utf-8 -*-
"""
월요일 추천 TOP 종목 + 국적 데이터 없는 MOMENTUM 종목 크롤링
nationality_signal.collect_daily_snapshots 사용
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
NAT_DIR = DATA_DIR / "nationality"


def get_codes_needing_nationality():
    """MOMENTUM v3 스캔 결과에서 국적 데이터 없는 종목 추출"""
    # monday_candidates.json 로드
    cands_path = DATA_DIR / "monday_candidates.json"
    if cands_path.exists():
        cands = json.load(open(cands_path, "r", encoding="utf-8"))
        codes_from_cands = [s["code"] for s in cands.get("candidates", [])]
    else:
        codes_from_cands = []

    # 핵심 MOMENTUM TOP 종목 (v3 결과)
    top_codes = [
        "097230",  # HJ중공업
        "486990",  # 노타
        "475150",  # SK이터닉스
        "402030",  # 코난테크놀로지
        "475400",  # 씨메스
        "047040",  # 대우건설
        "461300",  # 아이스크림미디어
        "010140",  # 삼성중공업
        "476830",  # 알지노믹스
        "098070",  # 한텍
        "206640",  # 바디텍메드
        "077970",  # STX엔진
        "089860",  # 롯데렌탈
        "003920",  # 남양유업
        "060370",  # LS마린솔루션
        "417200",  # LS머트리얼즈
        "174900",  # 앱클론
        "475830",  # 오름테라퓨틱
        "047920",  # HLB제약
        "003300",  # 한일홀딩스
        "064760",  # 티씨케이
        "010780",  # 아이에스동서
        "009410",  # 태영건설
        "000720",  # 현대건설
        "389030",  # 지니너스
        "452280",  # 한선엔지니어링
    ]

    all_codes = list(dict.fromkeys(top_codes + codes_from_cands))

    # 이미 국적 데이터 있는 종목 필터
    need_crawl = []
    already_have = []
    for code in all_codes:
        has_data = False
        # 날짜별 스냅샷 확인
        snapshots = list(NAT_DIR.glob(f"{code}_2*.csv"))
        if snapshots:
            has_data = True
        # 누적 프로파일 확인
        profile = NAT_DIR / f"nationality_{code}.csv"
        if profile.exists():
            has_data = True

        if has_data:
            already_have.append(code)
        else:
            need_crawl.append(code)

    print(f"전체 대상: {len(all_codes)}종목")
    print(f"이미 국적 데이터 있음: {len(already_have)}종목")
    print(f"크롤링 필요: {len(need_crawl)}종목")
    print()

    if need_crawl:
        universe = json.load(open(DATA_DIR / "universe.json", "r", encoding="utf-8"))
        for code in need_crawl:
            name = universe.get(code, {}).get("name", code)
            print(f"  {name}({code})")

    return need_crawl, already_have


def crawl(codes):
    """KRX 국적 데이터 크롤링"""
    if not codes:
        print("크롤링할 종목이 없습니다.")
        return

    print(f"\n{'=' * 60}")
    print(f"  KRX 국적 데이터 크롤링 시작 ({len(codes)}종목)")
    print(f"{'=' * 60}")

    try:
        from data.nationality_signal import collect_daily_snapshots
        results = collect_daily_snapshots(codes)
        print(f"\n수집 완료: {len(results)}/{len(codes)}종목")
        for code, data in results.items():
            if data:
                top3 = sorted(data.items(), key=lambda x: x[1], reverse=True)[:3]
                top3_str = ", ".join(f"{c}({v:,})" for c, v in top3)
                print(f"  {code}: {len(data)}개국 - {top3_str}")
    except Exception as e:
        print(f"collect_daily_snapshots 실패: {e}")
        print("fetch_nationality_batch로 fallback...")

        try:
            from data.krx_nationality_crawler import fetch_nationality_batch
            results = fetch_nationality_batch(codes, date_from="20260313", date_to="20260313")
            print(f"\n수집 완료: {len(results)}/{len(codes)}종목")
            for code, df in results.items():
                if df is not None and not df.empty:
                    print(f"  {code}: {len(df)}행")
        except Exception as e2:
            print(f"fetch_nationality_batch도 실패: {e2}")
            print("KRX 로그인이 필요할 수 있습니다.")
            print("python data/krx_nationality_crawler.py --login")


if __name__ == "__main__":
    need_crawl, already_have = get_codes_needing_nationality()

    if "--dry-run" not in sys.argv:
        crawl(need_crawl)
    else:
        print("\n[DRY RUN] 크롤링 건너뜀")
