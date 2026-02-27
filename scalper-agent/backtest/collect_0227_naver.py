# -*- coding: utf-8 -*-
"""2/27 수급 데이터 수집 — 네이버 금융 스크래핑 (pykrx KRX API 장애 우회)

네이버 금융의 외국인/기관 매매동향 페이지에서:
  - 기관 순매매량
  - 외국인 순매매량
  - 외국인 보유주수
  - 외국인 보유율(%)
을 수집하여 기존 flow CSV 형식에 맞게 저장합니다.
"""
import sys, io, time, json
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

FLOW_DIR = Path(__file__).resolve().parent.parent / "data_store" / "flow"
TARGET_DATE = "2026.02.27"
TARGET_TS = pd.Timestamp("2026-02-27")

# 유니버스 로드
uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
with open(uni_path, "r", encoding="utf-8") as f:
    universe = json.load(f)

codes = list(universe.keys())
print(f"=== 2/27 수급 수집 (네이버 금융): {len(codes)}종목 ===")
print(f"시작: {datetime.now().strftime('%H:%M:%S')}")

# 2/27 미반영 종목 확인
flow_need = []
for code in codes:
    csv_path = FLOW_DIR / f"{code}_investor.csv"
    if not csv_path.exists():
        # CSV가 없는 종목도 수집 대상
        flow_need.append(code)
        continue
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        if TARGET_TS not in df.index:
            flow_need.append(code)
    except:
        flow_need.append(code)

print(f"2/27 미반영: {len(flow_need)}종목")


def parse_number(text):
    """'+1,554,880' 또는 '-19,602,376' → int"""
    text = text.replace(",", "").replace("+", "").strip()
    if not text or text == "-":
        return 0
    try:
        return int(text)
    except:
        return 0


def fetch_naver_investor(code):
    """네이버 금융에서 외국인/기관 일별 매매 데이터 가져오기"""
    url = "https://finance.naver.com/item/frgn.naver"
    params = {"code": code, "page": "1"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    r = requests.get(url, params=params, headers=headers, timeout=10)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.content.decode("euc-kr", errors="replace"), "html.parser")
    tables = soup.find_all("table")

    if len(tables) < 4:
        return None

    # Table[3]: 일별 외국인/기관 순매수 테이블
    table = tables[3]
    rows = table.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        date_text = cells[0].get_text(strip=True)
        if date_text != TARGET_DATE:
            continue

        # 컬럼: 날짜, 종가, 전일비, 등락률, 거래량, 기관순매매, 외국인순매매, 보유주수, 보유율
        inst_net = parse_number(cells[5].get_text(strip=True))
        frgn_net = parse_number(cells[6].get_text(strip=True))

        frgn_hold = 0
        frgn_rate = 0.0
        if len(cells) >= 9:
            frgn_hold = parse_number(cells[7].get_text(strip=True))
            rate_text = cells[8].get_text(strip=True).replace("%", "")
            try:
                frgn_rate = float(rate_text)
            except:
                frgn_rate = 0.0

        return {
            "기관_순매매": inst_net,
            "외국인_순매매": frgn_net,
            "외국인_보유주수": frgn_hold,
            "외국인_보유율": frgn_rate,
        }

    return None


def update_flow_csv(code, data):
    """기존 flow CSV에 2/27 데이터 추가 (또는 새로 생성)"""
    csv_path = FLOW_DIR / f"{code}_investor.csv"

    if csv_path.exists():
        existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    else:
        # 새 CSV 생성
        existing = pd.DataFrame(
            columns=["기관_금액", "기타법인_금액", "개인_금액", "외국인_금액",
                      "기관_수량", "기타법인_수량", "개인_수량", "외국인_수량"]
        )
        existing.index.name = "날짜"

    # 네이버는 순매매'량'만 제공, 금액은 없음
    # 기존 CSV 컬럼에 맞춰서 수량 컬럼에 넣기
    new_data = {}
    for col in existing.columns:
        new_data[col] = [0]

    # 기관 순매매량 → 기관_수량 (금액은 0)
    if "기관_수량" in existing.columns:
        new_data["기관_수량"] = [data["기관_순매매"]]
    if "기관_금액" in existing.columns:
        new_data["기관_금액"] = [0]  # 네이버에서는 금액 미제공

    # 외국인 순매매량 → 외국인_수량
    if "외국인_수량" in existing.columns:
        new_data["외국인_수량"] = [data["외국인_순매매"]]
    if "외국인_금액" in existing.columns:
        new_data["외국인_금액"] = [0]  # 네이버에서는 금액 미제공

    new_row = pd.DataFrame(new_data, index=[TARGET_TS])
    new_row.index.name = existing.index.name or "날짜"
    new_row = new_row[[c for c in existing.columns if c in new_row.columns]]

    combined = pd.concat([existing, new_row])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()
    combined.to_csv(csv_path)


# 메인 수집 루프
updated = 0
errors = 0
no_data = 0

for i, code in enumerate(flow_need):
    try:
        data = fetch_naver_investor(code)

        if data is None:
            no_data += 1
        else:
            update_flow_csv(code, data)
            updated += 1

        if (i + 1) % 100 == 0:
            print(f"  진행: {i+1}/{len(flow_need)} (업데이트: {updated}, 없음: {no_data}, 에러: {errors})")

        time.sleep(0.15)  # 네이버 속도 제한

    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f"  에러 {code}: {e}")

print(f"\n수급 완료: {updated}종목 업데이트, {no_data}종목 데이터없음, {errors}에러")
print(f"종료: {datetime.now().strftime('%H:%M:%S')}")
