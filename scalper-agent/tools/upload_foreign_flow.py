"""
외국인 돈 흐름 Supabase 업로드
================================

intelligence_foreign_flow + intelligence_foreign_flow_sector 테이블에 업로드

스케줄: 매일 16:35 (flow_collector 완료 후)
실행: python3 tools/upload_foreign_flow.py [--date 2026-04-09]

파이프라인:
1. 전체 유니버스 (시총 2천억+) 스캔
2. 5일 롤링 외국인/기관 수급 집계
3. 3가지 랭킹 (single_day / cumul_5d / dual_buy)
4. 섹터별 집계
5. Supabase upsert
"""
from __future__ import annotations
import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict
import pandas as pd


# ─────────────────────────────────────────────
# 경로
# ─────────────────────────────────────────────
BASE = Path(__file__).resolve().parents[1]
FLOW_DIR = BASE / "data_store" / "flow"
UNIVERSE_PATH = BASE / "data_store" / "universe.json"

# .env 로드 (여러 경로 시도)
try:
    from dotenv import load_dotenv
    for env_path in [
        BASE.parent / ".env",
        BASE / ".env",
        Path("/home/ubuntu/bodyhunter/.env"),
        Path("/home/ubuntu/.env"),
    ]:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass


# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
MIN_MCAP = 2000       # 시총 하한 (억)
LOOKBACK_DAYS = 5
MIN_FOREIGN_TOTAL = 30  # 5일 누적 30억 이상만 업로드
QUIET_THRESHOLD = 7.0


def load_universe() -> dict:
    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def analyze() -> tuple[list[dict], list[dict], str]:
    """전체 유니버스 분석 → (종목 row, 섹터 row, 기준일)"""
    uni = load_universe()
    targets = {code: info for code, info in uni.items()
               if isinstance(info, dict) and info.get("cap_억", 0) >= MIN_MCAP}
    print(f"📊 분석 대상: {len(targets)}개 종목")

    stock_rows = []
    sector_agg = defaultdict(lambda: {
        "foreign_5d": 0.0, "inst_5d": 0.0, "count": 0, "stocks": []
    })
    last_date = None

    for code, info in targets.items():
        csv = FLOW_DIR / f"{code}_investor.csv"
        if not csv.exists():
            continue
        try:
            df = pd.read_csv(csv)
            # date 컬럼 정규화: "Unnamed: 0" → "date"
            if "date" not in df.columns and "Unnamed: 0" in df.columns:
                df.rename(columns={"Unnamed: 0": "date"}, inplace=True)
            df = df.tail(LOOKBACK_DAYS)
            if len(df) < 3:
                continue

            name = info.get("name", "?")
            sector = info.get("sector", "기타")
            mcap = int(info.get("cap_억", 0))

            # 날짜
            curr_date = df.iloc[-1]["date"]
            if last_date is None or curr_date > last_date:
                last_date = curr_date

            # 당일 금액 (백만원 → 억)
            last = df.iloc[-1]
            foreign_today = float(last["외국인_금액"]) / 100
            inst_today = float(last["기관_금액"]) / 100
            close = int(last["종가"])

            # 5일 누적
            foreign_5d = float(df["외국인_금액"].sum()) / 100
            inst_5d = float(df["기관_금액"].sum()) / 100

            # 매수일수
            foreign_buy_days = int((df["외국인_금액"] > 0).sum())
            inst_buy_days = int((df["기관_금액"] > 0).sum())

            # 주가 변동률
            closes = df["종가"].astype(float).values
            price_change = ((closes[-1] / closes[0]) - 1) * 100 if closes[0] > 0 else 0

            # 최소 필터: 5일 누적 외인 30억+
            if foreign_5d < MIN_FOREIGN_TOTAL:
                continue

            # 플래그
            is_dual_buy = (foreign_5d > 30 and inst_5d > 30
                          and foreign_buy_days >= 3 and inst_buy_days >= 3)
            is_quiet_accum = (foreign_buy_days >= 3 and foreign_5d > 0
                             and abs(price_change) <= QUIET_THRESHOLD)

            row = {
                "date": curr_date,
                "code": code,
                "name": name,
                "sector": sector,
                "mcap_억": mcap,
                "foreign_today_억": round(foreign_today, 1),
                "inst_today_억": round(inst_today, 1),
                "close": close,
                "foreign_5d_억": round(foreign_5d, 1),
                "inst_5d_억": round(inst_5d, 1),
                "foreign_buy_days": foreign_buy_days,
                "inst_buy_days": inst_buy_days,
                "is_dual_buy": is_dual_buy,
                "is_quiet_accum": is_quiet_accum,
                "price_change_5d_pct": round(price_change, 2),
                "rank_single": None,
                "rank_cumul": None,
                "rank_dual": None,
            }
            stock_rows.append(row)

            # 섹터 집계
            sector_agg[sector]["foreign_5d"] += foreign_5d
            sector_agg[sector]["inst_5d"] += inst_5d
            sector_agg[sector]["count"] += 1
            sector_agg[sector]["stocks"].append({
                "name": name,
                "foreign_5d": round(foreign_5d, 1),
                "inst_5d": round(inst_5d, 1),
            })
        except Exception as e:
            continue

    print(f"  ✅ 감지 종목: {len(stock_rows)}")
    print(f"  ✅ 감지 섹터: {len(sector_agg)}")

    # ───── 랭킹 부여 ─────
    # 1) 당일 외인 순매수 순위 (10억+ 매수만)
    single_sorted = sorted(
        [r for r in stock_rows if r["foreign_today_억"] > 10],
        key=lambda x: x["foreign_today_억"], reverse=True
    )
    for i, r in enumerate(single_sorted, 1):
        r["rank_single"] = i

    # 2) 5일 누적 외인 순위
    cumul_sorted = sorted(stock_rows, key=lambda x: x["foreign_5d_억"], reverse=True)
    for i, r in enumerate(cumul_sorted, 1):
        r["rank_cumul"] = i

    # 3) 쌍매수 순위
    dual_sorted = sorted(
        [r for r in stock_rows if r["is_dual_buy"]],
        key=lambda x: x["foreign_5d_억"] + x["inst_5d_억"], reverse=True
    )
    for i, r in enumerate(dual_sorted, 1):
        r["rank_dual"] = i

    # ───── 섹터 랭킹 ─────
    sector_list = sorted(
        sector_agg.items(),
        key=lambda x: x[1]["foreign_5d"],
        reverse=True
    )
    sector_rows = []
    for i, (sec, data) in enumerate(sector_list, 1):
        top_stocks = sorted(data["stocks"], key=lambda x: x["foreign_5d"], reverse=True)[:5]
        sector_rows.append({
            "date": last_date,
            "sector": sec,
            "foreign_5d_억": round(data["foreign_5d"], 1),
            "inst_5d_억": round(data["inst_5d"], 1),
            "stock_count": data["count"],
            "rank": i,
            "top_stocks": top_stocks,
        })

    return stock_rows, sector_rows, last_date


def upload_to_supabase(stock_rows: list[dict], sector_rows: list[dict]) -> bool:
    """Supabase upsert"""
    try:
        from supabase import create_client
    except ImportError:
        print("❌ supabase-py 설치 필요: pip install supabase")
        return False

    url = os.environ.get("SUPABASE_URL", "")
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
           or os.environ.get("SUPABASE_KEY", ""))

    if not url or not key:
        print(f"❌ Supabase 환경변수 없음 (URL: {bool(url)}, KEY: {bool(key)})")
        return False

    client = create_client(url, key)

    # 1) 종목별 업로드 (배치 100개씩)
    print(f"\n📤 intelligence_foreign_flow 업로드 시작 ({len(stock_rows)}건)")
    BATCH = 100
    success = 0
    for i in range(0, len(stock_rows), BATCH):
        batch = stock_rows[i:i+BATCH]
        try:
            # numpy.bool_ → Python bool 변환 (JSON 직렬화 호환)
            for row in batch:
                for bk in ("is_dual_buy", "is_quiet_accum"):
                    if bk in row:
                        row[bk] = bool(row[bk])
            client.table("intelligence_foreign_flow").upsert(
                batch, on_conflict="date,code"
            ).execute()
            success += len(batch)
            print(f"  배치 {i//BATCH + 1}: {len(batch)}건 ✓")
        except Exception as e:
            print(f"  배치 {i//BATCH + 1} 실패: {e}")
    print(f"✅ 종목: {success}/{len(stock_rows)}건 업로드")

    # 2) 섹터별 업로드
    print(f"\n📤 intelligence_foreign_flow_sector 업로드 시작 ({len(sector_rows)}건)")
    try:
        client.table("intelligence_foreign_flow_sector").upsert(
            sector_rows, on_conflict="date,sector"
        ).execute()
        print(f"✅ 섹터: {len(sector_rows)}건 업로드 완료")
    except Exception as e:
        print(f"❌ 섹터 업로드 실패: {e}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Supabase 업로드 없이 로컬 저장만")
    parser.add_argument("--save-json", action="store_true", help="JSON 저장")
    args = parser.parse_args()

    print("🔍 외국인 돈 흐름 분석 시작")
    print(f"   시총 하한: {MIN_MCAP}억 / Lookback: {LOOKBACK_DAYS}일")

    stock_rows, sector_rows, last_date = analyze()
    print(f"\n📅 기준일: {last_date}")

    # 상위 종목 미리보기
    top5_cumul = sorted(stock_rows, key=lambda x: x["foreign_5d_억"], reverse=True)[:5]
    print("\n🏆 5일 누적 외인 TOP 5")
    for r in top5_cumul:
        print(f"  [{r['rank_cumul']:>2}] {r['name']:<15} {r['foreign_5d_억']:>8,.0f}억  "
              f"({r['sector']})  dual={r['is_dual_buy']}  quiet={r['is_quiet_accum']}")

    # 섹터 TOP 5
    print("\n🏭 섹터 TOP 5")
    for s in sector_rows[:5]:
        print(f"  [{s['rank']}] {s['sector']:<12} 외인 {s['foreign_5d_억']:>8,.0f}억 "
              f"기관 {s['inst_5d_억']:>8,.0f}억  ({s['stock_count']}종목)")

    if args.save_json:
        out_path = BASE / "data_store" / "intelligence_foreign_flow.json"
        out = {
            "updated": datetime.now().isoformat(),
            "date": last_date,
            "stock_rows": stock_rows,
            "sector_rows": sector_rows,
        }
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n💾 JSON 저장: {out_path}")

    if not args.dry_run:
        ok = upload_to_supabase(stock_rows, sector_rows)
        if ok:
            print("\n✅ 전체 업로드 완료")
            return 0
        else:
            print("\n❌ 업로드 실패")
            return 1
    else:
        print("\n⚠️ Dry-run 모드: 업로드 생략")
        return 0


if __name__ == "__main__":
    sys.exit(main())
