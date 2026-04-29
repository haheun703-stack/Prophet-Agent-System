# -*- coding: utf-8 -*-
"""매집 합류 시그널 — 연기금+금투 합류 타이밍 스캔.

FLOWX 패널명: "매집 합류 시그널"

백테스트 결과 (1,714종목 249거래일):
  연기금 3일+ + 금투 매수 → D+5 +1.59%, 외인 무관
  연기금 3일+ + 금투 매수 + 외인 매도 → D+5 +1.58% (동일)
  최적 타이밍: 연기금 3~4일차에 금투가 들어오는 시점

감지 방식: 최근 10거래일 중 연기금 매수 5일+ (빈도 기반)
  → 하루 쉬어도 패턴을 놓치지 않음 (연속 방식 대비 개선)

데이터 소스:
  - quant_investor_extra.json (퀀트봇 pykrx, 매일 17:28 갱신)
  - flow/*_investor.csv (종가, 5d 수익률 계산)

COO G7 Stage4 C41에서 호출:
  from data.pension_finance_scan import scan_pension_finance
  result = scan_pension_finance()
"""
import json
import logging
from pathlib import Path
from typing import Optional

from tools.flow_intelligence import _parse_flow_csv, _csv_float

logger = logging.getLogger("BH.PensionScan")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
QUANT_JSON = DATA_STORE / "quant_investor_extra.json"
FLOW_DIR = DATA_STORE / "flow"
UNI_PATH = DATA_STORE / "universe.json"

# ETF 필터
ETF_PREFIXES = [
    "KODEX", "TIGER", "RISE", "PLUS", "KIWOOM", "KoAct", "TIME",
    "SOL", "HANARO", "KOSEF", "ACE", "ARIRANG", "BNK", "TIMEFOLIO",
    "FOCUS", "WOORI", "KB", "TREX",
]


def scan_pension_finance() -> Optional[dict]:
    """연기금 3-5일 연속매수 + 금투 합류 종목 스캔.

    Returns:
        {
            "date": "2026-04-28",
            "total_count": int,
            "best_count": int,      # 연기금 + 금투 오늘 진입
            "standby_count": int,   # 연기금만, 금투 아직
            "best_stocks": [...],   # 핵심후보
            "standby_stocks": [...],  # 대기 리스트
        }
    """
    if not QUANT_JSON.exists():
        logger.warning("quant_investor_extra.json 없음 — sync_from_vps.py 실행 필요")
        return None

    # 데이터 로드
    raw = json.loads(QUANT_JSON.read_text(encoding="utf-8"))
    daily = raw.get("daily", {})
    if not daily:
        logger.warning("quant_investor_extra.json daily 비어있음")
        return None

    uni = json.loads(UNI_PATH.read_text(encoding="utf-8"))

    best_stocks = []
    standby_stocks = []
    latest_date = ""

    for code, stock_data in daily.items():
        # 기본 필터
        info = uni.get(code, {})
        if not isinstance(info, dict):
            continue
        cap = info.get("cap_억", 0)
        if not cap or cap < 1000:
            continue
        name = stock_data.get("name", info.get("name", ""))
        if any(name.startswith(p) for p in ETF_PREFIXES):
            continue

        dates = stock_data.get("dates", {})
        if not dates:
            continue

        sorted_dates = sorted(dates.keys())
        if not sorted_dates:
            continue
        latest_date = sorted_dates[-1]

        # 연기금 매수일수 (최근 10거래일 중 매수한 날 수)
        # 하루 쉬어도 패턴을 놓치지 않도록 연속 대신 빈도 기반
        window = sorted_dates[-10:]  # 최근 10거래일
        pension_buy_days = sum(
            1 for d in window if dates[d].get("pension_net", 0) > 0
        )

        if pension_buy_days < 7:
            continue

        # 연기금 누적 (윈도우 전체)
        pension_cum = sum(
            dates[d].get("pension_net", 0)
            for d in window if dates[d].get("pension_net", 0) > 0
        )

        # 금투 오늘/어제
        fi_today = dates[sorted_dates[-1]].get("finance_net", 0)
        fi_yesterday = dates[sorted_dates[-2]].get("finance_net", 0) if len(sorted_dates) >= 2 else 0
        fi_3d = sum(dates[d].get("finance_net", 0) for d in sorted_dates[-3:])

        # 금투 진입 판별
        if fi_today > 0:
            fi_joined = "TODAY"
        elif fi_yesterday > 0:
            fi_joined = "YESTERDAY"
        else:
            fi_joined = ""

        # 5d 수익률 (CSV)
        ret5 = 0.0
        close_now = 0
        csv_path = FLOW_DIR / f"{code}_investor.csv"
        if csv_path.exists():
            try:
                rows = _parse_flow_csv(csv_path)
                rows.sort(key=lambda r: r.get("date", ""))
                # 중복 제거
                seen = set()
                deduped = []
                for r in rows:
                    d = r.get("date", "")
                    if d and d not in seen:
                        seen.add(d)
                        deduped.append(r)
                rows = deduped
                if len(rows) >= 5:
                    c_now = _csv_float(rows[-1], "종가")
                    c_5 = _csv_float(rows[-5], "종가")
                    if c_now > 0 and c_5 > 0:
                        ret5 = ((c_now / c_5) - 1) * 100
                        close_now = int(c_now)
            except Exception:
                pass

        entry = {
            "code": code,
            "name": name,
            "sector": info.get("sector", ""),
            "cap": cap,
            "pension_buy_days": pension_buy_days,
            "pension_cum": round(pension_cum, 1),
            "fi_today": round(fi_today, 1),
            "fi_3d": round(fi_3d, 1),
            "fi_joined": fi_joined,
            "ret5": round(ret5, 1),
            "close": close_now,
        }

        if fi_joined in ("TODAY", "YESTERDAY"):
            best_stocks.append(entry)
        elif fi_joined == "" and ret5 < 5:
            # 대기: 연기금만 매수중, 금투 아직, 덜 오른 것
            standby_stocks.append(entry)

    # 정렬
    best_stocks.sort(key=lambda x: -x["pension_cum"])
    standby_stocks.sort(key=lambda x: -x["pension_cum"])

    # 아직 덜 오른 핵심 후보 분리
    best_fresh = [s for s in best_stocks if s["ret5"] < 5]

    result = {
        "date": f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}" if len(latest_date) == 8 else latest_date,
        "total_count": len(best_stocks) + len(standby_stocks),
        "best_count": len(best_stocks),
        "best_fresh_count": len(best_fresh),
        "standby_count": len(standby_stocks),
        "best_stocks": best_stocks[:50],
        "best_fresh": best_fresh[:30],
        "standby_stocks": standby_stocks[:30],
    }

    logger.info(
        f"연기금스캔 완료: 핵심 {len(best_stocks)}종목 "
        f"(덜오른 {len(best_fresh)}) / 대기 {len(standby_stocks)}종목"
    )
    return result


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO)
    r = scan_pension_finance()
    if r:
        print(f"\n기준일: {r['date']}")
        print(f"핵심후보: {r['best_count']}종목 (덜오른것 {r['best_fresh_count']})")
        print(f"대기: {r['standby_count']}종목\n")
        print("=== 핵심후보 (연기금5d+ + 금투 진입) ===")
        for s in r["best_stocks"][:10]:
            print(f"  {s['name']:12s} 연기금{s['pension_buy_days']}d "
                  f"누적{s['pension_cum']:>+.0f}억 "
                  f"금투{s['fi_today']:>+.0f}억 "
                  f"5d{s['ret5']:>+.1f}%")
        print(f"\n=== 대기 (금투 미진입, 5d<5%) ===")
        for s in r["standby_stocks"][:10]:
            print(f"  {s['name']:12s} 연기금{s['pension_buy_days']}d "
                  f"누적{s['pension_cum']:>+.0f}억 "
                  f"5d{s['ret5']:>+.1f}%")
