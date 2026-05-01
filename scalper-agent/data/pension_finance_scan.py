# -*- coding: utf-8 -*-
"""매집 합류 시그널 — 연기금+금투 합류 타이밍 스캔.

FLOWX 패널명: "매집 합류 시그널"

백테스트 결과 (1,714종목 249거래일):
  연기금 3일+ + 금투 매수 → D+5 +1.59%, 외인 무관
  연기금 3일+ + 금투 매수 + 외인 매도 → D+5 +1.58% (동일)
  최적 타이밍: 연기금 3~4일차에 금투가 들어오는 시점

감지 방식: 최근 10거래일 중 연기금 매수 7일+ (빈도 기반)
  → 하루 쉬어도 패턴을 놓치지 않음 (연속 방식 대비 개선)
  → pension_score 기반 TOP 랭킹 (ranked_stocks)

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


def _calc_pension_score(entry: dict) -> int:
    """수급 강도 점수 산출.

    공식:
      연기금일수 × 15      (7d=105 ~ 10d=150)  꾸준함
    + 연기금누적 / 10      (100억=10점)          규모
    + min(금투오늘×2, 60)  (30억=60점, cap)      합류 강도
    + TODAY +20 / YESTERDAY +10                   신선도
    + 덜오름(ret5<5%) +15                        매수여지
    """
    score = (
        entry["pension_buy_days"] * 15
        + entry["pension_cum"] / 10
        + min(max(entry["fi_today"], 0) * 2, 60)
        + (20 if entry["fi_joined"] == "TODAY"
           else 10 if entry["fi_joined"] == "YESTERDAY" else 0)
        + (15 if entry["ret5"] < 5 else 0)
    )
    return round(score)


def scan_pension_finance() -> Optional[dict]:
    """연기금 7일+ 매수(10일 윈도우) + 금투 합류 종목 스캔.

    Returns:
        {
            "date": "2026-04-28",
            "total_count": int,
            "best_count": int,      # 연기금 + 금투 오늘 진입
            "standby_count": int,   # 연기금만, 금투 아직
            "best_stocks": [...],   # 핵심후보
            "standby_stocks": [...],  # 대기 리스트
            "ranked_stocks": [...],   # 점수 기반 TOP 정렬
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

        # 5d 수익률 + 외인 순매수 (CSV)
        ret5 = 0.0
        close_now = 0
        frgn_today = 0.0  # 외인 오늘 순매수 (억원)
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
                # 외인 오늘 순매수 (백만원 → 억원)
                if rows:
                    frgn_today = _csv_float(rows[-1], "외국인_금액") / 100
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
            "frgn_today": round(frgn_today, 1),
            "frgn_joined": frgn_today >= 1.0,  # 외인 1억+ 순매수
            "ret5": round(ret5, 1),
            "close": close_now,
        }
        entry["pension_score"] = _calc_pension_score(entry)

        # 매수등급: 금투합류 × 외인합류 × 덜오름 조합 (백테스트 근거)
        _is_fi = fi_joined == "TODAY"
        _is_early = ret5 < 5.0
        _is_frgn = frgn_today >= 1.0
        if _is_fi and _is_frgn and _is_early:
            entry["pension_grade"] = "S"   # 삼중합류 D+5 +1.09%
        elif _is_fi and _is_early:
            entry["pension_grade"] = "A"   # 이중합류 D+5 +0.42%
        elif fi_joined == "" and _is_frgn:
            entry["pension_grade"] = "B"   # 발화대기 D+5 +0.52%
        else:
            entry["pension_grade"] = "C"

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

    # 점수 기반 TOP 랭킹 (핵심 + 대기 전체 통합 정렬)
    all_scored = best_stocks + standby_stocks
    all_scored.sort(key=lambda x: -x["pension_score"])
    ranked_stocks = all_scored[:10]

    result = {
        "date": f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}" if len(latest_date) == 8 else latest_date,
        "total_count": len(best_stocks) + len(standby_stocks),
        "best_count": len(best_stocks),
        "best_fresh_count": len(best_fresh),
        "standby_count": len(standby_stocks),
        "best_stocks": best_stocks[:50],
        "best_fresh": best_fresh[:30],
        "standby_stocks": standby_stocks[:30],
        "ranked_stocks": ranked_stocks,
    }

    # 로컬 JSON 저장 (morning_recommendation에서 pension_grade 참조)
    save_path = DATA_STORE / "pension_scan.json"
    try:
        save_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"pension_scan.json 저장 실패: {e}")

    if ranked_stocks:
        s_cnt = sum(1 for s in all_scored if s.get("pension_grade") == "S")
        a_cnt = sum(1 for s in all_scored if s.get("pension_grade") == "A")
        logger.info(
            f"연기금스캔 완료: 핵심 {len(best_stocks)}종목 "
            f"(덜오른 {len(best_fresh)}) / 대기 {len(standby_stocks)}종목 "
            f"/ TOP1={ranked_stocks[0]['name']}({ranked_stocks[0]['pension_score']}점) "
            f"/ 등급 S={s_cnt} A={a_cnt}"
        )
    else:
        logger.info("연기금스캔 완료: 0종목")
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

        print("=== TOP 랭킹 (수급 강도 점수) ===")
        for i, s in enumerate(r.get("ranked_stocks", []), 1):
            print(f"  {i:>2}. {s['name']:12s} {s['pension_score']:>3}점 "
                  f"연{s['pension_buy_days']}d "
                  f"누적{s['pension_cum']:>+.0f}억 "
                  f"금투{s['fi_today']:>+.0f}억 "
                  f"합류={s['fi_joined'] or '-':4s} "
                  f"5d{s['ret5']:>+.1f}%")

        print(f"\n=== 핵심후보 (연기금7d+ + 금투 진입) ===")
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
