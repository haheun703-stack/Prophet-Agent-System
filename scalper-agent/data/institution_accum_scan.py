# -*- coding: utf-8 -*-
"""기관 매집 합류 시그널 — KIS 종목별 기관계 기반 (KRX-free, 6/19 신설).

배경(6/19 사장님 결정 B): 기존 pension_finance_scan은 종목별 연기금/금투를 quant_investor_extra.json
(퀀트봇 pykrx→KRX)에서 받는데, KRX 체인이 6/9에 박제 + KRX 과다크롤링 차단 → KRX 폐기.
KIS로는 종목별 연기금/금투 세분이 안 나오므로(4주체뿐), 우리가 매일 fresh하게 받는
**종목별 기관계(KIS FHKST01010900 → flow/*_investor.csv)**로 "기관 매집 합류"를 재정의.

★ 트레이드오프(정직): 연기금/금투 세분 → 기관계 통합(coarser proxy). 백테스트(연기금 3일+ +금투 합류
  → D+5 +1.59%)의 엣지가 기관계로도 유지되는지는 forward shadow로 검증 필요(institution_accum_shadow).
  source='kis_institution_total'로 명시 — 프론트가 "기관계 proxy"로 라벨·구분 가능.

★ KRX 0 · 키움 0 · 추가 인증 0. 차단 영향 0. 출력 규격은 pension_finance_scan과 동일(upload/
  morning_recommendation 무변경) — 단 actor가 연기금→기관계, 합류축이 금투→외국인.

데이터 소스: data_store/flow/{code}_investor.csv (KIS, 매일 fill로 fresh)
  컬럼: date,종가,...,외국인_금액,기관_금액,... (금액 단위=백만원 → 억=/100)

호출:
  from data.institution_accum_scan import scan_institution_accum
  from data.upload_pension_scan import upload_pension_scan
  upload_pension_scan(scan_institution_accum())
"""
import json
import logging
from pathlib import Path
from typing import Optional

from tools.flow_intelligence import _parse_flow_csv, _csv_float

logger = logging.getLogger("BH.InstitutionAccumScan")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
FLOW_DIR = DATA_STORE / "flow"
UNI_PATH = DATA_STORE / "universe.json"

SOURCE = "kis_institution_total"   # ★ 연기금/금투 정밀 아님 = 기관계 proxy 명시
BAEKMAN_TO_EOK = 100.0             # 백만원 → 억원

WINDOW_DAYS = 10                   # 매집 판정 윈도우 (최근 10거래일)
ACCUM_MIN_DAYS = 7                 # 기관계 매수 7일+ = 매집 (pension 7d+와 동일 기준)

ETF_PREFIXES = [
    "KODEX", "TIGER", "RISE", "PLUS", "KIWOOM", "KoAct", "TIME",
    "SOL", "HANARO", "KOSEF", "ACE", "ARIRANG", "BNK", "TIMEFOLIO",
    "FOCUS", "WOORI", "KB", "TREX",
]


def _calc_accum_score(entry: dict) -> int:
    """매집 강도 점수 (pension_finance_scan._calc_pension_score와 동일 공식, actor만 기관계)."""
    score = (
        entry["pension_buy_days"] * 15          # 기관계 매수일수 × 15
        + entry["pension_cum"] / 10             # 기관계 누적(억) / 10
        + min(max(entry["fi_today"], 0) * 2, 60)  # 외국인 오늘 합류 강도 (cap 60)
        + (20 if entry["fi_joined"] == "TODAY"
           else 10 if entry["fi_joined"] == "YESTERDAY" else 0)
        + (15 if entry["ret5"] < 5 else 0)      # 덜오름 = 매수여지
    )
    return round(score)


def _dedup_sorted(rows: list) -> list:
    """date 오름차순 정렬 + 중복일 제거(첫 등장 유지)."""
    rows = sorted(rows, key=lambda r: r.get("date", ""))
    seen, out = set(), []
    for r in rows:
        d = r.get("date", "")
        if d and d not in seen:
            seen.add(d)
            out.append(r)
    return out


def scan_institution_accum() -> Optional[dict]:
    """종목별 기관계 7일+ 매집(10일 윈도우) + 외국인 합류 스캔 (KRX-free).

    Returns: pension_finance_scan과 동일 스키마(date·counts·best/standby/ranked·entries) + source.
    """
    if not FLOW_DIR.exists():
        logger.warning("flow 디렉토리 없음 — fill 수급 수집 필요")
        return None
    try:
        uni = json.loads(UNI_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"universe 로드 실패: {e}")
        return None

    best_stocks, standby_stocks = [], []
    latest_date = ""

    for csv_path in FLOW_DIR.glob("*_investor.csv"):
        code = csv_path.name.replace("_investor.csv", "")
        info = uni.get(code, {})
        if not isinstance(info, dict):
            continue
        cap = info.get("cap_억", 0)
        if not cap or cap < 1000:
            continue
        name = info.get("name", "")
        if any(name.startswith(p) for p in ETF_PREFIXES):
            continue

        try:
            rows = _dedup_sorted(_parse_flow_csv(csv_path))
        except Exception:
            continue
        if len(rows) < ACCUM_MIN_DAYS:
            continue

        window = rows[-WINDOW_DAYS:]
        # 기관계 매수일수/누적 (백만원 → 억)
        inst_days = sum(1 for r in window if _csv_float(r, "기관_금액") > 0)
        if inst_days < ACCUM_MIN_DAYS:
            continue
        inst_cum_eok = sum(
            _csv_float(r, "기관_금액") for r in window if _csv_float(r, "기관_금액") > 0
        ) / BAEKMAN_TO_EOK

        # 외국인 합류 (오늘/어제) — 백만원 → 억
        frgn_today = _csv_float(rows[-1], "외국인_금액") / BAEKMAN_TO_EOK
        frgn_yest = _csv_float(rows[-2], "외국인_금액") / BAEKMAN_TO_EOK if len(rows) >= 2 else 0.0
        frgn_3d = sum(_csv_float(r, "외국인_금액") for r in rows[-3:]) / BAEKMAN_TO_EOK
        if frgn_today > 0:
            fi_joined = "TODAY"
        elif frgn_yest > 0:
            fi_joined = "YESTERDAY"
        else:
            fi_joined = ""

        # 5d 수익률
        ret5, close_now = 0.0, 0
        if len(rows) >= 5:
            c_now = _csv_float(rows[-1], "종가")
            c_5 = _csv_float(rows[-5], "종가")
            if c_now > 0 and c_5 > 0:
                ret5 = ((c_now / c_5) - 1) * 100
                close_now = int(c_now)

        latest_date = rows[-1].get("date", latest_date)

        entry = {
            "code": code,
            "name": name,
            "sector": info.get("sector", ""),
            "cap": cap,
            "pension_buy_days": inst_days,          # = 기관계 매수일수 (스키마 호환 키)
            "pension_cum": round(inst_cum_eok, 1),  # = 기관계 누적(억)
            "fi_today": round(frgn_today, 1),       # = 외국인 오늘(억, 합류축)
            "fi_3d": round(frgn_3d, 1),
            "fi_joined": fi_joined,                 # 외국인 합류 라벨
            "frgn_today": round(frgn_today, 1),
            "frgn_joined": frgn_today >= 1.0,
            "ret5": round(ret5, 1),
            "close": close_now,
            "source": SOURCE,                       # ★ 기관계 proxy 명시
        }
        entry["pension_score"] = _calc_accum_score(entry)

        # 등급: 기관 매집 + 외인 합류 타이밍 × 덜오름 (엣지는 forward shadow 재검증 대상)
        # ★ 외인이 유일 합류축 → TODAY/YESTERDAY/덜오름 조합으로 정의(원본의 금투/외인 2축 구조 미러링 시
        #   grade B가 논리적 불가(dead)였던 것 수정 — B=외인 어제합류로 reachable하게 재정의)
        _is_early = ret5 < 5.0
        if fi_joined == "TODAY" and _is_early:
            entry["pension_grade"] = "S"   # 외인 오늘 합류 + 덜오름(매수여지)
        elif fi_joined == "TODAY":
            entry["pension_grade"] = "A"   # 외인 오늘 합류(이미 오름)
        elif fi_joined == "YESTERDAY":
            entry["pension_grade"] = "B"   # 외인 어제 합류
        else:
            entry["pension_grade"] = "C"   # 외인 미합류(standby)

        if fi_joined in ("TODAY", "YESTERDAY"):
            best_stocks.append(entry)
        elif fi_joined == "" and ret5 < 5:
            standby_stocks.append(entry)

    best_stocks.sort(key=lambda x: -x["pension_cum"])
    standby_stocks.sort(key=lambda x: -x["pension_cum"])
    best_fresh = [s for s in best_stocks if s["ret5"] < 5]
    all_scored = best_stocks + standby_stocks
    all_scored.sort(key=lambda x: -x["pension_score"])
    ranked_stocks = all_scored[:10]

    result = {
        "date": latest_date,   # flow csv는 이미 YYYY-MM-DD
        "source": SOURCE,
        "total_count": len(best_stocks) + len(standby_stocks),
        "best_count": len(best_stocks),
        "best_fresh_count": len(best_fresh),
        "standby_count": len(standby_stocks),
        "best_stocks": best_stocks[:50],
        "best_fresh": best_fresh[:30],
        "standby_stocks": standby_stocks[:30],
        "ranked_stocks": ranked_stocks,
    }

    # 로컬 JSON (morning_recommendation pension_grade 참조 — 기존 pension_scan.json과 별도 보존)
    try:
        (DATA_STORE / "institution_accum_scan.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as e:
        logger.warning(f"institution_accum_scan.json 저장 실패: {e}")

    if ranked_stocks:
        s_cnt = sum(1 for s in all_scored if s.get("pension_grade") == "S")
        a_cnt = sum(1 for s in all_scored if s.get("pension_grade") == "A")
        logger.info(
            f"기관매집스캔(KRX-free) 완료 [{latest_date}]: 핵심 {len(best_stocks)} "
            f"(덜오른 {len(best_fresh)}) / 대기 {len(standby_stocks)} "
            f"/ TOP1={ranked_stocks[0]['name']}({ranked_stocks[0]['pension_score']}점) "
            f"/ 등급 S={s_cnt} A={a_cnt}")
    else:
        logger.info(f"기관매집스캔(KRX-free) 완료 [{latest_date}]: 0종목")
    return result


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    r = scan_institution_accum()
    if r:
        print(f"\n기준일: {r['date']} (source={r['source']}, KRX-free)")
        print(f"핵심후보: {r['best_count']}종목 (덜오른것 {r['best_fresh_count']}) / 대기: {r['standby_count']}\n")
        print("=== TOP 랭킹 (기관 매집 강도) ===")
        for i, s in enumerate(r.get("ranked_stocks", []), 1):
            print(f"  {i:>2}. {s['name']:12s} {s['pension_score']:>3}점 "
                  f"기관{s['pension_buy_days']}d 누적{s['pension_cum']:>+.0f}억 "
                  f"외인{s['fi_today']:>+.0f}억 합류={s['fi_joined'] or '-':9s} 5d{s['ret5']:>+.1f}%")
