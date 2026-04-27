# -*- coding: utf-8 -*-
"""기관 연속매수 초기 감지 스캔.

시총 1000억+ 종목 중 기관 3일+ 연속 순매수 종목을 찾아,
아직 급등 전(5일 수익 < 15%)인 "초기 매집" 종목을 감지한다.

사용:
    from data.inst_accumulation_scan import scan_inst_accumulation
    result = scan_inst_accumulation()  # dict
"""
import json
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger("BH.InstAccScan")

DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"

# 기존 유틸 사용 (규칙 C2: 새 함수 전 기존 유틸 확인)
try:
    from tools.flow_intelligence import _parse_flow_csv, _csv_float
except ImportError:
    # standalone 실행 시 fallback
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from tools.flow_intelligence import _parse_flow_csv, _csv_float


def _count_consecutive(rows: list[dict], key: str) -> int:
    """최근부터 역순으로 연속 양수 일수."""
    count = 0
    for r in reversed(rows):
        if _csv_float(r, key) > 0:
            count += 1
        else:
            break
    return count


def scan_inst_accumulation(
    min_cap: int = 1000,
    min_consec: int = 3,
    early_threshold: float = 15.0,
    top_n_early: int = 30,
    top_n_running: int = 10,
) -> dict:
    """기관 연속매수 초기 감지 스캔 실행.

    Args:
        min_cap: 최소 시총 (억 단위)
        min_consec: 최소 기관 연속매수 일수
        early_threshold: 초기 판단 기준 (5일 수익률 %)
        top_n_early: 초기 종목 최대 반환 수
        top_n_running: 진행 중 종목 최대 반환 수

    Returns:
        dict with keys: date, total_count, early_count, running_count,
                        early_stocks (list), running_stocks (list)
    """
    # universe 로드
    uni_path = DATA_STORE / "universe.json"
    if not uni_path.exists():
        logger.error("universe.json 없음")
        return {}

    uni = json.loads(uni_path.read_text(encoding="utf-8"))
    mcap_map = {}
    for code, info in uni.items():
        if not isinstance(info, dict):
            continue
        cap = info.get("cap_억", 0)
        if cap and cap >= min_cap:
            mcap_map[code] = {
                "name": info.get("name", ""),
                "cap": cap,
                "sector": info.get("sector", ""),
            }

    logger.info(f"시총 {min_cap}억+ 종목: {len(mcap_map)}개")

    # CSV 스캔
    flow_dir = DATA_STORE / "flow"
    results = []

    for csv_path in flow_dir.glob("*_investor.csv"):
        code = csv_path.stem.replace("_investor", "")
        if code.startswith("_"):
            continue
        info = mcap_map.get(code)
        if not info:
            continue

        try:
            rows = _parse_flow_csv(csv_path)
        except Exception:
            continue
        if len(rows) < 5:
            continue

        # [H1] 날짜 정렬 + 중복 제거 (concat 데이터 정합성)
        rows.sort(key=lambda r: r.get("date", ""))
        seen_dates = set()
        deduped = []
        for r in rows:
            d = r.get("date", "")
            if d and d not in seen_dates:
                seen_dates.add(d)
                deduped.append(r)
        rows = deduped
        if len(rows) < 5:
            continue

        # 기관 연속매수 일수
        inst_consec = _count_consecutive(rows, "기관_금액")
        if inst_consec < min_consec:
            continue

        # 기관 누적 (백만원 → 억)
        inst_cum = sum(
            _csv_float(r, "기관_금액") for r in rows[-inst_consec:]
        ) / 100

        # 외인 연속매수 일수
        frgn_consec = _count_consecutive(rows, "외국인_금액")

        # 수익률
        close_now = int(_csv_float(rows[-1], "종가"))
        # [H2] 거래정지/데이터오류(종가=0) 필터
        if close_now <= 0:
            continue
        close_5 = int(_csv_float(rows[-5], "종가")) if len(rows) >= 5 else 0
        close_10 = int(_csv_float(rows[-10], "종가")) if len(rows) >= 10 else 0

        ret5 = ((close_now / close_5) - 1) * 100 if close_5 else 0
        ret10 = ((close_now / close_10) - 1) * 100 if close_10 else 0

        today_inst = _csv_float(rows[-1], "기관_금액") / 100
        today_frgn = _csv_float(rows[-1], "외국인_금액") / 100

        # 태그
        tag = ""
        if frgn_consec >= 3:
            tag = "DUAL"
        elif frgn_consec >= 1:
            tag = "FRGN"

        results.append({
            "code": code,
            "name": info["name"],
            "sector": info["sector"],
            "cap": info["cap"],
            "inst_consec": inst_consec,
            "inst_cum": round(inst_cum, 1),
            "frgn_consec": frgn_consec,
            "ret5": round(ret5, 1),
            "ret10": round(ret10, 1),
            "today_inst": round(today_inst, 1),
            "today_frgn": round(today_frgn, 1),
            "close": close_now,
            "tag": tag,
            "last_date": rows[-1].get("date", ""),
        })

    # 분류
    early = [r for r in results if r["ret5"] < early_threshold]
    running = [r for r in results if r["ret5"] >= early_threshold]

    early.sort(key=lambda x: (x["inst_consec"], x["inst_cum"]), reverse=True)
    running.sort(key=lambda x: x["inst_consec"], reverse=True)

    today_str = date.today().isoformat()
    all_stocks = early or running
    data_date = all_stocks[0]["last_date"] if all_stocks else today_str

    output = {
        "date": data_date,
        "total_count": len(results),
        "early_count": len(early),
        "running_count": len(running),
        "early_stocks": early[:top_n_early],
        "running_stocks": running[:top_n_running],
    }

    logger.info(
        f"기관 연속매수 스캔 완료: {len(results)}종목 "
        f"(초기 {len(early)}, 진행 중 {len(running)})"
    )
    return output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = scan_inst_accumulation()
    if result:
        print(f"\n기관 3일+ 연속매수: 총 {result['total_count']}종목")
        print(f"초기(5일<15%): {result['early_count']}종목\n")
        for r in result["early_stocks"][:20]:
            tag = f" *{r['tag']}" if r["tag"] else ""
            print(
                f"  {r['name']:12} {r['sector']:6} "
                f"시총{r['cap']:>7,}억 기관{r['inst_consec']:>2}d "
                f"누적{r['inst_cum']:>+8.1f}억 외인{r['frgn_consec']}d "
                f"5d{r['ret5']:>+5.1f}% 10d{r['ret10']:>+5.1f}%{tag}"
            )
