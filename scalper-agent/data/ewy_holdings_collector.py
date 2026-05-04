# -*- coding: utf-8 -*-
"""EWY (iShares MSCI South Korea ETF) 보유종목 수집 + 변동 분석.

매일 07:30 실행 — iShares 공식 CSV에서 ~100종목 보유 비중을 수집하고
전일 데이터와 비교하여 비중 변동/신규편입/편출을 감지한다.

출력:
  data_store/ewy_holdings.json      — 오늘 수집 결과
  data_store/ewy_holdings_prev.json — 전일 백업 (비교용)

COO 배선: G1 A11B (미국장 마감 후, us_overnight_filter 이후)
"""
import csv
import io
import json
import logging
import urllib.request
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.EWY")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
EWY_PATH = DATA_DIR / "ewy_holdings.json"
EWY_PREV_PATH = DATA_DIR / "ewy_holdings_prev.json"
UNIVERSE_PATH = DATA_DIR / "universe.json"

EWY_CSV_URL = (
    "https://www.ishares.com/us/products/239681/"
    "ishares-msci-south-korea-etf/1467271812596.ajax"
    "?fileType=csv&fileName=EWY_holdings&dataType=fund"
)

# GICS 섹터 → 한국 섹터명 매핑
_SECTOR_KR = {
    "Information Technology": "전기전자",
    "Consumer Discretionary": "운송장비",
    "Industrials": "산업재",
    "Financials": "금융",
    "Materials": "화학",
    "Communication Services": "통신",
    "Energy": "에너지",
    "Health Care": "의료",
    "Consumer Staples": "필수소비재",
    "Utilities": "유틸리티",
    "Real Estate": "부동산",
}


def _load_universe() -> dict:
    """universe.json에서 코드→이름 매핑 로드."""
    if not UNIVERSE_PATH.exists():
        return {}
    try:
        data = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
        return data
    except Exception:
        return {}


def _download_csv() -> str:
    """iShares CSV 다운로드 → raw 텍스트 반환."""
    req = urllib.request.Request(
        EWY_CSV_URL,
        headers={"User-Agent": "Mozilla/5.0 (BodyHunter/4.0)"},
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return resp.read().decode("utf-8-sig", errors="replace")


def _parse_csv(raw: str, universe: dict) -> dict:
    """CSV 파싱 → 구조화된 데이터 반환.

    Returns:
        {
            "as_of": "May 01, 2026",
            "stocks": {
                "000660": {
                    "code": "000660",
                    "name": "SK하이닉스",
                    "name_en": "SK HYNIX INC",
                    "weight": 22.78,
                    "quantity": 5382737,
                    "sector": "Information Technology",
                    "sector_kr": "전기전자",
                    "market_value": 4667071050.43,
                },
                ...
            }
        }
    """
    lines = raw.split("\n")

    # 기준일 추출: "Fund Holdings as of","May 01, 2026"
    as_of = ""
    for line in lines[:10]:
        if "Fund Holdings as of" in line:
            # 메타 헤더에서 기준일 추출: 'Fund Holdings as of,"May 01, 2026"'
            _comma_pos = line.find(",")
            if _comma_pos >= 0:
                as_of = line[_comma_pos + 1:].strip().strip('"')
            break

    # 헤더 행 찾기
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Ticker,"):
            header_idx = i
            break

    if header_idx is None:
        logger.error("CSV 헤더를 찾을 수 없음")
        return {}

    # CSV 파싱
    data_text = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data_text))

    stocks = {}
    for row in reader:
        ticker = (row.get("Ticker") or "").strip()
        if not ticker:
            continue

        asset_class = (row.get("Asset Class") or "").strip()
        if asset_class != "Equity":
            continue

        # KRW 시장 종목만 (Market Currency 기준)
        market_currency = (row.get("Market Currency") or "").strip()
        if market_currency != "KRW":
            continue

        # 6자리 숫자 코드 검증
        if not ticker.isdigit() or len(ticker) != 6:
            continue

        try:
            weight = float((row.get("Weight (%)") or "0").strip())
        except ValueError:
            continue

        try:
            quantity_str = (row.get("Quantity") or "0").replace(",", "").strip()
            quantity = int(float(quantity_str))
        except (ValueError, TypeError):
            quantity = 0

        try:
            mv_str = (row.get("Market Value") or "0").replace(",", "").strip()
            market_value = float(mv_str)
        except (ValueError, TypeError):
            market_value = 0.0

        name_en = (row.get("Name") or "").strip()
        sector = (row.get("Sector") or "").strip()
        sector_kr = _SECTOR_KR.get(sector, sector)

        # universe.json에서 한글 이름 가져오기
        uni_info = universe.get(ticker, {})
        name_kr = uni_info.get("name", "")
        if uni_info.get("sector"):
            sector_kr = uni_info["sector"]

        stocks[ticker] = {
            "code": ticker,
            "name": name_kr or name_en,
            "name_en": name_en,
            "weight": weight,
            "quantity": quantity,
            "sector": sector,
            "sector_kr": sector_kr,
            "market_value": round(market_value, 2),
        }

    return {"as_of": as_of, "stocks": stocks}


def _compare(today: dict, prev: dict) -> dict:
    """전일 대비 변동 분석.

    Returns:
        {
            "changes": [...],       # 비중 변동 종목 (0.05%+ 변화)
            "new_entries": [...],   # 신규 편입
            "removed": [...],      # 편출
        }
    """
    today_stocks = today.get("stocks", {})
    prev_stocks = prev.get("stocks", {})

    today_codes = set(today_stocks.keys())
    prev_codes = set(prev_stocks.keys())

    # 신규 편입
    new_entries = []
    for code in sorted(today_codes - prev_codes):
        s = today_stocks[code]
        new_entries.append({
            "code": code,
            "name": s["name"],
            "weight": s["weight"],
            "sector": s["sector"],
            "sector_kr": s["sector_kr"],
            "impact": "패시브 강제매수 예상",
        })

    # 편출
    removed = []
    for code in sorted(prev_codes - today_codes):
        s = prev_stocks[code]
        removed.append({
            "code": code,
            "name": s["name"],
            "weight": s.get("weight", 0),
            "sector": s.get("sector", ""),
            "sector_kr": s.get("sector_kr", ""),
            "impact": "패시브 강제매도 예상",
        })

    # 비중 변동
    changes = []
    for code in sorted(today_codes & prev_codes):
        t = today_stocks[code]
        p = prev_stocks[code]
        w_change = round(t["weight"] - p["weight"], 4)

        if abs(w_change) < 0.05:
            direction = "STABLE"
        elif w_change > 0:
            direction = "UP"
        else:
            direction = "DOWN"

        if abs(w_change) >= 0.3:
            magnitude = "LARGE"
        elif abs(w_change) >= 0.1:
            magnitude = "MEDIUM"
        elif abs(w_change) >= 0.05:
            magnitude = "SMALL"
        else:
            magnitude = "NONE"

        if magnitude == "NONE":
            continue

        changes.append({
            "code": code,
            "name": t["name"],
            "weight": t["weight"],
            "weight_prev": p["weight"],
            "weight_change": round(w_change, 4),
            "direction": direction,
            "magnitude": magnitude,
        })

    # 변동 크기 순 정렬
    changes.sort(key=lambda x: abs(x["weight_change"]), reverse=True)

    return {
        "changes": changes,
        "new_entries": new_entries,
        "removed": removed,
    }


def collect_ewy_holdings() -> dict:
    """EWY 보유종목 수집 + 변동 분석 메인 함수.

    Returns:
        {
            "date": "2026-05-04",
            "as_of": "May 01, 2026",
            "total_stocks": 92,
            "top20": [...],
            "changes": [...],
            "new_entries": [...],
            "removed": [...],
            "summary": "요약 텍스트",
            "stocks": { ... },  # 전체 종목 (저장용)
        }
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    universe = _load_universe()

    # 1. CSV 다운로드 + 파싱
    logger.info("[EWY] iShares CSV 다운로드 시작")
    try:
        raw = _download_csv()
    except Exception as e:
        logger.error(f"[EWY] CSV 다운로드 실패: {e}")
        return {}

    parsed = _parse_csv(raw, universe)
    if not parsed or not parsed.get("stocks"):
        logger.error("[EWY] CSV 파싱 실패 — 종목 0개")
        return {}

    stocks = parsed["stocks"]
    as_of = parsed["as_of"]
    logger.info(f"[EWY] 파싱 완료: {len(stocks)}종목, 기준일={as_of}")

    # 2. 전일 데이터 로드 + 비교
    prev_data = {}
    if EWY_PATH.exists():
        try:
            prev_data = json.loads(EWY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    comparison = _compare(parsed, prev_data.get("_raw", prev_data))
    changes = comparison["changes"]
    new_entries = comparison["new_entries"]
    removed = comparison["removed"]

    # 3. TOP20 생성
    sorted_stocks = sorted(
        stocks.values(), key=lambda x: x["weight"], reverse=True
    )
    top20 = []
    for i, s in enumerate(sorted_stocks[:20], 1):
        prev_stock = prev_data.get("_raw", prev_data).get("stocks", {}).get(s["code"], {})
        w_prev = prev_stock.get("weight", 0)
        w_change = round(s["weight"] - w_prev, 4) if w_prev else 0

        signal = "STABLE"
        if w_change >= 0.1:
            signal = "UP"
        elif w_change <= -0.1:
            signal = "DOWN"

        top20.append({
            "rank": i,
            "code": s["code"],
            "name": s["name"],
            "name_en": s["name_en"],
            "weight": s["weight"],
            "weight_prev": round(w_prev, 4),
            "weight_change": round(w_change, 4),
            "quantity": s["quantity"],
            "sector": s["sector"],
            "sector_kr": s["sector_kr"],
            "signal": signal,
        })

    # 4. 요약 텍스트 생성
    summary_parts = []
    if top20:
        t1 = top20[0]
        summary_parts.append(f"{t1['name']} 비중 {t1['weight']}%로 최대")

    large_ups = [c for c in changes if c["direction"] == "UP" and c["magnitude"] == "LARGE"]
    large_downs = [c for c in changes if c["direction"] == "DOWN" and c["magnitude"] == "LARGE"]
    if large_ups:
        names = ", ".join(c["name"] for c in large_ups[:3])
        summary_parts.append(f"{names} 비중 대폭 증가")
    if large_downs:
        names = ", ".join(c["name"] for c in large_downs[:3])
        summary_parts.append(f"{names} 비중 대폭 감소")
    if new_entries:
        names = ", ".join(e["name"] for e in new_entries[:3])
        summary_parts.append(f"신규편입: {names}")
    if removed:
        names = ", ".join(r["name"] for r in removed[:3])
        summary_parts.append(f"편출: {names}")

    # 섹터 비중 합산
    sector_weight = {}
    for s in stocks.values():
        sk = s["sector_kr"]
        sector_weight[sk] = sector_weight.get(sk, 0) + s["weight"]
    top_sector = max(sector_weight.items(), key=lambda x: x[1], default=("?", 0))
    if top_sector[1] > 0:
        summary_parts.append(f"{top_sector[0]} 섹터 비중 {top_sector[1]:.1f}%")

    summary = ". ".join(summary_parts) + "." if summary_parts else "변동 없음."

    # 5. 결과 조립
    result = {
        "date": today_str,
        "as_of": as_of,
        "total_stocks": len(stocks),
        "top20": top20,
        "changes": changes,
        "new_entries": new_entries,
        "removed": removed,
        "summary": summary,
        # 내부용 (다음날 비교를 위해 전체 저장)
        "_raw": {"as_of": as_of, "stocks": stocks},
    }

    # 6. 파일 저장 (현재 → prev 백업 후 덮어쓰기)
    if EWY_PATH.exists():
        try:
            EWY_PREV_PATH.write_text(
                EWY_PATH.read_text(encoding="utf-8"), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"[EWY] prev 백업 실패: {e}")

    EWY_PATH.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(
        f"[EWY] 저장 완료: {len(stocks)}종목 | "
        f"변동 {len(changes)}건 | 편입 {len(new_entries)}건 | 편출 {len(removed)}건"
    )

    return result


# ── 직접 실행 ──
if __name__ == "__main__":
    import sys
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )
    result = collect_ewy_holdings()
    if result:
        print(f"\n{'=' * 60}")
        print(f"  EWY 보유종목 수집 완료")
        print(f"{'=' * 60}")
        print(f"  기준일: {result['as_of']}")
        print(f"  종목수: {result['total_stocks']}")
        print()
        print("  [TOP 10 비중]")
        for t in result["top20"][:10]:
            ch = f"{t['weight_change']:+.2f}%" if t["weight_prev"] else "  신규"
            print(f"  {t['rank']:2d}. {t['name']:<12s} {t['weight']:>6.2f}% ({ch}) {t['sector_kr']}")
        print()
        if result["changes"]:
            print(f"  [주요 변동] {len(result['changes'])}건")
            for c in result["changes"][:10]:
                arrow = "▲" if c["direction"] == "UP" else "▼"
                print(f"  {arrow} {c['name']:<12s} {c['weight']:>6.2f}% ({c['weight_change']:+.4f}%) [{c['magnitude']}]")
        if result["new_entries"]:
            print(f"\n  [신규 편입] {len(result['new_entries'])}건")
            for e in result["new_entries"]:
                print(f"  ★ {e['name']} {e['weight']:.2f}%")
        if result["removed"]:
            print(f"\n  [편출] {len(result['removed'])}건")
            for r in result["removed"]:
                print(f"  ✕ {r['name']} {r['weight']:.2f}%")
        print(f"\n  요약: {result['summary']}")
    else:
        print("수집 실패")
