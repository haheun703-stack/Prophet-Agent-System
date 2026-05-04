# -*- coding: utf-8 -*-
"""연기금 지분율 추적기 — 국민연금 보유 추이 vs 주가 상관분석.

FLOWX 패널명: "연기금 지분 추적기"

목적:
  1. 국민연금 지분율 5%+ 공시 종목 추적
  2. 매일 연기금 순매수 누적 → 추정 지분율 변화 계산
  3. 연기금 매수 추이 vs 주가 변화 상관관계 분석
  4. 지분율 상승 종목 = 주가 상승? 가설 검증

데이터 소스:
  - DART 공시 기준 지분율 (NPS_BASE_OWNERSHIP, 수동 업데이트)
  - quant_investor_extra.json (매일 연기금 순매수, VPS 갱신)
  - flow/*_investor.csv (종가 추이)

COO G7 Stage4 C42에서 호출:
  from data.pension_ownership_tracker import track_pension_ownership
  result = track_pension_ownership()
"""
import json
import logging
from pathlib import Path
from datetime import datetime

from tools.flow_intelligence import _parse_flow_csv

logger = logging.getLogger("BH.PensionOwnership")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
QUANT_JSON = DATA_STORE / "quant_investor_extra.json"
FLOW_DIR = DATA_STORE / "flow"
UNI_PATH = DATA_STORE / "universe.json"
TRACKER_PATH = DATA_STORE / "pension_ownership.json"

# ──────────────────────────────────────────────────────────
# 국민연금 지분율 5%+ 공시 기준 (2025-10 / 2026-03 DART 공시)
# ※ 새 공시가 나올 때마다 이 딕셔너리를 업데이트
# key: 종목코드, value: (지분율%, 기준일, 보유주식수)
# ──────────────────────────────────────────────────────────
NPS_BASE_OWNERSHIP: dict[str, tuple[float, str, int]] = {
    # KOSPI — 지분율 10%+
    "007660": (13.86, "2025-03", 0),   # 이수페타시스
    "014680": (13.64, "2025-03", 0),   # 한솔케미칼
    "016360": (13.50, "2025-03", 0),   # 삼성증권
    "161890": (13.48, "2025-03", 0),   # 한국콜마
    "192820": (13.36, "2025-03", 0),   # 코스맥스
    "039490": (13.14, "2025-03", 0),   # 키움증권
    "298040": (12.83, "2025-03", 0),   # 효성중공업
    "001040": (12.72, "2025-03", 0),   # CJ
    "139480": (12.71, "2025-03", 0),   # 이마트
    "006260": (12.67, "2025-03", 0),   # LS
    "002380": (12.49, "2025-03", 0),   # KCC
    "204320": (12.28, "2025-03", 0),   # HD현대인프라코어
    "241560": (12.18, "2025-03", 0),   # 코스메카코리아
    "204210": (12.14, "2025-03", 0),   # HL만도
    "071050": (12.12, "2025-03", 0),   # 한국투자금융지주
    "069960": (11.76, "2025-03", 0),   # 현대백화점
    "097950": (11.69, "2025-03", 0),   # CJ제일제당
    "375500": (11.68, "2025-03", 0),   # 디엘이앤씨
    "003300": (11.48, "2025-03", 0),   # 세아제강지주
    "004170": (11.39, "2025-03", 0),   # 신세계
    "011780": (11.11, "2025-03", 0),   # 금호석유화학
    "000120": (11.09, "2025-03", 0),   # CJ대한통운
    "069620": (10.93, "2025-03", 0),   # 대웅제약
    "004370": (10.89, "2025-03", 0),   # 농심
    "000210": (10.69, "2025-03", 0),   # DL
    "010620": (10.68, "2025-03", 0),   # HD현대미포
    "030000": (10.68, "2025-03", 0),   # 제일기획
    "034120": (10.67, "2025-03", 0),   # SBS
    "271560": (10.67, "2025-03", 0),   # 오리온
    "009150": (10.62, "2025-03", 0),   # 삼성전기
    "082740": (10.60, "2025-03", 0),   # 한화엔진
    "111770": (10.40, "2025-03", 0),   # 영원무역
    "077970": (10.28, "2025-03", 0),   # STX엔진
    "128940": (10.20, "2025-03", 0),   # 한미약품
    "105630": (10.16, "2025-03", 0),   # 한세실업
    "003230": (10.11, "2025-03", 0),   # 삼양식품
    "001120": (10.12, "2025-03", 0),   # LX인터내셔널
    # KOSPI — 지분율 5~10%
    "035420": (9.24, "2025-03", 0),    # NAVER
    "012330": (9.06, "2025-03", 0),    # 현대모비스
    "105560": (8.94, "2025-03", 0),    # KB금융
    "010120": (8.79, "2025-03", 0),    # LS일렉트릭
    "055550": (8.60, "2025-03", 0),    # 신한지주
    "000660": (7.55, "2025-03", 0),    # SK하이닉스
    "005380": (7.55, "2025-03", 0),    # 현대차
    "267250": (7.38, "2025-03", 0),    # HD현대중공업
    "005930": (7.26, "2025-03", 0),    # 삼성전자
    "000270": (7.14, "2025-03", 0),    # 기아
    "068270": (6.79, "2025-03", 0),    # 셀트리온
    # KOSDAQ — 지분율 5%+
    "213420": (8.97, "2025-03", 0),    # 덕산네오룩스
    "104830": (8.64, "2025-03", 0),    # 원익머트리얼즈
    "228340": (7.08, "2025-03", 0),    # 피에스케이
    "122870": (6.37, "2025-03", 0),    # 비나텍
    "078150": (6.26, "2025-03", 0),    # 에이티아이
    "361610": (6.26, "2025-03", 0),    # 에스티아이
    "095610": (6.11, "2025-03", 0),    # 테스
    "025320": (6.08, "2025-03", 0),    # 진시스템
    "035900": (6.06, "2025-03", 0),    # JYP엔터
    "141080": (6.03, "2025-03", 0),    # 리가켐바이오
    "064760": (6.03, "2025-03", 0),    # 티씨케이
    "056190": (5.35, "2025-03", 0),    # 에스에프에이
    "148150": (5.16, "2025-03", 0),    # 디어유
    "095340": (5.15, "2025-03", 0),    # ISC
    "195940": (5.12, "2025-03", 0),    # HK이노엔
    "228850": (5.10, "2025-03", 0),    # RFHIC
    "054040": (5.10, "2025-03", 0),    # 에이치브이엠
    "225570": (5.09, "2025-03", 0),    # AP시스템
    "138080": (5.09, "2025-03", 0),    # 미코
    "226350": (5.07, "2025-03", 0),    # 성광벤드
    "190180": (5.06, "2025-03", 0),    # 브이티
    "030520": (5.05, "2025-03", 0),    # 한글과컴퓨터
    "049950": (5.04, "2025-03", 0),    # 코미코
    "036540": (5.03, "2025-03", 0),    # 감성코퍼레이션
    "101490": (5.02, "2025-03", 0),    # 에스앤에스텍
    "357780": (5.02, "2025-03", 0),    # 솔브레인
    "084370": (5.01, "2025-03", 0),    # 유진테크
    "272210": (5.01, "2025-03", 0),    # 이녹스첨단소재
    "023000": (5.00, "2025-03", 0),    # 태광
}


def _load_quant_data() -> dict:
    """quant_investor_extra.json 로드."""
    if not QUANT_JSON.exists():
        logger.warning(f"quant_investor_extra.json 없음")
        return {}
    with open(QUANT_JSON, "r", encoding="utf-8") as f:
        return json.load(f).get("daily", {})


def _get_price_history(code: str) -> list[tuple[str, float]]:
    """flow CSV에서 종가 추이 추출. [(date, close), ...]

    C1 규칙: _parse_flow_csv 유틸 사용 — raw split 인덱스 금지.
    """
    csv_path = FLOW_DIR / f"{code}_investor.csv"
    if not csv_path.exists():
        return []
    rows = _parse_flow_csv(csv_path)
    prices = []
    for row in rows:
        date_str = row.get("날짜", row.get("date", ""))
        close_raw = row.get("종가", row.get("close", 0))
        try:
            close = float(str(close_raw).replace(",", ""))
            if date_str and close > 0:
                prices.append((date_str, close))
        except (ValueError, TypeError):
            continue
    return sorted(prices, key=lambda x: x[0])


def _calc_correlation(xs: list[float], ys: list[float]) -> float:
    """단순 피어슨 상관계수 (numpy 없이)."""
    n = len(xs)
    if n < 3:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    std_x = (sum((x - mean_x) ** 2 for x in xs)) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in ys)) ** 0.5
    if std_x == 0 or std_y == 0:
        return 0.0
    return round(cov / (std_x * std_y), 3)


def track_pension_ownership() -> dict:
    """국민연금 보유 추적 메인 함수.

    Returns:
        dict: {
            "date": "2026-05-04",
            "total_tracked": 65,
            "stocks": [...],
            "summary": {...},
        }
    """
    today = datetime.now().strftime("%Y-%m-%d")
    quant = _load_quant_data()

    # universe 로드 (시총 참조)
    uni = {}
    if UNI_PATH.exists():
        with open(UNI_PATH, "r", encoding="utf-8") as f:
            uni = json.load(f)

    results = []

    for code, (base_pct, base_date, _shares) in NPS_BASE_OWNERSHIP.items():
        name = uni.get(code, {}).get("name", "?")
        cap = uni.get(code, {}).get("cap_億", 0)

        # 연기금 순매수 데이터
        stock_data = quant.get(code, {})
        dates = stock_data.get("dates", {})
        sorted_dates = sorted(dates.keys())

        if not sorted_dates:
            # quant 데이터 없어도 기초 지분율은 기록
            results.append({
                "code": code,
                "name": name,
                "base_ownership_pct": base_pct,
                "base_date": base_date,
                "cap_b": cap,
                "pension_net_10d": 0,
                "pension_flow_trend": "NO_DATA",
                "price_change_pct": 0,
                "correlation": 0,
                "sector": uni.get(code, {}).get("sector", "?"),
            })
            continue

        # 연기금 순매수 누적 (전체 기간)
        pension_vals = [dates[d].get("pension_net", 0) for d in sorted_dates]
        pension_cum = [sum(pension_vals[:i+1]) for i in range(len(pension_vals))]

        # 최근 10일 / 5일 순매수
        net_10d = sum(pension_vals[-10:]) if len(pension_vals) >= 10 else sum(pension_vals)
        net_5d = sum(pension_vals[-5:]) if len(pension_vals) >= 5 else sum(pension_vals)

        # 매수/매도일 카운트
        buy_days = sum(1 for v in pension_vals if v > 0)
        sell_days = sum(1 for v in pension_vals if v < 0)

        # 추세 판정
        if net_10d > 5:
            trend = "ACCUMULATING"   # 매집 중
        elif net_10d < -5:
            trend = "DISTRIBUTING"   # 매도 중
        else:
            trend = "FLAT"           # 보합

        # 종가 추이
        prices = _get_price_history(code)
        price_change_pct = 0.0
        if len(prices) >= 2:
            p_start = prices[max(0, len(prices) - 10)][1]
            p_end = prices[-1][1]
            if p_start > 0:
                price_change_pct = round((p_end / p_start - 1) * 100, 2)

        # 상관관계: 연기금 누적 vs 종가
        corr = 0.0
        min_len = min(len(pension_cum), len(prices))
        if min_len >= 5:
            # 최근 N일 정렬 매칭
            recent_cum = pension_cum[-min_len:]
            recent_prices = [p[1] for p in prices[-min_len:]]
            corr = _calc_correlation(recent_cum, recent_prices)

        results.append({
            "code": code,
            "name": name,
            "base_ownership_pct": base_pct,
            "base_date": base_date,
            "cap_b": cap,
            "pension_net_10d": round(net_10d, 1),
            "pension_net_5d": round(net_5d, 1),
            "pension_buy_days": buy_days,
            "pension_sell_days": sell_days,
            "pension_total_net": round(sum(pension_vals), 1),
            "pension_flow_trend": trend,
            "price_change_pct": price_change_pct,
            "correlation": corr,
            "sector": uni.get(code, {}).get("sector", "?"),
        })

    # 지분율 높은 순 정렬
    results.sort(key=lambda x: x["base_ownership_pct"], reverse=True)

    # 상관관계 요약
    valid_corrs = [r for r in results if abs(r["correlation"]) > 0.01]
    avg_corr = (sum(r["correlation"] for r in valid_corrs) / len(valid_corrs)
                if valid_corrs else 0)
    strong_pos = [r for r in valid_corrs if r["correlation"] >= 0.5]
    strong_neg = [r for r in valid_corrs if r["correlation"] <= -0.5]
    accumulating = [r for r in results if r["pension_flow_trend"] == "ACCUMULATING"]
    distributing = [r for r in results if r["pension_flow_trend"] == "DISTRIBUTING"]

    output = {
        "date": today,
        "generated_at": datetime.now().isoformat(),
        "total_tracked": len(results),
        "stocks": results,
        "summary": {
            "avg_correlation": round(avg_corr, 3),
            "strong_positive_corr": len(strong_pos),
            "strong_negative_corr": len(strong_neg),
            "accumulating_count": len(accumulating),
            "distributing_count": len(distributing),
            "top_accumulating": [
                {"name": r["name"], "net_10d": r["pension_net_10d"],
                 "price_chg": r["price_change_pct"], "corr": r["correlation"]}
                for r in sorted(accumulating, key=lambda x: x["pension_net_10d"], reverse=True)[:5]
            ],
            "top_distributing": [
                {"name": r["name"], "net_10d": r["pension_net_10d"],
                 "price_chg": r["price_change_pct"], "corr": r["correlation"]}
                for r in sorted(distributing, key=lambda x: x["pension_net_10d"])[:5]
            ],
        },
    }

    # 로컬 저장
    with open(TRACKER_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info(
        f"연기금보유추적 완료: {len(results)}종목 · "
        f"매집 {len(accumulating)} / 매도 {len(distributing)} · "
        f"상관계수 평균 {avg_corr:.3f}"
    )
    return output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = track_pension_ownership()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
