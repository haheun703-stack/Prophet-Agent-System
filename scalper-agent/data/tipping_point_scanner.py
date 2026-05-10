# -*- coding: utf-8 -*-
"""수급 임계점 스캐너 (Supply Tipping Point Scanner)
=================================================
외국인/기관/연기금/금투/개인/기타법인 6자 수급을 통합 분석하여
"한 번만 매수세가 유입되면 오를 수 있는" 임계 상태 종목을 감지.

핵심 개념:
  스마트머니(외인+기관+연기금+금투)가 조용히 매집하는 동안
  개인 매도가 소진되고, 거래량이 줄어들며 에너지가 축적된다.
  이 상태에서 한 번의 추가 매수가 들어오면 가격이 크게 반응한다.
  → "코일(Coil)" = 장전된 스프링.

5가지 지표 (최대 100점):
  ① 매집 강도 (Accumulation Intensity): 최대 30점
     스마트머니 10일 누적 순매수 / 일평균 거래대금
     → "며칠치 물량을 조용히 흡수했는가"

  ② 매도 소진도 (Sell Exhaustion): 최대 20점
     최근 3일 개인 순매도 / 10일 평균 순매도
     → 1.0 미만이면 "팔 사람이 줄고 있다"

  ③ 거래량 압축 (Volume Compression): 최대 15점
     최근 3일 거래대금 / 10일 평균 거래대금
     → 1.0 미만이면 "조용히 축적 중" (에너지 압축)

  ④ 다자 합류 (Cross Confirmation): 최대 15점
     오늘 순매수 중인 투자자 유형 수 (최대 6자)
     → 4자+ 동시 매수 = 강력한 합류 시그널

  ⑤ 지속 확률 (Continuation Probability): 최대 20점
     연속 매수일수 기반 내일 매수 지속 추정
     → 오래 쌓을수록 아직 포지션 미완성 = 계속 살 가능성

데이터 소스:
  - data_store/flow/{code}_investor.csv (외국인/기관/개인/기타법인 순매수 백만원)
  - data_store/quant_investor_extra.json (연기금/금투 순매수 억원)
  - data_store/universe.json (시총/거래량/섹터)

출력: data_store/tipping_scan.json
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.TippingPoint")

DATA_DIR = Path(__file__).parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"
QUANT_JSON = DATA_DIR / "quant_investor_extra.json"
UNI_PATH = DATA_DIR / "universe.json"
OUTPUT_PATH = DATA_DIR / "tipping_scan.json"

# ── 필터 상수 ──
MIN_CAP = 3000          # 시총 3,000억+ (소형잡주 제외)
MIN_ACCUM_DAYS = 2      # 스마트머니 최소 2일 연속매수
LOOKBACK = 10           # 분석 윈도우 (10거래일)
MAX_SURGE = 15.0        # 이미 15%+ 상승 → 제외
TOP_N = 30              # 최대 출력 건수

# 연속매수 N일째 → 지속 확률 추정 (한국 시장 기관/외인 패턴)
# 보수적 추정. "최소 이 정도는 된다"는 하한선.
CONTINUATION_PROB = {
    1: 0.42, 2: 0.52, 3: 0.60, 4: 0.57, 5: 0.55,
    6: 0.53, 7: 0.52, 8: 0.51, 9: 0.50, 10: 0.50,
}

# ETF 제외
ETF_PREFIXES = [
    "KODEX", "TIGER", "RISE", "PLUS", "KIWOOM", "KoAct", "TIME",
    "SOL", "HANARO", "KOSEF", "ACE", "ARIRANG", "BNK", "TIMEFOLIO",
    "FOCUS", "WOORI", "KB", "TREX",
]


def scan_tipping_point(min_cap: int = MIN_CAP, top_n: int = TOP_N) -> dict:
    """전종목 수급 임계점 스캔.

    Returns:
        {
            "timestamp": "2026-05-12 16:45:00",
            "total_scanned": int,
            "coiled": [...],      # 코일 상태 (가격 ±5%, 스프링 장전)
            "warming": [...],     # 움직임 시작 (+5~10%)
            "launched": [...],    # 이미 출발 (+10~15%)
            "summary": {...}
        }
    """
    # 1) 데이터 로드
    if not UNI_PATH.exists():
        logger.error("universe.json 없음")
        return {"error": "universe.json missing"}

    universe = json.loads(UNI_PATH.read_text("utf-8"))

    # 연기금/금투 데이터 (있으면 사용, 없으면 4자만 분석)
    pension_data = _load_pension_data()

    # 시총 필터 + ETF 제외
    targets = {}
    for code, info in universe.items():
        if not isinstance(info, dict):
            continue
        cap = info.get("cap_억", 0)
        if cap < min_cap:
            continue
        name = info.get("name", "")
        if any(name.startswith(p) for p in ETF_PREFIXES):
            continue
        targets[code] = info

    logger.info(f"[임계점] 시총 {min_cap}억+ 종목: {len(targets)}개")

    # 2) 각 종목 분석
    results = []
    scanned = 0

    for code, info in targets.items():
        csv_path = FLOW_DIR / f"{code}_investor.csv"
        if not csv_path.exists():
            continue
        scanned += 1

        result = _analyze_stock(code, info, csv_path, pension_data)
        if result:
            results.append(result)

    # 3) 카테고리 분류 (5일 등락률 기준)
    coiled = []    # 코일: ±5% — 최고 기회
    warming = []   # 움직임: +5~10% — 초기
    launched = []  # 출발: +10~15% — 진행 중

    for r in sorted(results, key=lambda x: x["tipping_score"], reverse=True):
        chg = r.get("chg_5d", 0)
        if -5.0 <= chg <= 5.0:
            r["phase"] = "코일"
            coiled.append(r)
        elif 5.0 < chg <= 10.0:
            r["phase"] = "점화"
            warming.append(r)
        elif 10.0 < chg <= MAX_SURGE:
            r["phase"] = "이륙"
            launched.append(r)
        # 15%+ 이미 상승 → 제외

    coiled = coiled[:top_n]
    warming = warming[:15]
    launched = launched[:10]

    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "coiled": coiled,
        "warming": warming,
        "launched": launched,
        "summary": {
            "total_detected": len(results),
            "coiled_count": len(coiled),
            "warming_count": len(warming),
            "launched_count": len(launched),
            "top_coiled": [
                f"{s['name']}({s['tipping_score']}점)"
                for s in coiled[:5]
            ],
        },
    }

    # 저장
    try:
        tmp = OUTPUT_PATH.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(OUTPUT_PATH)
        logger.info(
            f"[임계점] 스캔 완료: {scanned}종목 → "
            f"코일 {len(coiled)} / 점화 {len(warming)} / 이륙 {len(launched)}"
        )
    except Exception as e:
        logger.warning(f"[임계점] 저장 실패: {e}")

    return output


# ── 종목 분석 ──

def _analyze_stock(
    code: str, info: dict, csv_path: Path, pension_data: dict
) -> dict | None:
    """개별 종목 6자 수급 임계점 분석."""
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception:
        return None

    if len(rows) < LOOKBACK:
        return None

    recent = rows[-LOOKBACK:]

    # 시계열 추출 (백만원 단위)
    frgn = [_sf(r.get("외국인_금액")) for r in recent]
    inst = [_sf(r.get("기관_금액")) for r in recent]
    indi = [_sf(r.get("개인_금액")) for r in recent]
    etc_ = [_sf(r.get("기타법인_금액")) for r in recent]
    close = [_sf(r.get("종가")) for r in recent]
    dates = [r.get("date", "") for r in recent]

    if not close or close[-1] <= 0:
        return None

    # 연기금/금투 시계열 (억원 → 백만원 변환)
    pension = []
    fi = []
    stock_pension = pension_data.get(code, {})
    for d in dates:
        # CSV date: "2026-05-09", JSON date: "20260509"
        d_key = d.replace("-", "")
        day_data = stock_pension.get(d_key, {})
        pension.append(day_data.get("pension_net", 0) * 100)  # 억→백만
        fi.append(day_data.get("finance_net", 0) * 100)

    # ━━ 지표 ① 매집 강도 (Accumulation Intensity) ━━
    # 스마트머니 = 외인 + 기관 + 연기금 + 금투 (양수만 합산)
    smart_cum = 0
    for i in range(len(recent)):
        buyers = [frgn[i], inst[i], pension[i], fi[i]]
        smart_cum += sum(max(0, b) for b in buyers)

    # 일평균 거래대금 (양쪽 거래 합산으로 추정)
    daily_turnover = []
    for i in range(len(recent)):
        turnover = abs(frgn[i]) + abs(inst[i]) + abs(indi[i]) + abs(etc_[i])
        daily_turnover.append(turnover)
    avg_turnover = sum(daily_turnover) / max(len(daily_turnover), 1)

    if avg_turnover < 1:
        return None

    accum_intensity = smart_cum / avg_turnover
    # 점수: 0~30
    accum_score = min(int(accum_intensity * 10), 30)

    if accum_intensity < 0.5:  # 0.5일치 미만 매집 → 의미 없음
        return None

    # ━━ 지표 ② 매도 소진도 (Sell Exhaustion) ━━
    # 개인 순매도(음수)의 절대값 비교: 최근 3일 vs 10일 평균
    indi_sell_3d = sum(abs(min(0, v)) for v in indi[-3:]) / 3
    indi_sell_10d = sum(abs(min(0, v)) for v in indi) / max(len(indi), 1)

    if indi_sell_10d > 0:
        sell_exh_ratio = indi_sell_3d / indi_sell_10d
    else:
        sell_exh_ratio = 0.5  # 개인 매도 자체가 없음 → 소진 완료

    # 비율 < 1.0 = 매도 줄어듦 → 점수 높음
    # 비율 0.3 = 매도 70% 감소 → 20점 만점
    if sell_exh_ratio <= 0.3:
        sell_score = 20
    elif sell_exh_ratio <= 0.5:
        sell_score = 16
    elif sell_exh_ratio <= 0.7:
        sell_score = 12
    elif sell_exh_ratio <= 1.0:
        sell_score = 8
    else:
        sell_score = 3  # 매도 증가 → 낮은 점수

    # ━━ 지표 ③ 거래량 압축 (Volume Compression) ━━
    vol_3d_avg = sum(daily_turnover[-3:]) / 3
    vol_10d_avg = avg_turnover

    if vol_10d_avg > 0:
        vol_ratio = vol_3d_avg / vol_10d_avg
    else:
        vol_ratio = 1.0

    # 비율 < 1.0 = 거래 줄어듦 → 에너지 축적
    if vol_ratio <= 0.5:
        vol_score = 15
    elif vol_ratio <= 0.7:
        vol_score = 12
    elif vol_ratio <= 0.85:
        vol_score = 8
    elif vol_ratio <= 1.0:
        vol_score = 5
    else:
        vol_score = 2  # 거래량 증가 → 이미 움직이는 중

    # ━━ 지표 ④ 다자 합류 (Cross Confirmation) ━━
    # 오늘 (마지막 날) 순매수 > 0인 투자자 유형 카운트
    today_buyers = []
    if frgn[-1] > 0:
        today_buyers.append("외인")
    if inst[-1] > 0:
        today_buyers.append("기관")
    if pension[-1] > 0:
        today_buyers.append("연기금")
    if fi[-1] > 0:
        today_buyers.append("금투")
    if etc_[-1] > 0:
        today_buyers.append("기타")
    # 개인은 보통 매도 → 매수이면 특이 시그널
    if indi[-1] > 0:
        today_buyers.append("개인")

    cross_count = len(today_buyers)

    cross_score_map = {0: 0, 1: 3, 2: 6, 3: 10, 4: 13, 5: 15, 6: 15}
    cross_score = cross_score_map.get(cross_count, 15)

    # ━━ 지표 ⑤ 지속 확률 (Continuation Probability) ━━
    # 스마트머니(외인+기관) 합산 연속매수 일수
    smart_series = [frgn[i] + inst[i] for i in range(len(recent))]
    smart_consec = _count_consecutive_buy(smart_series)

    # 연기금 연속매수 (별도 체크)
    pension_consec = _count_consecutive_buy(pension)

    # 가장 긴 연속매수 사용
    max_consec = max(smart_consec, pension_consec)

    # 지속 확률 추정
    cont_prob = CONTINUATION_PROB.get(
        min(max_consec, 10), 0.50
    )

    # 쌍매수(외인+기관 동시) 보정
    frgn_consec = _count_consecutive_buy(frgn)
    inst_consec = _count_consecutive_buy(inst)
    dual_buy = frgn_consec >= 2 and inst_consec >= 2

    if dual_buy:
        cont_prob = min(cont_prob * 1.15, 0.85)
    if pension_consec >= 3:
        cont_prob = min(cont_prob * 1.10, 0.85)

    # 점수: 확률 40%=0점 ~ 80%=20점
    cont_score = max(0, min(int((cont_prob - 0.40) * 50), 20))

    # ━━ 종합 임계점 점수 ━━
    tipping_score = accum_score + sell_score + vol_score + cross_score + cont_score

    if tipping_score < 25:  # 25점 미만 → 무의미
        return None

    # ━━ 5일 등락률 ━━
    chg_5d = 0.0
    if len(close) >= 5 and close[-5] > 0:
        chg_5d = (close[-1] / close[-5] - 1) * 100

    if chg_5d > MAX_SURGE:  # 이미 15%+ 상승
        return None

    # ━━ 패턴 라벨 ━━
    pattern = _make_pattern_label(
        frgn_consec, inst_consec, pension_consec, dual_buy, cross_count
    )

    # ━━ 내일 시나리오 ━━
    # "이 종목에 내일 100백만원(1억) 순매수가 들어오면 얼마나 움직일까"
    # 추정: 최근 10일 가격 민감도 = 스마트머니 순매수 1백만 → 가격 변동 %
    price_sensitivity = _calc_price_sensitivity(smart_series, close)

    return {
        "code": code,
        "name": info.get("name", ""),
        "sector": info.get("sector", ""),
        "market": info.get("market", ""),
        "cap": info.get("cap_억", 0),
        "close": int(close[-1]),
        "chg_5d": round(chg_5d, 1),

        # 종합
        "tipping_score": tipping_score,
        "phase": "",  # caller가 설정

        # 5개 지표 상세
        "accum_intensity": round(accum_intensity, 2),
        "accum_score": accum_score,

        "sell_exhaustion": round(sell_exh_ratio, 2),
        "sell_score": sell_score,

        "vol_compression": round(vol_ratio, 2),
        "vol_score": vol_score,

        "cross_count": cross_count,
        "today_buyers": today_buyers,
        "cross_score": cross_score,

        "continuation_prob": round(cont_prob * 100, 1),  # % 표시
        "cont_score": cont_score,

        # 연속매수 상세
        "smart_consec": smart_consec,
        "frgn_consec": frgn_consec,
        "inst_consec": inst_consec,
        "pension_consec": pension_consec,
        "dual_buy": dual_buy,
        "pattern": pattern,

        # 가격 민감도
        "price_sensitivity": round(price_sensitivity, 4),

        # 일별 상세 (최근 3일)
        "recent_3d": [
            {
                "date": dates[-(3 - i)] if len(dates) >= 3 else "",
                "frgn": round(frgn[-(3 - i)]),
                "inst": round(inst[-(3 - i)]),
                "pension": round(pension[-(3 - i)]),
                "fi": round(fi[-(3 - i)]),
                "indi": round(indi[-(3 - i)]),
                "close": int(close[-(3 - i)]),
            }
            for i in range(3)
            if len(dates) >= 3
        ],
    }


# ── 유틸리티 ──

def _sf(val, default=0.0) -> float:
    """Safe float"""
    try:
        return float(val) if val and str(val).strip() else default
    except (ValueError, TypeError):
        return default


def _count_consecutive_buy(series: list) -> int:
    """최근부터 역순으로 연속 매수(양수) 일수"""
    count = 0
    for val in reversed(series):
        if val > 0:
            count += 1
        else:
            break
    return count


def _load_pension_data() -> dict:
    """quant_investor_extra.json에서 연기금/금투 데이터 로드.

    Returns:
        {code: {date_str: {"pension_net": float, "finance_net": float}}}
        date_str 포맷: "20260509" (YYYYMMDD)
    """
    if not QUANT_JSON.exists():
        logger.info("[임계점] quant_investor_extra.json 없음 — 4자만 분석")
        return {}

    try:
        raw = json.loads(QUANT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}

    daily = raw.get("daily", {})
    result = {}
    for code, stock_data in daily.items():
        dates = stock_data.get("dates", {})
        if dates:
            result[code] = dates
    return result


def _make_pattern_label(
    frgn_c: int, inst_c: int, pension_c: int, dual: bool, cross: int
) -> str:
    """패턴 라벨 생성."""
    parts = []
    if dual:
        parts.append(f"쌍매수{min(frgn_c, inst_c)}D")
    elif frgn_c >= 2 and inst_c < 2:
        parts.append(f"외인{frgn_c}D")
    elif inst_c >= 2 and frgn_c < 2:
        parts.append(f"기관{inst_c}D")

    if pension_c >= 3:
        parts.append(f"연기금{pension_c}D")

    if cross >= 4:
        parts.append(f"{cross}자합류")

    return "+".join(parts) if parts else "매집"


def _calc_price_sensitivity(smart_series: list, close_series: list) -> float:
    """가격 민감도: 스마트머니 순매수 100백만원당 가격 변동률 추정.

    최근 10일간 스마트머니 순매수 ↔ 종가 변동률의 관계를 단순 계산.
    값이 클수록 = 조금만 사도 많이 움직인다.
    """
    if len(smart_series) < 3 or len(close_series) < 3:
        return 0.0

    # 일별: 스마트머니 순매수 → 익일 가격 변동
    sensitivities = []
    for i in range(1, min(len(smart_series), len(close_series))):
        buy_amount = smart_series[i - 1]  # 전일 순매수 (백만원)
        if abs(buy_amount) < 10:  # 너무 작은 금액 무시
            continue
        price_chg = (close_series[i] / close_series[i - 1] - 1) * 100
        # 순매수 100백만원(1억)당 가격 변동률
        sens = price_chg / (buy_amount / 100) if buy_amount != 0 else 0
        sensitivities.append(sens)

    if not sensitivities:
        return 0.0

    # 평균 민감도 (양수 = 매수→상승 관계)
    return sum(sensitivities) / len(sensitivities)


# ── 텔레그램 포맷 ──

def format_tipping_alert(scan: dict) -> str:
    """텔레그램 알림 포맷."""
    coiled = scan.get("coiled", [])
    warming = scan.get("warming", [])
    summary = scan.get("summary", {})

    lines = [
        "🎯 수급 임계점 스캐너",
        f"스캔: {scan.get('total_scanned', 0)}종목 | "
        f"코일 {summary.get('coiled_count', 0)} / "
        f"점화 {summary.get('warming_count', 0)} / "
        f"이륙 {summary.get('launched_count', 0)}",
        "",
    ]

    if coiled:
        lines.append("━━ 코일 (가격 ±5%, 스프링 장전) ━━")
        for s in coiled[:15]:
            badge = "🔴" if s["tipping_score"] >= 70 else "🟠" if s["tipping_score"] >= 50 else "🟡"
            dual = "⚡" if s.get("dual_buy") else ""
            buyers = ",".join(s.get("today_buyers", []))
            lines.append(
                f"{badge}{dual} {s['name']} [{s['sector']}] "
                f"{s['tipping_score']}점 {s['pattern']}"
            )
            lines.append(
                f"   매집{s['accum_intensity']:.1f}x "
                f"소진{s['sell_exhaustion']:.1f} "
                f"압축{s['vol_compression']:.1f} "
                f"지속{s['continuation_prob']:.0f}% "
                f"5D{s['chg_5d']:+.1f}%"
            )
            if buyers:
                lines.append(f"   오늘매수: {buyers}")
        lines.append("")

    if warming:
        lines.append("━━ 점화 (+5~10%, 시동) ━━")
        for s in warming[:10]:
            lines.append(
                f"🔵 {s['name']} [{s['sector']}] "
                f"{s['tipping_score']}점 {s['pattern']} "
                f"5D{s['chg_5d']:+.1f}%"
            )
        lines.append("")

    lines.append(f"🕐 {scan.get('timestamp', '')}")
    lines.append("")
    lines.append("⚠️ 임계점 = 확률적 상태이며 매수 추천이 아닙니다")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO)
    result = scan_tipping_point()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
    print()
    print(format_tipping_alert(result))
