# -*- coding: utf-8 -*-
"""
TIER2 Phase 2: ETF 투자자별 수급 분석
======================================
21개 ETF의 기관/외국인/개인 자금 흐름 추적.

"기관이 어떤 ETF를 사고 있나?" → 시장 방향성 판단.

핵심 시그널:
  - 인버스 ETF 기관 매수 급증 → 시장 하락 경고 (BRAIN 방어)
  - 레버리지 ETF 기관 매수 → 시장 상승 기대
  - 섹터 ETF 기관 집중 → 섹터 로테이션 확인
  - 원자재 ETF 기관 매수 → 안전자산/인플레 기대

사용법:
  from data.etf_fund_flow import analyze_etf_flow, format_telegram_report
  report = analyze_etf_flow()
  msg = format_telegram_report(report)

스케줄: COO G7 Stage 3 (16:40 이후) 실행
데이터: flow/{etf_code}_investor.csv (21종목)
"""
import csv
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"
RESULT_PATH = DATA_DIR / "etf_flow.json"


# ═════════════════════════════════════════════════════════════
# ETF 유니버스 (etf_universe.py에서 가져오되, 수급 분석용 메타 추가)
# ═════════════════════════════════════════════════════════════

ETF_FLOW_META = {
    # ── 방향성 ETF ──
    "069500": {"name": "KODEX 200",            "group": "directional", "direction": "long",    "alias": "코스피 추종"},
    "229200": {"name": "KODEX 코스닥150",       "group": "directional", "direction": "long",    "alias": "코스닥 추종"},
    "122630": {"name": "KODEX 레버리지",         "group": "directional", "direction": "long_2x", "alias": "코스피 2배 베팅"},
    "233740": {"name": "KODEX 코스닥150레버리지",  "group": "directional", "direction": "long_2x", "alias": "코스닥 2배 베팅"},
    "114800": {"name": "KODEX 인버스",           "group": "directional", "direction": "short",   "alias": "시장 하락 베팅"},
    "252670": {"name": "KODEX 200선물인버스2X",   "group": "directional", "direction": "short_2x","alias": "시장 하락 2배 베팅"},

    # ── 원자재 ETF ──
    "132030": {"name": "KODEX 골드선물(H)",       "group": "commodity", "direction": "gold",    "alias": "금 가격 추종"},
    "411060": {"name": "ACE KRX금현물",           "group": "commodity", "direction": "gold",    "alias": "금 현물"},
    "261220": {"name": "KODEX WTI원유선물(H)",     "group": "commodity", "direction": "oil",     "alias": "원유 가격 추종"},
    "130680": {"name": "TIGER 원유선물Enhanced(H)", "group": "commodity", "direction": "oil",     "alias": "원유 추종"},
    "139320": {"name": "TIGER 금속선물(H)",        "group": "commodity", "direction": "metals",  "alias": "구리/금속 추종"},

    # ── 섹터/테마 ETF ──
    "449450": {"name": "PLUS K방산",             "group": "sector", "direction": "defense",       "alias": "방위산업"},
    "091160": {"name": "KODEX 반도체",           "group": "sector", "direction": "semiconductor", "alias": "반도체"},
    "305720": {"name": "KODEX 2차전지산업",       "group": "sector", "direction": "battery",      "alias": "2차전지/배터리"},
    "244580": {"name": "KODEX 바이오",           "group": "sector", "direction": "bio",           "alias": "바이오/제약"},
    "117700": {"name": "KODEX 건설",             "group": "sector", "direction": "construction",  "alias": "건설"},
    "139230": {"name": "TIGER 조선TOP10",        "group": "sector", "direction": "shipbuilding",  "alias": "조선"},
    "091180": {"name": "KODEX 자동차",           "group": "sector", "direction": "auto",          "alias": "자동차"},
    "266370": {"name": "KODEX IT",              "group": "sector", "direction": "it",             "alias": "IT/소프트웨어"},
    "140700": {"name": "KODEX 보험",             "group": "sector", "direction": "insurance",     "alias": "보험/금융"},
    "102780": {"name": "KODEX 삼성그룹",          "group": "sector", "direction": "samsung",      "alias": "삼성그룹"},
}

# 그룹별 주린이 설명
GROUP_DESC = {
    "directional": "시장 방향 베팅 ETF",
    "commodity":   "원자재 ETF (금/원유/금속)",
    "sector":      "섹터/테마 ETF",
}


# ═════════════════════════════════════════════════════════════
# 데이터 클래스
# ═════════════════════════════════════════════════════════════

@dataclass
class ETFFlowItem:
    """개별 ETF 수급 데이터"""
    code: str = ""
    name: str = ""
    group: str = ""           # directional / commodity / sector
    direction: str = ""       # long / short / gold / semiconductor 등
    alias: str = ""           # 주린이용 설명

    # 기관 (억원)
    inst_1d: float = 0.0
    inst_3d: float = 0.0
    inst_5d: float = 0.0
    inst_consecutive: int = 0

    # 외국인 (억원)
    foreign_1d: float = 0.0
    foreign_3d: float = 0.0
    foreign_5d: float = 0.0
    foreign_consecutive: int = 0

    # 개인 (억원)
    retail_1d: float = 0.0
    retail_3d: float = 0.0

    # 종가/등락
    close: int = 0
    change_pct: float = 0.0

    # 판단
    signal: str = ""          # 기관매수 / 기관매도 / 외인매수 / 중립
    signal_desc: str = ""     # 주린이용 설명
    strength: int = 0         # 0~100 신호 강도


@dataclass
class ETFFlowReport:
    """ETF 수급 종합 보고서"""
    timestamp: str = ""
    data_date: str = ""
    total_etfs: int = 0
    etfs_with_data: int = 0

    # 개별 ETF 수급
    items: List[dict] = field(default_factory=list)

    # ── 핵심 시그널 ──
    # 방향성 판단: 기관이 롱/숏 어디에 베팅하나?
    market_direction: str = ""         # BULLISH / BEARISH / NEUTRAL
    market_direction_desc: str = ""    # 주린이용 설명
    inverse_warning: bool = False      # 인버스 기관매수 경고

    # 섹터 로테이션: 기관이 집중하는 섹터 ETF
    hot_sector_etfs: List[str] = field(default_factory=list)

    # 안전자산: 금/원자재 기관 매수
    safe_haven_signal: str = ""        # RISK_ON / RISK_OFF / NEUTRAL
    safe_haven_desc: str = ""

    # BRAIN 방어 연동
    brain_defense_score: float = 0.0   # -10 ~ +10


# ═════════════════════════════════════════════════════════════
# 헬퍼: flow CSV 읽기
# ═════════════════════════════════════════════════════════════

def _safe_float(val) -> float:
    if not val or not str(val).strip():
        return 0.0
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return 0.0


def _read_etf_investor_csv(code: str, days: int = 5) -> List[dict]:
    """
    flow/{code}_investor.csv → 최근 N일 수급 데이터.

    ETF CSV 형식: date,종가,전일대비,외국인_수량,기관_수량,개인_수량,외국인_금액,기관_금액,개인_금액

    금액 단위가 변경될 수 있으므로 수량 × 종가 / 1억으로 계산 (Phase 1과 동일 전략).
    """
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return []

    try:
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # 헤더 스킵
            for row in reader:
                if len(row) < 9:
                    continue
                rows.append(row)

        recent = rows[-days:] if len(rows) >= days else rows
        result = []

        for row in recent:
            try:
                date_str = row[0].strip()
                close = _safe_float(row[1])        # 종가
                change = _safe_float(row[2])        # 전일대비

                foreign_qty = _safe_float(row[3])   # 외국인_수량
                inst_qty = _safe_float(row[4])      # 기관_수량
                retail_qty = _safe_float(row[5])     # 개인_수량

                # 수량 × 종가 → 억원 (단위 안전)
                if close > 0:
                    inst_amt = inst_qty * close / 1e8
                    foreign_amt = foreign_qty * close / 1e8
                    retail_amt = retail_qty * close / 1e8
                else:
                    inst_amt = foreign_amt = retail_amt = 0.0

                result.append({
                    "date": date_str,
                    "close": close,
                    "change": change,
                    "inst_amt": inst_amt,
                    "foreign_amt": foreign_amt,
                    "retail_amt": retail_amt,
                })
            except (IndexError, ValueError):
                continue

        return result
    except Exception as e:
        logger.debug(f"[ETF수급] {code} CSV 읽기 실패: {e}")
        return []


def _count_consecutive(values: List[float]) -> int:
    """최근일부터 연속 양수/음수 카운트 (Phase 1 동일 로직)"""
    if not values:
        return 0
    last = values[-1]
    if abs(last) < 0.001:
        return 0
    direction = 1 if last > 0 else -1
    count = 0
    for val in reversed(values):
        if direction > 0 and val > 0:
            count += 1
        elif direction < 0 and val < 0:
            count += 1
        else:
            break
    return count * direction


# ═════════════════════════════════════════════════════════════
# 개별 ETF 시그널 판단
# ═════════════════════════════════════════════════════════════

def _judge_etf_signal(item: ETFFlowItem) -> tuple:
    """
    개별 ETF 기관/외인 수급 → 시그널 판단.

    Returns: (signal, signal_desc, strength)
    """
    inst_con = item.inst_consecutive
    foreign_con = item.foreign_consecutive

    # 기관+외인 모두 매수 (강력)
    if inst_con >= 2 and foreign_con >= 2:
        desc = f"기관+외국인 {min(inst_con, foreign_con)}일 연속 매수 (강력)"
        strength = min(100, 50 + (inst_con + foreign_con) * 8)
        return "합의매수", desc, strength

    # 기관 집중 매수
    if inst_con >= 3:
        desc = f"기관 {inst_con}일 연속 매수 중"
        strength = min(90, 40 + inst_con * 10)
        return "기관매수", desc, strength

    # 기관 집중 매도
    if inst_con <= -3:
        desc = f"기관 {abs(inst_con)}일 연속 매도 중"
        strength = min(90, 40 + abs(inst_con) * 10)
        return "기관매도", desc, strength

    # 외인 집중 매수
    if foreign_con >= 3:
        desc = f"외국인 {foreign_con}일 연속 매수 중"
        strength = min(80, 30 + foreign_con * 10)
        return "외인매수", desc, strength

    # 외인 집중 매도
    if foreign_con <= -3:
        desc = f"외국인 {abs(foreign_con)}일 연속 매도 중"
        strength = min(80, 30 + abs(foreign_con) * 10)
        return "외인매도", desc, strength

    # 기관+외인 반대 방향
    if (inst_con >= 2 and foreign_con <= -2) or (inst_con <= -2 and foreign_con >= 2):
        buyer = "기관" if inst_con > 0 else "외국인"
        return "의견분열", f"{buyer}만 매수 (반대쪽은 매도)", 30

    # 단기 방향성
    if inst_con >= 1 and foreign_con >= 1:
        return "매수전환", "기관+외인 매수 전환", 40

    if inst_con <= -1 and foreign_con <= -1:
        return "매도전환", "기관+외인 매도 전환", 40

    return "중립", "뚜렷한 방향 없음", 0


# ═════════════════════════════════════════════════════════════
# 시장 방향 판단 (방향성 ETF 수급 기반)
# ═════════════════════════════════════════════════════════════

def _judge_market_direction(items: List[ETFFlowItem]) -> tuple:
    """
    방향성 ETF 6종목의 기관 수급으로 시장 방향 판단.

    인버스 기관 순매수 ↑ → BEARISH (시장 하락 예상)
    레버리지 기관 순매수 ↑ → BULLISH (시장 상승 예상)

    Returns: (direction, desc, inverse_warning, brain_defense)
    """
    # 롱 ETF 기관 3일 합계
    long_inst = 0.0
    # 숏 ETF 기관 3일 합계
    short_inst = 0.0
    # 인버스2X 기관 연속 매수일
    inverse2x_consecutive = 0

    for item in items:
        if item.group != "directional":
            continue
        if item.direction in ("long", "long_2x"):
            long_inst += item.inst_3d
        elif item.direction in ("short", "short_2x"):
            short_inst += item.inst_3d
            if item.direction == "short_2x":
                inverse2x_consecutive = item.inst_consecutive

    total = long_inst + short_inst  # short_inst는 인버스 순매수이므로 +면 하락 베팅

    # ── 인버스 경고 ──
    # 기관이 인버스2X를 3일 이상 연속 매수 → 하락 강력 경고
    inverse_warning = inverse2x_consecutive >= 3

    # ── 방향 판단 ──
    if short_inst > 0 and abs(short_inst) > abs(long_inst) * 0.5:
        # 인버스에 기관 순매수가 크면 → BEARISH
        if short_inst > abs(long_inst):
            desc = (
                f"기관이 인버스 ETF에 집중 매수 중 ({short_inst:+,.0f}억)\n"
                f"  → 시장 하락을 예상하는 프로가 많음 (주의!)"
            )
            brain_defense = min(10.0, short_inst / 100)  # 인버스 매수 규모에 비례
            return "BEARISH", desc, inverse_warning, brain_defense
        else:
            desc = (
                f"인버스에도 기관 자금 유입 ({short_inst:+,.0f}억) — 헤지 가능성\n"
                f"  롱 ETF ({long_inst:+,.0f}억) vs 숏 ETF ({short_inst:+,.0f}억)"
            )
            brain_defense = min(5.0, short_inst / 200)
            return "CAUTIOUS", desc, inverse_warning, brain_defense

    if long_inst > 0 and short_inst <= 0:
        desc = (
            f"기관이 롱 ETF에 집중 ({long_inst:+,.0f}억)\n"
            f"  인버스는 매도 ({short_inst:+,.0f}억) → 시장 상승 기대"
        )
        return "BULLISH", desc, False, -min(5.0, long_inst / 200)

    if long_inst > 0 and short_inst > 0:
        desc = (
            f"롱({long_inst:+,.0f}억) + 숏({short_inst:+,.0f}억) 모두 유입\n"
            f"  → 변동성 확대 예상 (양방향 베팅)"
        )
        return "VOLATILE", desc, False, 2.0

    desc = "방향성 ETF에 뚜렷한 수급 쏠림 없음"
    return "NEUTRAL", desc, False, 0.0


# ═════════════════════════════════════════════════════════════
# 안전자산 판단 (원자재 ETF 수급 기반)
# ═════════════════════════════════════════════════════════════

def _judge_safe_haven(items: List[ETFFlowItem]) -> tuple:
    """
    원자재 ETF 기관 수급으로 위험선호/회피 판단.

    금 ETF 기관매수 ↑ → RISK_OFF (안전자산 선호 = 불안)
    원유/금속 ETF 기관매수 ↑ → RISK_ON (경기 기대)

    Returns: (signal, desc)
    """
    gold_inst_3d = 0.0
    oil_metals_inst_3d = 0.0

    for item in items:
        if item.group != "commodity":
            continue
        if item.direction == "gold":
            gold_inst_3d += item.inst_3d
        else:  # oil, metals
            oil_metals_inst_3d += item.inst_3d

    if gold_inst_3d > 50 and gold_inst_3d > oil_metals_inst_3d * 2:
        desc = (
            f"금 ETF에 기관 자금 집중 ({gold_inst_3d:+,.0f}억)\n"
            f"  → 안전자산 선호 (시장 불안 심리)"
        )
        return "RISK_OFF", desc

    if oil_metals_inst_3d > 30 and oil_metals_inst_3d > gold_inst_3d:
        desc = (
            f"원유/금속 ETF 기관 매수 ({oil_metals_inst_3d:+,.0f}억)\n"
            f"  → 경기회복 기대 (원자재 수요 증가)"
        )
        return "RISK_ON", desc

    if gold_inst_3d < -30 and oil_metals_inst_3d > 0:
        desc = "금 ETF 기관 매도 + 원자재 매수 → 위험선호"
        return "RISK_ON", desc

    return "NEUTRAL", "원자재 ETF 뚜렷한 쏠림 없음"


# ═════════════════════════════════════════════════════════════
# 메인 분석 함수
# ═════════════════════════════════════════════════════════════

def analyze_etf_flow(days: int = 5) -> ETFFlowReport:
    """
    ETF 투자자별 수급 종합 분석.

    1. 21개 ETF의 flow CSV 읽기
    2. 기관/외인/개인 수급 집계
    3. 시장 방향 판단 (방향성 ETF)
    4. 안전자산 판단 (원자재 ETF)
    5. 섹터 집중도 판단 (섹터 ETF)
    6. BRAIN 방어 점수 산출
    """
    all_items: List[ETFFlowItem] = []
    latest_date = ""
    etfs_with_data = 0

    for code, meta in ETF_FLOW_META.items():
        daily = _read_etf_investor_csv(code, days)
        if not daily:
            continue

        etfs_with_data += 1
        item = ETFFlowItem(
            code=code,
            name=meta["name"],
            group=meta["group"],
            direction=meta["direction"],
            alias=meta["alias"],
        )

        # 일별 데이터 집계
        inst_daily = [d["inst_amt"] for d in daily]
        foreign_daily = [d["foreign_amt"] for d in daily]
        retail_daily = [d["retail_amt"] for d in daily]

        item.inst_1d = inst_daily[-1] if inst_daily else 0.0
        item.inst_3d = sum(inst_daily[-3:])
        item.inst_5d = sum(inst_daily)
        item.inst_consecutive = _count_consecutive(inst_daily)

        item.foreign_1d = foreign_daily[-1] if foreign_daily else 0.0
        item.foreign_3d = sum(foreign_daily[-3:])
        item.foreign_5d = sum(foreign_daily)
        item.foreign_consecutive = _count_consecutive(foreign_daily)

        item.retail_1d = retail_daily[-1] if retail_daily else 0.0
        item.retail_3d = sum(retail_daily[-3:])

        # 종가/등락
        last = daily[-1]
        item.close = int(last["close"])
        if len(daily) >= 2:
            prev_close = daily[-2]["close"]
            if prev_close > 0:
                item.change_pct = round((last["close"] / prev_close - 1) * 100, 2)

        # 시그널 판단
        signal, desc, strength = _judge_etf_signal(item)
        item.signal = signal
        item.signal_desc = desc
        item.strength = strength

        # 최신 날짜
        if daily[-1]["date"] > latest_date:
            latest_date = daily[-1]["date"]

        all_items.append(item)

    # ── 시장 방향 판단 ──
    direction, direction_desc, inverse_warning, brain_defense = _judge_market_direction(all_items)

    # ── 안전자산 판단 ──
    safe_signal, safe_desc = _judge_safe_haven(all_items)

    # ── 섹터 ETF 기관 집중 TOP ──
    sector_items = [i for i in all_items if i.group == "sector"]
    sector_items.sort(key=lambda x: x.inst_3d, reverse=True)
    hot_sector_etfs = [
        f"{i.alias}({i.inst_3d:+,.0f}억)"
        for i in sector_items[:3]
        if i.inst_3d > 10  # 10억 이상만
    ]

    # BRAIN 방어 점수 보정 (안전자산 시그널)
    if safe_signal == "RISK_OFF":
        brain_defense += 3.0
    elif safe_signal == "RISK_ON":
        brain_defense -= 2.0
    brain_defense = max(-10.0, min(10.0, brain_defense))

    report = ETFFlowReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        data_date=latest_date,
        total_etfs=len(ETF_FLOW_META),
        etfs_with_data=etfs_with_data,
        items=[asdict(i) for i in all_items],
        market_direction=direction,
        market_direction_desc=direction_desc,
        inverse_warning=inverse_warning,
        hot_sector_etfs=hot_sector_etfs,
        safe_haven_signal=safe_signal,
        safe_haven_desc=safe_desc,
        brain_defense_score=round(brain_defense, 1),
    )

    # JSON 저장
    try:
        RESULT_PATH.write_text(
            json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info(f"[ETF수급] 저장 완료: {RESULT_PATH.name}")
    except Exception as e:
        logger.error(f"[ETF수급] 저장 실패: {e}")

    return report


# ═════════════════════════════════════════════════════════════
# 텔레그램 보고서 (주린이 한국어)
# ═════════════════════════════════════════════════════════════

def format_telegram_report(report: ETFFlowReport) -> str:
    """
    ETF 수급 텔레그램 보고서.

    주린이도 알기 쉽게:
    - "기관이 인버스를 사고 있다 = 시장 하락 예상"
    - "금 ETF에 돈이 몰린다 = 불안 심리"
    """
    lines = [
        f"📈 ETF 수급 리포트",
        f"📅 데이터: {report.data_date} | {report.etfs_with_data}/{report.total_etfs} ETF 분석",
        "",
    ]

    # ── 1. 시장 방향 (가장 중요) ──
    dir_icon = {
        "BULLISH": "🟢", "BEARISH": "🔴",
        "CAUTIOUS": "🟡", "VOLATILE": "⚡", "NEUTRAL": "⚪",
    }
    dir_label = {
        "BULLISH": "상승 기대", "BEARISH": "하락 경고",
        "CAUTIOUS": "주의", "VOLATILE": "변동성 확대", "NEUTRAL": "중립",
    }

    icon = dir_icon.get(report.market_direction, "⚪")
    label = dir_label.get(report.market_direction, "중립")
    lines.append(f"{icon} 시장 방향: {label}")
    if report.market_direction_desc:
        for desc_line in report.market_direction_desc.split("\n"):
            lines.append(f"  {desc_line}")

    if report.inverse_warning:
        lines.append(f"  🚨 인버스2X 기관 3일 연속 매수 — 하락 강력 경고!")
    lines.append("")

    # ── 2. 방향성 ETF 상세 ──
    items = report.items
    dir_items = [i for i in items if i["group"] == "directional"]
    if dir_items:
        lines.append("📊 방향성 ETF (기관 기준)")
        for i in dir_items:
            inst = i["inst_3d"]
            foreign = i["foreign_3d"]
            con = i["inst_consecutive"]
            streak = f" ({con}일연속)" if abs(con) >= 2 else ""

            # 인버스는 특별 표시
            if i["direction"] in ("short", "short_2x"):
                prefix = "🔻"
            elif i["direction"] in ("long_2x",):
                prefix = "🔺"
            else:
                prefix = "  "

            lines.append(
                f"  {prefix} {i['name']}: 기관 {_fmt_억(inst)}{streak} | 외인 {_fmt_억(foreign)}"
            )
        lines.append("")

    # ── 3. 안전자산 시그널 ──
    if report.safe_haven_signal != "NEUTRAL":
        safe_icon = "🛡️" if report.safe_haven_signal == "RISK_OFF" else "🔥"
        safe_label = "안전자산 선호" if report.safe_haven_signal == "RISK_OFF" else "위험자산 선호"
        lines.append(f"{safe_icon} {safe_label}")
        if report.safe_haven_desc:
            for desc_line in report.safe_haven_desc.split("\n"):
                lines.append(f"  {desc_line}")
        lines.append("")

    # ── 4. 섹터 ETF 기관 집중 ──
    if report.hot_sector_etfs:
        lines.append("🏭 기관이 집중하는 섹터 ETF")
        for i, s in enumerate(report.hot_sector_etfs):
            lines.append(f"  {i+1}. {s}")
        lines.append("")

    # ── 5. 눈에 띄는 수급 변화 ──
    strong_items = [i for i in items if i["strength"] >= 50]
    if strong_items:
        strong_items.sort(key=lambda x: -x["strength"])
        lines.append("💡 주목할 ETF 수급")
        for i in strong_items[:5]:
            sig_icon = "🟢" if "매수" in i["signal"] else ("🔴" if "매도" in i["signal"] else "🟡")
            lines.append(f"  {sig_icon} {i['name']}: {i['signal_desc']}")
        lines.append("")

    # ── 6. 주린이 해설 ──
    lines.append("💬 오늘의 핵심")
    if report.market_direction == "BEARISH":
        lines.append("  기관이 시장 하락에 베팅 중 → 신규 매수 자제")
        lines.append("  (인버스 ETF 매수 = '시장이 떨어질 것' 이라는 뜻)")
    elif report.market_direction == "BULLISH":
        lines.append("  기관이 시장 상승에 베팅 중 → 긍정적")
        lines.append("  (레버리지 ETF 매수 = '시장이 오를 것' 이라는 뜻)")
    elif report.market_direction == "VOLATILE":
        lines.append("  롱+숏 모두 유입 → 변동성 주의")
    elif report.market_direction == "CAUTIOUS":
        lines.append("  기관 일부 하락 헤지 중 → 관망 권장")
    else:
        lines.append("  뚜렷한 방향 없음 → 개별 종목 수급 확인")

    if report.safe_haven_signal == "RISK_OFF":
        lines.append("  금 ETF 매수 증가 → 불안 심리 (보수적 접근)")

    return "\n".join(lines)


def _fmt_억(val: float) -> str:
    """억원 포맷"""
    if abs(val) >= 1000:
        return f"{val:+,.0f}억"
    elif abs(val) >= 1:
        return f"{val:+,.0f}억"
    else:
        return f"{val:+.1f}억"


# ═════════════════════════════════════════════════════════════
# 외부 연동 API
# ═════════════════════════════════════════════════════════════

def get_etf_flow_defense_score() -> float:
    """
    BRAIN 방어 연동: 인버스 기관매수 경고 점수.

    Returns: -10.0 ~ +10.0
      양수: 하락 경고 (방어 강화 필요)
      음수: 상승 기대 (공격 가능)
    """
    if not RESULT_PATH.exists():
        return 0.0
    try:
        data = json.loads(RESULT_PATH.read_text("utf-8"))
        return data.get("brain_defense_score", 0.0)
    except Exception:
        return 0.0


def get_etf_market_direction() -> dict:
    """
    시장 방향 판단 요약 (추천/BRAIN 연동용).

    Returns: {
        "direction": "BULLISH/BEARISH/NEUTRAL/...",
        "inverse_warning": bool,
        "safe_haven": "RISK_ON/RISK_OFF/NEUTRAL",
        "hot_sectors": ["방산(+120억)", "반도체(+85억)"],
        "data_date": "2026-03-27",
    }
    """
    if not RESULT_PATH.exists():
        return {}
    try:
        data = json.loads(RESULT_PATH.read_text("utf-8"))
        return {
            "direction": data.get("market_direction", ""),
            "inverse_warning": data.get("inverse_warning", False),
            "safe_haven": data.get("safe_haven_signal", ""),
            "hot_sectors": data.get("hot_sector_etfs", []),
            "data_date": data.get("data_date", ""),
        }
    except Exception:
        return {}


def get_etf_flow_boost(code: str) -> float:
    """
    개별 ETF 수급 부스트 점수 (etf_recommender 연동).

    기관 3일 연속 매수 → +10
    기관+외인 합의매수 → +15
    기관 매도 → -10

    Returns: -15.0 ~ +15.0
    """
    if not RESULT_PATH.exists():
        return 0.0
    try:
        data = json.loads(RESULT_PATH.read_text("utf-8"))
        for item in data.get("items", []):
            if item.get("code") != code:
                continue
            signal = item.get("signal", "")
            strength = item.get("strength", 0)

            if signal == "합의매수":
                return min(15.0, strength * 0.15)
            elif signal == "기관매수":
                return min(10.0, strength * 0.12)
            elif signal == "외인매수":
                return min(8.0, strength * 0.1)
            elif signal == "기관매도":
                return max(-10.0, -strength * 0.12)
            elif signal == "외인매도":
                return max(-8.0, -strength * 0.1)
            elif signal == "합의매도":
                return max(-15.0, -strength * 0.15)
            else:
                return 0.0
        return 0.0
    except Exception:
        return 0.0


# ═════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    print("ETF 투자자별 수급 분석 시작...")
    report = analyze_etf_flow()

    print(f"\n총 {report.total_etfs}개 ETF 중 {report.etfs_with_data}개 데이터 있음")
    print(f"데이터 기준일: {report.data_date}\n")

    msg = format_telegram_report(report)
    try:
        print(msg)
    except UnicodeEncodeError:
        sys.stdout.reconfigure(encoding="utf-8")
        print(msg)

    # 상세 (--detail)
    if "--detail" in sys.argv:
        print("\n── 전체 ETF 상세 ──")
        for i in report.items:
            print(
                f"  {i['name']:25s} | "
                f"기관3D {i['inst_3d']:+8.0f}억 ({i['inst_consecutive']:+d}일) | "
                f"외인3D {i['foreign_3d']:+8.0f}억 ({i['foreign_consecutive']:+d}일) | "
                f"{i['signal']}"
            )
