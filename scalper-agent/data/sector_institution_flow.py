# -*- coding: utf-8 -*-
"""
TIER2 Phase 1: 섹터 기관 수급 집계
====================================
개별 종목 flow/ CSV → 23개 섹터별 기관/외국인 순매수 합산.

"큰손들의 돈이 어디로 이동하는지" 추적해서
어떤 섹터에 기관과 외국인이 동시에 돈을 넣고 있는지 파악.

사용법:
  from data.sector_institution_flow import analyze_sector_flow, format_telegram_report
  report = analyze_sector_flow()
  msg = format_telegram_report(report)

스케줄: COO G7 Stage2 (16:40) 이후 실행
데이터: flow/{code}_investor.csv (346+ 종목)
"""
import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
UNIVERSE_PATH = DATA_DIR / "universe.json"
FLOW_DIR = DATA_DIR / "flow"
RESULT_PATH = DATA_DIR / "sector_flow.json"

# ─── 섹터 한국어 별명 (주린이용) ──────────────────────
SECTOR_ALIAS = {
    "전기전자": "반도체/전자",
    "화학": "화학/소재",
    "제약": "바이오/제약",
    "기계장비": "기계/장비",
    "운송장비": "자동차/조선",
    "금융": "은행/증권",
    "IT서비스": "IT/플랫폼",
    "일반서비스": "서비스업",
    "금속": "철강/금속",
    "유통": "유통/소비",
    "음식료": "식품/음료",
    "건설": "건설/부동산",
    "의료정밀": "의료기기",
    "섬유의류": "섬유/의류",
    "오락문화": "엔터/미디어",
    "운송창고": "물류/운송",
    "비금속": "비금속/광물",
    "전기가스": "전력/에너지",
    "종이목재": "종이/목재",
    "기타제조": "기타제조",
    "출판매체": "출판",
    "통신": "통신",
    "기타": "기타",
}


@dataclass
class SectorFlow:
    """개별 섹터 수급 데이터"""
    sector: str                        # 섹터명 (원본)
    alias: str = ""                    # 주린이용 별명
    stock_count: int = 0               # 섹터 내 종목 수
    flow_stock_count: int = 0          # flow CSV 있는 종목 수

    # 기관 순매수 (억원)
    inst_1d: float = 0.0               # 당일
    inst_3d: float = 0.0               # 3일 합계
    inst_5d: float = 0.0               # 5일 합계
    inst_consecutive: int = 0          # 연속 순매수 일수 (음수=연속매도)

    # 외국인 순매수 (억원)
    foreign_1d: float = 0.0
    foreign_3d: float = 0.0
    foreign_5d: float = 0.0
    foreign_consecutive: int = 0

    # 종합 판단
    agreement: str = ""                # 합의매수 / 합의매도 / 의견분열 / 중립
    agreement_desc: str = ""           # 주린이용 설명
    boost_score: float = 0.0           # 추천 점수 보정 (-15 ~ +15)


@dataclass
class SectorFlowReport:
    """전체 섹터 수급 보고서"""
    timestamp: str = ""
    data_date: str = ""                # flow 데이터 기준일
    total_sectors: int = 0
    total_stocks_with_flow: int = 0
    sectors: List[dict] = field(default_factory=list)
    top_inflow: List[str] = field(default_factory=list)   # 매수 집중 TOP3
    top_outflow: List[str] = field(default_factory=list)  # 이탈 TOP3
    signal: str = ""                   # 한줄 요약


# ─── 헬퍼: universe.json → {code: sector} ──────────────
def _load_code_sector_map() -> Dict[str, str]:
    """종목코드 → 섹터 매핑"""
    if not UNIVERSE_PATH.exists():
        return {}
    try:
        uni = json.loads(UNIVERSE_PATH.read_text("utf-8"))
        return {
            code: info.get("sector", "기타")
            for code, info in uni.items()
            if isinstance(info, dict)
        }
    except Exception as e:
        logger.error(f"[섹터수급] universe.json 로드 실패: {e}")
        return {}


# ─── 헬퍼: flow CSV → 최근 N일 수급 ──────────────────
def _read_investor_csv(code: str, days: int = 5) -> List[dict]:
    """
    flow/{code}_investor.csv에서 최근 N일 데이터 읽기.

    Returns: [{date, inst_amt_억, foreign_amt_억}, ...]

    금액 계산: 수량 × 종가 / 1억 (단위 변경에 안전)
    """
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return []

    try:
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # 첫 줄 스킵
            for row in reader:
                if len(row) < 9:
                    continue
                rows.append(row)

        recent = rows[-days:] if len(rows) >= days else rows
        result = []

        for row in recent:
            try:
                date_str = row[0].strip()
                # CSV: date,종가,전일대비,외국인_수량,기관_수량,개인_수량,외국인_금액,기관_금액,개인_금액
                close = _safe_float(row[1])        # 종가
                inst_qty = _safe_float(row[4])     # 기관_수량
                foreign_qty = _safe_float(row[3])  # 외국인_수량

                if close > 0:
                    # 수량 × 종가 → 억원
                    inst_amt = inst_qty * close / 1e8
                    foreign_amt = foreign_qty * close / 1e8
                else:
                    inst_amt = 0.0
                    foreign_amt = 0.0

                result.append({
                    "date": date_str,
                    "inst_amt": inst_amt,
                    "foreign_amt": foreign_amt,
                })
            except (IndexError, ValueError):
                continue

        return result
    except Exception as e:
        logger.debug(f"[섹터수급] {code} CSV 읽기 실패: {e}")
        return []


def _safe_float(val) -> float:
    """안전한 float 변환"""
    if not val or not str(val).strip():
        return 0.0
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return 0.0


# ─── 연속 순매수/매도 일수 계산 ────────────────────────
def _count_consecutive(daily_values: List[float]) -> int:
    """
    최근일부터 역순으로 연속 양수(매수)/음수(매도) 카운트.

    예: [+10, -5, +20, +15, +8] → 마지막 3일 양수 → return 3
    예: [-10, -20, +5, -3, -7] → 마지막 2일 음수 → return -2
    """
    if not daily_values:
        return 0

    last = daily_values[-1]
    if abs(last) < 0.01:  # 거의 0 → 중립
        return 0

    direction = 1 if last > 0 else -1
    count = 0

    for val in reversed(daily_values):
        if direction > 0 and val > 0:
            count += 1
        elif direction < 0 and val < 0:
            count += 1
        else:
            break

    return count * direction


# ─── 합의 판단 ──────────────────────────────────────
def _judge_agreement(sf: SectorFlow) -> tuple:
    """
    기관+외인 동향으로 합의 판단.

    Returns: (agreement, agreement_desc, boost_score)
    """
    inst_3d = sf.inst_3d
    foreign_3d = sf.foreign_3d
    inst_con = sf.inst_consecutive
    foreign_con = sf.foreign_consecutive

    # 기관+외인 모두 3일 이상 매수
    if inst_con >= 3 and foreign_con >= 3:
        desc = f"기관+외국인 모두 {min(inst_con,foreign_con)}일 연속 매수 중 (강력 매수 신호)"
        boost = min(15.0, (inst_con + foreign_con) * 1.5)
        return "합의매수", desc, boost

    # 기관+외인 모두 매수 방향 (1~2일)
    if inst_con >= 1 and foreign_con >= 1:
        desc = "기관과 외국인 모두 매수 중 (긍정적)"
        boost = min(10.0, (inst_con + foreign_con) * 1.0)
        return "합의매수", desc, boost

    # 기관+외인 모두 매도
    if inst_con <= -3 and foreign_con <= -3:
        desc = f"기관+외국인 모두 {abs(max(inst_con,foreign_con))}일 연속 매도 중 (위험!)"
        boost = max(-15.0, (inst_con + foreign_con) * 1.5)
        return "합의매도", desc, boost

    if inst_con <= -1 and foreign_con <= -1:
        desc = "기관과 외국인 모두 매도 중 (주의)"
        boost = max(-10.0, (inst_con + foreign_con) * 1.0)
        return "합의매도", desc, boost

    # 기관 매수 + 외인 매도 (또는 반대)
    if (inst_con >= 2 and foreign_con <= -2) or (inst_con <= -2 and foreign_con >= 2):
        buyer = "기관" if inst_con > 0 else "외국인"
        seller = "외국인" if inst_con > 0 else "기관"
        desc = f"{buyer}은 사고 {seller}은 팔고 있음 (의견 분열 → 관망)"
        boost = 0.0
        return "의견분열", desc, boost

    # 한쪽만 강한 신호
    if inst_con >= 3:
        desc = f"기관 {inst_con}일 연속 매수 (외국인은 관망)"
        boost = inst_con * 1.0
        return "기관매집", desc, boost

    if foreign_con >= 3:
        desc = f"외국인 {foreign_con}일 연속 매수 (기관은 관망)"
        boost = foreign_con * 1.0
        return "외인매집", desc, boost

    if inst_con <= -3:
        desc = f"기관 {abs(inst_con)}일 연속 매도 (이탈 시작?)"
        boost = inst_con * 1.0
        return "기관이탈", desc, boost

    if foreign_con <= -3:
        desc = f"외국인 {abs(foreign_con)}일 연속 매도 (이탈 시작?)"
        boost = foreign_con * 1.0
        return "외인이탈", desc, boost

    # 중립
    return "중립", "뚜렷한 방향 없음", 0.0


# ─── 메인 분석 함수 ──────────────────────────────────
def analyze_sector_flow(days: int = 5) -> SectorFlowReport:
    """
    섹터별 기관/외국인 수급 분석.

    1. universe.json에서 종목→섹터 매핑
    2. flow/{code}_investor.csv에서 최근 N일 수급 읽기
    3. 섹터별 합산 + 연속일 계산 + 합의 판단
    4. 보고서 생성
    """
    code_sector = _load_code_sector_map()
    if not code_sector:
        logger.error("[섹터수급] universe.json 없음 — 분석 불가")
        return SectorFlowReport(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"))

    # 섹터별 일간 수급 집계
    # {sector: {date: {inst_sum, foreign_sum}}}
    sector_daily: Dict[str, Dict[str, dict]] = {}
    sector_stock_count: Dict[str, int] = {}
    sector_flow_count: Dict[str, int] = {}
    latest_date = ""
    total_with_flow = 0

    for code, sector in code_sector.items():
        sector_stock_count[sector] = sector_stock_count.get(sector, 0) + 1

        daily = _read_investor_csv(code, days)
        if not daily:
            continue

        total_with_flow += 1
        sector_flow_count[sector] = sector_flow_count.get(sector, 0) + 1

        if sector not in sector_daily:
            sector_daily[sector] = {}

        for d in daily:
            dt = d["date"]
            if dt > latest_date:
                latest_date = dt

            if dt not in sector_daily[sector]:
                sector_daily[sector][dt] = {"inst": 0.0, "foreign": 0.0}

            sector_daily[sector][dt]["inst"] += d["inst_amt"]
            sector_daily[sector][dt]["foreign"] += d["foreign_amt"]

    # 섹터별 분석
    sector_flows: List[SectorFlow] = []

    for sector, daily_map in sector_daily.items():
        sf = SectorFlow(
            sector=sector,
            alias=SECTOR_ALIAS.get(sector, sector),
            stock_count=sector_stock_count.get(sector, 0),
            flow_stock_count=sector_flow_count.get(sector, 0),
        )

        # 날짜 정렬
        sorted_dates = sorted(daily_map.keys())
        inst_daily = [daily_map[d]["inst"] for d in sorted_dates]
        foreign_daily = [daily_map[d]["foreign"] for d in sorted_dates]

        # 1일/3일/5일 합계
        sf.inst_1d = inst_daily[-1] if inst_daily else 0.0
        sf.inst_3d = sum(inst_daily[-3:]) if len(inst_daily) >= 1 else 0.0
        sf.inst_5d = sum(inst_daily)
        sf.foreign_1d = foreign_daily[-1] if foreign_daily else 0.0
        sf.foreign_3d = sum(foreign_daily[-3:]) if len(foreign_daily) >= 1 else 0.0
        sf.foreign_5d = sum(foreign_daily)

        # 연속 일수
        sf.inst_consecutive = _count_consecutive(inst_daily)
        sf.foreign_consecutive = _count_consecutive(foreign_daily)

        # 합의 판단
        agreement, desc, boost = _judge_agreement(sf)
        sf.agreement = agreement
        sf.agreement_desc = desc
        sf.boost_score = boost

        sector_flows.append(sf)

    # 정렬: 기관+외인 3일 합산 순매수 내림차순
    sector_flows.sort(key=lambda s: s.inst_3d + s.foreign_3d, reverse=True)

    # TOP 매수/매도 섹터
    top_inflow = [
        sf.alias for sf in sector_flows[:3]
        if sf.inst_3d + sf.foreign_3d > 0
    ]
    top_outflow = [
        sf.alias for sf in sector_flows[-3:]
        if sf.inst_3d + sf.foreign_3d < 0
    ]

    # 한줄 시그널
    if top_inflow:
        signal = f"돈이 몰리는 곳: {', '.join(top_inflow[:2])}"
    else:
        signal = "뚜렷한 수급 쏠림 없음"

    report = SectorFlowReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        data_date=latest_date,
        total_sectors=len(sector_flows),
        total_stocks_with_flow=total_with_flow,
        sectors=[asdict(sf) for sf in sector_flows],
        top_inflow=top_inflow,
        top_outflow=top_outflow,
        signal=signal,
    )

    # JSON 저장
    try:
        RESULT_PATH.write_text(
            json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info(f"[섹터수급] 저장 완료: {RESULT_PATH.name}")
    except Exception as e:
        logger.error(f"[섹터수급] 저장 실패: {e}")

    return report


# ─── 텔레그램 보고서 (주린이 한국어) ─────────────────
def format_telegram_report(report: SectorFlowReport) -> str:
    """
    주린이도 알기 쉬운 섹터 수급 텔레그램 보고서.

    - 억원 단위
    - 한국어 해설
    - 컬러 이모지
    """
    lines = [
        f"📊 섹터 수급 리포트",
        f"📅 데이터: {report.data_date} | {report.total_stocks_with_flow}종목 분석",
        "",
    ]

    sectors = report.sectors
    if not sectors:
        lines.append("데이터 없음")
        return "\n".join(lines)

    # ── 매수 집중 섹터 (기관+외인 3일 합산 양수, TOP 5) ──
    buy_sectors = [s for s in sectors if s["inst_3d"] + s["foreign_3d"] > 0]
    if buy_sectors:
        lines.append("🔥 돈이 몰리는 섹터 (3일 기준)")
        for i, s in enumerate(buy_sectors[:5]):
            icon = _flow_icon(s)
            inst = s["inst_3d"]
            foreign = s["foreign_3d"]
            con_inst = s["inst_consecutive"]
            con_foreign = s["foreign_consecutive"]

            # 연속일 표시
            inst_streak = f" ({con_inst}일연속)" if con_inst >= 2 else ""
            foreign_streak = f" ({con_foreign}일연속)" if con_foreign >= 2 else ""

            lines.append(
                f"  {i+1}. {icon} {s['alias']}"
            )
            lines.append(
                f"     기관 {_fmt_억(inst)}{inst_streak} | "
                f"외인 {_fmt_억(foreign)}{foreign_streak}"
            )
            # 합의 판단
            if s["agreement"] not in ("중립", ""):
                lines.append(f"     → {s['agreement_desc']}")
        lines.append("")

    # ── 이탈 섹터 (기관+외인 3일 합산 음수, 하위 5) ──
    sell_sectors = [s for s in sectors if s["inst_3d"] + s["foreign_3d"] < 0]
    sell_sectors.sort(key=lambda s: s["inst_3d"] + s["foreign_3d"])  # 가장 많이 빠진 순
    if sell_sectors:
        lines.append("🧊 돈이 빠지는 섹터 (3일 기준)")
        for i, s in enumerate(sell_sectors[:5]):
            inst = s["inst_3d"]
            foreign = s["foreign_3d"]
            con_inst = s["inst_consecutive"]
            con_foreign = s["foreign_consecutive"]

            inst_streak = f" ({abs(con_inst)}일연속)" if con_inst <= -2 else ""
            foreign_streak = f" ({abs(con_foreign)}일연속)" if con_foreign <= -2 else ""

            lines.append(
                f"  {i+1}. {s['alias']}"
            )
            lines.append(
                f"     기관 {_fmt_억(inst)}{inst_streak} | "
                f"외인 {_fmt_억(foreign)}{foreign_streak}"
            )
            if s["agreement"] not in ("중립", ""):
                lines.append(f"     → {s['agreement_desc']}")
        lines.append("")

    # ── 한줄 요약 ──
    lines.append(f"💡 {report.signal}")

    # ── 주린이 해설 ──
    agreement_buy = [s for s in sectors if s["agreement"] == "합의매수"]
    agreement_sell = [s for s in sectors if s["agreement"] == "합의매도"]
    diverge = [s for s in sectors if s["agreement"] == "의견분열"]

    if agreement_buy:
        names = ", ".join(s["alias"] for s in agreement_buy[:3])
        lines.append(f"✅ 기관+외인 합의매수: {names}")
        lines.append(f"   (프로들이 같은 방향 = 신뢰도 높음)")

    if agreement_sell:
        names = ", ".join(s["alias"] for s in agreement_sell[:3])
        lines.append(f"⛔ 기관+외인 합의매도: {names}")
        lines.append(f"   (큰손들이 빠지는 중 = 주의)")

    if diverge:
        names = ", ".join(s["alias"] for s in diverge[:2])
        lines.append(f"⚖️ 의견 분열: {names}")
        lines.append(f"   (기관과 외인이 반대 방향 = 관망)")

    return "\n".join(lines)


def _flow_icon(s: dict) -> str:
    """섹터 수급 아이콘"""
    agreement = s.get("agreement", "")
    if agreement == "합의매수":
        return "🟢"
    elif agreement in ("기관매집", "외인매집"):
        return "🔵"
    elif agreement == "합의매도":
        return "🔴"
    elif agreement == "의견분열":
        return "🟡"
    return "⚪"


def _fmt_억(val: float) -> str:
    """억원 포맷 (부호 포함)"""
    if abs(val) >= 1000:
        return f"{val:+,.0f}억"
    elif abs(val) >= 1:
        return f"{val:+,.0f}억"
    else:
        return f"{val:+.1f}억"


# ─── 추천 연동: 섹터 부스트 점수 조회 ────────────────
def get_sector_flow_boost(sector: str) -> float:
    """
    특정 섹터의 기관 수급 부스트 점수 반환.
    morning_recommendation.py에서 호출.

    Returns: -15.0 ~ +15.0
    """
    if not RESULT_PATH.exists():
        return 0.0
    try:
        data = json.loads(RESULT_PATH.read_text("utf-8"))
        for s in data.get("sectors", []):
            if s.get("sector") == sector or s.get("alias") == sector:
                return s.get("boost_score", 0.0)
        return 0.0
    except Exception:
        return 0.0


def get_sector_flow_summary() -> dict:
    """
    섹터 수급 요약 (BRAIN / 추천 시스템 연동용).

    Returns: {
        "top_inflow": ["반도체/전자", "자동차/조선"],
        "top_outflow": ["바이오/제약"],
        "signal": "돈이 몰리는 곳: 반도체/전자, 자동차/조선",
        "data_date": "2026-03-27",
    }
    """
    if not RESULT_PATH.exists():
        return {}
    try:
        data = json.loads(RESULT_PATH.read_text("utf-8"))
        return {
            "top_inflow": data.get("top_inflow", []),
            "top_outflow": data.get("top_outflow", []),
            "signal": data.get("signal", ""),
            "data_date": data.get("data_date", ""),
        }
    except Exception:
        return {}


# ─── CLI ──────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    print("섹터 기관 수급 분석 시작...")
    report = analyze_sector_flow()

    print(f"\n총 {report.total_sectors}개 섹터, {report.total_stocks_with_flow}종목 분석")
    print(f"데이터 기준일: {report.data_date}\n")

    # 텔레그램 보고서 미리보기
    msg = format_telegram_report(report)
    try:
        print(msg)
    except UnicodeEncodeError:
        # Windows cp949 fallback
        sys.stdout.reconfigure(encoding="utf-8")
        print(msg)

    # 상세 데이터 (--detail)
    if "--detail" in sys.argv:
        print("\n── 전체 섹터 상세 ──")
        for s in report.sectors:
            total = s["inst_3d"] + s["foreign_3d"]
            print(
                f"  {s['alias']:10s} | "
                f"기관3D {s['inst_3d']:+8.0f}억 ({s['inst_consecutive']:+d}일) | "
                f"외인3D {s['foreign_3d']:+8.0f}억 ({s['foreign_consecutive']:+d}일) | "
                f"합계 {total:+8.0f}억 | {s['agreement']}"
            )
