# -*- coding: utf-8 -*-
"""
시장 수급 건전성 모니터
========================
시장 전체의 수급 건강 상태를 진단하여 스윙매매 리스크 게이트 역할.

경보 체계:
  NORMAL   — 정상, 풀사이즈 진입 가능
  WARNING  — 주의, 절반 사이즈 권장
  CRITICAL — 위험, 신규 진입 금지

진단 항목:
  1. 외국인 순매수 추세 (KOSPI 전체)
  2. 기관 순매수 추세
  3. 시장 변동성 (KOSPI 일간 변동폭)
  4. 하락 종목 비율 (전체 대비)

사용법:
  python -m data.market_health              # 건전성 진단
  python -m data.market_health --telegram   # 텔레그램 전송
"""

import sys
import io
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
FLOW_DIR = DATA_DIR / "flow"
DAILY_DIR = DATA_DIR / "daily"


# ═══════════════════════════════════════════════════
#  경보 임계값
# ═══════════════════════════════════════════════════

THRESHOLDS = {
    # 외국인 5일 누적 순매도 (전체 유니버스 합산, 억원)
    "foreign_5d_warning": -5000,     # -5000억 이상 순매도 → WARNING
    "foreign_5d_critical": -15000,   # -1.5조 이상 순매도 → CRITICAL

    # 기관 5일 누적 순매도
    "inst_5d_warning": -3000,
    "inst_5d_critical": -8000,

    # 하락 종목 비율 (최근 5일 기준)
    "decline_ratio_warning": 0.65,   # 65% 하락 → WARNING
    "decline_ratio_critical": 0.80,  # 80% 하락 → CRITICAL

    # KOSPI 5일 변동률
    "kospi_drop_warning": -3.0,      # -3% → WARNING
    "kospi_drop_critical": -5.0,     # -5% → CRITICAL
}


@dataclass
class MarketHealthReport:
    """시장 건전성 진단 결과"""
    timestamp: str = ""
    alert_level: str = "normal"      # normal / warning / critical
    alerts: list = field(default_factory=list)

    # 외국인/기관 수급
    foreign_5d_net: float = 0        # 외국인 5일 누적 순매수 (억원)
    inst_5d_net: float = 0           # 기관 5일 누적 순매수 (억원)

    # 시장 상태
    decline_ratio: float = 0         # 하락 종목 비율
    kospi_5d_change: float = 0       # KOSPI 5일 수익률 (%)

    # 포지션 사이즈 조절
    position_multiplier: float = 1.0  # 1.0=풀, 0.5=절반, 0.0=금지

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "alert_level": self.alert_level,
            "alerts": self.alerts,
            "foreign_5d_net": self.foreign_5d_net,
            "inst_5d_net": self.inst_5d_net,
            "decline_ratio": self.decline_ratio,
            "kospi_5d_change": self.kospi_5d_change,
            "position_multiplier": self.position_multiplier,
        }


# ═══════════════════════════════════════════════════
#  데이터 수집 (기존 flow 데이터 활용)
# ═══════════════════════════════════════════════════

def _collect_flow_summary() -> dict:
    """유니버스 전체의 외국인/기관 순매수 합산"""
    from data.universe_builder import load_universe

    universe = load_universe()
    if not universe:
        return {"foreign_5d": 0, "inst_5d": 0}

    foreign_total = 0
    inst_total = 0
    count = 0

    for code in universe:
        flow_file = FLOW_DIR / f"{code}.csv"
        if not flow_file.exists():
            continue

        try:
            df = pd.read_csv(flow_file, index_col=0, parse_dates=True)
            if len(df) < 5:
                continue

            recent = df.tail(5)

            # 컬럼명 체크 (pykrx 형식: 기관계, 외국인계 / 또는 기관순매수, 외인순매수)
            for col_name in ["외국인계", "외인순매수", "외국인"]:
                if col_name in recent.columns:
                    foreign_total += recent[col_name].sum()
                    break

            for col_name in ["기관계", "기관순매수", "기관"]:
                if col_name in recent.columns:
                    inst_total += recent[col_name].sum()
                    break

            count += 1

        except Exception:
            continue

    # 단위 변환 (주 단위 → 억원 근사: 평균 주가 5만원 가정)
    # flow 데이터가 주(shares) 단위면 → 대략적 금액 환산
    # flow 데이터가 이미 금액이면 그대로
    logger.info(f"수급 합산: {count}종목 | 외인: {foreign_total:,.0f} | 기관: {inst_total:,.0f}")

    return {
        "foreign_5d": foreign_total,
        "inst_5d": inst_total,
        "stock_count": count,
    }


def _collect_market_breadth() -> dict:
    """시장 폭 (하락 종목 비율, KOSPI 변동률)"""
    from data.universe_builder import load_universe

    universe = load_universe()
    if not universe:
        return {"decline_ratio": 0, "kospi_5d_change": 0}

    up_count = 0
    down_count = 0
    total = 0

    for code in universe:
        daily_file = DAILY_DIR / f"{code}.csv"
        if not daily_file.exists():
            continue

        try:
            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(df) < 6:
                continue

            # 컬럼 표준화
            close_col = "종가" if "종가" in df.columns else "close"
            if close_col not in df.columns:
                continue

            close_5d_ago = df[close_col].iloc[-6]
            close_now = df[close_col].iloc[-1]

            if close_5d_ago > 0:
                change = (close_now - close_5d_ago) / close_5d_ago * 100
                if change < 0:
                    down_count += 1
                else:
                    up_count += 1
                total += 1

        except Exception:
            continue

    decline_ratio = down_count / max(total, 1)

    # KOSPI 대용 (삼성전자 사용 — 향후 KOSPI ETF로 대체 가능)
    kospi_change = 0
    kospi_file = DAILY_DIR / "069500.csv"  # KODEX 200
    if not kospi_file.exists():
        kospi_file = DAILY_DIR / "005930.csv"  # 삼성전자 폴백
    if kospi_file.exists():
        try:
            df = pd.read_csv(kospi_file, index_col=0, parse_dates=True)
            close_col = "종가" if "종가" in df.columns else "close"
            if len(df) >= 6 and close_col in df.columns:
                kospi_change = (df[close_col].iloc[-1] / df[close_col].iloc[-6] - 1) * 100
        except Exception:
            pass

    return {
        "decline_ratio": round(decline_ratio, 3),
        "up_count": up_count,
        "down_count": down_count,
        "total": total,
        "kospi_5d_change": round(kospi_change, 2),
    }


# ═══════════════════════════════════════════════════
#  경보 판정
# ═══════════════════════════════════════════════════

def diagnose() -> MarketHealthReport:
    """시장 건전성 진단 실행"""
    report = MarketHealthReport(timestamp=datetime.now().isoformat())
    T = THRESHOLDS

    print(f"\n🛡 시장 수급 건전성 진단")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # 1. 수급 데이터
    print("\n[1] 투자자 수급 합산...")
    flow = _collect_flow_summary()
    report.foreign_5d_net = flow["foreign_5d"]
    report.inst_5d_net = flow["inst_5d"]
    print(f"  외국인 5일: {report.foreign_5d_net:+,.0f}")
    print(f"  기관 5일: {report.inst_5d_net:+,.0f}")

    # 2. 시장 폭
    print("\n[2] 시장 폭 분석...")
    breadth = _collect_market_breadth()
    report.decline_ratio = breadth["decline_ratio"]
    report.kospi_5d_change = breadth["kospi_5d_change"]
    print(f"  하락비율: {report.decline_ratio*100:.1f}% ({breadth['down_count']}/{breadth['total']})")
    print(f"  시장 5일: {report.kospi_5d_change:+.2f}%")

    # ─── 경보 판정 ───────────────────────
    alerts = []

    # 외국인 순매도
    if report.foreign_5d_net <= T["foreign_5d_critical"]:
        alerts.append({"level": "critical", "type": "foreign",
                        "message": f"외국인 5일 순매도 {abs(report.foreign_5d_net):,.0f} — 대량 이탈"})
    elif report.foreign_5d_net <= T["foreign_5d_warning"]:
        alerts.append({"level": "warning", "type": "foreign",
                        "message": f"외국인 5일 순매도 {abs(report.foreign_5d_net):,.0f} — 주의"})

    # 기관 순매도
    if report.inst_5d_net <= T["inst_5d_critical"]:
        alerts.append({"level": "critical", "type": "institution",
                        "message": f"기관 5일 순매도 {abs(report.inst_5d_net):,.0f} — 기관 이탈"})
    elif report.inst_5d_net <= T["inst_5d_warning"]:
        alerts.append({"level": "warning", "type": "institution",
                        "message": f"기관 5일 순매도 {abs(report.inst_5d_net):,.0f} — 주의"})

    # 하락 비율
    if report.decline_ratio >= T["decline_ratio_critical"]:
        alerts.append({"level": "critical", "type": "breadth",
                        "message": f"하락 종목 {report.decline_ratio*100:.0f}% — 전면 하락"})
    elif report.decline_ratio >= T["decline_ratio_warning"]:
        alerts.append({"level": "warning", "type": "breadth",
                        "message": f"하락 종목 {report.decline_ratio*100:.0f}% — 약세 구간"})

    # KOSPI 급락
    if report.kospi_5d_change <= T["kospi_drop_critical"]:
        alerts.append({"level": "critical", "type": "kospi",
                        "message": f"시장 5일 {report.kospi_5d_change:+.1f}% — 급락"})
    elif report.kospi_5d_change <= T["kospi_drop_warning"]:
        alerts.append({"level": "warning", "type": "kospi",
                        "message": f"시장 5일 {report.kospi_5d_change:+.1f}% — 약세"})

    # 종합 레벨
    levels = [a["level"] for a in alerts]
    if "critical" in levels:
        report.alert_level = "critical"
        report.position_multiplier = 0.0
    elif "warning" in levels:
        report.alert_level = "warning"
        report.position_multiplier = 0.5
    else:
        report.alert_level = "normal"
        report.position_multiplier = 1.0
        alerts.append({"level": "normal", "type": "all_clear",
                        "message": "수급 구조 정상 범위 내"})

    report.alerts = alerts

    # 출력
    print(f"\n  ━━ 경보 ━━")
    icons = {"critical": "🚨", "warning": "⚠️", "normal": "✅"}
    for a in alerts:
        print(f"  {icons.get(a['level'], '?')} {a['message']}")

    print(f"\n  종합: {report.alert_level.upper()} | 포지션: {report.position_multiplier*100:.0f}%")

    # 저장
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    health_path = DATA_DIR / "market_health.json"
    with open(health_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
    print(f"\n  저장: {health_path}")

    return report


def get_position_multiplier() -> float:
    """시장 건전성 기반 포지션 배수 조회 (캐시 사용)

    Returns: 1.0 (정상) / 0.5 (주의) / 0.0 (위험)
    """
    health_path = DATA_DIR / "market_health.json"
    if not health_path.exists():
        return 1.0

    try:
        with open(health_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 6시간 이내 데이터만 유효
        ts = datetime.fromisoformat(data["timestamp"])
        if (datetime.now() - ts).total_seconds() > 6 * 3600:
            return 1.0

        return data.get("position_multiplier", 1.0)
    except Exception:
        return 1.0


def format_health_report(report: MarketHealthReport) -> str:
    """텔레그램용 포맷"""
    icons = {"critical": "🚨", "warning": "⚠️", "normal": "✅"}
    level_icon = icons.get(report.alert_level, "?")

    lines = []
    lines.append(f"🛡 시장 수급 건전성")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"")
    lines.append(f"경보: {level_icon} {report.alert_level.upper()}")
    lines.append(f"포지션: {report.position_multiplier*100:.0f}%")
    lines.append(f"")
    lines.append(f"외국인 5일: {report.foreign_5d_net:+,.0f}")
    lines.append(f"기관 5일: {report.inst_5d_net:+,.0f}")
    lines.append(f"하락비율: {report.decline_ratio*100:.1f}%")
    lines.append(f"시장 5일: {report.kospi_5d_change:+.1f}%")
    lines.append(f"")

    for a in report.alerts:
        lines.append(f"{icons.get(a['level'], '?')} {a['message']}")

    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="시장 건전성 모니터")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    args = parser.parse_args()

    report = diagnose()

    if args.telegram:
        try:
            from bot.telegram_bot import send_message
            msg = format_health_report(report)
            send_message(msg)
            print("\n  텔레그램 전송 완료")
        except Exception as e:
            print(f"\n  텔레그램 전송 실패: {e}")
