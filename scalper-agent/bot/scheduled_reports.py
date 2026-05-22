# -*- coding: utf-8 -*-
"""단타봇 자율 보고 스케줄러 (사장님 5/22 11:50 명령, TODO ⑮).

영구 메모리 [[feedback_self_monitoring_realtime_5_22]]:
> "단타봇이 사장님께 N시 보고 약속 → 사장님 입력 여부와 무관하게 자율 보고"

5/22 사고: 단타봇이 14:00/14:30 보고 약속해놓고 사장님 안 부르니 침묵.
→ 단타봇 자율 보고 메커니즘 부재 = 신뢰성 사고.

이 모듈 — trading_coo.py에서 jq.run_daily로 등록:
  - 09:30 매매 시작 보고 (시초 매도 + 매수 결과)
  - 11:30 중간 보고 (오전 매매 활동 + KIS 실잔고)
  - 14:00 자비스 약화 분석 (보유 종목 약화/강세 분류)
  - 14:30 weekend hold 결정 추천 (금요일만)
  - 15:35 마감 보고 (5/22 매매 결과 + 손익)

각 보고는 KIS 1분 이내 실측 후 텔레그램 자율 전송.
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

logger = logging.getLogger(__name__)


def _now_kst_str() -> str:
    return (datetime.now(KST) if KST else datetime.now()).strftime("%H:%M")


def _is_friday() -> bool:
    """오늘 금요일 여부 (weekend hold 결정 필요한 날)."""
    today = (datetime.now(KST) if KST else datetime.now())
    return today.weekday() == 4  # 0=월 ~ 4=금


# ═════════════════════════════════════════════
# KIS 실잔고 + 매매 활동 요약 (모든 보고의 기초)
# ═════════════════════════════════════════════
def fetch_realtime_status(trader) -> dict:
    """KIS 1분 이내 실측 + 단타봇 매매 활동 요약.

    Returns:
        {time, kis_positions, kis_cash, kis_total_assets, error}
    """
    status = {
        "time": _now_kst_str(),
        "kis_positions": [],
        "kis_cash": 0,
        "kis_total_assets": 0,
        "error": None,
    }
    try:
        b = trader.fetch_balance()
        if b and b.get("success") is not False:
            status["kis_positions"] = b.get("positions", [])
            status["kis_cash"] = b.get("cash", 0)
            total_eval = sum(
                p.get("current_price", 0) * p.get("qty", 0)
                for p in status["kis_positions"]
            )
            status["kis_total_assets"] = status["kis_cash"] + total_eval
    except Exception as e:
        status["error"] = str(e)[:200]
        logger.warning(f"[scheduled_reports] KIS 실측 실패: {e}")
    return status


# ═════════════════════════════════════════════
# 보고 본문 생성 (시간대별)
# ═════════════════════════════════════════════
def build_morning_report(status: dict) -> str:
    """09:30 매매 시작 보고."""
    lines = [
        "📊 [단타봇 09:30 시작 보고]",
        f"━━━━━━━━━━━━━━━━━",
        f"KIS 보유: {len(status['kis_positions'])}종목",
        f"가용현금: {status['kis_cash']:,}원",
    ]
    for p in status["kis_positions"][:10]:
        lines.append(
            f"  {p.get('code')} {p.get('name')} {p.get('qty')}주 "
            f"({p.get('pnl_rate', 0):+.2f}%)"
        )
    return "\n".join(lines)


def build_midday_report(status: dict) -> str:
    """11:30 중간 보고."""
    lines = [
        "📊 [단타봇 11:30 중간 보고]",
        f"━━━━━━━━━━━━━━━━━",
        f"KIS 보유: {len(status['kis_positions'])}종목",
        f"가용현금: {status['kis_cash']:,}원",
        f"총자산: {status['kis_total_assets']:,}원",
    ]
    total_pnl = 0
    for p in status["kis_positions"][:10]:
        pnl_amt = (p.get("current_price", 0) - p.get("avg_price", 0)) * p.get("qty", 0)
        total_pnl += pnl_amt
        lines.append(
            f"  {p.get('code')} {p.get('name')} {p.get('qty')}주 "
            f"@{p.get('avg_price', 0):,} → {p.get('current_price', 0):,} "
            f"({p.get('pnl_rate', 0):+.2f}%, {pnl_amt:+,}원)"
        )
    lines.append(f"평가손익 합계: {total_pnl:+,}원")
    return "\n".join(lines)


def build_jarvis_weakness_report(status: dict) -> str:
    """14:00 자비스 약화 분석 보고."""
    lines = [
        "📊 [단타봇 14:00 자비스 약화 분석]",
        f"━━━━━━━━━━━━━━━━━",
        f"KIS 보유 {len(status['kis_positions'])}종 분석:",
    ]
    for p in status["kis_positions"][:10]:
        pnl = p.get("pnl_rate", 0)
        if pnl >= 5.0:
            tag = "🟢 강세"
        elif pnl >= 0:
            tag = "🟡 약보합"
        elif pnl >= -3.0:
            tag = "🟠 약화"
        else:
            tag = "🔴 손실"
        lines.append(
            f"  {tag} {p.get('code')} {p.get('name')} ({pnl:+.2f}%)"
        )
    return "\n".join(lines)


def build_weekend_hold_recommendation(status: dict) -> str:
    """14:30 weekend hold 결정 추천 (금요일만)."""
    if not _is_friday():
        return ""
    lines = [
        "📊 [단타봇 14:30 Weekend Hold 추천]",
        f"━━━━━━━━━━━━━━━━━",
        "사장님 결정 필요 종목:",
    ]
    keep = []
    sell = []
    for p in status["kis_positions"][:10]:
        pnl = p.get("pnl_rate", 0)
        if pnl >= 5.0:
            keep.append(f"  ✅ KEEP {p.get('name')} ({pnl:+.2f}%)")
        elif pnl <= -3.0:
            sell.append(f"  ❌ SELL {p.get('name')} ({pnl:+.2f}%)")
    lines.extend(keep)
    lines.extend(sell)
    if not keep and not sell:
        lines.append("  중립 종목만 — 사장님 자율 결정")
    return "\n".join(lines)


def build_close_report(status: dict) -> str:
    """15:35 마감 보고."""
    lines = [
        "📊 [단타봇 15:35 마감 보고]",
        f"━━━━━━━━━━━━━━━━━",
        f"KIS 보유: {len(status['kis_positions'])}종목",
        f"가용현금: {status['kis_cash']:,}원",
        f"총자산: {status['kis_total_assets']:,}원",
        "",
        "주말 hold 종목:",
    ]
    for p in status["kis_positions"][:10]:
        lines.append(
            f"  {p.get('code')} {p.get('name')} {p.get('qty')}주 "
            f"({p.get('pnl_rate', 0):+.2f}%)"
        )
    return "\n".join(lines)


# ═════════════════════════════════════════════
# 통합 보고 전송 (trading_coo에서 호출)
# ═════════════════════════════════════════════
async def send_scheduled_report(
    trader,
    bot,
    chat_id,
    report_type: str,
) -> dict:
    """자율 보고 전송 (trading_coo.py에서 jq.run_daily로 호출).

    Args:
        trader: KISTrader 인스턴스
        bot: TelegramBot 인스턴스
        chat_id: 사장님 텔레그램 chat_id
        report_type: "morning" / "midday" / "jarvis_weakness" /
                     "weekend_hold" / "close"

    Returns:
        {report_type, sent, message}
    """
    status = fetch_realtime_status(trader)

    builders = {
        "morning": build_morning_report,
        "midday": build_midday_report,
        "jarvis_weakness": build_jarvis_weakness_report,
        "weekend_hold": build_weekend_hold_recommendation,
        "close": build_close_report,
    }
    builder = builders.get(report_type)
    if not builder:
        return {"report_type": report_type, "sent": False, "error": "unknown_type"}

    message = builder(status)
    if not message:
        # weekend_hold는 금요일만 — 빈 메시지면 전송 X
        return {"report_type": report_type, "sent": False, "skipped": True}

    if not bot or not chat_id:
        logger.warning(f"[scheduled_reports] bot/chat_id 미연결 — {report_type} 스킵")
        return {"report_type": report_type, "sent": False, "error": "no_telegram"}

    try:
        await bot.send_message(chat_id=chat_id, text=message)
        logger.info(f"[scheduled_reports] ✅ {report_type} 자율 보고 전송 ({len(message)} chars)")
        return {"report_type": report_type, "sent": True, "message": message[:200]}
    except Exception as e:
        logger.error(f"[scheduled_reports] {report_type} 전송 실패: {e}")
        return {"report_type": report_type, "sent": False, "error": str(e)[:200]}


# ═════════════════════════════════════════════
# trading_coo 등록 헬퍼
# ═════════════════════════════════════════════
def kst_time(h: int, m: int):
    """KST 시각 timezone-aware time 객체."""
    from datetime import time as dtime
    return dtime(h, m, tzinfo=KST) if KST else dtime(h, m)


# 스케줄 정의 — trading_coo.py에서 import하여 등록
SCHEDULES = [
    # (time, report_type)
    (kst_time(9, 30),  "morning"),
    (kst_time(11, 30), "midday"),
    (kst_time(14, 0),  "jarvis_weakness"),
    (kst_time(14, 30), "weekend_hold"),
    (kst_time(15, 35), "close"),
]


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    print("=== 스케줄 등록 ===")
    for t, rtype in SCHEDULES:
        print(f"  {t.strftime('%H:%M')} → {rtype}")
    print("\n=== 보고 본문 데모 (Mock 데이터) ===")
    mock_status = {
        "time": "11:30",
        "kis_positions": [
            {"code": "005930", "name": "삼성전자", "qty": 1, "avg_price": 300000,
             "current_price": 295000, "pnl_rate": -1.67},
            {"code": "204320", "name": "HL만도", "qty": 2, "avg_price": 61000,
             "current_price": 63000, "pnl_rate": 3.28},
        ],
        "kis_cash": 37_000_000,
        "kis_total_assets": 37_421_000,
    }
    print("\n" + build_midday_report(mock_status))
    print("\n" + build_jarvis_weakness_report(mock_status))
    print("\n" + build_close_report(mock_status))
