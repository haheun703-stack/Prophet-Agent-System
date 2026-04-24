# -*- coding: utf-8 -*-
"""
동시호가 스캐너 (Pre-Market Auction Scanner)
=============================================
08:30~08:55 동시호가 시간대에 추천 종목의 예상체결가/체결량을 모니터링.
갭업/갭다운/거래량 폭발 감지 → 텔레그램 알림.

사용처: COO A15 (08:30 스케줄)
"""

import asyncio
import json
import logging
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Callable

logger = logging.getLogger("BH.AuctionScanner")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"

# ── 알림 기준 ──
GAP_UP_ALERT = 3.0       # +3% 이상 → 강매수 신호
GAP_UP_WATCH = 1.5       # +1.5% 이상 → 주목
GAP_DOWN_ALERT = -2.0    # -2% 이하 → 진입 보류 경고
GAP_DOWN_DANGER = -4.0   # -4% 이하 → 매수금지
VOL_EXPLOSION = 500_000  # 예상체결량 50만주 이상 → 거래 폭발
BID_ASK_BULL = 150       # 매수/매도 잔량 비율 150% 이상 → 매수 우위


def load_recommendation() -> list[dict]:
    """recommendation.json에서 추천 종목 로드."""
    path = DATA_DIR / "recommendation.json"
    if not path.exists():
        logger.warning("recommendation.json 없음")
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        stocks = data.get("stocks", [])
        if not stocks:
            stocks = data if isinstance(data, list) else []
        return stocks
    except Exception as e:
        logger.error(f"recommendation.json 로드 실패: {e}")
        return []


def scan_auction_once(stocks: list[dict], kis) -> list[dict]:
    """추천 종목의 예상체결가를 1회 스캔.

    Args:
        stocks: [{"code": "005930", "name": "삼성전자", "close": 72000, ...}, ...]
        kis: KISTrader 인스턴스

    Returns: [{code, name, prev_close, expected_price, change_rate,
               expected_volume, bid_ask_ratio, signal}, ...]
    """
    results = []
    failed = 0
    skipped = 0
    for s in stocks:
        code = s.get("code", "")
        name = s.get("name", code)
        prev_close = s.get("close", 0)

        try:
            resp = kis.fetch_expected_price(code)
            if not resp.get("success"):
                logger.debug(f"[AuctionScanner] {name}({code}) 조회 실패: {resp.get('message', '')}")
                failed += 1
                continue

            # 동시호가 시간 아닐 때는 스킵 (정규장 데이터 혼입 방지)
            if not resp.get("is_auction", False):
                skipped += 1
                continue

            exp_price = resp.get("expected_price", 0)
            exp_vol = resp.get("expected_volume", 0)
            chg_rate = resp.get("change_rate", 0)
            bar = resp.get("bid_ask_ratio", 0)

            if not exp_price:
                failed += 1
                continue

            # 전일 종가가 없으면 API 응답값 사용
            if prev_close <= 0:
                prev_close = resp.get("prev_close", exp_price)

            # change_rate가 0이고 prev_close가 있으면 직접 계산
            if chg_rate == 0 and prev_close > 0 and exp_price > 0:
                chg_rate = round((exp_price - prev_close) / prev_close * 100, 2)

            # 시그널 판정
            signal = _classify_signal(chg_rate, exp_vol, bar)

            results.append({
                "code": code,
                "name": name,
                "prev_close": prev_close,
                "expected_price": exp_price,
                "change_rate": chg_rate,
                "expected_volume": exp_vol,
                "bid_ask_ratio": bar,
                "signal": signal,
            })
        except Exception as e:
            logger.warning(f"[AuctionScanner] {name}({code}) 개별 스캔 에러: {e}")
            failed += 1
            continue

        time.sleep(0.15)  # KIS API 초당 제한 (20건/초)

    total = len(stocks)
    logger.info(f"[AuctionScanner] 스캔: {len(results)}/{total} 성공"
                f" ({failed} 실패, {skipped} 비동시호가)")
    return results


def _classify_signal(chg_rate: float, vol: int, bar: float) -> str:
    """변동률 + 체결량 + 호가비율 기반 시그널 분류."""
    if chg_rate >= GAP_UP_ALERT:
        return "GAP_UP_STRONG"       # 강갭업 → 추격 주의
    if chg_rate >= GAP_UP_WATCH:
        return "GAP_UP"              # 갭업 → 주목
    if chg_rate <= GAP_DOWN_DANGER:
        return "GAP_DOWN_DANGER"     # 폭락 → 매수금지
    if chg_rate <= GAP_DOWN_ALERT:
        return "GAP_DOWN"            # 갭다운 → 보류 검토
    if vol >= VOL_EXPLOSION and bar >= BID_ASK_BULL:
        return "VOL_BULL"            # 거래폭발 + 매수우위
    if vol >= VOL_EXPLOSION:
        return "VOL_EXPLOSION"       # 거래량 폭발
    return "NORMAL"                  # 평온


def format_auction_alert(results: list[dict], scan_time: str) -> str:
    """텔레그램 알림 메시지 포맷."""
    if not results:
        return ""

    # 시그널별 이모지
    emoji_map = {
        "GAP_UP_STRONG": "🔥",
        "GAP_UP": "📈",
        "GAP_DOWN_DANGER": "🚫",
        "GAP_DOWN": "⚠️",
        "VOL_BULL": "💪",
        "VOL_EXPLOSION": "📊",
        "NORMAL": "✅",
    }

    lines = [
        f"📊 동시호가 스캔 ({scan_time})",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    # 시그널 심각도순 정렬
    priority = {
        "GAP_DOWN_DANGER": 0, "GAP_DOWN": 1,
        "GAP_UP_STRONG": 2, "GAP_UP": 3,
        "VOL_BULL": 4, "VOL_EXPLOSION": 5, "NORMAL": 6,
    }
    sorted_results = sorted(results, key=lambda x: priority.get(x["signal"], 9))

    for r in sorted_results:
        emoji = emoji_map.get(r["signal"], "")
        chg = r["change_rate"]
        sign = "+" if chg >= 0 else ""
        price_str = f"{r['expected_price']:,}"
        vol_str = _format_volume(r["expected_volume"])
        bar_str = f" B/A:{r['bid_ask_ratio']:.0f}%" if r["bid_ask_ratio"] > 0 else ""

        line = f"{emoji} {r['name']}  {price_str}원 ({sign}{chg:.1f}%)  {vol_str}{bar_str}"
        lines.append(line)

    return "\n".join(lines)


def format_final_summary(results: list[dict]) -> str:
    """08:53 최종 서머리 포맷."""
    if not results:
        return ""

    gap_ups = [r for r in results if r["signal"] in ("GAP_UP_STRONG", "GAP_UP")]
    gap_downs = [r for r in results if r["signal"] in ("GAP_DOWN", "GAP_DOWN_DANGER")]
    normals = [r for r in results if r["signal"] == "NORMAL"]
    vol_alerts = [r for r in results if "VOL" in r["signal"]]

    lines = [
        "📋 동시호가 최종 서머리 (08:55 장 직전)",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    if gap_downs:
        lines.append(f"⚠️ 갭다운 경고 ({len(gap_downs)}종목):")
        for r in gap_downs:
            action = "매수금지" if r["signal"] == "GAP_DOWN_DANGER" else "보류 검토"
            lines.append(f"  {r['name']} {r['change_rate']:+.1f}% → {action}")

    if gap_ups:
        lines.append(f"🔥 갭업 ({len(gap_ups)}종목):")
        for r in gap_ups:
            action = "추격금지(과열)" if r["signal"] == "GAP_UP_STRONG" else "주목"
            lines.append(f"  {r['name']} {r['change_rate']:+.1f}% → {action}")

    if vol_alerts:
        lines.append(f"📊 거래폭발 ({len(vol_alerts)}종목):")
        for r in vol_alerts:
            lines.append(f"  {r['name']} {_format_volume(r['expected_volume'])}")

    if normals:
        names = ", ".join(r["name"] for r in normals)
        lines.append(f"✅ 정상 ({len(normals)}종목): {names}")

    # 종합 판단
    if gap_downs and len(gap_downs) >= len(results) * 0.5:
        lines.append("\n⛔ 종합: 과반 갭다운 — 신중 모드 권장")
    elif gap_ups and len(gap_ups) >= len(results) * 0.5:
        lines.append("\n🔥 종합: 과반 갭업 — 시가 추격 주의, 눌림 대기")
    else:
        lines.append("\n✅ 종합: 혼조세 — 개별 종목 판단")

    return "\n".join(lines)


def save_auction_result(results: list[dict]):
    """동시호가 스캔 결과를 JSON 저장."""
    path = DATA_DIR / "auction_scan.json"
    data = {
        "timestamp": datetime.now().isoformat(),
        "count": len(results),
        "results": results,
    }
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        logger.info(f"동시호가 결과 저장: {path} ({len(results)}종목)")
    except Exception as e:
        logger.error(f"동시호가 결과 저장 실패: {e}")


async def run_auction_scanner(
    kis,
    send_fn: Callable,
    interval: int = 30,
    start_time: dtime = dtime(8, 30),
    end_time: dtime = dtime(8, 53),
    summary_time: dtime = dtime(8, 53),
):
    """동시호가 스캐너 메인 루프.

    Args:
        kis: KISTrader 인스턴스
        send_fn: async callable(text) — 텔레그램 전송 함수
        interval: 스캔 간격 (초)
        start_time: 스캔 시작 시각
        end_time: 루프 종료 시각
        summary_time: 최종 서머리 전송 시각
    """
    logger.info("[AuctionScanner] 시작")

    # 추천 종목 로드
    stocks = load_recommendation()
    if not stocks:
        logger.warning("[AuctionScanner] 추천 종목 없음 — 스킵")
        return {"success": False, "message": "추천 종목 없음"}

    logger.info(f"[AuctionScanner] 대상 {len(stocks)}종목")

    # 첫 스캔 전 동시호가 시간까지 대기
    now = datetime.now()
    if now.time() < start_time:
        wait_sec = (datetime.combine(now.date(), start_time) - now).total_seconds()
        if wait_sec > 0:
            logger.info(f"[AuctionScanner] {start_time} 대기 ({wait_sec:.0f}초)")
            await asyncio.sleep(min(wait_sec, 1800))  # 최대 30분

    # ── 스캔 루프 ──
    scan_count = 0
    last_results = []
    alert_sent = set()  # 이미 알림 보낸 종목 (중복 방지)

    while datetime.now().time() < end_time:
        scan_count += 1
        now_str = datetime.now().strftime("%H:%M")
        logger.info(f"[AuctionScanner] 스캔 #{scan_count} ({now_str})")

        results = await asyncio.to_thread(scan_auction_once, stocks, kis)
        if not results:
            await asyncio.sleep(interval)
            continue

        last_results = results

        # 이상 종목만 알림 (NORMAL 제외, 중복 방지)
        alerts = []
        for r in results:
            sig = r["signal"]
            if sig == "NORMAL":
                continue
            alert_key = f"{r['code']}_{sig}"
            if alert_key in alert_sent:
                continue
            alerts.append(r)
            alert_sent.add(alert_key)

        # 첫 스캔은 전체 전송 (현황 파악)
        if scan_count == 1:
            msg = format_auction_alert(results, now_str)
            if msg:
                try:
                    await send_fn(msg)
                except Exception as e:
                    logger.warning(f"[AuctionScanner] 텔레그램 실패: {e}")
        elif alerts:
            msg = format_auction_alert(alerts, now_str)
            if msg:
                try:
                    await send_fn(msg)
                except Exception as e:
                    logger.warning(f"[AuctionScanner] 텔레그램 실패: {e}")

        await asyncio.sleep(interval)

    # ── 최종 서머리 (08:53) ──
    if last_results:
        summary = format_final_summary(last_results)
        if summary:
            try:
                await send_fn(summary)
            except Exception as e:
                logger.warning(f"[AuctionScanner] 서머리 전송 실패: {e}")

        save_auction_result(last_results)

    logger.info(f"[AuctionScanner] 완료 (총 {scan_count}회 스캔)")
    return {
        "success": True,
        "scan_count": scan_count,
        "stock_count": len(last_results),
        "alerts": len(alert_sent),
    }


def _format_volume(vol: int) -> str:
    """체결량 포맷 (만주 단위)."""
    if vol >= 10_000:
        return f"{vol / 10_000:.1f}만주"
    if vol >= 1_000:
        return f"{vol / 1_000:.1f}천주"
    return f"{vol}주"
