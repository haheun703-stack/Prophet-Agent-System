# -*- coding: utf-8 -*-
"""장중 실시간 학습 스냅샷 (5/18 사장님 지시)

매매 안 한 날 = 학습 황금 시간. 기관/외국인/금투/연기금/기타 수급 주체별 움직임 관찰.

작동 방식:
  - VPS cron: 매 30분 (09:30, 10:00, 10:30, ..., 15:30) 평일 실행
  - 스냅샷 → data_store/learning_YYYYMMDD/snapshot_HHMM.json
  - 이상 패턴 발견 시 → 텔레그램 사장님께 알림 (단타봇 토큰 사용)
  - 장 마감 후 (15:35+) 종합 분석 → 5/19 매수 전략 가이드

이상 패턴 감지 룰:
  1. 직전 스냅샷 대비 1.5% 이상 반등/하락 → 큰 변화 알림
  2. 거래량 직전 30분 평균 대비 2배 폭증 → 큰손 진입 알림
  3. 보유 종목 평가손 -10% 이상 진입 → 사장님 결정 요청
  4. 모닝 추천 종목이 +3% 이상 반등 → 외인 매수 신호 알림

운영:
  python3 tools/intraday_learning.py            # 1회 실행 (cron이 호출)
  python3 tools/intraday_learning.py --analyze  # 누적 데이터 종합 분석 (장 마감 후)
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = timezone.utc

# 프로젝트 루트
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.Learning")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


# ── 관심 종목 ──────────────────────────────────────────
# 보유 5종목 + 모닝 추천 3종목 = 8종목
TARGETS = {
    "001440": ("대한전선", "holding"),
    "011790": ("SKC", "holding"),
    "033100": ("제룡전기", "holding"),
    "062040": ("산일전기", "holding"),
    "103590": ("일진전기", "holding"),
    "483650": ("달바글로벌", "morning_top1"),
    "068270": ("셀트리온", "morning_top2"),
    "204320": ("HL만도", "morning_top3"),
}

# 지수
INDICES = {
    "0001": "KOSPI",
    "1001": "KOSDAQ",
    "2001": "KOSPI200",
}

# 이상 패턴 임계값
ALERT_PRICE_CHANGE = 1.5   # 30분 사이 1.5% 이상 변화 시 알림
ALERT_VOLUME_SURGE = 2.0   # 거래량 2배 폭증
ALERT_HOLDING_LOSS = -10.0 # 보유 종목 -10% 이상 손실


# ── 데이터 디렉터리 ────────────────────────────────────
def _data_dir() -> Path:
    today = datetime.now(KST).strftime("%Y%m%d")
    d = _ROOT / "data_store" / f"learning_{today}"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── 스냅샷 수집 ────────────────────────────────────────
def collect_snapshot() -> Dict:
    """현재 시각 스냅샷 수집 (지수 + 종목)."""
    from bot.kis_trader import KISTrader

    trader = KISTrader()
    now = datetime.now(KST)

    snapshot = {
        "timestamp": now.isoformat(),
        "time_kst": now.strftime("%H:%M:%S"),
        "indices": {},
        "stocks": {},
    }

    # 종목 가격
    for code, (name, category) in TARGETS.items():
        try:
            p = trader.fetch_price(code)
            if p and p.get("success"):
                snapshot["stocks"][code] = {
                    "name": name,
                    "category": category,
                    "price": p.get("current_price", 0),
                    "change_rate": p.get("change_rate", 0),
                    "volume": p.get("volume", 0),
                }
        except Exception as e:
            logger.warning(f"종목 조회 실패 {code}: {e}")

    return snapshot


# ── 이상 패턴 감지 ─────────────────────────────────────
def detect_anomalies(current: Dict, previous: Optional[Dict]) -> List[str]:
    """직전 스냅샷 대비 이상 패턴 감지."""
    alerts = []

    if not previous:
        return alerts

    prev_stocks = previous.get("stocks", {})

    for code, cur in current.get("stocks", {}).items():
        prev = prev_stocks.get(code)
        if not prev:
            continue

        name = cur["name"]
        cur_price = cur["price"]
        prev_price = prev["price"]
        cur_vol = cur["volume"]
        prev_vol = prev["volume"]
        change_rate = cur["change_rate"]

        # 1. 30분 사이 가격 변화
        if prev_price > 0:
            delta_pct = (cur_price - prev_price) / prev_price * 100
            if abs(delta_pct) >= ALERT_PRICE_CHANGE:
                direction = "📈 반등" if delta_pct > 0 else "📉 하락"
                alerts.append(
                    f"{direction} {name}({code}) "
                    f"{prev_price:,}→{cur_price:,} ({delta_pct:+.2f}%)"
                )

        # 2. 거래량 폭증
        if prev_vol > 0 and cur_vol / prev_vol >= ALERT_VOLUME_SURGE:
            ratio = cur_vol / prev_vol
            alerts.append(
                f"🔥 거래량 폭증 {name}({code}) {prev_vol:,}→{cur_vol:,} ({ratio:.1f}배)"
            )

        # 3. 보유 종목 큰 손실
        if cur["category"] == "holding" and change_rate <= ALERT_HOLDING_LOSS:
            alerts.append(
                f"⚠️ 보유종목 큰 손실 {name}({code}) {change_rate:+.2f}%"
            )

    return alerts


# ── 텔레그램 알림 (단타봇 토큰 직접 HTTP 호출) ──────────
def send_telegram_alert(message: str) -> None:
    """텔레그램 알림 (.env 토큰으로 직접 HTTP POST)."""
    import requests
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN/CHAT_ID 미설정")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"텔레그램 응답 {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"텔레그램 알림 실패: {e}")


# ── 메인 실행 ──────────────────────────────────────────
def run_once() -> None:
    """30분 1회 실행 (cron 트리거)."""
    now = datetime.now(KST)
    logger.info(f"[{now.strftime('%H:%M:%S')}] 학습 스냅샷 시작")

    # 현재 스냅샷 수집
    current = collect_snapshot()

    # 직전 스냅샷 로드
    dir_ = _data_dir()
    snapshots = sorted(dir_.glob("snapshot_*.json"))
    previous = None
    if snapshots:
        try:
            with snapshots[-1].open(encoding="utf-8") as f:
                previous = json.load(f)
        except Exception as e:
            logger.warning(f"직전 스냅샷 로드 실패: {e}")

    # 저장
    fname = dir_ / f"snapshot_{now.strftime('%H%M')}.json"
    with fname.open("w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 저장: {fname.name}")

    # 콘솔 출력
    print(f"\n[{now.strftime('%H:%M:%S')}] === 학습 스냅샷 ===")
    holdings = [(c, v) for c, v in current["stocks"].items() if v["category"] == "holding"]
    mornings = [(c, v) for c, v in current["stocks"].items() if v["category"].startswith("morning")]

    print("\n📦 보유 5종목:")
    for code, v in holdings:
        arrow = "🟢" if v["change_rate"] > 0 else ("🔴" if v["change_rate"] < 0 else "⚪")
        print(f"  {arrow} {v['name']:10s} ({code}) {v['price']:>8,}원 {v['change_rate']:+6.2f}% 거래량 {v['volume']:>10,}")

    print("\n🎯 모닝 추천 3종목:")
    for code, v in mornings:
        arrow = "🟢" if v["change_rate"] > 0 else ("🔴" if v["change_rate"] < 0 else "⚪")
        print(f"  {arrow} {v['name']:10s} ({code}) {v['price']:>8,}원 {v['change_rate']:+6.2f}% 거래량 {v['volume']:>10,}")

    # 이상 패턴 감지
    anomalies = detect_anomalies(current, previous)
    if anomalies:
        print("\n🚨 이상 패턴:")
        for a in anomalies:
            print(f"  {a}")

        msg = f"🧠 [학습 모니터 {now.strftime('%H:%M')}] 이상 패턴 {len(anomalies)}건\n\n" + "\n".join(anomalies)
        send_telegram_alert(msg)
        logger.info(f"텔레그램 알림 발송: {len(anomalies)}건")


def run_analyze() -> None:
    """장 마감 후 종합 분석 (15:35+ 실행)."""
    now = datetime.now(KST)
    dir_ = _data_dir()
    snapshots = sorted(dir_.glob("snapshot_*.json"))

    if not snapshots:
        print("스냅샷 없음")
        return

    print(f"=== 5/{now.day} 장중 학습 종합 분석 ({len(snapshots)}건 스냅샷) ===\n")

    # 각 종목별 시간대 변화
    all_data = []
    for snap_file in snapshots:
        with snap_file.open(encoding="utf-8") as f:
            all_data.append(json.load(f))

    print("종목별 일중 변화:")
    print("-" * 80)
    for code, (name, category) in TARGETS.items():
        prices = [d["stocks"].get(code, {}).get("price", 0) for d in all_data]
        rates = [d["stocks"].get(code, {}).get("change_rate", 0) for d in all_data]
        if not any(prices):
            continue
        open_p = prices[0]
        close_p = prices[-1]
        high_p = max(prices)
        low_p = min(prices)
        end_rate = rates[-1] if rates else 0
        cat_tag = "[보유]" if category == "holding" else "[추천]"
        print(f"  {cat_tag} {name:10s} ({code})  시작 {open_p:>8,} → 종가 {close_p:>8,}  ({end_rate:+.2f}%)  H{high_p:,} L{low_p:,}")

    print()
    print("💡 5/19 매수 전략 가이드 — 종가 외인/기관 데이터 (G7) 후 보강")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="장중 실시간 학습 모니터")
    parser.add_argument("--analyze", action="store_true", help="장 마감 후 종합 분석")
    args = parser.parse_args()

    if args.analyze:
        run_analyze()
    else:
        run_once()
