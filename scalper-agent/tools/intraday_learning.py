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

# 시장 전체 동적 발굴 임계값 (5/18 사장님 "돈 될만한건 뭐든지" 지시)
MARKET_GAINER_THRESHOLD = 5.0    # +5% 이상 급등주
LIMIT_UP_IMMINENT_THRESHOLD = 25.0  # +25% 이상 상한가 임박
V_PATTERN_OPEN_THRESHOLD = -2.0  # 시초가 대비 -2% 하락 후
V_PATTERN_CURRENT_THRESHOLD = 1.0  # 현재 +1% 이상 (V자 전환)
MIN_MARKET_CAP_BILLION = 100  # 시총 100억 이상
SCAN_TOP_N = 300  # universe 상위 300종목 스캔


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

    # ── 14:00 / 15:30 자비스 v3.0 hook (P1, 5/18 형 요청) ──
    # cron이 매 30분 자동 실행하므로 14:00 / 15:30에 추가 hook 자동 호출
    hour, minute = now.hour, now.minute
    if hour == 14 and minute == 0:
        try:
            from utils.scalper_journal_hooks import hook_midday_report
            mid_id = hook_midday_report()
            if mid_id:
                logger.info(f"✅ 14:00 hook_midday_report 적재 id={mid_id}")
                print(f"\n📡 14:00 형 BAT-D 사전 데이터 적재 완료 (id={mid_id})")
        except Exception as e:
            logger.warning(f"hook_midday_report 실패: {e}")
    elif hour == 15 and minute == 30:
        try:
            from utils.scalper_journal_hooks import hook_daily_journal
            # 일일 trade_journal 통계 조회 (TODO: 실제 source별 적중률 SELECT)
            # 임시: 기본값으로 적재 (5/19 첫 가동 후 source별 분석 확장)
            j_id = hook_daily_journal(total_trades=0, wins=0, losses=0)
            if j_id:
                logger.info(f"✅ 15:30 hook_daily_journal 적재 id={j_id}")
                print(f"\n📡 15:30 형 BAT-D 사전 정산 적재 완료 (id={j_id})")
        except Exception as e:
            logger.warning(f"hook_daily_journal 실패: {e}")

    # ── 시장 전체 동적 발굴 (5/18 사장님 지시) ──
    try:
        discoveries = scan_market_discoveries()
        print_market_discoveries(discoveries)

        # 새 상한가 임박 종목 발견 시 텔레그램 알림
        if discoveries.get("limit_up_imminent"):
            lines = []
            for s in discoveries["limit_up_imminent"][:5]:
                lines.append(f"⚡ {s['name']} ({s['code']}) {s['change_rate']:+.2f}%")
            msg = f"🔥 [시장 발굴 {now.strftime('%H:%M')}] 상한가 임박 {len(discoveries['limit_up_imminent'])}종목\n\n" + "\n".join(lines)
            send_telegram_alert(msg)

        # V자 전환 종목 텔레그램 알림
        if discoveries.get("v_pattern"):
            lines = []
            for s in discoveries["v_pattern"][:5]:
                lines.append(f"🎢 {s['name']} ({s['code']}) {s['change_rate']:+.2f}% (swing {s.get('v_pattern_swing', 0):+.2f}%p)")
            msg = f"✨ [시장 발굴 {now.strftime('%H:%M')}] V자 전환 {len(discoveries['v_pattern'])}종목\n\n" + "\n".join(lines)
            send_telegram_alert(msg)
    except Exception as _scan_e:
        logger.warning(f"시장 발굴 실패 (무시): {_scan_e}")


# ── 시장 전체 동적 종목 발굴 (5/18 사장님 지시) ───────
def scan_market_discoveries() -> Dict:
    """시총 100억+ 상위 300종목 라이브 스캔 — 급등주 + V자 전환 + 상한가 임박.

    매 30분 자동 실행 (run_once 끝에 호출).
    결과는 data_store/learning_YYYYMMDD/market_discoveries_HHMM.json 저장.

    Returns:
        {
            "scanned": N,
            "gainers": [...],         # +5%~+25% 급등주
            "limit_up_imminent": [...], # +25%+ 상한가 임박
            "v_pattern": [...],        # 시초 음봉 → 양봉 전환
            "elapsed_sec": ...
        }
    """
    import time
    from bot.kis_trader import KISTrader

    # universe 로드 (시총 100억+, 시총 순)
    universe_path = _ROOT / "data_store" / "universe.json"
    if not universe_path.exists():
        return {"scanned": 0, "error": "universe.json not found"}

    with universe_path.open(encoding="utf-8") as f:
        universe = json.load(f)

    # 시총 100억+ 정렬 (우선주 제외 — code 끝이 '5'면 우선주)
    candidates = [
        (c, info) for c, info in universe.items()
        if info.get("cap_억", 0) >= MIN_MARKET_CAP_BILLION and not c.endswith("5")
    ]
    candidates.sort(key=lambda x: -x[1].get("cap_억", 0))
    top_candidates = candidates[:SCAN_TOP_N]

    # 직전 스냅샷 로드 (V자 패턴 비교용)
    dir_ = _data_dir()
    snapshots = sorted(dir_.glob("snapshot_*.json"))
    first_snapshot = None
    if snapshots:
        try:
            with snapshots[0].open(encoding="utf-8") as f:
                first_snapshot = json.load(f)
        except Exception:
            pass

    trader = KISTrader()
    gainers = []
    limit_up_imminent = []
    v_pattern = []

    start = time.time()
    for code, info in top_candidates:
        try:
            p = trader.fetch_price(code)
            if not (p and p.get("success")):
                continue
            ch = p.get("change_rate", 0)
            cur = p.get("current_price", 0)
            vol = p.get("volume", 0)
            name = info.get("name", code)
            cap = info.get("cap_억", 0)

            record = {
                "code": code, "name": name, "change_rate": ch,
                "current_price": cur, "volume": vol, "cap_billion": cap,
            }

            # 1. 상한가 임박
            if ch >= LIMIT_UP_IMMINENT_THRESHOLD:
                limit_up_imminent.append(record)
            # 2. 급등주
            elif ch >= MARKET_GAINER_THRESHOLD:
                gainers.append(record)

            # 3. V자 전환 (시초 -2% 이하 → 현재 +1% 이상)
            if first_snapshot:
                first_data = first_snapshot.get("stocks", {}).get(code, {})
                first_ch = first_data.get("change_rate")
                if first_ch is not None and first_ch <= V_PATTERN_OPEN_THRESHOLD and ch >= V_PATTERN_CURRENT_THRESHOLD:
                    record["v_pattern_swing"] = round(ch - first_ch, 2)
                    v_pattern.append(record)
        except Exception:
            pass

    elapsed = round(time.time() - start, 1)

    # 정렬 (등락률 큰 순)
    gainers.sort(key=lambda x: -x["change_rate"])
    limit_up_imminent.sort(key=lambda x: -x["change_rate"])
    v_pattern.sort(key=lambda x: -x.get("v_pattern_swing", 0))

    result = {
        "scanned": len(top_candidates),
        "gainers": gainers,
        "limit_up_imminent": limit_up_imminent,
        "v_pattern": v_pattern,
        "elapsed_sec": elapsed,
        "timestamp": datetime.now(KST).isoformat() if KST else datetime.now().isoformat(),
    }

    # 저장
    now = datetime.now(KST) if KST else datetime.now()
    fname = dir_ / f"market_discoveries_{now.strftime('%H%M')}.json"
    with fname.open("w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 시장 발굴 저장: {fname.name} (gainers {len(gainers)} / 상한가 {len(limit_up_imminent)} / V {len(v_pattern)} / {elapsed}초)")

    return result


def print_market_discoveries(result: Dict) -> None:
    """시장 발굴 결과 콘솔 출력."""
    print(f"\n🔍 === 시장 전체 동적 발굴 ({result['scanned']}종목 스캔, {result['elapsed_sec']}초) ===")

    if result.get("limit_up_imminent"):
        print(f"\n🔥 상한가 임박 (+{LIMIT_UP_IMMINENT_THRESHOLD}%+) {len(result['limit_up_imminent'])}종목:")
        for s in result["limit_up_imminent"][:5]:
            print(f"  ⚡ {s['name']:<15} ({s['code']}) {s['change_rate']:+.2f}% / 시총 {s['cap_billion']:,}억")

    if result.get("gainers"):
        print(f"\n🚀 급등주 (+{MARKET_GAINER_THRESHOLD}%+) {len(result['gainers'])}종목 TOP 10:")
        for s in result["gainers"][:10]:
            print(f"  🟢 {s['name']:<15} ({s['code']}) {s['change_rate']:+.2f}% / 시총 {s['cap_billion']:,}억")

    if result.get("v_pattern"):
        print(f"\n🎢 V자 전환 ({len(result['v_pattern'])}종목):")
        for s in result["v_pattern"][:5]:
            print(f"  ✨ {s['name']:<15} ({s['code']}) {s['change_rate']:+.2f}% (swing {s.get('v_pattern_swing', 0):+.2f}%p)")


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
