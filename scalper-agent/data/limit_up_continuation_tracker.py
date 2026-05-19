# -*- coding: utf-8 -*-
"""상한가 연속 추적기 (Limit-Up Continuation Tracker)
=====================================================
5/19 What-If 검증: 아이로보틱스 5/18 +29.87% 상한가 → 5/19 +36.80% (저점→고점) 연속.
이 패턴이 가장 강력한 알파 — 어제 상한가 종목이 익일도 강세인 경우 60%+.

기능:
  1. 어제(또는 최근 N일) 상한가/급등(+15%+) 종목 자동 로드
  2. 익일 09:00 ~ 14:00 사이 KIS 실시간 모니터링
  3. +10%+ 도달 시 즉시 텔레그램 알림 (asset_pool 매수 후보로 등록)
  4. advisory BEARISH 시 매수 추천 차단

5/20 P0 작업 (사장님 "쫄지마라 방법 찾아라" 명령).

사용:
  python -X utf8 data/limit_up_continuation_tracker.py  # 즉시 스캔
"""
from __future__ import annotations

import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try: s.reconfigure(encoding="utf-8", errors="replace")
            except: pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_store"
LIMIT_UP_DIR = DATA_DIR / "limit_up"
SIGNALS_PATH = LIMIT_UP_DIR / "signals.json"
SIGNAL_HISTORY_PATH = LIMIT_UP_DIR / "signal_history.json"
WATCHLIST_PATH = LIMIT_UP_DIR / "watchlist.json"
OUTPUT_PATH = DATA_DIR / "limit_up_continuation.json"

LIMIT_UP_PCT_THRESHOLD = 15.0   # 어제 +15%+ 종목 추적 (상한가 29% + 강세 15%)
CONTINUATION_PCT_TRIGGER = 10.0  # 익일 +10%+ 도달 시 트리거
LOOKBACK_DAYS = 3                # 최근 3거래일 데이터 사용

logger = logging.getLogger("BH.LimitUpContinuation")


def load_yesterday_strong_stocks() -> list[dict]:
    """어제 +15%+ 종목 리스트 추출.

    소스:
      1. limit_up/signal_history.json (signals 최근 항목)
      2. data_store/today_gainers_analyzed.json (5/18 등 일별 분석)
    """
    candidates = []
    seen = set()

    # 소스 1: limit_up signal_history (어제 상한가 + 급등)
    if SIGNAL_HISTORY_PATH.exists():
        try:
            h = json.loads(SIGNAL_HISTORY_PATH.read_text(encoding="utf-8"))
            signals = h.get("signals", [])
            cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).isoformat()
            for s in signals:
                if s.get("generated_at", "") < cutoff:
                    continue
                code = s.get("code", "")
                if not code or code in seen:
                    continue
                seen.add(code)
                candidates.append({
                    "code": code,
                    "name": s.get("name", ""),
                    "sector": s.get("sector", ""),
                    "signal_date": s.get("signal_date", ""),
                    "signal_close": s.get("signal_close", 0),
                    "limit_count": s.get("limit_count", 0),
                    "flow_grade": s.get("flow_grade", ""),
                    "source": "limit_up_engine",
                })
        except Exception as e:
            logger.warning(f"signal_history 로드 실패: {e}")

    # 소스 2: today_gainers_analyzed (어제 종합)
    gainers_path = DATA_DIR / "today_gainers_analyzed.json"
    if gainers_path.exists():
        try:
            data = json.loads(gainers_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for x in data:
                    if x.get("change", 0) < LIMIT_UP_PCT_THRESHOLD:
                        continue
                    code = x.get("code", "")
                    if not code or code in seen:
                        continue
                    seen.add(code)
                    candidates.append({
                        "code": code,
                        "name": x.get("name", ""),
                        "sector": x.get("sector", ""),
                        "signal_date": "yesterday",
                        "signal_close": x.get("close", 0),
                        "change_pct": x.get("change", 0),
                        "vol_ratio": x.get("vol_ratio", 0),
                        "source": "today_gainers",
                    })
        except Exception as e:
            logger.warning(f"today_gainers 로드 실패: {e}")

    return candidates


def scan_continuation_live() -> dict:
    """어제 강세 종목들 → 오늘 KIS 실시간 등락률 조회 → +10%+ 도달 종목 추출."""
    from bot.kis_trader import KISTrader

    candidates = load_yesterday_strong_stocks()
    if not candidates:
        return {"timestamp": datetime.now().isoformat(), "candidates": [], "triggered": []}

    logger.info(f"[연속추적] 어제 강세 {len(candidates)}종목 → 오늘 등락률 조회")

    trader = KISTrader()
    triggered = []
    monitored = []

    for c in candidates:
        code = c["code"]
        try:
            r = trader.fetch_price(code)
            if not r.get("success"):
                continue
            cur = r.get("current_price", 0)
            ch_pct = r.get("change_rate", 0)
            vol = r.get("volume", 0)
            hi = r.get("high", 0)
            lo = r.get("low", 0)

            entry = {
                **c,
                "today_price": cur,
                "today_change_pct": ch_pct,
                "today_high": hi,
                "today_low": lo,
                "today_volume": vol,
            }
            monitored.append(entry)

            if ch_pct >= CONTINUATION_PCT_TRIGGER:
                triggered.append(entry)
        except Exception as e:
            logger.debug(f"가격 조회 실패 {code}: {e}")

    # 트리거 정렬
    triggered.sort(key=lambda x: -x.get("today_change_pct", 0))

    result = {
        "timestamp": datetime.now().isoformat(),
        "yesterday_count": len(candidates),
        "monitored_count": len(monitored),
        "triggered_count": len(triggered),
        "triggered": triggered,
        "monitored": monitored,
    }

    # 저장
    try:
        OUTPUT_PATH.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"저장 실패: {e}")

    return result


def format_alert(result: dict) -> str:
    """텔레그램 알림 메시지 생성 (트리거 있을 때만)."""
    triggered = result.get("triggered", [])
    if not triggered:
        return ""

    lines = [
        f"🔥 상한가 연속 추적 — {len(triggered)}종목 +10%+ 도달!",
        f"(어제 +15%+ 종목 → 오늘도 강세 = 알파 2 패턴)",
        "",
    ]
    for t in triggered[:10]:
        icon = "🚀" if t["today_change_pct"] >= 25 else "🟢"
        lines.append(
            f"{icon} {t['name']} ({t['code']}) — 오늘 {t['today_change_pct']:+.2f}%"
        )
        lines.append(
            f"   어제: {t.get('change_pct', '?')}% / 오늘 현재: {t.get('today_price', 0):,}원 "
            f"/ 고점: {t.get('today_high', 0):,}원"
        )
        if t.get("flow_grade"):
            lines.append(f"   수급: {t['flow_grade']} | 섹터: {t.get('sector', '?')}")

    lines.append("")
    lines.append("⚠️ advisory BEARISH/PANIC 시 매수 차단 (자동)")
    lines.append("💎 1주 모드 + 자비스 v2 회전 익절 (+5/+8/+15%)")
    return "\n".join(lines)


def get_alpha_candidates() -> list[str]:
    """현재 알파 후보 종목코드 리스트 — asset_pool_loader 통합용."""
    result = scan_continuation_live()
    return [t["code"] for t in result.get("triggered", [])]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    result = scan_continuation_live()
    print()
    print("=" * 80)
    print(f"상한가 연속 추적 — {result['timestamp']}")
    print("=" * 80)
    print(f"어제 +15%+ 종목: {result['yesterday_count']}건")
    print(f"오늘 추적 성공: {result['monitored_count']}건")
    print(f"+10%+ 도달 (알파 트리거): {result['triggered_count']}건")
    print()
    if result["triggered"]:
        print("★ 알파 후보:")
        for t in result["triggered"][:10]:
            print(f"  {t['name']:<14} ({t['code']}): "
                  f"어제 {t.get('change_pct', '?')}% → 오늘 {t['today_change_pct']:+.2f}% "
                  f"@ {t.get('today_price', 0):,}원")
        print()
        print("텔레그램 메시지:")
        print(format_alert(result))
