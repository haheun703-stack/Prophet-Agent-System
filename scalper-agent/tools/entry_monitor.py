# -*- coding: utf-8 -*-
"""
장중 추적 모니터 + 텔레그램 알림
================================
3가지 모드:
  1. 전쟁 모드(기본): recommendation.json에서 추천 종목 자동 로드
  2. 바운스 모드: bounce_candidates.json에서 낙폭반등 종목 로드
  3. 레거시 모드: 기존 하드코딩 종목 사용

기능:
  - 30초 간격 실시간 가격 추적
  - 텔레그램 알림: 진입시그널 / 급등급락 / 30분 요약
  - 09:00~15:30 장중 자동 실행 (장전/장후 대기)

Usage:
  python tools/entry_monitor.py           # recommendation.json 기반
  python tools/entry_monitor.py --bounce  # bounce_candidates.json 기반 (낙폭반등)
  python tools/entry_monitor.py --bounce --top 3  # 바운스 TOP 3만
  python tools/entry_monitor.py --legacy  # 기존 하드코딩 종목
"""
import sys, os, time, json, requests, logging
from datetime import datetime
from pathlib import Path

# PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

from bot.kis_trader import KISTrader

logger = logging.getLogger(__name__)

# ─── 텔레그램 동기 알림 ──────────────────────────────

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram(text: str):
    """텔레그램 메시지 전송 (동기, requests 기반)"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[TG] 토큰/챗ID 없음 — 콘솔만 출력")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        }, timeout=10)
        if resp.status_code != 200:
            print(f"[TG] 전송 실패: {resp.status_code}")
    except Exception as e:
        print(f"[TG] 오류: {e}")


# ─── 추천 종목 로드 ──────────────────────────────────

def load_war_mode_targets() -> list[dict]:
    """recommendation.json에서 전쟁 모드 종목 로드"""
    rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
    if not rec_path.exists():
        print("[WARN] recommendation.json 없음")
        return []

    with open(rec_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    targets = []
    for s in data.get("stocks", []):
        if not s.get("code") or not s.get("name"):
            continue
        targets.append({
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "close": s.get("close", 0),
            "sl": s.get("sl", 0),
            "tp": s.get("tp", 0),
            "score": s.get("total_score", 0),
            "sources": s.get("sources", []),
            "tech_detail": s.get("tech_detail", ""),
            "memo": " + ".join(s.get("sources", [])[:2]),
        })
    return targets


# ─── 바운스 헌터 종목 로드 ────────────────────────────

def load_bounce_targets(top_n: int = 0) -> list[dict]:
    """bounce_candidates.json에서 낙폭반등 종목 로드

    Args:
        top_n: 상위 N개만 로드 (0=전체)
    """
    bc_path = Path(__file__).resolve().parent.parent / "data_store" / "bounce_candidates.json"
    if not bc_path.exists():
        print("[WARN] bounce_candidates.json 없음")
        return []

    with open(bc_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    targets = []
    candidates = data.get("candidates", [])
    if top_n > 0:
        candidates = candidates[:top_n]

    for c in candidates:
        cp = c.get("current_price", 0)
        tp = c.get("target_bounce", 0)  # 바운스 목표가
        sl = int(cp * 0.95) if cp > 0 else 0  # -5% 손절
        grade = c.get("inst_grade", "?")
        score = c.get("score", 0)

        _code = c.get("code", "")
        _name = c.get("name", "")
        if not _code or not _name:
            continue
        targets.append({
            "code": _code,
            "name": _name,
            "close": cp,
            "sl": sl,
            "tp": tp,
            "score": score,
            "memo": f"★{grade} 점수{score} RSI{float(c.get('rsi', 0)):.0f}",
        })
    return targets


# ─── 기존 하드코딩 종목 (legacy) ──────────────────────

LEGACY_PICKS = [
    {"code": "241560", "name": "두산밥캣", "sl": 52900, "tp": 72000,
     "memo": "PBR0.81 PER12.9", "close": 59200},
    {"code": "375500", "name": "DL이앤씨", "sl": 39770, "tp": 54700,
     "memo": "PBR0.38 극저평가", "close": 46600},
    {"code": "005490", "name": "POSCO홀딩스", "sl": 308460, "tp": 427500,
     "memo": "PBR0.46 배당3.0%", "close": 342000},
    {"code": "028050", "name": "삼성E&A", "sl": 29780, "tp": 39800,
     "memo": "PER11.1 배당2.5%", "close": 31200},
]


# ─── 장중 실시간 TV 스캔 ──────────────────────────────

TV_SCAN_INTERVAL = 300  # 5분


def _load_universe() -> dict:
    """유니버스 로드 (1회)"""
    uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
    if not uni_path.exists():
        return {}
    with open(uni_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _scan_tv_realtime(universe: dict, prev_codes: set) -> list:
    """5분 주기 TV 스캔 → 신규 강신호만 반환

    Returns:
        list of TVSignal (score >= 65, EXPLOSION or QUIET_ACCUMULATION, 미기보고)
    """
    try:
        from data.trading_value_scanner import scan_trading_value
        results = scan_trading_value(universe, min_tv_billion=10.0)
    except Exception as e:
        logger.warning(f"[TV Scan] 실패: {e}")
        return []

    new_signals = []
    for sig in results:
        if sig.code in prev_codes:
            continue
        if sig.score >= 65 and sig.pattern in ("EXPLOSION", "QUIET_ACCUMULATION"):
            new_signals.append(sig)
    return new_signals


def _format_tv_alert(sig) -> str:
    """TV 알림 메시지 포맷"""
    tag = "조용한매집" if sig.pattern == "QUIET_ACCUMULATION" else "거래대금폭발"
    return (
        f"[TV Alert] {sig.name} {tag}!\n"
        f"TV {sig.tv_ratio:.1f}x | 가격 {sig.change_pct:+.1f}% | score {sig.score:.0f}\n"
        f"거래대금 {sig.trading_value:,.0f}억 (평균 {sig.tv_avg20:,.0f}억)\n"
        f"{sig.detail or ''}"
    )


# ─── 모니터 로직 ─────────────────────────────────────

def run_monitor(targets: list[dict], interval: int = 30):
    """장중 추적 모니터 메인 루프

    - 30초 간격 가격 체크
    - 30분마다 텔레그램 요약 전송
    - 급등(+3%) / 급락(-3%) / SL 근접 알림
    - 09:00~15:30 장중만 활성
    """
    trader = KISTrader()

    # 상태 저장 (알림 중복 방지)
    alert_sent = {}    # {code: set(alert_type)}
    last_prices = {}   # {code: last_price}
    open_prices = {}   # {code: 시가}
    last_summary = time.time()   # 마지막 요약 시간

    # TV 실시간 스캔 상태
    tv_universe = _load_universe()
    tv_prev_codes = set()  # 이미 알림 보낸 종목 코드
    last_tv_scan = 0
    tv_enabled = len(tv_universe) > 0

    # 시작 알림
    names = ", ".join(t["name"] for t in targets[:8])
    memos = "\n".join(f"  {t['name']}: {t.get('memo','')}" for t in targets[:8])
    tv_status = f"TV스캔: {len(tv_universe)}종목 5분주기" if tv_enabled else "TV스캔: OFF"
    start_msg = (
        f"📡 장중 추적 모니터 시작\n"
        f"종목: {names}\n"
        f"{memos}\n"
        f"간격: {interval}초 | SL/급등락 알림\n"
        f"30분마다 요약 리포트\n"
        f"{tv_status}"
    )
    print(start_msg)
    send_telegram(start_msg)

    check_count = 0

    while True:
        now = datetime.now()

        # 장전 대기 (08:50 전)
        if now.hour < 8 or (now.hour == 8 and now.minute < 50):
            if check_count == 0:
                print(f"[{now:%H:%M}] 장전 대기 중... (08:50에 시작)")
            time.sleep(60)
            continue

        # 장후 종료 (15:35 이후)
        if now.hour > 15 or (now.hour == 15 and now.minute >= 35):
            close_msg = f"🔔 장 마감 — 모니터 종료 ({now:%H:%M})"
            print(close_msg)
            send_telegram(close_msg)
            # 최종 요약
            _send_summary(targets, last_prices, open_prices)
            break

        check_count += 1
        has_alert = False

        for t in targets:
            try:
                p = trader.fetch_price(t["code"])
                if not p or not p.get("success"):
                    continue

                cp = p["current_price"]
                chg = p.get("change_rate", 0)
                vol = p.get("volume", 0)

                # 시가 기록 (최초 1회)
                if t["code"] not in open_prices:
                    open_prices[t["code"]] = p.get("open", cp)

                last_prices[t["code"]] = cp

                sl = t.get("sl", 0)
                tp = t.get("tp", 0)

                # ── 알림 조건 체크 ──
                code_alerts = alert_sent.setdefault(t["code"], set())

                # 1) SL 근접 (-2% 이내)
                if sl > 0 and cp > 0 and cp > sl:
                    vs_sl = (cp / sl - 1) * 100
                    if vs_sl <= 2.0 and "SL_NEAR" not in code_alerts:
                        msg = f"⚠️ {t['name']} SL근접!\n현재 {cp:,} → SL {sl:,} ({vs_sl:+.1f}%)"
                        send_telegram(msg)
                        code_alerts.add("SL_NEAR")
                        has_alert = True

                # 2) SL 이탈
                if sl > 0 and cp <= sl and "SL_BREAK" not in code_alerts:
                    msg = f"🚨 {t['name']} SL 이탈!\n현재 {cp:,} ≤ SL {sl:,}"
                    send_telegram(msg)
                    code_alerts.add("SL_BREAK")
                    has_alert = True

                # 3) 급등 (+3% 이상 from 시가)
                open_p = open_prices.get(t["code"], cp)
                if open_p > 0:
                    day_chg = (cp / open_p - 1) * 100
                    if day_chg >= 3.0 and "SURGE" not in code_alerts:
                        msg = f"🚀 {t['name']} 급등!\n현재 {cp:,} ({day_chg:+.1f}% from 시가)"
                        send_telegram(msg)
                        code_alerts.add("SURGE")
                        has_alert = True

                    # 4) 급락 (-3% 이상 from 시가)
                    if day_chg <= -3.0 and "DROP" not in code_alerts:
                        msg = f"📉 {t['name']} 급락!\n현재 {cp:,} ({day_chg:+.1f}% from 시가)"
                        send_telegram(msg)
                        code_alerts.add("DROP")
                        has_alert = True

                # 5) TP 도달
                if tp > 0 and cp >= tp and "TP_HIT" not in code_alerts:
                    msg = f"🎯 {t['name']} TP 도달!\n현재 {cp:,} ≥ TP {tp:,}"
                    send_telegram(msg)
                    code_alerts.add("TP_HIT")
                    has_alert = True

                # 콘솔 출력
                vs_tp = (tp / cp - 1) * 100 if tp > 0 and cp > 0 else 0
                print(
                    f"  {t['name']:>8} {cp:>10,}원 ({chg:+5.1f}%) "
                    f"vol={vol:>12,} → TP({vs_tp:+.1f}%)"
                )

            except Exception as e:
                print(f"  {t['name']}: ERR {e}")

            time.sleep(0.2)

        # ── 5분 주기 TV 실시간 스캔 ──
        if tv_enabled and time.time() - last_tv_scan >= TV_SCAN_INTERVAL:
            try:
                new_tv = _scan_tv_realtime(tv_universe, tv_prev_codes)
                if new_tv:
                    print(f"  [TV] 신규 {len(new_tv)}종목 감지!")
                    for sig in new_tv[:5]:  # 최대 5개 알림
                        msg = _format_tv_alert(sig)
                        print(f"  [TV] {sig.name} {sig.pattern} TV={sig.tv_ratio:.1f}x")
                        send_telegram(msg)
                        tv_prev_codes.add(sig.code)
                else:
                    if check_count <= 2:
                        print(f"  [TV] 스캔 완료 — 신규 시그널 없음")
            except Exception as e:
                print(f"  [TV] 스캔 오류: {e}")
            last_tv_scan = time.time()

        # ── 30분 요약 ──
        if time.time() - last_summary >= 1800:
            try:
                _send_summary(targets, last_prices, open_prices)
            except Exception as e:
                print(f"  [ERR] 요약 전송 실패: {e}")
            last_summary = time.time()

        if not has_alert and check_count % 10 == 0:
            print(f"  [{now:%H:%M:%S}] 체크 #{check_count} 완료 — 이상 없음")

        time.sleep(interval)


def _send_summary(targets, last_prices, open_prices):
    """30분 요약 텔레그램 전송"""
    now = datetime.now()
    lines = [f"📊 추적 요약 [{now:%H:%M}]", ""]

    for t in targets:
        cp = last_prices.get(t["code"], 0)
        if cp == 0:
            continue

        open_p = open_prices.get(t["code"], cp)
        day_chg = (cp / open_p - 1) * 100 if open_p > 0 else 0
        tp = t.get("tp", 0)
        sl = t.get("sl", 0)
        vs_tp = (tp / cp - 1) * 100 if tp > 0 and cp > 0 else 0
        vs_sl = (cp / sl - 1) * 100 if sl > 0 and cp > 0 else 0

        icon = "🟢" if day_chg > 0 else ("🔴" if day_chg < -1 else "⚪")
        lines.append(
            f"{icon} {t['name']} {cp:,}원 ({day_chg:+.1f}%) "
            f"SL({vs_sl:+.1f}%) TP({vs_tp:+.1f}%)"
        )

    if len(lines) > 2:
        msg = "\n".join(lines)
        print(msg)
        send_telegram(msg)


# ─── CLI ──────────────────────────────────────────────

if __name__ == "__main__":
    legacy = "--legacy" in sys.argv
    bounce = "--bounce" in sys.argv

    # --top N 파싱
    top_n = 0
    if "--top" in sys.argv:
        idx = sys.argv.index("--top")
        if idx + 1 < len(sys.argv):
            try:
                top_n = int(sys.argv[idx + 1])
            except ValueError:
                print(f"[WARN] --top 값이 숫자가 아닙니다: {sys.argv[idx + 1]}")
                top_n = 0

    if bounce:
        label = f"바운스 모드 (bounce_candidates.json, TOP {top_n})" if top_n else "바운스 모드 (bounce_candidates.json)"
        print(label)
        targets = load_bounce_targets(top_n)
        if not targets:
            print("바운스 종목 없음! --legacy로 기존 종목 사용")
            targets = LEGACY_PICKS
    elif legacy:
        print("레거시 모드 (기존 하드코딩 종목)")
        targets = LEGACY_PICKS
    else:
        print("전쟁 모드 (recommendation.json 기반)")
        targets = load_war_mode_targets()
        if not targets:
            print("추천 종목 없음! --legacy로 기존 종목 사용")
            targets = LEGACY_PICKS

    print(f"\n감시 종목 {len(targets)}개:")
    for t in targets:
        print(f"  {t['name']}({t['code']}) SL:{t.get('sl',0):,} TP:{t.get('tp',0):,} | {t.get('memo','')}")
    print()

    run_monitor(targets, interval=30)
