# -*- coding: utf-8 -*-
"""nationality 수집량 건강성 모니터링 + 텔레그램 경보
=====================================================
매일 18:30 VPS sync 후 자동 실행됨 (BodyHunter_VPSSync 작업 스케줄러).

검사 항목:
1. 최근 7거래일 신규 스냅샷 수 추세 (`<code>_YYYYMMDD.csv`)
2. 누적형 파일 (`nationality_<code>.csv`) 최신 mtime 신선도
3. 어제(T-1) 대비 50% 이하 또는 100건 이하면 경보

5/14 KIS 계좌 교환 같은 일회성 사고 즉시 발견 목적.

Usage:
    python tools/nationality_health_check.py
    python tools/nationality_health_check.py --quiet   # 자동화용
    python tools/nationality_health_check.py --force-alert  # 강제 텔레그램 발송 (테스트)
"""

import os
import sys
import io
import argparse
import re
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path

# Windows cp949 출력 방지
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data_store" / "nationality"

# .env 로드 (텔레그램 토큰)
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR.parent / ".env")
except Exception:
    pass

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

# 임계값
ALERT_MIN_SNAPSHOTS = 100      # 절대 임계 (100건 이하면 CRITICAL)
ALERT_DROP_RATIO = 0.5         # 전일 대비 50% 이하면 WARN
ALERT_AVG_RATIO = 0.6          # 7일 평균 대비 60% 이하면 WARN (5/14 사고급 감지)
STALE_HOURS = 30               # 누적형 mtime 30시간 이상 묵으면 WARN (T-1 허용)
TREND_DAYS = 7                 # 최근 N 거래일 추세 + 평균 산정


def send_telegram(msg: str, quiet: bool = False) -> bool:
    """텔레그램 메시지 전송. 토큰 없으면 콘솔 출력만."""
    if not TG_TOKEN or not TG_CHAT:
        if not quiet:
            print("[TG] TOKEN/CHAT_ID 미설정 — 콘솔 출력만:")
            print(msg)
        return False
    try:
        import requests
        chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
        for chunk in chunks:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT, "text": chunk},
                timeout=15,
            )
            if r.status_code != 200:
                if not quiet:
                    print(f"[TG] HTTP {r.status_code}: {r.text[:200]}")
                return False
        return True
    except Exception as e:
        if not quiet:
            print(f"[TG] 전송 실패: {e}")
        return False


def scan_snapshots() -> dict:
    """nationality 디렉토리 스캔 → 일자별 스냅샷 수 집계."""
    if not DATA_DIR.exists():
        return {"error": f"디렉토리 없음: {DATA_DIR}", "by_date": {}, "total_cum": 0}

    # 패턴: <code>_YYYYMMDD.csv (스냅샷) vs nationality_<code>.csv (누적형)
    snap_pattern = re.compile(r"^(\d{6}|[A-Z0-9]{6,7})_(\d{8})\.csv$")
    by_date = defaultdict(int)
    cum_files = []

    for f in DATA_DIR.iterdir():
        if not f.is_file() or f.suffix != ".csv":
            continue
        name = f.name
        m = snap_pattern.match(name)
        if m:
            yyyymmdd = m.group(2)
            by_date[yyyymmdd] += 1
        elif name.startswith("nationality_"):
            cum_files.append(f)

    # 누적형 파일 mtime 가장 최근
    cum_latest_mtime = max((f.stat().st_mtime for f in cum_files), default=0)

    return {
        "by_date": dict(by_date),
        "total_cum_files": len(cum_files),
        "cum_latest_mtime": cum_latest_mtime,
        "cum_latest_ts": datetime.fromtimestamp(cum_latest_mtime).isoformat() if cum_latest_mtime else "",
    }


def _is_trading_day(d: date) -> bool:
    """간단 거래일 판정 (KST, 주말 제외, 공휴일은 trading_calendar 모듈 우선)."""
    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from utils.trading_calendar import is_trading_day as _is_td
        return _is_td(d)
    except Exception:
        return d.weekday() < 5


def evaluate(snap_data: dict) -> dict:
    """수집 데이터 기반 건강성 평가.

    Returns:
        {
            "status": "OK"|"WARN"|"CRITICAL",
            "alerts": [str, ...],
            "trend": [(date, count), ...]  # 최근 7거래일
        }
    """
    if "error" in snap_data:
        return {"status": "CRITICAL", "alerts": [snap_data["error"]], "trend": []}

    by_date = snap_data["by_date"]
    alerts = []

    # 최근 N 거래일 추세 (today 제외, T-1부터)
    today = date.today()
    trend = []
    d = today - timedelta(days=1)
    while len(trend) < TREND_DAYS:
        if _is_trading_day(d):
            ds = d.strftime("%Y%m%d")
            cnt = by_date.get(ds, 0)
            trend.append((d.strftime("%Y-%m-%d"), cnt))
        d -= timedelta(days=1)
        # 무한루프 방지
        if (today - d).days > 30:
            break

    # 임계값 검사: KRX T-1 지연 고려
    # - trend[0] = T-1(어제) 거래일 — KRX는 18:30 시점에 이 데이터가 들어와야 함
    # - 단, trend[0]가 0건이면 KRX 응답 미공개 가능성 → trend[1]을 기준 평가로 fallback
    if trend:
        latest_date, latest_cnt = trend[0]
        prev_date, prev_cnt = (trend[1] if len(trend) >= 2 else (None, 0))

        if latest_cnt == 0 and prev_cnt > 0:
            # T-1 미수신 = KRX T-1 지연 또는 16:30 D 미실행
            # trend[1]가 정상이면 "수집 대기 중" 정보성 메시지만
            alerts.append(
                f"ℹ️ {latest_date} 미수신 (KRX T-1 지연 가능 / 다음 거래일 확인)"
            )
            # 평가 기준을 trend[1]로 이동
            latest_date, latest_cnt = prev_date, prev_cnt
            prev_date, prev_cnt = (trend[2] if len(trend) >= 3 else (None, 0))

        # 절대 임계 (기준일 평가)
        if latest_cnt > 0 and latest_cnt < ALERT_MIN_SNAPSHOTS:
            alerts.append(
                f"❌ {latest_date} 스냅샷 {latest_cnt}건 — 임계값 {ALERT_MIN_SNAPSHOTS}건 미달"
            )

        # 전일 대비 drop ratio (기준일 vs 전 거래일)
        if prev_cnt > 0 and 0 < latest_cnt < prev_cnt * ALERT_DROP_RATIO:
            drop_pct = (1 - latest_cnt / prev_cnt) * 100
            alerts.append(
                f"⚠️ {latest_date} {latest_cnt}건 (전일 {prev_date} {prev_cnt}건 대비 -{drop_pct:.0f}%)"
            )

        # 7일 평균 대비 (5/14 사고급 점진적 이상 감지)
        # 기준일(latest)를 제외한 trend 전체로 평균 계산 → 단발성 사고도 잡힘
        other_counts = [c for d, c in trend if d != latest_date and c > 0]
        if other_counts and latest_cnt > 0:
            avg = sum(other_counts) / len(other_counts)
            if latest_cnt < avg * ALERT_AVG_RATIO:
                drop_pct = (1 - latest_cnt / avg) * 100
                alerts.append(
                    f"⚠️ {latest_date} {latest_cnt}건 (7일 평균 {avg:.0f}건 대비 -{drop_pct:.0f}%)"
                )

    # 누적형 신선도 검사
    if snap_data["cum_latest_mtime"]:
        age_hours = (datetime.now().timestamp() - snap_data["cum_latest_mtime"]) / 3600
        if age_hours > STALE_HOURS:
            alerts.append(
                f"⚠️ 누적형 파일 {age_hours:.0f}시간 미갱신 (최신: {snap_data['cum_latest_ts'][:16]})"
            )

    if not alerts:
        status = "OK"
    elif any("❌" in a for a in alerts):
        status = "CRITICAL"
    else:
        status = "WARN"

    return {"status": status, "alerts": alerts, "trend": trend}


def format_report(snap_data: dict, eval_data: dict) -> str:
    """텔레그램용 리포트 포맷."""
    icon = {"OK": "✅", "WARN": "⚠️", "CRITICAL": "❌"}.get(eval_data["status"], "❓")
    lines = [f"{icon} nationality 건강성 ({datetime.now().strftime('%m/%d %H:%M')})"]
    lines.append("━━━━━━━━━━━━━━━━━━━")

    # 최근 7거래일 추세
    lines.append("📊 최근 7거래일 신규 스냅샷:")
    for d, cnt in eval_data["trend"]:
        bar = "█" * min(int(cnt / 50), 20)
        lines.append(f"  {d}: {cnt:>4}건 {bar}")

    # 누적형 파일
    if snap_data.get("cum_latest_ts"):
        lines.append("")
        lines.append(f"📁 누적형: {snap_data['total_cum_files']}개")
        lines.append(f"   최신: {snap_data['cum_latest_ts'][:16]}")

    # 경보
    if eval_data["alerts"]:
        lines.append("")
        lines.append("🚨 경보:")
        for a in eval_data["alerts"]:
            lines.append(f"  {a}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="nationality 건강성 모니터링")
    parser.add_argument("--quiet", action="store_true", help="간략 출력 (자동화용)")
    parser.add_argument("--force-alert", action="store_true",
                        help="OK 상태도 강제로 텔레그램 발송 (테스트)")
    args = parser.parse_args()

    snap_data = scan_snapshots()
    eval_data = evaluate(snap_data)
    report = format_report(snap_data, eval_data)

    if args.quiet:
        # 1줄 요약 (스케줄러용)
        ts = datetime.now().strftime("%m/%d %H:%M")
        n_alert = len(eval_data["alerts"])
        latest = (eval_data["trend"][0] if eval_data["trend"] else ("?", 0))
        print(f"[{ts}] nationality_health: {eval_data['status']} "
              f"latest={latest[0]} {latest[1]}건 alerts={n_alert}")
    else:
        print(report)

    # 텔레그램: WARN/CRITICAL 또는 --force-alert
    if eval_data["status"] != "OK" or args.force_alert:
        send_telegram(report, quiet=args.quiet)

    # exit code: CRITICAL=2, WARN=1, OK=0
    sys.exit({"OK": 0, "WARN": 1, "CRITICAL": 2}[eval_data["status"]])


if __name__ == "__main__":
    main()
