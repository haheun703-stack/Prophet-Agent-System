# -*- coding: utf-8 -*-
"""ETF 주도주 발굴 자동 갱신 — Step B → C-3+4 → C-5 체인.

매주/매월 cron으로 자동 실행. KRX 데이터 → 네이버 etfAnalysis → Supabase 적재까지 한 번에.
실패/성공 모두 텔레그램 알림.

Usage:
    python tools/refresh_etf_leaders.py                  # 풀 체인 (B→C-3+4→C-5)
    python tools/refresh_etf_leaders.py --skip-b         # Step C만 재실행 (이미 B 결과 있을 때)
    python tools/refresh_etf_leaders.py --quiet          # 텔레그램 알림만 (콘솔 최소)

VPS cron 예시 (매주 토요일 자정 KST = 금요일 15:00 UTC):
    0 15 * * 5 cd /home/<user>/Prophet_Agent_System && python scalper-agent/tools/refresh_etf_leaders.py --quiet >> logs/etf_leader.log 2>&1

5/17 일요일 구축. AUDIT_BACKLOG §1 "ETF Step A→B→C 자동 실행 cron 등록" 항목.
"""
import os
import sys
import io
import argparse
import logging
import traceback
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

ROOT = Path(__file__).resolve().parent.parent.parent
SCALPER = ROOT / "scalper-agent"
sys.path.insert(0, str(SCALPER))

# .env 로드 (텔레그램 토큰)
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("RefreshETF")


def send_telegram(msg: str, quiet: bool = False) -> bool:
    """텔레그램 알림 (토큰 없으면 콘솔 출력)."""
    if not TG_TOKEN or not TG_CHAT:
        if not quiet:
            print("[TG] TOKEN/CHAT_ID 미설정 — 콘솔 출력:")
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


def _now_kst_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M") if KST else datetime.now().strftime("%Y-%m-%d %H:%M")


def run_step_b() -> str:
    """Step B 실행 → 최신 base_date 반환."""
    logger.info("[Step B] ETF 패턴 감지 파이프라인 시작...")
    # etf_leader_pipeline.py main() 호출
    from etf_leader_pipeline import main as run_b, find_last_trading_date
    base_date = find_last_trading_date()
    run_b()  # leader_picks_<base_date>.json 생성
    return base_date


def run_step_c34(base_date: str) -> dict:
    """Step C-3+4 실행 → leader_stocks_<date>.json 생성."""
    logger.info(f"[Step C-3+4] TOP 20 ETF 비중 수집 + 종목 점수화 (base_date={base_date})...")
    from tools.collect_etf_holdings import main as run_c34
    return run_c34(base_date)


def run_step_c5(base_date: str, create_table: bool = False) -> bool:
    """Step C-5 실행 → Supabase 적재."""
    logger.info(f"[Step C-5] Supabase scalper_etf_leader_picks 적재 (base_date={base_date})...")
    from tools.upload_etf_leader_picks import upload
    return upload(base_date, create_table=create_table)


def main(skip_b: bool = False, base_date_override: str = None, quiet: bool = False,
         create_table: bool = False):
    started = datetime.now(KST) if KST else datetime.now()
    started_str = _now_kst_str()

    print(f"\n{'='*70}\n  ETF 주도주 자동 갱신 시작 ({started_str} KST)\n{'='*70}\n")

    try:
        # 1) Step B (옵션)
        if base_date_override:
            base_date = base_date_override
            logger.info(f"base_date override: {base_date}")
        elif skip_b:
            # 가장 최근 leader_picks_*.json 찾기
            data_dir = SCALPER / "data_store" / "etf_backtest"
            candidates = sorted(data_dir.glob("leader_picks_*.json"), reverse=True)
            if not candidates:
                raise FileNotFoundError("leader_picks_*.json 없음 — --skip-b 사용 불가")
            base_date = candidates[0].stem.replace("leader_picks_", "")
            logger.info(f"[Step B] 스킵 — 최신 파일 사용: {base_date}")
        else:
            base_date = run_step_b()

        # 2) Step C-3+4
        c34_result = run_step_c34(base_date)
        top_stocks = c34_result.get("top_stocks", [])
        succeeded = c34_result.get("parameters", {}).get("etfs_succeeded", 0)
        total = c34_result.get("parameters", {}).get("top_etfs_used", 0)

        # 3) Step C-5 (Supabase 적재)
        upload_ok = run_step_c5(base_date, create_table=create_table)

        elapsed = (datetime.now(KST) if KST else datetime.now()) - started
        elapsed_sec = round(elapsed.total_seconds(), 1)

        # 4) 성공 텔레그램 알림
        top5_msg = "\n".join(
            f"  {i:>2}. [{s['stock_ticker']}] {s['stock_name'][:18]:<18s} "
            f"{s['leader_score']:>6.2f}점 ({s['etf_count']}ETF)"
            for i, s in enumerate(top_stocks[:5], 1)
        )
        msg = (
            f"✅ ETF 주도주 자동 갱신 완료 ({started_str} KST)\n"
            f"\n"
            f"📊 통계\n"
            f"  • base_date: {base_date}\n"
            f"  • ETF 수집: {succeeded}/{total}\n"
            f"  • TOP 30 적재: {'PASS' if upload_ok else 'FAIL'}\n"
            f"  • 소요: {elapsed_sec}초\n"
            f"\n"
            f"🏆 TOP 5 주도주\n"
            f"{top5_msg}\n"
            f"\n"
            f"다음 morning_recommendation부터 +N점 보너스 자동 반영."
        )
        send_telegram(msg, quiet=quiet)
        print(f"\n  ✅ 완료 — {elapsed_sec}초")
        return 0

    except Exception as e:
        err_trace = traceback.format_exc()
        elapsed = (datetime.now(KST) if KST else datetime.now()) - started
        elapsed_sec = round(elapsed.total_seconds(), 1)
        err_msg = (
            f"❌ ETF 주도주 자동 갱신 실패 ({started_str} KST)\n"
            f"\n"
            f"오류: {type(e).__name__}: {str(e)[:300]}\n"
            f"소요: {elapsed_sec}초\n"
            f"\n"
            f"VPS 로그 확인 필요. 수동 재실행 명령:\n"
            f"  python scalper-agent/tools/refresh_etf_leaders.py"
        )
        send_telegram(err_msg, quiet=quiet)
        logger.error(f"실패: {err_trace}")
        print(err_msg)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF 주도주 자동 갱신 (Step B→C 체인)")
    parser.add_argument("--skip-b", action="store_true",
                        help="Step B 스킵 (최신 leader_picks 재사용)")
    parser.add_argument("--base-date", type=str, default=None,
                        help="base_date 지정 (YYYYMMDD, 디버그용)")
    parser.add_argument("--quiet", action="store_true",
                        help="콘솔 출력 최소화 (cron 자동화용)")
    parser.add_argument("--create-table", action="store_true",
                        help="scalper_etf_leader_picks 테이블 1회성 자동 생성")
    args = parser.parse_args()

    sys.exit(main(
        skip_b=args.skip_b,
        base_date_override=args.base_date,
        quiet=args.quiet,
        create_table=args.create_table,
    ))
