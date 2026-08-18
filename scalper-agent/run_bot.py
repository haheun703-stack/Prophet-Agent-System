# -*- coding: utf-8 -*-
"""
Body Hunter v4 텔레그램 봇 실행 (자동 재시작)
==============================================
python run_bot.py          # 일반 실행 (크래시 시 자동 재시작)
python run_bot.py --once   # 1회 실행 (재시작 없음)
"""

import sys
import os
import io
import time
import logging
import traceback
from pathlib import Path
from datetime import datetime

# Windows UTF-8
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    except Exception:
        pass

# 프로젝트 경로
sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml

# 자동 재시작 설정
MAX_RESTARTS = 50          # 최대 재시작 횟수 (하루)
RESTART_DELAY_SEC = 30     # 재시작 대기 시간 (초)
CRASH_LOG = Path(__file__).parent / "logs" / "crash.log"
PID_FILE = Path(__file__).parent / "logs" / "bot.pid"


def _harden_third_party_logging() -> None:
    """Keep credential-bearing request URLs out of application logs."""
    for logger_name in ("httpx", "httpcore"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def _load_runtime_environment() -> None:
    """Load deployment credentials only when the executable actually starts."""
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _print_startup_safety_status() -> None:
    """Report safety state without emitting account or chat identifiers."""
    from bot.trade_runtime_config import is_paper_only

    automatic_mode = "PAPER_ONLY (실주문 차단)" if is_paper_only() else "LIVE 설정"
    print("\n  KIS 계좌: 설정됨")
    print("  Telegram Chat: 설정됨")
    print(f"  자동주문: {automatic_mode}")
    print("  수동주문: 사용자 명령 시 실주문 가능 (자동가드와 별도)")


def _check_already_running() -> bool:
    """PID 락 파일로 중복 실행 방지. 이미 실행 중이면 True."""
    import subprocess
    PID_FILE.parent.mkdir(exist_ok=True)
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text(encoding="utf-8").strip())
            # tasklist로 해당 PID가 python 프로세스인지 확인
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {old_pid}", "/FO", "CSV", "/NH"],
                capture_output=True, text=True, timeout=5
            )
            if "python" in result.stdout.lower():
                return True  # 이미 실행 중
        except Exception:
            pass
    # 현재 PID 기록
    PID_FILE.write_text(str(os.getpid()))
    return False


def setup_logging():
    from logging.handlers import TimedRotatingFileHandler

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # 기존 핸들러 제거 (재시작 시 중복 방지)
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)

    # TimedRotatingFileHandler: 매일 자동 로테이션, 30일 보관
    file_handler = TimedRotatingFileHandler(
        log_dir / "bot.log",
        when="midnight",
        interval=1,
        backupCount=30,      # 30일치 보관 후 자동 삭제
        encoding="utf-8",
    )
    file_handler.suffix = "%Y%m%d"

    # 프로덕션: 로깅 핸들러 내부 에러(--- Logging error ---) 억제
    # pykrx 등 외부 라이브러리의 response.json() 실패 시 stderr 노이즈 방지
    logging.raiseExceptions = False

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            file_handler,
        ],
    )
    _harden_third_party_logging()


def load_config():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def verify_kis():
    """KIS API 연결 검증"""
    from bot.kis_trader import KISTrader

    trader = KISTrader()
    print("\n  KIS API 연결 테스트...")

    # 1. 잔고 조회
    bal = trader.fetch_balance()
    if bal.get("success"):
        print(f"  ✅ 잔고 조회 성공: 현금 {bal['cash']:,}원 | 총평가 {bal['total_eval']:,}원")
        print(f"     보유 종목: {len(bal['positions'])}개")
    else:
        print(f"  ❌ 잔고 조회 실패: {bal.get('message')}")
        return False

    # 2. 현재가 조회
    price = trader.fetch_price("005930")
    if price.get("success"):
        print(f"  ✅ 삼성전자 현재가: {price['current_price']:,}원 ({price['change_rate']:+.2f}%)")
    else:
        print(f"  ❌ 현재가 조회 실패: {price.get('message')}")
        return False

    print("  ✅ KIS API 조회 연결 확인\n")
    return True


def _run_bot_once():
    """봇 1회 실행 (크래시 시 예외 전파)"""
    setup_logging()
    logger = logging.getLogger("BotMain")

    print("=" * 50)
    print("  Body Hunter v4 텔레그램 봇")
    print("  동적 목표가 + KIS 조회 연결")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # 환경변수 확인
    required = [
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
        "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACC_NO",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"\n  환경변수 누락: {', '.join(missing)}")
        print("  .env 파일을 확인하세요")
        sys.exit(1)

    _print_startup_safety_status()

    # KIS API 연결 테스트
    if not verify_kis():
        print("  KIS API 연결 실패 - 조회 기능만 사용 가능")

    # config 로드
    config = load_config()

    # 봇 시작
    from bot.telegram_bot import BodyHunterBot

    bot = BodyHunterBot(config)
    logger.info("텔레그램 봇 시작")
    print("  텔레그램 봇 시작됨 - Ctrl+C로 종료")
    print("  텔레그램에서 '도움말' 입력으로 명령어 확인\n")

    bot.run()  # 예외는 상위로 전파


def main():
    """자동 재시작 래퍼"""
    _load_runtime_environment()

    # 중복 실행 방지
    if _check_already_running():
        print("  봇이 이미 실행 중입니다. 중복 시작 차단.")
        sys.exit(0)

    once_mode = "--once" in sys.argv

    if once_mode:
        try:
            _run_bot_once()
        except KeyboardInterrupt:
            print("\n  봇 종료됨")
        except Exception as e:
            print(f"\n  봇 오류: {e}")
            sys.exit(1)
        return

    # ═══ 자동 재시작 모드 ═══
    restart_count = 0

    while restart_count < MAX_RESTARTS:
        try:
            _run_bot_once()
            # 정상 종료 (run_polling이 끝남) → 재시작 필요 없음
            break

        except KeyboardInterrupt:
            print("\n  봇 수동 종료됨")
            break

        except Exception as e:
            restart_count += 1
            tb = traceback.format_exc()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 크래시 로그 기록
            crash_msg = f"[{now}] 크래시 #{restart_count}: {e}\n{tb}\n{'='*60}\n"
            try:
                CRASH_LOG.parent.mkdir(exist_ok=True)
                with open(CRASH_LOG, "a", encoding="utf-8") as f:
                    f.write(crash_msg)
            except Exception:
                pass

            print(f"\n  봇 크래시 #{restart_count}/{MAX_RESTARTS}: {e}")
            print(f"  {RESTART_DELAY_SEC}초 후 자동 재시작...")

            time.sleep(RESTART_DELAY_SEC)
            print(f"  재시작 중...\n")

    if restart_count >= MAX_RESTARTS:
        print(f"\n  최대 재시작 횟수({MAX_RESTARTS}) 초과 - 봇 완전 종료")
        sys.exit(1)


if __name__ == "__main__":
    main()
