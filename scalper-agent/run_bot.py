# -*- coding: utf-8 -*-
"""
Body Hunter v3 텔레그램 봇 실행
================================
python run_bot.py
"""

import sys
import os
import io
import logging
from pathlib import Path
from datetime import datetime

# Windows UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# 프로젝트 경로
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import yaml


def setup_logging():
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / f"bot_{today}.log", encoding="utf-8"),
        ],
    )


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

    print("  ✅ KIS API 실매매 준비 완료\n")
    return True


def main():
    setup_logging()
    logger = logging.getLogger("BotMain")

    print("=" * 50)
    print("  🔮 Body Hunter v3 텔레그램 봇")
    print("  5D 디스크법 + KIS 실매매")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # 환경변수 확인
    required = [
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
        "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACC_NO",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"\n  ❌ 환경변수 누락: {', '.join(missing)}")
        print("  .env 파일을 확인하세요")
        sys.exit(1)

    print(f"\n  KIS 계좌: {os.getenv('KIS_ACC_NO')}")
    print(f"  Telegram Chat: {os.getenv('TELEGRAM_CHAT_ID')}")

    # KIS API 연결 테스트
    if not verify_kis():
        print("  ⚠️ KIS API 연결 실패 — 조회 기능만 사용 가능")

    # config 로드
    config = load_config()

    # 봇 시작
    from bot.telegram_bot import BodyHunterBot

    bot = BodyHunterBot(config)
    logger.info("텔레그램 봇 시작")
    print("  📡 텔레그램 봇 시작됨 — Ctrl+C로 종료")
    print("  텔레그램에서 '도움말' 입력으로 명령어 확인\n")

    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n  봇 종료됨")
    except Exception as e:
        logger.error(f"봇 치명적 오류: {e}")
        print(f"\n  ❌ 봇 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
