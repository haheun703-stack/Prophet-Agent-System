#!/bin/bash
# ============================================
# Body Hunter v5 — VPS 초기 셋업 스크립트
# Ubuntu 22.04+ 기준
# ============================================
# 사용법: bash deploy/setup_vps.sh

set -e

echo "===== Body Hunter VPS Setup ====="

# 1. 시스템 패키지
echo "[1/6] 시스템 패키지 설치..."
sudo apt update && sudo apt install -y python3.11 python3.11-venv python3-pip git

# 2. Python 가상환경
echo "[2/6] Python venv 생성..."
cd /home/ubuntu/Prophet-Agent-System/scalper-agent
python3.11 -m venv venv
source venv/bin/activate

# 3. 의존성 설치
echo "[3/6] pip 패키지 설치..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. .env 확인
echo "[4/6] .env 확인..."
if [ ! -f /home/ubuntu/Prophet-Agent-System/.env ]; then
    echo "!!! .env 파일이 없습니다!"
    echo "    cp deploy/env.template /home/ubuntu/Prophet-Agent-System/.env"
    echo "    nano /home/ubuntu/Prophet-Agent-System/.env  # 값 채우기"
    exit 1
fi
echo "  .env 존재 확인 OK"

# 5. data_store 디렉토리 생성
echo "[5/6] data_store 디렉토리 생성..."
mkdir -p data_store/learning
mkdir -p data_store/csv

# 6. systemd 서비스 등록
echo "[6/6] systemd 서비스 등록..."
sudo cp deploy/bodyhunter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable bodyhunter
sudo systemctl start bodyhunter

echo ""
echo "===== 셋업 완료! ====="
echo ""
echo "확인 명령어:"
echo "  sudo systemctl status bodyhunter    # 서비스 상태"
echo "  sudo journalctl -u bodyhunter -f    # 실시간 로그"
echo "  sudo systemctl restart bodyhunter   # 재시작"
echo ""
echo "KIS 토큰 갱신 확인:"
echo "  봇 시작 후 텔레그램에서 /상태 입력"
