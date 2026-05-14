# -*- coding: utf-8 -*-
"""정보봇(JGIS) 공유 데이터 경로 결정 헬퍼.

Why: 5/14 발견 — 5개 파일이 'D:/shared-bot-data/...' Windows 경로 하드코딩
되어 VPS(Linux)에서 정보봇 데이터를 못 읽음. df0175b 커밋 "정보봇 수급 통합
인텔리전스 3종 연동"이 사실상 로컬 전용으로 동작.

우선순위:
1. SHARED_BOT_DATA_DIR 환경변수 (명시적 오버라이드)
2. Windows → D:/shared-bot-data
3. Linux/Mac → /home/ubuntu/shared-bot-data (VPS 표준)
"""
import os
import platform
from pathlib import Path


def shared_bot_data_dir() -> Path:
    env = os.getenv("SHARED_BOT_DATA_DIR")
    if env:
        return Path(env)
    if platform.system() == "Windows":
        return Path("D:/shared-bot-data")
    return Path("/home/ubuntu/shared-bot-data")


def jgis_intel_path() -> Path:
    return shared_bot_data_dir() / "jgis_to_quant" / "daily_intelligence.json"


def jgis_breaking_path() -> Path:
    return shared_bot_data_dir() / "jgis_to_quant" / "breaking_alerts.json"
