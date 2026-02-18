"""
Shared Config Loader
=====================
YAML 설정 파일 로더 + .env 환경변수 통합
"""

from pathlib import Path
import os
import yaml


def load_env(env_path: str = None) -> dict:
    """.env 파일을 읽어 os.environ에 세팅하고 dict로 반환"""
    if env_path is None:
        # 프로젝트 루트의 .env 탐색
        candidates = [
            Path(__file__).resolve().parent.parent / ".env",
            Path.cwd() / ".env",
            Path.cwd().parent / ".env",
        ]
        for c in candidates:
            if c.exists():
                env_path = str(c)
                break

    env_vars = {}
    if env_path and Path(env_path).exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if value:
                        os.environ[key] = value
                        env_vars[key] = value
    return env_vars


def load_config(path: str = "config.yaml") -> dict:
    """YAML 설정 파일 로드 + .env 환경변수로 API 키 주입"""
    filepath = Path(path)
    if not filepath.exists():
        raise FileNotFoundError(f"설정 파일 없음: {path}")
    with open(filepath, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # .env 로드
    load_env()

    # .env → config 매핑
    env_map = {
        'TELEGRAM_BOT_TOKEN': ('api_keys', 'telegram_bot_token'),
        'TELEGRAM_CHAT_ID': ('api_keys', 'telegram_chat_id'),
        'DART_API_KEY': ('api_keys', 'dart_api_key'),
        'ANTHROPIC_API_KEY': ('api_keys', 'anthropic_api_key'),
        'XAI_API_KEY': ('api_keys', 'xai_api_key'),
        'OPENAI_API_KEY': ('api_keys', 'openai_api_key'),
        'FINNHUB_API_KEY': ('api_keys', 'finnhub_api_key'),
        'KIS_APP_KEY': ('api_keys', 'kis_app_key'),
        'KIS_APP_SECRET': ('api_keys', 'kis_app_secret'),
        'KIS_ACC_NO': ('api_keys', 'kis_acc_no'),
        'MODEL': ('api_keys', 'kis_model'),
    }

    for env_key, (section, field) in env_map.items():
        val = os.environ.get(env_key, '')
        if val and section in config:
            config[section][field] = val

    return config
