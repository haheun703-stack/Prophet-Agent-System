"""
Shared Supabase Client
======================
프로젝트 전역 Supabase 클라이언트 (lazy singleton)
"""

import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_supabase = None
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _ensure_env():
    """환경변수가 없으면 .env 로드"""
    if os.environ.get("SUPABASE_URL"):
        return
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if v:
                    os.environ.setdefault(k, v)


def get_client():
    """Supabase 클라이언트 lazy 초기화 (싱글턴)"""
    global _supabase
    if _supabase is not None:
        return _supabase

    _ensure_env()

    url = os.environ.get("SUPABASE_URL", "")
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
           or os.environ.get("SUPABASE_KEY", ""))

    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정 (.env 확인)")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    logger.info(f"Supabase 연결: {url[:40]}...")
    return _supabase
