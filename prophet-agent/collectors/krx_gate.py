# -*- coding: utf-8 -*-
"""KRX 접근 전역 kill switch (6/22 사장님 지시 — IP 차단 대응). prophet-agent(예언자)용.

단타봇 scalper-agent/data/krx_gate.py와 동일 정책. 루트 .env의 KRX_ENABLED 공유.
default = 차단. KRX_ENABLED=1 일 때만 KRX 실호출(pykrx) 허용.
사장님 6/22: "KRX 비활성화. 새 IP 받으면 1봇만 진행."
"""
import os
from pathlib import Path

# collectors → prophet-agent → 루트(D:\Prophet_Agent_System_예언자)\.env
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
_TRUE = {"1", "true", "yes", "on"}


def krx_enabled() -> bool:
    """KRX 실호출 허용 여부. default False(차단). KRX_ENABLED=1/true/yes/on 일 때만 True."""
    v = os.environ.get("KRX_ENABLED", "")
    if not v and _ENV_PATH.exists():
        try:
            for line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("KRX_ENABLED="):
                    v = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
        except Exception:
            return False
    return str(v).strip().lower() in _TRUE


def krx_block_reason() -> str:
    return ("KRX 비활성화(krx_gate, 6/22 IP 차단 대응) — KRX_ENABLED=1 전까지 차단. 새 IP 후 1봇만.")
