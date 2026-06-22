# -*- coding: utf-8 -*-
"""KRX 접근 전역 kill switch (6/22 사장님 지시 — IP 차단 대응).

사장님 6/22: "KRX IP 주소 락 걸렸다. 너희가 무자비하게 너무 들어가서 못 들어간다.
  KRX는 비활성화 해놓고, 내가 IP 새로 받으면 그때 다시 1명의 봇만 진행."

배경: 여러 봇(단타봇·prophet-agent 예언자·퀀트봇)이 KRX를 과다 크롤링 → 사무실/노트북 IP 차단
  (6/19에도 124.52.138.181 차단 기록). KRX 실호출은 모두 이 게이트를 통과해야만 나간다.

정책:
- default = 차단(False). KRX 실호출(pykrx 웹스크래핑·KRX nationality 크롤러·KRX OpenAPI)은
  krx_enabled()가 True일 때만 허용.
- .env(루트) KRX_ENABLED=1 또는 환경변수 KRX_ENABLED=1 일 때만 활성.
- ★ 새 IP 받은 뒤 '1봇만' 켤 때, 그 1봇 환경에서만 KRX_ENABLED=1 설정한다.
  (단타봇·prophet-agent는 루트 .env를 공유하므로, 1봇 일원화 시 봇별 env 분리 필요 — 사장님 결정.)
"""
import os
from pathlib import Path

# 루트 .env (D:\Prophet_Agent_System_예언자\.env) — scalper-agent/data 기준 2단계 상위
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
    """차단 사유 문자열 (로그용)."""
    return ("KRX 비활성화(krx_gate, 6/22 IP 차단 대응) — KRX_ENABLED=1 설정 전까지 차단. "
            "새 IP 후 1봇만 활성화.")
