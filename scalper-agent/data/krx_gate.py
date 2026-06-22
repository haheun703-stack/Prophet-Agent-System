# -*- coding: utf-8 -*-
"""KRX 접근 전역 kill switch (6/22 사장님 지시 — IP 차단 대응).

사장님 6/22: "KRX IP 주소 락 걸렸다. 너희가 무자비하게 너무 들어가서 못 들어간다.
  KRX는 비활성화 해놓고, 내가 IP 새로 받으면 그때 다시 1명의 봇만 진행."

배경: 여러 봇(단타봇·prophet-agent 예언자·퀀트봇)이 KRX를 과다 호출 → KRX 접근 차단.
  KRX 실호출은 모두 이 게이트를 통과해야만 나간다.

★ 원인 정정(6/22 정보봇·퀀트봇 진단): 처음엔 IP 차단으로 인지했으나(6/19 124.52.138.181),
  실제 원인은 **KRX 계정잠금(CD007)** — IP 무관·계정 단위. 같은 KRX 계정으로 로그인하는
  모든 봇/서버가 잠금에 기여한다. → 이 게이트(KRX 호출 0)는 원인이 IP든 계정잠금이든 더더욱
  유효하다. ('새 IP 받으면'은 초기 전제. 계정잠금이면 KRX 계정 해제가 핵심이고, 그 뒤에도
  KRX는 퀀트봇 1봇만 단일 계정 로그인해야 재잠금을 피한다.)

정책:
- default = 차단(False). KRX 실호출(pykrx 웹스크래핑·KRX nationality 크롤러·KRX OpenAPI)은
  krx_enabled()가 True일 때만 허용.
- .env(루트) KRX_ENABLED=1 또는 환경변수 KRX_ENABLED=1 일 때만 활성.
- ★★ 사장님 6/22 확정: KRX는 **퀀트봇(D:\\sub-agent-project_퀀트봇)이 1봇 전담**한다. ★★
  단타봇·prophet-agent(예언자)는 KRX_ENABLED **영구 OFF** — 새 IP 받아도 여기선 KRX 켜지 말 것.
  단타봇은 퀀트봇의 KRX 산출물(quant_investor_extra.json 등)을 sync로 받아 쓴다(KRX 직접 호출 X).
  → 즉 이 게이트는 단타봇/예언자에선 사실상 영구 차단 상태가 정상. KRX_ENABLED=1 설정 금지.
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
