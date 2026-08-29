# -*- coding: utf-8 -*-
"""단타봇 Supabase 키 검증 — read-only 1회. 키 값은 절대 출력하지 않는다.

용도: 키 교체 前 baseline / 교체 後 동일 명령으로 대조.
안전: select 1행만(사용한도 초과 상태라 최소 쿼리)·쓰기 0·삭제 0.
"""
import os, sys, datetime
from pathlib import Path

# ★8/29 fix — cwd 의존이었다. `cd scalper-agent` 에서 돌리면 상위의 shared/ 를 못 찾아
#   ModuleNotFoundError로 죽는다(8/29 실측). 실행 위치와 무관하게 repo 루트를 잡는다.
#   tools/manual/ 의 다른 도구들은 처음부터 __file__ 기준이었는데 이것만 빠져 있었다.
#   ★경로는 세지 말고 찍어서 확인했다 — parents[3]이 repo 루트(shared/ 보유) 실측.
_ROOT = Path(__file__).resolve().parents[3]              # …/tools/manual → repo 루트
for _p in (str(_ROOT), str(_ROOT / "scalper-agent")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.supabase_client import get_client, _ensure_env
_ensure_env()

url = os.environ.get("SUPABASE_URL", "")
key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_KEY", ""))
print(f"URL      : {url[:34]}…")
print(f"KEY 타입 : {'sb_secret_(service)' if key.startswith('sb_secret_') else ('sb_publishable_(공개키·RLS 막힘)' if key.startswith('sb_publishable_') else '구형/기타')}")
print(f"KEY 지문 : {key[:13]}…{key[-4:]}  (len={len(key)})")   # 앞13+뒤4만 — 대조용

cli = get_client()
if cli is None:
    print("❌ 클라이언트 생성 실패"); raise SystemExit(1)

# 단타봇이 실제로 적재하는 표 중 하나로 read 1회
TABLES = ["dashboard_swing", "intelligence_pension_scan", "sector_investor_flow", "signals"]
ok = 0
for t in TABLES:
    try:
        r = cli.table(t).select("*").limit(1).execute()
        n = len(r.data) if getattr(r, "data", None) is not None else 0
        print(f"  ✅ {t:28s} read OK (rows={n})")
        ok += 1
    except Exception as e:                                   # noqa: BLE001
        msg = str(e)[:110].replace(key, "[REDACTED]") if key else str(e)[:110]
        print(f"  ❌ {t:28s} {msg}")

print(f"\n결과: {ok}/{len(TABLES)} 표 read 성공 · "
      f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST")
raise SystemExit(0 if ok else 1)
