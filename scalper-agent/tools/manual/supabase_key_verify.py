# -*- coding: utf-8 -*-
"""단타봇 Supabase 키 검증 — read-only 1회. 키 값은 절대 출력하지 않는다.

용도: 키 교체 前 baseline / 교체 後 동일 명령으로 대조.
안전: select 1행만(사용한도 초과 상태라 최소 쿼리)·쓰기 0·삭제 0.
"""
import os, sys, datetime
sys.path.insert(0, os.getcwd())

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
