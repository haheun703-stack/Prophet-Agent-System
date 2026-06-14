"""야간 통합 관측 파이프라인 (6/10 신설) — 매일 보는 3단계 자동화.

배경:
  - shadow가 15:50 자동적재인데 일봉 fill은 저녁(수동)이라 forward가 매일 1주기 지연됨.
  - 해결: fill을 먼저 돌리고 그 뒤에 shadow/paper를 둬서 forward 당일 충전.

순서(★fill 후 shadow/paper = forward 지연 해결★):
  ① fill 4-way → ② step6 sync → ③ shadow(build+forward) → ④ paper 3-Type
  → ⑤ paper forward 충전 → ⑥ 수급 4종 → ⑦ 11주체 → ⑧ 국적별 → ⑨ F1
  - 관측 3단계(shadow·paper build·paper forward)는 일봉만 필요 → fill 직후 실행(수급보다 앞).
  - ⑤(6/15 신설): paper는 ④에서 코호트 생성만 → forward는 과거 코호트 대상이라 별도 단계.
    shadow가 ③에서 build+forward를 묶는 것과 동일 취지(H-3 dead code 해소).

안전:
  - 봇 OFF·매수 무접촉·실주문0·SAJANG 무변경(데이터/관측 전용, 검증된 진입점만 호출).
  - 각 단계 독립 try/except → 한 단계 실패해도 다음 진행. 어떤 예외도 exit 0.
  - 휴장일 skip. shadow/paper는 자체 멱등(기록된 날 skip).
  - 노트북이 단타봇 데이터 메인(VPS는 구버전). OS task 18:00(StartWhenAvailable)로 호출.
"""
import sys
import subprocess
import time
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from data.trading_calendar import is_trading_day  # noqa: E402

PY = sys.executable


def _log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_step(name, cmd, timeout):
    _log(f"━━ {name} 시작 ━━")
    t0 = time.time()
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), timeout=timeout,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        el = time.time() - t0
        tail = (r.stdout or "").strip().splitlines()
        tail_s = tail[-1] if tail else ""
        if r.returncode == 0:
            _log(f"✅ {name} 완료 ({el:.0f}s) | {tail_s[:160]}")
            return True
        _log(f"⚠ {name} exit={r.returncode} ({el:.0f}s) | {(r.stderr or '')[-180:].strip()}")
        return False
    except subprocess.TimeoutExpired:
        _log(f"⚠ {name} TIMEOUT ({timeout}s)")
        return False
    except Exception as e:  # noqa: BLE001
        _log(f"⚠ {name} ERROR: {str(e)[:150]}")
        return False


def main():
    today = date.today()
    _log(f"========== 야간 통합 관측 파이프라인 {today} ==========")
    if not is_trading_day(today):
        _log("휴장일 — 전체 skip (exit 0)")
        return 0
    tstr = today.isoformat()

    # (이름, 명령, timeout초) — 관측 3단계 먼저, 수급 뒤
    steps = [
        ("① fill 4-way",
         [PY, "tools/fill_daily_kis_incremental.py", "--parallel", "4"], 1800),
        ("② step6 sync",
         [PY, "-c", "import collect_all; collect_all.step6_sync_stock_data_daily()"], 600),
        ("③ shadow build+forward",
         [PY, "tools/run_sector_reversal_shadow_daily.py"], 600),
        ("④ paper 3-Type",
         [PY, "tools/paper_3type_daily_run_6_6.py", "--asof", tstr, "--scan-events"], 900),
        ("⑤ paper forward 충전",   # 6/15 H-3 해소 — 과거 코호트 forward_d1/d3/d5·MFE/MAE 멱등 충전
         [PY, "tools/run_paper_3type_forward_daily.py"], 300),
        ("⑥ 수급 4종",
         [PY, "-c", "import collect_all as C; C.step2_supply_demand(C.get_universe_codes())"], 3600),
        ("⑦ 11주체",
         [PY, "-c", "from data.market_investor_collector import collect_market_investor as f; r=f(days=3); print('ok' if r else 'EMPTY')"], 600),
        ("⑧ 국적별",
         [PY, "-c", "import collect_all; collect_all.step3_nationality()"], 600),
        ("⑨ F1 forward+preflight",
         [PY, "tools/run_f1_forward_preflight.py"], 300),
    ]

    results = {}
    t_all = time.time()
    for name, cmd, to in steps:
        try:
            results[name] = run_step(name, cmd, to)
        except Exception as e:  # noqa: BLE001 — 단계 호출 자체 예외도 격리
            _log(f"⚠ {name} 호출 예외: {str(e)[:120]}")
            results[name] = False

    _log(f"========== 완료 ({time.time()-t_all:.0f}s) ==========")
    ok_n = sum(1 for v in results.values() if v)
    _log(f"요약: {ok_n}/{len(results)} 성공")
    for n, ok in results.items():
        _log(f"   {'✅' if ok else '⚠'} {n}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 — 최상위 가드: 어떤 예외도 exit 0
        _log(f"최상위 예외(무시): {str(e)[:150]}")
        sys.exit(0)
