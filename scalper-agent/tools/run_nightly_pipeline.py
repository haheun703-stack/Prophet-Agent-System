"""야간 통합 관측 파이프라인 (6/10 신설) — 매일 보는 3단계 자동화.

배경:
  - shadow가 15:50 자동적재인데 일봉 fill은 저녁(수동)이라 forward가 매일 1주기 지연됨.
  - 해결: fill을 먼저 돌리고 그 뒤에 shadow/paper를 둬서 forward 당일 충전.

순서(★fill 후 shadow/paper = forward 지연 해결★):
  ① fill 4-way → ② step6 sync → ②-2 missed_gainers backfill → ③ shadow(build+forward) → ④ paper 3-Type
  → ⑤ paper forward 충전 → ⑥ 수급 4종 → ⑦ 11주체 → ⑧ 국적별 → ⑨ F1
  - ②-2(6/23 신설): 일봉 늦은 적재일 missed_gainers self-heal 재생성(fill·sync 직후).
  - 관측 3단계(shadow·paper build·paper forward)는 일봉만 필요 → fill 직후 실행(수급보다 앞).
  - ⑤(6/15 신설): paper는 ④에서 코호트 생성만 → forward는 과거 코호트 대상이라 별도 단계.
    shadow가 ③에서 build+forward를 묶는 것과 동일 취지(H-3 dead code 해소).

안전:
  - 봇 OFF·매수 무접촉·실주문0·SAJANG 무변경(데이터/관측 전용, 검증된 진입점만 호출).
  - 각 단계 독립 try/except → 한 단계 실패해도 다음 진행. 어떤 예외도 exit 0.
  - 휴장일 skip. shadow/paper는 자체 멱등(기록된 날 skip).
  - ★데이터 메인=VPS(AWS·cron 18:00). 노트북은 VPSSync(매일19:00)로 받기만·NightlyPipeline task Disabled.
    (7/1 검수 H5 정정 — 구 "노트북=데이터메인" 표기는 역전된 옛정보였음.)
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
        ("②-2 missed_gainers backfill",   # 6/23 STALE 사고 fix — 일봉 늦은 적재일 self-heal 재생성(① fill·② sync 직후)
         [PY, "tools/run_missed_gainers_backfill.py"], 300),
        ("③ shadow build+forward",
         [PY, "tools/run_sector_reversal_shadow_daily.py"], 600),
        ("③-2 early variant shadow",   # 6/16 초입 포착 strict/early 병렬 shadow(매수 무접촉·관측 전용)
         [PY, "tools/run_early_variant_shadow_daily.py"], 600),
        ("③-3 reentry shadow",   # 6/18 재진입 룰 시뮬(early 후보 소비·손절+재진입 vs 손절만 vs 홀딩·관측 전용)
         [PY, "tools/run_reentry_shadow_daily.py"], 600),
        ("③-4 theme relay shadow",   # 6/22 테마 relay-aware(대장주↔소부장 주도그룹·초입·바통·관측 전용)
         [PY, "tools/run_theme_relay_shadow_daily.py"], 600),
        ("④ paper 3-Type",
         [PY, "tools/paper_3type_daily_run_6_6.py", "--asof", tstr, "--scan-events"], 900),
        ("⑤ paper forward 충전",   # 6/15 H-3 해소 — 과거 코호트 forward_d1/d3/d5·MFE/MAE 멱등 충전
         [PY, "tools/run_paper_3type_forward_daily.py"], 300),
        ("⑤-2 paper 학습 누적",   # 6/19 사장님 지시 — forward 충전 뒤 구조적 집계·누적 학습DB(관측·룰변경0)
         [PY, "tools/run_paper_learning_daily.py"], 300),
        ("⑤-3 paper 규칙 연습장부",   # 6/19 — 학습 발견 규칙 가상적용(현행 vs 규칙적용 forward 비교·실선정변경0)
         [PY, "tools/run_paper_rule_shadow_daily.py"], 300),
        ("⑤-4 paper 전용 포트폴리오 시뮬",   # 6/27 봇OFF여도 매일 — ledger기반 가상계좌 복리(현행 vs breadth게이트·MDD·record-only)
         [PY, "-c", "from data.paper_sim_portfolio import build_paper_sim; print(build_paper_sim())"], 300),
        ("⑥ 수급 4종",
         [PY, "-c", "import collect_all as C; C.step2_supply_demand(C.get_universe_codes())"], 3600),
        ("⑦ 11주체",
         [PY, "-c", "from data.market_investor_collector import collect_market_investor as f; r=f(days=3); print('ok' if r else 'EMPTY')"], 600),
        ("⑧ 국적별",
         [PY, "-c", "import collect_all; collect_all.step3_nationality()"], 600),
        ("⑨ F1 forward+preflight",
         [PY, "tools/run_f1_forward_preflight.py"], 300),
        ("⑩ 기관매집스캔(KRX-free)",   # 6/19 연기금 KRX박제 대체 — KIS 기관계 fresh 시그널 + 업로드(⑥ 수급 뒤)
         [PY, "tools/run_institution_accum_scan.py"], 300),
        ("⑪ catalyst 명분맥점 발굴",   # 6/24 명분 있는 끼 종목(상한가·3일10%+ → 재료·명분등급·점화맥점) record-only
         [PY, "tools/run_catalyst_scanner_daily.py"], 600),
        ("⑫ 순위 스냅샷(6종)",   # 6/27 KIS 순위 6종(등락률/거래량/체결강도/상하한가/외인기관/야간선물) 날짜별 record-only 적재
         [PY, "-c", "from data.ranking_snapshot_collector import collect_ranking_snapshots as f; print(f())"], 300),
        ("⑬ 공매도 일별추이",   # 6/27 KIS 공매도(FHPST04830000) 변동성 급락 선행지표·record-only(pykrx 중단 대체)
         [PY, "-c", "import collect_all as C; from data.flow_collector import collect_short_sale; collect_short_sale(C.get_universe_codes())"], 5400),
        ("⑭ 신용잔고 일별추이",   # 6/27 KIS 신용(FHPST04760000) 반대매매 급락 선행지표·record-only (공매도와 분리=한쪽 실패 격리)
         [PY, "-c", "import collect_all as C; from data.flow_collector import collect_credit_balance; collect_credit_balance(C.get_universe_codes())"], 5400),
        ("⑮ 한미충격 관측(kr_us_shock)",   # 6/27 정보봇 verdict(한국 vs 미국 시장충격) record-only 관측 누적·매매 0접촉(공매도/신용과 통합 리스크 후보)
         [PY, "-c", "from data.kr_us_shock_observer import collect_kr_us_shock_observation as f; print(f())"], 120),
        ("⑯ 클러스터 수확 페이퍼",   # 7/1 사장님 — 명분클러스터(호남 건설/전선) 후발주 릴레이 +5~8% 매일수확 forward 페이퍼. record-only·실주문0·picks/SAJANG/order 무접촉·한국스윙(FLOWX) 무접촉. 2~3주 누적→흑자 엣지 판정→승인후 라이브
         [PY, "tools/cluster_harvest_paper.py"], 300),
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
