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
        # ★ 7/31 전체검수 [F-48] — 실패 경로가 stderr만 남겨 **실패 사유가 소실**됐다.
        # ⑳(완료위장 자가검증)은 STALE 목록을 stdout에 찍고 exit 1 하므로, 방어선이
        # 발동한 날 로그엔 `⚠ ⑳ … exit=1 |` 뒤가 비어 "무엇이 stale인지" 증거가 없었다.
        # stdout 꼬리를 함께 보존한다(아침 점검 A2의 진단력이 여기에 달려 있다).
        _out = " ".join(tail[-3:])[-200:].strip()
        _err = (r.stderr or "")[-160:].strip()
        _log(f"⚠ {name} exit={r.returncode} ({el:.0f}s) | out: {_out} || err: {_err}")
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
        ("⑪-2 명분 라벨(shadow 결합)",   # 7/7 사장님 "오를 근거 있는 종목 순·잡주X" — catalyst_archive 명분등급을 early/reentry shadow에 additive 부착 + MIN_KKI 병렬 코호트 요약(record-only·라벨/요약만·룰변경0·⑪ 이후 필수)
         [PY, "tools/run_meongbun_label_daily.py"], 120),
        ("⑫ 순위 스냅샷(6종)",   # 6/27 KIS 순위 6종(등락률/거래량/체결강도/상하한가/외인기관/야간선물) 날짜별 record-only 적재
         [PY, "-c", "from data.ranking_snapshot_collector import collect_ranking_snapshots as f; print(f())"], 300),
        ("⑬ 공매도 일별추이",   # 6/27 KIS 공매도(FHPST04830000) 변동성 급락 선행지표·record-only(pykrx 중단 대체)
         [PY, "-c", "import collect_all as C; from data.flow_collector import collect_short_sale; collect_short_sale(C.get_universe_codes())"], 5400),
        ("⑭ 신용잔고 일별추이",   # 6/27 KIS 신용(FHPST04760000) 반대매매 급락 선행지표·record-only (공매도와 분리=한쪽 실패 격리)
         [PY, "-c", "import collect_all as C; from data.flow_collector import collect_credit_balance; collect_credit_balance(C.get_universe_codes())"], 5400),
        ("⑮ 한미충격 관측(kr_us_shock)",   # 6/27 정보봇 verdict(한국 vs 미국 시장충격) record-only 관측 누적·매매 0접촉(공매도/신용과 통합 리스크 후보)
         [PY, "-c", "from data.kr_us_shock_observer import collect_kr_us_shock_observation as f; print(f())"], 120),
        # ⑯ 클러스터 수확 페이퍼 — 7/24 S-2 D-DAY 판정으로 제거(사장님 승인).
        #   폐기 근거: 재설계(동적 클러스터)로 신호일 4→46일까지 늘려 백테스트했으나
        #   6/12/24개월 전 구간 미달(12개월 113건 승률 26.5% -105.13%p)이고, 결정적으로
        #   대조군(클러스터 밖 동일필터)과 건당 -0.930 vs -0.928로 차이가 없었다
        #   = "명분 클러스터 소속"이라는 정보에 엣지 없음. 판정 도구·근거는
        #   tools/cluster_v2_backtest.py + data/strategy_deadlines.json judgments에 보존.
        ("⑰ 시장 레짐 게이트",   # 7/4 신설 — 전일 breadth≤0.45 회피신호(63일 검증된 유일 엣지)+지수 정배열 관측(사장님 6/7 숙제)+forward 자가검증. record-only·매수/매도/picks/SAJANG 무접촉. ④ ledger(당일 breadth) 이후 실행 필수
         [PY, "tools/run_market_regime_daily.py"], 120),
        ("⑱ 아침 브리핑 통합",   # 7/4 사장님 "장 시작 전 뭘 봐야 하나" — 레짐⑰+릴레이③-4+명분끼⑪+클러스터⑯+픽을 한 장(read-only 통합·재계산0). 플레이북 힌트=관측 라벨. ⑰ 이후 실행
         [PY, "tools/morning_briefing.py"], 60),
        ("⑲ 플레이북 페이퍼",   # 7/4 사장님 "바둑 경우의 수" — ticks(~20분 해상도·전종목·체결강도) 결정적 replay: PB-A 추매단타(+5%도달→다음관측 진입)·PB-B 상한가D+1 스파이크. record-only·매수/매도/picks/SAJANG 무접촉
         [PY, "tools/playbook_shadow.py"], 900),
        ("⑲-2 OBSERVE v2 대조",   # 7/10 압축 로드맵 — 장중 OBSERVE 러너(정제판 v2 실시간 intent·실주문 0) ↔ 최종 ticks 대조(일치/누락/유령/지연). 라이브 전환 판정 자료. record-only·주문 무접촉
         [PY, "tools/observe_v2_runner.py", "--compare"], 600),
        ("⑲-3 v2 페이퍼 장부",   # 7/16 사장님 "페이퍼로 승률·수익금부터" — 장중 intent를 최종 ticks로 페이퍼 정산(⑲ replay 동일규약·CB -6%p 리허설) → 누적 승률/수익금 장부+텔레그램. record-only·실주문 0·주문/SAJANG/picks 무접촉
         [PY, "tools/observe_v2_paper.py", "--daily"], 300),
        ("⑲-4 전략 데드라인 체크",   # 7/17 사장님 "전략에 데드라인=진짜 진화" — 대장(data/strategy_deadlines.json) D-day+기준 자동평가, D-3 이내/초과만 텔레그램. 자동 폐기 없음(판정 보고 의무·결정=사장님). read-only
         [PY, "tools/strategy_deadline_check.py", "--notify"], 120),
        ("⑳ 산출물 신선도 자가검증",   # 7/7 전체검수 fix — 핵심 산출물 오늘날짜 검증(완료위장 방지·stale이면 이 스텝이 ⚠) + DataVerifier 재실행(장전 박제 해소·fill 후 진실). read-only·매매 무접촉
         [PY, "tools/run_nightly_freshness_check.py"], 300),
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
