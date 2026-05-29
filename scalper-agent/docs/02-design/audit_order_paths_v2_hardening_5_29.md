# audit_order_paths.py **v2 하드닝** 스펙 — 단타봇 5/29 23:42 (read-only 설계)

> 작성: 단타봇(Opus 4.8) / 권한: 스펙까지 (구현=Codex + 사장님 승인)
> 전제: v1(`tools/audit_order_paths.py`)은 실행 성공·안전·결정성 CONFIRMED. 단 단타봇 검증(wf_2cca0730)이 한계 5건 발견 → v2가 이를 닫는다.
> 원본 설계: `docs/02-design/audit_order_paths_design_5_29.md` (8 CHECK 정의 유지) / 본 문서는 **delta(변경분)만**.
> 고정 원칙: "단타봇은 주문 경로가 하나로 정리되고, paper도 intent 없이 포지션을 만들 수 없을 때까지 operational PASS를 받을 수 없다."

## v1 한계 → v2 수정 (1:1)

### FIX-A. CHECK-6 변수명 하드앵커 제거 (과소집계 해소) — 최우선
- **문제(v1)**: 패턴이 `\bentry\b * {1.05/0.97/...}`로 변수명을 `entry`에 고정 → 같은 의미 고정 TP/SL을 다른 변수로 쓰면 전부 누락.
- **미탐 실증(v1이 못 잡은 REAL)**: `bot/auto_trader.py:4607` reversal_tp=int(cp*1.03) / `data/limit_up_engine.py:416,429,431` int(signal_close*0.97)·int(base*1.03) / `data/limit_up_scanner.py:925` tp_5=int(avg_price*1.05) / `data/morning_recommendation.py:4281` sl=int(war_low*0.97).
- **v2 규칙**: AST 기반으로 **임의 변수/속성 * 고정배수** 검출.
  - 매치 대상: `int(<any_expr> * <const>)` 또는 `<any_expr> * <const>` 에서 const ∈ {배수 화이트리스트}. 배수 화이트리스트 = `1.03, 1.05, 1.10, 0.95, 0.97, 0.975, 0.93` + 명명상수 `TP_PCT/SL_PCT/TARGET_PCT/STOP_PCT/*_TP_*/*_SL_*` 할당.
  - 좌변(할당 대상) 이름이 `{tp, sl, take_profit, stop_loss, target_price, tp_price, sl_price, *_tp, *_sl, reversal_tp}` 패턴이면 **TP/SL 의미 확정**(배수 무관하게 추가 검출).
  - 라인 단위가 아니라 **논리 site 단위**로 집계하되, 출력엔 라인 전부 표기(라인 분해 카운트로 인한 "폭증" 착시 방지 — `site_count`와 `line_count` 둘 다 출력).
- **PASS**: TP/SL 의미 할당이 전부 `SAJANG.get_take_profit()/get_trailing_stop()/get_normal_sl()` 등 헬퍼 경유 시 0건.

### FIX-B. order-path 관련성 분류 (FALSE_POSITIVE / UNCERTAIN 제거) — REAL만 게이트
- **문제(v1)**: backtest/learning/dashboard 모듈을 주문경로 위반으로 오탐. 실증 FP: `data/trade_learner.py:41/42`(P&L 시뮬레이션). UNCERTAIN: `data/upload_swing.py:303/304`(Supabase 업로드).
- **v2 규칙**: 각 CHECK-6/8 매치를 3분류 라벨 부여.
  - `ORDER_PATH`(REAL): 그 변수가 `PaperPortfolio.open_position`/`TradeTracker.*`/`trade_object` tp·sl 필드/`_positions[...]` / KIS 주문 인자 등 **알려진 sink**로 흐르거나, 해당 모듈이 order-path 화이트리스트(`bot/auto_trader.py, bot/telegram_bot.py, bot/trading_coo.py, bot/kis_trader.py, engine/paper_portfolio.py, engine/limit_up_paper_trader.py, data/trade_object.py, data/trade_tracker.py, data/limit_up_engine.py`)에 속함.
  - `SIM_LEARNING`(제외): 모듈/함수가 backtest·learning·simulation (`*backtest*`, `trade_learner.py`, `*_sim*`, `what_if*`) → 게이트 제외, 보고엔 INFO로 표기.
  - `DASHBOARD_UPLOAD`(분리): `upload_*.py`, Supabase 적재용 dict → 게이트 제외, 단 "live 경로로 승계되면 위반" 경고 태그.
  - **분류 방식**: (1)모듈 화이트/블랙리스트 1차 + (2)변수 sink 추적(같은 함수 내 그 변수가 위 sink 호출 인자로 전달되는지 AST 경량 dataflow) 2차. 둘 다 애매하면 `UNCERTAIN`으로 두되 **게이트엔 미포함**(과탐으로 게이트 막지 않음) + 보고에 노출.
- **게이트 규칙**: CHECK-6/8은 **ORDER_PATH(REAL) 라벨만** FAIL 판정에 반영. SIM_LEARNING/DASHBOARD_UPLOAD/UNCERTAIN은 보고만(INFO/WARN).

### FIX-C. CHECK-1 텍스트매칭 → AST화 (거짓 PASS 차단)
- **문제(v1)**: `_contains_intent_before`가 8라인 텍스트 윈도우에서 `record_order_intent`/`intent_id` 문자열 매칭 → 주석/무관 변수에 걸려 거짓 PASS 가능(현재 영향 0이나 잠재).
- **v2 규칙**: open_position/close_position 호출 노드가 속한 **함수 본문 AST**에서, 해당 호출보다 앞선 실제 `Call(record_order_intent)` 또는 호출 kwargs에 `intent_id=` 인자 존재를 AST로 확인. 주석/문자열은 ast가 자연 제외. 8라인 윈도우 폐기.

### FIX-D. 게이트 명세 정렬 (안전측 채택)
- **현황**: v1 GATE_REQUIRED = {1,2,4,5,6,7,8} (CHECK-5 포함/CHECK-3 제외). 설계 §5 = {1,2,4,6,7,8}.
- **v2 결정(단타봇 리딩)**: ★ v1의 CHECK-5 포함이 더 안전하므로 **설계를 v1 쪽으로 정렬** ★ → GATE_REQUIRED = {1,2,4,5,6,7,8}. CHECK-3 = 정수회귀(증가 시 WARN, 게이트 미포함 — 선차단 존재 자체는 설계상 정상이고 fix는 'intent 기록'이라 별도). CHECK-5(주문메서드 gate 경유)는 신규 무게이트 주문메서드 추가를 즉시 FAIL시켜야 하므로 게이트 유지.
- exit 0 = {1,2,4,5,6,7,8} 전부 PASS. exit 1 = 하나라도 FAIL. exit 2 = 분석오류(SyntaxError 등).

### FIX-E. 회귀 안정성 (선택, 권장)
- `--baseline <json>` 옵션: 이전 결과와 diff → "신규 위반 N건 / 해소 M건" 출력 (P1 fix 진척 추적용).
- def 라인은 계속 AST 동적 산출(하드코딩 금지). repo_head 포함(결과 추적성).

## v2 출력 (delta)
```json
"CHECK-6": {
  "site_count": <논리 site>, "line_count": <라인>,
  "order_path_real": [...], "sim_learning_excluded": [...], "dashboard_upload": [...], "uncertain": [...],
  "verdict": "FAIL if order_path_real>0"
}
"CHECK-8": { "order_path_real": [...], "excluded": [...], "verdict": ... }
"gate": {"required": [1,2,4,5,6,7,8], "check3_monitor": <int>, "overall": "PASS|FAIL"}
```

## v2 검증 기대값 (구현 후 Codex 자가대조)
- CHECK-6 order_path_real ⊇ {auto_trader:882, auto_trader:4607, telegram_bot:4259/4260, trading_coo:2664/2665, trade_object:181, trade_tracker:482, limit_up_engine:81/416/429/431, limit_up_scanner:925, morning_recommendation:4281, limit_up_paper_trader:43/44} (v1 10 REAL + v1 미탐 6 = ~16+, **단 site 단위로 집계**)
- trade_learner:41/42 → SIM_LEARNING(제외) / upload_swing:303/304 → DASHBOARD_UPLOAD
- CHECK-8 order_path_real = {kis_trader:1803, trading_cfo:106, trade_object:31}
- CHECK-2 7/7, CHECK-3 17/19 불변(회귀)

## 안전 경계 (v1과 동일, 재확인)
- ast.parse + read_text + git 조회만. 대상 모듈 import/실행 금지. 네트워크/KIS/텔레그램/거래 0. 쓰기는 `--json` 출력만.
- kill_switch/PAPER_ONLY/systemctl/cron/.env/실주문 무접촉.
- 단타봇: 본 스펙까지. 구현/실행/커밋 = Codex + 사장님 승인.

## 다음 (v2 PASS 게이트 확보 후)
1. v2 실행 → CHECK-6/8 order_path_real 권위 숫자 확정.
2. **P1 fix**: (a)전 order_path REAL site의 TP/SL을 SAJANG 헬퍼로 통일 (b)RCG-B/D paper 2원장 open/close에 record_order_intent gate (c)CHECK-1·2·6 PASS.
3. P2: RCG-A intent화 + blocked 3분기 쓰기강제(CHECK-3·4). P3: 부분체결/reconcile/30%현금(CHECK-7·8). P4: RCG-C QUARANTINE.
4. `--gate` exit 0 전까지 paper operational PASS·live 금지.

## 관련
- v1 스펙: `docs/02-design/audit_order_paths_design_5_29.md`
- v1 결과: `ops/codex_outbox/20260529T233024_scalper_audit_order_paths_result.json`
- v7 대시보드: `ops/codex_inbox/20260529T233854_p0_integrated_dashboard_v7.json`
- 도구 검증: wf_2cca0730 (단타봇 read-only)
