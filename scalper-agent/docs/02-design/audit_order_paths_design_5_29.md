# audit_order_paths.py 설계서 — 주문경로 정적 감사기 (단타봇 5/29 23:10 / read-only 설계)

> 작성: 단타봇(Opus 4.8) / 권한: 설계서만 (구현=Codex + 사장님 승인)
> 목적: v6 진단 fact를 **사람·LLM 의견이 아니라 결정적(deterministic) 도구**로 기계 재현 → 진단 루프 종료 + PASS 게이트 + 협업 evidence bundle.
> 고정 원칙: "단타봇은 주문 경로가 하나로 정리되고, paper도 intent 없이 포지션을 만들 수 없을 때까지 operational PASS를 받을 수 없다."

## 0. 왜 이 도구가 1번인가

- 이번 라운드에서 Codex가 텍스트만 보고 TradeTracker call-site를 def로 오인했다. LLM끼리 "확인 또 확인"은 의견 교환이지 증명이 아니다.
- 측정 도구 없이는 fix가 됐는지 객관 판정 불가.
- 이 도구가 v6 숫자(intent 없는 호출부 / 선차단 / 룰위반 TP·SL)를 **기계로 재현**하면 → ① 진단 fact 확정(단타봇·Codex 의견 제거) ② PASS 회귀 게이트 ③ inbox에 붙일 evidence bundle.

## 1. 성격 / 안전 경계 (필수)

- **순수 정적 분석기**. AST(`ast` 표준 라이브러리) + 텍스트 grep만. 코드 실행 X / import 실행 X / 네트워크 X / KIS·텔레그램 X / 파일 쓰기는 `--json` 출력 파일만.
- 거래·cron·systemctl·env·kill_switch **일절 안 건드림**.
- 종료코드: 0=PASS / 1=위반 발견(FAIL) / 2=분석 오류. CI/pre-commit/consumer에서 게이트로 사용.

## 2. 입력 / 실행

```
python tools/audit_order_paths.py            # 사람용 표 출력
python tools/audit_order_paths.py --json      # evidence bundle(JSON) stdout → inbox 첨부용
python tools/audit_order_paths.py --gate      # PASS/FAIL만 (종료코드) → 회귀 게이트
```
- 스캔 루트: `scalper-agent/` (bot/, engine/, data/, api/, tools/). 대상 파일 화이트리스트는 `CHECKS` 상수에 명시.

## 3. 검사 항목 (v6 fact 기반 — 각 항목은 기대값 vs 실측 비교)

### CHECK-1 paper open/close 호출부 intent 게이트 부재
- 대상 심볼: `PaperPortfolio.open_position` / `close_position` 호출부.
- 방법: AST로 `*.open_position(` / `*.close_position(` 호출 노드 수집 → 호출 직전(같은 함수 내 N라인) `record_order_intent`/intent_id 인자 유무 검사.
- 기대값(현재 v6): open 호출 4(`engine/limit_up_paper_trader.py:347,453`, `bot/telegram_bot.py:4261`, `bot/trading_coo.py:2667`), close 호출 9. **전부 intent 없음 = 현재 FAIL**.
- PASS 조건: intent 없는 호출부 0건.

### CHECK-2 TradeTracker 상태변경 메서드 intent 부재
- 대상: `data/trade_tracker.py` 의 paper 상태 mutate 메서드 **7개** — `register(55)/activate(93)/close(108)/register_paper_from_objects(209)/check_paper_prices(250)/paper_close_eod(296)/register_paper_preclose(396)`.
- 방법: AST로 def 추출(라인은 동적 산출, 하드코딩 X) + 본문에 `record_order_intent` 호출 유무.
- 기대값: 7개 전부 intent 없음 = FAIL. PASS: 0건.
- ※ Codex가 call-site를 def로 오인한 항목 — 도구는 **def 노드만** 집계해 그 오류를 구조적으로 차단.

### CHECK-3 caller-level 선차단(early-return) intent 미기록
- 대상: `if (self.)?_auto_trade_disabled()` / `if is_auto_trade_disabled()` / `if SAJANG.AUTO_TRADE_DISABLED` 분기 직후 `return`.
- 방법: AST로 해당 if 노드 + body가 `return`으로 끝나고 그 전에 `record_order_intent` 호출이 없는 것 집계.
- 기대값(v6): `auto_trader.py` 16 + SAJANG분기 1 = 17 / KIS전체 = 17 + `telegram_bot.py:1392` + `vwap_split_buy.py:107` = **19**.
- 보고: 정수 + 라인 목록. (회귀: 숫자가 늘면 신규 선차단 추가 경고 / 줄면 intent화 진척)

### CHECK-4 _order_gate intent 쓰기 실패 강제 여부 (blocked 3분기)
- 대상: `bot/kis_trader.py` `_order_gate` 의 4개 분기 — allowed(266)/runtime_block(198)/manual_protected(247)/disabled(286).
- 방법: 각 분기가 `_record_order_intent_or_block(...)` 반환을 변수에 받아 `if blocked: return blocked`로 전파하는지 AST 검사.
- 기대값(v6): allowed만 전파(266→278). runtime/protected/disabled 3분기는 미전파 = FAIL.
- PASS: 4분기 전부 쓰기실패 전파.

### CHECK-5 KIS 실주문 메서드의 _order_gate 경유
- 대상: `buy_market(791)/smart_buy(908)/chase_buy(1034)/smart_sell(1148)/safe_buy(1762)/nxt_safe_buy(1712)/_afterhours_order(1558)`.
- 방법: 각 메서드 본문에 `_order_gate(` 호출 존재 확인.
- 기대값: 전부 경유(현재 PASS) — 회귀 감시용(누가 새 주문 메서드를 gate 없이 추가하면 FAIL).

### CHECK-6 사장님 룰위반 고정 TP/SL (트레일링 only 위반)
- 대상: `entry * 1.05` / `* 0.97` / `* 1.03` / `* 0.975` / `TP_PCT=` / `SL_PCT=` 하드코딩 + SAJANG 미경유.
- 알려진 위반(v6): `telegram_bot.py:4259`, `trading_coo.py:2664-2665`, `engine/limit_up_paper_trader.py:43-44`.
- 방법: 위 패턴 grep + 같은 파일에 `from data.sajang_rules import SAJANG` / `get_take_profit` 호출 부재 시 위반.
- PASS: 매수/청산 TP·SL이 전부 SAJANG 헬퍼 경유.

### CHECK-7 order_intent 필드 충분성 (forensic)
- 대상: `bot/order_intent.py` `record_order_intent` 시그니처/record dict.
- 방법: `order_no` / `rt_cd` / `filled_qty` / `avg_fill_price` 키 존재 검사.
- 기대값: 현재 없음 = forensic 미완 FAIL. PASS: 4키 전부 존재.

### CHECK-8 30% 현금룰 단일진실
- 대상: `min_cash_ratio` 기본 폴백(`kis_trader.py:1803`=0.10) vs `SAJANG.CASH_RESERVE_PCT`(0.30).
- 방법: 매수 경로가 `SAJANG.calc_budget_per_stock`/`max_buy_amount` 경유 안 하고 config 폴백 쓰는 site 집계.
- PASS: 모든 매수 경로 SAJANG 경유.

## 4. --json 출력 스키마 (evidence bundle)

```json
{
  "tool": "audit_order_paths.py", "version": "1", "generated_by": "deterministic_static_analysis",
  "repo_head": "<git rev-parse HEAD>", "scanned_root": "scalper-agent",
  "checks": {
    "CHECK-1_paper_open_close_no_intent": {"expected_pass": 0, "found": {"open": [...], "close": [...]}, "verdict": "FAIL"},
    "CHECK-2_tradetracker_no_intent": {"methods": [{"name":"register","line":55,"has_intent":false}, ...], "verdict": "FAIL"},
    "CHECK-3_caller_preblock": {"auto_trader": 16, "sajang_branch": 1, "kis_total": 19, "lines": {...}},
    "CHECK-4_gate_write_enforce": {"allowed": true, "runtime_block": false, "protected": false, "disabled": false, "verdict": "FAIL"},
    "CHECK-5_order_gate_coverage": {"methods": [...], "verdict": "PASS"},
    "CHECK-6_fixed_tp_sl_violation": {"violations": [...], "verdict": "FAIL"},
    "CHECK-7_intent_forensic_fields": {"missing": ["order_no","rt_cd","filled_qty","avg_fill_price"], "verdict": "FAIL"},
    "CHECK-8_cash30_single_source": {"non_sajang_sites": [...], "verdict": "FAIL"}
  },
  "overall": "FAIL", "fail_count": N
}
```

## 5. PASS 게이트 정의 (operational PASS 회귀)

- `--gate` 실행 시 CHECK-1·2·4·6·7·8 전부 PASS여야 종료코드 0. CHECK-3은 정수 회귀(증가 시 경고), CHECK-5는 PASS 유지 감시.
- **이 게이트가 0을 반환하기 전엔 paper operational PASS / live 전환 금지** (사장님 영구 룰의 코드화).

## 6. 협업 루프 연결 (Stage 0)

- 봇/단타봇이 RCG/fix 의뢰 시 `audit_order_paths.py --json` 출력을 inbox에 **첨부**.
- Codex consumer는 단타봇 첨부 숫자를 신뢰하지 말고 **자신이 같은 도구를 재실행**해 diff → 일치해야 review 진행. ("증거 없는 review는 의견" — Codex 합의).
- 부분체결 테스트(filled_qty < requested_qty 시 포지션 수량 = 실체결)는 별도 회귀 테스트로 분리(이 도구는 정적이라 동적 부분체결은 범위 밖 — 명시).

## 7. 구현 경계 (사장님 승인 필요)

- 단타봇: 본 설계서까지. **구현 X** (read-only).
- Codex: 본 설계대로 `tools/audit_order_paths.py` 구현 + 자체 실행해 v6 숫자와 대조한 결과를 outbox에.
- 사장님: 도구 구현/실행 승인. (도구는 정적 분석이라 거래·인프라 무영향 = 저위험.)

## 8. 다음 단계 (이 도구 PASS 게이트 확보 후)

1. P1 fix: RCG-B/D(paper 2원장 + TradeTracker intent gate) + paper TP/SL SAJANG화 → CHECK-1·2·6 PASS 목표.
2. P2: RCG-A intent화 + blocked 3분기 쓰기강제 → CHECK-3·4 목표.
3. P3: 부분체결/reconcile/30%현금 → CHECK-7·8 + 동적 reconcile 테스트.
4. P4: RCG-C Kiwoom QUARANTINE.

## 관련

- v6 대시보드: `ops/codex_inbox/20260529T225356_p0_integrated_dashboard_v6.json`
- 인계 메모리: `memory/project_5_29_p0_hold_full_state_handoff.md`
