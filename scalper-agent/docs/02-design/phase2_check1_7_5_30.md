# Phase 2 설계 — CHECK-1 / CHECK-7 GREEN + stale test 정리

> 단타봇 read-only 설계 → Codex 구현 + 사장님 승인. 단타봇 코드 직접수정 X.
> 마스터 플랜 Phase 2. 종료조건: `audit --json` CHECK-1 + CHECK-7 verdict PASS (fail_count 3→1, CHECK-2만 잔존).
> ★ audit verdict 판정식 변경 금지 / Phase 3 CHECK-2 침범 금지 / 매도 로직 죽이면 실패. ★

## 0. 대전제 (불변)
목적 = 자동매매 재가동 + 수익률 개선. 매도 8원칙 유지. "구조 미학보다 audit 통과 + 사고 방지(No Intent No Order)가 먼저"(사장님 5/30).

## 1. 조사 확정 (직접 Read, 추정 0)

### CHECK-1 audit 탐지 방식 (`tools/audit_order_paths.py:286-340`)
- `PaperCallVisitor`가 `*.open_position()`/`*.close_position()` 호출을 찾고, 각 호출의 `has_intent_before` = ① 그 호출에 `intent_id=` kw가 있거나 ② **같은 함수 내 호출 라인 위쪽에 `record_order_intent`/`_record_order_intent_or_block`/`_emit_intent` 호출**이 있으면 True (`_contains_intent_before`).
- verdict(339): `PASS if not missing["open"] and not missing["close"]`. **이 식 변경 금지.**
- → ★ **호출부(같은 함수)에서 buy/sell 직전 record_order_intent 호출**이 정답. PaperPortfolio 내부 기록은 audit가 못 봄 → 이번 범위 제외. ★

### CHECK-1 대상 = paper open/close 호출부 (13 site / 실제 함수 6개)
- open 4: `bot/telegram_bot.py:_job_paper_register`(~4262) · `bot/trading_coo.py:_job_nxt_paper_register`(~2668) · `engine/limit_up_paper_trader.py:_process_new_signals`(~349) · `_check_pullback_entries`(~455)
- close 9: `bot/telegram_bot.py:_job_paper_check`(4314/4319/4322) · `_job_paper_eod`(~4362) · `bot/trading_coo.py:_job_nxt_paper_morning_close`(~2484) · `engine/limit_up_paper_trader.py:_check_existing_positions`(225/246/266) · `engine/paper_portfolio.py:mark_to_market`(258, TIME_STOP 자동청산)
- ※ `mark_to_market`(258) close는 PaperPortfolio 내부 호출 → 호출부 기록 원칙 적용 어려움. **이 1건은 mark_to_market 내 close 직전 record_order_intent 허용**(예외, 같은 함수 내 위쪽 기록이면 audit 통과). 나머지 12건은 외부 호출부.

### CHECK-7 audit (`tools/audit_order_paths.py:820-839`)
- `record_order_intent`의 `record` dict 키에 `order_no/rt_cd/filled_qty/avg_fill_price` **4키 모두 존재**하면 PASS. verdict식 `not missing` 변경 금지.
- 현재 `bot/order_intent.py:50-65` record = 14키, 4키 없음.

## 2. 설계 — CHECK-1 (호출부 기록)

### 원칙 (사장님 7개)
1. 13개 paper buy/sell 호출부에서 **주문 직전** record_order_intent 호출.
2. buy/sell 호출보다 **먼저** 기록(같은 함수 내 위쪽 = audit `_contains_intent_before` 충족).
3. skip/blocked/close/partial/manual 경로도 누락 없이 기록.
4. PaperPortfolio 내부 단일 기록 추가는 **범위 밖**.
5. audit CHECK-1 판정식 변경 금지.
6. 매도 로직 죽이면 실패.
7. Phase 3 CHECK-2 침범 금지.

### 기록 규약 (재사용: `bot/order_intent.py:record_order_intent`)
```python
from bot.order_intent import record_order_intent
# 매수 직전
record_order_intent(side="BUY", code=code, qty=shares, source="paper:daytrading_pick",
                    manual=False, allowed=True, reason="PAPER_OPEN",
                    message=name, estimate_amount_krw=entry*shares,
                    order_no="PAPER", rt_cd="PAPER", filled_qty=shares, avg_fill_price=entry)  # 4키 (CHECK-7)
ok = portfolio.open_position(code, name, entry, shares, "daytrading_pick", tp, sl, ...)
```
- source 규약: `paper:{원source}` (예: paper:daytrading_pick / paper:nxt / paper:limit_up) — live와 구분, audit/대사 용이.
- **skip 경로**(shares<=0 / 현금부족 / 슬롯소진 / BRAIN 관망 / 품질필터): buy 호출 안 되더라도 그 분기에서 `allowed=False, reason="PAPER_SKIP_*"` intent 1건.
- **close**: `record_order_intent(side="SELL", ..., reason="PAPER_CLOSE:{reason}")` close_position 직전.
- **partial**(자금부족 수량 감소): open_position이 내부 조정하므로, 호출부는 "요청 shares"로 기록 + 실제 체결은 paper 특성상 동일. (정합성은 리허설 1:1에서 확인.)

### ★ intent 중복 방지 키 설계 (사장님 명시)
- 문제: 30초 루프(`_job_paper_check`)·재시도로 같은 포지션에 intent 폭증 가능.
- 해결: 호출부에서 **dedupe 키** = `(date, side, code, reason, source)` 1일 1회 가드. 이미 기록된 키면 skip(로그만). open은 보통 1회지만 close 루프는 다회 호출 → "실제 close가 일어난 경우(close_position 반환 not None)에만" intent 기록하거나, dedupe 키로 1:1 보장.
- ★ 권장: **close는 close_position 반환값으로 가드** — 반환 None(미보유)이면 intent 안 남김(노이즈 0), 실제 청산 시에만 1건. open은 open_position True일 때 + skip은 분기별 1건.
- 단, audit는 "호출 위 40줄 내 record 존재"만 보므로 **무조건 호출 직전 기록**이 audit엔 안전. dedupe와 audit 충족을 둘 다 만족하려면: **record를 호출 직전 무조건 실행하되, dedupe 키로 파일 append만 1회 제한**(record_order_intent 내부 dedupe 또는 호출부 set 가드). → Codex가 dedupe를 record 레벨에서 처리(같은 키 재기록 skip)하면 audit(코드상 호출 존재)도 통과하고 파일 노이즈도 0.

## 3. 설계 — CHECK-7 (forensic 4키)
- `bot/order_intent.py:record_order_intent` 시그니처 + record dict에 4키 추가:
  - `order_no: str = ""` / `rt_cd: str = ""` / `filled_qty: int = 0` / `avg_fill_price: int = 0`
- live 경로(kis_trader): C1 반환 dict의 order_no·filled_qty 전달, rt_cd는 resp에서 추가 전달, avg_fill_price는 체결금액/수량(또는 fetch_balance pchs_avg_pric) — Codex가 연결.
- paper 경로: order_no="PAPER" / rt_cd="PAPER" / filled_qty=shares / avg_fill_price=entry.
- ★ CHECK-7은 키 존재만 검사 → 4키 추가로 즉시 PASS. 단 "연결성"(실제 값 채워짐)은 paper 리허설 1:1에서 실증.
- append-only JSONL 유지. (intent→result 2라인 조인은 후속 고도화, Phase 2는 record에 4키 포함으로 충족.)

## 4. stale test 정리
- `bot/test_dynamic_trailing.py`(d775fb6, 5/22): trail 5/7/10% 기대 = 5/25 "고점 -3% 일관"으로 폐기. 코드는 3.0 정상.
- 갱신: `test_moderate_strength_5_to_10pct`/`test_strong_strength_10pct_plus`/`test_limit_up_imminent_25pct_plus`/`test_compute_trailing_sl` 기대값을 **trail 3.0 / activation 3.0**으로 수정. 헤더에 "5/25 고점-3% 일관 룰 반영, 옛 multi-zone 5/7/10% 폐기" 명시.
- 폐기 근거: `SAJANG.TRAILING_PCT=3.0` + `feedback_trailing_only_tp` + 5/25 사장님 명령. 5/25 후속 테스트(trailing_simple/compute_real)는 이미 -3% 기준 PASS.

## 5. 검증 체크리스트 (Codex 구현 후 단타봇)
1. `audit --json`: CHECK-1 missing open=0 & close=0 / CHECK-7 missing=0 → verdict PASS. fail_count 3→1.
2. 게이트 미조작: CHECK-1(339)/CHECK-7(838) verdict식 5303004와 동일 diff 확인.
3. 회귀 무손상: `--baseline` CHECK-3/4/5/6/8 PASS 유지. 매도 살아있음(5/25 기준) PASS.
4. CHECK-2 미침범: TradeTracker 변경 0 (Phase 3).
5. stale test: `python bot/test_dynamic_trailing.py` PASS (PYTHONIOENCODING=utf-8).
6. paper 리허설 1:1: paper 매수/매도/skip/청산마다 order_intents JSONL 1줄 + forensic 4키 채워짐 실측. dedupe로 중복 0.
7. py_compile + pre-commit RULE-005~007(staged).

## 6. 재사용 인프라 (신규 writer 금지)
- `bot/order_intent.py:record_order_intent` (4키 추가) / `iter_order_intents`(대사용)
- `bot/kis_trader.py:_reconcile_fill`·반환 order_no/filled_qty (C1)
- `tools/audit_order_paths.py` --json/--gate/--baseline

## 7. 경계
단타봇 코드 직접수정 X / audit verdict식 변경 X / CHECK-2 침범 X / PaperPortfolio 내부 단일기록 X(범위 밖) / 매도 죽이면 실패 / live 금지(전체 게이트 FAIL 지속).
