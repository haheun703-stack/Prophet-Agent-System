# Phase 3 설계 — CHECK-2 (TradeTracker 7메서드 intent) → 게이트 8/8 GREEN

> 단타봇 read-only 설계 → Codex 구현 + 사장님 승인. 단타봇 코드 직접수정 X.
> 마스터 복구 플랜 **마지막 Phase**. 종료조건: `audit --gate` exit 0 (CHECK-2 PASS → fail_count 1→0 → overall PASS = 게이트 8/8).
> ★ audit verdict 판정식 변경 금지 / Phase 2(CHECK-1/7) 선행 의존 / 매도(보호매도) 죽이면 실패. ★

## 0. 대전제 (불변)
- 목적 = 자동매매 재가동 + No Intent No Order 객관 증명. 구조 미학 X.
- ★ Phase 3 = 게이트 복구의 **마지막 조각**. CHECK-2만 PASS시키면 fail_count=0 → `--gate` exit 0 = 마스터 복구 완료. ★
- 선행 의존: **Phase 2 먼저**(CHECK-7이 record_order_intent 시그니처에 4 forensic 키 추가 + 작업B dedupe). Phase 3 호출은 그 4키·dedupe를 사용 → Phase 2 미완 시 시그니처 불일치.

## 1. 조사 확정 (단타봇 직접 Read, 추정 0)

### 1.1 CHECK-2 audit 판정식 (`tools/audit_order_paths.py:343-371`) — 불변
- `target_names` 7개: register / activate / close / register_paper_from_objects / check_paper_prices / paper_close_eod / register_paper_preclose.
- `data/trade_tracker.py`의 각 FunctionDef(이름 일치)에 대해 `has_intent = _has_call_named(node, {"record_order_intent","_record_order_intent_or_block"})`.
- verdict(370): `PASS if not missing and len(methods)==len(target_names)`. **= 7메서드 각각이 record_order_intent(또는 _record_order_intent_or_block) 호출을 함수 내 어디든 포함하면 PASS.** (CHECK-1의 "직전"과 달리 위치 무관 = 존재 여부.)
- `_has_call_named`(258) + `_call_name`(135): Name.id / Attribute.attr 둘 다 매칭 → `from bot.order_intent import record_order_intent` bare 호출이면 충족.
- 현재 found: 7메서드 전부 `has_intent:false` → missing_intent_count=7 → FAIL.

### 1.2 TradeTracker = paper 추적 원장 (실사용 진입점)
- 실호출(grep): `bot/telegram_bot.py:4207` register_paper_from_objects / `:4286` check_paper_prices / `:4341` paper_close_eod. (PaperPortfolio[CHECK-1]와 별개 원장.)
- leaf↔wrapper 관계:
  - register(55) PLANNED 등록 / activate(93) 매수체결→ACTIVE / close(108) 매도체결→CLOSED(+trade_learner).
  - register_paper_from_objects(209) → 내부 `self.register(to)`(241)+`self.activate`(242) per 종목.
  - check_paper_prices(250) → TP/SL 도달분 `self.close(code,price,reason)`(292).
  - paper_close_eod(296) → 시간손절분 `self.close(code,close_price,"TIME_STOP")`(324).
  - register_paper_preclose(396) → `self._active[code]` 직접 기록(leaf 미경유, 437-464).
- ★ 핵심 이슈: wrapper가 leaf를 호출 → leaf·wrapper 둘 다 record하면 **중복 기록**. → Phase 2 dedupe로 흡수(아래 2.3).

## 2. 설계 — CHECK-2 (7메서드 intent)

### 2.1 원칙 (사장님 룰 정합)
1. 7메서드 각각이 `record_order_intent` **literal 호출** 포함(audit 충족). bare import.
2. intent는 그 메서드의 **실제 액션**(plan/fill/sell)을 반영. forensic 4키(Phase 2) 채움.
3. wrapper+leaf 중복은 **dedupe 키 `(date, side, code, reason, source)`**(Phase 2 작업B)로 파일 1건 흡수 — wrapper는 leaf와 **동일 reason·source**로 leaf 호출 직전 기록 → dedupe collapse.
4. record_order_intent 실패가 **매도/등록을 막으면 안 됨**(로깅성, 비차단). close는 pop 후 success 경로에서만.
5. audit verdict식 변경 금지 / CHECK-1·6·7 회귀 0 / 매도 무손상 / live 금지.

### 2.2 메서드별 기록 규약 (source 통일 = `paper:tracker`)
```python
from bot.order_intent import record_order_intent
```
| 메서드 | side | reason | forensic 4키 | 위치 |
|---|---|---|---|---|
| register(55) | BUY | `TRACKER_PLAN` | order_no="PLANNED"/rt_cd="PLANNED"/filled_qty=0/avg_fill_price=0 | trade_obj 유효 분기, _save 전. (계획 = 미체결) |
| activate(93) | BUY | `PAPER_OPEN` | order_no="PAPER"/rt_cd="PAPER"/filled_qty=shares/avg_fill_price=filled_price | code 존재 확인 후, _save 전 |
| close(108) | SELL | `PAPER_CLOSE:{reason}` | order_no="PAPER"/rt_cd="PAPER"/filled_qty=t.get("shares")/avg_fill_price=exit_price | `t=pop` 후 success 경로, return 전 |
| register_paper_from_objects(209) | BUY | `PAPER_OPEN` | filled_qty=shares/avg=entry | 루프 내 `self.register(to)` 직전 (activate와 동일 키 → dedupe) |
| check_paper_prices(250) | SELL | `PAPER_CLOSE:{reason}` | filled_qty=shares/avg=price | `for ... in to_close:` 루프 `self.close` 직전 (close와 동일 키 → dedupe) |
| paper_close_eod(296) | SELL | `PAPER_CLOSE:TIME_STOP` | filled_qty=shares/avg=close_price | `self.close(...,"TIME_STOP")` 직전 (close와 동일 키 → dedupe) |
| register_paper_preclose(396) | BUY | `PAPER_PRECLOSE_OPEN` | filled_qty=shares/avg=entry | _active 직접기록 직전 (leaf 미경유 = 독립 기록) |

공통 kwargs: `code=code, qty=shares, source="paper:tracker", manual=False, allowed=True, message=name, estimate_amount_krw=entry*shares`.

### 2.3 중복 방지 (dedupe로 파일 1건/논리액션/code/일)
- dedupe 키 = `(date, side, code, reason, source)` (Phase 2 작업B, 파일 append만 dedupe·True 반환).
- ★ 단타봇 ee9ce63 실측 보완: dedupe는 **opt-in** — `record_order_intent(dedupe_daily=True)` 전달해야 `_has_daily_dedupe`가 작동. 7메서드 호출 전부 `dedupe_daily=True` 필수(안 넘기면 wrapper+leaf 중복). ★
- register_paper_from_objects 루프: wrapper가 `PAPER_OPEN` 기록(written) → self.register `TRACKER_PLAN`(written, 다른 reason=plan 기록) → self.activate `PAPER_OPEN`(skip, dedupe). ⇒ 종목당 파일 2건(PLAN+OPEN, forensic trail로 의도적).
- check_paper_prices/paper_close_eod: wrapper가 `PAPER_CLOSE:{reason}` 기록(written) → self.close 동일 키(skip). ⇒ 청산당 파일 1건.
- ★ register의 `TRACKER_PLAN` 별도기록 = "계획 intent"(미체결, filled_qty=0). 사장님이 파일 최소화 원하면 register도 `PAPER_OPEN` 통일 가능(이 경우 plan/fill 구분 소실) — 확인포인트.

### 2.4 비차단 보장 (매도 무손상)
- close/activate/register의 record_order_intent는 **로깅성**. record가 raise해도 등록·청산 흐름이 죽으면 안 됨 → record 내부 예외처리 확인(Phase 2 order_intent.py가 이미 안전하면 OK, 아니면 호출부 try/except 권장).
- close: `t = self._active.pop(code)`(113) 후, pnl 계산·trade_learner 기록과 **무관하게** record 1줄 추가. shares = t.get("shares",0).

## 3. 검증 (의무, Codex 구현 후 단타봇 수행)
1. `audit --json`: CHECK-2 found.missing_intent_count=0, len(methods)=7 → **verdict PASS**. → `audit --gate` **exit 0 (8/8 GREEN)**.
2. `--baseline`(현 0f3e922 또는 Phase2 후 commit): CHECK-1/3/4/5/6/7/8 회귀 0. CHECK-2만 FAIL→PASS.
3. 4-Tier: code-analyzer → 호출 site grep(7메서드 record 존재) → 단위+실경로 회귀 → Codex 상호. py_compile.
4. 매도 살아있음 회귀(트레일링/룰B/C/EOD, PYTHONIOENCODING=utf-8) PASS. trade_tracker close 회귀(pnl·trade_learner 기록 무손상).
5. pre-commit RULE-001~007 통과(stage 후 실행, vacuous pass 금지).
6. ★ paper 리허설: TradeTracker 경로 register/activate/close 1건당 order_intents JSONL 매칭 + forensic 4키 채워짐. dedupe로 중복 0(논리액션당 1건). ★

## 4. 경계 / 의존 / 확인포인트
- **선행**: Phase 2(CHECK-1/7) 완료 후 착수(4키 시그니처·dedupe 의존). Phase 2와 동일 `record_order_intent`·dedupe 재사용.
- audit verdict식 변경 금지 / CHECK-1 침범 금지(PaperPortfolio 경로는 Phase 2) / 매도 죽이면 실패 / live 금지.
- 확인포인트: register `TRACKER_PLAN` 별도기록 유지 vs `PAPER_OPEN` 통일(파일 최소화). 기본 = 별도기록(forensic trail).
- register_paper_preclose(396) 호출부 미확인(grep 60 내 없음) — 저빈도 가능하나 audit 위해 intent 필수.
- 커밋 메시지에 audit 전후(CHECK-2 FAIL→PASS, fail_count 1→0, **게이트 8/8**) 명시.
