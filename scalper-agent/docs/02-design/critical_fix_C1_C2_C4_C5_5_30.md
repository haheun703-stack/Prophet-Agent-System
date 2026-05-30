# CRITICAL Fix 설계 — C1·C2·C4·C5 (5/30 토)

> **작성**: 단타봇 (read-only). **구현**: Codex + 사장님 승인 + 4-Tier + 회귀.
> **근거**: 전 항목 로컬 소스 직접 Read + Select-String + Grep + 적대 Plan 리뷰로 검증 (추정 0).
> **상태**: `AUTO_TRADE_DISABLED=1` / `PAPER_ONLY=1` (live 차단) — 단 결함 다수는 재개 즉시 활성(latent) 또는 paper/관측성 공통.
> **고정 문장**: "주문 경로가 하나로 정리되고, paper도 intent 없이 포지션을 만들 수 없을 때까지 operational PASS 불가."

---

## 0. 배경 — 왜 지금 이 4건

5/30(토) 아침 A5P HL만도 청산 실측 → **🔴 P0 HOLD 유지**:
- 청산 ✅ `06:30:26 매도 HL만도 @62,600 (+1.46%) [TOP_MORNING_SELL]` + paper history ✅ `closed_trades` 기록
- 그러나 **order_intent ❌ 0건** (`order_intents_2026-05-30.jsonl` 미존재, 최신 = 05-27) = "No Intent, No Order" 위반
- 신규 2건: ① A5P가 **토요일에도 실행** (`is_trading_day()` 가드 누락 실증) ② **15:45 NameError** (`name 'is_trading_day' is not defined`)

`full_review_5_30.md`의 최우선 CRITICAL 5건 중 **C1·C2·C4·C5** 설계 (C3 30%현금은 P1 별도).

구현 순서 = **위험 역순: C5 → C4 → C2 → C1**.

---

## C5 — `_order_gate` blocked 분기가 intent 쓰기 실패를 삼킴 (위험 ★☆☆ / 기계적)

### 결함 (검증)
`bot/kis_trader.py:_order_gate` (def 152). 차단 분기들이 `_record_order_intent_or_block(...)`를 호출하지만 **반환값을 버린다**. 이 헬퍼는 쓰기 실패 시 `{"blocked":True,"reason":"ORDER_INTENT_RECORD_FAILED"}`를 반환한다 (def 112, 실패 반환 145-150). **성공 경로(266-279)만** 반환을 체크한다.

쓰기 무시 분기 (실측):
- runtime_block (198-209) → 반환 무시
- RUNTIME_CONFIG_ERROR (219-229) → 반환 무시
- MANUAL_POSITION_PROTECTED (247-257) → 반환 무시
- AUTO_TRADE_DISABLED (286-296) → 반환 무시

결과: 차단은 됐는데 장부 쓰기가 실패하면 **에러 로그만 남고 호출부엔 silent** → "차단했는데 장부 0건" 가능 (호출 ≠ 영속 기록).

### 설계
각 차단 분기에서 반환값을 캡처해 응답에 병합 (주문 차단 동작은 불변):

```python
wf = self._record_order_intent_or_block(side=side_u, code=code, qty=qty, manual=False,
                                        source=source_s, allowed=False, reason=..., message=..., strategy=strategy)
resp = {"success": False, "blocked": True, "reason": ..., "message": ...}
if wf:                                    # intent 영속 실패
    resp["intent_logged"] = False
    resp["audit_warning"] = wf.get("message")
    # CRITICAL 텔레그램 알림 1줄 (관측성)
return resp
```

- 성공 경로는 이미 올바름 — 변경 없음.
- 신규 writer 없음. 반환 dict 필드 추가 + 알림만.
- **위험**: 낮음.

---

## C4 — `vwap_split_buy` 조기 차단 경로에서 order_intent 0건 (위험 ★★☆)

### 결함 (검증)
`bot/vwap_split_buy.py:execute_vwap_split_buy` (def 80).
- **정정**: 분할 실행은 `trader.chase_buy`(134/210...) / `trader.buy_market`(149) 경유 → 이들은 `_order_gate`를 타므로 **실행 경로 intent ✅**.
- 누락은 **조기 차단/폴백 2경로**:
  - ① `is_auto_trade_disabled()` 차단 (107-114) → `return result`, intent 0 (← 5/29 P0 위반 유형)
  - ② `total_qty < 3` 단일매수 분기는 정상 (chase 경유). 진짜 공백은 caller 측: `auto_trader.py:2585` `if split_buy_enabled: vwap_split else: chase`. config `vwap_split_buy_enabled` 기본 True → vwap 차단 시 else(chase) 미실행 → intent 0.

### 설계
분할 루프 **이전**에 정식 게이트 1회 호출 → 차단 시 게이트가 모든 사유(kill/auto-disabled/runtime) intent를 `allowed=False`로 기록:

```python
result = SplitBuyResult(code=code, total_target_qty=total_qty, total_bought_qty=0, avg_price=0.0)
blocked = trader._order_gate("BUY", code, total_qty, source="vwap_split_buy")
if blocked:
    result.blocked = True            # (SplitBuyResult에 blocked 필드 추가)
    return result
```

- 기존 로컬 `is_auto_trade_disabled()` 중복 체크(107-114) 제거 — 게이트가 대체.
- `SplitBuyResult`에 `blocked: bool = False` 필드 추가 (caller가 폴백 여부 판단).
- **주의(문서화)**: 차단 시 게이트 intent 1건 + 실행 시 분할당 chase intent N건 → 논리적 1매수에 복수 intent row. 정상(각 실제 시도). `audit_order_paths` dedup이 `source="vwap_split_buy"` + leg을 인지하도록 명시.
- 신규 writer 없음 (`_order_gate`·`record_order_intent` 재사용).
- **위험**: 낮음~중.

---

## C2 — `_check_quick_exit` 고정 +5% TP = 트레일링 only 영구 룰 위반 (위험 ★☆☆)

### 결함 (검증)
`bot/auto_trader.py:_check_quick_exit` (def 4187). `config.yaml:354-364 quick_exit` **현재 활성** (직접 검증):
```yaml
quick_exit:
  enabled: true            # :355
  mode: "balanced"         # :357
  balanced_tp_partial: 5.0 # :363
  defensive_tp_full: 5.0   # :364
```
- balanced → `+5%` 도달 시 50% 부분익절 (4230-4236)
- defensive → `+5%` 도달 시 전량익절 (4239-4244)
- **SAJANG 완전 우회** = `SAJANG.FIXED_TP_DISABLED=True` / `get_take_profit()→0` / 트레일링만 위반.
- 호출부 1곳: `job_daily_reeval` (4575) — 반환이 truthy면 다른 청산 로직을 override.
- aggressive 모드는 이미 `return None`(순수 트레일링).

### 설계 (이중 방어)
**① 코드 가드 (영속, 단일 진실)** — `enabled` 체크(4203) 직후:
```python
from data.sajang_rules import SAJANG
if SAJANG.FIXED_TP_DISABLED:          # 영구 룰 우선 — config 켜져 있어도 무력화
    return None, None
```
**② config (즉시)** — `config.yaml:355 quick_exit.enabled: false` (5/19 OFF 복원).

- "default-off 금지 / SAJANG 단일 진실" 준수 + audit CHECK-6 통과 (SAJANG 5줄 윈도우 화이트리스트).
- **위험**: 낮음.

---

## C1 — `_wait_for_fill` bool 반환 → 부분체결을 전량으로 오보고 (위험 ★★★ / 단계적 / 마지막)

### 결함 (검증)
`bot/kis_trader.py:_wait_for_fill` (def 1246, 반환 `bool`):
- open-orders 목록에서 사라지면 무조건 `return True` (1273-1275) — 전량/취소/부분 구분 X
- 타임아웃 시 부분체결 있으면 잔량 취소 후 `return True` (1291-1297). `filled`/`remain` **계산만 하고 폐기**.

오보고 발생 (전부 `if filled:` → `_log_trade(..., qty, ...)` 원주문 qty 기록):
- smart_buy 942/966/992, smart_sell 1181/1205/1231, chase_buy 1078

영향: 매매일지 유령 수량. **live면 KIS 계좌에 유령 잔량 주식** 방치 → liquidate/EOD/자금·포지션 무결성 훼손. "내가 실제 몇 주 얼마에 샀나/팔았나" 보장 불가 = 실매수의 심장.

### 설계 (적대 리뷰 정정 반영)
반환 타입만 int로 바꾸면 "사라짐=전량" 가정이 그대로 거짓 → **유일한 권위 = 주문 후 KIS reconcile**.
기준 패턴: `auto_trader.py:job_d1_gap_check`(~5236) 매도 후 `fetch_balance` 재조회.
재사용 조회: `fetch_open_orders`(701 `tot_ccld_qty`→`filled_qty`) / `fetch_balance`(443 `hldg_qty`).

**Stage A (관측성, 저위험)** — `_reconcile_fill(code, qty_before)` 헬퍼:
- 주문 종료 후 `fetch_balance`로 실보유 델타 산출 → 실제 체결수량 반환
- smart_buy/smart_sell/chase_buy: `if filled:` 직후 reconcile 호출 → **실수량으로 `_log_trade`** + 반환 dict `"filled_qty": 실제값`
- `_wait_for_fill` 폴링 계약은 유지 (저위험)

**Stage B (정합성)**:
- caller 부킹 정합 (auto_trader는 이미 `resp.get("qty")`/`split_result.total_bought_qty` 사용 → 정확 qty가 자동 전파)
- SELL 3단계 취소 후 실잔량 > 0 → **포지션 삭제 금지, 실잔량으로 갱신** + 정직한 부분 보고

- 블라스트: kis_trader 7개 call site + downstream.
- **위험**: 높음. **Tier-3에 실계좌/샌드박스 부분체결 회귀 필수** (단위 PASS ≠ 실경로 — 5/25 교훈).

---

## 검증 (Codex 구현 시 의무 — 4-Tier + 게이트)

1. Tier 1 `bkit:code-analyzer` / Tier 2 호출 site grep+AST (사장님 룰 위반 0) / Tier 3 단위 + **실매매 경로 회귀**
2. C1 = 실계좌/샌드박스 부분체결 시나리오 회귀 필수
3. C2 = `audit_order_paths` CHECK-6 + pre-commit RULE-005~007 통과
4. `tools/pre_commit_check.py` RULE-001~004 + Codex Tier-4 상호 검수
5. fix 후 `audit_order_paths.py --gate` exit 0 전까지 **paper PASS·live 금지** (도구 v3 필요)
6. paper 리허설에서 **모든 차단/주문에 order_intent 1:1 생성** 실증 → P0 HOLD 해소 조건

## 재사용 인프라 (신규 작성 금지)
- `bot/order_intent.py:record_order_intent` — JSONL 단일 writer (intent+blocked 모두 `allowed`로, 별도 blocked writer 없음)
- `bot/kis_trader.py:_order_gate` / `_record_order_intent_or_block` — 정식 게이트
- `bot/kis_trader.py:fetch_open_orders`(701) / `fetch_balance`(443) — 실체결·실잔고
- `bot/auto_trader.py:job_d1_gap_check`(~5236) — reconcile 모범사례
- `data/sajang_rules.py:SAJANG` — `FIXED_TP_DISABLED` / `get_take_profit` / `get_trailing_sl` / `TRAILING_PCT`

## 범위 밖 (사장님 결정 대기)
C3 30%현금 SAJANG 일원화(P1) / verification cron 제거 / 15:45 NameError 원인 fix / A5P `is_trading_day()` 가드 / `audit_order_paths` v3 스펙.
