# Phase 1 설계 — CHECK-6/8 SAJANG 단일 진실 (RC-2 종결)

> 마스터 플랜([master_recovery_plan_5_30.md](../01-plan/master_recovery_plan_5_30.md)) Phase 1.
> 단타봇 read-only 설계 → Codex 구현 → 단타봇 게이트 검증 → (신규 헬퍼 시) 사장님 승인.
> **Phase 1 종료 조건 (★ 전체 게이트 PASS 아님 ★)**: `audit --json` 에서 CHECK-6 verdict=PASS(`order_path_real`=0 & `uncertain`=0) + CHECK-8 verdict=PASS(`real_count`=0).
> 전체 `audit --gate`는 CHECK-1/2/7 잔여로 **FAIL이 정상** (전체 PASS는 Phase 2/3 완료 후 최종 종료). ★ 게이트 조건/임계 변경이나 CHECK-1/2/7 침범으로 PASS 만들기 금지. ★

---

## 0. 원칙 — 열거가 아니라 "도구가 worklist"

```text
  ❌ 1주일 함정:  메모리 "32건" 고정 리스트 → 셀 때마다 흔들림(14→42→32→실측 42)
  ✅ 구조 종결:   audit --json found.order_path_real[] = 살아있는 worklist
                 Codex가 각 항목 치환 → REAL=0 / --gate PASS까지 반복
```

★ 단타봇이 42줄을 손으로 옮기면 또 틀림(오늘 32 오보고 = RC-5). **Codex가 --json 직접 떠서 사용.** ★

---

## 1. 실측 현황 (commit e9e3c19, `audit --json`)

| CHECK | found | 구성 |
|-------|-------|------|
| CHECK-6 | order_path_real **42** / uncertain **13** / 제외 SIM 9·DASHBOARD 2 (총 66) | 아래 1-A |
| CHECK-8 | real_count **3** | kis_trader:1915 · trading_cfo:106 · trade_object:31 |
| CHECK-7 | missing **4키** | order_no · rt_cd · filled_qty · avg_fill_price |

### 1-A. CHECK-6 REAL 42의 3종류 (★ 처방이 다름 ★)

```text
 (가) 진짜 고정 TP/SL  → SAJANG 치환 (대다수)
   auto_trader 687(tp*1.10)/882(tp*1.05)/2667(tp 5.0)/2701(sl)/3626(sl)/4613(reversal*1.03)
   telegram_bot 4259(tp*1.05)/4260(sl*0.97) · trading_coo 2664(tp*1.03)/2665(sl*0.975)
   trade_object 43/44/181 · trade_tracker 482(target*1.05) · paper_portfolio 105(sl*0.95)
   morning_recommendation 2009/2010/4281 · limit_up_engine 81(TP_PCT=10)/734/1053(tp)
   limit_up_paper_trader 43/44/343/344/450/451 · auto_trader 686(sl*0.95)

 (나) 매수 진입가 밴드 (TP/SL 아님 → 도구 오분류, v3에서 제외)
   limit_up_engine 416/420/429/431 (tranche price) · 733(entry_price) · 1051(entry_low)

 (다) print-only / sink (→ v3 제외) — ★ 사용처 추적 후만, blanket 금지 ★
   limit_up_scanner 925/926 (tp_5/tp_10 display/log sink면 제외)
   ※ auto_trader 2666 style_sl_pct는 sink 아님 → 2701 stop_loss 실사용 = (가) REAL 치환. 2667 style_tp_pct는 2705 take_profit:0 이미 적용(잔존 변수 정리).
```

> ★ 정정: 도구가 (나)와 (다)일부(scanner sink)를 ORDER_PATH(REAL)에 잘못 포함 → v3 정밀화 필요. 단 (다)는 **blanket 제외 금지** — 사용처 추적 후 sink만 제외, ledger 적재면 REAL(예: style_sl_pct는 stop_loss 실사용=REAL). ★

### 1-B. CHECK-6 UNCERTAIN 13 (REAL/제외 판정 필요)
position_safety 83/84/184 · intraday_eye 752 · macro_strategy 59 · morning_recommendation 2143/2144/2160/2161 · bounce_hunter 455 · entry_monitor 134 · flow_intelligence 859 · monday_scan 260
→ 호출경로 추적: 실주문 TP/SL이면 REAL(치환), 스캐너/대시보드/추천산출용이면 제외.

---

## 2. 치환 패턴 (1:1, 기계적)

### CHECK-6 (나)(다) 제외 후 (가)+UNCERTAIN편입분
| 패턴(현재) | 치환(목표) |
|-----------|-----------|
| 고정 익절 `*1.05`/`*1.10`/`*1.03`/`TP_PCT=10` | `SAJANG.get_take_profit(buy)` (=0) 또는 제거 |
| 고정 손절 `buy*0.97`/`*0.95`/`*0.975` | `SAJANG.get_normal_sl(buy)` |
| 트레일링 `high*0.975` | `SAJANG.get_trailing_sl(high)` |
| 룰B/C/상한가 임계 | `SAJANG.is_rule_b/c/limit_up_split_triggered` |
| **신규 헬퍼 필요**(예: reversal +3%·limit_up TP 10%) | SAJANG 헬퍼 추가 → **사장님 승인 필수** |

### CHECK-8 (3건)
| 위치 | 패턴 | 치환 |
|------|------|------|
| kis_trader:1915 (=C3 safe_buy) | `min_cash_ratio 0.10` | `SAJANG.max_buy_amount(cash, total_eval)` |
| trading_cfo:106 | `min_cash_ratio 0.10` | `SAJANG.max_buy_amount` |
| trade_object:31 | `CASH_RESERVE=0.10` | `SAJANG.CASH_RESERVE_PCT` (0.30) |

---

## 3. audit v3 (정밀화 — 도구가 (나)(다) 제외하게)

```text
 CHECK-6 분류 보정:
   진입가 밴드 제외:  변수명 entry/tranche/entry_low/avg_price 좌변 → ENTRY_BAND(제외)
   sink 제외:        print()/로그/대시보드 인자 또는 미사용(죽은) 변수 → 제외
   UNCERTAIN 0화:    13건 호출경로로 REAL/제외 확정
 목표: found.order_path_real = (가)+UNCERTAIN편입분 만, (나)(다) 제거
```

---

## 4. 절차 + 종료 판정

```text
 ① Codex: audit --json 직접 → REAL 42 + UNCERTAIN 13 확보
 ② Codex: v3 정밀화((나)(다) 제외) + (가) SAJANG 치환 + CHECK-8 3건 치환
          신규 헬퍼 필요 시 → 단타봇이 사장님 승인 중계
 ③ 단타봇: audit --json REAL=0 & uncertain=0 & CHECK-8 real_count=0 확인 → --gate CHECK-6·8 PASS
 ④ pre-commit RULE-005~007 + Codex Tier-4 → 커밋
```

---

## 5. 재사용 인프라 (신규 작성 금지)
- `data/sajang_rules.py:SAJANG` — get_take_profit/get_normal_sl/get_trailing_sl/is_rule_*/max_buy_amount/calc_budget_per_stock/can_buy
- `tools/audit_order_paths.py` — --json(worklist) / --gate(판정) / --baseline(회귀)
- pre-commit RULE-005~007 — SAJANG 우회 차단

## 6. 경계
- 단타봇 코드 직접 수정 금지 — 설계·worklist·게이트 검증만.
- 신규 SAJANG 헬퍼 = 사장님 승인 필수.
- (나)(다)는 "치환"이 아니라 "도구 제외" — 코드 의미 변경 금지.
- Phase 1 끝 = CHECK-6/8 PASS (전체 끝 아님; CHECK-1/2/7은 Phase 2/3).
