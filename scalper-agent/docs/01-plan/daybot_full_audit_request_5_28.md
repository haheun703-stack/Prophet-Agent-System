# 단타봇 전체 검수 Codex 의뢰서 — 5/28 저녁 (P0/P1/P2 재분류)

**작성**: 2026-05-28T20:46:33+09:00  
**작성자**: 단타봇 (Intraday Alpha + Execution Bot)  
**역할**: 사장님 명시 P0/P1/P2 재분류 / 5요소 포함 / Codex 검수 의뢰  
**HEAD**: 66944da  
**근거**: 사장님 5/28 20:40 [전체 검수 1차 보고 — P0/P1/P2 재분류 결정문]

## 0. 한 줄 결론 (사장님 명시)

> ★ **"단타봇은 내일 paper를 하기 전에 '수익률 튜닝'보다 '사장님 룰 위반 경로 제거/차단 검수'가 먼저"** ★  
> ★ **"+5% TP가 paper에 남아 있으면 내일 결과도 또 가짜가 됩니다"** ★

## 1. 단타봇 영구 금지 (사장님 명시)

- ❌ 코드 수정
- ❌ 전체 commit
- ❌ `kis_token.json` commit
- ❌ `bot/kis_trader.py` 수정분 commit
- ❌ `limit_up_*` untracked commit
- ❌ "수정 완료" 표현
- ❌ "5/29 paper 가능" 단언

---

## 2. P0 — 5/29 paper 전 반드시 검수 (5건)

### P0-1: TP +5% 하드코딩 2건 (★ paper 직접 영향 ★)

#### 파일/라인 (2건)

**P0-1-A** — `scalper-agent/bot/auto_trader.py:882`
```python
                        if entry and not tp:
                            tp = int(entry * 1.05)   # 기본 +5%
```

**P0-1-B** — `scalper-agent/bot/telegram_bot.py:4259`
```python
                    shares = max(1, int(portfolio.cash * 0.25 / max(entry, 1)))
                    tp = int(entry * 1.05)   # 단타 TOP픽: +5% 목표
                    sl = int(entry * 0.97)   # -3% 손절
                    ok = portfolio.open_position(
                        code, name, entry, shares, "daytrading_pick",
                        tp, sl, time_stop_days=1,
                    )
```

#### 실제 실행 경로 여부

- **P0-1-A**: `macro_strategy` 실패 시 fallback TP / user_pick 후보 생성 단계 → 실행 가능
- **P0-1-B**: ★ **`portfolio.open_position(...)` 직접 호출 / `daytrading_pick` source / paper portfolio가 +5% TP 그대로 적용** ★

#### 사장님 룰 충돌

- 사장님 영구 룰: **TP 고정 +5% 폐기 / 트레일링 only** (`feedback_trailing_only_tp`)
- `SAJANG.FIXED_TP_DISABLED = True` / `SAJANG.get_take_profit() = 0`
- pre-commit RULE-005: TP+5% 자동 매도 패턴 CRITICAL

#### 5/29 paper 증거 체인 영향

- **★ 직접 영향 ★** — paper portfolio가 +5% TP 사용 → 내일 paper 결과 +5%에서 차익 잠금 → ★ **트레일링 -3% 룰 검증 불가** ★

#### 필요 테스트

- `SAJANG.get_take_profit()` 반환값으로 대체 (Rule Registry 단일 진실)
- paper portfolio 매도 시점 = 트레일링 -3% 일관 적용 검증
- 회귀: +5% 도달 후 -3% 빠짐 시점에 매도 (잠금 X)

#### 금지 작업

- 단타봇이 라인 882 / 4259 수정 X
- 단타봇이 SAJANG import 추가 X (Codex 검수)

---

### P0-2: `self.mode == "day"` 조기 매도/전량청산 경로 (3건)

#### 파일/라인 (3건)

**P0-2-A** — `scalper-agent/bot/auto_trader.py:4416` (체결강도 <90 조기 매도)
```python
                # ── 자비스 추세 약화 매도 (5/20 사장님 결정) ──
                #   asset_pool 종목 전용: +1.5% 이상 수익 + 체결강도 < 90 → 매도
                elif (
                    self.mode == "day"
                    and pos.get("source") == "asset_pool"
                    and pnl_pct >= 1.5
                    and price_info.get("strength", 100) < 90
                ):
```

**P0-2-B** — `scalper-agent/bot/auto_trader.py:4916` (day mode 전량청산)
```python
        if self.mode == "day":
            # 데이 모드: 전량 청산 (preclose/predawn/pending_next_day 태그 포지션은 제외)
            # ★ 5/25 사장님 룰 B ★ pending_next_day=True (분할 매도 후 이월) 종목 보호
            keep_sources = ("preclose", "predawn")
```

**P0-2-C** — `scalper-agent/bot/trading_cfo.py:119` (day mode 예산)
```python
        elif mode == "day":
            # config.auto_buy_amount x event_mult
            base = self.config.get("bot", {}).get("auto_buy_amount", 500000)
            total = int(base * event_mult)
```

#### 실제 실행 경로 여부

- **P0-2-A**: 정기 5분 cron / asset_pool source 종목 / 트레일링 활성 전 단계 발동 가능
- **P0-2-B**: EOD close 단계 (15:10 G5 MARKET_CLOSE) / `pending_next_day` 보호 적용됨 (룰 B 부합)
- **P0-2-C**: 예산 계산 / day mode일 때 `base × event_mult` (★ 사장님 30% 현금 보유 룰 + split_cash 모드와 충돌 검수 ★)

#### 사장님 룰 충돌

- 사장님 영구 룰: 트레일링 -3% 일관 (`SAJANG.TRAILING_PCT = 3.0`)
- 룰 B (`SAJANG.RULE_B_THRESHOLD = 10.0`) + D+1 이월
- 룰 D (`SAJANG.RULE_D_SURGE_MIN = 10.0`) + 오늘 +10%+ 강세
- 자금 룰: `SAJANG.CASH_RESERVE_PCT = 0.30` + `SAJANG.BUDGET_MODE = 'split_cash'`

**충돌 검수 항목**:
- **P0-2-A**: 체결강도 <90 매도 = 트레일링 -3% 룰보다 빠른 매도 → 트레일링 발동 전 차익 잠금 위험
- **P0-2-B**: 전량청산 keep_sources 매칭 검수 (룰 B D+1 이월 종목 100% 보호되는가)
- **P0-2-C**: `base = auto_buy_amount` 사용 = `SAJANG.calc_budget_per_stock()` 우회 / 30% 현금 보유 룰 위반 가능

#### 5/29 paper 증거 체인 영향

- **P0-2-A**: paper portfolio +1.5% 도달 시점에 조기 매도 → 트레일링 검증 불가
- **P0-2-B**: paper portfolio EOD 전량 청산 → D+1 이월 검증 (룰 B)
- **P0-2-C**: paper portfolio 매수 수량 = `auto_buy_amount` 기준 → 사장님 한도 정책과 불일치

#### 필요 테스트

- P0-2-A: 체결강도 <90 매도 = 사장님 룰 우선순위 명확화 (트레일링 vs 자비스 약화)
- P0-2-B: keep_sources 회귀 (asset_pool + pending_next_day 보호)
- P0-2-C: `SAJANG.calc_budget_per_stock(cash, total_eval, top_k)` 호출로 대체

#### 금지 작업

- 단타봇이 라인 4416 / 4916 / 119 수정 X
- 단타봇이 `mode` 분기 임의 변경 X

---

### P0-3: silent skip (6+ 위치 / P0-4 확장)

#### 파일/라인 (6+)

| # | 파일 | 라인 | 패턴 |
|---|---|---|---|
| 1 | `bot/auto_trader.py` | 1167 | `if not self.is_running:` |
| 2 | `bot/auto_trader.py` | 3526 | `if not self.is_running:` |
| 3 | `bot/auto_trader.py` | 3530 | `if not self.is_running:` |
| 4 | `bot/auto_trader.py` | 3655 | `if not self.is_running:` |
| 5 | `bot/auto_trader.py` | 4476 | `if not self.is_running or self.mode != "swing":` |
| 6 | `bot/auto_trader.py` | **5044** | `if not self.is_running:` (5/28 사장님 발견 / 15:26 룰 B silent skip 원인) |

#### 실제 실행 경로 여부

- 모두 scheduled job 또는 cron callback 진입점
- `self.is_running == False` 시 무로그 즉시 return

#### 사장님 룰 충돌

- 사장님 본인 통찰 (5/28 16:50): "봤다고 보고하는 시스템 = 깨짐"
- silent skip = 관측 불가 = 다음 사고 시 원인 추적 불가

#### 5/29 paper 증거 체인 영향

- ★ **직접 영향** ★ — paper training 발동/skip 모두 관측 가능해야 증거 체인 성립

#### 필요 테스트

- 최소한 scheduled job entry에는 skip 로그 필수
- 회귀: `is_running == False` 시 INFO 로그 1건 / 정상 시 1건 / 매매 시 1건+
- VPS 다음 거래일 15:26 / 09:15 / 14:50 로그 1건 이상 확인

#### 금지 작업

- 단타봇이 `auto_trader.py` 6+ 위치 수정 X
- 단타봇이 `is_running` 플래그 변경 X

---

### P0-4: `trailing_pct=2.0` config drift

#### 파일/라인

**`scalper-agent/bot/trading_cto.py:92`**
```python
        "risk": {
            "sl_pct": 3.5,
            "trailing_pct": 2.0,
            "max_hold_days_momentum": 5,
            "daily_loss_limit": 500000,
            "reserve_ratio": 0.10,
            "sl_pct_day": 2.0,
            "tp_pct_day": 5.0,
        },
```

#### 실제 실행 경로 여부

- `trading_cto.py` config dict 정의 → 실제 사용 site grep 필요 (Codex 검수)
- `tp_pct_day: 5.0` / `sl_pct_day: 2.0` / `sl_pct: 3.5` 모두 사장님 룰과 다름

#### 사장님 룰 충돌

- `SAJANG.TRAILING_PCT = 3.0` (5/25 사장님 룰 1) — **2.0과 불일치 (config drift)**
- `SAJANG.NORMAL_SL_PCT = 3.0` (5/25 사장님 룰) — `sl_pct: 3.5`와 불일치
- `SAJANG.FIXED_TP_DISABLED = True` — `tp_pct_day: 5.0`과 불일치

#### 5/29 paper 증거 체인 영향

- 실제 사용 여부와 관계없이 config drift 자체가 향후 사고 위험
- 단타봇/Codex가 이 config를 신뢰하면 사장님 룰 위반 매매 발생

#### 필요 테스트

- `trailing_pct` / `tp_pct_day` / `sl_pct` / `sl_pct_day` 호출 site 전수 grep
- 사용 0건이면 dead code 표시 후 archive 후보
- 사용 1건+ 시 `SAJANG.*` 헬퍼 호출로 대체

#### 금지 작업

- 단타봇이 `trading_cto.py` 수정 X
- 단타봇이 config 값 임의 변경 X

---

### P0-5: `kis_token.json` untracked + `.gitignore` 미포함

#### 파일/라인

- `scalper-agent/kis_token.json` (393 bytes / 5/12 생성)
- `.gitignore` (현재 `kis_token` 패턴 0건)

#### 실제 실행 경로 여부

- 봇이 KIS API 토큰 동적 갱신 시 이 파일에 저장 가능
- `git add .` 실행 시 자동 staging → public repo push 시 사장님 계좌 노출

#### 사장님 룰 충돌

- 사장님 본인 명시 P0-6: `.gitignore` 추가 의무

#### 5/29 paper 증거 체인 영향

- 직접 영향 X / 보안 위험만

#### 필요 테스트

- `.gitignore`에 `kis_token*.json` + `**/kis_token*.json` 추가
- `git check-ignore -v scalper-agent/kis_token.json` 통과
- 회귀: `git status --short`에서 `kis_token` 미포함

#### 금지 작업

- 단타봇이 `.gitignore` 수정 X (Codex 검수 후)
- 단타봇이 `git add .` 실행 X
- ★ **`kis_token.json` commit 영구 금지** ★

---

## 3. P1 — paper 증거 체인 이후/동시 검수 (4건)

### P1-1: `except: pass` 5건

| 파일 | 라인 | 분류 필요 |
|---|---|---|
| `data/bot_view_broadcast.py` | 27 | 데이터 무결성 경로? |
| `data/bottom_signal_detector.py` | 35 | 신호 생성 경로? |
| `data/limit_up_continuation_tracker.py` | 31 | 상한가 추적 경로? |
| `tools/safe_sync_positions.py` | 32 | 포지션 동기화 경로? (위험) |
| `tools/what_if_analyzer.py` | 23 | 분석 도구 (안전 가능) |

**Codex 검수 의뢰**: 주문/리포트/데이터 무결성 경로인지 분류 / 위험 분류 시 `except Exception as e: logger.warning(...)`로 대체

### P1-2: `scheduled_reports.send_message` 실패

→ 이전 P0-3 (사장님 5/28 16:50) — 동일 의뢰 (`trading_coo.py:4417` wrapper / `scheduled_reports.py:226` mismatch)

### P1-3: KIS rate limit

→ 이전 P1-8 (사장님 5/28 16:50) — 동일 의뢰 (`flow_collector.py:277/455` shared throttle/backoff)

### P1-4: surge_learner 외인 0.0% (컬럼명 mismatch)

→ 이전 P1-9 (사장님 5/28 16:50) — 동일 의뢰 (`surge_pattern_learner.py:140` 컬럼명 매핑)

---

## 4. P2 — 정리/부채

### P2-1: TODO/FIXME 잔존 8건

| 파일 | 라인 | TODO |
|---|---|---|
| `bot/intraday_supply_tracker.py` | 25 | TODO ⑱ |
| `bot/position_safety.py` | 236/239/263 | TODO ⑭ (5/22 단타봇 fix) |
| `bot/position_safety.py` | **474** | "큐 미구현 TODO → 실제 큐 구현" |
| `bot/scheduled_reports.py` | 2 | TODO ⑮ |
| `bot/trading_coo.py` | 4405/4933 | TODO ⑮ |

**Codex 검수 의뢰**: 각 TODO 영구 완료 또는 archive 결정

### P2-2: 오래된 legacy 텔레그램/모드 문구

- `auto_trader.py:4348` `elif False and pos.get("mode") == "day"` (dead code — False 가드)
- 기타 legacy 검수 (Codex 자율 판정)

### P2-3: 미사용 코드 archive 후보

- `limit_up_*` 4파일 봇 호출 0건 (이전 P1-11) — approved_intraday_selector 통합 판정 또는 archive
- Codex 자율 판정

---

## 5. 코드베이스 규모 (검수 범위)

| 모듈 | 줄 수 |
|---|---|
| `bot/auto_trader.py` | 6,384 |
| `bot/trading_coo.py` | 5,036 |
| `bot/kis_trader.py` | 2,440 |
| `bot/scheduled_reports.py` | 274 |
| **핵심 4개 합계** | **14,134** |
| 전체 Python 파일 | 361개 |

---

## 6. 단타봇 권한 한계 (작성 시 준수)

### ✅ 한 일 (작성만)
- grep 코드 라인 추출
- 사장님 룰 위반 패턴 매칭
- `sed -n` 컨텍스트 확인
- 본 의뢰서 작성

### ❌ 하지 않은 일 (영구 룰)
- ❌ 코드 수정 0건
- ❌ `.gitignore` 수정 X
- ❌ `git add` / commit / push X
- ❌ "수정 완료" 표현 X
- ❌ 단타봇 자율 fix 결정 X

---

## 7. 5/29 paper 재개 조건 (단언 X / 조건만 명시)

P0 5건 fix 완료 + P1 4건 fix 완료 + 회귀 테스트 PASS + 사장님 별도 승인 후에만 가능

**특히 P0-1 (+5% TP) fix 의무**: paper 결과 왜곡 방지

---

## 7-bis. agent 신규 발견 (bkit:code-analyzer 병행 검수 / 20:50)

### P0 신규 4건 (단타봇 grep 미포착 / ★ 5/26 삼화콘덴서 사고 잔존 경로 가능성 ★)

#### P0-A: REVERSAL 분기 TP 강제 설정 (RULE-005 우회 패턴)

- 파일: `scalper-agent/bot/auto_trader.py:4607-4610`
- 코드: `reversal_tp = int(cp * 1.03)` → `pos["take_profit"] = reversal_tp`
- 추가: `auto_trader.py:4562` `pos["take_profit"] = target_state.dynamic_tp` (engine.daily_reeval dynamic_tp>0 시 동일 위험)
- 실패 모드: pre-commit RULE-005 정규식 매칭 X (변수 중간 단계 우회) / SAJANG.FIXED_TP_DISABLED 무효화 / 5/26 사고 재발 위험
- 사장님 룰 충돌: `feedback_trailing_only_tp` / `SAJANG.FIXED_TP_DISABLED=True`
- 5/29 paper 영향: paper portfolio REVERSAL 분기 활성 시 TP 강제 + 차익 잠금

#### P0-B: `_positions` 등록 시 TP default = 10% (`int(cp * 1.10)`)

- 파일: `scalper-agent/bot/auto_trader.py:687-689`
- 코드: `tp = ... item.get("tp", int(cp * 1.10))` → `self._positions[code]["take_profit"] = tp`
- 실패 모드: `item.get("tp")=None` 시 default `cp*1.10` 적용 / SAJANG.FIXED_TP_FORCE_ZERO 미적용
- 사장님 룰 충돌: TP 고정 +10% = TP +5%보다 더 큰 룰 위반
- 5/29 paper 영향: 사장님 매수 보호 SYNC 경로 일부 통과 시 5/26 사고 재현

#### P0-C: safe_buy / nxt_safe_buy `qty=0` 게이트 우회 (★ 사장님 5/28 직접 발견 정확 ★)

- 파일: `scalper-agent/bot/kis_trader.py:1712` (safe_buy) + `:1762` (nxt_safe_buy)
- 코드: `_order_gate("BUY", code, 0, ...)` → `_estimate_order_amount(qty=0)` L101 `return None`
- 실패 모드: amount=None 통과 → daily_limit / max_position_amount 등 금액 기반 차단 우회
- 5/29 paper 영향: 자동매매 재가동 시 즉시 위험 (현재 AUTO_TRADE_DISABLED로 차단됨)
- → ★ 이전 P0-7 (.env 이중 소스)와 결합 = 단타봇 사고 재발 위험 ★

#### P0-D: verification/intraday `_positions` 등록 시 필수 키 누락

- 파일: `scalper-agent/bot/auto_trader.py:1832-1837` (verification) + `:2016-2019` (intraday verification)
- 실패 모드: position dict에 `stop_loss`/`take_profit`/`high_watermark`/`trailing_activated`/`trailing_sl` 미설정 → 매도 함수 (트레일링/룰 B/룰 C/REVERSAL) `pos["stop_loss"]` 직접 접근 시 KeyError 또는 트레일링 보호 전체 우회
- 사장님 룰 충돌: 트레일링 -3% 룰 작동 보장 불가
- 5/29 paper 영향: verification 매수 종목 = 매도 보호 로직 전체 우회 가능

### P1 신규 3건

#### P1-A: `scheduled_reports.py` SAJANG.CASH_RESERVE_PCT 직접 비교 없음

- 파일: `scalper-agent/bot/scheduled_reports.py` (274줄 전체)
- 실패 모드: 30% 현금 보유 룰 위반 시 보고서에 알림 누락
- 5/29 paper 영향: paper 결과 보고서에 사장님 룰 위반 감지 누락

#### P1-B: `pre_commit_check.py` RULE-005 정규식 부정확

- 파일: `tools/pre_commit_check.py:78`
- 패턴: `take_profit.*=.*\*.*1\.0[3-9]|take_profit.*int.*\*.*1\.[0-9]+`
- 실패 모드: P0-A `reversal_tp = int(cp * 1.03)` → `pos["take_profit"] = reversal_tp` 패턴 매칭 X / 변수 중간 단계 우회
- 5/29 paper 영향: 향후 신규 위반 코드 commit 차단 실패

#### P1-C: KIS 계좌번호 평문 노출 + telegram config 미확인

- 파일:
  - `scalper-agent/bot/kis_trader.py:1568` (acc_clean[:8] 일부)
  - `scalper-agent/tools/order_forensics.py:315` (계좌번호 노출 가능)
  - `output/telegram_alert.py:22-23` config 파일 `.gitignore` 미확인
- 실패 모드: 텔레그램 토큰 외 추가 시크릿 노출 위험

### P2 신규 3건

| # | 발견 |
|---|---|
| P2-A | `auto_trader.py:360-361, 394-395, 477, 524` except: pass 추가 4건+ (단타봇 P1 5건 외) |
| P2-B | `auto_trader.py:6266` predawn merge 정보 무경고 폐기 (entry_date 등) |
| P2-C | `kis_trader.py:1626, 1663` CODE_TO_NAME 미등록 시 code를 name으로 사용 |

### INFO

- `bot/auto_trader.py` **6,384줄** = 단일 파일 너무 큼 (권장 300줄)
- 5/26~5/27 사고 패치 누적 → fix 위치 추적 어려움 → Codex 검수 시 분할 권장
- 데드 코드 별도 스캔 필요 (pre_commit `check_unused_imports` staged 파일만)

### agent 종합 통찰 (영구 명심)

> "신규 P0 4건 = 사장님 5/21~5/26 영구 룰의 default-off / 옛 코드 잔존 패턴과 동일한 사고 클래스"
> "P0-A + P0-B = 5/26 삼화콘덴서 자동 매도 사고의 진짜 잔존 경로 가능성"
> "P0-C = 5/28 이후 자동매매 재가동 시 즉시 위험"
> "P0-D = 트레일링 보호 전체 우회 가능"

---

## 8. 첨부

본 의뢰서 동일 시각 JSON 의뢰 생성:
- `scalper-agent/ops/codex_inbox/20260528T204633_daybot_full_audit_request.json`

## 9. 관련 메모리

- `project_5_29_first_goal_evidence_chain.md` — 5/29 첫 목표 + 증거 체인
- `project_5_28_codex_decision_final.md` — 사장님 1차 Codex 결정문 (P0 7 + P1 5)
- `daybot_5_28_post_session_audit.md` — 5/28 장중 사후감사
- `daybot_p0_request_5_28.md` — 1차 P0 의뢰서 (silent fail 패턴)
- `feedback_preview_vs_confirmed_5_28.md` — preview vs confirmed 영구 룰
- `feedback_no_self_modification_5_28.md` — 자율 코드 수정 영구 금지

---

**의뢰서 작성 완료 / 단타봇 다음 행동 = 사장님·Codex 검수 대기 / 코드 수정 0건 유지**
