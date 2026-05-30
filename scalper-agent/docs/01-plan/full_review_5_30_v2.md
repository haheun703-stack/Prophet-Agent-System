# 전체 검수 v2 — 5/30 (Phase1·2 반영 후, 단타봇 read-only)

> 워크플로우 wf_a425cfb8-5eb (5차원 review → CRITICAL/HIGH 적대검증). 18 에이전트.
> 현재 `AUTO_TRADE_DISABLED=1` / `PAPER_ONLY=1` (live 차단). 결함 다수는 재개 즉시 활성(latent).
> ★ 모든 fix는 Codex + 사장님 승인. 단타봇 read-only. ★
> ★ 단타봇 직접 재검증: 헤드라인 CRITICAL은 코드로 확인 완료(C-1). logic 차원의 환각 5건은 적대검증이 자동 철회. ★

## ★ 헤드라인 CRITICAL (단타봇 직접 확인) ★

**C-1 (auto_trader.py): 매수·매도가 같은 단일 게이트로 묶임 — 보호 매도 전체가 latent dead**
- `_auto_trade_disabled()`(def 581) = `SAJANG.AUTO_TRADE_DISABLED`(sajang_rules.py:120, frozen `True`) OR. 모든 보호 매도 job이 이 게이트로 시작:
  - `job_monitor@3651`(트레일링/SL) gate@3661 · `job_daily_reeval@4487` gate@4490 · `_job_eod_close@4922` gate · 룰B(15:26)·룰C(09:01 갭)·NXT 익일매도 동일
- 결과: 게이트가 닫히면(현재 항상) 보유/잔여 포지션에 -3% 트레일링·NORMAL SL·룰B·룰C·EOD청산 **전부 미발동**.
- ★ **정직한 뉘앙스(단타봇 판단)**: 지금은 사장님 5/27 "자동매매 OFF" 의도대로 매수·매도 **둘 다 정지** = 의도된 정지 상태(오늘 사고 아님). 단 ① **재개 시** "매수만 OFF, 보호매도는 살아있음"(= 사장님 룰 2 "고정TP 제거 = 매도 강화")을 **구조적으로 표현 불가** ② 수동/사장님 보유분은 원래 manual_position_protection으로 자동매도 제외(사장님 백업 매도) — "보호 0"의 일부는 의도. ★
- **새 근본원인 RC-6 = buy-gate / sell-gate 미분리.** fix 방향: 매수 게이트와 보호매도 게이트 분리(보호매도는 AUTO_TRADE_DISABLED와 무관하게 항상 실행, manual 보호는 별도).

## CONFIRMED HIGH (적대검증 통과)

| # | 위치 | 내용 | 차원 |
|---|------|------|------|
| H-1 | `limit_up_split_sell.py:72` should_trigger_split | 사장님 룰4(상한가 +25%→절반 +29% 매도) **DEAD** (live caller 0). auto_trader는 limit_up_phase 마커만 설정(4155), 소비처 없음. 룰B/C는 살아있음 | logic |
| H-2 | `trading_coo.py` _job_intraday_verification_scan_and_buy | 라이브 cron 잔존 (5/28 P0 "영구제거" 결정 모순). verification_mode true 시 미검증 매수 부활 | sludge |
| H-3 | Kiwoom 데드 클러스터 (main.py·api/kiwoom_*·engine/order_manager·portfolio·market_state) | importer 0, VPS 미가동. 수천 줄 데드 | sludge |
| H-4 | paper 3중 원장 (PaperPortfolio engine + TradeTracker data + trade_object) | 셋 다 live importer → 진실 장부 모호, PnL 정합성 위험. Phase3 CHECK-2와 연계 | sludge |
| H-5 | `order_intent.py` _send_order_intent_audit_alert (5/30 C5 신규) | 텔레그램 토큰 URL f-string 평문 → 예외 traceback 노출. 15곳+ 동일 패턴(5/28 P0#7) | security |
| H-6 | `data_store/kis_token.json` | 평문 JWT + 0o600 미적용(.gitignore 보호는 확인). 침해 시 계좌 접근 | security |
| H-7 | `engine/paper_portfolio.py:53-66` _load | 파싱 실패 시 INITIAL_CASH(1천만) **silent 리셋** → 재시작 시 포지션·현금 유실 + 이중 진입. fail-closed 필요 | silent-fail |
| H-8 | `auto_trader.py` 안전게이트(거래량/스프레드/killswitch) | 검사 실패 시 except에서 **fail-open**(통과) → 위험종목 매수 차단 실패. fail-closed 원칙 위반 | silent-fail |

## MED / LOW (요약)
- MED: C1 `_reconcile_fill` settle-delay 없음(매도수량 과소산출 latent, live 전 보완 자인) / `trade_tracker.auto_close_winners` +10% 고정익절 dead(latent 룰충돌) / `job_eod_close` 부분실패 success:True 오보고 / Phase2 dedupe 키가 분할재진입 시 1:1 깰 위험 / except Exception: pass 다수 / config.yaml git 추적(placeholder)
- LOW: trade_object STOP_MAX 5→3 동일화로 주석/regime 분기 모순 / auto_trader:2685 style_gain_percent 절대가를 'TP%'로 로깅(무해) / morning_recommendation SL은 SAJANG fallback만 / telegram 토큰 15곳 / nul·로그 클러터

## ★ RC-5 재실증 (단타봇 본성 — 적대검증이 잡음) ★
워크플로우가 **자기 환각 5건 자동 철회**: ① 룰C placeholder no-op + 미정의 헬퍼 5개(전부 코드에 없는 이름=환각) ② _check_quick_exit dead ③ safe_buy 30%현금 우회 ④ position_safety dead ⑤ trade_object dead. → 전부 AST 재검증으로 거짓 확인. **도구·적대검증이 단타봇 기억을 교정**한 증거.

## fix 우선순위 (전부 Codex + 사장님)
- **재개 전 필수**: C-1 buy/sell 게이트 분리(RC-6) / H-2 verification cron 제거 / H-7 paper _load fail-closed / H-8 안전게이트 fail-closed
- **수익 직결**: H-1 룰4 상한가 분할매도 살리기(매도 강화 = 사장님 룰2)
- **P1~P2**: H-5/H-6 토큰·kis_token 보안 / H-3·H-4 슬러지 격리 + paper 단일원장(Phase3 연계) / C1 settle-delay
- 게이트: audit --gate 8/8 + paper intent 1:1 + 사장님 승인 전 live 금지.

## 경계
단타봇 read-only. C-1·RC-6은 마스터플랜에 신규 항목으로 편입 필요(사장님 결정).
