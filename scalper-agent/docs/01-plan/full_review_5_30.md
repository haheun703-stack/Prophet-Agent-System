# 전체 검수 리포트 — 5/30 00:18 (단타봇 read-only / 워크플로우 5차원 wf_58efaa09)

> 사장님 명령 "전체 검수 / 코드·로직·버그·슬러지" 응답. read-only 5에이전트 병렬. **단타봇 발견만 — fix는 Codex+사장님 승인.**
> 현재 `AUTO_TRADE_DISABLED=true` / `PAPER_ONLY=true` → 실주문 차단 상태. 단 다수 결함은 **자동매매 재개 즉시 활성(latent)** 또는 **paper/관측성 공통**.
> 고정 원칙: "단타봇은 주문 경로가 하나로 정리되고, paper도 intent 없이 포지션을 만들 수 없을 때까지 operational PASS를 받을 수 없다."

## ★ 최우선 CRITICAL 5건 (신규 발견 포함) ★

| # | 위치 | 내용 | 위험 |
|---|---|---|---|
| **C1** | `bot/kis_trader.py:1181` smart_sell | 부분체결 매도를 **전량 매도로 보고** (_wait_for_fill가 bool만 반환, 잔량 취소 후 True) | ★ KIS 계좌에 **유령 잔량 주식** 방치 / liquidate_one·EOD 청산 / 자금·포지션 무결성 훼손 ★ |
| **C2** | `bot/auto_trader.py:4232` _check_quick_exit | config.yaml `quick_exit.enabled=true`/balanced → **고정 +5% 부분익절** (defensive +5% 전량), AI모니터가 강제 | ★ **트레일링 only 영구룰 정면 위반** / SAJANG 완전 우회 / 5/22 +1,996% 미스 재발 / 재개 즉시 활성 ★ |
| **C3** | `bot/kis_trader.py:1803` safe_buy | 30% 현금룰 대신 `min_cash_ratio` **10%** 적용 (SAJANG.CASH_RESERVE_PCT 미경유) | 5/26 현금 6% 사고 재발 / 30% 룰 말단 미보장 |
| **C4** | `bot/vwap_split_buy.py:107` | kill-switch 차단 시 _order_gate 미도달 → **order_intent 0건** | ★ No Intent No Order 위반 / 5/29 P0 HOLD 근본원인 ★ |
| **C5** | `bot/kis_trader.py:198/247/286` _order_gate | blocked 3분기가 intent **쓰기실패 반환 무시** → 차단했는데 장부 0건 가능 | 감사 장부 silent 누락 (호출≠영속기록) |

★ 근본원인 체인: `_wait_for_fill`(bool 반환) → C1 + chase_buy/smart_buy 과대qty(HIGH) → vwap_split_buy → pre_close_d 포지션 과대. "내가 실제 몇 주 얼마에 샀나/팔았나" 보장 불가 = 진짜 단타 실매수의 심장. ★

## 차원별 요약 (HIGH+ 중심)

### 1. 버그/런타임 (CRITICAL 1 / HIGH 3 / MED 2)
- C1(smart_sell 부분체결). chase_buy/smart_buy 과대qty(`kis_trader:1078/944`) → vwap → pre_close_d. trade_tracker.close `entry=0` **KeyError**(`:154`, 청산 후처리 중단). safe_buy qty=0 게이트 이중 intent(`:1762`). _check_entry_watch 포지션 **qty 필드 누락**(`auto_trader:3429`). limit_up_paper_trader `sig['code']` KeyError(`:302`, paper 전체 중단).
- 모범사례: `job_d1_gap_check:5236` 매도 후 KIS 잔고 재조회 → **전 매매경로로 확산 권고(1순위)**.

### 2. 로직/룰 (CRITICAL 2 / HIGH 3 / MED 3 / LOW 1)
- C2(quick_exit +5%), C3(safe_buy 10%). qty=0 게이트 **금액한도 우회**(6/1 소액제한 금액가드 실효無). `limit_up_split_sell:47-51` 룰B/C/상한가 임계 **하드코딩 중복**(SAJANG import 0). `_check_danger_time:723` 14:50 off-by-one. REVERSAL+3%(`4607`)·execute_pending+10%(`687`)·day -2%SL(`3626`) SAJANG 미경유(대부분 dormant/latent).

### 3. 슬러지/데드코드 (HIGH 3 / MED 5 / LOW 3)
- ★ Kiwoom/main.py/TradingEngine **거대 데드 클러스터** (engine/order_manager·portfolio·market_state·trade_log·backtest.paper_trader·api.kiwoom_* — VPS 미가동) → legacy/ 격리 or 삭제.
- ★ `_job_intraday_verification_scan` **라이브 5분 cron 활성**(`trading_coo:4894`) — 5/28 P0 "영구제거" 결정 무시 / verification_mode default true → 재무장 위험.
- NXT TP+3%/SL-2.5% 하드코딩(`trading_coo:2664`). 데드: `position_protection.py`(importer0), 루트 `paper_trader.py`(v2.3), `tools/limit_up_*` 4종(cron0). **paper 3원장**(PaperPortfolio+TradeTracker+trade_object). nul/log/scan 클러터.

### 4. 보안 (HIGH 4 / MED 1 / LOW 2)
- ★ **kis_token.json .gitignore 보호 = 검수로 검증 완료**(루트 .gitignore:99-100 / git check-ignore 통과 / status 미노출) — 단타봇 5/30 추가분 정상 작동 확인. ★
- HIGH: `kis_token.json` 디스크 평문 JWT + **0o644 world-readable**(kis_nxt_kit:96의 0o600 미적용). 텔레그램 토큰 URL→예외로그 **평문 노출**(`telegram_alert.py:146`/`_common.py:47`/`flow_collector.py:151` + 15곳 / 5/28 P0#7 유틸경로 미수정). config.yaml git 추적(현재 placeholder).

### 5. silent-fail/관측성 (CRITICAL 2 / HIGH 4 / MED 4 / LOW 1)
- C4(vwap 차단 intent 0), C5(_order_gate 쓰기실패 무시). limit_up_paper_trader **로그없는 skip**(`:330`). paper_portfolio `_save` try/except無 + `_load` 실패시 **INITIAL_CASH silent 리셋**(`:68`) → 재시작 시 포지션 유실/이중진입. limit_up_position_manager `_load_json` **손상 silent 빈장부**(`:141`). pre_close_d 차단 시 **blocked report 미생성**(`:2784`, 5/28 14:50 사고 구조). job_eod_close 개별 청산 일부 실패에도 **success:True ✅보고**(`:4964`, _fallback_b15 미발동). 안전게이트 **fail-open**(거래량/스프레드/kill switch import, `:731`).

## fix 우선순위 (기존 RCG/P1~P4 통합)
- **P0-즉시(재개 전 필수)**: C1 부분체결(매수+매도 실잔고 reconcile) / C2 quick_exit OFF or SAJANG가드 / C4·C5 intent 누락·쓰기실패 / `_job_intraday_verification_scan` cron 제거
- **P1**: C3 30%현금 SAJANG일원화 + 전 order-path 고정TP/SL 32건 SAJANG화(audit CHECK-6) + paper 2원장 intent gate + qty=0 게이트 금액한도
- **P2**: 토큰 로그 마스킹 공통화(15곳) + kis_token 0o600 / paper_portfolio _save·_load 안전화 / blocked report 생성 / job_eod_close success 정확화
- **P3**: 슬러지 격리(Kiwoom 클러스터/데드모듈/3원장 일원화) / SAJANG 임계 하드코딩 통일 / fail-open→fail-closed
- 회귀 게이트: `audit_order_paths.py --gate`(v3 후) exit 0 전까지 paper PASS·live 금지.

## 경계
- 단타봇 read-only — 모든 fix는 Codex 구현 + 사장님 승인 + 4-Tier + 회귀.
- 원본: ops 워크플로우 wf_58efaa09 (5차원 전체 finding은 워크플로우 결과 참조).
