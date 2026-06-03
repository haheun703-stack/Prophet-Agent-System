# 전체 코드 검수 이슈맵 (6/3) — 1차 매매안전 + 2차 데이터/슬러지

> 사장님 지시(6/3): 전체 검수(코드·로직·버그·슬러지). 봇 OFF·실주문0·SAJANG무변경·매매코드 임의수정 금지.
> 방법: bkit:code-analyzer 8영역 병렬(1차 A~D 매매안전 / 2차 E~H 데이터·검증·선정·슬러지). 전부 read-only.
> ★ 종합 판정: **Critical 0**(골격 견고 — SAJANG 단일진실·게이트·봇OFF 이중가드·휴장가드·No Intent No Order). 봇 OFF라 현 실사고 0. 단 **가동 전 필수 fix(P0) 7건**.

## 종합 판정
- ✅ **견고(조작/우회 0)**: SAJANG 단일진실(sajang_rules), 게이트 CHECK-1~8(audit_order_paths), 봇 OFF 이중가드(AUTO_TRADE_DISABLED+PAPER_ONLY), 휴장 진입점 이중가드, No Intent No Order 골격, 매도실패→포지션유지.
- ⚠️ **공통 패턴**: "모듈은 있으나 실제 경로 미연결/우회"(5/25 사고형) + "SAJANG 미경유 두번째 TP" + "검증은 알림만, 차단 안 함" + "수집 이원화로 sync 사각".

---

## P0 — 가동 전 필수 (매매/룰 직결, 전부 사장님 확인+4-Tier 후 fix)
| ID | 위치 | 이슈 | 출처 |
|----|------|------|------|
| **P0-1** | auto_trader / limit_up_split_sell | **상한가 +25% 절반 락인 미연결** — should_trigger_split() 테스트만 호출, 실경로 0건. limit_up_phase 마커만 찍고 절반매도 실행 없음(룰7). 5/25 패턴 | 1차-A |
| **P0-2** | kis_trader / order_intent | **dedupe_daily 전 경로 미작동** — _record_order_intent_or_block이 파라미터 미수신·미전달. 중복주문 방지 X | 1차-B |
| **P0-3** | order_intent | dedupe 키가 분할/재진입 오탐 → P0-2만 켜면 분할 1/N만 체결. **P0-2와 함께 설계 필수** | 1차-B |
| **P0-4** | kis_trader | 실주문 응답(order_no/rt_cd/filled_qty/체결가) ledger 미반영 — intent=시도만, 일일한도가 미체결 포함 | 1차-B |
| **P0-5** | kis_trader | 저수준 buy/sell_market·smart_* 직접호출 시 KIS 실계좌 조회 의무 미보장 | 1차-B |
| **P0-6** | bot/jarvis_decision.py:250 | ★**+10% 고정 TP 살아있음**(dry_run=true라 현재 알림만). config "5/26 dry_run=false" 계획 → flip시 트레일링-only 룰 위반 실매도. SAJANG 미경유(P0-1과 동일 계열) | 2차-H |
| **P0-7** | data_verifier / telegram_bot:4066~4104 | **데이터 검증이 매매 차단 안 함**: can_proceed/_data_verified가 picks·매수 게이트 미연결 + 검증 예외 시 fail-open(=True). 미검증 데이터로 매매 가능 | 2차-F |

## P1 — 가동 전 권장 (데이터/선정 오염)
| ID | 위치 | 이슈 | 출처 |
|----|------|------|------|
| P1-1 | asset_pool_loader:519/1128 | **today_chg 캐시 편향** — kis_market_top_cache 결측 종목=0.0→+35점("미반영강세=안전") 자동부여 + 상한가 -50 페널티 우회. 선정 랭킹 왜곡 | 2차-G |
| P1-2 | flow_collector | 종목별 수급에 휴장가드·self-heal 없음(market_investor엔 있음, 비대칭). KIS 직전거래일 복제 잔존 위험 | 2차-E |
| P1-3 | telegram_bot:3681 (봇 경로) | stock_data_daily sync 부재 → collect_all 미가동시 stale. data_verifier에 신선도 체크 0(stale 무감지). 소비처=backtest/R&D(매매 직접 아님, 백테 오염) | 2차-E |
| P1-4 | data_verifier | 값 무결성(open=0·결측·이상치·복제) 검증 부재 — "도착 여부"만 봄 | 2차-F |
| P1-5 | trading_coo:4574 | 14:50 룰D 잡 is_trading_day 가드 비대칭(진입점엔 있어 무해, RULE-002 형태) | 1차-D |

## P2 — 위생/슬러지 (매매 위험 낮음, 재활성화시 룰위반)
- **죽은 옛 엔진 클러스터(라이브 import 0)**: engine/trading_engine·body_hunter_master·order_manager·portfolio, risk/risk_manager(+5% TP), strategies/composite, 루트 main.py·paper_trader.py, strategies/body_hunter.py(orphan). → 격리(`_archive/`) 또는 삭제. 재활성화시 +5% TP 룰위반.
- **config.yaml:177(0.05)·302(10.0) take_profit_pct dead config** — 라이브 미소비, "두번째 진실" 박제. 주석/제거.
- **HIGH_VOLATILITY -5 페널티**(asset_pool_loader:1095) — 끼=수익원 발견과 역방향. brick5 flip시 해소(사장님+관측).
- **COO _send_alert 미정의**(trading_coo 4603~4664) — 4개 알림 죽은경로(매매 무영향).
- **워치리스트 shadow 직렬 잔재**(auto_trader:5371) — shadow 무영향, G7 별도 task 분리 권장.
- **sector_relay vs sector_history 이중체계** — 저장 죽고 스캔 살아있음(절반폐기). 텔레그램 섹터릴레이 명령 빈데이터 동작 여부 확인.
- code_auditor cron 미등록·substring 탐지 약함 / daily_self_audit docstring 6개 중 4개만 구현 / market_investor 봇경로 days=1 백필부족.
- 죽은코드: flow_collector._try_pykrx_short_balance, collect_all --sync-only 분기, 미사용 import 일부.

## 보존(정리 금지)
- auto_trader.py:4357 `elif False and pos.mode=="day"` — 5/26 삼화콘덴서 사고 차단, test_sajang_rules.py:213 assert 보호.

## fix 순서 설계 (사장님 결정 후)
1. **P0-2+P0-3 동시**(dedupe — 분할오탐 때문에 분리 불가) → 게이트8/8·매도무손상 회귀.
2. **P0-6**(jarvis +10% TP → SAJANG 경유 or dry_run 영구 false-lock) — 단순·룰직결.
3. **P0-1**(상한가 분할 연결) — 룰B와 역할분담 설계 필요.
4. **P0-4+P0-5**(응답 ledger + 실계좌조회) — 주문경로 묶어서.
5. **P0-7**(데이터검증 게이트 연결 + fail-closed).
6. P1(today_chg·수급가드·신선도검증) → P2(슬러지 격리).
- 각 단계: 사장님 확인 → design-validator → 구현 → 4-Tier(code-analyzer·grep·게이트8/8·매도회귀·Codex) → 단위+실경로 회귀.

## 정직 한계
- 8영역 핵심 파일 위주(403파일 전수 아님). tools/(106)·data/(133) 개별 orphan은 grep 후보 기반(동적 import 누락 가능, "의심" 표기).
- 봇 OFF라 현 실사고 0. P0는 "가동 전" 필수이지 "지금 사고중" 아님.
- code-analyzer 결과는 정적분석 — 일부 오탐 가능(검수 중 nightwatch 매도경로 오탐 1건 정정됨). fix 전 실경로 재확인 필수.
