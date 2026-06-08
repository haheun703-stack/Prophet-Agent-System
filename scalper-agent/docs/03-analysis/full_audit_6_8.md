# 6/8 전체 검수 + 수급 보강 리포트

> 사장님 지시(6/8): 전체 검수(수급 업데이트·빠짐 + 코드/로직/버그/슬러지). 봇 OFF·실주문0·SAJANG무변경·매매코드 임의수정 금지.
> 방법: 수급 freshness 스캔(read-only) + bkit:code-analyzer 2영역 병렬 + 게이트 8/8 회귀 + 6/3 audit 대조. 보강은 데이터 수집만(매매 무관).
> ★ 종합 판정: **지금 위험 0**(봇 OFF·게이트 8/8 PASS·관찰코드 매매 무접촉·골격 견고). 수급은 보강 완료, 코드 Critical/High 0. 단 6/3 P0 백로그 + 신규 Med 1 = **가동 전 필수**.

---

## ① 수급 데이터 — 보강 완료

| 수급 | 검수 시 | 보강 후 | 비고 |
|---|---|---|---|
| 일봉 daily | 88.2% | **94.8%** | 사무실 세션 fill 446재시도 |
| stock_data_daily | — | **대칭 0/0·OHLCV 0행 0** | step6 3651 sync |
| 종목수급 investor | 84.1% | **99.1%** (3439/3470) | collect_investor_flow 552 타겟 재수집(merge 안전, 100% 성공) |
| 시장 11주체 flow_market | 6/4 멈춤 | **6/5·6/8 백필** | collect_market_investor(days=6), 5/22~6/8 연속 |
| 외국인소진 foreign_exh | 6/5 | 6/8 미확정(T+1) | 예탁원 T+1 정산 → 6/9 자연확정. 6/6 행은 복제 ghost 아님(값 상이 확인) |
| nationality | — | 스냅샷(날짜무관) | 종목별 국적 보유, 시계열 아님 = 검수대상 아님 |

**원인**: 봇 OFF + 로컬 수동작업이라 수급 자동수집(collect_all G6 15:40, market_investor_collector)이 안 돌았음. 수동 보강으로 해소.
**★ 중요**: 수급 stale은 6/8 paper 첫 장부와 무관(paper 3-Type=일봉 기반·수급 None hook). 보강은 수급 리포트/분석용이며 paper 기준선 불변.
**남은 누락**: investor 31종목(2010-05-13 등 상폐·거래정지), 일봉 191종목(ETN/스팩/소형/거래정지) = 정상 잔차.

---

## ② 코드/로직/버그/슬러지 — 골격 견고

| 영역 | 결과 |
|---|---|
| 6/8 신규 paper 코드(7파일) | **Critical 0 / High 0** / Med 4 / Low 4 — 관찰전용 견고(picks·주문·SAJANG 무개입, None 보존 정확) |
| 매매 핵심 경로 | **Critical 0 / High 0** / Med 1 / Low 1 — 봇OFF 단일 choke point(`kis_trader._order_gate`), 매도 무손상, No Intent No Order |
| 게이트 8/8 회귀 | **PASS** (`audit_order_paths.py --gate`) |

### 6/8 신규 코드 Medium (전부 매매 영향 0, 6/12 해석 caveat)
- `ledger_integrity_check_6_8.py:154` — `--skip-breadth` dry 시 M7(breadth) N/A인데 FAIL 출력(verdict 무영향, 리허설 보고 오인 주의).
- `price_structure_features_6_8.py:249` — gap=0인데 gap_fill=True(gap_up/down 둘 다 False 행 함께 봐야).
- `market_breadth_6_8.py:61` — 52주 신고가 당일 포함 정의(통념보다 보수적).
- float_candle baseline 캐싱(비효율, 무해).

### 매매 핵심 Medium 1 (★ 신규 발견 ★)
- 🟡 `auto_trader.py:688`·`:4571` — position `take_profit`에 `dynamic_target.dynamic_tp`(ATR기반/+5% 폴백, `strategies/dynamic_target.py:139·199`) 주입 = SAJANG 미경유(`get_take_profit()`=0 우회) = Rule Registry 단일진실 위반.
  - **현재 inert**: 유일한 고정-TP 매도 분기 `auto_trader.py:4357 elif False and ...`가 영구 dead(5/26 삼화콘덴서 사고 차단, 보존). dynamic_target의 `cp>=tp`는 매도가 아니라 트레일링 모드 진입 트리거이고 실 청산은 trailing-SL(고점-3%)에서만.
  - **latent**: dead-block(4357) 재활성 시 +5% TP 부활 → 트레일링only 위반. = 6/3 P0-6(jarvis +10%TP)과 **같은 "SAJANG 미경유 TP" 계열**.

### Low (매매 무영향)
- `trading_coo.py:4603~4663` — 4개 alert 잡이 `self._send_alert`(COO 미정의) bare 접근 → AttributeError가 try/except에 삼켜져 알림 영구 누락(죽은 경로, 6/3에도 기록). 정상 패턴=`getattr(self.auto_trader, ...)`.

---

## ★ 가동 전 fix 목록 (봇 켜기 전 필수, 전부 사장님 승인 + 4-Tier 후)

> 봇 OFF라 **현 실사고 0**. 아래는 "가동 전" 필수이지 "지금 사고중"이 아님. 매매경로라 임의수정 금지.

| ID | 위치 | 이슈 | 출처 |
|----|------|------|------|
| P0-1 | auto_trader/limit_up_split | 상한가 +25% 절반 락인 미연결(룰7) | 6/3 |
| P0-2+3 | kis_trader/order_intent | dedupe_daily 미작동 + 분할/재진입 오탐(동시 설계) | 6/3 |
| P0-4 | kis_trader | 실주문 응답(order_no/체결가) ledger 미반영 | 6/3 |
| P0-5 | kis_trader | 저수준 buy/sell 직접호출 시 실계좌조회 미보장 | 6/3 |
| P0-6 | jarvis_decision.py:250 | +10% 고정 TP 살아있음(dry_run=true) — flip시 트레일링only 위반 | 6/3 |
| P0-7 | data_verifier/telegram_bot | 데이터 검증이 매매 차단 안 함 + 예외시 fail-open | 6/3 |
| **AUD-8** | **auto_trader.py:688·4571** | **dynamic_tp SAJANG 미경유**(inert·latent, P0-6과 동일 계열) → `SAJANG.get_take_profit(cp)` 통일 | **6/8 신규** |

**fix 순서**(6/3 설계 + AUD-8): P0-2+3(dedupe) → P0-6+**AUD-8**(SAJANG 미경유 TP 묶음) → P0-1(상한가분할) → P0-4+5(주문ledger) → P0-7(데이터게이트) → P1 → P2 슬러지.

---

## 정직 한계
- code-analyzer 정적분석 = 일부 오탐 가능, fix 전 실경로 재확인 필수.
- 매매 핵심 검수는 핵심 파일 위주(전수 아님). 6/3 audit과 대조.
- 수급 보강은 데이터 수집만 — paper 기준선·SAJANG·주문 무변경. foreign_exh 6/8은 T+1이라 6/9 확정.
- 봇 OFF·실주문0·게이트 8/8 PASS·6/8 ledger 무손상 전부 유지.

관련: [[full_audit_issue_map_6_3]], [[project_paper_6_8_first_ledger]]
