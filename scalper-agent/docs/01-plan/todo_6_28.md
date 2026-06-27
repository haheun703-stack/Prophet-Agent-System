# 단타봇 TODO — 6/28(일)~ 바로 진행

> 작성: 단타봇 6/27 밤(KST). 사장님 "나머지 내일 todo 저장하고 내일 바로 진행".
> 안전 전제: 봇 OFF(AUTO_TRADE_DISABLED)·실주문0·picks불변·매도무손상·SAJANG단일진실·관측없이 flip금지·KRX무접촉.

---

## ✅ 6/27 완료 (연속성 — 어제 한 것)

- **KIS 확장 1차**(순위6종+공매도+신용)·AWS 자동수집 확정 (73ca5e0). 공매도 99.7%·신용 98.4% 실측. nightly ⑫⑬⑭ + deploy_pull로 월 18:00 VPS 자동.
- **kr_us_shock 관측**(7b18bc4): 정보봇 한미충격 verdict를 단타봇이 record-only 수신. nightly ⑮. graceful(월 08:00 공급 전 skip).
- **페이퍼 개선 — 데이터 드리븐**:
  - 공매도/신용 종목게이트 = **기각**(소급 d3 -2.75%p 역효과). 직박 안 한 게 정답.
  - **breadth(시장 폭) 게이트 = 진짜 변별자** — VPS 205표본 승률 28→55%, 페이퍼 전용 시뮬 누적 +1.96→+6.06%·MDD -3.87→0.
  - NXT breadth 게이트(0210cd5): trading_coo _job_nxt_paper_register(페이퍼전용·라이브격리 PASS). ★단 봇 OFF라 현재 NXT 진입 0=대비분.
  - 페이퍼 전용 포트폴리오 시뮬(5c689f6): 봇OFF여도 ledger로 매일 가상계좌(현행 vs breadth). nightly ⑤-4.
- **NXT stale 진단**: 원인=봇 OFF(nightwatch_decide skip)→NXT 소스 5/26 정지→페이퍼 매매 5/29 정지. **내 paper 작업 무관**(확정).

---

## ★★ 0순위 — 월요일(6/29) 첫 자동실행 검증 (★한 번도 통합실행 안 됨)

- VPS nightly 18:00 **⑫순위·⑬공매도·⑭신용·⑮한미충격·⑤-4 페이퍼시뮬 + NXT breadth게이트 첫 통합실행** 로그 확인(`~/bodyhunter/logs/nightly.log`).
- 공매도/신용 전체 2596 적재 커버리지 + rate-limit 소요시간(각 5400s 충분한지).
- **kr_us_shock 키 실측**: 월 08:00 정보봇 첫 공급분 → ⑮ 관측 csv 적재되는지 + 내 파싱(verdict/kr_shock/us_shock/channels/drivers)이 실구조와 일치하는지 (mock만 검증함).
- 순위 6종 + 야간선물(18:00 개장이라 월요일 첫 정상 적재) 확인.

## 1순위 — 페이퍼 전용 루프 정밀화 (낙관편향 제거)

- 현재 MVP는 MFE익절 낙관편향+미투입현금 미반영 → **절대수익 해석금지, 상대비교만**. 정밀화 필요.
- OHLC 트레일링 정밀(`portfolio_daily_simulator_5_24.simulate_trailing_with_ohlc` 재사용) + 재진입(`reentry_shadow`) + 미투입현금 정밀 → 절대수익 신뢰화.
- nightly ⑤-4 매일 표본 축적 → breadth 효과 통계 확정(7코호트→2주+).

## 2순위 — 끼/명분 게이트 결합 (흑자엔 "언제+뭘" 둘 다)

- breadth = "언제 사나"(시장), 끼/명분(`catalyst_scanner`) = "뭘 사나"(종목). 결합 시뮬 → 흑자 전환 시도.
- breadth는 손실 줄이기지 돈 벌기 아님(d3 여전히 마이너스). 종목 선정과 결합해야 흑자.

## 3순위 — 정리/수정 (Medium)

- 공매도/신용 기각 게이트 paper_rule_shadow 정리(반증기록 유지 vs 제거 — 노이즈).
- kr_us_shock observer 첫 공급분 키 실측 후 파싱 확정. NXT 게이트 breadth 보류 intent 기록(추적).

## ★ 6/27 전체검수 결과 (4관점) — 반영할 것

- **종합 PASS**: 안전/라이브격리/버그·로직/SAJANG·영구룰 전부 PASS. Critical 0·라이브 영향 0·SAJANG 우회 0·불변식 충족. 배포 안전.
- **High 2(공통, flip 전 해소)**:
  - ① paper_sim `_stock_pnl` MFE 익절 낙관편향 = **버그 아님**(baseline/gated 동일편향·상대비교 유효·코드 note 해석금지 명시). → 1순위 OHLC 정밀화로 절대수익 신뢰화.
  - ② KIS 공매도/신용 필드명 = **이미 노트북+VPS 실측 완료**(공매도 99.7%·신용 98.4% 실제값). docstring "추정" 표현만 "실측 확정"으로 정리함(6/27). 월요일 전체 적재 재확인만.
- **슬러지(3순위 정리)**:
  - flow_collector 데드함수 3 (`collect_short_balance`/`collect_short_volume`/`_try_pykrx_short_balance` — KRX중단). ★호출처(collect_all_flow 등) 있어 stub 유지 안전. **호출처를 collect_short_sale(KIS)로 교체** 필요.
  - paper_rule_shadow 기각 게이트 4 = 반증기록 유지(주석 강화함 6/27).
  - Kiwoom v1.0 726줄 = 경고표기됨. DEAD_KIWOOM.md 존재 확인.
- **Medium**: ranking foreign_inst dedup이 code 필드 의존(fetch가 code 주는지 확인) / paper_gate_shock lru_cache stale(통합러너 시 주의) / credit FID 날짜범위 비대칭(1행만 쌓일 수 있음).

## 결정 대기 (사장님)

- **봇 가동 여부** — 실매매·NXT 페이퍼 모두 봇 OFF라 멈춤. breadth 게이트 실효과는 봇 켜야(또는 페이퍼 전용 루프로 계속 검증).
- breadth/끼·명분 **라이브 flip** = 표본 확정 + 사장님 승인 후(관측없이 flip 금지).
