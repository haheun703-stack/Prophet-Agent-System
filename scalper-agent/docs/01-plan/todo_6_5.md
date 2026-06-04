# 6/5(금) TODO — 6/4 밤 인계

> 6/4 작업 마무리 후 인계. 봇 OFF·매매 무관·SAJANG 무변경 유지 중.
> 통합 지시서: `paper-training-accelerated-directive-6_4.md`(사장님 퀀트봇 세션 작업트리, 이 노트북엔 없음).

## 0. 어제(6/4) 완료한 것
- **수급 8종 6/4 채움**: 일봉(pykrx 막힘 → KIS `collect_daily_kis` 전환, `tools/fill_daily_kis_incremental.py`, 커버 95.3%) · 통짜 step6 sync(3645) · 시장 11주체 양경로(self-heal) · 국적별 209 · 종목수급 99%.
- **foreign_exh ghost 대응**: 휴장 ghost 22,860행 제거(`tools/fix_foreign_exh_ghost.py`, 6/3 선거휴장 + 누적 주말) → 6/4 force 재수집 self-heal(005930 = 47.81/351500 진짜값 확인).
- **전달서 검토**: 수급 = 11주체 fact layer(시장레벨만, 종목은 4주체뿐=키움 대기), 기관합계 hard gate 금지, divergence 관측.
- **day_trade_bot.py 정체**: 사장님 퀀트봇 세션 프로토타입(안전·실주문0)이나 수급=hard gate(REQUIRE_SUPPLY=True)라 **채택금지 = 참고 프로토타입**. → `project_day_trade_bot_prototype_6_4` 메모리.

## 1. ★ foreign_exh force 재수집 완료 검증 (어제 백그라운드)
- 전종목 6/4 진짜값 갱신 전수 확인 (`logs/foreign_exh_refetch_6_4.log`)
- **휴장 ghost 0 전수 재확인** (지시서 "유령행 0")

## 2. ★ 근본 코드 fix (B) — collect_foreign_exhaustion 휴장가드 + 종가확정 가드
- 현재 결함: 현재가 스냅샷 API + `datetime.now()`로 날짜 찍음 → 휴장/장전 수집 시 ghost.
- fix: ①`is_trading_day` 휴장가드(휴장일 행 추가 금지) ②장 마감 전(종가 미확정)엔 today 행 생성 금지 — 또는 현재가 API 응답의 거래일 필드 활용.
- **안 고치면 다음 주말 ghost 재발.** 4-Tier + 사장님 승인.

## 3. paper training 6/5 시작 선결조건 (지시서)
- 퀀트봇: processed 지표 재동기화, C60 BULL 재확인, 유령행 0
- 단타봇: A/B/C paper ledger 분리, MFE/MAE/회피사유 기록 확인
- 공통: 실주문 0, scheduler/SAJANG 변경 금지

## 4. 3-Type paper training 구현 (6/3 설계 c1aa4d3, `docs/02-design/paper_training_3type_6_3.md`)
- ①PaperPortfolio ledger ②B(ROTATION_PULLBACK) ③C(ROTATION_RIDE) ④EVENT hook
- 각 4-Tier · 게이트 8/8 · 주문 0 · SAJANG 무변경 · scheduler 금지
- 매일 수동 step6 sync(pykrx 막힘 동안 KIS fill 도구 사용) → 3타입 스캔

## 5. 6/10 실전 조건 (지시서)
- 6/9까지 paper 유효샘플 + 안전검수 PASS + 수급 hard gate 미사용 + 사장님 승인 후 **소액만**

## 운용 철학 (지시서)
- 퀀트봇 = 밸류 바닥 + 주봉/일봉 반등 확인(방어형)
- 단타봇 = 섹터/그룹 로테이션 눌림·올라타기(수익형)
- **단타로 벌고, 퀀트로 방어.**
