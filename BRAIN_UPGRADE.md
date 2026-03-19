# BRAIN_UPGRADE.md
# Market Brain 업그레이드 체크리스트
# 
# 사용법: Claude Code 세션 시작 시 이 파일을 읽고,
#         [ ] 미완료 항목 중 가장 위의 것부터 진행.
#         완료되면 [x]로 체크하고 커밋.
#         중간에 다른 지시가 오면 중단 → 다음 세션에서 이어서.
#
# 원칙:
# - 각 항목은 독립적. 순서가 바뀌어도 동작함.
# - 각 항목은 1시간 이내 완료 가능한 크기.
# - 각 항목 완료 즉시 효과가 있음 (반쪽짜리 아님).
# - 기존 코드 깨뜨리지 않음. 추가/수정만.

---

## FIX-01: BRAIN 실행 순서 뒤집기 (치명적)
상태: [x]
파일: morning_recommendation.py
문제: save_recommendation() 후에 generate_brain_report() 호출
     → 추천이 먼저, BRAIN이 나중 → BRAIN 판단이 추천에 반영 안 됨
수정:
  1. generate_brain_report()를 save_recommendation() 이전으로 이동
  2. brain_report 결과(verdict, pct)를 recommendation 파이프라인에 전달
  3. pct를 morning_recommendation의 최종 종목 수 제한에 반영
     - 100%: 최대 8종목
     - 70%: 최대 5종목  
     - 50%: 최대 3종목
     - 30%: 최대 1종목
     - 0%: 추천 0종목 (관망)
테스트: BRAIN verdict가 "방어 50%"일 때 추천 종목이 3개 이하인지 확인
효과: BRAIN이 진짜 추천을 제어하게 됨 (지금은 장식)

---

## FIX-02: brain_report ↔ brain_allocation 통합 (치명적)
상태: [x]
파일: market_brain.py, auto_trader.py
문제: market_brain이 brain_report.json 생성
     auto_trader가 jarvis/brain/brain_allocation.json 사용
     → 두 파일이 별개 → BRAIN 판단과 실제 매매가 따로 놈
수정:
  1. market_brain.py의 generate_brain_report() 마지막에
     brain_allocation.json도 같이 갱신하도록 추가
  2. allocation_krw.bh_swing = 총자산 × brain_pct × 가용비율
  3. cross_signal.max_positions_cap = pct별 종목 수 (FIX-01과 일치)
  4. auto_trader의 _load_brain_allocation()이 이 파일을 읽는 것 확인
테스트: BRAIN "관망 0%" → auto_trader 매수 0건인지 확인
효과: BRAIN 판단 = 실제 매매 비중 (지금은 불일치 가능)

---

## FIX-03: 원자재 Phase 실제 반영 (누락)
상태: [x]
파일: market_brain.py (_phase6_synthesis)
문제: Phase 2 commodity 결과가 Phase 6 점수 계산에 미사용
     구리 5% 폭등해도 BRAIN 비중에 0% 영향
수정: Phase 6 score 계산에 추가:
  - active_signals 2개+ → +8점 (원자재 모멘텀)
  - active_signals 1개 → +3점
  - relay 발동 (GOLD→SILVER→COPPER) → +5점
  - 원자재 전면 하락 → -5점
  코드 위치: _phase6_synthesis() 내 score 계산부 (L670 부근)
  추가할 라인:
    # 원자재 반영
    n_active = len(commodity.active_signals)
    if n_active >= 2: score += 8
    elif n_active >= 1: score += 3
    if commodity.relay_stage not in (None, 'NONE'): score += 5
    if commodity.narrative and '하락' in commodity.narrative: score -= 5
테스트: 구리+유가 동시 급등 시 score에 +8 반영되는지
효과: 원자재 사이클이 실제 투자 비중에 영향

---

## FIX-04: insights.json BRAIN 연결 (단절)
상태: [x]
파일: market_brain.py
문제: _load_insights() 선언만 있고 미사용
     daily_learner가 매일 학습하는데 BRAIN에 안 들어옴
수정:
  1. generate_brain_report() 시작 시 insights.json 로드
  2. Phase 6에서 활용:
     - 적중률 높은 섹터(70%+) → 해당 섹터 HOT일 때 score +3 부스트
     - 적중률 낮은 섹터(30%-) → 해당 섹터 HOT이어도 score 부스트 제거
     - 전체 적중률 50% 미만 → score -5 (시스템 자체 신뢰도 하락)
  3. brain_report.json에 "insights_applied" 필드 추가 (투명성)
테스트: insights에 "반도체 hit_rate 80%" → 반도체 HOT 시 score +3 확인
효과: 어제 학습 결과가 오늘 BRAIN 판단에 반영 (자기 학습 시작)

---

## FIX-05: 지수 기술적 분석 추가 (맹점)
상태: [x]  
파일: market_brain.py (_phase1_macro 또는 신규 헬퍼)
문제: 코스피/코스닥 MACD, 이동평균, RSI 없음
     지수가 20일선 깨고 내려가도 BRAIN 감지 못함
수정:
  1. pykrx로 코스피/코스닥 일봉 최근 60일 가져오기
     (이미 다른 모듈에서 pykrx 사용 중이므로 의존성 추가 없음)
  2. 계산: 5일/20일/60일 이동평균 + MACD(12,26,9) + RSI(14)
  3. Phase 1에 지수 상태 추가:
     - 코스피 > 20일선 + MACD 양전환 → index_bull = True
     - 코스피 < 20일선 + MACD 음전환 → index_bear = True
     - RSI < 30 → index_oversold (극단적 과매도 = 반등 가능)
     - RSI > 70 → index_overbought (과열 주의)
  4. Phase 6 score에 반영:
     - index_bull → +5
     - index_bear → -10
     - index_oversold → +3 (역발상)
     - index_overbought → -5
테스트: 코스피 20일선 하회 시 score -10 반영되는지
효과: 시장 방향을 VIX뿐 아니라 실제 지수 추세로도 판단

---

## FIX-06: 시장 전체 수급 추가 (맹점)
상태: [x]
파일: market_brain.py (_phase4_flow 또는 신규 헬퍼)
문제: 개별 종목 flow_signal만 집계. 시장 전체 기관/외인 수급 안 봄
수정:
  1. pykrx 또는 KIS API로 코스피/코스닥 투자자별 매매동향 가져오기
     - 기관 순매수 (당일 + 5일 누적)
     - 외국인 순매수 (당일 + 5일 누적)
     - 개인 순매수 (당일)
  2. Phase 4에 시장 레벨 수급 추가:
     - 기관+외인 동반 순매수 5일 연속 → market_flow_bull
     - 기관+외인 동반 순매도 3일 연속 → market_flow_bear
     - 개인만 순매수 + 기관/외인 매도 → retail_panic_buy (위험 신호)
  3. Phase 6 score에 반영:
     - market_flow_bull → +8
     - market_flow_bear → -8
     - retail_panic_buy → -5
테스트: 기관+외인 5일 연속 순매수 시 score +8 확인
효과: "시장 전체에 돈이 들어오는가 빠지는가" 판단 가능

---

## FIX-07: 장중 BRAIN 갱신 — 긴급 모드 (실시간 대응)
상태: [x]
파일: market_brain.py (신규 함수), telegram_bot.py
문제: BRAIN 하루 1회 고정. VIX 30 돌파해도 아침 판단 유지
수정:
  1. market_brain.py에 emergency_reassess() 함수 추가
     - VIX만 실시간 체크 (yfinance, 이미 nightwatch에서 사용 중)
     - 트리거 조건:
       a. VIX 30+ 돌파 (현재 brain 판단 시 VIX < 25였을 때)
       b. 원달러 1510+ 돌파
       c. S&P 선물 -2% 이상 급락
     - 트리거 시: Phase 5(리스크)만 재계산 → Phase 6 score 재산출
       (Phase 1~4는 재계산 불필요 — 리스크만 급변했으므로)
     - 결과: brain_report.json + brain_allocation.json 즉시 갱신
  2. telegram_bot.py의 기존 30초 job_monitor에 VIX 체크 추가
     - VIX 현재값을 nightwatch 캐시에서 읽기 (추가 API 호출 없음)
     - 트리거 시 emergency_reassess() 호출 + 긴급 알림 전송
  3. auto_trader에서 brain_allocation 재로드 (다음 매수 사이클에 반영)
테스트: VIX를 임의로 35로 설정 → BRAIN "관망" 전환 + 신규 매수 중단 확인
효과: 장중 위기 시 실시간 방어 (지금은 무방비)
주의: Phase 1~4 전체 재계산이 아님. Phase 5 리스크만 빠르게 재산출.
      무거운 작업 아님 — 숫자 5개 비교하고 점수 다시 더하는 것.

---

## FIX-08: BRAIN 자기 학습 (성과 추적)
상태: [x]
파일: daily_learner.py (추가), market_brain.py (로드)
문제: BRAIN 판단의 적중률을 아무도 안 봄.
     "공격 100% 줬는데 시장 -3%" → 반영 안 됨
수정:
  1. daily_learner.py에 brain_performance_check() 추가
     - 당일 BRAIN verdict (공격/표준/방어/최소/관망)
     - 당일 코스피 등락률
     - 당일 추천 종목 평균 수익률
     - 기록: brain_performance.json (30일 롤링)
     예시:
     {"date": "2026-03-19", "verdict": "표준70%", "score": 18,
      "kospi_change": -1.2, "rec_avg_return": -0.8, "correct": false}
  2. 적중 판단:
     - 공격/표준 + 시장 상승 → correct
     - 방어/관망 + 시장 하락 → correct  
     - 공격 + 시장 -2% 이상 → bad_call
     - 관망 + 시장 +2% 이상 → missed_opportunity
  3. FIX-04의 insights 연결과 합쳐서:
     - BRAIN 적중률 < 40% (최근 10일) → Phase 6 score에 -5 자동 적용
     - BRAIN 적중률 > 70% → +3
  4. 마감 리포트에 "BRAIN 적중률 62% (최근 10일)" 한 줄 추가
테스트: brain_performance.json에 10일치 기록 확인
효과: BRAIN이 자기가 잘했는지 못했는지 안다 → 보수적/공격적 자동 조절

---

# 완료 기록
# 각 FIX 완료 시 아래에 날짜 + 한줄 메모 추가
#
# 2026-03-19 FIX-01 완료 — BRAIN→추천 순서 뒤집기, 종목 수 캡 적용 (pct별 8/5/3/1/0)
# 2026-03-19 FIX-02 완료 — brain_allocation.json 통합, 관망 시 _block_all_buys 차단
# 2026-03-19 FIX-03 완료 — 원자재 Phase 6 반영 (active+8/relay+5/하락-5)
# 2026-03-19 FIX-04 완료 — insights.json → Phase 6 적중률/섹터부스트 연결
# 2026-03-19 FIX-05 완료 — 코스피 지수 기술적 분석 (MA/MACD/RSI → Phase 6)
# 2026-03-19 FIX-06 완료 — 시장 전체 기관/외인 수급 (pykrx → Phase 6)
# 2026-03-19 FIX-07 완료 — 장중 emergency_reassess() + telegram_bot 30분 VIX 감시
# 2026-03-19 FIX-08 완료 — brain_performance_check() + Phase 6 적중률±5/+3 반영
