# TIER1_ENHANCEMENTS.md
# 단타봇 Tier 1 강화 체크리스트
#
# 사용법: Claude Code 세션 시 이 파일 읽고 미완료 항목부터 진행.
#
# 1순위: 뉴스 감성 실시간 (기존 모듈 확장)
# 2순위: 공매도 잔고 스코어링 연결 (기존 데이터 활용)
# 3순위: 옵션/선물 시그널 (신규)

---

## ===== 1순위: 뉴스 감성 실시간 =====
##
## 현재: news_ai_scanner.py가 Claude API로 감성분석 → 장후 1회 배치
## 문제: 악재 뉴스는 수급보다 빠름. 장중에 감지 못하면 SL까지 기다려야 함
## 해결: 보유종목 뉴스를 장중에도 감시 → 부정 감성 → 즉시 경고

## NEWS-01: 장중 뉴스 크롤링 스케줄 추가
상태: [ ]
파일: telegram_bot.py + news_collector.py (또는 news_ai_scanner.py)
내용:
  현재 뉴스 수집: 온디맨드 (장중 수동 호출만)
  변경: 보유종목 + 감시종목 대상 자동 크롤링

  스케줄 추가:
  ```
  09:30  장중 뉴스 1차 (장 시작 직후 뉴스 체크)
  11:00  장중 뉴스 2차
  13:30  장중 뉴스 3차
  ```
  
  대상: 보유종목 + 당일 매수 후보 (최대 10종목)
  소스: 네이버 증권 뉴스 (기존 news_collector.py 재사용)
  
  수집만 하고 분석은 NEWS-02에서.
테스트: 09:30에 보유종목 뉴스 자동 수집 확인
효과: 장중 뉴스 데이터 확보

---

## NEWS-02: 장중 뉴스 감성분석 + Kill-Switch
상태: [ ]
파일: news_ai_scanner.py + position_guardian.py
내용:
  1. 장중 수집된 뉴스에 대해 감성분석 실행
     - 기존 news_ai_scanner.py의 Claude API 호출 재사용
     - 단, 장중이므로 빠르게: 헤드라인 + 요약만 분석 (본문 X)
     - 프롬프트: "이 뉴스가 {종목명} 주가에 긍정/부정/중립? -1.0~+1.0 점수"

  2. Kill-Switch 로직:
     ```python
     def check_news_kill_switch(self, stock_code, news_list):
         """
         보유종목 관련 뉴스 감성 체크
         부정 감성 -0.7 이하 → Position Guardian에 긴급 시그널
         """
         for news in news_list:
             sentiment = self._analyze_headline(news['title'], stock_code)
             
             if sentiment <= -0.7:
                 # 강한 부정 → Guardian에 즉시 통보
                 return {
                     'action': 'KILL_SWITCH',
                     'stock_code': stock_code,
                     'headline': news['title'],
                     'sentiment': sentiment,
                     'recommendation': 'EXIT 검토'
                 }
             elif sentiment <= -0.4:
                 # 중간 부정 → 경고만
                 return {
                     'action': 'WARNING',
                     'stock_code': stock_code,
                     'headline': news['title'],
                     'sentiment': sentiment,
                     'recommendation': '주시'
                 }
         
         return {'action': 'CLEAR'}
     ```

  3. Position Guardian 연동:
     - Kill-Switch 발동 시 Guardian risk_score에 +25점 추가
     - Guardian 기존 임계: EXIT 60점, REDUCE 35점
     - 뉴스 악재 +25점 → 기존 리스크 35점이면 → 60점 = EXIT
     - 즉, 뉴스 악재 하나로 "주시" → "청산"으로 전환 가능

  4. AI Eye 연동:
     - Kill-Switch 발동 시 AI Eye에도 전달
     - Eye가 ALIVE 판정이어도 뉴스 악재면 → WEAKENING으로 강제 전환
테스트: 테스트 뉴스 "삼성전자 반도체 수율 문제" → 감성 -0.8 → Kill-Switch
효과: 뉴스 악재 시 수급 데이터 반응 전에 선제 대응

---

## NEWS-03: 뉴스 Kill-Switch 텔레그램 알림
상태: [ ]
파일: telegram_bot.py
내용:
  Kill-Switch 발동 시 긴급 알림 (텔레그램 재설계 원칙 유지):

  ```
  🚨 [뉴스 경고] SK하이닉스 — 11:02
     "HBM 수율 이슈로 생산 차질 우려" (감성 -0.78)
     → Guardian +25점 → EXIT 검토 중
  ```

  WARNING 레벨은 텔레그램 안 보냄 (로그만).
  KILL_SWITCH 레벨만 긴급 알림.
테스트: Kill-Switch 발동 → 텔레그램 1개
효과: 핵심 악재만 즉시 알림

---

## NEWS-04: 뉴스 감성 → 스코어링 반영
상태: [ ]
파일: morning_recommendation.py
내용:
  기존 뉴스 AI 점수(MAX 15점)를 장중 뉴스로도 업데이트:

  1. 아침 추천 시: 전일 뉴스 감성 반영 (기존대로)
  2. 프리클로즈(14:30) 시: 당일 장중 뉴스 감성 추가 반영
     - 당일 부정 뉴스 2건+ → 프리클로즈 후보에서 제외
     - 당일 긍정 뉴스 2건+ → 프리클로즈 점수 +5 부스트

  3. Market Journal에 뉴스 원인 태깅 연동:
     - 급등주 원인에 NEWS_POSITIVE 태그
     - 급락주 원인에 NEWS_NEGATIVE 태그
테스트: 당일 부정 뉴스 2건 종목 → 프리클로즈 제외 확인
효과: 뉴스가 매매 판단에 실시간으로 반영

---

## ===== 2순위: 공매도 잔고 스코어링 연결 =====
##
## 현재: pykrx로 공매도 잔고 수집 중 (전수조사 확인)
## 문제: 수집만 하고 스코어링에 안 씀
## 해결: 공매도 잔고 변화를 점수화 → 추천/회피 반영

## SHORT-01: 공매도 잔고 지표 계산
상태: [ ]
파일: 신규 `short_analyzer.py` 또는 기존 수집 모듈 확장
내용:
  1. 종목별 공매도 잔고 지표:
     ```python
     def analyze_short_interest(self, stock_code) -> dict:
         """
         공매도 잔고 분석
         """
         # (1) 공매도 잔고율 (전체 상장주식 대비)
         #     잔고율 5%+ → HIGH_SHORT (주의)
         #     잔고율 2~5% → MODERATE_SHORT
         #     잔고율 2%- → LOW_SHORT
         
         # (2) 잔고 변화 추이 (5일)
         #     5일 연속 증가 → SHORT_BUILDING (기관 하락 베팅 중)
         #     5일 연속 감소 → SHORT_COVERING (숏커버링 = 매수 시그널)
         #     횡보 → NEUTRAL
         
         # (3) 대차잔고 변화 (기관 공매도 준비)
         #     대차잔고 급증(20%+) → SHORT_PREPARING
         #     대차잔고 급감(-20%) → SHORT_UNWINDING
         
         return {
             'short_ratio': 3.2,          # 공매도 잔고율 %
             'short_level': 'MODERATE',    # HIGH/MODERATE/LOW
             'short_trend': 'COVERING',    # BUILDING/COVERING/NEUTRAL
             'trend_days': 3,              # 추세 지속일
             'borrow_change': -15.2,       # 대차잔고 변화율 %
             'signal': 'BULLISH',          # BULLISH(숏커버)/BEARISH(숏빌딩)/NEUTRAL
             'score_adjustment': 5,        # 스코어링 반영 점수
         }
     ```

  2. 점수 반영 기준:
     ```
     SHORT_COVERING (숏커버링 중) → +5점 (강한 매수 시그널)
     SHORT_COVERING + 거래량 폭발 → +8점 (숏 스퀴즈 가능)
     SHORT_BUILDING (숏 빌딩 중) → -5점 (기관 하락 베팅)
     HIGH_SHORT + 잔고 증가 → -8점 (위험, 추천 제외 검토)
     LOW_SHORT → 0점 (영향 없음)
     ```
테스트: 공매도 잔고율 5%+ 종목 → -8점 반영 확인
효과: 기관이 하락 베팅하는 종목 회피

---

## SHORT-02: 스코어링 연동
상태: [ ]
파일: morning_recommendation.py + preclose_scanner.py
내용:
  1. _calculate_total_score()에 공매도 점수 추가:
     ```python
     # 기존 점수 요소에 추가
     short_result = short_analyzer.analyze_short_interest(stock_code)
     short_score = short_result['score_adjustment']
     # total_score에 반영
     ```

  2. 진입 필터 추가:
     - SHORT_BUILDING + HIGH_SHORT → 추천 목록에서 제외 (Hard Filter)
     - SHORT_COVERING → 우선 추천 (부스트)

  3. Trade Object에 공매도 정보 추가:
     - trade.short_signal = 'COVERING' / 'BUILDING' / 'NEUTRAL'
     - 텔레그램 모닝 브리프에 표시:
       "제일일렉트릭 87점 A+ | R:R 2.21 | 숏커버 ✅"
       "에코프로 62점 B | R:R 1.3 | 숏빌딩 ⚠️ → REJECT"
테스트: 숏커버링 종목에 +5점, 숏빌딩 종목에 -5점
효과: 공매도 데이터가 실제 매매 판단에 반영

---

## SHORT-03: Market Journal 연동
상태: [ ]
파일: market_journal.py
내용:
  1. 일일 저널에 공매도 섹션 추가:
     - 공매도 잔고 급증 TOP 5 (새로 숏 포지션 잡는 종목)
     - 공매도 잔고 급감 TOP 5 (숏커버링 진행 종목)
  
  2. 원인 태깅에 SHORT_SQUEEZE 추가:
     - 급등주 중 공매도 잔고율 5%+ → SHORT_SQUEEZE 태그

  3. 패턴 누적:
     - "SHORT_COVERING → 평균 +4.8% (3일)" 같은 통계
테스트: 일지에 공매도 섹션 표시
효과: 숏커버링 패턴의 수익성을 데이터로 축적

---

## ===== 3순위: 옵션/선물 시그널 =====
##
## 신규 데이터 소스 확보 필요
## NXT의 VIX 의존도를 낮추고 한국 옵션 데이터로 보완

## OPT-01: 데이터 소스 확보
상태: [ ]
파일: 신규 `options_signal.py`
내용:
  1. KRX/pykrx에서 가져올 수 있는 옵션 데이터 확인:
     - KOSPI200 옵션 거래량 (콜/풋)
     - Put/Call Ratio 계산
     - 외국인 선물 순매수 포지션
  
  2. KIS API에서 가져올 수 있는 데이터 확인:
     - 선물 미결제약정
     - 외국인 선물 순매수
  
  3. 데이터 수집 함수:
     ```python
     def collect_options_data(self) -> dict:
         """
         일일 옵션/선물 데이터 수집
         """
         return {
             'pc_ratio': 1.25,           # Put/Call 비율
             'pc_level': 'FEAR',          # GREED(<0.8) / NEUTRAL / FEAR(>1.2)
             'foreign_futures_net': -5200, # 외인 선물 순매수 (계약수)
             'foreign_futures_trend': 'SELLING',  # BUYING/SELLING/NEUTRAL
             'open_interest_change': 3.2,  # 미결제약정 변화율 %
         }
     ```
테스트: pykrx/KIS에서 옵션 데이터 수집 가능 여부 확인
효과: 한국 옵션 시장 데이터 확보

---

## OPT-02: 옵션 시그널 → NXT 연동
상태: [ ]
파일: nightwatch.py + options_signal.py
내용:
  1. NXT score에 한국 옵션 시그널 반영:
     ```python
     # korea_strength에 옵션 데이터 추가
     
     # P/C Ratio
     if pc_ratio >= 1.5:
         score += 1.0   # 극단 공포 = 역발상 매수 (과매도)
     elif pc_ratio >= 1.2:
         score -= 0.5   # 공포 = 주의
     elif pc_ratio <= 0.7:
         score -= 1.0   # 극단 탐욕 = 과열 주의
     
     # 외인 선물
     if foreign_futures_trend == 'BUYING' and net > 3000:
         score += 1.0   # 외인 선물 대량 매수 = 상승 전망
     elif foreign_futures_trend == 'SELLING' and net < -3000:
         score -= 1.0   # 외인 선물 대량 매도 = 하락 전망
     ```

  2. VIX 의존도 감소:
     - 기존: VIX 25 → divergence -2.0 (NXT-03에서 -1.5로 캘리브레이션)
     - 추가: P/C Ratio가 NEUTRAL이면 VIX 영향 추가 감소
       "VIX는 미국 공포인데, 한국 옵션은 공포 아니면 → 한국은 괜찮다"
테스트: P/C Ratio 1.5 + VIX 25 → NXT가 과민반응 안 하는지
효과: NXT가 VIX 대신 한국 자체 심리를 반영

---

## OPT-03: BRAIN Phase 신규 추가 (옵션 심리)
상태: [ ]
파일: market_brain.py
내용:
  Phase 2.5 또는 Phase 5에 옵션 심리 통합:

  ```python
  # P/C Ratio → 시장 심리 판단
  # 외인 선물 → 방향성 판단
  # → Phase 6 score에 ±5점 반영
  
  if pc_level == 'EXTREME_FEAR' and pc_ratio >= 1.5:
      score += 5   # 역발상: 극단 공포 = 바닥 근처
  elif pc_level == 'FEAR':
      score -= 3   # 공포: 보수적
  elif pc_level == 'EXTREME_GREED' and pc_ratio <= 0.6:
      score -= 5   # 극단 탐욕: 과열 위험
  ```
테스트: P/C Ratio 극단값 → BRAIN score 반영
효과: BRAIN이 옵션 시장 심리까지 판단

---

# 완료 기록
#
