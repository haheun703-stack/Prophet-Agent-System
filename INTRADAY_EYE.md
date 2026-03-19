# INTRADAY_EYE.md
# 장중 AI Eye 엔진 체크리스트
#
# 사용법: Claude Code 세션 시작 시 이 파일을 읽고,
#         [ ] 미완료 항목 중 가장 위의 것부터 진행.
#
# 목표: 보유종목이 "살아있나 죽어가나"를 장중 실시간 판단
# 원칙: 1분봉 + 수급 + 거래량 → 5분마다 상태 판정 → 액션

---

## EYE-01: IntradayEye 기본 클래스 + 이평선 계산
상태: [ ]
파일: 신규 `intraday_eye.py`
내용:
  1. IntradayEye 클래스 생성
     - 입력: stock_code, 1분봉 Parquet 경로
     - 1분봉 로드 → 5분봉/15분봉/60분봉 리샘플링
  
  2. calc_intraday_ma() 구현
     - 5분봉 기준 EMA(5) = 약 25분 이평
     - 5분봉 기준 EMA(12) = 약 60분 이평
     - 5분봉 기준 EMA(48) = 약 240분(하루) 이평
     - 정배열 판정: EMA5 > EMA12 > EMA48
     - 역배열 판정: EMA5 < EMA12 < EMA48
     - 수렴 판정: 3개 EMA 간 거리가 0.3% 이내
  
  3. 출력:
     {
       'ema_5': 52340,
       'ema_12': 52180,
       'ema_48': 51900,
       'alignment': 'BULLISH',  # BULLISH / BEARISH / CONVERGING
       'ema_spread': 0.85,       # EMA5 vs EMA48 괴리율 %
     }
테스트: 오늘 1분봉으로 이평 계산 → 차트 대비 검증
효과: 장중 추세 방향을 실시간으로 판단 가능

---

## EYE-02: VWAP 위치 판단
상태: [ ]
파일: intraday_eye.py
내용:
  1. calc_vwap_position() 구현
     - VWAP = Σ(가격 × 거래량) / Σ(거래량) — 장 시작부터 누적
     - 현재가 vs VWAP 위치:
       above_vwap = (현재가 - VWAP) / VWAP × 100
     - VWAP 밴드: VWAP ± 1σ (거래량 가중 표준편차)
  
  2. VWAP 상태 판정:
     - 현재가 > VWAP + 1σ → STRONG_ABOVE (강한 매수 우위)
     - 현재가 > VWAP → ABOVE (매수 우위)
     - 현재가 ≈ VWAP (±0.3%) → AT_VWAP (균형)
     - 현재가 < VWAP → BELOW (매도 우위)
     - 현재가 < VWAP - 1σ → STRONG_BELOW (강한 매도 압력)
  
  3. VWAP 추이 (이전 판단 대비):
     - VWAP 위 → VWAP 위 유지 = "매수 유지"
     - VWAP 위 → VWAP 하회 = "이탈 경고" (중요!)
     - VWAP 아래 → VWAP 돌파 = "전환 신호" (중요!)
  
  4. 출력:
     {
       'vwap': 52100,
       'vwap_upper': 52450,  # +1σ
       'vwap_lower': 51750,  # -1σ
       'position': 'ABOVE',
       'distance_pct': 0.46,
       'transition': 'NONE',  # NONE / BREAK_ABOVE / BREAK_BELOW
     }
테스트: VWAP 계산값 vs 증권사 차트 VWAP 비교
효과: 봇이 VWAP을 진입뿐 아니라 보유 판단에도 활용

---

## EYE-03: 장중 지지/저항 동적 계산
상태: [ ]
파일: intraday_eye.py
내용:
  1. calc_support_resistance() 구현
     방법: 볼륨 프로파일 기반
     - 오늘 1분봉의 (가격, 거래량) 데이터
     - 가격을 호가 단위로 그룹핑
     - 각 가격대의 누적 거래량 계산
     - 거래량 상위 3개 가격대 = "볼륨 노드" (지지/저항 후보)
  
  2. 지지/저항 판정:
     - 볼륨 노드 중 현재가 아래 → 지지선
     - 볼륨 노드 중 현재가 위 → 저항선
     - 오늘 고점 = 저항
     - 오늘 저점 = 지지
     - 시가 = 참조선
  
  3. 터치 횟수 추적:
     - 특정 가격대에 N번 접근(±0.2%) 후 반등 → 강한 지지
     - 특정 가격대에 N번 접근 후 밀림 → 강한 저항
  
  4. 출력:
     {
       'supports': [{'price': 52000, 'strength': 'STRONG', 'touches': 3}],
       'resistances': [{'price': 52800, 'strength': 'MODERATE', 'touches': 1}],
       'today_high': 52900,
       'today_low': 51800,
       'open': 52100,
     }
테스트: 장중 지지/저항이 실제 반등/밀림 지점과 일치하는지
효과: "52,000에서 3번 튕겼다 = 강한 지지" 판단 가능

---

## EYE-04: 거래량/수급 흐름 추적
상태: [ ]
파일: intraday_eye.py
내용:
  1. calc_volume_flow() 구현
     - 시간대별 거래량 추이 (30분 윈도우 롤링)
       recent_30min_vol vs prev_30min_vol → 가속도
     - 체결강도 추이 (KIS API tday_rltv)
       100 이상 = 매수우위, 100 미만 = 매도우위
     - 체결강도 방향: 30분 전 대비 상승/하락
  
  2. calc_supply_flow() 구현
     - 기관/외인 당일 누적 순매수 (flow_collector.py 활용)
     - 시간대별 누적 변화:
       10시: +20억 → 11시: +35억 → 12시: +28억
       = "11시까지 매수 가속 → 12시 매수 감속"
     - 외인 소진율 추이 (KIS hts_frgn_ehrt)
  
  3. 종합 수급 상태:
     - INFLOW: 거래량↑ + 체결강도>100 + 기관순매수↑
     - OUTFLOW: 거래량↑ + 체결강도<100 + 기관순매도
     - DRYING: 거래량↓ + 체결강도 중립 → "관심 소진"
     - ACCUMULATION: 거래량 보합 + 기관순매수↑ → "조용한 매집"
  
  4. 출력:
     {
       'volume_acceleration': 1.35,     # 최근30분 / 이전30분
       'volume_trend': 'INCREASING',    # INCREASING / STABLE / DECREASING
       'strength': 112.5,               # 체결강도
       'strength_trend': 'RISING',      # RISING / FALLING / STABLE
       'inst_net': 3500000000,          # 기관 순매수 (원)
       'inst_trend': 'ACCELERATING',    # ACCELERATING / DECELERATING / REVERSING
       'flow_state': 'INFLOW',          # INFLOW / OUTFLOW / DRYING / ACCUMULATION
     }
테스트: 수급 INFLOW 판정 시 실제 가격 상승 동반 확인
효과: "기관이 10시부터 꾸준히 사고 있다" 판단 가능

---

## EYE-05: 모멘텀 종합 판단
상태: [ ]
파일: intraday_eye.py
내용:
  1. calc_momentum() 구현
     - 장중 RSI(14) on 5분봉
     - 장중 MACD(5,13,4) on 5분봉 (단기 세팅)
     - 가격 가속도: 최근 15분 변화율 vs 이전 15분 변화율
  
  2. 모멘텀 상태:
     - ACCELERATING: RSI↑ + MACD 확대 + 가격가속↑
     - STEADY: RSI 50~70 + MACD 양수 유지
     - DECELERATING: RSI↓ + MACD 축소 (아직 양수)
     - REVERSING: MACD 데드크로스 or RSI < 40
  
  3. 출력:
     {
       'rsi_5min': 62.3,
       'macd_histogram': 45,     # 양수=강세, 음수=약세
       'macd_trend': 'EXPANDING', # EXPANDING / CONTRACTING / CROSSING
       'price_acceleration': 0.15, # 양수=가속, 음수=감속
       'momentum_state': 'STEADY',
     }
테스트: 모멘텀 REVERSING 판정 후 실제 가격 하락 확인
효과: "힘이 빠지기 시작했다"를 숫자로 감지

---

## EYE-06: 종합 판정 엔진 (synthesize)
상태: [ ]
파일: intraday_eye.py
내용:
  1. synthesize() — 위 5개 레이어 종합
  
  판정 로직:
  
  ALIVE (살아있다 — 홀드/추가):
    조건: 정배열 + VWAP 위 + INFLOW + STEADY 이상
    의미: 추세 살아있고, 돈 들어오고, 모멘텀 유지
    액션: HOLD. 트레일링 SL 타이트하게 (고점 -1.5%)
  
  BREAKING (돌파 — 홀드 + 목표 상향):
    조건: 저항선 돌파 + 거래량 폭발(가속 1.5x+) + INFLOW
    의미: 새로운 레벨로 진입
    액션: HOLD. TP 상향. SL을 돌파 가격으로 올림
  
  WEAKENING (약해지는 중 — 주시):
    조건: 이평 수렴 + 거래량 감소 + DRYING
    의미: 힘이 빠지고 있지만 아직 무너지진 않음
    액션: WATCH. SL 유지. 다음 5분에 DYING 전환 시 청산 준비
  
  BOUNCING (반등 시작 — 관찰):
    조건: 지지선 터치(2회+) + 거래량 증가 + 체결강도 > 100
    의미: 지지 확인, 반등 가능성
    액션: HOLD. SL을 지지선 -0.5%로 설정
  
  DYING (죽어간다 — 청산):
    조건: 역배열 + VWAP 하회 + OUTFLOW + REVERSING
    의미: 추세 꺾이고, 돈 빠지고, 모멘텀 소멸
    액션: EXIT. 즉시 청산 또는 다음 반등에 청산
  
  2. 판정 신뢰도:
     - 5개 레이어 중 4개 이상 일치 → HIGH
     - 3개 일치 → MEDIUM
     - 2개 이하 → LOW (판단 보류, 현 상태 유지)
  
  3. 출력:
     {
       'stock_code': '005930',
       'timestamp': '2026-03-20 11:35:00',
       'state': 'ALIVE',
       'confidence': 'HIGH',
       'action': 'HOLD',
       'details': {
         'alignment': 'BULLISH',
         'vwap': 'ABOVE',
         'flow': 'INFLOW',
         'momentum': 'STEADY',
         'support_test': False,
       },
       'action_params': {
         'trailing_sl': 52100,  # 고점 -1.5%
         'tp_adjust': None,     # 변경 없음
       },
       'narrative': "정배열 + VWAP 위 + 기관매수 지속. 모멘텀 유지 중."
     }
테스트: 보유종목 5분마다 판정 → 실제 가격과 대조
효과: "이 종목 지금 살아있나 죽어가나" 실시간 판단

---

## EYE-07: Position Guardian 연동
상태: [ ]
파일: intraday_eye.py + position_guardian.py + auto_trader.py
내용:
  1. AI Eye 판정 → Position Guardian 입력으로 연결
     - 기존 Guardian: 일봉 데이터 기반, 하루 1~2회 판정
     - 신규: AI Eye 실시간 데이터가 Guardian에 추가 입력
     
     Guardian 리스크 점수에 AI Eye 반영:
       DYING → +30점 (EXIT 임계 60점에 크게 기여)
       WEAKENING → +10점
       ALIVE → 0점
       BREAKING → -10점 (리스크 감소)
  
  2. auto_trader.py job_monitor(30초)에서:
     - 매 5분마다 (30초 루프의 10번째마다) AI Eye 실행
     - 보유종목 + 당일 매수 종목만 대상
     - DYING + HIGH confidence → 즉시 Guardian 재평가 트리거
  
  3. 자동 액션:
     - AI Eye DYING + Guardian EXIT(60+) → auto_trader가 즉시 매도
     - AI Eye DYING + Guardian REDUCE(35+) → 절반 매도
     - AI Eye WEAKENING → Guardian 리스크 업데이트만 (매도 안 함)
     - AI Eye ALIVE/BREAKING → SL/TP 조정만
  
  4. 트레일링 SL 동적 조정:
     - ALIVE: 고점 대비 -1.5% (타이트)
     - WEAKENING: 고점 대비 -2.5% (여유)
     - BREAKING: 돌파 가격 -0.5% (돌파선 사수)
테스트: DYING 판정 → Guardian EXIT → 자동 매도 확인
효과: 장중에 "수급 빠졌다 나가자"를 봇이 스스로 판단

---

## EYE-08: 텔레그램 연동 (긴급 시만)
상태: [ ]
파일: intraday_eye.py + telegram_bot.py
내용:
  1. AI Eye 상태 변화 시에만 텔레그램 (텔레그램 재설계 원칙 유지)
     
     전송 조건 (이것만):
     - ALIVE → DYING 전환 시 (긴급: 청산 실행 알림)
     - ALIVE → BREAKING 전환 시 (정보: 돌파 알림)
     - DYING → EXIT 실행 시 (체결 알림 = MSG-04와 통합)
     
     전송 안 하는 것:
     - ALIVE 유지 → 당연한 거니까 안 보냄
     - WEAKENING → 아직 청산 아니니까 로그만
     - 5분마다 상태 업데이트 → 로그만
  
  2. 긴급 전환 메시지:
     ```
     🛑 [AI Eye] 풍산 — DYING 전환 (11:35)
        역배열 + VWAP 하회 + 기관매도 전환
        → 청산 실행 중
     ```
  
  3. 돌파 알림:
     ```
     ✅ [AI Eye] 제일일렉트릭 — 돌파 (10:22)
        저항 12,200 돌파 + 거래량 2.1x
        → 목표가 상향 12,748 → 13,100
     ```
  
  4. /눈 명령어 추가:
     현재 보유종목의 AI Eye 상태 즉시 조회
     ```
     👁 AI Eye 현황 (11:40)
     풍산: ALIVE (HIGH) — 정배열+VWAP위+기관매수
     제일일렉: BREAKING (HIGH) — 12,200 돌파+거래량폭발
     ```
테스트: ALIVE→DYING 전환 시 텔레그램 1개 + 로그 기록
효과: 장중 핵심 변화만 알림, 나머지는 봇이 알아서 처리

---

## EYE-09: 마감 리포트에 AI Eye 요약 추가
상태: [ ]
파일: intraday_eye.py + telegram_bot.py (_send_daily_closing)
내용:
  1. 하루 동안 AI Eye 판정 히스토리 요약
     - 각 보유종목의 상태 전환 타임라인
     - ALIVE 유지 시간 / WEAKENING 시간 / DYING 전환 시점
  
  2. 마감 리포트에 추가 섹션:
     ```
     👁 AI Eye 요약:
        제일일렉: ALIVE(09:05~10:20) → BREAKING(10:22) → ALIVE(10:40~)
        풍산: ALIVE(09:05~11:30) → WEAKENING(11:35) → ALIVE(13:00~)
     ```
  
  3. 학습 데이터 기록:
     - eye_history.json에 일별 판정 히스토리 저장
     - 추후 "DYING 판정 후 실제 하락률" 등 정확도 검증용
테스트: 마감 리포트에 AI Eye 타임라인 표시
효과: AI Eye가 장중에 얼마나 정확했는지 사후 검증 가능

---

# 구현 순서 (의존관계)
# EYE-01~05: 독립적 (병렬 가능)
# EYE-06: EYE-01~05 전부 필요 (종합 판정)
# EYE-07: EYE-06 필요 (Guardian 연동)
# EYE-08: EYE-06 필요 (텔레그램)
# EYE-09: EYE-06 필요 (마감 리포트)
#
# 권장: EYE-01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09

---

# 완료 기록
# 2026-03-20: EYE-01~09 전체 구현 완료
#   - EYE-01: calc_intraday_ma() — EMA(5/12/48) 정배열/역배열/수렴
#   - EYE-02: calc_vwap_position() — VWAP + σ밴드 + 전이감지
#   - EYE-03: calc_support_resistance() — 볼륨프로파일 S/R + 터치카운트
#   - EYE-04: calc_volume_flow() — 거래량가속 + 체결강도 + 수급상태
#   - EYE-05: calc_momentum() — RSI(14) + MACD(5,13,4) + 가격가속도
#   - EYE-06: synthesize() — 룰기반(BREAKING/DYING/BOUNCING) + 점수기반(ALIVE/WEAKENING)
#   - EYE-07: Guardian 연동 — eye_risk_adj (+30/-10) → 자동 EXIT/REDUCE
#   - EYE-08: 텔레그램 /눈(ㄴ) 명령 + 상태전이 알림
#   - EYE-09: 마감리포트 AI Eye 타임라인 + eye_history.json 저장
