# 단타봇 긴급 개선 — A: 장중 TV 실시간 + B: 14:30 프리클로즈 스캔

## 배경

현재 단타봇의 구조적 문제 2가지를 수정한다.

**문제 A:** 장중 거래대금(TV) 스캐너가 daily CSV만 읽어서 어제 데이터를 반복 스캔함 (MEMORY.md HIGH 버그)
→ 1분봉 Parquet이 이미 실시간 수집 중이므로, 이걸 TV Scanner에 연결하면 해결됨

**문제 B:** 저녁 분석(16:45)이 끝나면 이미 장이 끝남. 다음날 08:55에 로드하면 이벤트 종목은 8시부터 이미 올라가고 있어서 한 타이밍 늦음
→ 14:30에 장중 데이터 기반 프리클로즈 스캔을 돌려서, 15:00~15:20에 진입할 수 있게 함

**목표:** 전날 장마감 전에 내일 후보에 미리 깔아놓을 수 있는 구조

---

## A. 장중 TV 실시간 수정

### 현재 문제 (entry_monitor.py)
```
scan_trading_value() → daily CSV 읽음 → 장중에는 어제 확정 데이터만 반복
→ "오늘 누가 조용히 매집하고 있는지"를 오늘 장중에 감지 못함
→ 16:00 일봉 수집 후에야 알게 됨 = 늦음
```

### 이미 있는 것
```
- 1분봉 Parquet: 09:01~15:30 실시간 수집 중 (_job_start_tick_polling)
- trading_value_scanner.py: 3패턴 감지 로직 (EXPLOSION/QUIET_ACCUMULATION/GRADUAL_BUILDUP)
- 5팩터 스코어 로직
```

### 수정 방향

`trading_value_scanner.py`에 **장중 모드 함수** 추가:

```python
def scan_intraday(self, stock_code: str, minute_data_path: str) -> dict:
    """
    장중 1분봉 Parquet 기반 실시간 TV 스캔
    
    기존 scan()은 daily CSV 기반 (장후 배치용) → 그대로 유지
    이 함수는 장중 1분봉 기반 (실시간용) → 신규 추가
    
    계산 방식:
    1. 오늘 1분봉에서 현재까지 누적 거래대금 계산
      - intraday_tv = sum(close * volume) for today's candles
    
    2. 시간 보정 (중요!)
      - 지금이 11:00이면 전체 장(6.5시간) 중 2시간 경과 = 30.8%
      - 예상 하루 거래대금 = intraday_tv / 경과비율
      - 이걸 20일 평균 일거래대금과 비교
    
    3. 기존 3패턴 판정 로직 그대로 적용
      - EXPLOSION: 예상 tv_ratio >= 3.0 + 가격변동 > 3%
      - QUIET_ACCUMULATION: 예상 tv_ratio >= 2.0 + 가격 ±3% 이내
      - GRADUAL_BUILDUP: 시간별 추세 상승 + 예상 tv_ratio >= 1.5
    
    4. 추가: 시간대별 거래대금 가속도
      - 최근 30분 거래대금 vs 이전 30분 → 가속 중이면 부스트
      - 이건 "지금 갑자기 돈이 몰리기 시작했다"를 감지하는 것
    
    Returns:
      {
        'stock_code': '005930',
        'intraday_tv': 523.4,           # 현재까지 누적 거래대금 (억)
        'estimated_daily_tv': 1698.7,   # 하루 예상 거래대금 (억)
        'avg_20d_tv': 892.3,            # 20일 평균 거래대금 (억)
        'estimated_tv_ratio': 1.90,     # 예상 TV 비율
        'pattern': 'QUIET_ACCUMULATION', # 감지된 패턴 (or None)
        'acceleration': 1.35,           # 최근 30분 가속도
        'confidence': 'MEDIUM',         # 시간 경과에 따른 신뢰도
                                        # 09:30 = LOW (데이터 부족)
                                        # 11:00 = MEDIUM
                                        # 13:00+ = HIGH
        'score': 72                     # 5팩터 스코어 (기존 로직 재활용)
      }
    """
```

### 시간 보정 상세

```python
def _calc_elapsed_ratio(self) -> float:
    """
    장 시작(09:00)부터 현재까지 경과 비율
    
    09:00 = 0.0
    11:00 = 0.308 (2h / 6.5h)
    13:00 = 0.615 (4h / 6.5h)  
    14:30 = 0.846 (5.5h / 6.5h) ← 프리클로즈 스캔 시점
    15:30 = 1.0
    
    주의: 11:30~12:30 점심시간은 거래 감소 반영
    → 단순 시간비율이 아니라 '거래량 가중 시간비율' 사용
    → 과거 20일 시간대별 거래량 분포로 보정
    """
```

### 신뢰도 레벨

```
09:00~10:00: VERY_LOW (패턴 감지 안 함, 데이터 축적만)
10:00~11:30: LOW (패턴 감지하되 참고용)
11:30~13:00: MEDIUM (패턴 감지 + 프리클로즈 후보 예비 플래그)
13:00~15:30: HIGH (패턴 감지 + 프리클로즈 스캔 사용 가능)
```

### entry_monitor.py 수정

```python
# 기존: scan_trading_value() → daily CSV
# 수정: 장중이면 intraday 모드, 장후면 daily 모드 자동 전환

def scan_trading_value(self, stock_code, force_daily=False):
    if self._is_market_hours() and not force_daily:
        return self.tv_scanner.scan_intraday(stock_code, self._get_minute_parquet_path())
    else:
        return self.tv_scanner.scan(stock_code)  # 기존 로직
```

### 스케줄 연동

```
기존 30초 반복 job_monitor에서:
  → 보유종목 + 진입감시 종목에 대해 scan_intraday() 호출
  → QUIET_ACCUMULATION 감지 시 텔레그램 알림:
    "🔍 [장중감지] SK하이닉스 — 조용한 매집 (TV 2.3x, 가격 +0.8%, 가속 1.4x)"
  → 이 데이터가 14:30 프리클로즈 스캔의 입력이 됨
```

---

## B. 14:30 프리클로즈 스캔

### 핵심 개념

```
16:45 저녁 분석: "어제 데이터로 점수 매기기" (후행)
14:30 프리클로즈: "오늘 장중 데이터로 내일 후보 찾기" (선행)

→ 둘 다 돌린다. 저녁 분석은 그대로 유지.
→ 프리클로즈는 "오늘 장중에 뭔 일이 있었나" 기반으로
   "내일 움직일 가능성이 높은 종목"을 찾는 것.
→ 15:00~15:20에 진입 판단.
```

### 프리클로즈 스캔이 보는 것 (A의 실시간 데이터 활용)

```
14:30 시점에 이미 확보된 데이터:

1. 장중 TV 실시간 (A에서 수정) → 오늘 거래대금 폭발/매집 감지
2. 장중 수급 누적 → 기관/외인 오늘 순매수 현황
3. 장중 1분봉 → 가격 패턴 (장중 눌림 후 반등 등)
4. 섹터 순환 → 오늘 HOT 섹터 확인
5. 해외 데이터 → 미국 선물/원자재/환율 현재값
6. 뉴스 AI → 오늘 나온 뉴스 감성
7. 어제 insights.json → 학습 보정값
```

### 새 스케줄 항목

```
14:30  _job_preclose_scan (신규)
       → 장중 데이터 기반 "내일 후보" 스캔
       → 결과: preclose_candidates.json
       
14:45  _job_preclose_trade_objects (신규)
       → 후보에 대해 Trade Object 생성 (R:R 게이트 적용)
       → 결과: preclose_trades.json
       
14:50  텔레그램: 프리클로즈 리포트 전송
       → "📋 [프리클로즈] 내일 후보 3종목"
       → 각 종목별 Trade Object (진입가/목표가/손절가/R:R)
       → "15:00~15:20 진입 권장"
       
15:00  _job_preclose_entry (신규, 수동 확인 모드)
       → preclose_trades.json 로드
       → 진입 6조건 중 적용 가능한 것만 체크
         (14:30 기준이므로 종가 미확정 → 양봉 조건 등은 제외)
       → 수동 확인 후 매수 (confirm_real_order: true)
```

### 프리클로즈 스코어링 — 저녁 분석과 다른 점

```python
class PrecloseScanner:
    """
    저녁 분석(morning_recommendation.py)과의 차이:
    
    저녁 분석: 확정 일봉 + 확정 수급 → 정밀하지만 늦음
    프리클로즈: 장중 미확정 데이터 → 덜 정밀하지만 빠름
    
    따라서 프리클로즈만의 가중치가 필요:
    """
    
    # 프리클로즈에서 중요한 것 (장중에 실시간으로 보이는 것)
    PRECLOSE_WEIGHTS = {
        'intraday_tv_pattern': 30,     # 오늘 장중 거래대금 패턴 (A의 핵심)
        'intraday_supply': 25,         # 오늘 기관/외인 누적 순매수
        'intraday_price_action': 15,   # 장중 가격 행동 (눌림→반등 등)
        'sector_today': 15,            # 오늘 섹터 상태
        'macro_current': 10,           # 현재 미국선물/VIX/환율
        'news_today': 5,               # 오늘 뉴스
    }
    # 합계: 100점 만점
    
    # 프리클로즈에서 못 보는 것 (일봉 확정 후에야 정확)
    # → 저녁 분석에서 보완
    EXCLUDED_FROM_PRECLOSE = [
        '확정 종가 기반 기술적 지표 (MACD/RSI)',  # 종가 미확정
        '확정 일봉 기반 릴레이',                    # 미확정
        '당일 확정 수급 (15:30 이후)',              # 미확정
    ]
    
    def scan(self, universe: list) -> list:
        """
        14:30 시점 스캔
        
        1단계: 장중 TV 실시간으로 "오늘 뭔가 있었던 종목" 1차 필터
          → QUIET_ACCUMULATION or EXPLOSION or GRADUAL_BUILDUP 감지된 종목
          → + 기관/외인 장중 누적 순매수 상위 종목
          → 합쳐서 ~30~50개 1차 후보
        
        2단계: 프리클로즈 스코어링 (100점 만점)
          → 1차 후보에 대해 점수 계산
          → 50점+ 종목만 통과
        
        3단계: Trade Object 생성
          → TradeBuilder 호출 (기존 trade_object.py 재사용)
          → R:R Gate 적용
          → ACCEPT 종목만 최종 후보
        
        4단계: 순위화
          → R:R 높은 순으로 정렬
          → 상위 3~5개만 최종 추천
        
        Returns: [PrecloseCandidate, ...]
        """
```

### 프리클로즈 vs 저녁 분석 — 역할 분담

```
프리클로즈 (14:30):
  질문: "오늘 장중에 뭔 일이 있었나 → 내일 이어질 종목은?"
  강점: 실시간 데이터 기반, 장마감 전 진입 가능
  약점: 확정 데이터 아님, 정밀도 낮음
  용도: 15:00~15:20 매수 (선제 진입)

저녁 분석 (16:45):
  질문: "확정 데이터 기준 내일 최고 종목은?"
  강점: 확정 일봉/수급/기술적 지표, 정밀도 높음
  약점: 이미 장 끝남, 다음날 아침에야 진입
  용도: 다음날 09:00 매수 (정밀 진입)

두 결과가 겹치는 종목:
  → 확신도 UP → Trade Object conviction = "HIGH"
  → 프리클로즈에서 이미 진입했으면 → 저녁 분석에서 추가 매수 고려
```

### 프리클로즈 진입 시 주의사항

```python
# 프리클로즈 진입은 "내일을 위한" 매수이므로:

# 1. 당일 청산 대상이 아님
#    → job_eod_close(15:10)에서 프리클로즈 매수분은 제외해야 함
#    → 포지션에 'preclose' 태그 부착

# 2. 포지션 사이즈 제한
#    → 확정 데이터가 아니므로 정규 사이즈의 70%
#    → 저녁 분석에서 확인되면 나머지 30% 추가 가능

# 3. 손절은 확정
#    → Trade Object의 stop_price는 프리클로즈 시점에 확정
#    → 다음날 장중에 stop 도달 시 즉시 청산

# 4. 기존 max_auto_positions(2개)와 별도 카운트
#    → preclose_max_positions: 2 (신규 config)
#    → 정규 + 프리클로즈 합쳐서 최대 4개
```

---

## 구현 순서

### Phase A: 장중 TV 실시간 (먼저)

```
Step 1: trading_value_scanner.py에 scan_intraday() 추가
  - 1분봉 Parquet 읽기
  - 시간 보정 (경과비율 + 점심시간 보정)
  - 기존 3패턴 판정 재사용
  - 가속도(최근 30분 vs 이전 30분) 추가

Step 2: entry_monitor.py 수정
  - 장중이면 scan_intraday(), 장후면 scan() 자동 전환
  
Step 3: job_monitor(30초 반복)에서 scan_intraday 호출
  - 유니버스 전체는 부하 큼 → 2단계 접근:
    a. 보유종목 + 진입감시 종목: 30초마다 (기존대로)
    b. 유니버스 전체: 30분마다 빠른 스캔 (거래대금 상위 50만)
  - QUIET_ACCUMULATION 감지 시 텔레그램 알림

Step 4: 테스트
  - 장중에 scan_intraday() 결과 vs 장후 scan() 결과 비교
  - 오차범위 확인 (14:30 예상 TV비율 vs 16:00 확정 TV비율)
```

### Phase B: 14:30 프리클로즈 스캔 (A 완료 후)

```
Step 1: preclose_scanner.py 신규 생성
  - PRECLOSE_WEIGHTS 기반 스코어링
  - scan_intraday() 결과를 주 입력으로 사용
  - TradeBuilder 호출 (기존 trade_object.py 재사용)

Step 2: 스케줄 추가 (telegram_bot.py)
  - 14:30 _job_preclose_scan
  - 14:45 _job_preclose_trade_objects  
  - 14:50 텔레그램 리포트
  - 15:00 _job_preclose_entry (수동 확인)

Step 3: auto_trader.py 수정
  - preclose 태그 포지션은 job_eod_close에서 제외
  - preclose_max_positions config 추가

Step 4: 저녁 분석과 교차 검증
  - 프리클로즈 후보 vs 저녁 분석 후보 겹침 체크
  - 겹치면 conviction 상향

Step 5: PAPER 모드 먼저
  - 최초 2주는 [PAPER-PRECLOSE] 태그로 가상 매수
  - trade_tracker.py에 preclose 카테고리 추가
  - 프리클로즈 진입 vs 다음날 아침 진입 수익률 비교
```

---

## 텔레그램 출력 예시

### A: 장중 감지 알림 (수시)
```
🔍 [장중감지] 14:02
━━━━━━━━━━━━━━━━━━━
풍산 (103140)
  거래대금: 287억 (예상 TV 2.4x) — 조용한 매집
  장중 가격: +0.6% (눌림 후 횡보)
  기관 순매수: +42억 (누적)
  가속도: 1.35x (최근 30분 ↑)
  신뢰도: HIGH (13시 이후)
━━━━━━━━━━━━━━━━━━━
```

### B: 프리클로즈 리포트 (14:50)
```
📋 [프리클로즈] 내일 후보 — 14:50
━━━━━━━━━━━━━━━━━━━

1️⃣ 풍산 (103140) — PRECLOSE 78점
   촉발: 장중 조용한 매집 (TV 2.4x + 기관 +42억)
   진입가: 52,300 (현재가 근처)
   목표가: 54,900 (+5.0%)
   손절가: 50,500 (-3.4%)
   R:R: 1.47 → 사이즈 70% 적용 시 1.47 ⚠️
   보유: 2~3일

2️⃣ LS전선 (026260) — PRECLOSE 71점
   촉발: 구리 선물 +2.1% + 기관 3일 연속
   진입가: 128,500
   목표가: 136,200 (+6.0%)  
   손절가: 124,000 (-3.5%)
   R:R: 1.71 ✅
   보유: 2~3일

💡 15:00~15:20 진입 권장
   프리클로즈 사이즈: 정규의 70%
   저녁 분석(17:30)에서 확인되면 30% 추가
━━━━━━━━━━━━━━━━━━━
```

---

## config.yaml 추가 항목

```yaml
# 프리클로즈 설정
preclose:
  enabled: true
  scan_time: "14:30"
  trade_object_time: "14:45"
  report_time: "14:50"
  entry_window: "15:00~15:20"
  max_positions: 2                    # 프리클로즈 최대 종목수
  size_multiplier: 0.7                # 정규 사이즈의 70%
  min_preclose_score: 50              # 최소 프리클로즈 점수
  paper_mode: true                    # 초기 2주 PAPER
  
# 장중 TV 실시간 설정  
intraday_tv:
  enabled: true
  universe_scan_interval: 1800        # 유니버스 전체 스캔: 30분
  watchlist_scan_interval: 30         # 감시 종목 스캔: 30초
  min_confidence: "MEDIUM"            # 알림 최소 신뢰도
  alert_patterns:                     # 알림 대상 패턴
    - QUIET_ACCUMULATION
    - EXPLOSION
```

---

## 수정/신규 파일 요약

| 파일 | 작업 | 내용 |
|------|------|------|
| `trading_value_scanner.py` | 수정 | scan_intraday() 추가 (~100줄) |
| `entry_monitor.py` | 수정 | 장중/장후 자동 전환 (~20줄) |
| `preclose_scanner.py` | 신규 | 14:30 프리클로즈 스캔 엔진 (~300줄) |
| `telegram_bot.py` | 수정 | 3개 스케줄 추가 + 장중감지 알림 (~80줄) |
| `auto_trader.py` | 수정 | preclose 태그 + EOD 제외 (~30줄) |
| `trade_tracker.py` | 수정 | preclose 카테고리 추가 (~20줄) |
| `config.yaml` | 수정 | preclose + intraday_tv 섹션 추가 |

**기존 로직 변경 없음.** 전부 추가/확장이므로 기존 저녁 분석, 아침 추천, Position Guardian 등은 그대로 돌아감.
