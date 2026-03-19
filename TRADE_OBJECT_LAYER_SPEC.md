# 단타봇 Trade Object Layer 설계서
## "스크리너"에서 "트레이드 팩토리"로의 전환

---

## 1. 문제 정의

### 현재 시스템이 답하는 것
```
✅ "이 종목 점수 높다" (스크리닝)
✅ "수급이 들어오고 있다" (관찰)
✅ "섹터가 HOT이다" (상태)
✅ "모멘텀 레짐이다" (환경)
```

### 현재 시스템이 답 못하는 것
```
❌ "얼마에 사야 하나" (진입가)
❌ "얼마나 오를까" (기대수익률)
❌ "언제까지 들고 있나" (보유기간)
❌ "얼마에서 손절하나" (진입가 기준 손절)
❌ "이 트레이드의 리스크 대비 수익은?" (R:R)
❌ "이 트레이드가 돈을 벌었나?" (트레이드 단위 학습)
```

### 핵심 진단
> **점수가 높다 ≠ 돈을 번다**
> 점수 80짜리를 사서 1% 먹고 나오면 수수료 빼고 본전.
> 점수 60짜리를 사서 5% 먹고 나오면 그게 진짜 수익.
> "언제 얼마에 사서 얼마에 파는가"가 없으면 스크리너일 뿐.

---

## 2. 아키텍처 변경

### Before (현재)
```
[데이터 16소스] → [스코어링] → [점수 순위] → [추천 목록] → [진입 6조건 체크] → [매수]
                                                                                    ↓
                                                                           [Position Guardian] → [EXIT/REDUCE/HOLD]
```

**문제:** 추천 → 매수 사이에 "트레이드 설계" 과정이 없음

### After (신규)
```
[데이터 16소스] → [스코어링] → [점수 순위]
                                    ↓
                            ┌── LAYER 1: 스크리너 (기존) ──┐
                            │  후보 종목 + 점수 + 신호      │
                            └──────────────────────────────┘
                                    ↓
                            ┌── LAYER 2: 트레이드 빌더 (신규) ──┐
                            │                                    │
                            │  ① 기대수익률 산출                  │
                            │  ② 손절가 산출                      │
                            │  ③ R:R 비율 계산                   │
                            │  ④ 보유기간 추정                    │
                            │  ⑤ 포지션 사이즈 결정               │
                            │  ⑥ R:R < 1.5 → REJECT             │
                            │                                    │
                            │  OUTPUT: Trade Object              │
                            └────────────────────────────────────┘
                                    ↓
                            ┌── LAYER 3: 실행기 (기존 개선) ──┐
                            │  Trade Object 기반 주문          │
                            │  + 진입가 기준 실시간 P&L 추적   │
                            └──────────────────────────────────┘
                                    ↓
                            ┌── LAYER 4: 학습 (기존 개선) ──┐
                            │  종목 단위 → 트레이드 단위     │
                            │  "추천이 맞았나" → "돈을 벌었나"│
                            └──────────────────────────────────┘
```

---

## 3. Trade Object 정의

### 3.1 데이터 구조

```python
@dataclass
class TradeObject:
    # === 식별 ===
    trade_id: str              # "T_20260319_005930" (날짜_종목코드)
    stock_code: str            # "005930"
    stock_name: str            # "삼성전자"
    created_at: datetime       # 생성 시각
    
    # === 스크리너 결과 (기존 시스템에서 받아옴) ===
    score: float               # 기존 total score (예: 87.3)
    score_breakdown: dict      # {릴레이: 30, 수급: 25, ...}
    regime: str                # "NORMAL" | "MOMENTUM"
    sector_cycle_day: int      # HOT 몇 일차
    momentum_score: float      # 모멘텀 레짐 점수 (0~1)
    
    # === 트레이드 설계 (신규 핵심) ===
    entry_price: float         # 목표 진입가
    entry_method: str          # "MARKET" | "LIMIT" | "VWAP_CHASE"
    
    target_price: float        # 목표가 (1차)
    target_price_2: float      # 목표가 (2차, optional)
    expected_return: float     # 기대수익률 % (예: 4.2)
    
    stop_price: float          # 손절가
    stop_pct: float            # 손절 % (예: -2.0)
    time_stop_days: int        # 시간 손절 (예: 3일)
    
    rr_ratio: float            # Risk:Reward 비율 (예: 2.1)
    
    # === 포지션 관리 ===
    position_size_pct: float   # 총 자산 대비 비중 % (예: 8.0)
    position_size_won: int     # 원화 금액 (예: 3_000_000)
    max_position_pct: float    # 이 종목 최대 허용 비중
    
    # === 보유기간 ===
    expected_hold_days: int    # 예상 보유일 (예: 2~3일)
    hold_day_min: int          # 최소 보유일
    hold_day_max: int          # 최대 보유일 (시간 손절)
    
    # === 확신도 ===
    conviction: str            # "HIGH" | "MEDIUM" | "LOW"
    conviction_reason: str     # "수급폭발 + 섹터초기 + 모멘텀"
    
    # === 실행 후 추적 ===
    status: str                # "PLANNED" → "ENTERED" → "MONITORING" → "EXITED"
    actual_entry: float        # 실제 매수가
    actual_exit: float         # 실제 매도가
    actual_return: float       # 실제 수익률
    actual_hold_days: int      # 실제 보유일
    exit_reason: str           # "TARGET_HIT" | "STOP_HIT" | "TIME_STOP" | "GUARDIAN_EXIT"
```

### 3.2 Trade Object 생성 흐름

```python
class TradeBuilder:
    """스크리너 결과를 받아서 Trade Object로 변환"""
    
    def build(self, candidate: ScreenerResult) -> TradeObject | None:
        """
        Returns None if trade doesn't meet minimum criteria (R:R < 1.5)
        """
        # Step 1: 기대수익률 산출
        expected = self.calc_expected_return(candidate)
        
        # Step 2: 손절가 산출
        stop = self.calc_stop_price(candidate)
        
        # Step 3: R:R 필터
        rr = expected.return_pct / abs(stop.loss_pct)
        if rr < 1.5:
            return None  # ← 이게 핵심. 좋아 보여도 R:R 안 나오면 안 함
        
        # Step 4: 보유기간 추정
        hold = self.estimate_hold_period(candidate)
        
        # Step 5: 포지션 사이즈
        size = self.calc_position_size(candidate, stop)
        
        # Step 6: Trade Object 생성
        return TradeObject(
            entry_price=expected.entry,
            target_price=expected.target,
            stop_price=stop.price,
            rr_ratio=rr,
            expected_hold_days=hold.days,
            position_size_pct=size.pct,
            conviction=self.assess_conviction(candidate, rr, hold),
            ...
        )
```

---

## 4. 핵심 모듈 설계

### 4.1 기대수익률 엔진 (ExpectedReturnEngine)

**원리:** "과거에 비슷한 조건의 종목이 평균 얼마나 움직였는가"

```python
class ExpectedReturnEngine:
    """
    3가지 방법으로 기대수익률 산출 → 가중 평균
    """
    
    def calc(self, candidate) -> ExpectedReturn:
        
        # 방법 1: 점수 구간별 과거 수익률 (Historical Score Bucket)
        # ─────────────────────────────────────────────────
        # 학습 데이터: 과거 추천 종목의 (점수, N일 후 수익률) 누적
        # 
        # 예시 테이블 (실제로는 자동 학습):
        # ┌──────────┬────────────┬────────────┬──────────┐
        # │ 점수 구간  │ 3일 평균    │ 5일 평균    │ 표본수    │
        # ├──────────┼────────────┼────────────┼──────────┤
        # │ 40~59    │ +1.8%      │ +2.1%      │ 45       │
        # │ 60~79    │ +3.2%      │ +3.8%      │ 32       │
        # │ 80~99    │ +4.5%      │ +5.7%      │ 18       │
        # │ 100+     │ +6.1%      │ +8.2%      │  7       │
        # └──────────┴────────────┴────────────┴──────────┘
        #
        # → 점수 87이면 → 3일 기대 +4.5%, 5일 기대 +5.7%
        score_based = self._score_bucket_return(candidate.score)
        
        
        # 방법 2: 컨센서스 업사이드 (기존 로직 활용)
        # ─────────────────────────────────────────────────
        # 목표가 50,000 / 현재가 45,000 = 업사이드 +11.1%
        # 단, 단타이므로 업사이드의 30~50%만 단기 기대치로 할인
        # → +11.1% × 0.35 = +3.9%
        consensus_based = self._consensus_discount(candidate)
        
        
        # 방법 3: 유사 패턴 매칭 (Pattern Similarity)
        # ─────────────────────────────────────────────────
        # 현재 종목의 조건 벡터:
        #   [수급점수, 섹터사이클일, 거래대금비율, 모멘텀여부, regime]
        # 과거 DB에서 가장 유사한 10개 케이스의 수익률 중간값
        pattern_based = self._pattern_match_return(candidate)
        
        
        # 가중 평균 (표본이 많은 방법에 가중치)
        weights = self._dynamic_weights(score_based, consensus_based, pattern_based)
        expected_return = (
            score_based.ret * weights[0] +
            consensus_based.ret * weights[1] +
            pattern_based.ret * weights[2]
        )
        
        # 목표가 산출
        target_price = candidate.current_price * (1 + expected_return / 100)
        
        return ExpectedReturn(
            return_pct=expected_return,
            target=target_price,
            entry=candidate.current_price,  # 또는 VWAP 근접가
            confidence=self._confidence_level(score_based, consensus_based, pattern_based),
            method_detail={
                'score_bucket': score_based,
                'consensus': consensus_based,
                'pattern': pattern_based
            }
        )
```

### 4.2 손절가 엔진 (StopPriceEngine)

**원리:** "이 가격 밑으로 가면 내 시나리오가 틀린 것"

```python
class StopPriceEngine:
    """
    손절가 = 진입 시나리오가 무효화되는 가격
    고정 % 손절이 아니라, 구조적 손절
    """
    
    def calc(self, candidate) -> StopPrice:
        
        # 방법 1: ATR 기반 (변동성 적응형)
        # ─────────────────────────────────────────────────
        # ATR(14) = 최근 14일 평균 일일 변동폭
        # 손절가 = 현재가 - (ATR × 배수)
        #
        # 배수 결정:
        #   NORMAL 레짐: 1.5 ATR (타이트)
        #   MOMENTUM 레짐: 2.0 ATR (모멘텀에 숨 쉴 여유)
        atr_stop = self._atr_based_stop(candidate)
        
        
        # 방법 2: 지지선 기반 (구조적)
        # ─────────────────────────────────────────────────
        # 최근 5일 저점, 20일 이동평균, 볼린저 하단 중
        # 가장 가까운 지지선 아래 -0.5%
        support_stop = self._support_based_stop(candidate)
        
        
        # 방법 3: 수급 이탈선 (수급 시나리오 무효화)
        # ─────────────────────────────────────────────────
        # 기관/외인 순매수 전환 시점의 가격
        # 그 아래로 가면 = "수급 진입 근거 소멸"
        supply_stop = self._supply_invalidation_price(candidate)
        
        
        # 최종 손절가: 3개 중 가장 보수적(=높은 가격) 선택
        # → 손실을 최소화하는 방향
        # 단, 현재가 대비 -5% 이내로 캡 (너무 넓으면 R:R 붕괴)
        stop_price = max(atr_stop, support_stop, supply_stop)
        stop_pct = (stop_price - candidate.current_price) / candidate.current_price * 100
        
        # 캡 적용
        if stop_pct < -5.0:
            stop_price = candidate.current_price * 0.95
            stop_pct = -5.0
        
        return StopPrice(
            price=stop_price,
            loss_pct=stop_pct,
            method=self._best_method_name(atr_stop, support_stop, supply_stop),
            invalidation_reason=self._reason(candidate)
        )
```

### 4.3 보유기간 추정 엔진 (HoldPeriodEngine)

**원리:** "이 시나리오가 실현되는 데 보통 며칠 걸리는가"

```python
class HoldPeriodEngine:
    """
    단타 = 1~5일. 이 범위 내에서 최적 보유기간 추정.
    """
    
    def estimate(self, candidate) -> HoldPeriod:
        
        # Factor 1: 섹터 사이클 잔여 수명
        # ─────────────────────────────────────────────────
        # HOT 1일차 → 아직 3~4일 남음 → 보유 가능 3일
        # HOT 3일차 → 1~2일 남음 → 보유 1일 (들어가자마자 나올 준비)
        # HOT 5일+ → 소진 임박 → 당일~1일
        sector_life = self._sector_remaining_life(candidate.sector_cycle_day)
        
        
        # Factor 2: 수급 지속성
        # ─────────────────────────────────────────────────
        # 기관 5일 연속 순매수 중 → 추가 2~3일 지속 확률 높음
        # 기관 1일 순매수 → 지속성 불확실 → 1일
        supply_duration = self._supply_persistence(candidate)
        
        
        # Factor 3: 과거 유사 점수대 평균 보유일
        # ─────────────────────────────────────────────────
        # 점수 80~100 종목이 목표가 도달까지 평균 2.3일
        historical_avg = self._historical_hold_days(candidate.score)
        
        
        # 최종: 가장 짧은 것 기준 (보수적)
        expected = min(sector_life, supply_duration, historical_avg)
        
        return HoldPeriod(
            days=round(expected),
            min_days=1,
            max_days=min(round(expected) + 2, 5),  # 단타 최대 5일
            limiting_factor=self._limiting_factor_name(...)
        )
```

### 4.4 포지션 사이즈 엔진 (PositionSizer)

**원리:** "손절 시 최대 손실 = 총 자산의 1~2%"

```python
class PositionSizer:
    """
    켈리 공식 변형 + 고정 리스크 방식
    """
    
    # 설정값
    MAX_RISK_PER_TRADE = 0.02   # 트레이드당 최대 리스크 = 총자산 2%
    MAX_POSITION_PCT = 0.15     # 단일 종목 최대 비중 15%
    CASH_RESERVE_PCT = 0.25     # 현금 25% 유지 (형의 철칙)
    
    def calc(self, trade: TradeObject, portfolio) -> PositionSize:
        
        # 사용 가능 자산 = 총자산 × (1 - 현금유보)
        available = portfolio.total_value * (1 - self.CASH_RESERVE_PCT)
        
        # 리스크 기반 사이징
        # 최대 손실 허용액 = 총자산 × 2%
        max_loss_won = portfolio.total_value * self.MAX_RISK_PER_TRADE
        
        # 포지션 크기 = 최대 손실 허용액 / 손절 %
        # 예: 총자산 5천만, 손절 -2% → 최대 손실 100만 → 포지션 5천만원
        # 예: 총자산 5천만, 손절 -5% → 최대 손실 100만 → 포지션 2천만원
        position_won = max_loss_won / (abs(trade.stop_pct) / 100)
        
        # 최대 비중 캡
        position_won = min(position_won, available * self.MAX_POSITION_PCT)
        
        # 확신도에 따른 조절
        conviction_mult = {
            'HIGH': 1.0,
            'MEDIUM': 0.7,
            'LOW': 0.5
        }
        position_won *= conviction_mult[trade.conviction]
        
        # Market Brain 레짐 반영
        position_won *= portfolio.market_brain_multiplier  # 0.3x~1.0x
        
        return PositionSize(
            won=int(position_won),
            pct=position_won / portfolio.total_value * 100,
            shares=int(position_won / trade.entry_price),
            risk_won=int(position_won * abs(trade.stop_pct) / 100)
        )
```

### 4.5 R:R 게이트 (최종 필터)

```python
class RRGate:
    """
    R:R < 1.5인 트레이드는 아무리 점수 높아도 REJECT
    이것이 스크리너와 트레이드의 결정적 차이
    """
    
    MIN_RR = 1.5          # 최소 R:R
    PREFERRED_RR = 2.0    # 선호 R:R
    
    def evaluate(self, trade: TradeObject) -> TradeDecision:
        
        rr = trade.expected_return / abs(trade.stop_pct)
        
        if rr < self.MIN_RR:
            return TradeDecision(
                action="REJECT",
                reason=f"R:R {rr:.1f} < {self.MIN_RR} (기대수익 {trade.expected_return:.1f}% vs 손절 {trade.stop_pct:.1f}%)",
                # ← 점수 90이어도 여기서 걸러짐
            )
        
        if rr >= self.PREFERRED_RR:
            return TradeDecision(action="STRONG_BUY", rr=rr)
        
        return TradeDecision(action="BUY", rr=rr)
```

---

## 5. 학습 시스템 전환

### Before: 종목 단위 학습 (현재)
```
"삼성전자 추천했는데 올랐다" → 적중 ✅
"하이닉스 추천했는데 내렸다" → 미적중 ❌
→ 소스별 적중률 계산
```

**문제:** 추천 적중 ≠ 돈 벌었다

### After: 트레이드 단위 학습 (신규)

```python
class TradeLearner:
    """
    매일 15:40 장마감 후:
    종목이 맞았냐가 아니라, 트레이드가 돈을 벌었냐
    """
    
    def daily_review(self, closed_trades: list[TradeObject]):
        
        for trade in closed_trades:
            
            # 1. 수익률 기록
            trade.actual_return = (trade.actual_exit - trade.actual_entry) / trade.actual_entry * 100
            
            # 2. 기대 vs 실제 비교
            prediction_error = trade.actual_return - trade.expected_return
            
            # 3. 분류: 왜 이 결과가 나왔나
            if trade.exit_reason == "TARGET_HIT":
                category = "PLAN_WORKED"      # 계획대로 됐다
            elif trade.exit_reason == "STOP_HIT":
                category = "WRONG_THESIS"     # 시나리오가 틀렸다
            elif trade.exit_reason == "TIME_STOP":
                category = "DEAD_MONEY"       # 안 움직였다 (기회비용)
            elif trade.exit_reason == "GUARDIAN_EXIT":
                category = "CHANGED_CONDITION" # 조건이 변했다
            
            # 4. 학습 포인트 추출
            self._update_score_bucket_table(trade)       # 점수 구간별 실제 수익률 업데이트
            self._update_hold_period_stats(trade)         # 보유기간 통계 업데이트
            self._update_sector_cycle_stats(trade)        # 섹터 사이클 위치별 성과
            self._update_stop_effectiveness(trade)        # 손절이 적절했는가
            self._update_rr_calibration(trade)            # R:R 예측 정확도
    
    
    def generate_insights(self, trades_history: list[TradeObject]) -> dict:
        """
        축적된 트레이드 데이터에서 패턴 추출
        → 내일 TradeBuilder에 반영
        """
        return {
            # 점수 구간별 실제 평균 수익률 (기대수익률 캘리브레이션용)
            'score_bucket_returns': self._calc_bucket_returns(trades_history),
            
            # 섹터 사이클 일차별 실제 성과 (보유기간 캘리브레이션용)
            'sector_day_performance': self._calc_sector_day_perf(trades_history),
            
            # 손절 히트율 (손절 설정 캘리브레이션용)
            'stop_hit_rate': self._calc_stop_stats(trades_history),
            
            # 시간 손절 비율 (보유기간 설정 캘리브레이션용)
            'time_stop_rate': self._calc_time_stop_rate(trades_history),
            
            # R:R 실현율 (R:R 필터 캘리브레이션용)
            'rr_realization': self._calc_rr_realization(trades_history),
            
            # DEAD_MONEY 비율 (진입 타이밍 개선 시그널)
            'dead_money_rate': self._calc_dead_money(trades_history),
        }
```

---

## 6. 텔레그램 출력 변경

### Before (현재)
```
🔔 [매수 신호] 삼성전자 (005930)
📊 점수: 87.3 | 등급: A+
📈 수급: 기관 +523억 | 외인 +312억
🔥 섹터: 반도체 HOT 2일차
```

### After (신규)
```
🔔 [TRADE] 삼성전자 (005930)
━━━━━━━━━━━━━━━━━━━
📊 점수: 87.3 (A+) | 확신: HIGH

💰 트레이드 설계:
   진입가: 63,200원 (현재가 부근)
   목표가: 65,800원 (+4.1%)
   손절가: 62,000원 (-1.9%)
   R:R = 2.2 ✅

⏱ 보유기간: 2~3일 (섹터 HOT 2일차)
💵 포지션: 300만원 (총자산 6.0%)
📉 최대손실: -57,000원

📋 시나리오:
   수급폭발(기관 5일 연속 +523억)
   + 섹터 초기(HOT 2/5일)
   + 모멘텀 레짐
   
   무효화 조건: 기관 순매도 전환 or 62,000 이탈
━━━━━━━━━━━━━━━━━━━
```

### 종료 시
```
📊 [TRADE 종료] 삼성전자
   진입: 63,200 → 종료: 65,500
   수익: +3.6% (+108,000원)
   보유: 2일 | 사유: TARGET_HIT
   
   기대 vs 실제: +4.1% → +3.6% (오차 -0.5%p)
```

---

## 7. 데이터 축적 전략 (Cold Start 해결)

### 문제: 기대수익률 테이블이 비어있다
처음에는 과거 데이터가 없으므로 기대수익률을 계산할 수 없다.

### 해결: 3단계 부트스트랩

```
Phase 0 (1~2주): 백테스트 시드
─────────────────────────────────
- 과거 3개월 추천 데이터 + 실제 주가 매칭
- score_bucket_returns 초기 테이블 생성
- 대략적인 기대수익률 베이스라인 확보

Phase 1 (2~4주): 페이퍼 트레이딩
─────────────────────────────────
- Trade Object 생성하되, 실제 주문 안 함
- [PAPER] 태그로 텔레그램 전송
- 목표가/손절가 도달 여부 자동 추적
- trade_history.json에 결과 축적
- 기대수익률 테이블 캘리브레이션

Phase 2 (4주~): 실거래 전환
─────────────────────────────────
- 축적된 데이터 기반 실전 투입
- 단, 초기 2주는 포지션 사이즈 50%로 제한
- 학습 루프 본격 가동
```

---

## 8. 기존 시스템 변경 최소화

### 안 건드리는 것 (기존 유지)
- 16개 데이터 소스 수집
- 스코어링 로직 (raw_total 계산)
- 모멘텀 레짐 감지
- 섹터 사이클 추적
- NIGHTWATCH 매크로 모니터링
- Market Brain 포지션 사이징 (레짐 반영)

### 변경하는 것
| 기존 | 변경 |
|------|------|
| 진입 6조건 → 즉시 매수 | 진입 6조건 → **TradeBuilder** → Trade Object → 매수 |
| Position Guardian (시장 상태 기반) | Position Guardian + **진입가 기준 P&L 추적** |
| 학습: 종목 적중률 | 학습: **트레이드 수익/손실** |
| 텔레그램: 종목+점수 | 텔레그램: **Trade Object 전체** |

### 새로 만드는 것
| 모듈 | 파일 | 역할 |
|------|------|------|
| TradeObject | `trade_object.py` | 데이터 클래스 |
| TradeBuilder | `trade_builder.py` | 스크리너→트레이드 변환 |
| ExpectedReturnEngine | `expected_return.py` | 기대수익률 산출 |
| StopPriceEngine | `stop_engine.py` | 손절가 산출 |
| HoldPeriodEngine | `hold_period.py` | 보유기간 추정 |
| PositionSizer | `position_sizer.py` | 포지션 크기 |
| RRGate | `rr_gate.py` | R:R 필터 |
| TradeLearner | `trade_learner.py` | 트레이드 단위 학습 |
| TradeTracker | `trade_tracker.py` | 실행 중 P&L 추적 |

### 수정하는 것
| 기존 파일 | 수정 내용 |
|-----------|----------|
| `main_engine.py` (or equivalent) | 스크리너 결과를 TradeBuilder에 전달하는 연결 |
| `position_guardian.py` | Trade Object의 stop_price/target_price 참조 추가 |
| `telegram_sender.py` | Trade Object 포맷 출력 |
| `daily_learner.py` | TradeLearner 호출 추가 |
| `insights.json` | trade_insights.json으로 확장 |

---

## 9. 핵심 설계 원칙 (요약)

```
1. 스크리너는 "후보", 트레이드는 "계획"
   → 후보에서 계획으로 변환하는 과정이 핵심

2. 모든 트레이드는 들어가기 전에 나갈 곳을 안다
   → entry, target, stop, time_stop 4개가 없으면 진입 불가

3. R:R < 1.5는 무조건 거부
   → 점수 100점이어도 리스크 대비 수익이 안 나오면 안 함

4. 현금 25%는 절대 룰
   → PositionSizer에 하드코딩

5. 학습은 "돈을 벌었냐"로 측정
   → 추천 적중률이 아니라 트레이드 P&L

6. Cold Start는 백테스트 + 페이퍼로 해결
   → 데이터 없이 실전 투입 금지
```

---

## 10. 실행 로드맵

```
Week 1: 기반 구축
├── trade_object.py (데이터 클래스)
├── expected_return.py (점수 구간 테이블 + 컨센서스 할인)
├── stop_engine.py (ATR + 지지선)
├── rr_gate.py (R:R 필터)
└── Phase 0: 과거 3개월 백테스트로 초기 테이블 시드

Week 2: 통합 + 페이퍼
├── trade_builder.py (전체 파이프라인 조립)
├── trade_tracker.py (목표/손절 도달 자동 추적)
├── 텔레그램 [PAPER] 출력
└── Phase 1 시작: 페이퍼 트레이딩

Week 3~4: 학습 + 캘리브레이션
├── trade_learner.py (트레이드 단위 학습)
├── 기대수익률 테이블 실데이터 캘리브레이션
├── 손절/보유기간 통계 축적
└── Phase 1 마무리: 2주 페이퍼 결과 리뷰

Week 5~: 실전 전환
├── Position Guardian에 Trade Object 연결
├── PositionSizer 실전 가동 (초기 50% 제한)
└── Phase 2: 실거래 + 학습 루프 본격화
```
