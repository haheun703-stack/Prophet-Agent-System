# BRAIN 5D 스트레스 테스트 결과

실행 시각: 2026-03-07 21:58:05

## 검증 3-1: 데이터 누락 시뮬레이션

| 테스트 | 결과 | 상세 |
|--------|------|------|
| FRED 정상 캐시 존재 | PASS | 캐시 존재=True, rrp=519건 |
| FRED 다운 → 캐시 폴백 | PASS | 폴백 데이터 rrp=519건 |
| FRED 다운 → analyze_liquidity 정상 완료 | PASS | score=-0.9, level=NEUTRAL |
| CFTC 정상 데이터 | PASS | 240건 |
| CFTC 404 → 캐시 폴백 | PASS | 폴백 240건 |
| CFTC 404 → analyze_cot 정상 완료 | PASS | score=-2.6, signal=NEUTRAL |
| Yahoo 잘못된 티커 → graceful None | PASS | 반환값=None |
| Yahoo 티커 오류 → analyze_stress 생존 | PASS | stress=0.0, level=NORMAL |
| M2 60일 오래됨 → 분석 정상 완료 | PASS | score=-0.9, m2 date=2026-01-06 |
| M2 오래됨 → z-score 계산됨 | PASS | m2_z=0.0, m2_val=22442.1 |

## 검증 3-2: 극단값 테스트

| 테스트 | 결과 | 상세 |
|--------|------|------|
| 2D 4지표 경고 → IMMINENT | PASS | signal=IMMINENT |
| 2D 확신도 90% | PASS | confidence=0.9 |
| 2D 선제조정: 현금+10, 인버스+5 | PASS | adj={'cash_shift_pct': 10, 'inverse_shift_pct': 5, 'reason': '채권시장 경고 4/4 — 선제 방어'} |
| 3D 4쌍 붕괴 → CRITICAL | PASS | index=10.0 |
| 3D 스트레스 지수 ≥ 7.0 | PASS | index=10.0 |
| 3D 배분: 스윙 -25%p, 현금 +20%p | PASS | adj={'cash_add_pct': 20, 'inverse_add_pct': 10, 'swing_reduce_pct': 25, 'reason': '유동성 위기 — 방어 최우선'} |
| 4D 모든 계약 극단 → RISK_OFF | PASS | score=-8.1, signal=RISK_OFF |
| 4D 극단 카운트 ≥ 4 | PASS | extreme=7 |
| 4D 스코어 ≤ -5.0 (강한 RISK_OFF) | PASS | score=-8.1 |
| 5D TGA급등+M2마이너스 → CONTRACTION 이하 | PASS | score=-4.7, level=MILD_CONTRACTION |
| 5D 배분조정: 스윙 감소 | PASS | adj={'swing_shift_pct': -3, 'cash_shift_pct': 2, 'inverse_shift_pct': 0, 'reason': '완만한 수축 (-4.7) — 소폭 방어적'} |
| 5D 최악 시나리오 → DRAIN | PASS | score=-6.3, level=LIQUIDITY_DRAIN |

## 검증 3-3: 파이프라인 통합 테스트

| 테스트 | 결과 | 상세 |
|--------|------|------|
| 실행 순서: 2D→3D→4D→5D | PASS | 실제=['_load_regime_leading', '_load_cross_asset_stress', '_load_cot_smartmoney', '_load_liquidity_cycle'], 기대=['_load_regime_leading', '_load_cross_asset_stress', '_load_cot_smartmoney', '_load_liquidity_cycle'] |
| 4D 에러 → run_brain 생존 | PASS | regime=CAUTIOUS |
| 4D 에러 → cot_smartmoney 빈 dict | PASS | cot={} |
| 4D 에러 → 1D/5D 정상 | PASS | 1D=CAUTIOUS, 5D_level=NEUTRAL |
| 4D NEUTRAL → 텔레그램 생략 | PASS | COT 블록 생략됨 (OK) |
| 5D NEUTRAL → 텔레그램 생략 | PASS | 유동성 블록 생략됨 (OK) |
| 배분합 100% (PANIC) | PASS | 합=100%, pct={'v10_swing': 0, 'group_etf': 0, 'gold_etf': 25, 'inverse_etf': 15, 'small_cap': 0, 'cash': 60} |
| 배분합 100% (FEAR_EXTREME) | PASS | 합=100%, pct={'v10_swing': 15, 'group_etf': 10, 'gold_etf': 20, 'inverse_etf': 10, 'small_cap': 0, 'cash': 45} |
| 배분합 100% (RISK_OFF) | PASS | 합=100%, pct={'v10_swing': 30, 'group_etf': 20, 'gold_etf': 15, 'inverse_etf': 5, 'small_cap': 0, 'cash': 30} |
| 배분합 100% (CAUTIOUS) | PASS | 합=100%, pct={'v10_swing': 45, 'group_etf': 30, 'gold_etf': 5, 'inverse_etf': 0, 'small_cap': 5, 'cash': 15} |
| 배분합 100% (NEUTRAL) | PASS | 합=100%, pct={'v10_swing': 55, 'group_etf': 30, 'gold_etf': 5, 'inverse_etf': 0, 'small_cap': 10, 'cash': 0} |
| 배분합 100% (RISK_ON) | PASS | 합=100%, pct={'v10_swing': 45, 'group_etf': 25, 'gold_etf': 0, 'inverse_etf': 0, 'small_cap': 20, 'cash': 10} |
| 배분합 100% (EUPHORIA) | PASS | 합=100%, pct={'v10_swing': 30, 'group_etf': 15, 'gold_etf': 10, 'inverse_etf': 10, 'small_cap': 5, 'cash': 30} |
| 전차원 최악 → 합계 100% | PASS | 합=100% |
| 전차원 최악 → 방어자산(현금+인버스) > 60% | PASS | 현금=39% + 인버스=36% = 75% |
| 전차원 최악 → 스윙 < 20% | PASS | 스윙=0% |
| 전차원 최악 → 스윙 ≥ 0 (음수 아님) | PASS | 스윙=0% |

## 발견된 문제점

- [IMPROVEMENT] M2 데이터 45일+ 오래됨 경고 표시 기능 추가 필요

## 통계: 39/39 통과 (100%)