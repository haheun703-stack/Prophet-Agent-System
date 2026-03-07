# BRAIN 5D 실시간 크로스 체크

실행 시각: 2026-03-07 21:58:05

| 테스트 | 결과 | 상세 |
|--------|------|------|
| 1D 레짐 판정 | PASS | regime=CAUTIOUS |
| 2D 전환신호 유효 | PASS | signal=CLEAR, direction=UNCLEAR |
| 3D 스트레스 레벨 유효 | PASS | level=NORMAL, index=0.0, anomaly=0/4 |
| 4D COT 신호 유효 | PASS | signal=NEUTRAL, score=-2.6, date=2026-03-03 |
| 5D 유동성 레벨 유효 | PASS | level=NEUTRAL, score=-0.9 |
| 5D RRP z-score 범위 | PASS | rrp_z=0.15 |
| 5D TGA z-score 범위 | PASS | tga_z=-1.07 |
| 5D M2 z-score 범위 | PASS | m2_z=0.0 |
| FRED RRP 최신값 확인 | PASS | FRED=1.5B$ (2026-03-06) |
| FRED TGA 최신값 확인 | PASS | FRED=832B$ (2026-03-04) |
| FRED M2 최신값 확인 | PASS | FRED=22.4T$ (2026-01-01) |
| 최종 배분 합계 100% | PASS | 합=100%, pct={'v10_swing': 45, 'group_etf': 30, 'gold_etf': 5, 'inverse_etf': 0, 'small_cap': 5, 'cash': 15} |
| 센서 조정 없음 (모두 중립) | PASS | 2D~5D 전부 중립 → 조정 0 |

## 통계: 13/13 통과 (100%)