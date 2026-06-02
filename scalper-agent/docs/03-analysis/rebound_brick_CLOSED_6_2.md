# 반등매매(rebound+피라미딩) brick — 최종 정리 · CLOSED (6/2)

> ★ 상태: **CLOSED / HOLD / shadow only / 실전 flip 금지.** 사장님 6/2 결정.
> 한 줄: **"rebound 룰 방향은 맞지만, 지금 데이터는 아직 켜지 말라."**

## 1. 룰 카드 (검증 대상이었던 고정 룰)
| 항목 | 값 |
|---|---|
| 관찰 | 전일 종가/저가 ≤ 전전일 종가 ×(1−3%) |
| 반등확인 | 당일 5분봉 종가 ≥ 전일종가 ×(1+1%) (1봉=primary, 2봉=비교) |
| 진입 | 확인 시 10% |
| 피라미딩 | +1%/+2%/+3%마다 30% (10→40→70→100) |
| 손절/트레일 | −3%(STOP3=primary) / −4%(STOP4=비교) |
| 자격게이트 | 끼≥MODERATE & VWAP/양봉/전일저점 score≥70 & 당일 ≤+30% |
| 유니버스 | ACTIVE(봇 풀)=primary / 블루칩 바스켓=control |

## 2. 최종 결론 (데이터)
- **일봉 +81%는 신기루** — 현실체결(−44%)+lookahead(−32%)+비용(−12%) 빼면 피라미딩 net +1.9~5.4%(ACTIVE 약음수).
- **엄격 walk-forward replay(2026-03-13~04-03, lookahead제거·비용)**: confirm1|STOP3 **net −0.36% / 휩쏘 77% / missedUp 11.7% > avoidLs 8.7%**.
- **판정**: net 미생존(≤0) + 휩쏘≥50% → **실전 투입 불가.** 선정 게이트는 작동(hard 2808→후보 371), 문제는 rebound 진입의 장중 휩쏘.
- **방향성은 보존**: STOP3>STOP4 · confirm1>confirm2 · 선정게이트 정상. regime(2~4월초 고휩쏘) 바뀌면(추세하락장 avoided>missed) 재평가 여지.

## 3. 검증 경로 (커밋)
| commit | 내용 |
|---|---|
| e3b2c76 | 정직 재검증 + shadow 설계서 |
| 57010fe | 종목선정 게이트 포함 shadow 관측 체계 + 1차결과 |
| ad9728f | **엄격 walk-forward replay (공식 기준)** — lookahead 제거판 |

★ **앞으로 rebound 분석의 유일 공식 기준 = `tools/recent_10d_rebound_replay_6_2.py`.** 이전 rebound_shadow_system의 −0.08% 등은 미래참조 부풀림이라 폐기.

## 4. 안전 증빙
- gate `audit --gate` exit 0 (8/8, 회귀 0) · selftest PASS · py_compile OK · Codex 5/5 PASS(Tier4).
- **실주문 0 · 주문함수 0 · `SAJANG.AUTO_TRADE_DISABLED=True` · SAJANG 무변경 · 매도경로/picks 무접촉 · scheduler/systemd 무변경 · 봇 OFF.**

## 5. 재개 조건 (사장님 승인 시에만)
1. **VPS 5분봉 동기화** → 진짜 최근(5월말) replay (현 결과는 가용최근 04-03까지일 뿐).
2. 그 replay에서 **net 양전환 AND 휩쏘 <50%**면 → 실시간 forward 배선(스케줄 brick, 4-Tier) 재검토.
3. 그 전까지 **OFF / HOLD / 실주문 0 / shadow only.**

## 6. 다음 장 (방향 전환)
- 반등 = "안 켜는 이유"를 찾고 닫음.
- 다음 brick = **`leader_momentum_ride_shadow`** — 주도주(삼성SDS·LG전자·LG CNS·로보스타·두산로보틱스 등)를 우리 시스템이 **언제 포착**했는지 + 올라타기(진입가/익절가/수익금/보유일/트레일링 성과) 검증. 실주문0·SAJANG0·scheduler0.
