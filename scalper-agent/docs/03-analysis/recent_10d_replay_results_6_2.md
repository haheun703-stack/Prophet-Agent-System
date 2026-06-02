# recent_10d_rebound_shadow_replay — 결과 (6/2)

> 도구: `tools/recent_10d_rebound_replay_6_2.py` (엄격 walk-forward, lookahead 금지, 비용포함, 실주문 0).
> 창: **2026-03-13 ~ 04-03 (10거래일)** — ★로컬 분봉이 04-03까지라 "가용 최근 10거래일". 달력상 진짜 최근(5월말)은 VPS 분봉 동기화 필요(별도).★
> 고정 룰: ACTIVE pool / STOP3 / confirm1 = primary. STOP4·confirm2 = 비교참고.
> 이전 하니스 대비 교정: 끼·유동성 = D-1까지 / score = 진입봉까지만(당일 종가·저가 미사용) = lookahead 제거.

## 사장님 요구 산출

**선정 퍼널(ACTIVE)**: hard 2808 → 전일-3% 1303 → 끼MODERATE+(D-1) 1234 → **후보(score≥70&confirm1) 371**
**탈락 TOP5**: LIQ_LOW 3257 · NO_DOWN_SETUP 1505 · NO_CONFIRM 562 · LOW_SCORE 293 · KKI_LOW 69
**missed_entry**(confirm2가 confirm1 대비 놓침): 37건

| 셀 | 진입 | net% | MDD% | whip% | false | 풀필 | missedUp% | avoidLs% |
|---|---|---|---|---|---|---|---|---|
| **confirm1\|STOP3** (primary) | 371 | **−0.36** | 1.16 | **77** | 286 | 98 | 11.66 | 8.69 |
| confirm1\|STOP4 | 371 | −0.40 | 1.85 | 61 | 228 | 144 | 11.20 | 8.38 |
| confirm2\|STOP3 | 334 | −0.42 | 1.20 | 73 | 243 | 101 | 11.54 | 8.78 |
| confirm2\|STOP4 | 334 | −0.59 | 1.84 | 57 | 190 | 135 | 11.19 | 8.40 |

**증빙**: `SAJANG.AUTO_TRADE_DISABLED=True` / 실주문 호출 0 / 주문함수 0 / SAJANG·scheduler·systemd 무변경 / HOLD.

## 판정 (사장님 4문항 + 한 줄)
| 질문 | 결과 | 답 |
|---|---|---|
| net 살아남는가? | confirm1\|STOP3 = **−0.36%** | **NO (미생존)** |
| 휩쏘율 <50%? | **77%** | **NO** |
| STOP3 우위 유지? | STOP3 −0.36 ≥ STOP4 −0.40 | YES |
| confirm1 우위? | c1 −0.36 ≥ c2 −0.42 | YES |

> ★ **"실제 forward로 갈 가치가 있는가" = 아직 없음.** net 음수 + 휩쏘 77% + **missedUp(11.7%) > avoidLs(8.7%)** = 손절이 회복할 자리를 더 잘라먹음(손절 自害). shadow 유지가 답. ★

## 정직 해석
1. **lookahead 제거하니 더 나빠짐**: 이전(오염) confirm1\|STOP3 net −0.08% → 엄격 walk-forward −0.36%. = 미래참조가 부풀렸던 것 재확인.
2. **missed_upside > avoided_loss** = 이 창에선 손절당한 종목이 평균적으로 떨어진 것(8.7%)보다 더 올랐다(11.7%). 손절이 추세를 자른 게 아니라 **휩쏘에 털린 것**. 단 이건 **고휩쏘/되돌림 regime색**(2~4월 초). 추세하락장이면 avoided가 지배할 것.
3. **STOP3·confirm1 상대우위는 유지** — 채택 방향은 맞으나, 전략 자체 절대 net이 음수라 켤 수 없음.
4. **선정 게이트는 작동**(2808→371 압축). 문제는 선정이 아니라 rebound 진입의 장중 휩쏘.

## 한계
- 가용분봉 04-03까지 = **달력 최근 아님**. 진짜 최근(5월말)은 VPS 분봉 동기화 후 재실행 필요.
- thin(10일·단일 regime, 고휩쏘 장세 가능) → net 부호 regime색. 휩쏘 지배(메커니즘)는 견고.
- ACTIVE = 1min 보유 전수(봇 실제 top_k 아님). 생존편향·시장경보 미적용(hook).

## 결론 / 다음
- **단타봇 HOLD / shadow only 유지가 데이터로 재확인됨.** flip 금지.
- 진짜 forward 판정 = ①VPS 5분봉 동기화로 달력최근 재실행 OR ②실시간 forward 누적 배선(스케줄 brick, 사장님 승인). 둘 다 사장님 신호 대기.
