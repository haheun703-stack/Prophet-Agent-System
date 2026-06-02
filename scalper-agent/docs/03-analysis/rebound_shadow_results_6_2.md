# 하락후반등+피라미딩 shadow 관측 — 1차 결과 (6/2)

> 도구: `tools/rebound_shadow_system_6_2.py` (read-only, 실주문 0·SAJANG무변경·봇OFF).
> 데이터: 분봉 2026-02-19~03-30(~16일, **thin·단일 regime**) + stock_data_daily(보유·청산).
> ★ 절대수치 신뢰 X — 방향·셀간 상대비교만. forward 누적이 본검증. flip 금지. ★
> 끼 게이트 = 봇 실함수 `score_kki`(MODERATE컷30·유동성50억) 재사용 = 봇과 동일 진실.

## 사장님 요구 숫자

### 1) 선정 퍼널 + 탈락 사유 TOP5
| 유니버스 | hard통과 | 전일-3%setup | 끼MODERATE+ | **후보(score≥70&confirm1)** |
|---|---|---|---|---|
| ACTIVE(봇 풀) | 6,504 | 2,610 | 2,415 | **468** |
| CONTROL(사장님 바스켓) | 301 | 89 | 68 | **10** |

- ACTIVE 탈락 TOP5: LIQ_LOW 8866 · NO_DOWN_SETUP 3894 · NO_CONFIRM 1254 · LOW_SCORE 693 · KKI_LOW 193
- CONTROL 탈락 TOP5: NO_DOWN_SETUP 212 · LIQ_LOW 58 · NO_CONFIRM 44 · KKI_LOW 21 · LOW_SCORE 14
- → 게이트가 ACTIVE 6504 hard통과 → 468(7.2%)로 압축. 거름의 대부분은 유동성(거래대금<50억)·셋업부재. **선정 게이트는 작동**(질 압축 확인).

### 2) 3% vs 4% (net / MDD / whipsaw) + false_entry — confirm1 기준
| 유니버스 | stop | 진입 | net% | net원 | MDD% | whip% | false_entry | 풀필 |
|---|---|---|---|---|---|---|---|---|
| ACTIVE | **STOP3** | 468 | **−0.08** | −3.9M | 1.23 | 74 | 347 | 177 |
| ACTIVE | STOP4 | 468 | −0.28 | −13.2M | 2.07 | 58 | 271 | 235 |
| CONTROL | **STOP3** | 10 | **+0.13** | +0.13M | 1.09 | 40 | 4 | 3 |
| CONTROL | STOP4 | 10 | −0.20 | −0.20M | 1.53 | 30 | 3 | 3 |

### 3) 1봉 vs 2봉 (missed_entry / false_entry)
| 유니버스 | confirm | 진입 | net%(STOP3) | false_entry | missed_entry |
|---|---|---|---|---|---|
| ACTIVE | confirm1 | 468 | −0.08 | 347 | — |
| ACTIVE | confirm2 | 442 | −0.24 | 318 | **52**(2봉이 1봉 대비 놓침) |
| CONTROL | confirm1 | 10 | +0.13 | 4 | — |
| CONTROL | confirm2 | 10 | +0.16 | 4 | 0 |

### 4) 봇 OFF·실주문 0 증빙
- `SAJANG.AUTO_TRADE_DISABLED=True` / 하니스 실주문 호출 **0건** / SAJANG·매도경로·picks·order_intent 무접촉(순수 분석 import만).

## 정직 해석 (방향만)
1. **선정 게이트 = 작동**: 6504→468 질 압축. 탈락 대부분 유동성·셋업부재 = 의도대로.
2. **rebound 진입은 장중 휩쏘가 지배적(whip 55~74%)** = 일봉 백테 신기루의 실체. confirm/stop 무관하게 절반 이상 같은날 털림.
3. **net은 ACTIVE 전 셀 약음수(−0.08~−0.57%), CONTROL STOP3만 약양수(+0.13~0.16%)** = thin 창에선 marginal~breakeven. 사장님 백테 +81%와 정반대 스케일(=신기루 재확인).
4. **STOP3 > STOP4**(덜 음수/양수) — 타이트 손절이 빨리 잘라 손실 작음. 영구룰 -3%(STOP3)와 정합 = 4% 넓힐 이유 없음(thin).
5. **confirm2(2봉)는 도움 안 됨** — net 더 음수 + 진입 52건 놓침. 1봉이 나음(방향).
6. **CONTROL(블루칩)이 ACTIVE(소형 봇풀)보다 덜 깨짐** = 소형주 rebound가 더 휩쏘. 단 CONTROL 표본 10 = 신뢰 낮음.

## 한계 (선언)
- **thin**: 16거래일·단일 regime(2~3월, 고휩쏘 장세 가능) → net 부호는 regime색. 휩쏘 지배(메커니즘)는 견고, 절대 net은 X.
- ACTIVE=1min 보유 1668종목 전수(봇 실제 top_k 아님, 광의). 봇은 더 선별적.
- 생존편향(상폐부재)·시장경보 hard filter 미적용(hook).
- **본검증 = forward 누적**(live active pool·실시간 5분봉). 이 1차는 게이트·메커니즘 검증용.

## 다음
- forward 관측 배선(일일 shadow 기록) = 별도 brick(스케줄 연결, 워치리스트 shadow 패턴).
- 사장님 확인포인트(설계서 7장) 중 stop은 데이터가 STOP3 지지. confirm은 1봉 지지.
