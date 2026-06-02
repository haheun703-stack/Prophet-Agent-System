# 하락후반등 + 피라미딩 진입 — shadow 설계 (6/2)

> 단타봇 read-only 설계 → 사장님 승인 → shadow 구현(관측) → paper → live. 단타봇 코드 직접수정 X (이 문서는 스펙).
> ★ 매도(보호매도) 무손상 / SAJANG 단일진실(리터럴 금지) / 게이트 8/8 회귀 0 / 봇 OFF·picks 불변 유지 ★
> ★ 출처: 사장님 flowx 세션 백테 → 단타봇 6/2 정직 재검증(`tools/rebound_pyramid_verify_6_2.py`). 신규 매수 archetype(RULE_D·재진입과 별개). ★

## 0. 대전제 (불변)
- 목적 = "전일 크게 빠진 종목이 오늘 전일종가 +1%를 **5분봉 종가로 회복·유지**하면 10% 정찰 → 오를수록 빠르게 불타기(10→40→70→100) → -3% 밀리면 전량 손절". 하락장에서 물타기 대신 손절+반등재진입.
- ★ **shadow 먼저**: 자동매수 X / picks 불변 / 기록·로그만 → forward 관측 → (사장님 승인) paper → live. 관측 없이 flip 금지(검증전 가동 사고 재발 방지).
- ★ **정직 sizing**: 이건 "+80% 캐시머신" 아님. 정직 net(아래 1장)은 6개월 **+1.9~5.4%(피라미딩)**, 사실상 marginal. 엣지는 **승률 아니라 손익비**(실승률 ~25~40%). 베타·생존편향·일봉 잔존 → 절대수익 신뢰 금지, 상대·방향만.

## 1. 데이터 근거 (단타봇 6/2 정직 재검증)
사장님 백테(일봉 6개월, 2025-12-02~2026-06-02, 종목당 1천만)를 **같은 데이터·종목**으로 재현 후 편향 분해:

| | boss 재현 | +현실체결 | +lookahead제거 | +비용 = **정직 net** |
|---|---|---|---|---|
| 대형 B 풀진입 | +122.2M(승77%) | +68.0M | +29.2M | **+14.2M(38%) +9.5%** |
| **대형 P 피라미딩** | +44.5M(70%) | +17.1M | +9.1M | **+2.8M(24%) +1.9%** |
| 중소형 B 풀진입 | +122.5M(69%) | +53.3M | +6.8M | **−7.7M 적자** |
| **중소형 P 피라미딩** | +42.8M(59%) | +16.3M | +13.5M | **+7.5M(25%) +5.4%** |
| A 물타기 | +28.6/47.0M | … | … | **둘 다 net 적자** |

- **재현 검증**: boss열 = 사장님 JSON과 일치(도구 신뢰). +81%의 88% 증발 = 비용(-12%)이 아니라 **현실체결(-44%)+lookahead(-32%)**.
- ★ **lookahead가 핵심**: 승률 77%→38%. 70%대 승률은 "양봉으로 끝난 날만 골라 진입한"(green_day=종가 미래참조) 인공물. → **5분봉 종가확인이 이 교정**(2.2절).
- **분봉 실측 확인**(45이벤트, 2/19~3/30, thin·방향만): 같은날 -3~4% 손절 휩쏘 **29%**(일봉모델 "진입일 무손절" 거짓) / 음봉마감 **44%**(일봉 green필터가 사후제거한 것 = lookahead 실재). 재진입 #1→#2(일봉5배→분봉0.5배) 패턴과 동일.
- **방향 결론(신뢰)**: 손절+재진입(B/P) > 물타기(A, net 적자). 단타봇 5/31 STOP_REENTER(손절+재진입>홀딩 +5~6%p)와 독립 일치. 단 magnitude는 breakeven~한자리%.

## 2. 룰 스펙 (신규 매수 archetype = REBOUND_PYRAMID)

### 2.1 트리거 체인
```
관찰: 전일 종가 OR 전일 저가 ≤ 전전일 종가 ×(1−DOWN_TRIGGER)   (전일 -3%↓ 셋업)
확인: 당일 5분봉 종가 ≥ 전일종가 ×(1+CONFIRM_PCT)  연속 CONFIRM_BARS봉   (★ lookahead제거: 일봉 양봉필터 대체)
     AND 자격게이트(2.3) 통과
진입: 확인 시 종목예산의 10% 매수 → anchor=확인가
불타기: 현재가 ≥ anchor×(1+LV1/2/3%) 각 도달 시 30%씩 추가 (10→40→70→100)
손절: 현재가 ≤ max(평단×(1−STOP), 고점×(1−TRAIL)) → 전량 매도 (기존 보호매도 헬퍼)
당일재진입: 손절·트레일 후 같은 날 재진입 금지(SAME_DAY_REENTRY=False)
```

### 2.2 핵심 교정 — 5분봉 종가확인 (lookahead 제거)
- 백테 `green_day = close>open`(종가 정보)는 미래참조 → 일봉 +81% 신기루의 32%p.
- 실전/shadow = **5분봉 종가가 전일종가+1% 위에서 마감(CONFIRM_BARS봉 유지)**할 때만 확인 = 종가 안 훔쳐봄. 사장님이 직접 지목한 교정.
- 데이터: `data_store/1min`(5분봉 집계) — 분봉수집기 재사용.

### 2.3 자격 게이트 (잡주 추격 차단 = 4단·재진입과 동일 철학)
- **명분 유효**: thesis(4단 Stage0) 미붕괴(invalidation_kind 미발동).
- **끼 유효**: kki_grade ≥ MODERATE (SLUGGISH 진입 금지).
- **과열회피**: 당일 상승률 ≤ 30%(blow-off 추격 금지 — 연속자 study와 정합).
- **유니버스**: 봇 기존 스캔 풀 ∩ 반등셋업. ★ 고정 27종 블루칩 바스켓 아님(백테는 블루칩 — 라이브 유니버스는 확인포인트 #4).
- 하나라도 불만족 → 진입 안 함.

## 3. SAJANG 단일진실 추가 (`sajang_rules.py`)
```python
# ── 하락후반등+피라미딩 (6/2, 정직검증값) — 주문룰, shadow 우선 ──
REBOUND_PYRA_ENABLED: bool = False         # ★shadow 단계 자동매수 OFF. flip=사장님 승인+paper후
REBOUND_PYRA_SHADOW: bool = True           # 관측 기록만(picks·주문 불변)
REBOUND_DOWN_TRIGGER_PCT: float = 3.0      # 전일 -3%↓ 셋업
REBOUND_CONFIRM_PCT: float = 1.0           # 전일종가 +1% 회복
REBOUND_CONFIRM_TF_MIN: int = 5            # 확인 타임프레임 = 5분봉
REBOUND_CONFIRM_BARS: int = 1              # 5분봉 종가 +1% 위 유지 봉수(lookahead제거)
PYRA_ADD_LEVELS_PCT: tuple = (1.0, 2.0, 3.0)        # anchor 대비 추가매수 트리거
PYRA_ALLOC_PCT: tuple = (10.0, 30.0, 30.0, 30.0)    # 10→40→70→100 (합 100)
REBOUND_STOP_PCT: float = 3.0              # ★ SAJANG.TRAILING_PCT(-3% 영구룰) 일관. 백테 4%는 확인포인트 #1
REBOUND_SAME_DAY_REENTRY: bool = False     # 손절 후 당일 재진입 금지
REBOUND_MIN_KKI_GRADE: str = "MODERATE"    # 끼 자격게이트
REBOUND_MAX_SURGE_PCT: float = 30.0        # 당일 +30%↑ 과열 추격 금지
```
- 헬퍼(리터럴 0): `rebound_setup_ok(prev_close, prev_low, prev_prev_close)`, `rebound_confirm_price(prev_close)`, `pyra_add_price(anchor, idx)`, `pyra_alloc_amount(symbol_budget, idx)`, `rebound_eligible(kki_grade, thesis_ok, today_surge_pct)`.
- `PYRA_ALLOC_PCT` 합=100 assert. `list_all_rules()` 노출(self-audit 단일진실).
- ★ stop/trail은 **SAJANG.TRAILING_PCT(3%) 일관** — 백테 4%값 하드코딩 금지. 3% 기준 정직 net 재측정 = 숙제(5장).

## 4. shadow 통합 위치 (구현 가이드)
- **shadow 원장**: `data_store/shadow/rebound_pyramid_{date}.json` — would-be 진입/추가/손절을 기록만(체결 X). continuation 워치리스트 shadow·4단 브릭과 동일 패턴.
- **체크 지점**: 기존 분봉/모니터 잡에서 신호 평가 → shadow 기록 + 로그 2줄. **주문 함수 호출 X, picks 변경 X, asset_pool 정렬 X.**
- **매도 무접촉**: shadow는 매수측 시뮬만. 보호매도(트레일/룰B/C/D/EOD)·SAJANG 매도헬퍼 미수정.
- **No Intent No Order**: flip(paper/live) 단계서만 record_order_intent(reason="REBOUND"/"PYRAMID_ADD") 1:1. shadow 단계는 주문 자체가 없음(intent도 없음).
- **봇 OFF 유지**: AUTO_TRADE_DISABLED 무관(shadow는 매수경로 안 탐).

## 5. 라이브 전 숙제 (정직 — paper도 이거 통과 후)
1. **분봉 정밀(후보 유니버스)**: 6/2 스팟체크는 16일·블루칩. flip 전 **실제 후보 유니버스의 분봉 forward**로 휩쏘·5분봉확인 슬리피지 재측정.
2. **SAJANG 3% stop 정직 net**: 본 검증은 4% — 영구룰 3%(타이트)로 휩쏘↑ 가능. 3% 기준 net 재측정 필수.
3. **비용·슬리피지**: 추격 레그↑=비용↑. 5분봉확인 진입가 슬리피지 실측(0.48%/왕복 가정 점검).
4. **생존편향·regime**: 상폐 포함 재측정 + 6개월 단일 regime(하락장 프레임) → 강세장 재현 확인. 절대수익 신뢰 금지.
5. **유니버스 정합**: 블루칩 바스켓 엣지 ≠ 봇 끼/상한가 소형 유니버스. 어디에 적용할지 확정 후 그 유니버스로 재검증.
6. **자본/슬롯**: 10→100% 피라미딩이 cash30·슬롯 모델과 양립(한 종목이 예산 100% 먹는 구조 점검).

## 6. 검증 계획 (shadow 구현 후 단타봇)
1. `audit --gate` exit 0 + `--baseline` new_violations=[] (게이트 8/8 회귀 0).
2. SAJANG 헬퍼만 사용(리터럴 곱셈 0) — RULE-005/006/007 통과. CHECK-6 무손상.
3. 매도 살아있음 회귀(트레일/compute_real/dynamic/guards/룰D/vwap/restart/sajang) 전 스위트 PASS.
4. shadow 리허설: 반등셋업→5분봉확인→would-be 진입/추가/손절 기록 1:1 + 자격게이트(끼/명분/과열) 작동(SLUGGISH·명분붕괴·+30%과열 진입 안 됨). **주문경로·picks 무접촉 확인(audit_order_paths 미수정).**
5. 단위테스트: rebound_setup_ok/confirm_price/pyra_alloc 경계 + PYRA_ALLOC 합100 + 리터럴 0건 assert.

## 7. 확인포인트 (사장님 결정 영역)
1. **stop 3% vs 4%**: 영구룰 -3% 일관 권장(SAJANG.TRAILING_PCT). 백테는 4% — 3%로 재측정 후 결정.
2. **첫 진입 비중·피라미딩**: 10→40→70→100 그대로 vs 보수적(반등확인 후 더 크게=B형은 변동성↑ 적자위험). 정직 net은 P > 안정적.
3. **REBOUND_MIN_KKI_GRADE**: MODERATE 기본 — 더 빡빡(HUNTABLE)?
4. **유니버스**: 블루칩 바스켓 / 봇 기존 끼·상한가 풀 / 둘 다. (엣지 측정은 블루칩서 — 적용처 확정 필요.)
5. **5분봉 확인 봉수(CONFIRM_BARS)**: 1봉 vs 2봉(더 빡빡=가짜돌파 차단↑ 진입↓).

## 8. 순서 / 의존
- **의존**: 4단 Stage1(끼)·Stage3(thesis) — 자격게이트가 끼·명분 사용(4단 shadow 브릭1~4 완료).
- **순서**: 게이트 8/8 baseline 고정(완료) → SAJANG 상수·헬퍼 추가 → shadow 원장·로그 배선 → `--baseline` 회귀 0 → forward 관측(≥10거래일, 휩쏘·5분봉확인·자격게이트 분포) → 5장 숙제 통과 → 사장님 승인 → paper → live.
- 단타봇 직접구현 가능(순수 read-only/shadow, Codex 키 불요) or Codex 병행. 커밋 메시지에 audit 전후 + 검증 5종 명시.
