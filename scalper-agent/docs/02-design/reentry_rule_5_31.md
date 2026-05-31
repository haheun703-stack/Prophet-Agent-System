# 재진입 규칙 설계 — 손절+재진입(STOP_REENTER) + 선택적 추세라이드 (5/31)

> 단타봇 read-only 설계 → 사장님 승인 → 구현 → 검증. 단타봇 코드 직접수정 X (이 문서는 스펙).
> ★ 매도(보호매도) 무손상 / SAJANG 단일진실(리터럴 0.97 금지) / 게이트 8/8 회귀 0 / live OFF 유지 ★
> ★ 4단 스타일(Stage2/3)에 끼워넣는 "재진입" 조각. 선정(명분·끼)은 4단 설계 선행. ★

## 0. 대전제 (불변)
- 목적 = "흔들기에 -3% 털린 뒤, 추세 살아있으면 다시 타서" 수익 회복 + 단타 회전 유지.
- ★ 데이터 입증된 값만 박는다(감 금지). 검증 산출: `docs/03-analysis/stop_and_selection_study_5_31.md` + tools/{breakout_reentry,reentry_param_optimizer,reentry_ride_vs_fixed}_5_31.py (commit b286cf1·c391eda·e931bfd).
- ★ 이건 **강한 후보(가설)**. 절대수익은 생존편향 과대 → 라이브 전 숙제(5장) 필수. paper 먼저.

## 1. 데이터 근거 (오늘 검증, 요약)
1. **손절+재진입 > MDD 견디기**: 전 보유기간 +5~6%p (HOLD ≈ 0%). 견디기는 함정.
2. **끼↑ → 수익 단조**: 끼0 +4.56% → 끼5+ +9.98%. 高끼 선정(4단 Stage1)이 엣지.
3. **파라미터 최적(plateau·OOS 견고)**: 최대재진입=2 / 타이밍=당일종가 / 조건=손절선회복(reclaim) / 비중=풀(1.0). (익일시가 +2.60% << 종가 +5.58%; +MA5 효과 +0.01%p=무의미)
4. **보유 = 자본회전 트레이드오프**: 하루당수익 D+3 2.76%/일 > RIDE 1.00%/일(7.4일 보유). 거래당은 RIDE +7.42% > D+3 +4.96%. → **D+3 기본 + 高끼 선택적 RIDE 하이브리드**.

## 2. 룰 스펙 (재진입 = 기존 매도 후 새 lifecycle 단계)

### 2.1 트리거 체인 (기존 트레일 손절에 이어붙임)
```
보유 → (기존) 고점-3% 트레일 손절 발동 → 매도 체결(SAJANG.get_trailing_sl)
     → ★재진입 감시 등록★ (reentry_watch): last_stop_price·thesis·kki_grade·hold_deadline·re_count
     → 매 모니터/종가: 종가 >= last_stop_price (reclaim) AND 명분·끼 유효 AND 윈도우 내 AND re_count<MAX
       → 당일 종가에 재매수(기존 매수경로·order_intent 게이트 통과) → re_count++ → 트레일 재무장
```

### 2.2 파라미터 (전부 SAJANG, 데이터값)
| 항목 | 값 | 근거 |
|---|---|---|
| 재진입 조건 | 종가 ≥ 직전 트레일선(reclaim) | [1.3] reclaim 단독 충분 |
| 재진입 타이밍 | 당일 종가 | [1.3] 종가 >> 익일시가 |
| 최대 재진입 | **2회** | [1.3] plateau(2=3=∞), 칼줍기 방지 |
| 재진입 비중 | 풀(1.0) | [1.3] 1.0 > 0.5 |
| 기본 보유 | **D+3** | [1.4] 자본효율 최고(2.76%/일) |
| 선택적 RIDE | 高끼(EXPLOSIVE) + 트레일 미붕괴 + 신고가 지속 시 D+3 넘겨 추세 끝까지(캡 D+10) | [1.4] 高끼 RIDE +11.73% |

### 2.3 재진입 자격 게이트 (잡주 칼줍기 차단 = 4단 연동)
- **명분 유효**: thesis(Stage0) 안 깨짐 — invalidation_kind(NEWS_DECAY/SUPPLY_EXIT/STRUCTURE_BREAK) 미발동.
- **끼 유효**: kki_grade ≥ MODERATE (SLUGGISH는 재진입 금지 = 끼 없는 종목 재추격 X).
- **윈도우 내**: 최초 진입 후 hold_deadline(D+3, 高끼면 RIDE 연장) 이내만.
- **추세**: 종가가 트레일선 회복(reclaim)이 곧 추세 confirm. (RIDE 연장은 추가로 '신고가 갱신' 요구.)
- 하나라도 불만족 → 재진입 안 함(종료). = "방향 틀리면 안 탄다."

## 3. SAJANG 단일진실 추가 (`sajang_rules.py`)
```python
# ── 재진입 룰 (5/31, 데이터 검증값) — 주문룰 ──
REENTRY_ENABLED: bool = True              # 사장님 영구룰 default 활성(off 금지 원칙)
REENTRY_MAX: int = 2                       # 최대 재진입 횟수 (plateau)
REENTRY_AT: str = "close"                  # 당일 종가 체결
REENTRY_REQUIRE_RECLAIM: bool = True       # 종가 ≥ 직전 트레일선
REENTRY_MIN_KKI_GRADE: str = "MODERATE"    # 끼 없는 종목 재추격 금지
REENTRY_SIZE_RATIO: float = 1.0            # 풀 비중
HOLD_BASE_DAYS: int = 3                     # D+3 단타 기본
RIDE_KKI_GRADE: str = "EXPLOSIVE"          # 이 등급만 RIDE 연장
RIDE_MAX_DAYS: int = 10                     # RIDE 안전 캡
```
- 헬퍼: `SAJANG.should_reenter(close, last_stop, kki_grade, re_count, day_offset, thesis_ok) -> bool`,
  `SAJANG.effective_hold_days(kki_grade) -> int` (EXPLOSIVE→RIDE_MAX, else HOLD_BASE). **리터럴 0건.**
- `list_all_rules()` 노출 → self-audit 단일진실. (limit_up 도구 -4.5%·×0.97 하드코딩도 이 통로로 흡수)

## 4. 통합 위치 (구현 가이드 — Codex/단타봇)
- **재진입 감시 원장**: 손절 매도 시 `reentry_watch[code] = {...}` 기록(positions와 별개, 종료 시 제거).
- **체크 지점**: 기존 종가/모니터 잡(예 _job_monitor / EOD)에서 `should_reenter` 평가 → 통과 시 기존 매수 함수 호출(신규 주문경로 X = order_intent 게이트·cash30·AVOID 전부 재사용).
- **매도 무접촉**: 재진입은 매수경로만. 보호매도(트레일/룰B/C/D/EOD)·SAJANG 매도헬퍼 미수정.
- **No Intent No Order**: 재진입 매수도 record_order_intent(reason="REENTRY") 1:1. CHECK-1 경로 준수.

## 5. 라이브 전 숙제 (정직 — paper도 이거 통과 후)
1. **분봉 정밀**: 종가 재진입 체결 슬리피지 / 장중 -3% 정밀(일봉 근사 보정). data_store/1min 활용.
2. **비용 민감도**: 재진입 레그↑ = 비용↑. 0.48%/leg 가정 깨지면 엣지 축소 — 실비용 재측정.
3. **생존편향 보정**: 상폐 포함 데이터로 절대수익 재측정(현 절대값 신뢰 금지).
4. **OOS·6팩터 끼**: 프록시(급등빈도) 아닌 실제 score_kki(4단 Stage1)로 재검증.
5. **자본/슬롯**: D+3 회전이 효율적이려면 재투입 기회 충분해야 — 슬롯·현금(cash30) 모델과 양립 확인.

## 6. 검증 계획 (구현 후 단타봇)
1. `audit --gate` exit 0 유지 + `--baseline` new_violations=[] (게이트 8/8 회귀 0).
2. SAJANG 헬퍼만 사용(리터럴 곱셈 0) — RULE-005/006/007 통과. CHECK-6 무손상.
3. 매도 살아있음 회귀(트레일/룰B/C/D/EOD/sajang) 전 스위트 PASS.
4. paper 리허설: 손절→reclaim→재진입 시퀀스에서 record_order_intent(REENTRY) 1:1 + 자격게이트(끼/명분/윈도우) 작동. SLUGGISH·명분붕괴·윈도우초과 종목 재진입 안 됨 검증.
5. 단위테스트: should_reenter/effective_hold_days 경계값 + 리터럴 0건 assert.

## 7. 확인포인트 (사장님 결정 영역)
1. **RIDE 초기 활성?**: 보수적으로 **D+3-only로 시작**(REENTRY만), 高끼 RIDE는 paper 검증 후 켤지. (RIDE는 절대수익 과대 최대 = 신중)
2. **REENTRY_MIN_KKI_GRADE**: MODERATE 기본 — 더 빡빡(HUNTABLE)하게 할지.
3. **자본/슬롯**: 재진입이 새 슬롯을 먹나, 원래 슬롯 유지하나(현금 30% 룰과의 관계).
4. **명분 유효 판정 소스**: thesis invalidation을 어느 신호로 볼지(4단 Stage3 invalidation_kind 연동).

## 8. 순서 / 의존
- **의존**: 4단 Stage1(끼)·Stage3(thesis/invalidation) 선행 — 재진입 자격게이트가 끼·명분을 씀.
- **순서**: 게이트 8/8 baseline 고정(완료) → 4단 선정 구현 → 재진입 구현 → `--baseline` 회귀 0 → paper(5장 숙제 통과 후) → 사장님 승인 → live.
- Codex 전달 or 단타봇 직접구현(사장님 선택). 커밋 메시지에 audit 전후 + 검증 5종 명시.
