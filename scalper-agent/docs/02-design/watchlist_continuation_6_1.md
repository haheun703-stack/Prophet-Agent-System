# 워치리스트 생성기 설계 — 휴면→첫급등 연속자 필터 (6/1, shadow-first)

> 단타봇 read-only 설계 → Codex 구현 + 사장님 승인 → 단타봇 검증. 이 문서는 스펙(코드 직접수정 X).
> ★ 순수 read-only 스캔(후보 JSON + 관측만) — 주문경로·매도 무접촉. 6/1부터 즉시 shadow 가동 가능(무위험). ★
> ★ 실행(진입/재진입)은 별도 = reentry_rule_5_31.md(완성). 이 문서는 그 앞단 "어떤 종목을 볼까"만. ★

## 0. 대전제 (불변)
- 목적 = 데이터로 검증된 "연속 갈 첫급등 종목"을 매일 **워치리스트 후보**로 산출 + forward 관측.
- ★ shadow-first: 후보 산출·기록·forward추적만. **자동매수 X / picks 변경 X / 매도 무접촉.** (4단 shadow와 동일 철학)
- 실행은 reentry_rule_5_31.md(STOP_REENTER, SAJANG 단일통로, paper 게이트) — 검증 후에만, 절대 자동 X.

## 1. 데이터 근거 (5/31 검증, 요약)
- **연속자 필터 입증**(surge_filter_reentry_5_31.py, commit db5cd76): 휴면→첫급등 + 종가강도≥0.8 + 거래량≥5x + 당일상승≤30%(과열회피) → STOP_REENTER **리프트 +1.9~2.4%p·승률 +7~8%p**, 전 regime(FULL=약세포함 +1.89p) 견고. OHLCV만으로 계산.
- **연속자 vs 페이더 판별**(dormant_first_surge_supply, 20f89a7): 연속자 종가강도 0.86(≥0.8→연속 66% vs 위꼬리 27%)·거래량 20x vs 11x·과열아님(페이더 T0 +39.5% = blow-off).
- ★ **단순 외인/기관 순매수 부호는 예측력 0**(오히려 역상관) — 합계가 세력 가림(11주체 세력=금융투자 라인, market_investor_collector 수집중). → 이번 워치리스트는 **OHLCV 필터만**, 수급은 hook으로 확장.
- ★ 정직: 절대수익은 초강세장 베타 과대(상대 리프트만 신뢰) / 생존편향(상폐부재) / 수급·분봉 미반영 → forward shadow가 본검증.

## 2. 워치리스트 생성기 스펙 (신규 `data/watchlist_continuation.py`)

### 2.1 탐지 (휴면→첫급등 연속자 후보)
```
입력: 일봉(stock_data_daily 또는 data_store/daily) 전 유니버스
조건(전부 충족):
  휴면     : 직전 PRE(=20)일 최대 일간상승 < DORMANT_MAX(=10%) AND 기지 pos20 ≤ BASE_POS(=60%)
  첫급등   : 당일 종가상승 ≥ SURGE_MIN(=15%)
  연속자필터: 종가강도 ≥ CS_MIN(=0.80) AND 거래량 ≥ VOL_MIN(=5.0)×MA20 AND 당일상승 ≤ OVERHEAT(=30%)
  유동성   : 거래대금 ≥ LIQ_FLOOR(=30억) (못 사는 끼 제외)
거래일 가드: trading_calendar.is_trading_day (휴일 스캔 skip — flow_market 버그 교훈)
```
- 임계는 전부 SAJANG 상수(아래). 5/31 검증값이나 **잠정**(forward shadow + 튜닝으로 조정).

### 2.2 후보 피처 (각 종목)
- code/name/급등%/종가강도/거래량배수/거래대금억
- **끼**: `score_kki`(limit_up_scanner.py, 재사용) + kki_grade
- (hook, 미사용) 수급: 11주체 세력(금융투자) 잔류 — market_investor_collector(시장) / 키움 opt10059(종목별, 사장님 결정 후). 지금은 None.

### 2.3 출력 (shadow)
- `data_store/watchlist/continuation_{YYYYMMDD}.json` : 후보 리스트 + 피처 + 생성시각.
- 로그 2줄: `[워치리스트/shadow] N후보 (끼분포 {...}) 평균종가강도 X`.
- ★ recommendation.json/picks **불변** — 별도 파일, 관측 전용.

## 3. shadow forward 추적 (검증 인프라)
- `recommendation_tracker.py`(save_recommendation_snapshot 패턴, line 44) 재사용 또는 병렬 `watchlist_tracker`:
  - 매일 워치리스트 종목의 **T+1/T+2/T+3 forward 수익**(종가·고점) 자동 기록.
  - 누적 → "필터 통과 후보가 실제 forward 연속/수익 났나"를 현 regime·실시간으로 검증(백테스트 생존편향 회피).
- ★ ≥10 거래일 누적 + 연속률·forward수익 확인 → 그때 사장님께 "실행(reentry) 배선" 제안. 관측 없이 실행 금지.

## 4. 다운스트림 (이미 설계됨, gated)
- 검증된 후보 → reentry_rule_5_31.md 의 STOP_REENTER lifecycle(D+3 기본, 최대재진입2, 高끼 RIDE).
- ★ 그건 **새 매매행동** → SAJANG 단일통로 + 게이트 8/8 회귀0 + 매도 무손상 + No Intent No Order + **paper 먼저, 자동 금지.** 이 워치리스트 설계는 거기까지 안 감(후보 공급까지만).

## 5. SAJANG 단일진실 추가 (`sajang_rules.py`)
```python
# ── 워치리스트(휴면→첫급등 연속자) 선정 임계 (6/1, 검증값·잠정) — 주문룰 아님 ──
WL_PRE_DAYS: int = 20
WL_DORMANT_MAX: float = 0.10     # 휴면 = 직전20일 최대상승 < 10%
WL_BASE_POS: float = 0.60        # 기지 상한
WL_SURGE_MIN: float = 0.15       # 첫급등 최소
WL_CS_MIN: float = 0.80          # 종가강도 (연속자 판별 1번)
WL_VOL_MIN: float = 5.0          # 거래량 MA20 배수
WL_OVERHEAT: float = 0.30        # 과열회피(blow-off 차단)
WL_LIQ_FLOOR_억: float = 30.0
```
- `list_all_rules()` 노출. ON/OFF 플래그 금지(생성기는 항상 돌되 shadow=관측만이 안전밸브).

## 6. 검증 (Codex 구현 후 단타봇)
1. 순수 read-only 증명: diff가 주문경로(open/close_position·order_gate·record_order_intent)·SAJANG 매도헬퍼 **미접촉**(grep + audit). **게이트 8/8 PASS 회귀 0**(`--baseline`).
2. 매도 살아있음 회귀 전 스위트 PASS(워치리스트는 매매 무관이나 안전 재확인).
3. py_compile + 단위테스트: 탐지 조건 경계값(종가강도 0.79/0.80, 거래량 4.9/5.0, 과열 29/31%) + is_trading_day 가드.
4. shadow 실측: 6/1 스캔 → continuation_*.json 생성 + 피처 정확 + recommendation/picks 불변 확인.

## 7. 순서 / 확인포인트
- **순서**: 생성기+forward추적 구현(shadow) → 6/1~ 누적 → ≥10거래일 검증 → (사장님 승인) reentry 실행 배선(paper) → live.
- 확인포인트(사장님): ① 생성기 가동 시점(G6 15:40 _job_collect_daily에 연결 vs 16:45 evening vs 독립 job) ② 임계 튜닝(종가강도 0.8 vs 0.9 / 거래량 5x vs 10x — 리프트 vs 선택률) ③ 수급 hook 우선순위(시장 11주체 먼저 vs 키움 종목별 대기).
- ★ 정직: 백테스트 검증은 상대 리프트(+2%p)만 신뢰, 절대치 베타 과대. 수급 미반영. forward shadow가 진짜 검증.
