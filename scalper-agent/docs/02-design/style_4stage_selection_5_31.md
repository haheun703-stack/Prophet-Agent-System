# 매매 스타일 설계 — "명분·끼부림·맥점·시나리오" 4단 종목선정 재배선 (5/31)

> 단타봇 read-only 설계 → Codex 구현 + 사장님 승인. 단타봇 코드 직접수정 X.
> 사장님 5/31 명령: *"넌 어떻게 수익률 높이는 스타일을 구축할래? 우리가 가진 엔진을 통합할 건데 어떻게?"*
> ★ 매도(보호매도) 죽이면 실패 / SAJANG 단일진실 / ON·OFF 플래그 금지(degrade가 안전밸브) / audit 게이트 회귀 0. ★

---

## 0. 대전제 (불변)

- 목적 = **자동매매 재가동 + 수익률 개선**. 단순 방어 아님. 당일~D+3 단타 수익 실현.
- 진단(실측): 종목선정은 **신호가 부족한 게 아니라 스타일(뼈대)이 없음**. 44개 신호를 단순 합산하고 유일 게이트가 `total_score > 0`(`morning_recommendation.py:2062`). → 명분 없이 약신호 더미로도 뽑힘.
- 해결: 사장님 철학 **명분(종목선정 80%)·맥점(진입)·줄먹(청산) + 7요소**를 뼈대로 기존 엔진 재배선. 약점 **5(끼부림)·7(시나리오)** 보강.
- ★ 중대 발견: 약점5·7은 이미 ~80% 구현됨 → **신규 모듈이 아니라 "재배선 + 게이트 추가"**. 회귀 위험 大폭 감소.
- ★ 정직 단서: 수익 보장 공식 아님. 약신호 더미 픽을 죽이고 끼·시나리오를 명시화해 엣지를 올리는 **규율**. 70억 미션엔 알고리즘보다 먼저.

---

## 1. 조사 확정 (단타봇 직접 Read — 추정 명시)

### 1.1 현 종목선정 = 단순 합산 + 단일 게이트 (`morning_recommendation.py`)
- `_step5_cross_validate()` (951~2096): 신호 루프(1402~2060) → `raw_total = relay+premove+tech+bargain+nat+tv+bomb+... (44 가산항)` (1883-1908) → `total = raw_total × regime_mult` (1984) → softcap(1964)·no_flow_pen(1975) 보정.
- **유일 hard cutoff = `total_score > 0`** (2062-2063, 주석 그대로 "유일한 hard cutoff"). 정렬(2066) → TOP8 + TV전용슬롯3(2068-2096).
- `RecommendedStock` (30-84): code/name/close/각 score/total_score/today_chg/relative_str/entry/sl/tp/sl_source/sources/confidence/tv_*/fib_* 보유. **thesis(명분)·끼점수·시나리오 계획 필드 없음.**
- `run_evening_recommendation()` (2188): 16:45 저녁 6단계 파이프라인. Step1 릴레이~Step6 KIS검증.

### 1.2 회귀 경계 = recommendation.json 4키 (`auto_trader.py`)
- 매수측은 `recommendation.json`에서 **`signal_type`/`grade`/`code`** 만 읽어 AVOID·D·F 차단 (2349-2369, 단타봇 직접 검증). predawn은 `total_score`(100+)·`grade` 사용(Agent 보고, 구현 전 라인 재확인).
- → **`code/signal_type/grade/total_score` 4키 의미만 보존하면 매수·매도 경로 무손상.** 매도(보호매도)는 SAJANG+보유포지션 기반, recommendation.json 안 읽음 → 선정 변경이 매도에 닿을 구조적 경로 없음.

### 1.3 점수경로는 **둘** (단타봇 직접 검증)
1. `morning_recommendation._step5_cross_validate` → recommendation.json (저녁 픽 + AVOID).
2. `asset_pool_loader` (장중 랭킹) → `ranked` → auto_trader 매수. 끼 모순 패널티가 여기 있음(1.5).

### 1.4 약점5·7 기존 자산 (단타봇 직접 검증한 핵심 2건 + Agent 보고)
- ✅ **직접 검증**: `TradeObject` (`trade_object.py:56`) = 이미 시나리오 90% (trade_id/total_score/sources/regime/confidence/entry_price/stop_loss/stop_source/target_price/rr_ratio/conviction/expected_hold_days/time_stop_days/status/close_reason) + `invalidation_narrative()` (107, SUPPORT/INST_COST/MOMENTUM 분기).
- ✅ **직접 검증**: `LimitUpStock` (`limit_up_scanner.py:59-80`) = 끼 원자료 전부 (change_pct/volume_ratio/trading_value_억/market_cap_억/turnover_pct/close_strength/consecutive_limit/score/pattern). 상수 LIMIT_UP_PCT=29.5/NEAR_LIMIT_PCT=25/STRONG_SURGE_PCT=15.
- (Agent 보고, 구현 전 재확인) `IndicatorCalc.atr` (`indicator_calc.py:138`), `SupplyAnalyzer._score_volatility`/`calc_baseline`/`BaselineLevels.invalidation` (`supply_analyzer.py:873/1149/254`), `HoldPeriodEngine`/`STYLE_PROFILES` (`trade_object.py:262`/`trade_style_decider.py:47`), `score_continuation` (`limit_up_scanner.py:991`), `TargetState` (`dynamic_target.py:51`).

### 1.5 ★ 모순 발견 (단타봇 직접 검증) ★
- `asset_pool_loader.py:1017-1096` `HIGH_VOLATILITY_SECTORS = {의약품·제약·의료정밀·바이오·헬스케어·엔터·게임}` → 해당 섹터 **`score -= 5`** (1095-1096).
- 주석 그대로: *"★ 5/21 박사 자율 — 섹터 변동성 패널티 (개선 액션 #2 단순 버전) ★ ATR 백테스트는 주말 작업. 우선 변동성 큰 섹터 -점. 5/21 로킷헬스케어 -15.4% 사고."*
- → **"끼=수익원(단타)" 철학과 정면충돌.** 단 5/21 사고 방지 목적이 진짜 → **단순삭제 금지**. 종목별 끼점수 + 시나리오 invalidation으로 리스크 이전(아래 Stage1·확인포인트).

---

## 2. 4단 설계 (재배선 — 신규 엔진 X)

### Stage 0 — 명분 게이트 (왜 오르는데? = 종목선정 80%)

**목적**: "약신호 더미 픽" 사살. total>0 단일게이트를 명분 통과로 강화.

**(a) RecommendedStock 신규 필드** (`morning_recommendation.py:84` 부근, 전부 기본값 = 하위호환 안전)
```python
thesis_buckets: dict = field(default_factory=dict)  # {"재료":0.0,"수급":0.0,"기술위치":0.0,...} 정규화값
thesis_strong: list = field(default_factory=list)   # 강(强) 통과 버킷명 리스트
thesis_pass: bool = True                            # Stage0 통과 (기본 True = fail-open/degrade 안전)
kki_score: float = 0.0                              # Stage1 끼점수 0~100
kki_grade: str = ""                                 # EXPLOSIVE/HUNTABLE/MODERATE/SLUGGISH
scenario: dict = field(default_factory=dict)        # Stage3 시나리오 객체 직렬화
```

**(b) 버킷 집계 위치**: 루프 내 `rec = RecommendedStock(...)` **직전** (현 2022 부근, 신호 지역변수 `relay_sc/nat_sc/tv_direct/tech_sc/...`가 살아있는 유일 지점). 신규 헬퍼 `_aggregate_thesis_buckets(locals 신호들) -> (buckets, strong, thesis_pass)` 호출 후 `rec`에 부착.

**(c) 24신호 → 7요소 버킷 매핑** (변수명: 코드상 상한)

| 버킷 | 신호 (변수 : 상한) | 강(强) 정규화 기준 |
|---|---|---|
| 재료/모멘텀 | tv_direct(~30)·relay_sc(45)·premove_sc(30)·surge_sc(10)·us_relay_sc(10)·rotation_bonus(12) | norm ≥ 0.6 |
| 수급 | nat_sc(50)·nat_power_sc·doublebuy_sc(18)·largecap_sc(~34)·fi_sc(15)·invflow_sc·stflow_sc·bomb_sc(15)·dual_buy_sc(12)·etf_flow_sc | norm ≥ 0.6 |
| 기술위치 | tech_sc(25)·trix_sc(25)·fib_adj(~20)·eq_adj·gap_adj·or_bias_adj | norm ≥ 0.6 |
| 재무 | pension_sc(15)·시총(cap) 프록시 | pension S/A (추측 — 1.4 확인포인트) |
| 스케줄 | event_risk(report) | event HIGH/EXTREME = 약화 |
| 끼부림 | Stage1 kki_score | (Stage1) |
| 시나리오 | Stage3 출력 | n/a (입력 아님) |

- 정규화 = `min(1.0, max(0, score) / 버킷대표상한)`. 음수 페널티는 정규화 0.
- 강 임계 `SAJANG.THESIS_GATE_STRONG_NORM` (초안 0.6).

**(d) 게이트 규칙**: `{재료, 수급, 기술위치}` 중 **강 ≥ `SAJANG.THESIS_GATE_MIN_STRONG`(초안 2)** → `thesis_pass=True`. 재무/스케줄/끼는 modifier(자격박탈 아님).

**(e) cutoff 강화 (line 2062 교체 — 삭제 X)**
```python
gated = [c for c in candidates if c.total_score > 0 and c.thesis_pass]
if len(gated) < SAJANG.THESIS_MIN_CANDIDATES:          # 초안 3
    gated = [c for c in candidates if c.total_score > 0]  # degrade: 기존 합산 복귀
    logger.warning(f"[Stage0 DEGRADE] 명분통과 {len(...)} < {SAJANG.THESIS_MIN_CANDIDATES} → 합산 fallback")
candidates = gated
```
- 정렬(2066)·TV전용슬롯(2068-2096) **그대로 유지**. 합산 total_score = 게이트 통과 후 **랭킹용**(자격 binary ⊥ 서열 continuous).
- **ON/OFF 플래그 없음** — degrade가 안전밸브. `thesis_pass` 기본값 True = 버킷계산 예외 시 fail-open.

**(f) 산출**: `sources`에 `thesis(재료+수급)` 태그 추가 + JSON에 buckets breakdown (감사·텔레그램 리포트·shadow 관측).

---

### Stage 1 — 끼부림 필터 (★약점5★ = 단타 수익원)

**(a) 신규 `score_kki()`** (`limit_up_scanner.py:991` `score_continuation` 옆, 동일 패턴). 0~100, 합 100:

| 팩터 | 배점 | 산식 (원자료) |
|---|---|---|
| F1 변동성기질 | 35 | ATR% (`IndicatorCalc.atr` 재활용): >6%→35 / >4.5→28 / >3.5→20 / >2→10 / else 3 |
| F2 급등빈도 | 25 | 최근 60일 +15%↑일수×2 + 상한가일수×4, min(25, ·) |
| F3 회전율 | 15 | turnover_pct: ≥20→15 / ≥10→12 / ≥5→8 / ≥2→4 |
| F4 연속성 | 10 | consecutive_limit: ≥3→10 / ≥2→7 / ≥1→3 |
| F5 종가강도 | 10 | close_strength: ≥0.95→10 / ≥0.8→7 / ≥0.6→4 |
| F6 거래량배수 | 5 | volume_ratio: ≥10→5 / ≥5→4 / ≥3→2.5 / ≥2→1.5 |

- 시그니처 초안: `score_kki(stock: LimitUpStock, atr_pct: float, surge_days: int, limit_days: int) -> float`.
- F2 `surge_days`/`limit_days` 카운트 헬퍼는 신규(daily CSV에서 `_calc_change_pct` 반복). ATR/turnover/volume_ratio는 **재활용**.
- kki_grade: ≥70 EXPLOSIVE / ≥50 HUNTABLE / ≥30 MODERATE / else SLUGGISH.

**(b) 랭킹 반영 (필터 X — "hard gate 제거" 철학 준수)**: `asset_pool_loader.py:1098` `_continuation_score_adjustment` 패턴으로 `_kki_score_adjustment(kki)` 추가:
```
kki < 30 → -15   (저변동 우량주 = 단타 비중↓, 차단 아닌 강한 디스카운트)
kki < 50 →  +5
kki < 70 → +20   (Sweet Zone)
kki ≥ 70 → +25   (고끼 최우선)
```

**(c) ★ 모순 해소 (1.5)**: `HIGH_VOLATILITY_SECTORS -5`(1095-1096) 섹터 일괄 패널티 → **종목별 kki + 시나리오 invalidation으로 리스크 이전**. 섹터 -5 제거. 끼 높지만 명분 약한 종목은 invalidation(Stage3)이 잡음. **← 사장님 확인 포인트 (5/21 로킷헬스 사고 방지장치라 사장님 판단 필요).**

**(d) 비중 디스카운트**: `PositionSizer.calc_conviction`(`trade_object.py:303`)에 `if kki < 30: mult *= 0.7` (차단 아닌 "작게 들어감" = 단타 규율).

**(e) 데이터 무결성**: trading_value_억<50 → kki=0(유동성 부족 = 끼 무효, `_score_liquidity` 50억 기준 차용). 신규상장(daily<20행) → kki=None(가산 0, NaN 전파 금지). close≤0/atr NaN → calc_baseline fallback 패턴 차용.

---

### Stage 2 — 맥점 진입 (이미 강함, 유지)
- 눌림 -3%(`SAJANG.ENTRY_MODE_DEFAULT='pullback_3pct'`) / VWAP 3분할 유지. **신규 로직 최소.**
- 맥점 등급(돌파/눌림/지지변곡)만 시나리오 객체에 기록.

---

### Stage 3 — 시나리오 객체 (★약점7★ = 줄먹+손절 체계화)

**(a) `TradeObject` 확장 (신규 클래스 X — 3중중복 방지)** (`trade_object.py:84` 부근 필드 추가)
```python
kki_score: float = 0.0
kki_grade: str = ""
thesis: str = ""                       # 명분: "수급"/"재료"/"상한가연속"/"순환매"
thesis_source: str = ""                # sources에서 도출 (RRGate._normalize_source 재활용)
tp1_trigger_pct: float = 10.0          # 1차 줄먹 절반 발동선 (= SAJANG.RULE_B_THRESHOLD)
tp1_action: str = "PARTIAL_SELL_50"
trail_arm_pct: float = 3.0             # = SAJANG.TRAILING_ACTIVATION_PCT
invalidation_price: int = 0            # = stop_loss (동일값 명시)
invalidation_kind: str = ""            # SUPPLY_EXIT/NEWS_DECAY/STRUCTURE_BREAK/GAP_DOWN
hold_plan: str = ""                    # "D+0"/"D+1"/"D+0~D+3"
```

**(b) 값은 전부 SAJANG 경유** (리터럴 0.97/1.05/0.03 금지 — RULE-005/006/007 차단)
```python
from data.sajang_rules import SAJANG
stop_loss          = StopPriceEngine().calc(...)   # 내부 BaselineLevels.invalidation → sl_fib → SAJANG.get_normal_sl 폴백
invalidation_price = stop_loss
trail_arm_pct      = SAJANG.TRAILING_ACTIVATION_PCT
tp1_trigger_pct    = SAJANG.RULE_B_THRESHOLD       # 10.0
take_profit        = SAJANG.get_take_profit(entry) # 항상 0 (RULE-005 안전)
```

**(c) "1차 줄먹 절반" = 고정가 아님** (get_take_profit=0): **% 트리거 + 행동**으로 표현.
- 일반: `tp1_trigger_pct=SAJANG.RULE_B_THRESHOLD`(10) + `PARTIAL_SELL_50` → 룰B(15:26 +10%↑ 절반, `is_rule_b_triggered` `sajang_rules.py:169`)에 1:1.
- 상한가형: `LIMIT_UP_SPLIT_THRESHOLD`(25) → +29% 절반매도 + 절반 D+1 이월.
- 시나리오는 "어느 룰이 발동될지 **선언**"만, 실제 절반매도는 **기존 매도엔진**이 수행.
- `target_price`/`tp_fib`는 "명분 달성 참고선"으로만 표시, **자동매도 트리거로 안 씀**(트레일링온리 철학).

**(d) invalidation(명분 훼손) — 명분 종류별** (기존 트리거 재활용)

| thesis | invalidation_kind | 트리거 (기존 코드) |
|---|---|---|
| 수급 | SUPPLY_EXIT | `check_supply_withdrawal`(auto_trader.py:4517 — 구현 전 라인 재확인) / SupplyMomentum DEC |
| 재료 | NEWS_DECAY | `_get_news_score` ≤ -5 |
| 상한가연속 | STRUCTURE_BREAK | `BaselineLevels.invalidation` 이탈 |
| 갭/오버나잇 | GAP_DOWN | `SAJANG.is_rule_c_triggered`(gap ≤ -7%) |
| 공통 | STRUCTURE_BREAK | `current_price < invalidation_price`(=stop_loss) |

- `invalidation_narrative()`(107) 확장: kind별 1줄 문장 추가(기존 SUPPORT/INST_COST/MOMENTUM 유지).

**(e) 시나리오↔실행 1:1 매핑 (검증 가능)** — 시나리오=계획, 실행=기존 엔진

| 시나리오 필드 | 라이브 position 키 | 매도 트리거 |
|---|---|---|
| trail_arm_pct=3 | trailing_activated | `is_trailing_activated`(≥3%)→`get_trailing_sl`(고점-3%) |
| tp1_trigger_pct=10 + PARTIAL_SELL_50 | (pnl) | 룰B(15:26) 절반매도 |
| tp1(상한가25) | (pnl) | LIMIT_UP_SPLIT → +29% 절반 + D+1 이월 |
| invalidation_price | stop_loss | dynamic_sl 하회 → STOP_LOSS |
| invalidation_kind=GAP_DOWN | entry_date(D+1) | 룰C(gap≤-7%) |
| hold_plan=D+0~D+3 | style_max_hold_days | hold_days≥effective_max → 청산 |
| invalidation_kind=SUPPLY_EXIT | regime | check_supply_withdrawal |

- 검증: 신규 `scenario_to_position()` + 단위테스트 — "모든 트리거가 라이브 키로 손실없이 매핑 + SAJANG 헬퍼만 사용(리터럴 0건)" assert.

---

## 3. SAJANG 단일진실 추가 (`sajang_rules.py`)

선정 전용 상수 섹션 추가 (frozen 유지, **flag 없음**, 주문룰과 주석 구분):
```python
# ── 선정 스타일 파라미터 (4단 재배선, 5/31) — 주문룰 아님 ──
THESIS_GATE_STRONG_NORM: float = 0.6   # 버킷 강 정규화 임계
THESIS_GATE_MIN_STRONG: int = 2        # {재료·수급·기술위치} 강 최소
THESIS_MIN_CANDIDATES: int = 3         # 이하면 합산 degrade
# 끼 가중치는 score_kki 내 상수(F1~F6) — 추후 SAJANG 이관 검토
```
- `list_all_rules()` 노출 → self-audit 단일진실 유지.
- 임계값(0.6/2/3) = **backtest 보정 전 잠정 초안**.

---

## 4. 회귀 경계 & 검증 (Codex 구현 후 단타봇 수행)

1. **정적 게이트(기계적)**: `python -m tools.audit_order_paths --gate` PASS + `--baseline`으로 `new_violations: []`. CHECK-1/5(주문경로)·CHECK-6(TP/SL) 무손상.
2. **CHECK-6 보존**: Stage3 sl/tp/진입 = SAJANG 헬퍼만(리터럴 곱셈 금지). `limit_up_scanner.py:925` TP/SL **불변**(import-읽기만).
3. **직렬화 계약**: save→load round-trip 신규필드 안전, 구버전 JSON 기본값 복원.
4. **매수 필터 불변**: AVOID·predawn(grade/100점+) 게이트 전후 동일 종목집합. `_calc_grade`/`_calc_signal_type` 미변경.
5. **degrade 경로**: thesis_pass 전부 False → 기존 `total_score>0` TOP-N과 동일 출력(게이트 OFF==기존동작).
6. **매도 무접촉(음성증명)**: morning_recommendation/limit_up_scanner/asset_pool_loader diff가 open/close_position·_order_gate·record_order_intent·SAJANG 매도헬퍼 미호출/미수정(grep + CHECK-1/5).
7. **끼·시나리오 단위테스트**: zone 경계값(test_continuation_score 패턴) + scenario_to_position 매핑 + 리터럴 0건 assert.
8. **paper 리허설**: 명분통과 종목 paper 매수 intent 1:1, 매도(트레일링/룰B/C/EOD) 살아있음 회귀.

---

## 5. 순서 / 미해결 / 사장님 확인 포인트

- **순서**: 이 작업은 매수(진입) 경로를 건드림. **게이트 8/8 복구 완료 → baseline 고정 → 구현 → `--baseline` 회귀 0 증명** 권장. (설계는 read-only라 충돌 없음.)
- **확인포인트 1 — HIGH_VOLATILITY 패널티 역전**: 5/21 로킷헬스 -15.4% 사고 방지장치. 끼+invalidation 리스크 이전이 맞는지 사장님 판단.
- **확인포인트 2 — 재무 버킷**: 순수 재무신호 희박(pension/시총 프록시). 강 판정 데이터소스 추가 필요할 수 있음(추측).
- **확인포인트 3 — 임계값**: 0.6/2/3·끼 가중치 전부 backtest 보정 전 잠정. backtest는 `*_sim`/`backtest` 네이밍으로 CHECK-6/8 자동제외.
- **Codex 전달**: `ops/codex_inbox/PASTE_TO_CODEX_style_4stage_5_31.md`(로컬, .gitignore) → 사장님이 Codex 전달 → 구현 → 단타봇 검증(4장).
