# STEADY_S_DART_RIDE — 룰 설계서 (6/2, read-only)

> 단타봇 read-only 설계 → 사장님 승인 → forward shadow → paper → live. 단타봇 코드 직접수정 X (이 문서는 스펙).
> ★ 매도(보호매도) 무손상 / SAJANG 단일진실 / 게이트 8/8 회귀 0 / 봇 OFF·picks 불변 / scheduler·systemd 무변경 ★
> ★★ 신규 **별도 트랙** — 소형 끼 반등/스캘핑 본류와 분리. 대형/중형 공시기반 3~10일 라이드. ★★
> ★★★ **-3% 단타 영구룰(SAJANG.TRAILING_PCT) 침범 금지** — S급 MAE −10%라 −3%는 휩쏘. SDART 전용 손절. ★★★

## 0. 대전제 (불변)
- 목적 = "완만 신고가 돌파 종목에 **S급 확정성 공시(명분)**가 붙으면, 수일~10일 추세를 탄다."
- **shadow 먼저**: 자동매수 X / picks 불변 / 기록·로그만 → forward 관측 → (사장님 승인) paper → live.
- ★ 정직 sizing: median +3.21%·승률 67%(S급 45건)는 **생존편향 과대·thin** → forward 본검증 전 룰 확정 금지. 절대수익 X.

## 1. 데이터 근거 (6/2 검증, 기간확대)
출처: `tools/leader_catalyst_extend_6_2.py` (commit 70d25e2), docs/03-analysis/leader_catalyst_extend_results_6_2.md.
- 17개월(2025-01~2026-05) DART 98,745공시, STEADY 2,761후보 × 본문 S/A/B/제외, 호재공시 314건.
- **S급 45건: median +3.21·승률 67·D+10 +8.33·MAE −10.04.** 등급 단조 S(+3.21)>A(−2.62/38%)>B(−5.04). **ALL호재 −2.51 vs S +3.21 = 강명분만.** 6개월(+2.23/61%)보다 강해짐.
- **수익 D+3~D+10, D+20 소멸(−0.07)** = D+10 청산. S급 시총 대형34/중형9/소형2 = **대형/중형 트랙.**

## 2. 룰 스펙

### 2.1 진입 후보 (STEADY 신고가 돌파)
- 120일 신고가 돌파(`close ≥ 120일 high`) AND 당일 +10% 미만(완만, 급등형 분리) AND 거래대금 ≥100억·≥1.5×(20일평균).
- ★ 급등형(SURGE: +10%&2.5x) **명분 적용 금지**(데이터: SURGE×호재 median −8.58 = 끝물 추격). STEADY만.

### 2.2 S급 명분 게이트 (DART 본문 판정)
- DART `document.xml` 본문 파싱(검증됨): 공급계약→계약금액/매출액→**매출대비비율** / 자사주→취득금액.
- **S급 기준**: 공급계약 **매출대비 ≥10%** OR 자사주 취득 **≥500억** OR 수치 큰 확정성 호재(대형 수주).
- **제외(명분 미인정)**:
  - A급(매출대비 3~10%·실적·배당/소각/무상): 데이터 median −2.62/승률38% = 약함 → **제외**.
  - B급(소액·미추출): −5.04 → 제외. 일반호재 뭉뚱(ALL −2.51) → 제외.
  - **정정공시([기재정정])**: 이미 알려진 재료 → 제외.
  - **뉴스 단독**: 본 트랙 미적용(DART 본문 확정성만; 뉴스/Tavily/테마는 후순위 별도).

### 2.3 진입 시점 (lookahead 없음)
- 공시 **접수일 T0** 기준. 진입 = **T0 종가**(관측) 또는 T+1 시가(확인포인트 #1). 공시 접수일 ≤ 관측일만 사용(미래 공시 금지).
- (옵션) 5분봉 확인: T0 종가가 돌파가 유지 시. shadow 단계엔 기록만.

### 2.4 청산 (데이터 비교 — D+10/ma10/구조이탈)
- **1순위 ma10 이탈**(종가 < 10일선): 데이터상 S급 +3.21% 최고, 추세 살아있는 동안 라이드.
- **2순위 D+10 보유 후 종가 청산**: D+10 +8.33% 후 D+20 소멸 → D+10 캡.
- 구조이탈(전저점): 비교용. → 셋 다 shadow서 병행 기록, forward로 확정.

### 2.5 손절 (★ -3% 강제 금지)
- ★ S급 MAE **−10%** = S급 리더는 진입 후 ~10% 역행 후 추세 재개 → **-3% 단타룰이면 다 휩쏘.**
- 후보: **−8% / −10% / ATR(2.5×)**. (−3/−5는 휩쏘 위험 — forward서 −8 vs −10 vs ATR 비교 확정.)
- 기본 권장 = **−8%**(MAE −10%보다 약간 타이트, forward 조정). **SAJANG.TRAILING_PCT(−3%, 단타 본류) 무침범.**

### 2.6 유니버스 (별도 트랙)
- **대형/중형만**(시총 ≥1,000억). 소형 끼 본업(반등/스캘핑)과 **분리 운용**. 자본·슬롯도 별도.
- STEADY 거래대금 ≥100억 필터가 자연히 대형/중형으로 수렴(데이터: S급 대형34/중형9).

## 3. SAJANG 단일진실 추가 (`sajang_rules.py`) — TRAILING_PCT 무침범
> ★ 6/3 정정(사장님 "SAJANG 무변경"): shadow 단계엔 **SAJANG 상수 추가 보류**. '넓게 병행 기록'이라
>   확정값이 없어 단일진실에 박을 게 없음. 아래 상수는 forward로 5개 확정 후 paper/live 진입 시 박는다.
>   shadow 도구(`tools/sdart_shadow_record_6_3.py`)는 SAJANG 무변경·scheduler 무연결 순수 read-only.
```python
# ── STEADY_S_DART_RIDE (6/2 데이터 검증, 신규 트랙·shadow 우선) ──
SDART_ENABLED: bool = False              # shadow only, flip=사장님 승인+forward후
SDART_SHADOW: bool = True
SDART_HIGH_N: int = 120                  # 120일 신고가
SDART_SURGE_MAX_PCT: float = 10.0        # 당일 +10%미만(완만, 급등형 분리)
SDART_TURN_X: float = 1.5                # 거래대금 1.5x↑
SDART_TURN_FLOOR_억: float = 100.0       # 거래대금 100억↑
SDART_CAP_FLOOR_억: float = 1000.0       # 대형/중형(소형 제외)
SDART_SUPPLY_SALES_RATIO_S: float = 0.10 # 공급계약 매출대비 ≥10% = S
SDART_BUYBACK_AMT_S_억: float = 500.0    # 자사주 ≥500억 = S
SDART_MIN_GRADE: str = "S"               # S급만(A/B/제외 차단)
SDART_CAT_LOOKBACK: int = 3              # 공시 T-3..T0
SDART_HOLD_DAYS: int = 10                # D+10 중심
SDART_EXIT_MODE: str = "ma10"            # ma10 이탈(데이터 우세)
SDART_STOP_PCT: float = 8.0              # ★ 전용 손절(-3% 금지). forward로 -8/-10/ATR 확정
```
- 헬퍼(리터럴 0): `sdart_candidate_ok(...)`, `sdart_grade_ok(ratio, buyback_amt)`, `sdart_exit(...)`, `sdart_stop_price(entry, peak)`. **SAJANG.TRAILING_PCT(−3%) 미사용·미수정.**
- `list_all_rules()` 노출. RULE-005/006/007 통과.

## 4. forward shadow 기록 필드 (스키마)
`data_store/shadow/sdart_{date}.json`:
```
{ code, name, date(T0), sector, cap_억, track:"large_mid",
  trigger:"steady_high_breakout", high_120d, turnover_억, surge_pct,
  catalyst:{ rcept_no, report_nm, kind, sales_ratio|buyback_amt, grade:"S", detail },
  entry_ref_price(T0 close), would_exit:{ ma10_date, d10_date, struct_date },
  fwd:{ d1,d3,d5,d10,d20 }, mfe, mae, status:"observed(no order)" }
```
- 기록만(체결 X). picks·asset_pool 불변. shadow 단계 record_order_intent 없음(주문 자체 없음).

## 5. 라이브 전 숙제 (정직 — paper도 이거 통과 후)
1. **forward shadow ≥10~20건 누적** — 45건 백테는 생존편향 과대, forward(상폐 포함 실시간)가 본검증.
2. **손절 확정** — −8/−10/ATR forward 비교(−3% 단타룰 강제 금지 재확인).
3. **본문추출 정확도** — A급 "미추출" 일부 → 추출 실패율·오분류 점검(진짜 S급 누락 방지).
4. **생존편향 보정** — 상폐 포함 재측정.
5. **유니버스/자본** — 대형/중형 별도 슬롯·현금(cash30 관계).
6. **뉴스/테마 결합** = 후순위(순수 DART S급 단단해진 뒤).

## 6. 검증 계획 (구현 후 단타봇)
1. `audit --gate` exit 0 + `--baseline` new_violations=[] (게이트 8/8 회귀 0).
2. SAJANG 헬퍼만(리터럴 0) — RULE-005/006/007 통과. TRAILING_PCT 미수정 = 단타 본류 무손상.
3. 매도 살아있음 회귀(트레일/compute_real/dynamic/guards/룰D/vwap/restart/sajang) PASS.
4. shadow 리허설: STEADY×S급 → sdart_{date}.json 기록 1:1 + 주문/picks 무접촉(audit_order_paths 미수정).
5. 단위테스트: sdart_grade_ok/exit 경계 + 리터럴 0 assert.

## 7. 확인포인트 (사장님 결정 영역)
> ★★ 6/3 사장님 결정: **5개 전부 '넓게 병행 기록'(단일 확정 X)** — forward ≥10~20건으로 확정.
>   진입 T0+T+1 / 손절 -8·-10·ATR / 청산 ma10·D+10·struct / 유니버스 cap_억 원값 / S급 ratio 원값 전부 기록.
>   구현: `tools/sdart_shadow_record_6_3.py` (read-only, 게이트8/8·selftest12/12·Tier1 H-1수정).
1. **진입 시점**: T0 종가 vs T+1 시가(공시 후 갭 회피).
2. **손절 폭**: −8 / −10 / ATR (기본 −8 제안, forward 확정).
3. **청산 우선순위**: ma10 vs D+10 캡 vs 구조이탈.
4. **유니버스 컷**: 시총 ≥1,000억(중형 포함) vs ≥1조(대형만).
5. **S급 기준**: 공급계약 매출대비 ≥10% 유지 vs 상향(≥15%).

## 8. 순서 / 의존
- **의존**: STEADY 탐지(leader_prospective_scan) + DART 본문 grading(leader_catalyst_deep/extend) — 둘 다 구현·검증 완료.
- **순서**: 게이트 8/8 baseline(완료) → SAJANG SDART 상수·헬퍼 추가 → shadow 기록 배선(스케줄 brick=4-Tier·승인) → `--baseline` 회귀 0 → forward ≥10~20건 + 5장 숙제 → 사장님 승인 → paper → live.
- 단타봇 직접구현 가능(shadow=read-only). 커밋에 audit 전후 + 검증 5종 명시.
