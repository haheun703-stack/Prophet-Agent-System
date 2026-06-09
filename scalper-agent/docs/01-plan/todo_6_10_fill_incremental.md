# 내일(6/10) 1순위 작업 — fill 증분화 (적응형 recent_days + fallback 복구)

> 사장님 지시(6/9): 단순 recent_days 축소가 아니라, **기본 증분 + 이상 감지 시 종목별 복구 fallback**.
> 오늘 데이터 다 채웠으니 실매매 급한 불 없음. 봇 OFF·매매 무관 유지.

---

## 배경 (6/9 실측)
6/9 기초데이터 수집 총 ~32분 중 시간 분해:
| 단계 | 시간 | 비중 |
|---|---|---|
| **① 일봉 fill** | **~20분** | **62% ← 병목** |
| ② step6 sync | 1.5분 | |
| ③ 수급 1317 | 6.5분(393초) | |
| ④ 11주체 | ~10초 | |
| ⑤ 국적별 211 | 2분 | |

**원인**: `tools/fill_daily_kis_incremental.py`가 매일 **3651종목 × recent_days=20**(20일치)를 일괄 재확인.
어제 데이터 멀쩡한 정상 종목까지 20일을 다시 받음 = 낭비.

## 목표
정상 종목은 **5일 증분**, 이상 종목만 **20일 복구** → 일봉 fill **20분 → 5~7분**.
단, recent_days=20의 손상복구 안전장치(과거 005930 손상 교훈)는 **fallback으로 보존**.

---

## 설계 (사장님 5요구 충실)

### 현재 구조 (fill 98줄)
- `fill(codes, recent_days=20, sleep=0.1, log_every=200)` — start = now − recent_days
- `collect_daily_kis`로 신규 수신 → df_old concat df_new → **keep='first'**(기존 보존, 신규 거래일만 추가) → to_csv
- 변경 핵심 = "종목별 recent_days를 동적 결정" + "완료 후 검증 step"

### 신규 로직 ① — 종목별 fetch window 분류기
`classify_fetch_window(code, daily_dir, asof) → (recent_days, reason)`
1. **정상**: CSV 존재 + 무손상 + 마지막날짜가 직전 거래일과 ≤5거래일 gap → `recent_days=5` (NORMAL)
2. **누락**: 마지막날짜 gap > 5거래일 → `recent_days=20` (STALE/MISSING)
3. **CSV 손상/행 부족/날짜 역전**: 읽기실패·헤더깨짐·행<N·마지막날짜 파싱불가·날짜 역전(내림차순/중복) → `recent_days=20` 강제 + 복구 (CORRUPT)
4. **신규(CSV 없음)**: → `recent_days=20` (NEW)
- 직전 거래일 계산 = `data.trading_calendar.is_trading_day` 역산(단일 진실, 휴장 반영)

### 신규 로직 ② — fill()이 종목별 window 적용
- 기존 일괄 `recent_days` → `classify_fetch_window`로 종목별 분기
- keep='first' merge 그대로 유지(과거 무손상 = 절대 불변)
- 분류 카운트 로그: NORMAL/STALE/CORRUPT/NEW 각 몇 종목

### 신규 로직 ③ — 완료 후 전체 최신일 검증 step
- 6/9(=asof) 기준 전체 종목 최신일 분포 집계
- **기준선 비교**: 증분 모드 커버리지 ≥ 전체20일 모드 커버리지(오늘 90.3%) — 누락 0 보장
- 손상 잔존(행부족·날짜역전) 0건 확인
- FAIL 시 해당 종목만 20일 재시도(자동 fallback 2차)

---

## 4-Tier 검증 계획 (커밋 전 필수)
- **Tier 1**: `bkit:code-analyzer` (신규 분류기 + fill 수정)
- **Tier 2**: 호출 site grep (fill 호출 = collect_all? scheduler? 영향 추적) + AST
- **Tier 3 회귀**: ★핵심★ **증분 모드 결과 == 전체20일 모드 결과** (오늘 6/9 데이터를 기준선으로, 종목별 마지막날짜 동일·누락0). 손상 시나리오 셀프테스트(빈CSV·행1개·날짜역전 주입 → CORRUPT 분류 → 20일복구 확인)
- **Tier 4**: Codex 상호검수
- 게이트 8/8 PASS(매매 무관이지만 회귀 확인) · 봇 OFF · 실주문 0

## 안전 불변식
- keep='first' merge 유지 = 과거 데이터 100% 보존(덮어쓰기 금지)
- trading_calendar 단일 진실로 직전거래일 계산(휴장/주말 반영)
- 봇 OFF · 매매 코드 무접촉 · SAJANG 무변경 · scheduler 무연결
- 검증 누락0 확인 전 커밋 금지

## 착수 순서 (내일)
1. classify_fetch_window 구현 + 셀프테스트(정상/누락/손상/신규 4케이스)
2. fill() 종목별 적용 + 분류 로그
3. 완료 후 검증 step + 자동 2차 fallback
4. 회귀(증분==전체) + 손상시나리오 + 4-Tier
5. PASS 시 커밋

---
**작성**: 2026-06-09 (6/9 수집 완료 후 인계) · 다음세션/내일 단타봇이 이거 먼저 읽고 착수
