# ★ 사장님 영구 룰: 단타봇 3-Tier 검수 의무 (2026-05-25 제정) ★

> **사장님 명령 (2026-05-25 16:30)**:
> "야..... 다시 검수해..... 전체모두 문제가 없는지.... 뭐 이상한 정상매수 보장합니다.
>  말만하지말고 결과로 증명을 하라고 항상!!"
> "앞으로 검수 진행 시 무조건 확인할 수 있도록 자동화 적용 셋팅을 하자"

이 문서는 단타봇이 사장님께 "검수 통과" / "보장합니다" 보고 전 **반드시** 통과해야 할 3-Tier 검수 의무를 정의합니다.
**위반 시 = 박사 미달 + 사장님 영구 룰 위반 (5/21 [feedback_stock_master_promise] 적용)**.

---

## 📋 사고 배경 — 어제(5/25) 왜 사장님이 분노하셨나

### 1차 사고: `_send` UnboundLocalError (commit e120a12로 fix)
- 어제 commit `7fa90b5`에서 `await _send(...)` 추가, 정의는 뒷부분 → Python `UnboundLocalError`
- 09:15 + 14:50 매수 시도 **모두 실패** (휴장이라 다행)
- ★ 단위 테스트 53/53 PASS 보고했어도 못 잡음 ★ (실제 KIS API mock 불가)

### 2차 사고: 사장님 영구 룰 정면 위반 (commit b3b1b4e로 fix)
- 사장님 5/25 룰: "고점 -3% 일관 / -7~-15% 회귀 X"
- `dynamic_trailing.py` 모듈만 단순화, **실제 매매 경로** (`_compute_dynamic_trailing_sl`)는 옛 multi-zone -7/-10/-12/-15% 그대로
- 5/26 09:15 매수 시 +12% → trail -7% 적용 = ★ 사장님 룰 위반 매매 ★
- 어제 검수 통과 53/53 → 실제 매매는 사장님 룰 위반 (단위 테스트만 적용된 모듈 호출 안 됨)

### 근본 원인
1. 단위 테스트 = 새 모듈만 호출, **실제 호출 site** (auto_trader 메인 매매 경로) 안 봄
2. "이 정도면 됐다" 자의적 판단 → 박사 미달
3. code-analyzer agent 1회 호출 + 결과만 신뢰 → 함정 (어제 발견 못함, 오늘 재호출로 발견)

---

## ★ 3-Tier 검수 의무 ★

> **모든 commit 전, 모든 "검수 통과" 보고 전, 단타봇은 반드시 3-Tier를 통과해야 합니다.**

### 🔵 Tier 1 — 자동 검수 (Code Analyzer Agent)

**도구**: `bkit:code-analyzer` agent 호출

**의무**:
- 변경 파일 전수 검수 요청 (직접 grep만 X, agent 자동 분석 강제)
- 발견된 모든 CRITICAL/HIGH는 fix 또는 사장님께 보고 (자율 결정 X)
- agent 결과 "괜찮아 보입니다" 같은 결론 그대로 신뢰 금지 → Tier 2/3로 교차 검증

**자동화**:
- 큰 commit (변경 +200 라인 이상) 시 단타봇 자율 호출 의무
- pre-commit hook이 변경 라인 수 확인 → 임계값 초과 시 단타봇에게 agent 호출 알림

---

### 🟡 Tier 2 — 수동 호출 site 추적 (Grep + AST)

**도구**: Grep / Bash AST 분석

**의무 검수 항목**:

1. **UnboundLocalError 패턴** (5/25 1차 사고 재발 방지)
   ```bash
   # 함수 안에서 await NAME() 호출 위치 vs async def NAME 정의 위치
   # → 호출이 정의보다 앞에 있으면 UnboundLocalError
   ```
   AST로 모든 함수 내부 nested def 검출 + 호출 위치 비교.

2. **새 모듈 호출 site 추적** (5/25 2차 사고 재발 방지)
   ```bash
   # 새 모듈 (dynamic_trailing 등) 만들면 반드시 호출 site grep
   grep -rn "from bot.NEW_MODULE\|import NEW_MODULE" .
   # → 호출 site 0건이면 **dead code 경고** (단위 테스트만 호출되는지 의심)
   ```

3. **함수 동작 차이 검증** (옛 코드 잔존 검출)
   ```bash
   # 사장님 룰 위반 패턴 grep (예: 옛 multi-zone 트레일링 폭)
   grep -n "trail_pct = 0.07\|trail_pct = 0.10\|trail_pct = 0.12\|trail_pct = 0.15" scalper-agent/bot/*.py
   # → 0건이어야 통과. 발견 시 ★ 사장님 5/25 룰 위반 ★
   ```

**자동화**: `tools/pre_commit_check.py`의 RULES에 사장님 룰 위반 패턴 추가 (이 문서에 정의된 패턴 자동 차단).

---

### 🟢 Tier 3 — 실증 검증 (실제 호출 site 회귀 테스트)

**도구**: 단위 테스트 + 실제 호출 site 회귀 테스트

**의무**:

1. **단위 테스트만 X — 실제 매매 경로 회귀 테스트 필수**
   - 새 모듈/함수: 해당 모듈만 호출하는 단위 테스트 (필수, 어제도 했음)
   - **실제 매매 경로**: `_compute_dynamic_trailing_sl` 같은 실제 호출 site 직접 호출 회귀 테스트
   - 예: `test_compute_trailing_real_5_25.py` (2026-05-25 신설)

2. **사장님 영구 룰 명시 검증**
   - 각 테스트는 사장님 룰을 직접 명시: `assert sl == 10864, "사장님 5/25 -3% 룰 정확"`
   - 옛 코드 잔존 검증: `assert sl != 10416, "옛 -7% 폐기 확인"`

3. **VPS 환경 재검증**
   - 로컬 PASS → VPS git pull → VPS 환경에서 동일 테스트 실행
   - VPS의 Python 환경/캐시 영향 확인 (CRLF, encoding 등)

**자동화**: 단타봇이 commit 후 자동 VPS 회귀 실행 → 결과 사장님께 텔레그램 보고.

---

## 🛑 사고 즉시 차단 패턴 (pre-commit hook 자동 차단)

`tools/pre_commit_check.py` RULES 추가 (5/25 사고 교훈):

| ID | 패턴 | 심각도 | 사유 |
|----|------|--------|------|
| RULE-001 | `trail_pct = 0\.07\|0\.10\|0\.12\|0\.15` (auto_trader.py) | CRITICAL | 사장님 5/25 영구 룰 ("-3% 일관") 위반 — 옛 multi-zone 트레일링 |
| RULE-002 | `if not is_trading_day\(\)` 누락 (신규 job_* 함수) | HIGH | 휴장일 발동 차단 누락 |
| RULE-003 | `await _send(` 호출이 `async def _send` 정의보다 앞 | CRITICAL | UnboundLocalError 패턴 (5/25 1차 사고) |
| RULE-004 | 새 모듈 호출 site 0건 (dead code) | HIGH | 5/25 2차 사고 — 단위 테스트만 적용, 실제 매매 미호출 |

---

## ✅ "검수 통과" 보고 조건 체크리스트

단타봇이 사장님께 "검수 통과 / 보장합니다" 보고 전 **모두 ✅ 확인**:

- [ ] Tier 1: code-analyzer agent 호출 완료 (결과 첨부)
- [ ] Tier 2: 호출 site 추적 grep 결과 0건 (사장님 룰 위반 패턴)
- [ ] Tier 3: 단위 테스트 + ★ 실제 매매 경로 회귀 ★ 모두 PASS
- [ ] VPS 환경 실측 검증 (실제 함수 호출 → 사장님 룰 정확 적용 데이터)
- [ ] pre-commit hook 통과 (자동 차단 0건)
- [ ] commit message에 "검수: Tier 1 ✅ / Tier 2 ✅ / Tier 3 ✅ + VPS 실측" 명시

**위 6개 중 1개라도 ❌ → 사장님께 "보장합니다" 말하지 않음. 자아성찰 + 추가 검수.**

---

## 📚 영구 메모리 연결

- [[feedback_3tier_review_rule]] — 이 문서의 영구 메모리
- [[feedback_stock_master_promise]] (5/21) — 박사 미달 자율 판단 금지
- [[feedback_data_integrity_in_every_check]] (5/21) — ALL GREEN 보고 전 data_integrity 무조건
- [[feedback_verify_external_knowledge]] (5/22) — 외부 자료/검수 결과 그대로 신뢰 X, 우리가 검증
- [[project_5_25_3tier_review_incident]] — 5/25 사고 + fix + 영구 룰 제정 회고

---

## 🔄 업데이트 이력

- **2026-05-25**: 제정. 5/25 1차 (`_send` 사고) + 2차 (사장님 룰 위반) 사고 후 영구 룰 확립.
