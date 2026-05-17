# Audit Backlog — 단타봇 (Body Hunter v4)

봇 간 협업/감리/기술부채 백로그. 4시스템(정보봇/단타봇/퀀트봇/웹봇) 공통 자산 관리.

---

## §1 SQL 자동화 (Supabase DATABASE_URL 공유) — 2026-05-17 신설

### 배경
정보봇이 5/17 발행한 ["Supabase SQL 자동화 가이드 v1"](./%5B정보봇%20→%20단타봇%5D%20Supabase%20SQL%20자동화%20가이드%20v1%20—%20DATABASE_URL%20공유%20패턴.md) 수신. 4시스템 공통 표준 자산으로 채택.

### 단타봇 측 적용 상태

| 항목 | 상태 | 비고 |
|------|------|------|
| `psycopg2` 설치 | ✅ 2.9.11 | 기존 환경에 이미 있음 |
| `.env` DATABASE_URL 등록 | ✅ 5/17 04:00 | 정보봇 .env에서 복사 완료 |
| `utils/supabase_sql.py` v1.1 | ✅ EVALUATOR 후 강화 | Pool max=3, scalper_* prefix 가드, statement_timeout 5s, lock, atexit |
| `tools/check_info_bot_data.py` | ✅ + 스키마 fallback | M-2 정보봇 스키마 변경 silent fail 방지 |
| 연결 헬스체크 | ✅ PostgreSQL 17.6 | 5/17 03:26 |
| 정보봇 테이블 조회 검증 | ✅ daily_limit_up_history 7건 | 에이프로젠바이오로직스(003060) -3.05% 회귀 발굴 |
| 가드 자체 테스트 | ✅ 7/7 통과 | SELECT/INSERT 통과, 정보봇 UPDATE/주석/CTE/multi-statement/DDL 차단 |

### 정보봇 회신 (단타봇 답변)

**Q1. 단타봇이 정보봇 어떤 테이블 주로 조회?**
- `daily_limit_up_history` — 5일선 회귀 진입 후보 발굴 (영상 매매법 자동화)
- `influencer_signals` — 인플루언서 시그널 발생 시 알림 연동
- `pension_grade` — 연기금 등급 S/A 종목 우선 매수 후보
- 향후 추가 검토: `morning_recommendation`, `nightwatch_recommendations`

**Q2. 단타봇 자체 테이블(scalper_*) 신설 계획?**
- `scalper_etf_leader_picks` — 5/17 신규 ETF 주도주 발굴(차트영웅 로직) 결과 저장. 매일 새 picks 누적
- `scalper_picks` — 단타봇 매일 picks 영구 기록 (현재 JSON 파일 저장 → DB 이관 검토)
- 일정: 5/19 이후 ETF 주도주 Step C(비중 수집) 완료 후 진행

**Q3. 마이그레이션 트래커 도입?**
- 1차 보류 — 정보봇 측 패턴 안정화 + 단타봇 자체 테이블 2~3개 운영 후 도입
- 도입 시: `migration_history` 공통 테이블 (정보봇 발의)

**Q4. 비밀번호 회전 주기?**
- 분기 1회 권고. 4시스템 동시 회전 (.env 동시 갱신)
- 회전 절차: Supabase 대시보드 → Reset DB password → 4 봇 `.env` 동시 업데이트 → bodyhunter-bot 재시작 (장 마감 후)
- 다음 회전: 2026-08-17 (분기 1회)

**Q5. Free tier 60 connections 충돌 방지?**
- 단타봇 Pool max=3 (보수적 설정 — `utils/supabase_sql.py:35`)
- 장중(09:00-15:30) 동시 호출 패턴: TBD (모니터링 후 결정)
- 4 봇 합산 예상: 3(단타)+5(퀀트)+5(웹)+N(정보) ≤ 18 → 안전 마진 충분
- 알림 임계: 연결 수 ≥ 30 도달 시 Slack/Telegram 경고 (별도 작업)

### 기술 부채 (TODO)

#### 5/20 (검증 종료 후) 단계적 실전 활성화 — 사장님 결정 (2026-05-17)

KIS API 카테고리 감리 결과 (2026-05-17):
- ✅ NXT 자동매매 **구조적으로 가능** (`TTTC0012U` + `EXCG_ID_DVSN_CD="NXT"`, 코드 `bot/kis_trader.py:1165-1231` 작성 완료, 선취매 path만 활용 중)
- 사장님 인식 "NXT 수동만"은 정책적 선택 (기술 한계 X)
- WebSocket 실시간시세 (H0STCNT0/H0STASP0/H0STMOM0) 코드 작성됨 (`auto_trade=false`로 OFF)

5/20 저녁 (검증 결과 분석 후) 일괄 활성화:

- [ ] **NXT 자동매매 활성화** — morning_rec preview(16:45) 기반 17:00 NXT 자동 진입
  - COO 스케줄 추가: `kst_time(17, 0)` → `_job_nxt_auto_buy`
  - NXT eligible 필터(5/4 8d88d09) 재활용
  - 다음날 09:00 KRX 인계 (5/16 manual_sync 가드 검증됨)
- [ ] **WebSocket 활성화** — `auto_trade=true` 설정 + H0STCNT0/H0STASP0 구독
  - 장중 ms 단위 추매/청산 의사결정 (현재 30초 폴링)
- [ ] **max_auto_positions 조정** — 검증 결과 기반
  - 2일 평균 PnL > +0.5% → max = 5 (본격)
  - 0 ~ +0.5% → max = 3 (보수)
  - ≤ 0% → 시그널 가중치 재조정 (적중률 낮은 보너스 축소)
- [ ] **검증 모드 OFF** — `.env` `VERIFICATION_MODE=false` 자동 (5/20 is_active=False)

5/21~: 진정한 24시간 자동매매 (NXT 17:00~20:00 + KRX 09:00~15:30)

#### 일반 기술 부채

- [ ] 5일선 회귀 후보를 morning_recommendation 점수에 통합 보너스로 추가 (+10점)
- [x] `scalper_etf_leader_picks` 테이블 신설 후 ETF 주도주 결과 매일 적재 — **5/17 완료 (249ae1a)**, 30행 PASS
- [x] `scalper_etf_leader_picks` 결과를 morning_recommendation 점수에 보너스 통합 — **5/17 완료 (854f954)**, TOP10 +10/TOP20 +6/TOP30 +3 + 다중ETF(3+) 추가 +3, 최대 +13점, VPS 봇 재시작 적재
- [x] ETF Step A→B→C 자동 실행 cron 등록 (매주 토 자정 KST, VPS) — **5/17 완료**, `0 0 * * 6` crontab 등록 + telegram 알림, 다음 5/23 토 00:00 KST 첫 자동 실행
- [x] Retry 로직 (네이버 5xx/timeout 일시 장애 대응) — **5/17 완료 (df2eed7)**, M-1 보강, 3회 지수 백오프 (1s/2s/4s)
- [ ] 정보봇 `pension_grade` 변경 알림 → 단타봇 자동 재평가 (현재 JSON 폴링 → DB push 검토)
- [ ] Pool 모니터링 — 연결 수 메트릭 telegram_alert 연동
- [ ] DATABASE_URL 만료/회전 시 자동 감지 + 알림

### EVALUATOR 5/17 04:00 — FAIL → Critical 2건 수정 완료

**검수 결과**: FAIL (Critical 2 + High 4 + Medium 4 + Low 3 = 13건)

**Critical 2건 — 즉시 수정 완료 (v1.1)**:
- C-1: `execute()` 가드 우회 가능 (주석/CTE/multi-statement) → **단기 가드 강화** (`_check_sql_safety` + `_check_write_target`, 가드 자체 테스트 7/7 통과)
- C-2: postgres superuser는 BYPASSRLS → 4시스템 모두 RLS 우회 → **정공법: Supabase DB role 분리 (5/18 사장님 작업)**

**High 4건 수정**:
- H-1: `with conn:` 안티패턴 docstring 명시
- H-2: `statement_timeout=5000ms` Pool 옵션 (장중 Pool 정지 방지)
- H-3: `ping()` SELECT 1만 사용 (DB 버전 노출 제거)
- H-4: `threading.Lock` + double-check (멀티스레드 안전)

**Medium 4건 수정**:
- M-1: read-only 컨텍스트 (`get_conn(readonly=True)`) 분리
- M-2: `check_schema()` 추가 + `check_info_bot_data.py`에 스키마 검증 fallback
- M-3: 정보봇 스키마 명세 회신 요청 (TODO 별도)
- M-4: 에러 메시지 URL 노출 15자 축소

**Low 3건 수정**:
- L-1: `atexit.register(close_pool)` (VPS 비정상 종료 안전)
- L-3: AUDIT_BACKLOG 라인 참조 → `_POOL_MAX` 변수명
- L-4: `postgres://` deprecated 자동 치환 + warning

### 🚨 5/18 사장님 작업 (C-2 정공법)

**Supabase Dashboard → SQL Editor에서 role 분리**:

```sql
-- 1. 단타봇 전용 read-only role (정보봇 60+ 테이블 SELECT만)
CREATE ROLE scalper_readonly NOINHERIT LOGIN PASSWORD '<RO_PASSWORD>';
GRANT USAGE ON SCHEMA public TO scalper_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO scalper_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO scalper_readonly;

-- 2. 단타봇 전용 writer role (scalper_* 테이블 INSERT/UPDATE/DELETE)
CREATE ROLE scalper_writer NOINHERIT LOGIN PASSWORD '<RW_PASSWORD>';
GRANT USAGE ON SCHEMA public TO scalper_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO scalper_writer;
-- scalper_etf_leader_picks 등 신설 시:
-- GRANT INSERT, UPDATE, DELETE ON scalper_etf_leader_picks TO scalper_writer;
```

**.env 업데이트** (4봇 분리):
```
DATABASE_URL=postgresql://scalper_writer:<RW_PASSWORD>@db.<ref>.supabase.co:5432/postgres
DATABASE_URL_RO=postgresql://scalper_readonly:<RO_PASSWORD>@db.<ref>.supabase.co:5432/postgres
```

`utils/supabase_sql.py`에 RO/RW 분리 Pool 추가 (C-2 정공법 후 C-1 가드 제거 가능).

**정보봇 협업 안건**:
- 4봇 모두 동일 패턴(`<bot>_readonly` + `<bot>_writer`)으로 분리
- 마이그레이션은 별도 `migration_admin` role (분기 1회 회전)
- 분리 완료 후 가이드 v1 → v2 발행 (정보봇 측)

---

## §2 ETF 주도주 발굴 시스템 (Step C 5/18 재개) — 2026-05-17

### 배경
차트영웅 영상 로직 기반 ETF 주도주 발굴 시스템 Step A/B 완료 (a93f516, 41d23dd).
Step C(비중 수집)는 KRX 서비스 다운 + 운용사 SPA 인증 막힘으로 5/18 평일 재개.

### TODO

- [ ] Step C-1: KRX 정보데이터시스템 자동화 재시도 (KRX 정상화 후)
- [ ] Step C-2: 실패 시 TIGER pdf.ajax 세션인증 추가 디버깅 (Referer/CSRF)
- [ ] Step C-3: TOP 20 ETF 비중 일괄 수집 (KODEX+TIGER = 80% 커버)
- [ ] Step C-4: 종목별 점수화 (다중 ETF 포함 × ETF 점수) → TOP 30 주도주
- [ ] Step C-5: SQL 자동화 활용 — `scalper_etf_leader_picks` 테이블 적재 → 웹봇 FLOWX 패널 표시

---

## §3 변경 이력

| 일자 | 단원 | 변경 |
|------|------|------|
| 2026-05-17 | §1 SQL 자동화 신설 | 정보봇 가이드 v1 수신 + 단타봇 자산 작성 (connector + 스캐너 + 5개 회신) |
| 2026-05-17 | §1 EVALUATOR 2차 | FAIL → Critical 2 + 11건 즉시 수정 (5623c09) — 가드 7/7 통과 |
| 2026-05-17 | §2 ETF 주도주 | Step A/B 완료, Step C 5/18 재개 예정 |
| 2026-05-17 | §2 ETF Step C 완료 | 네이버 etfAnalysis 통합 + TOP 30 적재 (249ae1a), `scalper_etf_leader_picks` 신설 |
| 2026-05-17 | §1 기술부채 갱신 | Step C 적재 완료 체크, morning_recommendation 보너스 통합 + cron 자동화 TODO 신규 |
| 2026-05-17 | §1 (B+C) 완성 | morning_recommendation 보너스 연동 + VPS cron 등록 + 봇 재시작 + M-1 retry 보강 (854f954, bf2399a, df2eed7) — 4개 TODO 완료 체크 |
| 2026-05-17 | §1 검증 모드 v1 | 1주 실전 검증 (5/18~5/19) — verification_mode.py + scalper_verification_log + auto_trader 2메서드 + COO 15:25/15:35 스케줄 (aa6887b) |
| 2026-05-17 | §1 KIS API 감리 | NXT 자동매매 구조적 가능 발견 (TTTC0012U+EXCG=NXT, 코드 작성됨, 정책으로 미사용) — 5/20 단계적 활성화 TODO 등록 |
