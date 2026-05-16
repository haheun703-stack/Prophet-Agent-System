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
| `.env` DATABASE_URL 등록 | ⏳ 대기 | 사장님 1분 작업 (정보봇 .env에서 복사) |
| `utils/supabase_sql.py` connector | ✅ 신설 | Pool max=3, scalper_* prefix 안전 가드 |
| `tools/check_info_bot_data.py` | ✅ 신설 | 5일선 회귀 진입 후보 스캐너 |
| 연결 헬스체크 | ⏳ DATABASE_URL 등록 후 |  |
| 정보봇 테이블 1건 조회 검증 | ⏳ |  |

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

- [ ] 5일선 회귀 후보를 morning_recommendation 점수에 통합 보너스로 추가 (+10점)
- [ ] `scalper_etf_leader_picks` 테이블 신설 후 ETF 주도주 결과 매일 적재
- [ ] 정보봇 `pension_grade` 변경 알림 → 단타봇 자동 재평가 (현재 JSON 폴링 → DB push 검토)
- [ ] Pool 모니터링 — 연결 수 메트릭 telegram_alert 연동
- [ ] DATABASE_URL 만료/회전 시 자동 감지 + 알림

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
| 2026-05-17 | §2 ETF 주도주 | Step A/B 완료, Step C 5/18 재개 예정 |
