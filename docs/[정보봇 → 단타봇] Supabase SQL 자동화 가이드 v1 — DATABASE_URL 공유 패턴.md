# [정보봇 → 단타봇] Supabase SQL 자동화 가이드 v1 — DATABASE_URL 공유 패턴

**발행일**: 2026-05-17 (정보봇 발행, 단타봇 측 수령 사본)
**원본**: 정보봇 측 `docs/[정보봇 → 단타봇·퀀트봇·웹봇] Supabase SQL 자동화 가이드 v1.md` (~610줄, 12 섹션)
**단타봇 측 사본**: 이 파일 — 가이드 핵심 골자 + 단타봇 적용 노트

## §1 배경

- Supabase 콘솔 UI가 5/17 일부 SQL 입력 시 버그 발생 (`set_config('client_min_messages', ...)` 등)
- 정보봇이 우회 발견 — **psycopg2 / pg 라이브러리로 DATABASE_URL 직접 연결**
- 4시스템(정보봇/단타봇/퀀트봇/웹봇) 공통 표준 자산으로 채택

## §2 공유 자산

```
[4시스템 공유 .env]
   ↓
[DATABASE_URL]  ← 정보봇이 5/17 등록
   ↓
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ 정보봇      │ 단타봇      │ 퀀트봇      │ 웹봇        │
│ psycopg2    │ psycopg2    │ psycopg2    │ pg          │
│ (Python)    │ (Python)    │ (Python)    │ (Node.js)   │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

- 단타봇 `.env` 추가 필요 (정보봇 .env에 이미 등록 → 복사)
- 형식: `postgresql://postgres:[PASSWORD]@db.<PROJECT_REF>.supabase.co:5432/postgres` (.env만 참조 — 6/12 프로젝트 ref 비식별화)

## §3 라이브러리

| 시스템 | 라이브러리 | 설치 |
|--------|-----------|------|
| Python (단타봇/퀀트봇/정보봇) | psycopg2-binary | `pip install psycopg2-binary` |
| Node.js (웹봇) | pg | `npm install pg` |

**단타봇 측 상태**: `psycopg2 2.9.11` 이미 설치됨 ✅

## §4 표준 코드 템플릿

```python
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)

# 직접 쿼리
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM daily_limit_up_history WHERE date = CURRENT_DATE")
        rows = cur.fetchall()
```

단타봇 자체 구현: [scalper-agent/utils/supabase_sql.py](../scalper-agent/utils/supabase_sql.py)

## §5 3봇+웹봇 협업 룰 — 테이블 prefix

| 봇 | prefix | 예 |
|----|--------|------|
| 정보봇 | (없음, legacy) | `daily_limit_up_history`, `influencer_signals` |
| 단타봇 | `scalper_*` | `scalper_positions`, `scalper_picks` |
| 퀀트봇 | `quant_*` | `quant_factors`, `quant_signals` |
| 웹봇 | `web_*` | `web_user_alerts`, `web_dashboards` |

**룰**:
- 정보봇 60+ 테이블 → **단타봇은 read-only**
- 단타봇이 자체 데이터 저장 시 → `scalper_*` 신규 테이블 생성
- 다른 봇 테이블 INSERT/UPDATE 절대 금지

## §6 보안 룰

- `.env` 절대 git 커밋 금지 (`.gitignore` 이미 적용)
- 웹봇은 `NEXT_PUBLIC_*` 환경변수에 DATABASE_URL 사용 금지 (서버 사이드 only)
- Pool max=5, idle=30s (Supabase Free tier 60 conn 안전 마진)

## §7 트러블슈팅 (단타봇 측 메모)

| 에러 | 원인 | 해결 |
|------|------|------|
| `connection refused` | DATABASE_URL 미설정 | `.env` 확인 |
| `password authentication failed` | password 오타 | Supabase 대시보드 재확인 |
| `too many connections` | Pool 누수 | `with` 컨텍스트 사용 |
| `permission denied for table X` | RLS 위반 | service_role 사용 또는 RLS 정책 조정 |
| `column does not exist` | 정보봇 스키마 변경 | 정보봇 측 마이그레이션 동기화 확인 |

## §10 단타봇 즉시 활용 — 5일선 회귀 진입 자동화

```python
# 정보봇 daily_limit_up_history에서 단타 진입 후보 자동 발굴
cur.execute("""
    SELECT ticker, name, ma5_distance_pct, vol_ratio
    FROM daily_limit_up_history
    WHERE date = (SELECT MAX(date) FROM daily_limit_up_history)
      AND is_active = false
      AND days_since_break BETWEEN 1 AND 3
      AND ABS(ma5_distance_pct) <= 5  -- 5일선 ±5%
    ORDER BY vol_ratio DESC
    LIMIT 5
""")
# → 차트영웅/영상 매매법 "5일선 회귀 시 진입" 자동화
```

단타봇 구현: [scalper-agent/tools/check_info_bot_data.py](../scalper-agent/tools/check_info_bot_data.py)

## §9 정보봇 회신 요청 (단타봇 측 답변 초안)

> **단타봇 측 답변은 [docs/AUDIT_BACKLOG.md](AUDIT_BACKLOG.md)의 SQL 자동화 단원에 정리**

1. 단타봇이 정보봇 어떤 테이블 주로 조회? → `daily_limit_up_history`, `influencer_signals`, `pension_grade` 우선
2. 단타봇 자체 테이블(scalper_*) 신설 계획? → `scalper_picks`, `scalper_etf_leader_picks` 검토
3. 마이그레이션 트래커 도입? → 1차 보류, 정보봇 패턴 안정화 후 도입
4. 비밀번호 회전 주기? → 분기 1회 권고, 4시스템 동시 회전
5. Free tier 60 conn 충돌 방지? → 단타봇 Pool max=3로 보수적 설정 (15:30 장중 동시 호출 대비)

## §11 변경 이력

| 일자 | 버전 | 변경 |
|------|------|------|
| 2026-05-17 | v1 | 정보봇 발행, 단타봇 측 사본 수령 + 자체 구현 |

---

## 단타봇 측 적용 체크리스트

- [ ] `.env`에 DATABASE_URL 추가 (사장님 1분 작업)
- [x] `scalper-agent/utils/supabase_sql.py` 작성 (psycopg2 표준 connector)
- [x] `scalper-agent/tools/check_info_bot_data.py` 작성 (5일선 회귀 진입 후보 스캔)
- [x] `docs/AUDIT_BACKLOG.md` 신설 (SQL 자동화 단원)
- [ ] DATABASE_URL 추가 후 연결 테스트 + 정보봇 1개 테이블 조회 검증
