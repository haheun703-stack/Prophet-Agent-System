# ETF 주도주 자동 갱신 — VPS cron 등록 안내

ETF Step A→B→C 체인을 VPS에서 자동으로 주기적으로 실행하는 안내서.
스크립트: `scalper-agent/tools/refresh_etf_leaders.py`

## 1. 사전 점검 (1회)

VPS에서 다음을 확인:

```bash
# 1) Python venv 활성화 (단타봇 기존 환경 그대로)
cd /home/<user>/Prophet_Agent_System
source venv/bin/activate

# 2) 수동 실행 한 번 — 정상 동작 확인
python scalper-agent/tools/refresh_etf_leaders.py --skip-b --quiet

# 기대 출력:
#   ✅ 완료 — 12초 내외
#   텔레그램에 "✅ ETF 주도주 자동 갱신 완료" 메시지 도착
```

`--skip-b`는 Step B 스킵 + 최신 `leader_picks_*.json` 재사용 → 빠른 검증.
Step B(KRX → ETF 마스터 + 패턴 감지)는 1~2분 추가 소요.

## 2. cron 등록 (권장: 매주 토요일 자정 KST)

ETF holdings는 자주 변하지 않음. **매주 1회 갱신이 적절** — 평일 매매 영향 없음, 토요일 자정에 새 데이터 반영.

```bash
crontab -e
```

다음 라인 추가 (UTC 기준이라 KST 토요일 00:00 = UTC 금요일 15:00):

```cron
# ETF 주도주 자동 갱신 — 매주 토요일 자정 KST (= 금요일 15:00 UTC)
0 15 * * 5 cd /home/<user>/Prophet_Agent_System && /home/<user>/Prophet_Agent_System/venv/bin/python scalper-agent/tools/refresh_etf_leaders.py --quiet >> logs/etf_leader.log 2>&1
```

**대안 옵션**:

| 주기 | cron 표현 | 설명 |
|------|-----------|------|
| 매주 토요일 자정 KST | `0 15 * * 5` | 권장 — 평일 영향 없음 |
| 매월 1일 03:30 KST | `30 18 1 * *` | 월 1회만 — 데이터 최신성 약함 |
| 매일 03:30 KST | `30 18 * * *` | 매일 갱신 — 부하 큼, 네이버 차단 위험 |
| 분기 첫 토요일 자정 | (커스텀) | 가장 보수적 — ETF 비중 변동 빈도 고려 |

## 3. 로그 확인

```bash
# 최근 실행 로그
tail -50 /home/<user>/Prophet_Agent_System/logs/etf_leader.log

# 실패만 필터
grep -E "ERROR|실패|FAIL" /home/<user>/Prophet_Agent_System/logs/etf_leader.log

# 텔레그램 알림 정상이면 별도 로그 확인 불필요
```

## 4. 텔레그램 알림 형식

성공 시:
```
✅ ETF 주도주 자동 갱신 완료 (2026-05-17 16:45 KST)

📊 통계
  • base_date: 20260515
  • ETF 수집: 19/20
  • TOP 30 적재: PASS
  • 소요: 11.9초

🏆 TOP 5 주도주
   1. [042700] 한미반도체           100.67점 (4ETF)
   2. [068270] 셀트리온              74.13점 (5ETF)
   3. [329180] HD현대중공업          60.37점 (4ETF)
   4. [010140] 삼성중공업            54.93점 (4ETF)
   5. [010120] LS ELECTRIC      50.82점 (3ETF)

다음 morning_recommendation부터 +N점 보너스 자동 반영.
```

실패 시:
```
❌ ETF 주도주 자동 갱신 실패 (2026-05-17 16:45 KST)

오류: ConnectionError: Failed to fetch...
소요: 5.3초

VPS 로그 확인 필요. 수동 재실행 명령:
  python scalper-agent/tools/refresh_etf_leaders.py
```

## 5. 옵션 플래그

```bash
# 풀 체인 (Step B + C-3+4 + C-5) — 보통 1~2분
python scalper-agent/tools/refresh_etf_leaders.py

# Step B 스킵 (이미 leader_picks_*.json 있을 때) — 10~15초
python scalper-agent/tools/refresh_etf_leaders.py --skip-b

# 디버그용 — 특정 base_date 강제 지정
python scalper-agent/tools/refresh_etf_leaders.py --skip-b --base-date 20260515

# cron용 — 콘솔 출력 최소화 (텔레그램 알림만)
python scalper-agent/tools/refresh_etf_leaders.py --quiet

# 1회성 — 신규 환경에서 scalper_etf_leader_picks 테이블 자동 생성
python scalper-agent/tools/refresh_etf_leaders.py --create-table
```

## 6. 단타봇 매매 연동 확인

cron 자동 실행 후, 다음 morning_recommendation 사이클에서 자동 반영:

- TOP 10 합류 종목 (예: 한미반도체) → **+10점 + cross 1**
- TOP 20 합류 종목 → **+6점**
- TOP 30 합류 종목 → **+3점**
- 다중 ETF (3개+) 합류 추가 보너스 → **+3점**
- 최대 +13점 (TOP 10 + 다중 ETF)

로그에서 다음 메시지 확인:
```
[step5] ETF주도주 맵: 30종목 (TOP10=10)
```

추천 메시지 `key_reasons`에 `etf_lead(TOP10/4ETF:+13)` 표시됨.

## 7. 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| `ImportError: utils.supabase_sql` | 작업 디렉토리가 프로젝트 루트 아님 | cron 라인에 `cd /home/.../Prophet_Agent_System &&` 추가 |
| `테이블 없음` | scalper_etf_leader_picks 미생성 | `--create-table` 플래그 1회 실행 |
| 텔레그램 미수신 | TELEGRAM_BOT_TOKEN/CHAT_ID 미설정 | `.env` 확인 |
| 19/20 → 18/20 등 | 해외 ETF 추가 (네이버 미제공) | 정상 — 자연 누락, 단타봇은 국내만 매매 |
| 5xx 일시 실패 | 네이버 부하 | retry 로직 추가 예정 (M-1 TODO) |

---

**관련 문서**:
- `docs/AUDIT_BACKLOG.md` §1 — Supabase SQL 자동화 + 기술부채 TODO
- 메모리 `project_etf_leader_pipeline.md` — Step A/B/C 전체 흐름
- 메모리 `reference_supabase_sql_automation.md` — DB 연결 패턴
