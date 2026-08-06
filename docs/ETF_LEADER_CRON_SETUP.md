> # 🚫 폐기 문서 — 따라하지 마십시오 (2026-08-06 [F-117])
>
> **이 안내서대로 하면 사장님 절대 룰(KRX 무접촉)을 위반합니다.**
>
> | 항목 | 현재 상태 |
> |---|---|
> | 대상 스크립트 `tools/refresh_etf_leaders.py` | **7/1 삭제됨**(a1560fe) — 저장소에 없음 |
> | VPS cron(토요일 갱신) | **7/30 제거됨** — 그전까지 매주 `No such file`로 실패 중이었음 |
> | Step B의 KRX 수집 | **6/22 전역 kill switch** — 사장님 절대 룰(KRX는 퀀트봇 1봇 전담) |
> | 소비자 `utils/etf_leader_bonus.py` | 살아 있으나 신선도 가드로 **항상 가산 0**(매매 무해) |
>
> 즉 **복구 자체가 룰 위반**이라, 되살릴 수 있는 경로가 없습니다.
> 아래 본문은 삭제하지 않고 **이력으로만** 남깁니다(7/7 교훈 — 삭제 대신 격리·명시).
> `etf_leader_sc` 가산항을 선정 로직에서 완전히 걷어낼지는 **사장님 결정** 사항입니다.

---

# ETF 주도주 자동 갱신 — VPS cron 등록 안내 <sub>(폐기·이력 보존용)</sub>

ETF Step A→B→C 체인을 VPS에서 자동으로 주기적으로 실행하는 안내서.
스크립트: `scalper-agent/tools/refresh_etf_leaders.py` — **★현재 존재하지 않음(7/1 삭제)**

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

**⚠️ 중요**: Body Hunter VPS는 시스템 타임존이 **KST**(`# KST 시각` 주석으로 확인).
cron 표현이 KST 시각 그대로 적용됨 (UTC 변환 불필요).

ETF holdings는 자주 변하지 않음. **매주 1회 갱신이 적절** — 평일 매매 영향 없음, 토요일 자정에 새 데이터 반영.

```bash
crontab -e
```

다음 라인 추가 (KST 시각 그대로):

> 🚫 **이 cron을 등록하지 마십시오** — 스크립트는 7/1 삭제됐고(등록해도 매주 `No such file`),
> 살아 있더라도 Step B가 KRX를 수집하므로 **사장님 절대 룰 위반**입니다. 7/30에 제거된 라인입니다.

```cron
# [폐기·등록 금지] ETF 주도주 자동 갱신 — 매주 토요일 자정 KST
0 0 * * 6 cd /home/ubuntu/bodyhunter && /home/ubuntu/bodyhunter/venv/bin/python scalper-agent/tools/refresh_etf_leaders.py --quiet >> logs/etf_leader.log 2>&1
```

**대안 옵션** (모두 KST 시스템 타임존 가정):

| 주기 | cron 표현 | 설명 |
|------|-----------|------|
| 매주 토요일 자정 KST | `0 0 * * 6` | 권장 — 평일 영향 없음 |
| 매월 1일 03:30 KST | `30 3 1 * *` | 월 1회만 — 데이터 최신성 약함 |
| 매일 03:30 KST | `30 3 * * *` | 매일 갱신 — 부하 큼, 네이버 차단 위험 |
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
