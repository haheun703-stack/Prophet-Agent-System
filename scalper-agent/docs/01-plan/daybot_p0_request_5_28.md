# 단타봇 P0 의뢰서 — 5/28 silent fail 패턴 검수 요청

**작성**: 2026-05-28T16:56:54+09:00  
**작성자**: 단타봇 (Intraday Alpha + Execution Bot)  
**역할**: 사고 원인을 증거로 포장 / Codex 검수 의뢰 / **고치는 AI X**  
**HEAD**: 66944da `feat: add install_codex_hook.sh + LF gitattributes`  
**근거**: 사장님 [Codex 결정문 — 5/28 15:26~16:45] (16:50)

---

## 0. 의뢰 범위

| 우선순위 | 건수 |
|---|---|
| P0 | 7건 |
| P1 (blocker — rate limit) | 1건 |
| **총** | **8건** |

## 1. 사장님 Codex 결정문 — 판정 인용

| 항목 | 판정 |
|---|---|
| 5/28 자동매매 0건 확인 | PASS |
| 운영 감시 / 보고 / 학습 데이터 품질 | FAIL |
| 장중 사고 | 없음 |

→ 본 의뢰서는 운영 감시 계통 FAIL 8건의 fix 요청

## 2. 단타봇 영구 금지 (사장님 명시)

| # | 금지 |
|---|---|
| 1 | `systemctl restart/start/enable/unmask` |
| 2 | cron 복원 |
| 3 | `AUTO_TRADE_DISABLED` / `kill_switch` 변경 |
| 4 | `.env` 변경 |
| 5 | 텔레그램 토큰 포함 URL 재출력 |
| 6 | "수정 완료" 표현 사용 |
| 7 | "5/29 paper 가능" 표현 사용 |
| 8 | Codex 검수 전 단타봇 자율 코드 적용 |

## 3. P0 의뢰 7건 — 상세

---

### P0-1: 텔레그램 봇 URL 토큰 평문 노출

**파일/라인**: `journalctl -u bodyhunter-bot.service` 전 구간 (5/28 09:51~16:46)

**실패 모드**: telegram-python-bot 기본 httpx 로깅이 URL 전체를 INFO 레벨로 출력 → 봇 토큰(`bot8451***...***`) 평문 journalctl 노출

**재현 로그** (토큰 마스킹):
```
May 28 15:30:03 ip-172-26-2-140 python3.11[115302]:
  15:30:03 [httpx] INFO: HTTP Request:
  POST https://api.telegram.org/bot84***[REDACTED]***/getUpdates
  "HTTP/1.1 200 OK"
```

발생 빈도: 매 polling cycle (수 초당 1건) / 가동 시간 6시간 55분 누적

**필요 테스트**:
- httpx 로깅 레벨 WARNING으로 조정 또는 URL 마스킹 필터 추가
- journalctl에 토큰 평문 미노출 확인 (1시간 가동 후 grep)
- 텔레그램 봇 토큰 rotate 검토 (사장님 결정)

**금지 작업**:
- 단타봇이 토큰 rotate 직접 실행 X
- 단타봇이 logging config 수정 X
- 본 의뢰서 외 일체 보고에 토큰 재출력 X

---

### P0-2: `_job_daily_self_audit` NameError — `is_trading_day` 전역 import 누락

**파일/라인**: `scalper-agent/bot/trading_coo.py:4671`

**실패 모드**:
```python
File "/home/ubuntu/bodyhunter/scalper-agent/bot/trading_coo.py", line 4671,
  in _job_daily_self_audit
    if not is_trading_day():
           ^^^^^^^^^^^^^^
NameError: name 'is_trading_day' is not defined
```

다른 함수 안 local import와 착각한 구조 — 전역 import 없음.

**재현 로그** (5/28 15:45:00):
```
May 28 15:45:00 ip-172-26-2-140 python3.11[115302]:
  15:45:00 [BH.Bot] ERROR: 봇 에러: name 'is_trading_day' is not defined
May 28 15:45:00 ip-172-26-2-140 python3.11[115302]: Traceback (most recent call last):
May 28 15:45:00 ip-172-26-2-140 python3.11[115302]:
  File "/home/ubuntu/bodyhunter/venv/lib/python3.11/site-packages/telegram/ext/_jobqueue.py",
  line 1010, in _run
    await self.callback(context)
  File "/home/ubuntu/bodyhunter/scalper-agent/bot/trading_coo.py", line 4671,
  in _job_daily_self_audit
    if not is_trading_day():
NameError: name 'is_trading_day' is not defined
```

**영향**: 사장님 영구 룰 13건 자동 감리 (Daily Self-Audit) **실행 0회** / 매일 15:45 KST 발생

**필요 테스트**:
- 회귀 테스트: `_job_daily_self_audit` 휴장일 + 거래일 분기 단위 테스트
- VPS 가동 후 다음 거래일 15:45 KST 정상 실행 확인 (journalctl)
- 사장님 영구 룰 13건 검증 결과 텔레그램 보고 도달 확인 (P0-3 의존)

**금지 작업**:
- 단타봇이 `trading_coo.py` 수정 X
- 단타봇이 import 추가 X
- 회귀 테스트 작성도 Codex 검수 후

---

### P0-3: `scheduled_reports` 전송 실패 — wrapper 객체 mismatch

**파일/라인**:
- `scalper-agent/bot/trading_coo.py:4417` (wrapper 전달)
- `scalper-agent/bot/scheduled_reports.py:226` (`bot.send_message(...)` 호출)

**실패 모드**:
- `trading_coo.py:4417` → `self.bot` (BodyHunterBot wrapper) 전달
- `scheduled_reports.py:226` → `bot.send_message(...)` 직접 호출
- wrapper 객체에 `send_message` 속성 없음 → AttributeError

**재현 로그** (5/28 15:35:00):
```
May 28 15:35:00 ip-172-26-2-140 python3.11[115302]:
  15:35:00 [bot.scheduled_reports] ERROR:
  [scheduled_reports] close 전송 실패:
  'BodyHunterBot' object has no attribute 'send_message'
```

**영향**: 마감(close) / 단타봇 자율 보고 5건 모두 텔레그램 전송 실패 / 사장님이 봇 결과 텔레그램으로 못 받음

**수정 방향 후보** (Codex 판정):
1. `trading_coo.py:4417`에서 `context.bot` 전달
2. 또는 `self.bot._app.bot` 전달
3. 또는 `scheduled_reports.py:226`에서 wrapper의 `_app.bot` 추출

**필요 테스트**:
- 단위 테스트: 5건 보고 타입 각각 정상 전송 (morning / midday / jarvis_weakness / weekend_hold / close)
- VPS 가동 후 다음 거래일 15:35 KST close 보고 도달 확인
- 텔레그램 채팅 ID 마스킹 검증

**금지 작업**:
- 단타봇이 `trading_coo.py` / `scheduled_reports.py` 수정 X

---

### P0-4: 15:26 Rule B silent skip — 무로그 빠짐

**파일/라인**:
- 등록: `scalper-agent/bot/trading_coo.py:4850`  
  `self.auto_trader.job_eod_split_check`
- 차단: `scalper-agent/bot/auto_trader.py:5044`  
  `if not self.is_running: return` (첫 줄)

**실패 모드**: `self.is_running == False` 시 무로그 즉시 return → journalctl에 룰 B 실행/skip 어느 쪽도 없음

**재현 로그** (5/28 15:25~15:35):
```
May 28 15:25:00 [COO] AUTO_TRADE_DISABLED — skip verification_close
May 28 15:25:15 [COO] AUTO_TRADE_DISABLED — skip intraday_verification_scan
May 28 15:28:15 [COO] AUTO_TRADE_DISABLED — skip safety_check
May 28 15:30:15 [COO] AUTO_TRADE_DISABLED — skip intraday_verification_scan
May 28 15:33:15 [COO] AUTO_TRADE_DISABLED — skip safety_check
May 28 15:34:15 [COO] AUTO_TRADE_DISABLED — skip jarvis_decision

★ 15:26 job_eod_split_check 로그 부재 ★
```

다른 cron들은 AUTO_TRADE_DISABLED skip 로그 남김 / `job_eod_split_check`만 무로그

**영향**: 룰 B silent skip 상태 = 관측 불가 = 다음 사고 시 원인 추적 불가능

**필요 테스트**:
- `not self.is_running` 분기에도 INFO 로그 추가
- 회귀 테스트: `is_running == False` 시 로그 1건 / 정상 시 로그 1건 / 매도 시 로그 1건+
- VPS 다음 거래일 15:26 KST 로그 1건 이상 확인

**금지 작업**:
- 단타봇이 `auto_trader.py` 수정 X
- 단타봇이 `is_running` 플래그 강제 변경 X

---

### P0-5: `verification_mode` 잔존 코드 영구 제거

**파일/라인** (`scalper-agent/bot/auto_trader.py`):
| 라인 | 내용 |
|---|---|
| 1155 | `from data import verification_mode as _vm` |
| 1158 | `"🧪 [검증모드 ACTIVE]"` 메시지 |
| 1693 | `from data import verification_mode as _vm` (재import) |
| 1715 | 검증모드 사전 필터 (AVOID 종목 차단) |
| 1738 | `"🚫 [검증모드 사전 필터]"` |
| 1744 | `"🟡 [검증모드] 필터 후 매수 후보 0종"` |
| 1754 | `"💰 [검증모드 예산]"` |
| 1847 | `"🧪 [검증모드 매수 결과]"` |
| **1865** | ★ **`async def intraday_verification_scan_and_buy`** ★ |
| 1873 | `from data import verification_mode as _vm` (재import) |
| 2116 | `# - verification_mode 체크` 주석 |

**추가**: `bot/trading_coo.py` 15:25/15:35 검증모드 cron 등록 + 5/28 실측 skip 로그:
```
May 28 15:25:00 [COO] AUTO_TRADE_DISABLED — skip verification_close
May 28 15:25:15 [COO] AUTO_TRADE_DISABLED — skip intraday_verification_scan
May 28 15:30:15 [COO] AUTO_TRADE_DISABLED — skip intraday_verification_scan
```

**실패 모드**: 5/26 commit 79c504e + 5cb037d로 영구 차단 선언 후에도 코드 잔존 / `_order_gate`에서 차단되지만 함수 자체 존재 = 재발 위험

**관련 사고**: 5/26 D-Day 사고 #1 — 단타봇 자율 추가 verification_mode가 매일 47만 매매 (사장님 인지 X)

**필요 테스트**:
- 9곳 모두 제거 + 함수 `intraday_verification_scan_and_buy` 영구 제거
- 15:25/15:30/15:35 검증모드 cron 등록 제거
- 회귀: `verification_mode` import grep 0건 / `_vm` 변수 0건

**금지 작업**:
- 단타봇이 `auto_trader.py` 수정 X
- 단타봇이 `verification_mode.py` 삭제 X
- 단타봇이 cron 등록 변경 X

---

### P0-6: `kis_token*.json` `.gitignore` 추가

**파일/라인**:
- `scalper-agent/kis_token.json` (5/12 생성, 393 bytes)
- 프로젝트 `.gitignore` (현재 `kis_token` 패턴 미포함)

**실패 모드**: KIS API 인증 토큰이 git untracked 상태 / `.gitignore` 미포함 → 누군가 `git add .` 실행 시 자동 staging → public repo push 시 사장님 계좌 노출

**재현 절차**:
```bash
$ ls -la scalper-agent/kis_token.json
-rw-r--r-- 1 ASUS 197121 393 May 12 11:09 scalper-agent/kis_token.json

$ grep -i 'kis_token\|token.json' .gitignore scalper-agent/.gitignore
(0건)

$ git status --short | grep kis_token
?? scalper-agent/kis_token.json
```

**필요 테스트**:
- `.gitignore`에 `kis_token*.json` + `**/kis_token*.json` 추가
- `git check-ignore -v scalper-agent/kis_token.json` 통과
- 회귀: `git status --short`에서 `kis_token` 미포함

**금지 작업**:
- 단타봇이 `.gitignore` 수정 X (Codex 검수 후 적용)
- 단타봇이 `kis_token.json` 삭제 / 이동 X
- 단타봇이 `git add .` 실행 X

---

### P0-7: `.env` / `kill_switch.json` 이중 소스 정책 정리

**파일/라인**:
- `.env` (프로젝트 루트 또는 VPS `/home/ubuntu/bodyhunter/.env`)
  - `AUTO_TRADE_DISABLED=1`
- `scalper-agent/data/kill_switch.json`
  - `"AUTO_TRADE_DISABLED": false` (단타봇 09:44 변경)

**실패 모드**: 두 소스 우선순위 비문서화 / 단타봇이 `kill_switch.json`만 변경 → `.env` 우선 → 변경 무효 / 단타봇 09:53 "PAPER 진행 가능" 보고 = 실제 불일치

**재현 로그** (5/28 14:50:00):
```
14:50:00 [COO] AUTO_TRADE_DISABLED — skip asset_pool_previous_close
```
→ `.env=1` 우선 차단 / `kill_switch.json=false` 무효

**옵션** (Codex 판정):
1. `.env` 단일 소스 (`kill_switch.json` 의존 제거)
2. `kill_switch.json` 단일 소스 (`.env` 의존 제거)
3. 둘 다 유지 + 우선순위 명문화 + 변경 시 동기화 강제

**필요 테스트**:
- 단일 소스화 시 모든 `_auto_trade_disabled()` 호출 site 회귀
- 또는 우선순위 문서 + 변경 도구(`tools/toggle_auto_trade.py`) 작성
- 회귀: kill_switch / .env 어느 쪽 변경하더라도 봇 동작 일관

**금지 작업**:
- 단타봇이 `.env` / `kill_switch.json` 변경 X
- 단타봇이 우선순위 결정 X

---

## 4. P1 (blocker — paper 재개 전) 1건

---

### P1-8: `flow_collector` KIS shared throttle/backoff

**파일/라인**:
- `scalper-agent/data/flow_collector.py:277` — `time.sleep(0.12)`
- `scalper-agent/data/flow_collector.py:455` — `time.sleep(0.12)`

**실패 모드**: 두 수집 경로 병렬 호출 / 각각 `time.sleep(0.12)` (≈ 8.3 req/s) / KIS 계좌 기준 합산 초당 호출 수 초과

**재현 로그** (5/28 15:45+):
```
May 28 15:45:03 [data.flow_collector] WARNING: KIS 투자자 API 실패 466940:
  초당 거래건수를 초과하였습니다.
May 28 15:45:04 [data.flow_collector] WARNING: KIS 투자자 API 실패 121600:
  초당 거래건수를 초과하였습니다.
May 28 15:45:05 [data.flow_collector] WARNING: KIS 투자자 API 실패 036530:
  초당 거래건수를 초과하였습니다.
(이하 15+ 종목 연속 실패)
```

**영향**: flow_collector 데이터 수집 실패 다발 / paper 재개 시 신호 누락 가능

**필요 테스트**:
- shared throttle (token bucket / semaphore) 추가
- backoff (429 응답 시 exponential)
- 회귀: 100종목 연속 호출 시 실패 0건

**금지 작업**:
- 단타봇이 `flow_collector.py` 수정 X
- 단타봇이 throttle 파라미터 변경 X

---

## 5. 5/29 paper 재개 — 단언 X / 조건만 명시

본 의뢰서는 5/29 paper 재개 가능 여부를 **단언하지 않음**.

사장님 명시 재개 조건 (모두 충족 필수):
1. P0 7건 fix 완료
2. P1-8 fix 완료
3. 회귀 테스트 PASS
4. 사장님 별도 승인

조건 미충족 시 5/29 paper 재개 **금지**.

## 6. 단타봇 권한 한계 (이 의뢰서 작성 시 준수)

### ✅ 단타봇이 한 일 (작성만)
1. journalctl 실측 추출
2. Supabase 조회
3. 코드 라인 grep
4. 사장님 Codex 결정문 인용
5. 본 의뢰서 작성

### ❌ 단타봇이 하지 않은 일 (영구 룰)
1. ❌ 코드 수정 0건
2. ❌ `.gitignore` 수정 X
3. ❌ `systemctl` 변경 X
4. ❌ cron 변경 X
5. ❌ `AUTO_TRADE_DISABLED` / `kill_switch` 변경 X
6. ❌ `.env` 변경 X
7. ❌ "수정 완료" 표현 사용 X
8. ❌ "5/29 paper 가능" 표현 사용 X

## 7. 첨부 — Codex inbox JSON 의뢰

본 의뢰서 동일 시각 JSON 의뢰 생성:
- `scalper-agent/ops/codex_inbox/20260528T165654_daybot_p0_silent_fail_request.json`

## 8. 관련 메모리 (사장님 영구 룰)

- `project_5_28_codex_decision_final.md` — 사장님 [Codex 결정문 — 5/28 15:26~16:45]
- `project_4bot_architecture_blueprint_5_28.md` — 4봇 청사진
- `feedback_no_self_modification_5_28.md` — 단타봇 코드 수정 영구 금지
- `incident_2026_05_28_env_kill_switch_dual_source.md` — `.env` 이중 소스 사고
- `incident_2026_05_28_dantabot_arbitrary_5_stocks.md` — 임의 5종 사고
- `feedback_no_arbitrary_stock_selection_5_28.md` — 임의 종목 선정 영구 금지
- `project_codex_p0_obligations_5_28.md` — P0 의뢰 (이 의뢰서로 대체)
- `project_local_uncommitted_risk_5_28.md` — 로컬 위험 파일 분류

---

**의뢰서 작성 완료 / 단타봇 다음 행동 = 사장님·Codex 검수 대기 / 코드 수정 0건 유지**
