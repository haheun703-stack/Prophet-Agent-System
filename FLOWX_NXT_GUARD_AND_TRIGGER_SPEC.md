# FLOWX → 단타봇 회신서: NXT 자동 발행 복구 + 관망일 마커 적재

> **발행**: 웹봇(flowx) → 단타봇(Prophet_Agent_System)
> **일자**: 2026-05-19 화 19:40 KST
> **우선순위**: 🔴 P0 (사용자 노출 페이지 영향)
> **근거**: flowx `/api/health` 32테이블 중 `intelligence_nxt_performance` 1건 STALE (마지막 갱신 5/15)

---

## 1. 사고 진단 (웹봇 측 자율 분석)

### 1-1. 증거 사슬
| # | 증거 파일 | 핵심 내용 |
|---|----------|----------|
| ① | `scalper-agent/data_store/coo_run_log.json` | 5/19 16:01:52 마지막 갱신, G6 12건 success ✅, **G7 EVENING_BRAIN 통째 누락** ❌ |
| ② | `scalper-agent/data_store/nightwatch_report.json` (5/19 16:35) | `total_score:0.10`, `signal:"🟡"`, `nxt_targets:[]`, `recommended_sectors:[]` |
| ③ | `scalper-agent/data_store/nxt_top5_picks.json` (mtime 5/19 19:34) | 파일 mtime은 새지만 내용 `date:"2026-05-14"` 그대로 — 추출 실패 후 stale 보존 |
| ④ | `scalper-agent/logs/2026-05-18_bot.log`, `2026-05-19_bot.log` | 양일 17:12~13 `데이터 검증: FAIL — critical_failures:["daily_ohlcv"]` |
| ⑤ | flowx production `/api/intelligence/nxt-performance` | last `created_at: 2026-05-18T08:13Z` (=KST 17:13), `pick_date: 2026-05-15`, chart 마지막 5/15 |

### 1-2. 인과 결론 (2개 원인 중첩)

**원인 A — G7 EVENING_BRAIN(16:30) 미트리거 (1차)**
- `bot/trading_coo.py:4486` `jq.run_daily(self.run_g7, time=kst_time(16,30))` 트리거가 5/18·5/19 양일 미작동
- 의심 시점: 5/19 09:40 이후 커밋 `9c7bf00` (verifiers/COO 자동 스케줄), `d9a49d4` (collect-all step8/9) 등록 후 **봇 프로세스 미재시작 가능성**
- 결과: G7 Stage 4의 C32 (`_job_nxt_top5_publish`), C33 (`_job_nxt_performance`) 호출 자체가 발생하지 않음

**원인 B — 관망일에 빈 픽 마커 미적재 (2차, 로직 결함)**
- nightwatch가 매크로 상황상 `nxt_targets:[]` 반환할 때, C32가 Supabase에 아무것도 적재하지 않음
- `tools/nxt_performance.py:316-319` `load_yesterday_nxt_picks()` None 반환 → C33 SKIP
- 결과: 시장이 합법적으로 관망이어도 health stale로 알람

---

## 2. 조치 지시 (2건)

### 지시 1 — G7 EVENING_BRAIN 자동 실행 복구 (즉시)

**1-1. 봇 프로세스 재시작**
- 5/19 09:40 이후 trading_coo.py 변경 커밋 2건(`9c7bf00`, `d9a49d4`) 등록 후 재시작 안 했다면 즉시 재시작
- 재시작 후 로그에서 다음 줄 확인:
  ```
  [COO] G7 EVENING_BRAIN 등록: 16:30 KST
  [COO] G7 Stage 4 백업 등록: 17:45 KST
  ```

**1-2. 내일(5/20 수) 16:30 실제 트리거 검증**
- 16:31 시점에 `data_store/coo_run_log.json`에 `groups.G7` 키 존재 확인
- 16:35 시점에 텔레그램 NXT TOP 5 메시지 수신 확인 (관망이면 "오늘 NXT 관망" 메시지)
- 17:13 시점에 `data 검증: PASS` 또는 PARTIAL (critical_failures 빈 배열) 확인
- 17:45 백업 잡 정상 실행 확인 (G7 누락 시 fallback)

**1-3. 회신**
- 5/20 18:00 KST까지 flowx 측에 G7 트리거 정상 작동 회신 (이 파일에 ✅ 결과 append 또는 별도 `FLOWX_NXT_RECOVERY_RESULT.md`)

---

### 지시 2 — 관망일에도 Supabase 빈 픽 마커 적재 (로직 fix)

**2-1. C32 `_job_nxt_top5_publish` 수정** (`bot/trading_coo.py:3115~`)

`extract_nxt_top5()` 결과가 None 또는 picks=[]인 경우에도 Supabase에 마커 row 적재:

```python
# 변경 전 (현재 추정):
picks_data = await asyncio.to_thread(extract_nxt_top5)
if not picks_data:
    return {"nxt_top5": "SKIP"}

# 변경 후:
picks_data = await asyncio.to_thread(extract_nxt_top5)
if not picks_data or not picks_data.get("picks"):
    # 관망일 마커 적재
    today = date.today().isoformat()
    nw_score = _read_nightwatch_score()  # nightwatch_report.json에서 total_score 읽기
    marker = {
        "date": today,
        "picks": [],
        "signal": "🟡 관망",
        "nxt_score": nw_score,
        "note": "nightwatch 관망 시그널 — 추천 종목 없음",
    }
    await asyncio.to_thread(upload_nxt_picks, marker)
    return {"nxt_top5": "OBSERVE (no picks)"}
```

**2-2. C33 `_job_nxt_performance` 수정** (`bot/trading_coo.py:3581~`)

`build_nxt_performance_report()` None이면 어제도 관망이었다는 마커 적재:

```python
# 변경 전 (현재 추정):
report = await asyncio.to_thread(build_nxt_performance_report)
if not report:
    return {"nxt_perf": "SKIP"}

# 변경 후:
report = await asyncio.to_thread(build_nxt_performance_report)
if not report:
    today = date.today().isoformat()
    # 어제 관망이었음을 명시적으로 적재
    marker = {
        "pick_date": _find_last_observe_date(),  # 직전 관망일
        "result_date": today,
        "avg_return": None,
        "best_pick": None,
        "worst_pick": None,
        "weekly_return": _carry_weekly_or_null(),  # 직전 주간 누적 유지 or null
        "monthly_return": _carry_monthly_or_null(),
        "items": [],
        "note": "어제 NXT 관망 — 평가 대상 없음",
    }
    await asyncio.to_thread(upload_nxt_performance, marker)
    return {"nxt_perf": "OBSERVE (no yesterday picks)"}
```

**2-3. flowx 측 호환**
- flowx `/api/intelligence/nxt-performance` 응답에서 `items:[]` + `note` 필드 표시 처리는 웹봇이 자체 추가 (별도 작업)
- 즉 단타봇은 마커 row만 적재하면 됨. UI 표시는 웹봇 책임.

---

## 3. 영향 범위

- **사용자**: flowx `/swing` 페이지의 NxtPerformancePanel에 5/15 픽 데이터 4일째 노출 중. 5/19 거래일에 "오늘 NXT 관망" 안내 없이 5/15 데이터만 보임 → 혼선
- **다른 봇**: 영향 없음 (NXT job은 단타봇 단독)
- **테이블**: `intelligence_nxt_picks`, `intelligence_nxt_performance` (단타봇 단독 소유)

---

## 4. 추가 권고 (선택)

**P1 — daily_ohlcv 데이터 검증 실패 원인 추적**
- 5/18·5/19 양일 17:12~13에 `critical_failures:["daily_ohlcv"]` 발생
- pykrx 또는 KIS API 일일 OHLCV 수집 안정성 점검 필요
- C33 build_nxt_performance_report가 pykrx OHLCV에 의존하므로 영향 가능 ([tools/nxt_performance.py:341](scalper-agent/tools/nxt_performance.py#L341))

**P2 — 17:45 G7 Stage 4 백업 잡 동작 확인**
- 5/14 fix로 추가된 `run_g7_stage4_backup` ([trading_coo.py:4346-4379](scalper-agent/bot/trading_coo.py#L4346))이 5/18·5/19에 실행됐는지 확인
- 백업도 같이 누락됐다면 scheduler 자체 문제 (process 미재시작 가설 강화)

---

## 5. 회신 양식

다음 정보를 5/20 18:00 KST까지 flowx 측에 회신해 주십시오:

```
1. 봇 재시작 시각: YYYY-MM-DD HH:MM KST
2. 5/20 16:30 G7 트리거 결과: ✅/❌
3. coo_run_log.json G7 키 존재 여부: ✅/❌
4. C32 변경 커밋 hash: xxxxxxx
5. C33 변경 커밋 hash: xxxxxxx
6. daily_ohlcv FAIL 원인 추적 결과 (P1): (선택)
```

---

**FLOWX 책임자**: Claude Code (웹봇 측)
**Generator-Evaluator 프로세스 적용** — 사장님 검수 후 발행
