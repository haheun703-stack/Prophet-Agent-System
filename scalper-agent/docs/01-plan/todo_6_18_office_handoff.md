# 단타봇 사무실 인계 TODO — 6/18(목) 오전 작업 후속

> 작성: 단타봇 6/18 오전(KST). 사장님 "사무실 가서 마저 하자" → 인계.
> 안전 전제: 봇 관측모드(active·AUTO_TRADE_DISABLED=True·실주문 0)·picks 불변·매도 무손상.
> 근거 메모리: project_jarvis_rt_fixed_tp_unify_6_18

---

## ✅ 오전 완료분 (전부 push 완료: bf14bef → 30d5fe6)

가동전 P0 **4건 전부 해소** — 고정TP 잔재 SAJANG 단일진실 통일:
- **A-1** jarvis +10%고정TP·시총별SL → SAJANG.get_take_profit/get_normal_sl (commit 1b66c0a)
- **A-2** RealtimeMonitor tp=0 즉시전량매도 트랩 가드 (commit 1b66c0a)
- **B-2** dynamic_target ATR dynamic_tp → pos take_profit 누수 3곳(생성2·재평가1) 차단 (commit 3f09481)
- **B-1** 상한가 +25% 룰7 장중 즉시 분할 라이브 연결 (신규 job_limit_up_split_check, commit 30d5fe6)
- 4건 모두 4-Tier PASS + **통합 안전 재검수 PASS**(4건 상호작용 일관·이중매도/안팔림 0·기존 단위테스트 27/27·jarvis 회귀)

---

## ★ 사무실 1순위 — VPS 봇 최신코드 재시작 (사장님 결정 1건 대기)

**현황(오전 확인)**:
- VPS 봇 = **active** (6/14 04:02 기동, 관측모드). AUTO_TRADE_DISABLED=True / MORNING_AUTO_BUY_DISABLED=True → 실주문 0 (로그 전부 "AUTO_TRADE_DISABLED — skip").
- **VPS 코드 = bf14bef (옛 버전)** — 오늘 4 commit 미반영. 봇 프로세스는 6/14 코드 메모리로 돎.
- 오늘 4 commit은 **전부 매도/TP 경로** → AUTO_TRADE_DISABLED 상태선 비활성 → **관측 데이터 영향 0**. 새 코드 반영은 **실매매 해제 전에만 필수**.

**✅ 사장님 결정(6/18 사무실) = ① 오늘 20:00 이후 재시작**.

**✅ cron 자동 pull 확인 완료(6/18 12:58 실측)**:
- `deploy_pull.sh` = 17:45(평일) + 06:00(매일) cron 등록·**정상 작동**(오늘 06:00 클린 실행 로그 "runtime restored / done").
- 동작 = `git pull --ff-only` + 런타임 파일(kill_switch/runtime_config/universe) stash 보존. **봇 재시작은 안 함**(스크립트 명시).
- 06:00엔 origin이 아직 bf14bef라 미반영 → **오늘 17:45 cron이 디스크를 bf14bef → 3a76e1b 자동 pull 예정**.
- 봇 프로세스 = PID 400, 6/14 04:02 기동(4일+) → 옛 코드 메모리 유지 확정 → **재시작 필수**.

**→ 20:00 작업 = `restart만`으로 확정** (pull은 17:45 cron이 처리):
1. 디스크 HEAD = `3a76e1b` 확인(17:45 cron 결과). 만약 미반영이면 수동 `deploy_pull.sh` 먼저.
2. `sudo systemctl restart bodyhunter-bot.service`
3. 검증: 크래시0 · `job_limit_up_split_check` 2분주기 등록 · 게이트 둘다 True(AUTO_TRADE_DISABLED·GLOBAL) · 실주문0 · 게이트 skip 로그.

---

## 진행중 관측 (자동 — VPS 18:00 nightly)

- **early shadow 2주 관측** ~6/30 만기 → 사장님 flip 결정 (strict vs early forward·pos20 단조성·거래량·would_stop). 관측 없이 flip 금지.
- **B-1 M-1**: 룰7/룰B 경계 종목(+24~26%) paper 관측 권장 (실경로는 구간분리로 차단됨).
- paper/forward/매매일지 — nightly 자동 누적. 봇 관측모드라 계속 쌓임.

## 후순위 (저위험·저가치)

- **D 슬러지 정리**: engine/risk/구 strategies(구 PyQt 잔재, test만 import) → /deprecated 이동 or 주석. git history 보존.

---

## ⚠️ 메모 (오전 발견)

- **로컬 노트북 PC 시계 9시간 오차**: 로컬 02:30 vs VPS(NTP) 11:30. **VPS 시각이 진실**. 로컬에서 시각 기반 판단 금지(재시작 안전 윈도우 등은 VPS 시각 기준).
- pre-commit IMP-001 "미사용 SAJANG L4576" = 오탐(L4655·L4701 실사용, 함수스코프 휴리스틱 한계). MEDIUM 허용.

## 안전 체크 (불변)
- 봇 OFF 게이트 둘 다 True 유지 / 실주문 0 / picks 불변 / 매도 무손상 / SAJANG 단일진실
