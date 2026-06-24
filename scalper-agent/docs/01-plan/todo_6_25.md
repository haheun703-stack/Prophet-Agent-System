# 단타봇 TODO — 6/25(목) : 6/24 전체검수 남은 항목

> 작성: 단타봇 6/24 밤(KST). 사장님 "나머지는 내일 TODO로 저장하고 내일 보자".
> 안전 전제: 봇 PAPER 모드(active·PAPER_ONLY=true·실주문 0)·picks 불변·매도 무손상·SAJANG 단일진실.
> 근거 메모리: project_full_audit_6_24 (5에이전트 전체검수 — 안전 PASS·Critical 0)

---

## ✅ 6/24 완료분 (전부 push 완료)

- **foreign_exh 늦은-게시 재수집** — run_foreign_exh_late_recollect.py + cron 19:40 (commit 296cf02). nightly ⑥ 18:09 조기수집 보완. 4-Tier PASS.
- **전체검수 확정버그 A·B·C 수정** (commit d1eee78·VPS배포·봇재시작 21:14):
  - A trading_coo.py:3105 죽은 import(`tools`→`data.trading_calendar`) — 동시호가 잡 매일 silent사망 fix
  - B kis_trader.py:1841 nxt_safe_buy 0원 나눗셈 가드
  - C auto_trader.py:4729 daily_reeval entry_price 가드(0원 포지션 SL 재평가 스킵 방지)
  - 4-Tier PASS. Codex가 C를 CRITICAL 차단했으나 단타봇 재확인=오해(재평가 스킵→매도 혼동·동작무변경)→사장님 confirm 후 진행.

---

## ★ 내일 할 것 — 우선순위

### 1순위 — D: limit_up_split_sell SAJANG 단일진실 치환 (라이브 매도경로)
- 위치: `bot/limit_up_split_sell.py:47-51`
- 문제: 룰7/룰B/룰C 임계값(LIMIT_UP_TRIGGER_PCT=25 / EOD_SPLIT=10 / D1_TRAIL=3 / D1_GAP=-7)을 **SAJANG import 0**으로 모듈상수 재정의. `trading_coo.py:5245` job_limit_up_split_check(2분반복) 라이브 호출. 현재값은 SAJANG과 일치하나 사장님이 SAJANG 바꾸면 이 매도경로는 안 따라감(드리프트).
- 조치: 모듈상수 → SAJANG.LIMIT_UP_SPLIT_THRESHOLD / RULE_B_THRESHOLD / TRAILING_PCT / D1_GAP_SELL_THRESHOLD 참조 치환.
- 검수: 라이브 매도경로라 4-Tier 필수. + auto_trader.py:3539 mtm_sl -4.5% 하드코딩도 같이 검토(MOMENTUM SL 의도면 SAJANG 상수화).

### 2순위 — E: catalyst `_ignition` 모드 충돌 (관측 정확도 — 돈버는 길 직결)
- 위치: `tools/catalyst_scanner.py` `_ignition`(384-396, 오늘봉 포함가정) vs `_intraday_ignition`(168-192, 어제까지 가정)
- 문제: 둘이 상반된 가정인데 scan_catalyst가 무조건 둘 다 호출·모드감지 없음. nightly는 일봉 fill 후 catalyst라 **마감후에도 "어제까지" 함수가 오늘봉 섞인 rows로 돌아 진짜 점화를 놓침**. 관측 데이터 오염 → "관측→검증→flip" 전제 훼손. 보해양조식 포착 정확도 직결.
- 조치: 실행모드(장중 vs 마감후)를 명시 플래그로 받고 한쪽 ignition만 사용. 또는 _intraday_ignition에서 rows 마지막행==today면 잘라 "어제까지" 정규화.
- 검수: record-only라 매매영향 0이나 관측정확도라 중요.

### 3순위 — F/G: catalyst 오탐·오라벨
- F `tools/catalyst_scanner.py:330` `_find_next_candidates` — `"AI"` 2글자 substring이 `"SI,AI,ASF,조류병"`(축산주 62종목)과 매칭=오탐. 조치: 영문 짧은토큰 단어경계 매칭(`\bAI\b`) 또는 한글 테마키워드만.
- G 마감후 경로에서 상한가(+30%)가 "추격위험" 아닌 "초입점화"(매수자리) 오라벨. _ignition에 상한가 상한(today_chg<25) 적용 또는 _maek_jeom에서 is_upper를 초입점화보다 먼저 검사.

### 4순위 — 후속: SL 폴백 가드 통일 (C와 동류)
- 위치: `bot/auto_trader.py:4461` `_job_monitor_fallback`
- 문제: `entry = pos["entry_price"]` 직접 인덱싱(.get 아님). 0원은 4468 `if entry>0`로 자체커버되나 entry_price 키 완전누락 시 KeyError 잔존. (C가 막은 것과 같은 클래스)
- 조치: `pos.get("entry_price", 0)` + 가드 통일.

---

## 슬러지 정리 (별도 트랙 — 삭제 아님·격리/통합·사장님 결정)

- **H ★ Kiwoom v1.0 통째 데드 48모듈 + main.py** — 진짜봇=run_bot.py→telegram_bot→trading_coo/auto_trader(KIS). main.py·api/·backtest/·죽은 engine/strategies/risk는 키움(계좌없음)용. **신규세션/Codex가 진짜매매로 오인할 혼란이 최대비용** → 삭제말고 아카이브 디렉토리 격리 또는 README "DEAD-KIWOOM" 명시. (살아있는 engine 4개·strategies 12개·paper shim은 보존)
- **I orphan 73 + doc-only 14 ≈ 90+ 정리후보** (import 0건). 일회성 study/backtest(`*_5_31`·`*_6_2` 류), make_icon/make_shortcut, manual_order 등.
- **J 기관매집 스캐너 중복 2개** — `data/inst_accumulation_scan.py`(구·COO 3622) vs `data/institution_accum_scan.py`(신·6/19 KRX-free·nightly⑩). 둘 다 trading_coo 배선 → 어느 출력이 실소비되는지 확인 후 구버전 통합.

## 별건 — 인프라 개선
- **cp949 pre_commit_check 버그** — `tools/pre_commit_check.py check_unused_imports`가 한글 포함 파일을 cp949로 읽어 occurrence 적게 세 IMP-001 오탐 남발(6/24 is_trading_day·SAJANG 모두 오탐). open()에 encoding="utf-8" 명시로 fix.

---

**전부 봇 PAPER·실주문 0·매도 무손상 전제. D는 라이브 매도경로라 4-Tier 필수·사장님 승인 후.**
