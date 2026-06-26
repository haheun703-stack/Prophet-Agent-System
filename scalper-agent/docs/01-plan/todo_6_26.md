# 단타봇 TODO — 6/26(금)

> 작성: 단타봇 6/25 밤(KST). 사장님 "나머지는 내일 TODO로 저장하고 내일 보자".
> 안전 전제: 봇 PAPER 모드(active·PAPER_ONLY=true·실주문 0)·picks 불변·매도 무손상·SAJANG 단일진실·관측 없이 flip 금지.
> 근거 메모리: project_paper_perf_korean_swing_6_25 / project_full_audit_6_24 / project_foreign_exh_nightly_timing_6_24

---

## ✅ 6/25 완료분

- **6/25 기초데이터 점검(KIS 4종)** — nightly 17/17 성공. 종가 3349(진짜누락7)·investor 2594·F1 14건 완비. ★foreign_exh 19:40 late-recollect cron **첫 자동실행 성공**(11%→2508/2596 96.6%·당일적재2493·삼성 47.38%·종가358500 daily정합). 6/24 자동화 1사이클 완결 — 이후 평일 무인 자동충전.
- **한국스윙 페이퍼 누적수익률 적재 구현** (commit bc7f1d5·push·VPS배포):
  - `data/upload_swing.py::_build_paper_performance()` — PaperPortfolio read-only(_save 미호출·장부무손상) → 전체자산(미실현포함) + 청산합(avg 병기) **둘 다 나란히** JSONB.
  - `upload_dashboard_swing` row에 `paper_performance` 추가 + graceful(컬럼없어도 본류무해).
  - SQL `sql/20260625_dashboard_swing_paper_performance.sql` + 웹봇 요청 문서.
  - 4-Tier PASS(T1 95/100 매매무접촉·장부무손상 / T2 0건 / T3 VPS 6/25 −12.51%·today실현−180103 정합 / T4 Codex5 / T5 pre-commit).

## ✅ 6/26 완료분

- **E·F·G catalyst 관측정확도** (commit ba5d692·push·VPS배포):
  - E `_is_today_loaded` 모드감지 — 마감후(오늘봉적재)=_ignition·장중(어제까지)=_intraday. nightly 진짜점화 놓침 fix.
  - G `IGNITE_CHASE_MAX=25` 상한 통일 + `_maek_jeom` is_upper 우선 — 상한가 초입점화 오라벨 방지.
  - F `_find_next_candidates` AI 동음이의 — 조류독감(Avian Influenza)·축산질병 테마 제외.
  - 4-Tier PASS(T1 96/100 record-only무손상·is_upper동치성 정밀검증 / T2 0건 / T3 단위+VPS실증 장중모드감지·상한가추격위험유지 / T4 Codex5 / T5 pre-commit).

---

## ★ 내일 할 것 — 우선순위

### 0순위(즉시·간단) — 한국스윙 페이퍼 적재 마무리
- **(사장님)** Supabase SQL Editor에 1줄 실행:
  `ALTER TABLE dashboard_swing ADD COLUMN IF NOT EXISTS paper_performance JSONB;`
  (단타봇 query()는 읽기전용 설계라 DDL 불가 — 사장님/웹봇 영역)
- 실행 후 → 다음 upload_dashboard_swing 시 자동 적재 → **한국스윙 '시장판단&전략' 옆 렌더 실측**(paper_performance 1건 확인).
- 웹봇 회신 받기(렌더 위치/표기·한국색상·paper관측배지). 요청문서=`docs/[단타봇 → 웹봇] 페이퍼 매매 누적수익률 한국스윙 적재 요청_2026-06-25.md`.
- ★ 정직성: 단타봇 자체 페이퍼 −12.51%(미실현포함·악화중) 직시. 스크린샷 +6.7%는 통합봇(quantum-master)·청산합 방식이라 낙관편향.

### ✅ 1순위 완료(e4c722d·6/26) — D: limit_up_split_sell SAJANG 단일진실 치환 (라이브 매도경로)
> 모듈상수 4개 SAJANG 파생화·함수로직 0변경·동작무변경(25/10/3/-7 불변). 4-Tier PASS(T1 Crit0·순환참조0 / T2 우회0 / T3 단위27/27+경계 / T4 Codex5). SPLIT_RATIO만 유지(SAJANG LIMIT_UP_HOLD_RATIO와 의미반대). AUTO_TRADE_DISABLED=True(봇OFF)라 봇 가동 시 자동반영·실발동0.
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

## ✅ 별건 완료 — IMP-001 오탐 fix (commit b196beb·cp949 오진 정정)
- **진짜 원인 = 인라인 주석 파싱 버그**(cp949 아님). `from x import is_trading_day  # noqa` → name에 주석이 통째로 붙어("is_trading_day  # noqa") 실제 사용처와 매칭 안 돼 occurrence=1 → IMP-001 오탐.
- fix: `check_unused_imports`가 imports_str에서 인라인 주석(# 이후) 제거.
- read_text는 최초 커밋(2670356)부터 utf-8 — cp949로 읽으면 UnicodeDecodeError로 파일 SKIP(미검사)지 오탐 아님. 6/24 "cp949" 진단은 오진이었음.
- 재현+회귀 PASS(오탐파일 0건 / 미사용 path→1·noqa+미사용→1 탐지 보존).

---

**전부 봇 PAPER·실주문 0·매도 무손상 전제. D는 라이브 매도경로라 4-Tier 필수·사장님 승인 후.**
