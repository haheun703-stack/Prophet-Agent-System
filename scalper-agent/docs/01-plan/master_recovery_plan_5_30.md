# 마스터 복구 플랜 (5/30) — 증상 추적 종결, "구조"로 끝낸다

> 사장님 명령(5/30): "전체 문제점 발굴 → 해결방안 목표/목차 도식화 → 진행."
> Opus 4.8(단타봇 설계·검증) + Codex(구현) + 사장님(승인) 3자 체계.
> 단타봇 read-only. 본 문서 = 단일 로드맵(끝의 정의 = audit --gate 8/8 green).
>
> ★ **목적 정정(5/30 사장님)**: 단순 "안전 방어"가 아니라 **자동매매 재가동 + 수익률 개선**이다. 고정TP/하드코딩TP·SL/10%현금/기록누락은 보존하지 말고 사장님 룰로 **교체·삭제·재구성**. 단 "고정TP 제거 = 매도 제거" 아님 → **동적 단타 청산(트레일링·D+0~D+3·룰B/C/D) 강화**. 매도 철학 8원칙은 Phase 1 설계서 참조. ★
> 리딩=단타봇 / 구현=Codex(삭제·교체·재구성) / 검증=단타봇(audit·paper·intent).

---

## 0. 왜 1주일째 끝이 안 났나 (근본 원인 — 정직)

```
   ┌─────────────────────────────────────────────────────────┐
   │  두더지 잡기 악순환 (5/24 ~ 5/30)                          │
   │                                                           │
   │   증상 발견 ──> 말단 fix ──> 새 증상 발견 ──> 또 fix ──┐    │
   │      ▲                                                │    │
   │      └────────────────────────────────────────────────┘    │
   │                                                           │
   │   원인: ① 구조 안 건드리고 말단만 막음                       │
   │        ② "끝"의 객관 기준 부재 (게이트 도구가 5/29에야 생김) │
   │        ③ 단타봇 LLM 본성(환각/추정/매번 새 발견)            │
   │        ④ fix 단위가 "증상 리스트"(C1~5, P0 9, 32건…매번 흔들림)│
   └─────────────────────────────────────────────────────────┘
            ↓  ↓  ↓   전환
   ┌─────────────────────────────────────────────────────────┐
   │  구조 종결 (5/30 ~ )                                       │
   │   증상 N개 ──> 5개 구조 결함으로 수렴 ──> 한 번에 닫음       │
   │   ──> audit --gate 8/8 green (단 하나의 종료 조건)          │
   └─────────────────────────────────────────────────────────┘
```

**핵심 전환**: "결함 리스트를 쫓는다"(끝없음) → "구조 5개를 닫으면 게이트가 초록"(유한·객관).

---

## 1. 전체 문제 지도 (실측 근거 = audit --gate + v6/v7 진단 + full_review_5_30)

```
                        [ 자동매매 재개 불가 = P0 HOLD ]
                                     │
        ┌────────────┬──────────────┼──────────────┬───────────────┐
        ▼            ▼              ▼              ▼               ▼
   RC-1 주문경로   RC-2 단일진실   RC-3 다중원장   RC-4 silent-fail  RC-5 검증 비대칭
   파편화          미적용          +lifecycle      (오보고)          (LLM 본성)
        │            │              │              │               │
   CHECK-1(8)    CHECK-6(32)    CHECK-2(7)     CHECK-7(3)       (도구로 해소:
   CHECK-4 ✅     CHECK-8(3)                    +success오보고     audit --gate)
   CHECK-5 ✅                                   +history리셋
```

### audit --json 현재 상태 (commit e9e3c19 실측 — ★ 도구가 진실, 메모리 숫자 정정 ★)
```
 CHECK-1  paper open/close no intent       [FAIL]  13건 (close 9 + open 4)
 CHECK-2  TradeTracker no intent           [FAIL]  7 메서드 무기록
 CHECK-3  caller pre-block                 [PASS]  (monitor 19)
 CHECK-4  gate write enforce               [PASS]  ← 5/30 C5 fix (4 branch propagate)
 CHECK-5  order_gate coverage              [PASS]  ← 7 메서드 전부 게이트
 CHECK-6  fixed TP/SL via SAJANG           [FAIL]  order_path REAL 42 (+uncertain 13 / 제외 SIM9·DASH2 / 총 66 site)
 CHECK-7  intent forensic fields           [FAIL]  4키 누락(order_no/rt_cd 등) — record_keys 14
 CHECK-8  30% cash reserve via SAJANG      [FAIL]  order_path 3 (kis_trader:1803·trading_cfo:106·+1)
 ────────────────────────────────────────────────────────────
 GATE = FAIL (fail_count=5: CHECK-1·2·6·7·8)   →  남은 적색 5개 = 잔여 작업 전부
```

> ★ 정정(RC-5 실증): 메모리엔 "CHECK-6=32 / CHECK-1=8 / CHECK-7=silent skip 3"이었으나 도구 실측 = **42 / 13 / forensic 4키**. 단타봇 기억이 또 틀렸고 도구가 진실. 이래서 게이트가 유일 판정. ★
> 남은 결함 총량 ≈ **65개 항목** (42 + 13 + 7 + 4키 + 3) — 무한이 아니라 **유한·셀 수 있음**. 단, CHECK-6 uncertain 13은 추가 판정 후 일부 REAL 편입 가능. 이게 "끝이 보인다"의 근거.
> ★ 도구가 이미 ORDER_PATH / SIM_LEARNING / DASHBOARD_UPLOAD / UNCERTAIN 4분류 수행 → "audit v3 OVER_MATCH 제외 기능"은 대부분 이미 존재. 남은 v3 작업 = UNCERTAIN 13 판정뿐. ★

---

## 2. 근본 원인 5개 (RC) + 처방

| RC | 구조 결함 | 증상(CHECK, 도구 실측) | 처방(한 번에 닫는 방식) |
|----|----------|----------------------|----------------------|
| **RC-1** | 주문 경로 N갈래(smart_buy/sell·chase·safe·vwap·buy_market·afterhours·nxt·limit_up) 각각 게이트/intent 제각각 | CHECK-5 PASS(7메서드)·CHECK-4 PASS — **재발 방지 중** | 모든 경로 **단일 `_order_gate` 의무 통과** 유지 |
| **RC-2** | TP/SL·자금이 SAJANG 안 거치고 하드코딩 | **CHECK-6(42)·CHECK-8(3)** | 전 경로 `SAJANG.get_take_profit/get_trailing_sl/max_buy_amount`로 치환 |
| **RC-3** | 원장 다중(PaperPortfolio·TradeTracker·trade_object) + paper open/close lifecycle intent 미기록 | **CHECK-1(13)·CHECK-2(7)** | **단일 원장 + open/close 시 order_intent 의무** |
| **RC-4** | 주문 의도 기록에 사후추적(forensic) 필드 누락 — "샀다 후 무슨 일" 추적 불가 | **CHECK-7(4키: order_no/rt_cd 등)** | order_intent record에 forensic 4키 추가 + 주문 후 intent 갱신 |
| **RC-5** | 단타봇 LLM이 "봤다/32건"이라 오보고 → 검증 신뢰 불가 (★ 오늘 32 vs 실측 42로 재실증 ★) | (메타) | **객관 게이트 audit --json/--gate가 유일 판정·worklist** + Codex 상호검증 |

---

## 3. 목표 (단 하나의 종료 조건 — 도식)

```
  [5개 RC 닫기] ──> [audit --gate exit 0 = 8/8 GREEN]
                          │
                          ▼
        [paper 리허설: 모든 주문·차단 order_intent 1:1 실증]
                          │
                          ▼
            [사장님 승인] ──> [live 소액 제한 가동]
```

★ "검수 통과/보장" 표현은 **게이트 green + 사장님 승인 전까지 영구 금지** (5/26 룰). ★

---

## 4. 목차 / 단계 (Phase 로드맵)

```
 Phase 0 ✅ 완료   C1/C2/C4/C5 + fetch_minute_chart  → commit e9e3c19 (push 완료)
                  (CHECK-4/5 GREEN 전환)

 Phase 1 ▶ 다음   RC-2: CHECK-6 (고정 TP/SL REAL 42건 → SAJANG)   ← 가장 큰 덩어리
                  + CHECK-8 (현금 3건 → SAJANG.max_buy_amount)
                  + CHECK-6 UNCERTAIN 13건 판정(REAL/제외)
                  설계(단타봇) → Codex 구현 → audit 재실행 검증

 Phase 2          RC-3: CHECK-1 (paper open/close intent 13건)
                  + RC-4: CHECK-7 (forensic 4키 추가 — 가장 쉬운 단일 fix)

 Phase 3          RC-3: CHECK-2 (TradeTracker 7 메서드 — 단일원장 intent)

 Phase 4          audit --gate 8/8 GREEN 확인 → 슬러지 격리(데드코드/verification cron)
                  → paper 리허설(intent 1:1 실증)

 Phase 5          사장님 승인 → live 소액 (AUTO_TRADE_DISABLED 해제는 이때만)
```

### Phase별 종료 기준 (객관)
| Phase | Done 기준 (도구 판정) |
|-------|----------|
| 1 | `audit --json` CHECK-6 REAL=0 + CHECK-8 real_count=0 (verdict PASS) |
| 2 | CHECK-1 missing=0 + CHECK-7 missing keys=0 |
| 3 | CHECK-2 missing_intent_count=0 |
| 4 | `--gate` exit 0 (fail_count=0) + paper 리허설 intent 1:1 |
| 5 | 사장님 승인 서명 |

---

## 5. 역할 분담 (3자 체계 — 도식)

```
  ┌── 단타봇 (Opus 4.8) ──┐   ┌──── Codex ────┐   ┌── 사장님 ──┐
  │ • 결함 실측/설계서     │──▶│ • 코드 구현    │──▶│ • 큰결정    │
  │ • Tier-1/2 직접검증    │   │ • Tier-4 상호  │   │ • 룰 변경   │
  │ • audit --gate 판정    │◀──│   검증         │   │ • live 승인 │
  │ • read-only (코드수정X)│   └───────────────┘   └────────────┘
  └────────────────────────┘
        ▲                                                │
        └──────────── 게이트 green 까지 반복 ◀────────────┘
```

- 단타봇 코드 직접 수정 **영구 금지** (설계·검증·게이트만).
- 매 Phase = 단타봇 설계 → Codex 구현 → 단타봇 audit 검증 → (룰/자금/ live는) 사장님 승인.

---

## 6. 일정 (간트 — 휴장일 가드)

```
        5/30(토,휴장)  5/31(일)   6/1(월)      6/2(화)~
 P1 설계 ████          
 P1 구현      ████ Codex
 P1 검증           ████
 P2/P3                  ████████ 설계·구현·검증
 P4 게이트green                  ████ + paper 리허설(장중)
 P5 승인/live                        ████ 사장님 승인 후 소액
```

> 5/30·5/31 휴장 → 설계·구현·게이트 검증은 휴장에도 진행(주문 X). paper 리허설/실측은 6/1(월) 장중.

---

## 7. 다음 1스텝 (즉시 착수)

**Phase 1 설계**: CHECK-6 32건 + CHECK-8 3건을 SAJANG 헬퍼로 일원화하는 설계서 작성
→ 구체 32+3 항목은 `audit_order_paths.py --json`(v3)으로 **권위 확정** 후 1:1 fix 매핑
→ Codex 의뢰 → 구현 → `audit --gate` CHECK-6·8 PASS 검증.

### 대상 항목 (★ 손으로 나열 금지 = RC-5 함정. 도구가 worklist ★)
- **확정 방법**: `python tools/audit_order_paths.py --json` → `checks["CHECK-6_fixed_tp_sl_violation"]["found"]["order_path_real"]` (42개 배열, 각 {file,line,snippet,classification}) = SAJANG 치환 대상.
- CHECK-8: 동일 JSON `["CHECK-8_cash30_single_source"]["found"]["order_path_real"]` (3개: kis_trader:1803=C3 · trading_cfo:106 · +1).
- CHECK-6 UNCERTAIN 13개: `[...]["found"]["uncertain"]` → Codex가 REAL(치환) vs 제외(분류 보정) 판정.
- Codex가 --json을 직접 떠서 작업 (단타봇이 42줄 옮기면 또 틀림 = 오늘 32 오보고 교훈).

---

## 부록 — 슬러지/후속 (Phase 4 격리 대상, 게이트와 별개)
- `_job_intraday_verification_scan` 라이브 cron 잔존(재무장 위험) → 제거
- A5P `is_trading_day()` 가드 누락(5/30 토요일 실행 실증) / 15:45 NameError → fix
- Kiwoom/main.py 거대 데드클러스터 / paper 다중원장 → legacy 격리
- 토큰 로그 마스킹 15곳 / kis_token 0o600
