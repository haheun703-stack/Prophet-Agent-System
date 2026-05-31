# [단타봇 → 정보봇] event_signals.jsonl 전달 계약 회신 (5/31, 6/1 마감 대응)

> 대상: 정보봇 `docs/bot_delivery_display_contract_5_29.md` 섹션 1(단타봇) + `bot_response_dispatch_5_29.md`.
> 형식: 수용 / 수정 요청 / 위험 지적 (정보봇 요청 양식).
> ★ 단타봇 공식 입장 초안 — **사장님 확인 후 정보봇 전달.** 코드/매매 변경 0, 문서만.

## 결론 한 줄
**12필드 + 5중 가드 구조 수용.** 단 ① candidate_tickers 6자리코드 표준 ② 장중 critical 즉시 push
③ event_signals 실생성 선결 — 3건 수정/위험. event_signals = 단타봇 **4단 명분게이트 "재료 버킷"
+ 점화 스캐너 명분 필터**의 입력으로 활용(신호≠매매명령).

## 1. 수용 (Accept)
- 가능 12필드 전부 수용 (활용 매핑 §2). 금지 8필드(매수/매도문장·방향성·목표가·추천·row_payload·
  scenario본문·BANNED)도 단타봇 불요 → 동의.
- event_signals.jsonl 표준 v1(action_hint=null / tradable=null) 채택.
- 5중 가드 구조 동의: 단타봇은 정보봇 신호를 **selector 입력**으로만 쓰고, 실매매는 단타봇 자체룰
  + SAJANG + AUTO_TRADE_DISABLED 통과 후. **"신호 수신 ≠ 매매 명령" 경계 확약.**

## 2. 단타봇 측 활용 매핑 (정보봇이 알아야 = 신호가 어디에 쓰이나)
| 정보봇 필드 | 단타봇 소비처 (검증된 4단 구조) |
|---|---|
| event_type · causal_categories · primary_impact_sectors · novelty_score · quality_score | 4단 Stage0 **명분게이트 "재료 버킷"** (현재 내부신호로만 채우던 재료를 **실재료/뉴스로 강화**) |
| occurred_at · freshness_min · next_verification_at | **스케쥴 요소**(실적·이벤트 D-day) + 신선도 가중 |
| candidate_tickers · primary_impact_sectors | **점화 스캐너 명분 필터** (지금 점화 중인 종목 ↔ 진짜 재료 매칭) |
| quality_score · event_state | 명분 강도 가중 (PRIMARY_CONFIRMED 가점) |

→ 단타봇 7요소 중 **재료·스케쥴**이 비어있었음(로컬 데이터 없음). 정보봇 event_signals가 정확히 그 갭을 채움.

## 3. 수정 요청 (Modify)
1. **candidate_tickers = 6자리 종목코드(zfill 6) 표준** — 단타봇 끼/점화 스캐너·SAJANG이 6자리코드 키.
   종목명/심볼이면 매칭 깨짐. (예: "242040" O / "나무기술" X)
2. **장중 critical 즉시 push** — 정보봇 일배치(16:30)는 단타(당일~D+3) 타이밍에 늦음. weight≥8 또는
   freshness 낮은 장중 이벤트는 즉시(정보봇 하이브리드 critical 채널) → event_signals.jsonl append +
   단타봇 폴링. 일배치+즉시 하이브리드.
3. **quality_score 임계 분리(단타용)** — 단타봇은 PRIMARY_CONFIRMED 외 CORROBORATED+quality≥(합의값)도
   "명분 약(modifier)"으로 활용 가능(속도). 단 5중 가드·자체검증 유지. 임계값 합의 요청.

## 4. 위험 지적 (Risk)
1. ★ **event_signals.jsonl 실생성 0 = 단타봇 명분게이트 빈손** — 계약(설계)만 있고 파일 미생성
   (정보봇 Phase C HOLD). 단타봇 4단 명분 활용의 **전제 = 정보봇 실생성.** 우선순위 협의 필요.
2. **종목 매핑 정확도** — causal_path→candidate_tickers 부정확 시 단타봇이 엉뚱한 종목에 명분 부여.
   "왜 이 종목인지" 근거(매핑 사유) 필드 권장.
3. **시각/단위 정합** — 단타봇 SAJANG·KIS는 KST. occurred_at/detected_at = ISO +09:00 확인.

## 5. 단타봇 담당 가드 확약 (5중 중 단타봇 몫)
- 가드 4 (단타봇 자체 selector): ✅ 정보봇 event_signals는 **명분게이트 입력**일 뿐, 매매는 단타봇
  selector(끼·맥점·시나리오) + 손절재진입 + SAJANG 통과 후만.
- 가드 5 (GLOBAL_AUTO_TRADE_DISABLED): ✅ 현재 봇 OFF, 어떤 신호도 실주문 0.

## 6. 진행 (합의 시)
event_signals 실생성(정보봇) + 6자리코드 + 장중 push 합의 → 단타봇 명분게이트가 실재료 소비 시작
→ 4단 flip 근거 강화 + 점화 무기의 재료 필터 완성. 단타봇은 수신 즉시 selector 통합(shadow 먼저).

---
**단타봇 입장: 계약 구조 OK, 즉시 협업 가능. 단 정보봇의 event_signals 실생성이 선결 전제.**
