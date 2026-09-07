# [운영자 → 단타봇] advisory 필드 축소 확인 · `nationality_flows` 재탕 정지 · `DATABASE_URL` 노출 확인 · 외인 수급 소스 재연결 지시서

- 작성일: 2026-09-07
- 배경 근거: 퀀트봇 9/7 조치(`8b6bcbf2`) · 정보봇 9/7 10:2x 실측 통보 · 퀀트봇 9/7 소유 확인 회신 · 퀀트봇 HEALTH 90일 STALE 항목
- 첨부: `[퀀트봇 → 단타봇] quant_bot_advisory 필드 축소 통보_2026-09-07.md` · `[퀀트봇 → 정보봇·단타봇] nationality_flows 소유 확인 회신_2026-09-07.md`

---

## 1. 확인 — `quant_bot_advisory` 필드 축소 (조치 없음, 확인만)

퀀트봇이 9/7 11:25부터 `quant_bot_advisory`의 `reasoning`·`related_tickers`·`body`에서 매매판단 내용을 뺐다(데이터계약 260724 §1). 단타봇 게이트가 읽는 `market_regime`·`market_strength_avg`·`inverse_etf_strength`는 그대로다 — 퀀트봇이 단타봇 코드(`auto_trader.py:1328-1330`·`brain_state_builder.py:85-97`·`quant_advisory_subscriber.py:152-188`)를 읽고 확인했고, 11:25 라이브 1회로 검증했다.

**지시**: 9/8(화) 09:00 진입 전 게이트 로그(`[quant_advisory] regime=… 통과`)가 평소와 같은지 확인하고 한 줄 회신하라. 다르면 로그 화면 그대로 첨부.

---

## 2. ★지시 — `nationality_flows` 재탕 정지 (2개월 반 진행 중)

정보봇 9/7 실측: `nationality_flows` 표에 **매일 1,911행**(8/28~9/6 열흘, 주말 포함)이 들어오는데 **내용 날짜(`countries.date_new`)는 6/19에 멈춰 있다.** 누적 273,698행으로 Supabase 최대 표다. 정보봇 참조 0건, 웹봇 참조 0건.

소유자는 **단타봇**으로 확정됐다(퀀트봇 9/7 코드 실측):

```
data/upload_short.py:3        담당 테이블: short_signals, nationality_flows
bot/trading_coo.py:2453       C25 「국적 X-ray」 → upload_nationality_flows()
bot/trading_coo.py:4078-4081  AUTO-RECOVERY "nationality_xray" (마커 data_store/nationality/_last_upload.json)
bot/trading_coo.py:4299-4306  _recover_nationality_flows() — 마커가 낡으면 재업로드
```

즉 6/22 KRX 접근 차단 이후 **6/19자 옛 스냅샷이 매일 오늘 날짜로 다시 적재**되고 있다. 잡은 성공하고 행수도 일정해 로그로는 보이지 않는다.

**지시**:
1. 소유 여부를 코드로 확인해 회신하라(맞다/아니다).
2. 맞으면 **업로드 지점에 신선도 가드**를 넣어라 — 원천 CSV의 내용 날짜가 기준일 대비 N일(권고 4일)을 넘으면 **업로드를 스킵**하고 그 사실을 로그로 남긴다. 낡은 값을 오늘 날짜로 내보내지 않는다. 퀀트봇이 8/11 같은 사고(B-61: 63일 묵은 시그널이 픽의 55%를 좌우) 후 `flowx_uploader.py:962-972`에 넣은 가드가 참고 예시다.
3. AUTO-RECOVERY(`_recover_nationality_flows`)도 같은 가드를 통과하도록 하라 — **복구 경로가 가드를 우회하면 정지가 무의미하다.**
4. 소급 정리(273,698행 중 재탕분 삭제) 여부는 별도 판단이다. **먼저 흐름을 멈추고** 규모를 보고하라.

★ 이 지시는 **소스 복구(§4)와 별개**다. 재탕을 멈추는 것이 먼저다 — 낡은 값이 흐르는 동안에는 복구가 됐는지조차 확인되지 않는다.

---

## 3. 지시 — `DATABASE_URL` 노출 여부 (8/21 퀀트봇 문의, 미회신)

5/17 정보봇 가이드 v1로 셋(단타봇·퀀트봇·웹봇)이 같은 `DATABASE_URL`(psycopg2 직결 문자열)을 공유한다. 8/9 Supabase 키 노출 사건 이후 웹봇은 8/29에 자기 저장소 조사 결과를 회신했고 **단타봇·정보봇만 남았다.**

**지시**: 단타봇 저장소(`scalper-agent`)와 배포 환경에서 `DATABASE_URL` 값이 커밋·공개 이력·로그·문서에 노출된 적이 있는지 확인해 회신하라. `git log -p -S` 수준으로 이력까지 본다. **"없음"도 완전한 답이다.** 있으면 어디·언제인지 적는다 — 있으면 DB 비밀번호 회전을 넷이 동시에 잡아야 한다.

---

## 4. 지시 — 외인 국적별 수급 소스 재연결 (퀀트봇 90일 결손)

퀀트봇 `data/krx_nationality/nationality_signal.json`이 **6/9 이후 갱신되지 않아**(HEALTH `⏸️ 국적별수급 STALE`, 9/4 기준 90일) FLOWX `foreign_flow` 업로드가 신선도 가드로 매일 스킵되고, `scan_tomorrow_picks` 전략 L·`build_killer_picks`가 비어 있다. 3봇 분업상 외인 수급은 단타봇 담당이다.

**지시**:
1. 단타봇이 **종목 단위** 국적별 외인 수급을 제공할 수 있는지(소스·주기·시작 가능일) 답하라. §2에서 보듯 현재 단타봇 원천도 6/19에 멈춰 있으므로, **먼저 그 소스가 살아 있는지부터** 답하라. 불가하면 불가라고 답하라. ★**9/7 정정**: 대체 후보로 검토했던 정보봇 `supply_daily/*_nationality_flow.json`은 **쓸 수 없다** — 정보봇 회신(9/7 12:0x)에 따르면 필드명이 `nation`일 뿐 값은 국적이 아니라 **섹터**다(`외국인전체(반도체)` 형태). 즉 단타봇이 불가하면 **현재 대체 소스가 없다**. 그 경우 국적별 수급을 쓰는 퀀트봇 경로(`scan_tomorrow_picks` 전략 L·`build_killer_picks`·FLOWX `foreign_flow`)를 **정식 폐기할지** 별도 판단이 필요하다.
2. 가능하면 산출 경로·스키마 제안을 첨부하라. 퀀트봇 소비처는 `scan_tomorrow_picks`(전략 L)·`build_killer_picks`·FLOWX `foreign_flow`다.

---

## 5. 회신

- §1: 9/8 오전 · §2: 확인은 즉시, 가드는 금주 내 · §3·§4: 금주 내
- 파일명 관례: `[단타봇→운영자] advisory확인_nationality정지_DATABASE_URL_수급소스_회신_2609XX.md`

— 이상.
