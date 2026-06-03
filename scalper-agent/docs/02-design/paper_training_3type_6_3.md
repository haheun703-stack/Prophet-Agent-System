# 3-Type 병행 Paper Training + EVENT_SIGNAL 명분 확장 — 설계서 (6/3, read-only)

> 사장님 지시(6/3): 6/4~6/12 3개 타입(A 명분추세 / B 로테이션 눌림 / C 로테이션 올라타기) **병행 paper training**.
> 목적 = "최종 룰 확정"이 아니라 **"어느 타입이 살아있는지 1차 판정"**. 돈은 섹터/그룹 안에서 돌고, 우리가 먹는지는 진입위치·비중·청산에 달림.
> ★ 전제(불변): 실주문 0 / 봇 OFF / SAJANG 무변경 / scheduler 자동매매 금지 / paper·shadow ledger만 / trading_calendar fix 완료(commit 5ba7c29) 후 시작.
> ★★ 룰 확정 금지 — 진입 파라미터는 A처럼 **넓게 병행 기록**, forward로 판정. "검증 없이 손대지 말것"(사장님 영구룰).

## 0. 기간·운영
- **2026-06-04(목) ~ 2026-06-12(금)**, 매일 장마감 기준 기록. 가능하면 장중 후보 별도 로그.
- 6/6(토·현충일) 휴장. 실거래일 = 6/4·6/5·6/8·6/9·6/10·6/11·6/12 (7거래일).
- 봇 OFF 유지 → 매일 **수동 실행**(sync → 3타입 스캔). scheduler 배선 금지(승인 전).

## 1. 3개 타입

### A. STEADY_S_DART_RIDE → (확장후보) STEADY_EVENT_RIDE
- 완만 신고가 돌파 + **S급 DART 명분**(매출대비≥10% 공급계약 / 자사주≥500억). 대형/중형.
- **이미 구현**: `tools/sdart_shadow_record_6_3.py` (넓게 병행 기록). primary 명분 = DART_S.
- 목적: "명분 있는 추세 라이드"가 실제로 돈 되는지.

### B. ROTATION_PULLBACK_BUY (신규)
- **강한 섹터/그룹 안에서 조정일 매수**. 대형/소부장.
- 진입(넓게 기록): 주도 섹터/그룹 내 **눌림 -3·-5·-7% 셋 다** / MA10·MA20 지지 / 거래대금 유지.
- 청산(넓게): 분할익절 · ma10 회복 · 전고점 접근.
- 토대: `sector_relay.scan_all_sectors()` momentum_5d·breadth로 "강한 그룹" 판정 → 그 안에서 눌림 종목.

### C. ROTATION_RIDE_BUY (신규)
- **강한 섹터/그룹 안에서 올라타기**. 대형/소부장.
- 진입(넓게): **+5·+7% 돌파 / 강한 양봉+거래대금 증가 / 전고점·신고가 돌파** 다 기록.
- 청산(넓게): 트레일링 · ma5 · ma10 · 목표수익.
- 목적: 강한 놈에 올라타는 방식 검증.

## 2. EVENT_SIGNAL 명분 레이어 (DART 단독 → 통합 확장)
> 사장님 통찰: 주식은 DART만으로 안 움직인다. DART=확정성 명분(백테가능·느림), 뉴스=속도 명분(빠름·노이즈). 둘 다 봐야.
> 단 **룰 확정 금지**: 검증된 DART_S만 primary, 나머지는 forward 기록(보조 명분)만.

| 명분 | 상태 | 소스(조사결과) | 사용 |
|---|---|---|---|
| **DART_S** | ✅ 검증(45건 median+3.21·승률67) | event_detector.py · sdart 도구 | **primary** |
| NEWS_HOT | ⚠️ 미검증·백필불가 | news_collector(네이버+Grok) · event_detector(10대테마+수혜주) | forward 기록 |
| REPORT_UP | ❌ 소스 없음 | (리포트/컨센서스 수집 코드 부재) | 소스 확보 후 |
| POLICY_EVENT | ⚠️ 정적 | event_calendar.py(중앙은행) · 10대 매크로테마 | forward 기록 |
| SUPPLY_CONFIRM | ✅ 있음 | flow_collector · sector_institution_flow · 11주체 | forward 기록 |
- 6/12까지 비교: **DART 단독 vs DART+NEWS vs NEWS 단독** (명분 소스별 수익 기여도).
- ★ 정직: 뉴스 과거 백필 불가(실시간만) → NEWS는 6/4부터 forward만. 리포트/정책은 소스 없음(있는 척 X).

## 3. 인프라 재사용 (조사 6/3 — Explore)
- **B/C 로테이션**: `data/rotation_detector.py` · `agent류 sector_relay.scan_all_sectors(as_of)` · `group_relay.py`. ★일봉만으로 계산 + 과거백필(as_of) 가능 = 임의 파라미터 없이 데이터 기반★.
- **수급**: `flow_collector.py` · `sector_institution_flow.py` · 11주체 collector(외인/기관/연기금/금융투자).
- **명분**: `event_detector.py`(DART+10대테마+수혜주) · `news_collector.py`.
- **ledger**: `engine/paper_portfolio.py`(PaperPortfolio: 가상체결·손익·source 구분) 재사용.
- ★ 구현 전 **sector_relay·paper_portfolio 정독 필수**(Explore 요약 검증 — 외부지식 검증 룰).

## 4. 공통 ledger 스키마 (사장님 17필드)
`data_store/paper/paper_{type}_{date}.json` 또는 PaperPortfolio source=A/B/C:
```
date, type(A/B/C), ticker, name, sector, group,
entry_reason, signal_source(DART_S/NEWS_HOT/POLICY/SUPPLY/ROTATION),
virtual_entry_price, virtual_exit_price, unrealized_pnl, realized_pnl,
max_favorable_excursion(MFE), max_adverse_excursion(MAE), holding_days,
supply(foreign/institution/pension/financial 가능시), market_regime,
sector_rotation_score, group_rotation_score, capital_allocated, position_size_pct
```
- 후보 0건도 정상 데이터로 기록(빈 ledger). 하루 후보 많으면 **점수순 상위 3~5개만**.

## 5. 자본 배분 (가정)
- 총 paper seed **100**. A 30 / B 35 / C 35. 또는 동일비중 1종목당 10~20.
- 가상 체결가·손익 기록(실주문 0). position_size_pct·capital_allocated 기록.

## 6. 판정 기준 (6/12 — 1차 판정, 최종확정 아님)
- 타입별: 누적수익률 · 승률 · 평균수익/평균손실 · MDD · MAE · MFE · 보유일수 · false breakout 비율 · 놓친 상승 · 손절 후 재상승 여부 · **타입별 수익 기여도**.
- 명분별(A/EVENT): DART_S vs +NEWS vs NEWS 단독 기여도.
- ★ 결론 = "어느 타입/명분이 살아있나" 1차 판정. 룰 확정·자동매매 배선은 그 다음(사장님 승인+추가검증).

## 7. 구현 순서 (단계적 — 하루에 다 X)
1. ✅ A(sdart) 기존 — ledger 연결.
2. 공통 paper ledger 모듈(PaperPortfolio source 분리 or shadow JSON). py_compile·게이트8/8·매도무손상.
3. B(ROTATION_PULLBACK): sector_relay 강한그룹 → 눌림 넓게 기록. 4-Tier.
4. C(ROTATION_RIDE): sector_relay 강한그룹 → 돌파 넓게 기록. 4-Tier.
5. EVENT_SIGNAL hook: DART_S primary + NEWS/SUPPLY/POLICY 기록 배선(event_detector 재사용).
6. 매일 수동: sync(stock_data_daily 최신) → 3타입 스캔 → ledger 누적.
- 각 단계 게이트8/8 회귀0·주문0·SAJANG무변경·scheduler무접촉.

## 8. 확인포인트 (사장님 결정)
1. **진입 파라미터**: B/C도 A처럼 '넓게 병행 기록'(권장) vs 단일 확정.
2. **ledger 방식**: PaperPortfolio 재사용(가상체결·손익) vs shadow JSON(기록만, sdart방식).
3. **"강한 섹터/그룹" 임계** (6/3 검증·제안): sector_relay 기본 HOT=`5D≥3 & vol≥1.5 & breadth≥0.6`는 6/2(약세장) 12섹터 중 **1개(IT +15%)만** 잡힘 = B/C 후보 너무 좁음. ★제안: 임계 재발명 X, **status(HOT+WARMING+RELAY) 다 기록(넓게)** → forward로 어느 status가 돈 되는지 판정★(A의 grade 일관). 단 하루(6/2) 데이터 대표성 없음 → 7거래일 누적 후 6/12 적정성 판정. 유니버스 제한(12섹터 ~70종목)은 "섹터 전체 확장" 여부 6/4 구현 시 결정.
4. **자본 배분**: A30/B35/C35 vs 동일비중. 하루 상위 3 vs 5.
5. **EVENT 비교 범위**: NEWS_HOT 6/4부터 forward만(백필불가) 인정?

## 9. 정직 한계
- B/C 신규 = 백테 근거 없음(A는 45건 있었음) → forward 자체가 1차 검증. 절대수익 신뢰 X.
- 뉴스 백필 불가 → NEWS 명분은 짧은 forward(7거래일)만 = thin. 리포트/정책 소스 없음.
- sector_relay 생존편향(상폐부재)·일봉 휩쏘 과소. 7거래일 = 매우 thin → "1차 판정"이지 확정 아님.
- 6/2→6/4 거래일 분절(선거 휴장)을 보유일수 계산이 거래일 기준으로 처리하는지 점검 필요.
