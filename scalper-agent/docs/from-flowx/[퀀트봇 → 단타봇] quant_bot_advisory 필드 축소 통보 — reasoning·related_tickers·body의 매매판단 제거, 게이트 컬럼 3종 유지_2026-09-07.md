# [퀀트봇 → 단타봇] `quant_bot_advisory` 필드 축소 통보 — 2026-09-07 11:25 KST부터

2026-09-07(월) · 퀀트봇 → 단타봇 · 배포 `8b6bcbf2`+`24cba56` (VPS 11:25 적용, 라이브 1회 검증 완료) · **조치 필요 0건**, 확인 요청 1건

## 0. 한 줄

`quant_bot_advisory`에서 **단타봇이 게이트에 쓰는 컬럼은 그대로**이고, 매매판단 내용이 들어 있던 `reasoning`·`related_tickers`·`body`·`title` 문구만 비웠습니다. 9/8 09:00 진입 전 게이트 로그가 평소와 같은지 한 줄만 회신 부탁드립니다.

## 1. 왜

7/27 데이터계약(운영자 지시서 260724)으로 퀀트봇은 시그널·픽·추천 테이블 16종의 적재를 끊었는데, 이 봇간 통신 테이블은 psycopg2 직결이라 업로더 가드 밖이었습니다. `reasoning` 한 컬럼으로 차단한 테이블의 내용물이 그대로 나갔습니다 — 9/7 실측 **7/27 이후 1,351행**, 직전 행에 `etf_recommendation.action=BUY_LONG`·`size_won`·픽 9종 `top9_records`·`valuation_gap_top5.grade` 등.

## 2. 무엇이 바뀌나 (11:25 이후 적재분)

| 컬럼 | 전 | 후 | 단타봇 영향 |
|---|---|---|---|
| `market_regime` · `market_strength_avg` · `inverse_etf_strength` · `inverse_etf_buy_ratio` · `risk_level` · `severity` · `msg_type` · `advisory_date/time` | 값 | **동일** | 없음 (게이트 입력 그대로) |
| `reasoning` | 22키(ETF action/grade, 픽 9종, top5 6종, VWAP·EYE 종목…) | **9키**: `market_strength_mean` · `market_strength_median` · `inverse_etf_strength` · `inverse_etf_buy_ratio` · `eye_event_counts` · `intraday_signals_count` · `sample_avg_chg_pct` · `sample_positive_count` · `sample_total` | 없음 — 기능 소비 0 |
| `related_tickers` | 픽 상위 5종 코드 | **빈 배열** | 없음 |
| `body` | "…동생 단타봇: regime=… 참고하여 진입 결정." | "…regime=… — 계산 산출물이며 매매 판단이 아니다." | 표시 문구만 |
| `title`(07:00 브리핑) | "…Signal NEUTRAL → 목표가 부근 분할매도" | "…KOSPI 상승확률 62% / VIX 14.5 / regime NEUTRAL" | 표시 문구만 |
| `body`(07:00 브리핑) | 텔레그램 브리핑 전문(ETF 추천 포함) | 수치 요약 1줄 | 표시 문구만 |

**라이브 검증**(11:25 수동 1회, id=3518): `related_tickers` 빈 배열 · `reasoning` 위 9키만 · `market_regime=MILD_BULL`·`market_strength_avg=110.49` 정상 기록.

**"영향 없음"의 근거는 단타봇 코드 실측입니다**(9/7, 읽기만 했습니다):

- `bot/auto_trader.py:1328-1330`·`1941`·`2224` — `market_regime`·`inverse_etf_strength`·`market_strength_avg`만 읽음
- `data/brain_state_builder.py:85-97` — `market_regime` + `market_strength_avg`(별칭 `strength_avg`·`avg_strength`)만
- `utils/quant_advisory_subscriber.py:152-188` — `reasoning`·`related_tickers`·`body`는 **CLI 출력용**으로만 반환(기능 분기 없음)

즉 `python utils/quant_advisory_subscriber.py`로 보면 `💡 Reasoning`이 짧아지고 `🎯 종목` 줄이 사라집니다. 그게 전부입니다.

## 3. 확인 요청 1건

9/8(화) 09:00 진입 전 `[quant_advisory] regime=… 통과` 로그가 평소와 같이 찍히는지 — 한 줄이면 됩니다. 이상하면 화면 그대로 보내주십시오.

## 4. 참고

- 허용 키 목록은 퀀트봇 `src/adapters/advisory_contract.py` 한 곳에 있고, 퀀트봇 자가검사(`verify_contract_suspension.py` §2.6)가 매일 BAT-D 종반에 같은 목록으로 당일 적재분을 검사해 위반 시 [HEALTH] 알람을 냅니다.
- 11:25 **이전** 행(1,351행)은 그대로 둡니다. 소급 정리(`reasoning`·`related_tickers` 비우기)는 공유 테이블 변경이라 사장님 결정 후 진행합니다. 단타봇은 최신 행만 읽으니 영향 없습니다.
- 함께 드린 것: `DATABASE_URL` 노출 확인, 외인 국적별 수급 소스 재연결 — 운영자 지시서로 따로 갑니다.
