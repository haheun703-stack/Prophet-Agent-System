# [단타봇 → 웹봇] 퀀트 대시보드 7개 테이블

> 작성일: 2026-03-28 | 최종갱신: 2026-05-13 | 커밋: d54934b
> 빈페이지 방지: upsert 실패 시 전일 데이터 유지
> v2: 테마별 수급(#6) + 테마별 모멘텀(#7) 추가 — KIS 302개 테마 기반

---

## [단타봇 → 웹봇] 섹터 수급 흐름

1. 테이블명: `quant_sector_flow`
2. 컬럼 구조:
   - `date` DATE PK
   - `sectors` JSONB — 배열: {섹터, 기관_당일, 기관_3일, 기관_5일, 기관_연속일, 외인_당일, 외인_3일, 외인_5일, 외인_연속일, 판단, 설명, 보정점수}
   - `top_inflow` JSONB — 매수 집중 TOP3 섹터명
   - `top_outflow` JSONB — 이탈 TOP3 섹터명
   - `signal` TEXT — 한줄 요약
   - `total_sectors` INT
   - `created_at` TIMESTAMPTZ
3. SQL 파일: `sql/quant_sector_flow_migration.sql`
4. 스케줄: 16:45
5. 데이터 설명: 23개 섹터 기관·외국인 순매수 + 연속일 + 합의매수 판단

---

## [단타봇 → 웹봇] ETF 투자자 수급

1. 테이블명: `quant_etf_fund_flow`
2. 컬럼 구조:
   - `date` DATE PK
   - `etfs` JSONB — 배열: {종목코드, ETF명, 설명, 분류, 기관_당일, 기관_3일, 기관_연속일, 외인_당일, 외인_3일, 외인_연속일, 개인_당일, 시그널, 시그널_설명, 강도, 등락률}
   - `market_direction` TEXT — BULLISH/BEARISH/NEUTRAL
   - `market_direction_desc` TEXT — 주린이용 방향 설명
   - `inverse_warning` BOOLEAN — 인버스 기관매수 경고
   - `hot_sector_etfs` JSONB — 기관 집중 섹터 ETF명
   - `safe_haven_signal` TEXT — RISK_ON/RISK_OFF/NEUTRAL
   - `safe_haven_desc` TEXT — 안전자산 설명
   - `brain_defense_score` REAL — -10~+10
   - `created_at` TIMESTAMPTZ
3. SQL 파일: `sql/quant_etf_fund_flow_migration.sql`
4. 스케줄: 16:45
5. 데이터 설명: 21개 ETF 기관·외인·개인 수급 + 시장방향 + 인버스경고 + 안전자산

---

## [단타봇 → 웹봇] 섹터 모멘텀

1. 테이블명: `quant_sector_momentum`
2. 컬럼 구조:
   - `date` DATE PK
   - `market_return_1d` REAL — 시장 전체 당일 수익률(%)
   - `sectors` JSONB — 배열: {섹터, 상태(HOT/WARMING/NEUTRAL/COOLING/COLD), 순위, 당일수익률, 3일수익률, 5일수익률, 상승비율, 가속도, 거래량폭증, 부스트, 주도주}
   - `hot_sectors` JSONB — HOT 섹터명 목록
   - `cold_sectors` JSONB — COLD 섹터명 목록
   - `rotation_signal` TEXT — 로테이션 요약
   - `created_at` TIMESTAMPTZ
3. SQL 파일: `sql/quant_sector_momentum_migration.sql`
4. 스케줄: 16:45
5. 데이터 설명: 23개 섹터 HOT/COLD 상태 + 수익률 + 가속도 + 주도주

---

## [단타봇 → 웹봇] ETF 추천

1. 테이블명: `quant_etf_recommendation`
2. 컬럼 구조:
   - `date` DATE PK
   - `picks` JSONB — 배열: {종목코드, ETF명, 분류(시장방향/원자재/섹터테마), 신호(매수/매도/보유), 신뢰도(높음/보통/낮음), 점수, 진입가, 손절가, 목표가, 손실률, 사유, 보유일}
   - `pick_count` INT — 추천 수
   - `has_directional` BOOLEAN
   - `has_commodity` BOOLEAN
   - `has_sector` BOOLEAN
   - `created_at` TIMESTAMPTZ
3. SQL 파일: `sql/quant_etf_recommendation_migration.sql`
4. 스케줄: 16:45
5. 데이터 설명: 오늘의 ETF 추천 최대 3개 (시장방향/원자재/섹터 각 1개) + 진입/SL/TP

---

## [단타봇 → 웹봇] BRAIN 시장판단

1. 테이블명: `quant_market_brain`
2. 컬럼 구조:
   - `date` DATE PK
   - `overall_verdict` TEXT — 오늘의 종합 판단 (한글 서술)
   - `position_size_pct` INT — 투자비중 권장 (0~100%)
   - `position_size_reason` TEXT — 비중 이유
   - `macro_direction` TEXT — STRONG_BULL~STRONG_BEAR
   - `macro_narrative` TEXT — 매크로 설명
   - `vix` REAL, `nasdaq_chg` REAL, `usdkrw` REAL, `usdkrw_chg` REAL, `gold_chg` REAL
   - `commodity_relay` TEXT — 원자재 릴레이 단계
   - `commodity_narrative` TEXT
   - `hot_sectors` JSONB, `next_sectors` JSONB, `cooling_sectors` JSONB
   - `sector_narrative` TEXT
   - `dominant_buyer` TEXT — 주도 매수 주체
   - `flow_narrative` TEXT
   - `risk_level` TEXT — LOW/MEDIUM/HIGH/EXTREME
   - `risk_score` REAL, `risk_narrative` TEXT
   - `stock_narratives` JSONB — 배열: {종목코드, 종목명, 점수, 등급, 분석, 리스크, 매크로연동}
   - `created_at` TIMESTAMPTZ
3. SQL 파일: `sql/quant_market_brain_migration.sql`
4. 스케줄: 16:45
5. 데이터 설명: AI 6단계 시장분석 (매크로/원자재/섹터/수급/리스크/종합) + 투자비중 + 종목 서술

---

## [단타봇 → 웹봇] 테마별 수급 (v2 신규)

1. 테이블명: `quant_theme_flow`
2. 컬럼 구조:
   - `date` DATE PK
   - `themes` JSONB — 배열: {테마, 종목수, 테마전체, 기관_당일, 기관_3일, 기관_5일, 기관_연속일, 외인_당일, 외인_3일, 외인_5일, 외인_연속일, 판단, 설명, 보정점수}
   - `top_inflow` JSONB — 자금 유입 TOP5 테마명
   - `top_outflow` JSONB — 자금 이탈 TOP5 테마명
   - `signal` TEXT — 한줄 요약 (예: "세트 자금 유입: 전기차충전소, 스마트폰, MLCC")
   - `total_themes` INT — 분석 대상 테마 수
   - `created_at` TIMESTAMPTZ
3. 스케줄: 16:45
4. 데이터 설명: KIS 302개 테마 기준 기관·외국인 순매수 집계.
   기존 quant_sector_flow(23개 KRX 섹터)의 세분화 버전.
   "전기전자"가 아닌 "MLCC", "2차전지", "HBM" 등 실제 세트매매 단위로 분석.
   한 종목이 여러 테마에 속하므로 교차 집계됨.
5. SQL 파일: `sql/quant_theme_flow_migration.sql`
6. 소스 JSON: `scalper-agent/data_store/theme_flow.json`
7. 생성 함수: `sector_institution_flow.py → analyze_theme_flow()`

---

## [단타봇 → 웹봇] 테마별 모멘텀 (v2 신규)

1. 테이블명: `quant_theme_momentum`
2. 컬럼 구조:
   - `date` DATE PK
   - `market_return_1d` REAL — 시장 전체 당일 수익률(%)
   - `themes` JSONB — 배열: {테마, 상태(HOT/WARMING/NEUTRAL/COOLING/COLD), 순위, 종목수, 당일수익률, 3일수익률, 5일수익률, 상승비율, 가속도, 부스트, 주도주}
   - `hot_themes` JSONB — HOT/WARMING 테마명 목록
   - `cold_themes` JSONB — COLD/COOLING 테마명 목록
   - `rotation_signal` TEXT — 테마 로테이션 요약
   - `total_themes` INT — 분석 대상 테마 수
   - `created_at` TIMESTAMPTZ
3. 스케줄: 16:45
4. 데이터 설명: KIS 302개 테마 기준 HOT/COLD 모멘텀.
   기존 quant_sector_momentum(23개 KRX 섹터)의 세분화 버전.
   Naver Finance API로 전체 종목 등락률을 조회한 뒤 테마별 평균 수익률·브레드쓰·가속도 계산.
5. SQL 파일: `sql/quant_theme_momentum_migration.sql`
6. 소스 JSON: `scalper-agent/data_store/theme_momentum.json`
7. 생성 함수: `sector_momentum.py → analyze_theme_momentum()`

---

## 기존 섹터(23개) vs 신규 테마(302개) 관계

| 구분 | 기존 (섹터) | 신규 (테마) |
|------|------------|------------|
| 분류 기준 | KRX 23개 업종 | KIS 302개 테마코드 |
| 예시 | "전기전자" = 반도체+2차전지+디스플레이 혼합 | "MLCC", "2차전지(생산)", "HBM" 각각 분리 |
| 테이블 | quant_sector_flow, quant_sector_momentum | quant_theme_flow, quant_theme_momentum |
| 용도 | 매크로 시야 (큰 그림) | 세트매매 단위 (실제 트레이딩) |
| 공존 | 유지 — 매크로 관점 필요 | 추가 — 세분화된 테마 관점 |

> **웹봇 참고**: 섹터 패널과 테마 패널을 탭으로 구분하거나, 테마를 메인으로 노출하고 섹터는 서브로 배치 권장.

---

## Supabase 테이블 생성 SQL

```sql
-- 6. 테마별 수급
CREATE TABLE IF NOT EXISTS quant_theme_flow (
  date       DATE PRIMARY KEY,
  themes     JSONB NOT NULL DEFAULT '[]',
  top_inflow JSONB NOT NULL DEFAULT '[]',
  top_outflow JSONB NOT NULL DEFAULT '[]',
  signal     TEXT NOT NULL DEFAULT '',
  total_themes INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 7. 테마별 모멘텀
CREATE TABLE IF NOT EXISTS quant_theme_momentum (
  date            DATE PRIMARY KEY,
  market_return_1d REAL NOT NULL DEFAULT 0,
  themes          JSONB NOT NULL DEFAULT '[]',
  hot_themes      JSONB NOT NULL DEFAULT '[]',
  cold_themes     JSONB NOT NULL DEFAULT '[]',
  rotation_signal TEXT NOT NULL DEFAULT '',
  total_themes    INT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
