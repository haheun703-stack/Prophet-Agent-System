# [단타봇 → 웹봇] FLOWX 대시보드 신규 탭 — 실전매매 결과 v1

**발행**: 단타봇 2026-05-17
**수신**: 웹봇 (FLOWX 대시보드)
**우선순위**: 5/20 이후 (검증 모드 데이터 누적 후 의미 있음)

---

## 1. 배경

사장님 요청 (2026-05-17): "실전매매 결과를 마지막 탭에 페이지를 만들어서 그래프화 + 매매 일지 기록하듯이 적용"

단타봇이 데이터 인프라(테이블 + 자동 적재) 완성. 웹봇에서 페이지 구현 요청.

**데이터 시작일**: 2026-05-18 (검증 모드 1주 매수부터)
**자동 갱신**: 봇이 매수/매도 시점에 실시간 INSERT (5분 이내 반영)

---

## 2. 데이터 소스

### 2.1 메인 테이블 `scalper_trade_journal`

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `event_time` | TIMESTAMPTZ | 이벤트 시점 (KST 포함) |
| `event_date` | DATE | 일별 집계용 |
| `event_type` | TEXT | `buy` / `sell_tp` / `sell_sl` / `sell_close` / `partial_sell` / `manual_buy` / `manual_sell` |
| `source` | TEXT | `morning_rec` / `verification` / `nxt_auto` / `predawn` / `manual` / `guardian` / `eye` |
| `stock_ticker` | TEXT | 종목코드 |
| `stock_name` | TEXT | 종목명 |
| `qty` | INT | 수량 |
| `price` | NUMERIC | 체결가 |
| `total_amount` | NUMERIC | qty × price |
| `matched_buy_price` | NUMERIC | 매도 시 평균 매수가 |
| `pnl_pct` | NUMERIC | 매도 시 수익률 % (수수료 차감 전) |
| `pnl_amount` | NUMERIC | 매도 시 수익 금액 |
| `signal_tags` | TEXT | "etf_lead(TOP10/4ETF:+13) dual_buy(+12) pension(S:+15)" |
| `final_score` | NUMERIC | morning_recommendation 점수 |
| `order_no` | TEXT | KIS 주문번호 |
| `note` | TEXT | 특이 사유 |

### 2.2 분석용 View (직접 쿼리 권장)

- `scalper_trade_daily_pnl` — 일별 PnL 집계 (Equity Curve / 일별 막대용)
- `scalper_trade_stock_pnl` — 종목별 누적 PnL (TOP/Bottom 차트용)

### 2.3 보조 테이블 (통합 view 가능)

- `scalper_verification_log` — 검증 모드 별도 기록 (5/18~5/19)
- `scalper_etf_leader_picks` — ETF 주도주 보너스 데이터

---

## 3. 페이지 레이아웃 — "📊 실전매매 결과"

FLOWX 대시보드 **마지막 탭**에 신규 추가. 모바일/데스크탑 반응형.

```
┌──────────────────────────────────────────────────────────────────┐
│  📊 단타봇 실전매매 결과 — 5/18 ~ 오늘 (자동 갱신)                │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ [KPI 5장]                                                     ││
│  │  💰 총 PnL    🎯 승률    📈 평균PnL    🔢 매매수   📉 최대DD  ││
│  └──────────────────────────────────────────────────────────────┘│
│                                                                  │
│  ┌─────────────────────────────┬────────────────────────────────┐│
│  │ ① 누적 수익률 곡선          │ ② 시그널 적중률 도넛           ││
│  │   Equity Curve              │   etf_lead/dual_buy/pension...  ││
│  │   (영역 차트, 가로 길게)     │   (도넛, 정사각형)              ││
│  └─────────────────────────────┴────────────────────────────────┘│
│                                                                  │
│  ┌─────────────────────────────┬────────────────────────────────┐│
│  │ ③ 일별 PnL 막대 + 드로다운  │ ④ TOP/Bottom 종목 양방향 막대  ││
│  │   콤보 (좌축 PnL 막대,       │   상위 10개 + 하위 5개         ││
│  │   우축 드로다운 음영 영역)   │   (다이버징 막대)              ││
│  └─────────────────────────────┴────────────────────────────────┘│
│                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ ⑤ 매매 일지 Timeline (역시간순, 무한 스크롤)                  ││
│  │   [SELL] 2026-05-20 15:25  한미반도체  +2.3%  (etf_lead+13)  ││
│  │   [BUY]  2026-05-20 09:00  한미반도체  1주 @125,000          ││
│  │   ... 페이지당 50건 + load more                              ││
│  └──────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. 각 컴포넌트 SQL + 추천 차트 라이브러리

### 4.1 [KPI 5장] — 상단 요약 카드

```sql
SELECT
    SUM(pnl_amount) FILTER (WHERE event_type LIKE 'sell%') AS total_pnl,
    COUNT(*) FILTER (WHERE event_type = 'buy') AS total_trades,
    AVG(pnl_pct) FILTER (WHERE event_type LIKE 'sell%') AS avg_pnl_pct,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%' AND pnl_pct > 0)::FLOAT
        / NULLIF(COUNT(*) FILTER (WHERE event_type LIKE 'sell%'), 0) * 100 AS win_rate,
    -- 최대 드로다운: 누적 PnL 최고점 대비 최대 하락폭
    (
        SELECT MIN(cum_pnl - running_max) FROM (
            SELECT
                SUM(pnl_amount) OVER (ORDER BY event_time) AS cum_pnl,
                MAX(SUM(pnl_amount) OVER (ORDER BY event_time)) OVER (ORDER BY event_time) AS running_max
            FROM scalper_trade_journal
            WHERE event_type LIKE 'sell%'
        ) t
    ) AS max_drawdown
FROM scalper_trade_journal;
```

**UI**: 카드 5장 (Tailwind/Bootstrap), 색상 코딩:
- 💰 총 PnL: 양수 녹색 / 음수 빨강 / 큰 폰트
- 🎯 승률: 60%+ 녹색 / 40-60% 회색 / <40% 빨강
- 📈 평균 PnL: 동일
- 🔢 매매수: 회색
- 📉 최대 DD: 항상 빨강 (리스크 인지)

### 4.2 ① 누적 수익률 곡선 (Equity Curve)

```sql
WITH daily AS (
    SELECT event_date, SUM(pnl_amount) AS daily_pnl
    FROM scalper_trade_journal
    WHERE event_type LIKE 'sell%'
    GROUP BY event_date
    ORDER BY event_date
)
SELECT
    event_date,
    daily_pnl,
    SUM(daily_pnl) OVER (ORDER BY event_date) AS cum_pnl
FROM daily;
```

**차트 추천**: Recharts `<AreaChart>` 또는 Chart.js `line` (fill: 'origin')
- X축: `event_date` (일자)
- Y축: `cum_pnl` (누적 PnL)
- 0 기준선 빨간 점선
- 양수 영역 녹색 fill / 음수 영역 빨강 fill

### 4.3 ② 시그널 적중률 도넛

```sql
-- 각 매도 이벤트의 signal_tags를 공백으로 split → 태그별 집계
WITH expanded AS (
    SELECT
        UNNEST(STRING_TO_ARRAY(REGEXP_REPLACE(j.signal_tags, '\(.*?\)', '', 'g'), ' ')) AS tag,
        b.pnl_pct,
        b.pnl_amount
    FROM scalper_trade_journal b
    JOIN scalper_trade_journal j
        ON b.stock_ticker = j.stock_ticker
        AND b.event_date = j.event_date
        AND j.event_type = 'buy'
    WHERE b.event_type LIKE 'sell%' AND j.signal_tags IS NOT NULL AND j.signal_tags <> ''
)
SELECT
    tag,
    COUNT(*) AS trades,
    AVG(pnl_pct) AS avg_pnl_pct,
    SUM(pnl_amount) AS total_pnl,
    COUNT(*) FILTER (WHERE pnl_pct > 0)::FLOAT / COUNT(*) * 100 AS win_rate
FROM expanded
WHERE tag <> ''
GROUP BY tag
HAVING COUNT(*) >= 3   -- 최소 3건 이상만 통계적 유의
ORDER BY avg_pnl_pct DESC
LIMIT 10;
```

**차트 추천**: Recharts `<PieChart>` 도넛 또는 horizontal bar
- 색상: avg_pnl_pct 기반 그라데이션 (빨강→노랑→녹색)
- 레이블: `tag (N건, +X.X%)`
- 클릭 시 해당 시그널의 종목 리스트 모달 (5/21+ 확장)

### 4.4 ③ 일별 PnL 막대 + 드로다운

```sql
SELECT * FROM scalper_trade_daily_pnl
ORDER BY event_date;

-- 드로다운 별도 계산
WITH cum AS (
    SELECT
        event_date,
        SUM(daily_pnl_amount) OVER (ORDER BY event_date) AS cum_pnl
    FROM scalper_trade_daily_pnl
)
SELECT
    event_date,
    cum_pnl - MAX(cum_pnl) OVER (ORDER BY event_date) AS drawdown
FROM cum;
```

**차트 추천**: Recharts `<ComposedChart>` (Bar + Area)
- Bar: `daily_pnl_amount` (좌축, 양수 녹색/음수 빨강)
- Area: `drawdown` (우축, 빨강 fill 음영)
- 일자별 hover tooltip: 거래수/승률/PnL

### 4.5 ④ TOP/Bottom 종목 양방향 막대

```sql
(SELECT * FROM scalper_trade_stock_pnl WHERE total_pnl_amount IS NOT NULL ORDER BY total_pnl_amount DESC LIMIT 10)
UNION ALL
(SELECT * FROM scalper_trade_stock_pnl WHERE total_pnl_amount IS NOT NULL ORDER BY total_pnl_amount ASC LIMIT 5);
```

**차트 추천**: Recharts `<BarChart layout="vertical">` 다이버징 막대
- 양수 종목: 녹색, 오른쪽 방향
- 음수 종목: 빨강, 왼쪽 방향
- 레이블: 종목명 + PnL + 매매횟수

### 4.6 ⑤ 매매 일지 Timeline

```sql
SELECT
    id,
    event_time AT TIME ZONE 'Asia/Seoul' AS event_time_kst,
    event_type,
    source,
    stock_ticker,
    stock_name,
    qty,
    price,
    pnl_pct,
    pnl_amount,
    signal_tags,
    note
FROM scalper_trade_journal
ORDER BY event_time DESC
LIMIT 50 OFFSET $1;   -- 무한 스크롤
```

**UI**:
- 카드 리스트 (각 이벤트 = 1 카드)
- 좌측 아이콘: BUY=🟢 / SELL_TP=💰 / SELL_SL=⚠️ / SELL_CLOSE=⏰ / MANUAL=🤲
- 우측 PnL 배지 (매도 시): +X.X% (녹색/빨강)
- 하단: signal_tags chip 형태로 표시
- 무한 스크롤 (`LIMIT 50 OFFSET ?`)
- 필터 옵션: source / event_type / 기간 / 종목

---

## 5. 추가 요청

### 5.1 자동 갱신
- 5분 간격 자동 폴링 (또는 Supabase Realtime 채널 활용)
- 신규 매매 발생 시 우측 상단 토스트 알림: "🎯 새 매매: [종목] [매수/매도] [PnL]"

### 5.2 모바일 반응형
- 320px ~ 1920px 지원
- 모바일에서는 ①②③④ 세로 스택, ⑤ Timeline 풀너비

### 5.3 사장님 권한 (RLS)
- 현재 DB는 anon read 허용 (RLS policy 적용)
- 추가 인증 불필요 (대시보드 진입 시점에 사장님 확인됨)

### 5.4 디자인 시스템
- 기존 FLOWX 색상 팔레트 따르기
- 손익 표시 색상: 양수 `#10b981` (emerald-500) / 음수 `#ef4444` (red-500)
- 중립/회색: `#6b7280` (gray-500)

---

## 6. 개발 우선순위 (제안)

1. **1단계 (5/19~5/20)**: KPI 5장 + ⑤ Timeline만 먼저 (검증 데이터 시각화)
2. **2단계 (5/21+)**: ① Equity Curve + ② 시그널 도넛 (실전 진입 후 의미)
3. **3단계 (5/25+)**: ③ 일별 PnL + 드로다운 + ④ TOP/Bottom (1주일 데이터 누적 후)
4. **4단계 (5/30+)**: 자동 갱신 + 토스트 알림 + 모바일 반응형 미세 조정

---

## 7. 단타봇 측 자산 위치 (참고)

- DDL: `scalper-agent/sql/scalper_trade_journal_migration.sql`
- 적재 헬퍼: `scalper-agent/data/trade_journal.py`
- 자동 호출 위치: `scalper-agent/bot/auto_trader.py` (검증 모드 매수/청산 hook, 5/20+ 일반 매매 hook 추가 예정)

## 8. 변경 이력

| 일자 | 변경 |
|---|---|
| 2026-05-17 | v1 발행 — DDL + 적재 헬퍼 + 검증 모드 hook 완성, FLOWX 측 페이지 구현 대기 |
