# FLOWX 스윙시스템 — 자동 업데이트 스케줄

## 페이지별 데이터 소스 + 스케줄

| 페이지 | Supabase 테이블 | 컬럼/데이터 | COO 챕터 | 실행 시각 | 비고 |
|--------|----------------|-------------|----------|----------|------|
| 대시보드 | `dashboard_swing` | 전체 row | C19 | G7 Stage 3 (~16:45) | 메인 스윙 대시보드 |
| 대형주 피보나치 | `dashboard_swing` | `fib_leaders` (JSONB) | C19 | G7 Stage 3 (~16:45) | 시총 상위 30종목 |
| 전체 피보나치 | `dashboard_swing` | `fib_stocks` (JSONB) | C19 | G7 Stage 3 (~16:45) | 전체 50종목 |
| 섹터 로테이션 | `dashboard_swing` | `sector_rotation` (JSONB) | C19 | G7 Stage 3 (~16:45) | 섹터별 점수+단계 |

## COO 파이프라인 흐름

```
G6 DATA_PIPELINE (16:00~)
  ├─ C3  ★ 일봉 수집 (daily/*.csv)
  ├─ C5Z   수급 Z-score 사전계산
  └─ C7  ★ 데이터 검증

G7 EVENING_BRAIN (16:30~)
  ├─ Stage 1: C8(시그널) + C10(스윙선정) + C11(브레인배분)
  ├─ Stage 2: C12(일간학습) → C13 ★ 이브닝분석 (추천 파이프라인)
  └─ Stage 3 (병렬):
       ├─ C14  클로징 브리프
       ├─ C15  선취매
       ├─ C16  MACD 스캔
       ├─ C17  국적 차트 업로드
       ├─ C18  파이프라인 헬스
       ├─ C19 ★ FLOWX 스윙 업로드    ← 여기서 4개 페이지 모두 업데이트
       ├─ C20  섹터 기관 수급
       ├─ C21  ETF 수급
       ├─ C23  TRIX 사전스캔
       ├─ C25  국적 X-ray 업로드
       ├─ C26  NXT Paper 등록
       └─ C27  Paper 일일 성적표
```

## C19 내부 동작 (`run_flowx_swing_upload`)

```
1) refresh_fib_prices()
   └─ bottom_scan.json + fib_leaders.json 종가를 flow CSV에서 갱신

2) generate_swing_page_data()
   └─ brain_report + 스윙 추천 + 포트폴리오 생성

3) upload_swing_to_supabase()
   └─ swing_signals 테이블 upsert

4) upload_dashboard_swing()
   ├─ NXT 데이터 로드 (nightwatch_report.json)
   ├─ Brain Allocation 로드 (brain_allocation.json)
   ├─ _build_fib_stocks()      ← 전체 피보나치 50종목
   ├─ _build_fib_leaders()     ← 대형주 피보나치 30종목
   ├─ _build_fx_monitor()      ← 환율/달러 모니터
   ├─ _build_sector_rotation() ← 섹터 로테이션 맵
   ├─ _build_nxt_rationale()   ← 채권자경단 NXT 근거
   └─ dashboard_swing 테이블 upsert (date 기준)
```

## 피보나치 데이터 갱신 경로

```
[원본] bottom_scan.json (시총1조+ 전체 종목)
       fib_leaders.json (시총 상위 30)
       ↓
[C19] refresh_fib_prices()
       flow/{code}_investor.csv 종가 읽어서 price/drop/upside 갱신
       ↓
[C19] _build_fib_stocks() → 50종목 JSONB
       _build_fib_leaders() → 30종목 JSONB
       ↓
[C19] dashboard_swing.fib_stocks / fib_leaders 업로드
```

## 섹터 로테이션 데이터 갱신 경로

```
[원본] bottom_scan.json + fib_leaders.json
       flow/{code}_investor.csv (수급 3일)
       ↓
[C19] _build_sector_rotation()
       섹터별 종합점수 = 모멘텀(등락률) + 수급(기관+외인) + 쌍매수 보너스
       로테이션 단계: 선도/추격/대기/후발
       ↓
[C19] dashboard_swing.sector_rotation 업로드
```

## 업데이트 확인 체크리스트

매일 장 마감 후 (17:00 이후) 다음 확인:

- [ ] `dashboard_swing` 오늘 날짜 row 존재
- [ ] `fib_stocks` 50종목 (price/drop이 오늘 종가 반영)
- [ ] `fib_leaders` 30종목 (price/drop이 오늘 종가 반영)
- [ ] `sector_rotation` 섹터별 점수+단계 존재

## 장애 대응

| 증상 | 원인 | 대응 |
|------|------|------|
| 오늘 row 없음 | C19 실패 또는 G7 미실행 | VPS 로그 확인 → 수동 실행 |
| fib_stocks 비어있음 | bottom_scan.json 없음 | bottom_scan 재생성 필요 |
| fib_leaders 비어있음 | fib_leaders.json 없음 | fib_leaders 재생성 필요 |
| 종가 미갱신 | flow CSV 미수집 | C3 일봉수집 확인 |
| sector_rotation 비어있음 | bottom_scan+fib_leaders 둘 다 없음 | 원본 데이터 확인 |

## 원본 데이터 수동 갱신

```bash
# bottom_scan.json 재생성 (시총1조+ 피보나치 분석)
cd ~/bodyhunter/scalper-agent
python3 -c "from data.fibonacci_analyzer import scan_universe; scan_universe()"

# fib_leaders.json 재생성 (시총 상위 30)
python3 -c "from data.fibonacci_analyzer import scan_leaders; scan_leaders()"

# C19 수동 실행 (피보나치 갱신 + 대시보드 업로드)
python3 -c "from data.upload_swing import run_flowx_swing_upload; run_flowx_swing_upload()"
```
