# 자비스 v3.0 + 5/20 D-Day 배포 가이드

**작성**: 2026-05-19 (D-Day 관망 모드 중 사전 작성)
**대상**: 사장님 — 5/20 새벽 (06:00~08:30) 배포 체크리스트
**전제**: [규칙 -2] 장 시작 09:00 전 완료 / 장 중 재시작 금지

---

## 0. 5/19 작업 요약 (배포 전 컨텍스트)

### 5/19 D-Day 관망 모드 결정 사유
- asset_pool_loader → auto_trader wire-up 미완성 발견
- D-Day 첫 가동에 변경 투입은 위험 → 관망 + 패턴 학습

### 사후 검증 (결정의 정당성)
- 만약 09:00 매수했다면 7종목 1주씩 **-169,680원 (-8.2%)** 손실
- 약세장 매크로(KOSPI -5%)에 로보틱스 동반 하락
- → **관망 결정 = 옳음**

### 5/19 작업 결과물 (5/20 배포 대상)
1. `bot/kis_trader.py` — `fetch_ranking_strength` 거래량+ETF 필터 추가
2. `bot/auto_trader.py` — `asset_pool_scan_and_buy()` 메서드 추가
3. `data/sector_concurrent_surge.py` — 알고리즘 A (섹터 동조 카운터) 신규
4. `tools/live_surge_tracker.py` + `surge_loop_tracker.py` + `surge_aggregator.py` — 학습 시스템
5. `docs/surge_master_20260519.md` — 급등 마스터 (9장)
6. `docs/surge_report_20260519.md` — 장 후 집계 (15:35 자동 생성)

---

## 1. 5/20 배포 체크리스트 (사장님 새벽 06:00~08:30)

### 1-1. 사전 확인 (06:00~06:30)
- [ ] **장 마감 데이터 확인** — `data_store/limit_up/watchlist.json`, `tipping_scan.json` updated_at 5/19 18:00+ 확인
- [ ] **5/19 brain_state** — `data_store/brain_state.json` regime 5/20 BEARISH/CAUTIOUS/NEUTRAL 중 무엇인지 확인
- [ ] **막내(정보봇) intelligence_short_signal** — 5/20 매크로 경고 메시지 확인
- [ ] **큰형(퀀트봇) advisory** — Supabase quant_advisory 테이블 5/20 LEADING/SNAPSHOT 도착 여부

### 1-2. 코드 배포 (06:30~07:30)

```bash
# VPS 접속 (bodyhunter-bot 서비스 중지 NO — 아직 5/20 장 시작 전)
ssh root@bodyhunter-vps

# 코드 동기화 (git pull)
cd /opt/bodyhunter
git pull origin main

# 새 종속성 확인 (없음 — 모두 기존 라이브러리 사용)

# 봇 재시작 (장 시작 전이므로 [규칙 -2] 비적용)
systemctl restart bodyhunter-bot

# 가동 확인
systemctl status bodyhunter-bot
journalctl -u bodyhunter-bot -f --since "1 minute ago"
```

### 1-3. 검증 (07:30~08:30)

#### 검증 1: 결함 1 fix 동작
```bash
python -X utf8 -c "
from bot.kis_trader import KISTrader
t = KISTrader()
r = t.fetch_ranking_strength(top_n=10)
for x in r:
    print(f'{x[\"code\"]} {x[\"name\"]:<14} 강도{x[\"strength\"]:>6.1f} 거래량{x[\"volume\"]:>10,}')
"
# 기대: 일반 종목 (KODEX/TIGER/ETN/채권 X)
# 기대: 거래량 모두 10만주+
```

#### 검증 2: 알고리즘 A 동작
```bash
python -X utf8 data/sector_concurrent_surge.py --min-group 4
# 기대: 5/20 KIS API 호출 후 섹터별 +10%+ 종목 분포 출력
```

#### 검증 3: wire-up 미가동 확인 (아직)
```bash
# auto_trader.asset_pool_scan_and_buy() 가 scheduler에 등록되어 있는지 확인
grep -n "asset_pool_scan_and_buy" bot/*.py data/trading_coo.py
# 결과 0건이면 정상 — 5/20에는 호출 안 함
# 5/21 이후 trading_coo 스케줄러에 09:05 한 번 호출 등록 예정
```

### 1-4. 텔레그램 `시작` 버튼 (08:54 전 1회 필수)

> 메모리 [project_5_19_d_day_preparation]에 명시된 5/20도 동일 필요.

---

## 2. 5/20 D-Day 가동 시나리오

### A안: **보수 가동** (강력 추천)
- 09:00 morning_rec 정상 가동
- 09:05~14:00 intraday_scanner 정상 가동
- **wire-up 미가동** (코드 배포는 했지만 자동 매수 X)
- 사장님이 텔레그램으로 자산풀 결과만 수신, 매수 여부 수동 결정
- 안전망: 매크로 BEARISH 시 0매수 = 0손실

### B안: **진보 가동** (사장님 결정 시)
- A안 + `trading_coo._job_morning_jarvis_v3` 에 `await trader.asset_pool_scan_and_buy(top_k=3, qty_per_stock=1)` 추가
- 09:05 한 번 호출 → 자산풀 TOP 3 × 1주 자동 매수
- advisory 게이트 BEARISH/PANIC 시 자동 차단
- 15:25 verification 청산에 함께 청산
- 잠재 손익: -3만 ~ +5만 (1주 모드 가정)

### **추천**: A안 시작 → 5/21 검증 후 B안 전환

---

## 3. 5/20~5/22 검증 일정

### 5/20 D-Day
- 09:00 라이브 가동
- 09:05~15:30 모든 시그널 로깅
- 15:30 brain_state 5/20 정리
- 18:00 summary 보고서 자동 생성

### 5/21 D+1
- 5/20 결과 분석 → A안→B안 전환 결정
- B안 채택 시 wire-up 스케줄러 추가

### 5/22 D+2
- 종합 평가: 1주 모드 손익 vs 일반 모드 잠재 손익

---

## 4. 비상 대응

### 시나리오 1: 5/20 또 약세장 (5/19 반복)
- advisory BEARISH 자동 차단 → wire-up 가동돼도 매수 0건
- 손실 0, 학습 +1

### 시나리오 2: 5/20 강세장
- advisory BULLISH/NEUTRAL → wire-up 정상 가동
- 자산풀 TOP 3-5 매수
- 정상 회전 + 15:25 청산

### 시나리오 3: KIS API 장애
- intraday_scanner 자동 차단 (try/except 처리됨)
- 큰형 advisory 게이트도 자체 fallback (어제 데이터)

### 시나리오 4: 매수 실행 중 오류
- try/except 종목별 처리 → 다른 종목 영향 없음
- failed 리스트 텔레그램 알림

---

## 5. 코드 변경 영향 범위 (회귀 테스트 체크)

### 5-1. `fetch_ranking_strength` 시그니처 변경
- **변경**: `(market, top_n)` → `(market, top_n, min_volume=100000, exclude_etf=True)`
- **하위호환**: 모든 추가 인자에 기본값 → 기존 호출은 그대로 동작
- **영향**: intraday_scanner의 결과가 더 정확해짐 (긍정적)
- **회귀 위험**: 거의 없음

### 5-2. `auto_trader.asset_pool_scan_and_buy` 신규
- **변경**: 새 메서드 추가만, 기존 코드 수정 0건
- **영향**: 호출 안 하면 0 영향
- **회귀 위험**: 0

### 5-3. `data/sector_concurrent_surge.py` 신규
- **변경**: 새 파일
- **영향**: 호출 안 하면 0 영향
- **회귀 위험**: 0

---

## 6. 사장님 결정 필요 사항

### Q1: 5/20 가동 방식
- [ ] A안 (보수 — 코드 배포만, wire-up 미가동) ← 추천
- [ ] B안 (진보 — wire-up 가동, 1주 모드)

### Q2: 알고리즘 A 텔레그램 알림
- [ ] 5/20부터 동조 그룹 4개+ 시 즉시 알림 활성화
- [ ] 5/21 이후 활성화 (관찰 1일)

### Q3: 결함 2 영구 수정 시점
- [ ] 5/20 새벽 (이 문서대로 배포)
- [ ] 5/21 새벽 (5/20 더 관찰 후)

---

**작성 완료**. 5/19 장 후 보고서(`surge_report_20260519.md`)와 함께 검토 부탁드립니다.
