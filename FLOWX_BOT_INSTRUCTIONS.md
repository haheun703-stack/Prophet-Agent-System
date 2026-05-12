# FLOWX 섹터맵 — 봇별 작업 지시서
## 2026.03.21 | 정보봇(이 채팅) → 각 봇 프로젝트에 전달

---

# ═══════════════════════════════════════════
# 1. 웹봇 (Claude Code) 지시서
# ═══════════════════════════════════════════

## 네가 할 일: 코드 구현 + DB 세팅

### Step 1: Supabase 테이블 생성

```sql
-- 1) 섹터 유니버스 (종목 마스터)
CREATE TABLE sector_universe (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sector_key text NOT NULL,
  sector_name text NOT NULL,
  tier integer NOT NULL CHECK (tier BETWEEN 1 AND 5),
  stock_name text NOT NULL,
  ticker text NOT NULL,
  market text NOT NULL CHECK (market IN ('US','KR','EU','JP','CN','UK')),
  "desc" text,
  change_pct real DEFAULT 0,
  volume_ratio real DEFAULT 1,
  foreign_net bigint DEFAULT 0,
  institution_net bigint DEFAULT 0,
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_sector ON sector_universe(sector_key);
CREATE INDEX idx_tier ON sector_universe(sector_key, tier);
CREATE INDEX idx_ticker ON sector_universe(ticker);

-- 2) 공급망 연결 (기업 간 관계)
CREATE TABLE sector_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sector_key text NOT NULL,
  from_stock text NOT NULL,
  to_stock text NOT NULL,
  relation text NOT NULL,
  strength integer DEFAULT 1 CHECK (strength BETWEEN 1 AND 5),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_links_sector ON sector_links(sector_key);
CREATE INDEX idx_links_from ON sector_links(from_stock);
CREATE INDEX idx_links_to ON sector_links(to_stock);
```

### Step 2: 시드 데이터 INSERT

`FLOWX_SECTOR_UNIVERSE.json` 파일을 파싱해서:
- `sectors` → `sector_universe` 테이블에 INSERT (13섹터 259종목)
- `links` → `sector_links` 테이블에 INSERT (5섹터 88개 연결)

⚠️ links의 `to`는 배열이므로 개별 행으로 풀어서 넣어야 함:
```
{"from": "NVIDIA", "to": ["TSMC","SK하이닉스","한미반도체"], "rel": "HBM/파운드리"}
```
→ 3개 행:
```
(semiconductor, NVIDIA, TSMC, HBM/파운드리, 5)
(semiconductor, NVIDIA, SK하이닉스, HBM/파운드리, 5)
(semiconductor, NVIDIA, 한미반도체, HBM/파운드리, 5)
```

### Step 3: 컴포넌트 구현

`FLOWX_DATAVIZ_SPEC.md` 섹션 8 참고해서 순서대로:
1. `SectorSwimlane.tsx` — 스윔레인 레인 + 종목 카드 + SVG 커넥션
2. `SectorNetwork.tsx` — Canvas 네트워크 그래프 (PRO 전용)
3. `SectorMapView.tsx` — 듀얼 뷰 토글 래퍼

### Step 4: API 엔드포인트

```typescript
// app/api/sectors/[key]/route.ts
// GET /api/sectors/semiconductor
// → sector_universe WHERE sector_key = 'semiconductor' ORDER BY tier DESC
// → sector_links WHERE sector_key = 'semiconductor'
```

### ⚠️ 중요
- 너는 change_pct, foreign_net 등 **실시간 데이터를 생성하지 않는다**
- 그건 정보봇과 단타봇이 매일 UPDATE 해줌
- 너는 Supabase에서 SELECT 해서 화면에 뿌리기만 하면 됨
- 데이터 없으면 → 스켈레톤 로딩 + "장 시작 전" 표시

### 전달 파일
- `FLOWX_DATAVIZ_SPEC.md` (1,070줄 — 전체 시각화 스펙)
- `FLOWX_SECTOR_UNIVERSE.json` (609줄 — 시드 데이터 + 스키마)


---

# ═══════════════════════════════════════════
# 2. 정보봇 (JGIS) 지시서
# ═══════════════════════════════════════════

## 네가 할 일: 매일 장마감 후 change_pct UPDATE

### 왜 정보봇인가
- 이미 US/KR 종가 CSV를 매일 저장하고 있음
- 259종목의 일일 등락률(change_pct)을 Supabase에 밀어넣는 게 핵심 역할
- 뉴스 기반 섹터 모멘텀도 여기서 나옴

### 추가할 Python 스크립트

```python
# scripts/update_sector_universe.py
# 매일 장마감 후 실행 (KR: 15:30, US: 다음날 06:00 KST)

import os
from supabase import create_client

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']  # service role key (write 권한)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_change_pct():
    """sector_universe 테이블의 change_pct를 일일 종가 기준으로 업데이트"""
    
    # 1) 현재 sector_universe에서 전체 종목 조회
    result = supabase.table('sector_universe').select('id, ticker, market').execute()
    stocks = result.data
    
    # 2) 각 종목의 일일 등락률 계산
    for stock in stocks:
        ticker = stock['ticker']
        market = stock['market']
        
        if market == 'KR':
            # 한국 종목: 이미 저장된 CSV에서 등락률 가져오기
            # (기존 정보봇 파이프라인에서 가져오는 방식 사용)
            change = get_kr_change_pct(ticker)  # 기존 함수 활용
        elif market == 'US':
            # 미국 종목: yfinance 또는 기존 API에서 가져오기
            change = get_us_change_pct(ticker)  # 기존 함수 활용
        else:
            # EU/JP/CN 종목
            change = get_global_change_pct(ticker, market)
        
        if change is not None:
            # 3) Supabase UPDATE
            supabase.table('sector_universe').update({
                'change_pct': round(change, 2),
                'updated_at': 'now()'
            }).eq('id', stock['id']).execute()
    
    print(f"[정보봇] sector_universe {len(stocks)}종목 change_pct 업데이트 완료")

# 기존 정보봇 함수 활용 (이미 있는 것)
def get_kr_change_pct(ticker):
    """기존 종가 CSV에서 해당 티커의 일일 등락률 반환"""
    # TODO: 기존 정보봇의 종가 데이터 로직 연결
    pass

def get_us_change_pct(ticker):
    """미국 종목 등락률 반환"""
    # TODO: yfinance 또는 기존 API 활용
    pass

def get_global_change_pct(ticker, market):
    """EU/JP/CN 종목 등락률 반환"""
    # TODO: 기존 글로벌 데이터 소스 활용
    pass

if __name__ == '__main__':
    update_change_pct()
```

### cron 설정 추가

```
# 기존 cron에 추가 (PythonAnywhere 또는 Railway)
# 한국장 마감 후 (15:35 KST)
35 15 * * 1-5 python scripts/update_sector_universe.py

# 미국장 마감 후 (다음날 06:05 KST)  
05 6 * * 2-6 python scripts/update_sector_universe.py
```

### ⚠️ 중요
- 너는 **change_pct만** 업데이트한다
- foreign_net, institution_net, volume_ratio는 단타봇 담당 → 건드리지 마
- stock_name, ticker, tier 같은 마스터 데이터는 변경하지 않음
- 에러 시 텔레그램 알림 (기존 알림 파이프라인 활용)

### 네가 UPDATE 하는 컬럼
| 컬럼 | 설명 | 빈도 |
|------|------|------|
| `change_pct` | 일일 등락률 (%) | 매일 2회 (KR장 마감 + US장 마감) |
| `updated_at` | 마지막 갱신 시각 | change_pct와 함께 |


---

# ═══════════════════════════════════════════
# 3. 단타봇 지시서
# ═══════════════════════════════════════════

## 네가 할 일: 매일 장마감 후 수급 데이터 UPDATE

### 왜 단타봇인가
- 수급 분석이 핵심 역량 (외인 국가별 수급, 기관 동반)
- foreign_net, institution_net, volume_ratio → 단타봇만 계산 가능
- 이 데이터가 섹터맵에서 "어디에 돈이 들어오고 있나"를 보여줌

### 추가할 Python 스크립트

```python
# scripts/update_sector_supply.py
# 매일 장마감 후 실행 (15:35 KST)

import os
from supabase import create_client

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_supply_data():
    """sector_universe 테이블의 수급 데이터 업데이트"""
    
    # 1) 한국 종목만 조회 (외인/기관 수급은 KR 종목에만 해당)
    result = supabase.table('sector_universe') \
        .select('id, ticker, market') \
        .eq('market', 'KR') \
        .execute()
    kr_stocks = result.data
    
    for stock in kr_stocks:
        ticker = stock['ticker']
        
        # 2) 기존 단타봇 수급 분석 함수 활용
        supply = get_supply_data(ticker)  # 기존 함수
        
        if supply:
            supabase.table('sector_universe').update({
                'foreign_net': supply['foreign_net'],       # 외인 순매수 (억원)
                'institution_net': supply['institution_net'], # 기관 순매수 (억원)
                'volume_ratio': round(supply['volume_ratio'], 2),  # 거래량 배율
                'updated_at': 'now()'
            }).eq('id', stock['id']).execute()
    
    # 3) 미국 종목은 volume_ratio만 업데이트 (외인/기관 개념 없음)
    us_result = supabase.table('sector_universe') \
        .select('id, ticker') \
        .eq('market', 'US') \
        .execute()
    
    for stock in us_result.data:
        vol = get_us_volume_ratio(stock['ticker'])
        if vol:
            supabase.table('sector_universe').update({
                'volume_ratio': round(vol, 2),
                'updated_at': 'now()'
            }).eq('id', stock['id']).execute()
    
    total = len(kr_stocks) + len(us_result.data)
    print(f"[단타봇] sector_universe {total}종목 수급 데이터 업데이트 완료")

def get_supply_data(ticker):
    """기존 단타봇의 수급 분석 결과 반환"""
    # TODO: 기존 단타봇 수급 분석 로직 연결
    # return {'foreign_net': 150, 'institution_net': -30, 'volume_ratio': 2.5}
    pass

def get_us_volume_ratio(ticker):
    """미국 종목 거래량 배율 반환"""
    # TODO: 기존 거래량 분석 로직 연결
    pass

if __name__ == '__main__':
    update_supply_data()
```

### cron 설정 추가

```
# 한국장 마감 후 (15:40 KST — 정보봇보다 5분 뒤)
40 15 * * 1-5 python scripts/update_sector_supply.py
```

### ⚠️ 중요
- 너는 **foreign_net, institution_net, volume_ratio만** 업데이트한다
- change_pct는 정보봇 담당 → 건드리지 마
- 한국 종목(market='KR')은 3개 컬럼 모두 업데이트
- 미국 종목(market='US')은 volume_ratio만 업데이트 (외인/기관 개념 없음)
- EU/JP/CN 종목은 현재 스킵 (추후 확장)

### 네가 UPDATE 하는 컬럼
| 컬럼 | 대상 | 설명 | 빈도 |
|------|------|------|------|
| `foreign_net` | KR만 | 외인 순매수 (억원) | 매일 장마감 |
| `institution_net` | KR만 | 기관 순매수 (억원) | 매일 장마감 |
| `volume_ratio` | KR+US | 20일 평균 대비 거래량 배율 | 매일 장마감 |
| `updated_at` | 전체 | 마지막 갱신 시각 | 위와 함께 |


---

# ═══════════════════════════════════════════
# 4. 퀀트봇 지시서
# ═══════════════════════════════════════════

## 네가 할 일: 자기 테이블에만 INSERT (섹터맵과 직접 무관)

### 왜 퀀트봇은 sector_universe를 안 건드리나
- 퀀트봇의 데이터(ETF 시그널, 섹터 로테이션, 중국자금)는 **별도 테이블**에 들어감
- 섹터맵(sector_universe)은 정보봇+단타봇이 채우고, 웹봇이 읽음
- 퀀트봇 데이터는 웹봇이 **별도 차트**(PortfolioDonut, ChinaFlowArea 등)에서 읽어감

### 퀀트봇이 쓰는 테이블
| 테이블 | 용도 | FlowX 패널 |
|--------|------|------------|
| `etf_signals` | ETF 매수/매도/홀드 시그널 | ETF 시그널 차트 |
| `china_flow` | 북향/남향 자금 흐름 | 중국자금 흐름 차트 |
| `paper_trades` | 페이퍼 트레이딩 기록 | 누적 수익률 차트 |

### ⚠️ sector_universe 테이블은 읽기만 가능
- 퀀트봇이 5축 스코어로 종목을 평가할 때 sector_universe를 **참조(READ)**는 OK
- 하지만 change_pct나 foreign_net 등을 **수정(UPDATE)**하면 안 됨
- 그건 정보봇/단타봇 영역

### 향후 연결 가능성
- 퀀트봇의 섹터 로테이션 데이터가 **섹터맵과 연결**될 수 있음
- 예: "지금 자금이 반도체 → 방산으로 이동 중" → 섹터맵에서 화살표로 표시
- 이건 Phase C 이후 논의


---

# ═══════════════════════════════════════════
# 실행 타임라인 요약
# ═══════════════════════════════════════════

```
Day 1-2:  웹봇 → Supabase 테이블 생성 + 시드 INSERT + SectorSwimlane 구현
Day 3-4:  웹봇 → SectorNetwork 구현 + SectorMapView 래퍼
Day 5:    정보봇 → update_sector_universe.py 작성 + cron 등록
Day 6:    단타봇 → update_sector_supply.py 작성 + cron 등록
Day 7:    통합 테스트 → 장마감 후 데이터 자동 갱신 확인
```

### 데이터 흐름 최종 확인

```
매일 15:30 한국장 마감
  │
  ├─ 15:35 정보봇 → sector_universe.change_pct UPDATE (KR 종목)
  ├─ 15:40 단타봇 → sector_universe.foreign_net/institution_net/volume_ratio UPDATE
  │
  └─ flowx.kr Supabase 실시간 구독 → 대시보드 자동 갱신

매일 06:00 미국장 마감
  │
  ├─ 06:05 정보봇 → sector_universe.change_pct UPDATE (US 종목)
  ├─ 06:10 단타봇 → sector_universe.volume_ratio UPDATE (US 종목)
  │
  └─ flowx.kr 자동 갱신
```


---

*작성: 정보봇 (JGIS) × Claude | 2026.03.21*
*배포: 각 봇 프로젝트 루트에 이 파일의 해당 섹션만 복사*
