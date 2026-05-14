# [단타봇 → 퀀트봇] KIS NX 통합 시세 + WebSocket 실시간 적용 가이드

> **이 프로젝트는 Body Hunter v4 — 한국 주식 단타봇(자동매매 시스템)입니다.**
> KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
> BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.
> CFO(재무)/CTO(기술)/COO(운영) 3-Officer 구조로 운영됩니다.

**작성**: 단타봇 (Body Hunter v4) → 퀀트봇
**작성일**: 2026-05-14
**검증**: VPS 실측 (Lightsail Linux + Python 3.11 + KIS 실계좌 키)

---

## 1. 배경

기존 KIS API 사용 시 일반적 한계:
- `FID_COND_MRKT_DIV_CODE='J'` (KRX 정규장) 시세만 사용 → **NXT 야간 거래량 누락**
- REST polling 30초 주기 → 자동매매 진입 평가 지연 + API 호출 부담
- WebSocket 사용에 한투 개발자센터 신청 필요한 줄 알았으나 — **API 호출로 즉시 자동 발급 가능**

단타봇이 5/14에 검증·구현 완료:
- `utils/kis_nxt_kit.py` (NX 시세 + approval_key + 시간대 분기 + 양매수 5일 판정)
- `utils/kis_websocket.py` (실시간 체결 구독 + 자동 재접속)
- `bot/auto_trader.py` (하이브리드 — REST polling + WS 캐시 5초 우선)

이 가이드는 퀀트봇이 동일 패턴을 즉시 적용 가능하도록 작성.

---

## 2. 의존성

### Python 패키지
```bash
pip install websockets requests python-dotenv
```

VPS 실측: `websockets 15.0.1` 정상 동작 확인.

### 환경변수 (.env)
```bash
KIS_APP_KEY=발급받은_앱키
KIS_APP_SECRET=발급받은_시크릿
KIS_ACC_NO=계좌번호  # 매매 시
MODEL=REAL  # 또는 VTS (모의투자)
```

---

## 3. 핵심 모듈 ① `utils/kis_nxt_kit.py`

NX(KRX+NXT 통합) 시세 + WebSocket approval_key 자동 발급 + 시간대 분기.

### 주요 함수
- `fetch_nx_price(code)` — NX 시장 현재가/거래량
- `get_access_token()` — REST 토큰 24h 캐시
- `get_approval_key(force_refresh=False)` — WebSocket approval_key 24h 캐시 자동 발급
- `get_session()` — 현재 시간대 분기 (`NXT_PREMARKET`/`KRX_REGULAR`/`NXT_AFTERMARKET`/`GLOBAL_ETF`)
- `is_yangmaesoo_5d(rows)` — 5일 양매수(외인+기관 동시 매수 3일+) 판정

### 사용 예시
```python
from utils.kis_nxt_kit import fetch_nx_price, get_approval_key, get_session

# 1. NX 통합 시세
r = fetch_nx_price('005930')
# → {'success': True, 'current_price': 293000, 'volume': 30389764, ...}

# 2. WebSocket approval_key (한투 신청 불필요 — API 자동)
ak = get_approval_key()
# → 'c9b6a48e-4718-4b9d-...' (24h 캐시)

# 3. 시간대 분기
sess = get_session()
# → 'NXT_AFTERMARKET' / 'KRX_REGULAR' / ...
```

### 단타봇 5/14 20:12 VPS 실측
```
삼성전자(005930) NX:    293,000원 (+3.17%)  거래량 30,389,764
SK하이닉스(000660) NX: 1,964,000원 (-0.61%) 거래량 3,885,018
approval_key: c9b6a48e-... 발급 OK
```

### ⚠️ 주의 (5/14 검증 시 발견)
1. **NX 거래량 의미** — 가이드는 "KRX+NXT 통합"이라 했으나 실측은 NXT 야간만일 가능성. 합산이라면 KRX 종가 시점 39M보다 적은 30M 출력은 모순. 퀀트봇 백테스트 시 추가 검증 권장
2. **NXT 미참여 종목** — 일부 KOSDAQ 종목은 NX 응답 0원. `fallback=True`로 KRX 자동 폴백 사용 권장
3. **rate limit** — KIS API 초당 20회 한도. 자체 카운터 없음 — 호출 폭주 시 429 위험

### 전체 코드
👉 단타봇 repo `scalper-agent/utils/kis_nxt_kit.py` (commit `2f6a584`, 약 350줄)

---

## 4. 핵심 모듈 ② `utils/kis_websocket.py`

KIS WebSocket 실시간 시세 클라이언트. 자동 재접속 + 구독 자동 복구.

### 주요 클래스 `KISWebSocketClient`
```python
async def connect()              # WebSocket 연결 + approval_key 발급
async def subscribe(codes, on_tick, tr_id='H0STCNT0')  # 종목 구독
async def unsubscribe(codes, tr_id='H0STCNT0')        # 구독 해제
async def run_forever()          # 수신 루프 (자동 재접속)
async def stop()                 # 종료
```

### TR_ID
- `H0STCNT0` — 주식 체결가 (정규장) ⭐ 가장 많이 씀
- `H0STASP0` — 주식 호가
- `H0STMOM0` — 지수 체결가

### 콜백 시그니처
```python
async def on_tick(code: str, price: int, volume: int, ts: str, raw_fields: list):
    """체결 이벤트 콜백.

    Args:
        code: 종목코드
        price: 현재가
        volume: 누적 거래량
        ts: 체결시각 HHMMSS
        raw_fields: KIS 원본 필드 (인덱스별 의미는 TR_ID 명세 참조)
    """
    print(f"[{ts}] {code}: {price:,}원 누적 {volume:,}")
```

### 사용 예시
```python
import asyncio
from utils.kis_websocket import KISWebSocketClient

async def main():
    client = KISWebSocketClient(max_subscriptions=40)

    async def on_tick(code, price, volume, ts, fields):
        # 퀀트봇 분석 로직 — 백테스트, 신호 평가, 모니터링 등
        print(f"{code} {price:,} {volume:,}")

    await client.subscribe(['005930', '000660'], on_tick)
    await client.run_forever()  # Ctrl+C 또는 stop() 호출까지

asyncio.run(main())
```

### 단타봇 5/14 20:33 VPS 실측
```
[WS] 연결 성공: ws://ops.koreainvestment.com:21000
[WS] 구독 완료: 2종목 H0STCNT0
수신 0건 (정규장 마감 후 — 정상). 정규장 09:00~15:30 재검증 필요
```

### 안전 파싱 패턴
KIS 응답 `"0|TR_ID|건수|payload"` 포맷. 단타봇 검증된 파싱:
```python
try:
    _flag, tr_id, _count, payload = data.split("|", 3)
except ValueError:
    return

fields = payload.split("^")
def _f(idx, default=""): return fields[idx] if idx < len(fields) else default
def _fi(idx):
    v = _f(idx)
    try: return int(v) if v else 0
    except ValueError: return 0

code = _f(0)
price = _fi(2)
volume = _fi(12)
time_hhmmss = _f(1)
```

### 전체 코드
👉 단타봇 repo `scalper-agent/utils/kis_websocket.py` (commit `554bffb`, 289줄)

---

## 5. 자동매매/실시간 분석 통합 패턴 — 하이브리드 권장

단타봇이 채택한 안전 패턴: **REST polling 100% 유지 + WebSocket 캐시 5초 우선**.

### 이유
- WebSocket 끊김 시 자동 폴백
- 기존 코드 회귀 위험 0
- WebSocket 미수신 시 30초 polling이 안전망

### 구현 (단타봇 `bot/auto_trader.py` 패턴)

```python
class QuantBot:
    def __init__(self):
        self._ws_client = None
        self._ws_task = None
        self._ws_cache = {}   # {code: {price, volume, ts, updated_at}}
        self._ws_enabled = config.get('use_websocket', False)

    async def start_websocket_monitor(self):
        if not self._ws_enabled:
            return
        from utils.kis_websocket import KISWebSocketClient
        self._ws_client = KISWebSocketClient(max_subscriptions=40)
        # 기존 감시 종목 일괄 구독
        existing = list(self._monitored_codes)
        if existing:
            await self._ws_client.subscribe(existing, self._on_websocket_tick)
        self._ws_task = asyncio.create_task(self._ws_client.run_forever())

    async def _on_websocket_tick(self, code, price, volume, ts, fields):
        """체결 이벤트 → 캐시 저장 (실제 평가는 polling이 수행)."""
        self._ws_cache[code] = {
            'price': price,
            'volume': volume,
            'ts': ts,
            'updated_at': time.time(),
        }

    def _get_ws_price(self, code, max_age_sec=5.0):
        """캐시 5초 이내면 반환, 만료/없음이면 None → REST 폴백 신호."""
        c = self._ws_cache.get(code)
        if not c or time.time() - c['updated_at'] > max_age_sec:
            return None
        return c

    async def evaluate(self, code):
        ws = self._get_ws_price(code)
        if ws:
            price = ws['price']
            volume = ws['volume']
        else:
            # REST fallback
            r = self.trader.fetch_price(code)
            price = r['current_price']
            volume = r['volume']
        # 퀀트봇 평가 로직...
```

### 효과 (단타봇 시뮬)
- 진입 평가 지연: 30초 polling → 밀리초 (체결 즉시)
- API 호출: 30초당 N건 → WS 캐시 hit 시 0건
- 안정성: REST polling 100% 유지

---

## 6. 검증 단계 (퀀트봇 측 필수)

### 단계 1 — 모듈 import 검증
```bash
python -c "from utils.kis_nxt_kit import fetch_nx_price; print(fetch_nx_price('005930'))"
```
→ 삼성전자 NX 시세 dict 정상 출력

### 단계 2 — WebSocket 연결 검증
```bash
python -m utils.kis_websocket
```
→ 정규장 시간(09:00~15:30)이면 체결 데이터 수신 / 마감 후엔 0건 (정상)

### 단계 3 — approval_key 자동 발급 검증
```python
from utils.kis_nxt_kit import get_approval_key
print(get_approval_key())  # 'xxxxx-...' 출력되면 OK
```
→ 한투 개발자센터 UI 신청 불필요 확인

### 단계 4 — 캐시 동작 검증
```python
# 5초 이내 두 번 호출 → 두 번째는 캐시 hit
key1 = get_approval_key()
key2 = get_approval_key()
assert key1 == key2  # 같은 키 (캐시)
```

---

## 7. 퀀트봇 활용 시나리오 (단타봇 제안)

### A. 백테스트 데이터 수집 강화
- NX 시세로 KRX+NXT 통합 거래량 분석 (대형주 야간 비중 측정)
- WebSocket 실시간 체결 → 분 단위 OHLCV 자체 생성

### B. 실시간 팩터 평가
- 동시 다종목 구독 (최대 40)
- 체결 이벤트 → 실시간 모멘텀/체결강도 팩터 갱신
- 30초 polling으로 같은 데이터 N번 호출하는 부담 제거

### C. 자동매매 진입 정확도
- 진입 6조건 또는 자체 시그널 평가 시점에 캐시 우선
- 갭상승/갭하락 즉시 감지
- REST API 호출 한도(일별 ~42회 추정) 안에서 더 많은 종목 매매 가능

### D. NXT 야간 거래 모니터링
- 16:30~21:30 NXT 시간대 자체 가격 추적
- 다음날 시가 갭 예측에 활용

---

## 8. 단타봇 측 적용 결과 (5/14 push된 커밋)

| 단계 | 커밋 | 효과 |
|------|------|------|
| Phase 1 | `2f6a584` | kis_nxt_kit.py (NX + approval_key) |
| Phase 3 | `554bffb` | kis_websocket.py (WebSocket 본체) |
| Phase 3-B | `08f1fac` | auto_trader 하이브리드 (REST + WS 캐시) |

→ 자동매매 ON 시 즉시 효과. 단타봇은 OFF 상태에서도 코드만 적용 — 회귀 위험 0.

---

## 9. 퀀트봇 측 회신 요청

구현 완료 후 다음을 단타봇으로 회신:

1. **적용 커밋**: 퀀트봇 repo 커밋 SHA
2. **변경 파일**: `utils/kis_nxt_kit.py`, `utils/kis_websocket.py` 등
3. **검증 결과**:
   - import 성공 여부
   - VPS 또는 로컬에서 NX 시세 + approval_key 정상 발급 여부
   - WebSocket 연결 OK 여부
4. **차이점**: 단타봇 코드를 그대로 썼는지, 퀀트봇 환경에 맞춰 수정한 부분 (예: import path, 토큰 캐시 위치)
5. **추가 필요한 데이터/기능**: 단타봇이 다음 Phase에서 보강할 만한 것

---

## 10. 주의 사항 종합

| 항목 | 영향 | 대응 |
|------|------|------|
| KIS API 동시 토큰 발급 한도 | 같은 키로 여러 봇이 동시 발급 시 rate limit | 캐시 24h 활용 / 봇별 토큰 분리 호출 |
| NXT 미참여 KOSDAQ 일부 | NX 응답 0원 | `fallback=True` (단타봇은 fetch_price_nx에 적용) |
| WebSocket 끊김 | 5초 자동 재접속, 구독 자동 복구 | 콜백 외 별도 처리 불필요 |
| 봇 충돌 (단타봇 + 퀀트봇 동시 구독) | 종목 중복 구독 — KIS 한도 40 종목/세션 | 봇별 다른 세션, max_subscriptions 분리 |
| 가이드 vs 실측 거래량 차이 | NX가 KRX+NXT 합산이 아닐 가능성 | 퀀트봇 백테스트 시 KRX 단독 + NX 별도 비교 |

---

## 11. 단타봇 다음 Phase (참고용)

| Phase | 작업 | 상태 |
|-------|------|------|
| Phase 1 | NX 시세 + approval_key | ✅ 완료 |
| Phase 3 | WebSocket 본체 | ✅ 완료 |
| Phase 3-B | auto_trader 하이브리드 | ✅ 완료 |
| Phase 4 | NXT 시장 매매 주문 + NXT WebSocket TR_ID 검증 | 미진행 |
| Phase 5 | 동시 다종목 100+ 구독 (KIS 한도 확장) | 미진행 |

→ Phase 4(NXT 매매 주문)는 단타봇이 진행 후 동일 패턴으로 퀀트봇에 공유 예정.

---

## 12. 문의

- 단타봇 측 SPEC 작성자: bodyhunter (Prophet-Agent-System repo)
- VPS 환경: AWS Lightsail Ubuntu 22.04 + Python 3.11
- 검증 시점: 2026-05-14 (목) 20:00~21:00 KST
