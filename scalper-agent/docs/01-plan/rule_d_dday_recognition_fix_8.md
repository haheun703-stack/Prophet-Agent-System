# 🎯 룰 D D-Day 인식 + 수익 극대화 Fix 8가지 (Codex 작업서)

> **작성**: 2026-05-27 단타봇 / **승인**: 사장님 / **실행**: Codex (5/28~5/30)
> **목표**: 5/22 같은 시그널 → D+0 매수 보장 + 정점 회피 + 매도 자동 연계
> **사장님 1년 70억 미션 부합** + 6/1 D-Day 실전 적용

---

## 🚨 사고 회고 (5/27 D+3 매수 사고)

| 시점 | 동작 | 결과 |
|---|---|---|
| 5/22 16:02 | LimitUpEngine 후보 등록 (대양금속 1,550원) | 등록만 (장 마감 후) |
| 5/23~5/26 | 봇 매수 0건 (D+0/D+1/D+2 무시) | 시그널 발생일 추적 X |
| **5/27 14:51/14:59** | **로컬 run_bot.py 매수** | **D+3 정점 매수 → 손실** |

**5거래일 +84% 폭등 누적 → 정점 매수 사고**

---

## 🛠️ Fix 8가지 통합 명령

### Fix 1: LimitUpEngine watchlist signal_date 저장 ★ 전제 ★

**위치**: `data/limit_up_engine.py`

**현재**: watchlist에 시그널 발생일 메타데이터 없음

**Fix**:
```python
# 후보 등록 시
watchlist[code] = {
    "code": code,
    "name": name,
    "signal_date": today.isoformat(),  # ★ NEW
    "signal_close_price": current_close,  # ★ NEW (누적 % 계산용)
    "monitor_until": (today + timedelta(days=7)).isoformat(),
    "strategy": "limit_up_pullback_v3",
    ...
}
```

**테스트**: watchlist.json에 signal_date / signal_close_price 필드 존재 확인

---

### Fix 2: LimitUpEngine 장중 실시간 가동 ★★★ 가장 중요 ★★★

**위치**: `crontab` + `data/limit_up_engine.py`

**현재**: 16:02 / 17:45 (장 마감 후) — **같은 날 14:50 룰 D 매수 불가능**

**Fix**:
- crontab에 **5분 단위 장중 LimitUpEngine** 추가:
  ```cron
  */5 9-14 * * 1-5 cd /home/ubuntu/bodyhunter/scalper-agent && python -m data.limit_up_engine --realtime
  ```
- `--realtime` 모드: 빠른 스캔 (전체 412종목 X / KIS 실시간 +10%+ 종목만)
- 09:30~14:45 (장중) 5분 단위 = 매수 진입 가능 시간

**효과**: 5/22 14:30 대양금속 +22% 도달 → 14:30 후보 등록 → 14:50 룰 D 매수 = **D+0 종가 매수**

---

### Fix 3: asset_pool에 LimitUpEngine 통합 ★ 데이터 흐름 ★

**위치**: `utils/asset_pool_loader.py`

**현재**: `collect_all_candidates()` 결과에 LimitUpEngine watchlist 미포함

**Fix**:
```python
def collect_all_candidates():
    candidates = {
        ...,
        "limit_up_pullback": load_limit_up_watchlist(),  # ★ NEW
    }
    return candidates

def get_candidate_source_map():
    src_map = {}
    for src, items in candidates.items():
        ...
        # limit_up_pullback 소스 가중치 +20점 (사장님 5/22 패턴)
        if src == "limit_up_pullback":
            for item in items:
                src_map[item["code"]].add(f"limit_up_pullback({item['signal_date']})")
    return src_map
```

**효과**: 룰 D 후보 선정 시 LimitUpEngine 시그널 종목 우선 포함 + signal_date 메타데이터 함께 전달

---

### Fix 4: D+0 우선 매수 / D+1은 +5% 이내만 ★ 사장님 룰 D 정확 적용 ★

**위치**: `bot/auto_trader.py:pre_close_d_scan_and_buy()` (라인 2843)

**현재**: `if chg < 10.0: continue` (당일 강세만 체크)

**Fix**:
```python
from datetime import date
today = date.today()

for code, _src_count in candidates_sorted[:50]:
    # ★ Fix 1 추가 가드: signal_date 확인 ★
    signal_info = limit_up_watchlist.get(code)
    if signal_info:
        signal_date = date.fromisoformat(signal_info["signal_date"])
        days_since = (today - signal_date).days
        
        if days_since == 0:
            # D+0 — 사장님 룰 D 정확 적용 (우선 매수)
            priority_boost = 30
        elif days_since == 1:
            # D+1 — 신호 종가 +5% 이내만
            signal_close = signal_info["signal_close_price"]
            current_price = price_info["current_price"]
            cumulative_pct = (current_price - signal_close) / signal_close * 100
            if cumulative_pct > 5.0:
                logger.info(f"[pre_close_d] {code} D+1 +{cumulative_pct:.1f}% 갭업 — 차단")
                continue
            priority_boost = 15
        else:
            # D+2 이상 — 영구 차단
            logger.info(f"[pre_close_d] {code} D+{days_since} 정점 회피 — 차단")
            continue
    
    # 기존 조건 (오늘 +10%+, 눌림 등) 유지
    ...
```

**효과**:
- 5/22 시그널 → 5/22 (D+0) 즉시 매수
- 5/23 (D+1)은 +5% 이내만 (1,628원 이하)
- 5/24~ (D+2+) 자동 차단

---

### Fix 5: 누적 폭등률 가드 +20% ★ 정점 회피 ★

**위치**: `bot/auto_trader.py:pre_close_d_scan_and_buy()` (라인 2843)

**Fix**:
```python
if signal_info:
    signal_close = signal_info["signal_close_price"]
    current_price = price_info["current_price"]
    cumulative_pct = (current_price - signal_close) / signal_close * 100
    
    # ★ 누적 +20%+ 차단 (사장님 룰 5 limit_up_split_threshold 25% 직전 영구 룰) ★
    if cumulative_pct > 20.0:
        logger.info(f"[pre_close_d] {code} 누적 +{cumulative_pct:.1f}% 정점 차단")
        continue
```

**효과**: 시그널 종가 대비 +20%+ 폭등 시 매수 영구 차단 / 5/27 대양금속 (1,550 → 2,084 = +34%) 같은 사고 영구 차단

---

### Fix 6: D+2 이상 매수 영구 차단 (Fix 4에 통합)

→ Fix 4 코드의 `else: continue` 부분 = D+2 이상 자동 차단

---

### Fix 7: 매도 시스템 자동 연계 ★ 수익 보장 ★

**위치**: `bot/auto_trader.py:pre_close_d_scan_and_buy()` 매수 성공 후

**현재**: 룰 D 매수 후 별도 매도 로직 (수동)

**Fix**: 매수 성공 시 자동:
```python
# 트레일링 SL (고가 -3%) 즉시 활성
trailing_sl_pct = SAJANG.TRAILING_PCT  # 3.0
self._activate_trailing_sl(code, trailing_sl_pct)

# 룰 B (asset_pool +10%+ 절반 익절) 자동 적용
rule_b_threshold = SAJANG.RULE_B_THRESHOLD  # 10.0
self._register_rule_b_watch(code, rule_b_threshold)

# 룰 C (D+1 갭다운 -7%+ 즉시 매도) 자동 적용
rule_c_threshold = SAJANG.D1_GAP_SELL_THRESHOLD  # -7.0
self._register_rule_c_protection(code, rule_c_threshold)

# 사장님 룰 4 (+25%+ 절반 익절) 자동 적용
limit_up_threshold = SAJANG.LIMIT_UP_SPLIT_THRESHOLD  # 25.0
self._register_limit_up_split_sell(code, limit_up_threshold)
```

**효과**: 매수 시점부터 4가지 매도 룰 자동 가동 / 사장님 영구 룰 100% 부합

---

### Fix 8: 5/22 폭등 시뮬레이션 테스트 ★ 검증 ★

**위치**: `bot/test_rule_d_dday_5_27.py` (신규)

**테스트 케이스**:
```python
def test_dday_d0_buy():
    """5/22 시그널 → 5/22 매수 통과 검증"""
    watchlist = {"009190": {"signal_date": "2026-05-22", "signal_close_price": 1550}}
    # 5/22 14:50 시점 시뮬레이션
    # 기대: 매수 통과

def test_dday_d3_block():
    """5/22 시그널 → 5/27 D+3 매수 차단 검증"""
    watchlist = {"009190": {"signal_date": "2026-05-22", "signal_close_price": 1550}}
    # 5/27 14:50 시점 시뮬레이션
    # 기대: D+3 차단

def test_cumulative_20pct_block():
    """누적 +20%+ 차단 검증"""
    # 5/22 1,550 → 5/23 1,860 (+20%)
    # 기대: D+1 차단 (5% 초과)

def test_d1_within_5pct_pass():
    """D+1 +5% 이내 통과 검증"""
    # 5/22 1,550 → 5/23 1,620 (+4.5%)
    # 기대: D+1 통과
```

**기대**: 4건 PASS / 5/22 같은 시그널 → D+0 매수 보장 + 정점 회피

---

## 🎯 6/1 D-Day 시뮬레이션 (Fix 8 적용 후)

| 시점 | 동작 | 가격 | 결과 |
|---|---|---|---|
| 5/22 14:30 | LimitUpEngine 실시간 후보 등록 (대양금속 +22%) | 1,550 | D+0 후보 |
| 5/22 14:50 | 룰 D D+0 매수 | 1,550 | **매수 ★** |
| 5/23 09:00 | D+1 갭업 시초 | 1,875 (+21%) | 트레일링 활성 |
| 5/23 09:30 | 룰 B +10%+ 도달 | 1,705 | 절반 익절 (+10%) |
| 5/23 종가 | 트레일링 SL 발동 | 1,800 (+16%) | 나머지 익절 |
| **5/23 총 수익** | | | **+13% (1일)** |

→ **5/22 D+0 매수 → 5/23 D+1 익절** = 사장님 룰 D 정확 적용

---

## 📋 Codex 진행 우선순위

| # | 작업 | 일정 | 검증 |
|---|---|---|---|
| **1** | Fix 1 (signal_date 저장) | 5/28 오전 | watchlist.json 필드 확인 |
| **2** | Fix 2 (장중 LimitUpEngine cron) | 5/28 오전 | crontab + --realtime 모드 |
| **3** | Fix 3 (asset_pool 통합) | 5/28 오후 | collect_all_candidates 결과 |
| **4** | Fix 4 (D+0/D+1 가드) | 5/28 오후 | 단위 테스트 |
| **5** | Fix 5 (+20% 누적 가드) | 5/29 오전 | 단위 테스트 |
| **6** | Fix 7 (매도 자동 연계) | 5/29 오후 | 통합 테스트 |
| **7** | Fix 8 (시뮬레이션 테스트) | 5/30 오전 | 4건 PASS |
| **8** | 5/30 최종 리허설 검증 | 5/30 오후 | 5/29 PAPER + 5/30 검증 |

---

## 🛡️ 검증 의무 (Codex)

- ✅ restart readiness 4/4 PASS 유지
- ✅ vwap_split_buy 7/7 PASS 유지
- ✅ 신규 테스트 4건 PASS (Fix 8)
- ✅ kill_switch + PAPER_ONLY + 전략 스위치 OFF 유지
- ✅ 5/28 PAPER 리허설 미영향
- ✅ 5/30 최종 리허설에서 Fix 8가지 전체 통합 검증

---

## 🎯 사장님 1년 70억 미션 부합

- 종목 선정 정확도 80% 도달 시 ([[project_5_24_stock_selection_evolution]])
- D+0 매수 + 자동 매도 연계 = 1일 평균 +10~20% 가능
- 5/22 같은 +1,996% 폭등 미스 영구 차단

---

**작성자**: 단타봇 (5/27 D+3 사고 회고 + 수익 극대화 비전)
**검수 의무**: Codex (4-Tier 통과)
**최종 승인**: 사장님
