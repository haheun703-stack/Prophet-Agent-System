# ⚠️ DEAD-KIWOOM — 데드코드 경고 (신규 세션/Codex 필독)

> **6/26 작성 (슬러지 H).** 이 파일의 목적: 신규 세션/Codex/운영자가 **Kiwoom v1.0 데드코드를 진짜 라이브 봇으로 오인하는 것**(6/24 검수가 지목한 "최대 비용")을 막는다. **삭제가 아니라 격리/표기**다. git 이력에 보존됨.

---

## ✅ 진짜 라이브 봇 (KIS·mojito) — 이것만 실제로 돈다

```
run_bot.py                      ← 진짜 진입점 (텔레그램 봇 + KIS 실매매)
  └ bot/telegram_bot.py
      ├ bot/kis_trader.py       ← KIS REST API (★ mojito 기반 · Kiwoom 아님)
      ├ bot/auto_trader.py      ← 자동매매 엔진
      ├ bot/trading_coo.py      ← 운영 총괄 (7-Group 스케줄)
      ├ bot/trading_cfo.py / trading_cto.py / bot_logger.py
      └ data/ · engine/(4개) · strategies/(20개) · risk/(3개) · output/

tools/run_nightly_pipeline.py   ← 야간 데이터 파이프라인 (cron 18:00) = 라이브
```

**판별 근거**: `bot/kis_trader.py`(line 378~)가 `import mojito` → `mojito.KoreaInvestment(...)`. 한국투자증권(KIS) REST API다. **키움(Kiwoom)·QAxWidget·OCX 안 씀.**

---

## ❌ 데드 — Kiwoom v1.0 (라이브 경로에서 import 0건 · 손대지 말 것)

### 확정 데드 (명백한 키움 v1.0)
| 대상 | 정체 | 근거 |
|---|---|---|
| **`main.py`** | Kiwoom v1.0 진입점 (PyQt5 + QAxWidget COM·키움 HTS 필요) | `from api.kiwoom_core import KiwoomCore` 등 import / 라이브 경로에서 `import main` 0건 / 마지막수정 2025-02-18 |
| **`api/`** (전체 5개) | 키움 Open API 래퍼 (kiwoom_core·kiwoom_data·kiwoom_realtime·kiwoom_order·kiwoom_constants) | `grep "from api\." bot/ data/ tools/ engine/` → 0건. **main.py만 호출** |

### 데드 후보 (라이브 import 0 · main.py 전용 · 정리 후보)
| 대상 | 비고 |
|---|---|
| `backtest/` (~22개) | main.py 백테스트 전용. (단 `backtester.py`는 라이브 경로 호출 가능성 — 확인 후 처리) |
| `strategies/body_hunter.py`·`body_hunter_v3.py`·`daily_filter.py`·`group_rotation.py` | 라이브 strategies 20개에서 제외됨 (body_hunter_v2로 대체) |
| `engine/trading_engine.py`·`body_hunter_master.py`·`portfolio.py`·`market_state.py`·`order_manager.py` | main.py 전용 / `portfolio.py`는 `paper_portfolio.py`로 대체됨 |

---

## 🔒 보존 (절대 데드로 오인 말 것 — 같은 디렉토리에 섞여 있음)
- **engine/ 살아있는 4개**: `algo_detector.py`·`auction_scanner.py`·`limit_up_paper_trader.py`·`paper_portfolio.py`
- **strategies/ 살아있는 20개**: base_strategy·bargain_scanner·composite·crisis_etf_signal·dynamic_target·equal_level_detector·etf_filter·five_min_scalping·gap_support·ma_crossover·macd_zero_scanner·momentum_scanner·opening_range·position_advisor·premium_levels·scanner·trend_breakout·trix_divergence·volume_spike·**body_hunter_v2**
- **risk/ 살아있는 3개**: daily_guard·risk_manager·drawdown_shield

---

## 다음 단계 (사장님 결정)
- 이 README(표기)는 1차 안전 조치. **아카이브 디렉토리 격리**(파일 이동·git 복원가능)는 사장님 결정 후 별도 진행.
- 격리 전 `backtest/backtester.py`·`risk/position_sizer.py`는 라이브 호출 여부 정밀 재확인 필요(Explore가 "조건부"로 분류).

**불변식**: 이 작업은 표기만 — 봇 OFF·실주문 0·매도 무손상·라이브 코드 무변경.
