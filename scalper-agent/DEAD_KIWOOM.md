# ⚠️ DEAD-KIWOOM — 데드코드 제거 기록 + 라이브 오인 방지 (신규 세션/Codex 필독)

> **★ 7/1 2차 제거 완료 (사장님 "키움 삭제"·commit a1560fe):**
> 키움 데드 클러스터 + 데드 연구 **145개 삭제**. 라이브 import 0건 **2중 검증**(라이브 진입점
> transitive closure 243파일 + belt-and-suspenders grep) 후 삭제. git 이력 보존(복원 가능).
> 삭제: engine 5·strategies 13·risk 3·backtest 19+2·data고아 8·utils 4·output 1·root 3·tools 89.
>
> **★ 6/30 1차 제거**: `main.py` + `api/kiwoom_*.py` 5개 + `api/__init__.py`(키움 Open API v1.0).
>
> 이 파일 목적: 신규 세션/Codex/운영자가 **남은 라이브 코드를 데드로 오인**하거나 **보류 도구를
> 데드로 오인**하는 것을 막는다(6/24 검수 "최대 비용").

---

## ✅ 진짜 라이브 봇 (KIS·mojito) — 이것만 실제로 돈다
```
run_bot.py → bot/telegram_bot.py
  ├ bot/kis_trader.py (KIS REST·mojito·★키움 아님)·auto_trader.py·trading_coo.py·trading_cfo.py·trading_cto.py·portfolio_monitor.py
  └ data/ · engine/(라이브 4) · strategies/(라이브 다수) · risk/drawdown_shield · output/(send_4d_report·telegram_alert)
tools/run_nightly_pipeline.py (VPS cron 18:00) + 호출 러너 13종
```

## 🔒 삭제하면 안 되는 라이브 (같은 디렉토리에 섞여 있으니 데드 오인 주의)
- **engine/ 라이브 4**: `algo_detector`·`auction_scanner`·`limit_up_paper_trader`·`paper_portfolio` (나머지 5개 = trading_engine·body_hunter_master·market_state·order_manager·portfolio는 **삭제됨**)
- **strategies/ 라이브** (auto_trader·trading_coo·morning_recommendation가 import): `premium_levels`·`equal_level_detector`·`gap_support`·`macd_zero_scanner`·`dynamic_target`·`momentum_scanner`·`crisis_etf_signal`·`opening_range`·`trix_divergence`·`bargain_scanner` 등 (data 그래프가 못 잡던 것 = 7/1 검증에서 KEEP 확정)
- **risk/ 라이브 1**: `drawdown_shield` (bot/trading_cfo 사용). ★구 문서 "살아있는 risk 3개"는 오분류 — `risk_manager`·`position_sizer`·`daily_guard`는 실제 데드·**삭제됨**.
- **data/ 라이브**: jarvis_learning·reentry_watch·trade_journal·verification_mode·pension_finance_scan·trade_learner 등 (import 그래프 재추적으로 KEEP)
- **verifiers/ 전부 라이브** (trading_coo 사용): code_auditor·data_integrity·env_checker·flow_monitor·reporter·daily_self_audit
- **CRON 러너 (import 0건이나 스케줄/cron 배선)**: sync_from_vps·nationality_health_check·run_foreign_exh_late_recollect·collect_daily·data/daily_collector

## ⏸ 보류 — 운영 CLI/인프라 7개 (데드 아님·사장님 결정 대기)
`tools/manual_order.py`·`safe_sync_positions.py`·`check_balance.py`·`order_forensics.py`·`audit_order_paths.py` = 사장님(백업 매도자) 수동 운영 도구(import 0건은 CLI라 정상). `codex_auto_request.py`·`codex_collab.py` = **git pre-commit hook이 참조하는 Codex 인프라**(쉘 참조라 import 그래프가 못 잡음). → 삭제 시 별도 `tools/manual/` 격리 권장(사장님 판단).

---
**불변식**: 봇 OFF·실주문 0·매도 무손상·라이브 코드 무변경(삭제만). 데드 판정은 **정적 import + 동적 문자열 디스패치 + VPS cron + 쉘 hook** 4중 확인 필수(그래프만 보면 오판).
