# 🎯 5/28 PAPER 리허설 검증 계획서

> **작성**: 2026-05-27 단타봇 / **승인**: 사장님 / **실행**: 2026-05-28 (목)
> **목표**: 실주문 0건 + 5/27 사고 재발 방지 검증 + 6/1 D-Day 실전 준비
> **대상**: scalper-agent 단일 매매 봇 (quantum-scheduler masked / jgis 정보봇)

---

## 1. 🚦 시작 전 상태 확인 (5/28 08:00~08:55)

### 1.1 VPS 서비스 상태 검증
| 서비스 | 기대값 | 명령 |
|---|---|---|
| `bodyhunter-bot.service` | **inactive** (시작 전) | `systemctl is-active bodyhunter-bot.service` |
| `quantum-scheduler.service` | **masked** ★ | `systemctl is-enabled quantum-scheduler.service` |
| `jgis-scheduler.service` | **active** (정보봇 / 매매 X) | `systemctl is-active jgis-scheduler.service` |

### 1.2 VPS HEAD 검증
| Repo | 기대 HEAD |
|---|---|
| scalper-agent | `75a9114` 이상 |
| quantum-master | `340a7a9` 이상 |
| jgis | `25d701f` 이상 |

### 1.3 resume_preflight 실행
```bash
cd /home/ubuntu/bodyhunter/scalper-agent
python tools/resume_preflight.py --expect off
# 기대: status: PASS
```

### 1.4 로컬 노트북 / Startup 검증
| 항목 | 기대값 |
|---|---|
| 로컬 `run_bot.py` 프로세스 | 없음 |
| Windows Startup `BodyHunterBot.lnk` | 없음 (이미 삭제) |
| Task Scheduler 자동 매매 항목 | 없음 (3개 데이터 수집만) |

---

## 2. 🔧 런타임 설정 확인 (5/28 08:55~09:00)

### 2.1 kill_switch.json
```json
{
  "AUTO_TRADE_DISABLED": true,
  "manual_orders_allowed": true,
  "reason": "5/28 PAPER rehearsal — automated orders still blocked"
}
```

### 2.2 trade_runtime_config.json
| 설정 | 값 | 의미 |
|---|---|---|
| `PAPER_ONLY` | **true** | 실주문 차단 |
| `runtime_mode` | `paper_rehearsal` | |
| `AUTO_BUY_ENABLED` | **false** | 5/28은 OFF |
| `AUTO_SELL_ENABLED` | **false** | 5/28은 OFF |
| `VWAP_SPLIT_BUY_ENABLED` | **false** | 5/28은 OFF |
| `ENTRY_WATCH_ENABLED` | **false** | 5/28은 OFF |
| `ASSET_POOL_ENABLED` | **false** | 5/28은 OFF |
| `NXT_BUY_ENABLED` | **false** | 5/28은 OFF |
| `QUANT_ENABLED` | **false** | 5/28은 OFF |
| `max_auto_buy_orders_per_day` | 1 | |
| `max_order_amount_krw` | 300,000 | |
| `block_new_entry_after` | 14:40 | |

### ★ 5/28 리허설 — 전략 스위치 모두 OFF 유지 ★
- 5/28 = **가동 + 차단 리허설** (실주문 X / 시그널 + 차단 기록만)
- 실제 paper fill 생성은 5/29 최종 리허설에서 전략 스위치를 1개씩 ON 하며 별도 검증
- 전략 스위치는 5/29 최종 리허설에서 1개씩 ON 테스트 예정
- 6/1 D-Day = 1~2개 스위치 점진 ON

---

## 3. 🎯 리허설 범위 (5/28 09:00~15:30)

### 3.1 검증 항목 (실주문 0건)
| # | 항목 | 검증 방법 |
|---|---|---|
| 1 | **종목 선정** | asset_pool 후보 풀 생성 로그 확인 |
| 2 | **시그널 발생** | LimitUpEngine watchlist 갱신 / 동적유니버스 확인 |
| 3 | **주문 의도 기록** | `data_store/order_intents/order_intents_2026-05-28.jsonl` 적재 |
| 4 | **차단 리포트** | `[ORDER BLOCKED] AUTO_TRADE_DISABLED=True` 로그 누적 |
| 5 | **수동 보유 보호** | KIS 잔고 vs 메모리 audit |
| 6 | **체결강도 / 호가 / VI** | 정보봇 + KIS API 수집 정상 |

### 3.2 봇 가동 절차
```bash
# 08:55 사장님 명령 시
sudo systemctl start bodyhunter-bot.service

# 봇 시작 후 5분 모니터링
sudo journalctl -u bodyhunter-bot.service -f
# 기대 로그:
#   "kill_switch.json AUTO_TRADE_DISABLED=true loaded"
#   "PAPER_ONLY=true — automated orders blocked"
```

### 3.3 09:00~15:30 자동 가동 항목 (모두 PAPER)
| 시각 | 동작 | 실주문 |
|---|---|---|
| 09:00 | 봇 시작 / 어드바이저리 발신 | 0건 |
| 09:15 | asset_pool 매수 시도 | **PAPER 차단** ✅ |
| 09:01 | 룰 C (D+1 갭다운) 매도 시도 | **PAPER 차단** ✅ |
| 5분 반복 | `_check_entry_watch` | **AUTO_TRADE_DISABLED 차단** ✅ |
| 14:50 | 룰 D 매수 시도 | **PAPER 차단** ✅ |
| 15:10 | 데이 모드 청산 | **PAPER 차단** ✅ |
| 15:25 | 검증모드 청산 | **PAPER 차단** ✅ |
| 15:26 | 룰 B (+10% 절반 익절) | **PAPER 차단** ✅ |

---

## 4. 📋 필수 산출물 (5/28 16:00 회수)

| 산출물 | 위치 | 검증 |
|---|---|---|
| **order_intent 장부** | `data_store/order_intents/order_intents_2026-05-28.jsonl` | `allowed=false` 다수 / `allowed=true` 0건 |
| **blocked report** | systemd journal `bodyhunter-bot.service` | `[ORDER BLOCKED]` 카운트 |
| **리허설 로그** | `scalper-agent/logs/2026-05-28_bot.log` | 시그널 + 차단 흐름 |
| **KIS 체결/미체결 0건** | KIS API `inquire-daily-ccld` (5/28) | `output1: []` |
| **5/28 데이터 적재** | Supabase + jgis CSV | 종가/수급/국적별/CSV 4종 OK |
| **포지션 변화 0** | `data_store/positions.json` | KIS 잔고와 동일 (변화 없음) |

---

## 5. 🛡️ 5/27 사고 재발 검증 (5/28 15:35~16:00)

| 검증 항목 | 기대값 | 방법 |
|---|---|---|
| **로컬 run_bot.py 프로세스** | 없음 | `tasklist | findstr python` |
| **quantum-scheduler masked** | masked | `systemctl is-enabled quantum-scheduler.service` |
| **raw mojito bypass 차단** | PASS | `venv/bin/python -m pytest tests/test_no_raw_mojito_order_bypass.py` 또는 로컬 `36 passed` 결과 대조 |
| **Kiwoom direct guard 차단** | PASS | `test_kiwoom_order_kill_switch_guard` |
| **kill_switch fail-close** | PASS | `test_kill_switch_file_missing_fail_close` |
| **로컬 Startup 자동 시작** | 없음 | `Startup` 폴더 확인 (Ollama만) |
| **Git 밖 KISOpenAPI 4개** | fail-close 유지 | `grep ALLOW_LEGACY_KISOPENAPI_ORDERS` |
| **VPS 8개 진입점** | 모두 가드 | `grep is_auto_trade_disabled` 호출 site 확인 |

---

## 6. ✅ 종료 후 확인 (5/28 15:40~17:00)

### 6.1 봇 정상 종료
```bash
sudo systemctl stop bodyhunter-bot.service
# 5분 대기
sudo systemctl is-active bodyhunter-bot.service  # → inactive
ps auxf | grep run_bot.py | grep -v grep  # → 비어있음
```

### 6.2 사고 0건 검증
| 항목 | 기대값 |
|---|---|
| KIS 5/28 매수 체결 | **0건** |
| KIS 5/28 매도 체결 | **0건** |
| 포지션 변화 | **없음** (KIS 잔고 동일) |
| order_intent `allowed=true` (auto) | **0건** |
| order_intent `allowed=false` (auto) | **N건** (차단 정상 작동) |

### 6.3 5/28 데이터 적재 검증
| 데이터 | 기대 시각 | 검증 |
|---|---|---|
| stock_master (종가) | 16:54 | Supabase 2,500+건 |
| nationality_flow | 16:22 | Supabase 30개국+ |
| market_investor_trend | 16:22 | KOSPI + KOSDAQ |
| Daily CSV | 15:45 | updated > 0 |

### 6.4 5/29 최종 리허설 사전 점검
| 체크리스트 | 상태 |
|---|---|
| 5/28 사고 0건 | □ |
| 모든 가드 작동 검증 | □ |
| 사장님 영구 룰 14대 미위반 | □ |
| Codex 검수 통과 | □ |
| → 5/29 최종 리허설 진행 | □ |

---

## 7. 🚨 비상 절차 (5/28 사고 발생 시)

### 7.1 자동매수 발생 시 (가장 위험)
1. **즉시** `sudo systemctl stop bodyhunter-bot.service`
2. KIS 잔고 즉시 조회 + 매수 내역 확인
3. 사장님 텔레그램 즉시 보고
4. 매수 출처 추적 (단타봇 + Codex 협업)
5. Codex 안전장치 추가 패치

### 7.2 가드 우회 발견 시
1. 즉시 봇 정지 + quantum 다시 mask 확인 + jgis 정지
2. order_intent 장부 + KIS 체결 비교
3. Codex 협업 — 우회 경로 추가 가드

### 7.3 데이터 미적재 시
1. 5/27 패턴 적용 ([[reference_krx_nationality_pattern]])
2. _recover_* 4종 수동 트리거
3. 5/29 자동 적재 대기 또는 즉시 recover

---

## 8. 🎯 단타봇 사장님 약속

- ✅ 실주문 0건 보장 (12개 진입점 모두 가드)
- ✅ 사장님 통합 비전 유지 (매매 봇 1개 + 정보봇 1개)
- ✅ 사고 발생 시 즉시 보고 (추정 라벨 X / KIS 체결 기준)
- ❌ "박사" 단어 X / 옵션 남발 X / 미루기 X
- ✅ 5/27 사고 5건 학습 적용 ([[incident-2026-05-27-vwap-split-buy-local-runbot]])

---

## 9. 다음 단계 (5/28 리허설 통과 시)

| 일정 | 작업 |
|---|---|
| 2026-05-29 (금) | 최종 리허설 — kill_switch True/False 전환 테스트 / 전략 스위치 1개씩 ON |
| **2026-06-01 (월) D-Day** | **소액 제한 실전 재개** (종목당 10~30만 / 하루 1건 / 14:40 cutoff) |

---

**작성자**: 단타봇 (5/27 사고 회고 후)
**검수**: 사장님 + Codex
**최종 승인 대기**
