# 🌅 5/18 (월) TODO — 자비스 1.0 첫 가동일

**작성**: 2026-05-17 (일요일 저녁)
**자동 작동 시작**: 5/18 09:00 KST

---

## 📋 사장님 점검 — 시간순 actionable

### [08:30 KST 전] 사전 점검 (5분)

```bash
# VPS 봇 active 확인 (텔레그램 봇이 살아있는지)
ssh -i _tmp_zips/lightsail_60gb.pem ubuntu@13.209.153.221 "sudo systemctl is-active bodyhunter-bot"
# 기대: active
```

확인 항목:
- [ ] 봇 active 상태 OK
- [ ] 텔레그램 봇 응답 OK (텔레그램에 `/status` 명령)
- [ ] KIS 잔고 충분한지 (검증 모드는 가용 현금의 30% 사용, 최대 ~23종목)

### [09:00 KST] 첫 자동 가동 확인 (텔레그램 알림 받기)

- [ ] **💰 [검증모드 예산] 가용 현금 XX,XXX원 × 30% = YY,YYY원** 알림 도착
- [ ] **🧪 [검증모드 매수 결과] ✅ 성공: N/13종목** 알림 도착
- [ ] 매수 종목 중 한미반도체/셀트리온 같은 ETF 주도주 보너스 (+13) 표시 확인

**문제 발생 시**:
- 매수 0종목 → KIS 잔고 부족 가능성, `fetch_balance` 결과 확인
- 매수 일부만 → "💸 예산 초과 스킵 N" 알림 정상 (예산 한도 작동)

### [09:05~14:00 KST] 장중 멀티시그널 진입 알림

- [ ] **🚀 [장중 멀티시그널 진입] N종목 발견 — 1주씩 매수 진행** 알림 받기
  - 후보 0건이면 알림 없음 (정상 — 5분 스캔이지만 폭주 차단)
  - 일일 최대 10종목 추가
- [ ] tipping_score 45개 후보 + 체결강도 150+ + 거래량 TOP 30 동시 충족 = 진입

### [15:25 KST] 청산 알림

- [ ] **🧪 [검증모드 청산 결과] ✅ 청산: N/N종목** 알림 도착
- [ ] **🚨 청산 실패 N종목 (3회 재시도 후)** 알림 있으면 → KIS HTS에서 수동 매도

### [15:35 KST] 정산 리포트 (오늘 가장 중요)

- [ ] **📊 [검증모드 2026-05-18 정산]** 텔레그램 도착
- [ ] 평균 PnL / 승률 / TOP 3 / BOTTOM 3 / 시그널별 적중률 확인
- [ ] 데이터 캡처해서 저장 (5/19 비교용)

---

## 🛠 우리(claude) 측 작업 — 5/18 처리할 것

### 우선순위 1 — 5/18 검증 데이터 확인 (저녁)

- [ ] 검증 1일차 결과 분석
  - `scalper_verification_log` 30+ 종목 조회
  - `scalper_trade_journal` source별 (morning_rec vs intraday_scan vs verification) 비교
- [ ] 시그널 적중률 초기 트렌드 보고
- [ ] tipping 시그널 합류 종목이 실제로 진입했는지 검증

### 우선순위 2 — 5/19 사이클 + Medium/Low 보강

- [ ] **M8. CFO 정책 검토** — 사장님 자금 운용 신중성 vs 검증 모드 23종목 자동
  - 정책 정합성 확인 + 필요 시 한도 조정 (30% → 20%?)
- [ ] **M9. EVALUATOR 일관성** — verification v1+v2 통합 검수 (Critical/High/Medium 분류)
- [ ] **M10. MEMORY.md 200줄 초과** — 인덱스 정리 (오래된 세션 → archive)
- [ ] **L13. INFO 로그 잔존** — 5/18 운영 로그 보고 추가 logger.debug 전환

### 우선순위 3 — 5/20 본격 진입 준비 작업

- [ ] **NXT 자동매매 활성화 코드 준비** (`bot/trading_coo.py` _job_nxt_auto_buy 신규)
  - morning_rec preview(16:45) → 17:00 NXT 자동 진입
  - NXT eligible 필터(5/4 8d88d09) 재활용
- [ ] **WebSocket 활성화 준비** (`.env` auto_trade=true 시점 결정)
- [ ] **일반 매매 hook 추가** — trade_journal log_buy/log_sell 호출 10여 곳
  - smart_buy/smart_sell, buy_market/sell_market
  - NXT 매수/매도, 선취매(predawn), 분할 익절
- [ ] **max_auto_positions 조정 결정** — 5/18~5/19 검증 결과 기반
  - PnL > +0.5% → 5 / 0~+0.5% → 3 / ≤ 0% → 가중치 재조정

---

## 📦 오늘(5/17) 못한 거 — 명시적 분리

### 별도 PDCA 사이클로 분리 (~1주일 후)

- [ ] **L11. intraday_scanner 단위 테스트** — 5/18 실측 데이터 누적 후 mock 작성
- [ ] **L12. 점수 공식 백테스트** — `min(100, strength/10 + tipping/2 + 11)` 검증 (5/25+ 1주 데이터 후)
- [ ] **5일선 회귀 → morning_recommendation +10점 통합** (AUDIT_BACKLOG §1 기술부채)
- [ ] **Pool 모니터링 + telegram alert** (AUDIT_BACKLOG §1)
- [ ] **DATABASE_URL 회전 자동 감지** (AUDIT_BACKLOG §1)
- [ ] **pension_grade 변경 알림 DB push 검토** (AUDIT_BACKLOG §1)
- [ ] **AUDIT_BACKLOG §1 C-2 정공법** — Supabase scalper_readonly + scalper_writer role 분리 (5/17 세션(2) 미뤘던 항목)

### 5/20+ 본격 활성화 (검증 결과에 따라)

- [ ] NXT 자동매매 ON (5/21+)
- [ ] WebSocket ON (5/21+)
- [ ] **FLOWX 대시보드 페이지 구현** — [docs/FLOWX_TRADE_JOURNAL_SPEC.md](FLOWX_TRADE_JOURNAL_SPEC.md) 사장님이 웹봇에 전달
  - 1단계 (5/19~5/20): KPI 5장 + Timeline
  - 2단계 (5/21+): Equity Curve + 시그널 도넛
  - 3단계 (5/25+): 일별 PnL + TOP/Bottom
  - 4단계 (5/30+): 자동 갱신 + 모바일 반응형

---

## 🚨 사고 대응 시나리오 (대비)

| 증상 | 대응 |
|---|---|
| 매수 0종목 + "잔고 조회 실패" | 봇 재시작 (KIS 토큰 만료 가능) — 토/일이 아니라 평일이라 위험. 사장님 판단 후 결정 |
| 매수 1~5종목만 (예산 한도) | 정상 — "💸 예산 초과 스킵" 알림 확인 |
| "🚨 청산 실패 N종목" | KIS HTS에서 수동 매도 + 텔레그램 봇에 `/sell <code>` 명령 |
| 텔레그램 알림 안 옴 | `sudo journalctl -u bodyhunter-bot --since '5 minutes ago' \| grep ERROR` |
| 장중 진입 0건 | 정상 — 멀티시그널 3개 동시 충족 조건이 보수적. 후보 발견 시만 알림 |
| 봇 다운 (active = inactive) | `sudo systemctl restart bodyhunter-bot` (장중이면 [규칙 -2] 위반이라 신중) |

---

## 📊 오늘 17개 커밋 정리 (참고)

| # | 커밋 | 내용 |
|---|---|---|
| 1 | `a93f516` ~ `5623c09` | ETF Step A/B/C + SQL 자동화 v1 (5개) |
| 2 | `249ae1a` ~ `fbfd23c` | (B+C) 보너스/cron + M-1 retry + AUDIT (5개) |
| 3 | `379cbb6` | KIS API 감리 + NXT 자동 발견 + 5/20 TODO |
| 4 | `aa6887b` | 검증 모드 v1 (1주 실전, 사장님 제안) |
| 5 | `d54e28a`, `c5cafed` | 실전매매 일지 + FLOWX 지시서 |
| 6 | `eaa5806` | 검증 모드 v2 (장중 멀티시그널) |
| 7 | **`a9a1211`** | **자기반성 — Critical 4건 + High 1건 실측 보강** |

---

## 💡 사장님 5/18 첫 가동 한 줄 요약

> "08:30 봇 active 확인 → 09:00~15:35 텔레그램 알림 4번 받기 → 정산 데이터 캡처. 사고 시 사고대응 시나리오 표 참조. 끝."

**자비스 1.0 첫 가동 — 5/18 09:00 KST. 화이팅 퐝가님 🤖**
