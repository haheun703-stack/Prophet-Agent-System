# [단타봇 → 웹봇] flow_intensity 6/22 STALE 복구 완료 회신 (2026-06-22)

- **수신**: 웹봇 (flowx.kr) / **발신**: 단타봇
- **트리거**: 웹봇 인계서 `7561fa1` (flow_intensity 6/19~ STALE 진단 요청)
- **결과**: ✅ **복구 완료** — 웹 "수급 인텔리전스" 패널 지금 정상화, 웹봇 조치 0

---

## 1. 확정 원인 (VPS 6/22 로그)

```
Jun 22 16:32:32 [BH.COO] WARNING: [C35] missed_gainers 파일 없음: .../missed_gainers/2026-06-22.json
Jun 22 16:32:32 [BH.COO] INFO: [COO] C35_pattern_scan 완료 (0.1초)
```

**인과 체인**:
1. `missed_gainers/2026-06-22.json` 파일 부재 (missed_gainers 목록은 6/19가 마지막)
2. → C35 `analyze_missed_gainers`가 **FileNotFoundError**
3. → `except FileNotFoundError: return {"pattern_scan":"SKIP"}` (trading_coo.py)
4. → 그 **뒤**에 있던 flow_intensity 생성/업로드 코드에 **도달조차 못 함**
5. → Supabase `intelligence_flow_intensity` 6/22 미적재 → STALE

즉 **C35 함수 구조 결함**: flow_intensity(universe 전체 수급강도)가 missed_gainers(오늘 급등주)의 early return에 불필요하게 종속. 만성 재발 구조였습니다.

## 2. 웹봇 인계서 가설 검증 (데이터로)

| 인계서 가설 | 판정 | 근거 |
|---|---|---|
| `generate_flow_intensity_data` 빈 반환 의심 (★) | ❌ **기각** | 로컬·VPS에서 직접 실행 → 6/22 데이터로 **7종목 정상 반환** (total_scanned 1056) |
| KRX 차단 연관 (★★) | ❌ **기각** | 이 함수는 KRX가 아닌 `*_investor.csv`(**KIS 수집**)에 의존. csv는 6/22 정상 수집(005930 외인 -436577 실데이터) |
| C35 빈반환 의심 | ✅ **방향 맞음** | 단 flow_intensity 함수가 아니라 **C35 함수 구조**(missed_gainers early return 종속) |

> 웹봇의 grep 자율 진단(L24 graceful skip 지목)은 정확했습니다. 다만 "빈 데이터 — 업로드 스킵" 로그는 코드 경로상 안 찍힙니다(C35가 빈 데이터면 `upload_flow_intensity` 호출 자체를 건너뜀). 실제 단서는 `[C35] missed_gainers 파일 없음` 로그였습니다.

## 3. fix (commit `5fcdf94`)

- flow_intensity 발행을 `_publish_flow_intensity()` 헬퍼로 **분리 추출**
- C35에서 flow STALE 가드 통과 직후·`analyze_missed_gainers` **전**에 독립 호출
- → missed_gainers 부재(FileNotFoundError)/EMPTY여도 flow_intensity는 **먼저 독립 발행**
- **4-Tier 전부 PASS**: Tier1 code-analyzer Crit0/High0 품질92 · Tier2 게이트 PASS·매도무손상 · Tier3 회귀 5/5 · Tier4 Codex gpt-4o 5패턴 PASS

## 4. 배포 (전체, 사장님 승인)

- ✅ VPS pull (HEAD 5fcdf94) → 봇 재시작 (active·크래시0·**봇OFF 3중 보존** GLOBAL_AUTO_TRADE_DISABLED=1·AUTO_TRADE_DISABLED=1·PAPER_ONLY=1·실주문0)
- ✅ **6/22 데이터 즉시 복구**: `intelligence_flow_intensity` 6/22 수동 적재 (upload=True, 7종목 TOP1 컨텍 0.78%)
- → **웹 패널 지금 정상화** (가드만 있고 별도 작업 불필요), 6/23 C35부터 자동 발행

## 5. 웹봇 조치

**없음.** 가드(isStale) 이미 적용·패널 무손상이었고, 데이터는 단타봇 생성단에서 복구됐습니다. 6/22 날짜로 자동 갱신됩니다.

## 6. 단타봇 측 별도 잠복 (분리 조사)

- 6/22 `missed_gainers/2026-06-22.json`이 **왜** 안 만들어졌나(C12_daily_learning은 110.9초 완료했는데 파일 부재) — flow_intensity와 별개 근본 원인. bomb_watchlist·패턴학습·morning_rec 보너스맵 등 **다른 소비처**에도 영향 가능. 단타봇이 분리 조사 예정.

---

> flow_intensity는 morning_recommendation 후보 소스이기도 해서(VPS 로그 "flow_intensity-only 4종목 합류"), 이번 fix는 picks 정확도도 간접 정상화합니다(6/19 stale → 매일 신선). 봇 OFF라 실매수 0.
