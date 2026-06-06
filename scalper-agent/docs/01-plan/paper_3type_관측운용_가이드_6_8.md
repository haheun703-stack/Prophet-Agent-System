# 3-Type Paper Training — Forward 관측 운용 가이드 (6/8~6/12)

> 사장님 지시(6/6): **구현은 닫는다. 6/8~6/12 forward 관측 단계로 전환.**
> 단타봇은 이제 종목을 "찾았는지"가 아니라, **A1/A2/B/C 네 방식 중 무엇이 실제 장에서 돈 냄새가 나는지** 같은 장부로 비교한다.
> 후보 0건도 기록이다. A2가 A1보다 못하면 "뉴스는 소음"이라는 답이고, B/C가 좋으면 "섹터 로테이션이 본체"라는 답이다.

## 0. 측정 대상 (완성된 판)
| 타입 | 정의 | 신호 소스 |
|---|---|---|
| **A1** | STEADY + DART_S | sdart (DART 단독, primary) |
| **A2** | STEADY + DART_S + NEWS/EVENT | sdart + event_detector (보강 태그) |
| **B** | 강한 섹터/그룹 눌림 | rotation (-3/-5/-7%·ma지지) |
| **C** | 강한 섹터/그룹 올라타기 | rotation (+5/+7%·강한양봉·신고가) |

구현 커밋: B/C 7de72d2 · A연결/통합러너 eaaa8e6 · EVENT hook 6a32fce (전부 push).

## 1. 매일 수동 순서 (장 마감 후 또는 다음날 아침)
> ★ scheduler 배선 금지(승인 전). 매일 사람이 수동 실행. ★
> 작업 디렉토리: `scalper-agent/`

```bash
# ── 1단계: 데이터 최신화 ──
# (1-a) KIS 일봉 incremental → data_store/daily 최신화 (pykrx 막힘 대응, 네트워크)
python tools/fill_daily_kis_incremental.py
# (1-b) data_store/daily → stock_data_daily 병합 (네트워크 0)
python -c "import collect_all; collect_all.step6_sync_stock_data_daily()"

# ── 2단계: 3-Type 통합 스캔 + ledger 기록 ──
#   --scan-events : event_detector 실시간 스캔(DART+네이버, 네트워크)으로 A2(NEWS 보강) 판정
python tools/paper_3type_daily_run_6_6.py --asof <오늘날짜 YYYY-MM-DD> --scan-events
```

- `--scan-events` 빼면 A2 보강 없이 A1만(events.json stale 시 자동 A1).
- A(sdart)도 DART 네트워크 조회 → 전체 ~수 분 소요 가능.

## 2. 매일 확인 항목 (실행 후)
1. ✅ `data_store/paper_3type/ledger_{date}.json` **생성** 확인
2. ✅ 요약 출력: **A1 / A2 / B / C 후보 수**, 강한 섹터 수, EVENT hook fresh 여부
3. ✅ ledger 안에 각 후보의 **MFE / MAE / raw_fwd / position_size_pct / capital_allocated** 기록 확인
4. ✅ EVENT hook: `event_layer.matched`(A2) / `a_variant`(A1·A2) / `labels`(휴리스틱) 기록 확인
5. ✅ **안전 5점**: 실주문 0 / scheduler 0 / SAJANG 무변경 / 봇 OFF / picks·asset_pool 불변
   - 빠른 점검: `python tools/audit_order_paths.py` → overall PASS, order_path_real 0

★ **후보 0건도 정상 데이터**로 인정(빈 ledger도 기록). 0건인 날도 관측 표본.

## 3. 판정 기준 (6/12 — 1차 판정)
- ❌ **capital 절대금액 비교 금지** (A30 vs B/C35 base가 달라 불공정)
- ✅ 비교 지표: **position_size_pct · raw_fwd 수익률 · MFE/MAE · 후보 적중률**
- 6/12까지는 **1차 판정만**. 결과가 좋아도 **영구룰 확정 금지**.
- 좋은 타입은 **2주차 forward로 연장 검증**.

## 4. 특별 주의 (사장님 6/6)
- ★ **A2가 좋아도 NEWS/EVENT를 hard gate로 승격 금지** — 뉴스는 명분 보강 태그, 매수 버튼 아님.
- 뉴스는 **fresh한 당일 이벤트만** 보강 태그로 인정. **stale 뉴스는 A1**로 처리(NEWS 백필 불가 전제).
- 정보봇 novelty 라벨이 없으면(현재 미생성) **휴리스틱 라벨임을 기록**(EVENT_TYPE/THEME/direction 근사).
- 관측 없이 **flip(선정 변경) 금지**.

## 5. 별도 작업 (관측판과 분리)
- `tools/pre_commit_check.py` IMP-001 오탐 수정(noqa 라인·함수내 lazy import·`from __future__ import annotations` 미사용 오판)은 **지금 관측판과 분리**. 관측 첫 주는 **러너/ledger 안정성 우선**.

## 6. 정직 한계
- B/C·A2 백테 근거 없음(A는 45건) → **forward 자체가 1차 검증**. 절대수익 신뢰 X.
- 7거래일(6/4·8·9·10·11·12) = thin. "1차 판정"이지 확정 아님.
- A 실데이터는 sdart 공시목록이 forward 기반이라 과거 백필 불가 → forward에 신고가+DART호재 종목이 나와야 A 기록됨.
