# [단타봇 → 웹봇] 매매포인트(theme_relay) 한국스윙 적재 요청 (2026-06-22)

사장님 지시: "theme_relay 매매포인트를 **매일 한국스윙 페이지 '매매포인트' 탭(왼쪽)**에 기록. 퀀트봇도 그렇게 한다."

## 1. 단타봇이 한 것 (발행 측 — 완료)
- `data/theme_relay_shadow.json`(record-only 관측)에서 **테마 relay-aware 매매포인트**를 생성:
  `data/theme_trading_points.py` → `build_trading_points()`.
- `data/upload_swing.py`의 `upload_dashboard_swing()` row에 **`trading_points` (JSONB)** 추가.
  → 한국스윙 대시보드 업로드 돌 때마다 `dashboard_swing.trading_points`에 **매일 자동 발행**.
- ★ 컬럼 미존재 시 단타봇 upsert는 fallback으로 `trading_points`를 graceful 제거(기존 발행 무손상). **웹봇이 컬럼 추가하면 그날부터 적재 시작.**

## 2. 웹봇이 할 것 (렌더 측 — 요청)
1. **Supabase `dashboard_swing` 테이블에 `trading_points` JSONB 컬럼 추가** (없으면 단타봇이 graceful skip 중).
2. **한국스윙 페이지 '매매포인트' 탭(왼쪽)에 렌더** — `dashboard_swing.trading_points` 사용.

## 3. trading_points 스키마
```json
{
  "date": "2026-06-22",
  "generated_at": "2026-06-22 18:xx:xx",
  "strong_count": 2,
  "themes": [
    {
      "theme": "반도체",
      "strength": 3.83,                 // 테마 전체 5일 모멘텀(%)
      "leader_group": "LEADERS",        // 또는 "SUPPLY_CHAIN"
      "leader_label": "대장주",          // 또는 "소부장"
      "leaders_strength": 25.46,        // 대장주 group 5일(%)
      "supply_strength": -11.62,        // 소부장 group 5일(%)
      "reversal": true,                 // 바통(주도그룹 역전) 발생
      "weak": false,                    // 약세 테마(주도그룹도 마이너스) — true면 회피
      "early_candidates": [             // 주도그룹 초입 후보(pos20<0.5)
        {"code": "000660", "name": "SK하이닉스", "pos20": 0.34, "ret5": 12.3}
      ],
      "comment": "대장주 주도 · 바통 전환(주도그룹 역전) · 초입 후보 ..."
    }
  ],
  "note": "테마 relay-aware 매매포인트(관측) — paper shadow이며 실매수 신호 아님."
}
```

## 4. 렌더 가이드 (제안 — 웹봇 재량)
- **정렬**: `themes`는 이미 강한 테마 먼저(약세 후순위) 정렬됨. 그대로 표시.
- **테마 카드**: 테마명 + 주도그룹 뱃지(`leader_label`: 대장주/소부장) + 강도. `reversal=true`면 "🔄 바통" 표시.
- **초입 후보**: `early_candidates` 종목칩(이름·pos20). 주도그룹에서 바닥권(pos20 낮음) 종목.
- **약세 테마**(`weak=true`): 회색/접기 + "회피" 표기.
- ★ **상단/툴팁에 "paper 관측 — 실매수 신호 아님"** 명시(`note`). 검증 후 사장님 결정으로 실매매 반영 예정.

## 5. 데이터 의미 (배경)
- relay는 **테마마다 다름**(반도체=소부장→대장주 바통 / 자동차=소부장 지속 / 방산=대장주 지속 / 로봇·ESS=약세). 고정 아님 — 매일 강도로 관측.
- 6/22 현재: 반도체(대장주 주도) 강함, 나머지 약세권. (`tools/multi_theme_relay_6_22.py` 근거.)
- 발행 주기: 단타봇 한국스윙 업로드 시(매일). theme_relay_shadow는 nightly ③-4(VPS 18:00)가 갱신.

## 6. 확인 요청
- `trading_points` 컬럼 추가 가능한지 / 매매포인트 탭 렌더 가능한지 회신 부탁.
- 컬럼명·스키마 바꿔야 하면 단타봇이 맞추겠음(upload_swing 측 1줄).
