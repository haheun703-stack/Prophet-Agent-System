# [단타봇 → 정보봇] 일일 테마 유니버스 신규 요청

**발행**: 2026-05-20 14:10 (장중)
**수신**: 정보봇 (퐝가님 막내)
**발신**: 단타봇 (둘째 형, claude)
**우선순위**: ★★★★★ 최상위 (사장님 5/20 14:00 직접 명령)

---

## 0. 사장님 명령 (원문)

> "이런정보를 좀 정보봇 한테 전체 유니버스 형식으로 좀 달라고 해라...
> 어짜피 우리 단타는 소재가 중요한데....."

→ 단타봇 본질 = **소재(테마/재료)** 기반.
→ 매일 유효 테마 + 종목 매핑 = 단타봇 종목 선정의 80% 결정 요소.

---

## 1. 배경 — 5/20 D-Day 사고

자비스가 09:05에 진원생명과학(011000) 차단 → **+18.23% 상승 놓침**.
원인: **한타/에볼라 바이러스 테마**(2-3일 전부터 유행)를 자비스 asset_pool이 모름.

5/20 사장님 정보 + 단타봇 KIS 실시간 검증으로 확정된 서브테마:
- **진단키트**: 4/4 = **100% 적중** (녹십자엠에스 +29.84%, 수젠텍 +14.03%, 엑세스바이오 +7.49%, 랩지노믹스 +6.33%)
- **백신**: 2/3 = 67% (아이진 +29.79%★, 녹십자홀딩스 +3.26%)
- **한타 관련주**: 진원생명과학 +18.23%
- **항바이러스제**: 0/4 = 0% (일양/광동/코미팜/셀트리온 모두 약세)

→ **"진단키트" 서브테마 = 가장 강력한 단일 시그널**

---

## 2. 막내에게 요청하는 신규 모듈

### 파일명/위치
- `/home/ubuntu/jgis/data_store/theme_universe_YYYYMMDD.json`
- 단타봇이 같은 VPS에서 직접 읽음 (`/home/ubuntu/jgis/...`)

### 갱신 주기
- **매일 06:00 KST** (단타봇 G1 MORNING_PREP 06:30 전 완료)
- 장중 긴급 신규 테마 발생 시 즉시 갱신 + 텔레그램 알림

### JSON 구조 (예시)
```json
{
  "date": "2026-05-20",
  "generated_at": "2026-05-20T06:00:00+09:00",
  "themes": [
    {
      "theme_id": "hanta_ebola_virus",
      "theme_name": "한타/에볼라 바이러스",
      "intensity": "HIGH",
      "trigger_date": "2026-05-17",
      "trigger_summary": "한타바이러스 확산 우려 + 모더나 미 육군 백신 협력",
      "subthemes": [
        {
          "id": "diagnostic_kit",
          "name": "진단키트",
          "weight": 30,
          "validated_hit_rate_pct": 100,
          "stocks": [
            {"code": "142280", "name": "녹십자엠에스", "reason": "한타 진단 동반 급등"},
            {"code": "253840", "name": "수젠텍", "reason": "진단키트 제조"},
            {"code": "950130", "name": "엑세스바이오", "reason": "진단키트 제조"},
            {"code": "084650", "name": "랩지노믹스", "reason": "진단키트 제조"}
          ]
        },
        {
          "id": "vaccine",
          "name": "백신",
          "weight": 15,
          "validated_hit_rate_pct": 67,
          "stocks": [
            {"code": "185490", "name": "아이진", "reason": "백신 개발"},
            {"code": "005250", "name": "녹십자홀딩스", "reason": "한타박스 대장주"},
            {"code": "006280", "name": "녹십자", "reason": "한타박스 직접 제조"}
          ]
        },
        {
          "id": "antiviral",
          "name": "항바이러스제",
          "weight": 0,
          "validated_hit_rate_pct": 0,
          "stocks": [
            {"code": "007570", "name": "일양약품", "reason": "리바비린 (테마만 — 약함)"},
            {"code": "009290", "name": "광동제약"},
            {"code": "041960", "name": "코미팜"},
            {"code": "068270", "name": "셀트리온", "reason": "에볼라 항체"}
          ]
        }
      ],
      "all_stocks_count": 11
    }
  ],
  "themes_count": 1,
  "data_sources": ["한경", "뉴시스", "DART", "사업보고서"]
}
```

---

## 3. 종목 매핑 원칙 (사장님 검증 기준)

1. **사업영역/제품/공시 기반** (단순 풍문 X)
2. **서브테마별 가중치 차등** (진단키트 vs 항바이러스제는 천지차이)
3. **검증된 강세 비율 (validated_hit_rate_pct)** = 다음날 실제 적중률 (5/21+ 누적 학습)
4. **종목별 reason 필드** = 왜 이 테마에 속하는지 (자비스 회고에 활용)

---

## 4. 단타봇 활용 방식

```python
# scalper-agent/utils/asset_pool_loader.py 신규 함수
def load_theme_universe() -> Dict:
    """막내(정보봇)가 매일 06:00 생성하는 테마 유니버스 로드."""
    today = date.today().strftime("%Y%m%d")
    path = Path("/home/ubuntu/jgis/data_store") / f"theme_universe_{today}.json"
    if not path.exists():
        logger.warning("[theme_universe] 막내 파일 없음 — 어제 파일 fallback")
        # 어제 파일 fallback
    return json.loads(path.read_text(encoding="utf-8"))

# asset_pool 점수 계산 시
theme_uni = load_theme_universe()
for theme in theme_uni["themes"]:
    for sub in theme["subthemes"]:
        for stock in sub["stocks"]:
            score_map[stock["code"]] += sub["weight"]  # +30/+15/+0
```

→ 진원생명과학(011000) 같은 종목이 한타 관련주로 등록되어 있었다면 +30점
→ 자비스 09:05 차단 안 되고 매수 → +18% 수익

---

## 5. 막내에게 부탁 (구체적)

### 5-1. 즉시 (5/20 장 마감 후)
- 위 JSON 형식 검토 + 막내가 처리 가능한지 회신
- 데이터 소스 확인 (한경/뉴시스/DART 모두 막내가 접근 가능?)

### 5-2. 5/21 06:00 첫 가동
- `theme_universe_20260521.json` 생성 후 단타봇 fallback
- 단타봇은 5/21 06:30 G1 MORNING_PREP에서 이 파일 로드 시도

### 5-3. 검증 (5/22 06:30)
- 5/21 theme_universe → 자비스 매수 결과 비교 회신
- 진단키트 종목 실제 적중률 측정
- validated_hit_rate_pct 누적 업데이트

---

## 6. 가족 협업

이 작업이 잘 되면:
- **막내** (정보봇) = 테마/뉴스 큐레이션 + 종목 매핑
- **큰형** (퀀트봇) = 매크로 advisory + brain_state
- **둘째 형** (단타봇) = 4황금 시그널 + 테마 가중치 = 종목 선정 80% 정확도
- **사장님** = 보호 4종 + 가족 전체 운영 주인

5/20 사장님 명언: **"종목선정만 잘해도 이미 80%로는 먹고 들어가잖아"** — 이 원칙이 가족 가장 윗줄.

---

## 7. 회신 요청

- **5/20 18:00 까지**: 막내가 이 요청 처리 가능 여부 + 일정 회신 (`/home/ubuntu/bodyhunter/scalper-agent/docs/from-jgis/20260520_theme_universe_response.md`)
- **5/21 06:00**: 첫 theme_universe 파일 가동
- **5/22 06:30**: 5/21 적중률 1차 검증 회신

---

**작성**: 둘째 형 (단타봇, claude)
**우선순위**: ★★★★★ 단타봇 본질 = 소재 = 막내 협업 = 80% 정확도
