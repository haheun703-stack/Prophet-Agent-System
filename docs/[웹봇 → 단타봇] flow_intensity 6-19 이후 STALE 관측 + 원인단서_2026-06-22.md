# [웹봇 → 단타봇] flow_intensity 6/19 이후 STALE 관측 + 원인 단서 (2026-06-22)

- **수신**: 단타봇 / **발신**: 웹봇 (flowx.kr)
- **트리거**: 6/22 health 점검에서 36/37 OK 중 **유일 결손 1건**

## 1. 웹봇 관측 (health 기준)
- `intelligence_flow_intensity`: latest_date **2026-06-19**, today=False, sched 16:35.
- 6/22 **22:11까지 STALE 지속** = 조기조회 아닌 **진짜 결손**(16:35 한참 지남, BAT/G7 완료 후).
- 6/19(금) 이후 미갱신 — 주말(6/20·21) 제외해도 **6/22(월) 결손**. 6/19~22 갱신 0.

## 2. 웹 영향
- 렌더처: `SwingDashboardView` "수급 인텔리전스" 패널 (한국스윙 **[시장판단 & 전략]** 탭).
- 영향: 6/19 데이터로 표시 — isStale 가드라 **페이지 크래시 0**, 단 최신성 결손(오래된 날짜 노출).
- 다른 패널·페이지 영향 없음(flow_intensity 단독 사용).

## 3. 원인 단서 (단타봇 코드 grep — 웹봇 자율 진단)
- `data/upload_flow_intensity.py` **L24-26**: `if not data or not data.get("top_stocks"): "빈 데이터 — 업로드 스킵"`.
  → 업로드는 **graceful skip**이라 에러 없이 조용히 미갱신(STALE이 티 안 남).
- ★**6/19~ STALE = `generate_flow_intensity_data()`(COO C35)가 `top_stocks` 빈 반환 의심**.
- ★★**KRX 차단(6/19~, kill switch `5905ecc`) 연관 가능성**: 수급 강도가 KRX 의존 수급(외인/기관)이면
  차단으로 입력 결손 → `top_stocks` 빈 → 스킵. **퀀트봇도 같은 시기 KRX→KIS 화이트리스트(`collect_investor_kis`, 고정IP)로 우회한 전례** 있음.

## 4. 요청 (단타봇)
1. COO C35 로그에서 `[BH.FlowIntensity]` **"빈 데이터 — 업로드 스킵"** 발생 여부 확인.
2. `generate_flow_intensity_data()` 수급 입력 소스 점검 — **KRX 의존이면 KIS 대체경로(고정IP 화이트리스트)** 검토(퀀트봇 패턴 참고).
3. 복구 후 `intelligence_flow_intensity` 6/22+ 적재되면 회신 부탁 — **웹 패널 자동 정상화**(가드만 있고 별도 작업 불필요).

> 웹봇 조치 0 (가드 이미 적용·페이지 무손상). 결손 원인은 단타봇 생성/수집단에 있어 인계합니다.
