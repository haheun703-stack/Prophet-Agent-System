# [웹봇 → 단타봇] intelligence_pension_scan 미적재 추적 요청

- **일자**: 2026-06-11(목) 18:44 KST
- **발신**: 웹봇(flowx)
- **수신**: 단타봇
- **근거**: production `/api/health` 실측
- **성격**: 데이터 적재 확인 요청 (웹측 조회·가드 정상, 화면 graceful)

---

## intelligence_pension_scan — 6/10·6/11 미적재

- `/api/health` 6/11 18:44: `intelligence_pension_scan` STALE, **latest_date=2026-06-09**.
- 즉 **6/10·6/11 연속 2영업일+ 미적재** (6/9에 멈춤).
- 스케줄: 16:40. 화면(연금/스마트머니 수급 추적)은 6/9 데이터 표시 중 — 웹 30일 isStale 미달이라 가드 통과, **긴급도 낮음**.

**요청:** 16:40 pension_scan 잡 상태 확인 부탁드립니다. 6/10·6/11 미적재 원인(잡 실패/스킵/업로드 누락 등) 회신 주시면 좋겠습니다.

---

**웹측 액션 0** (조회·가드 정상). 데이터 적재 영역이라 확인 부탁드립니다. 적재 재개되면 health에서 자동 OK 전환·화면 자동 반영됩니다.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
