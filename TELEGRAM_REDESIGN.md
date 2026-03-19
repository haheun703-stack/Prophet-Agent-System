# TELEGRAM_REDESIGN.md
# 단타봇 텔레그램 메시지 재설계 체크리스트
#
# 사용법: Claude Code 세션 시작 시 이 파일을 읽고,
#         [ ] 미완료 항목 중 가장 위의 것부터 진행.
#         완료되면 [x]로 체크하고 커밋.
#
# 목표: 하루 35개+ → 5~7개
# 원칙: 기존 job 로직은 유지. send_message() 호출만 제어.
#       데이터 수집/계산은 그대로 돌림. 텔레그램 전송만 정리.

---

## MSG-01: 로그 시스템 구축 (기반)
상태: [x]
파일: 신규 `bot_logger.py` + telegram_bot.py
구현: bot/bot_logger.py 신규 생성 — log_event(), read_today_log(), cleanup_old_logs()
     24개 카테고리 + JSON 직렬화 + 30일 자동 로테이션
     telegram_bot.py에 import + startup 시 cleanup 호출

---

## MSG-02: 모닝 브리프 통합 메시지 (08:50)
상태: [x]
파일: telegram_bot.py
구현: 이전 세션(cea5552)에서 _send_morning_brief() 구현 완료.
     모닝 job들(_job_global_event_scan, _job_options_expiry_alert,
     _job_position_guardian, _job_premove_scan) 이미 Silent 패턴.
     이번 세션: 각 job에 log_event() 호출 추가.

---

## MSG-03: 매수 체결 알림 통합
상태: [x]
파일: telegram_bot.py (_send_alert 래퍼)
구현: _send_alert 콜백에 키워드 필터링 래퍼 적용.
     체결/긴급 키워드(✅ 자동 매수, ❌ 매수 실패, ⛔ 등)만 텔레그램 전송,
     나머지(스캔 시작, 후보 로드, 조건 체크 등)는 log_event만.
     _on_startup + cmd_auto_start 양쪽 모두 적용.

---

## MSG-04: 매도 체결 알림 통합
상태: [x]
파일: telegram_bot.py (_send_alert 래퍼)
구현: MSG-03과 동일한 _send_alert 필터링 래퍼로 처리.
     매도 체결/청산 키워드 포함 시에만 텔레그램 전송.

---

## MSG-05: 긴급 알림 시스템
상태: [x]
파일: telegram_bot.py
구현:
  - _job_war_startup: Silent (log_event만)
  - _job_war_tracker: SL_BREAK + -5% 급락만 텔레그램 🚨 [긴급], 나머지 log_event
  - _job_war_summary: Silent (log_event만, /ㅍ로 수동 조회)
  - _job_portfolio_alert: -5% 급락만 텔레그램 유지 + 전 알림 log_event 추가
  - BRAIN 긴급 하향: 텔레그램 유지 + log_event 추가

---

## MSG-06: 프리클로즈 리포트 통합 (14:50)
상태: [x]
파일: telegram_bot.py
구현: 이전 세션(cea5552)에서 _send_preclose_brief() 구현 완료.
     _job_preclose_report는 Silent로 변환 (log_event만).
     _send_preclose_brief가 14:50에 통합 메시지 전송.

---

## MSG-07: 일일 마감 리포트 통합 (16:45)
상태: [x]
파일: telegram_bot.py
구현: 이전 세션(cea5552)에서 _send_daily_closing() 구현 완료.
     장후 job들 이미 Silent 패턴.
     이번 세션: 각 job에 log_event() 호출 추가.

---

## MSG-08: 온디맨드 명령어 추가
상태: [x]
파일: telegram_bot.py
구현: 이전 세션(cea5552)에서 cmd_port(/ㅍ), cmd_brain_status(/뇌),
     cmd_watchlist(/감시) 구현 완료.
     이번 세션: cmd_event_log(/ㄹ, /이벤트로그) 추가 — read_today_log() 사용.

---

# 적용 후 하루 메시지 흐름
#
# 08:50  모닝 브리프 (1개)              ← MSG-02
# 장중   매수 체결 (0~2개)              ← MSG-03
# 장중   매도 체결 (0~2개)              ← MSG-04
# 장중   긴급 알림 (보통 0개)           ← MSG-05
# 14:50  프리클로즈 리포트 (1개)        ← MSG-06
# 16:50  일일 마감 리포트 (1개)         ← MSG-07
# 수시   온디맨드 (/ㅍ /ㅂ /ㄱ /ㄹ)    ← MSG-08
#
# 합계: 3~7개/일 (기존 35개+)

---

# 완료 기록
# 2026-03-19 MSG-01 완료 — bot_logger.py 신규 생성 (log_event + 30일 로테이션)
# 2026-03-19 MSG-02 완료 — 모닝 job들 log_event 추가 (_send_morning_brief 이미 존재)
# 2026-03-19 MSG-03 완료 — _send_alert 키워드 필터링 래퍼 (체결만 텔레그램)
# 2026-03-19 MSG-04 완료 — MSG-03과 동일 래퍼로 매도 체결만 전송
# 2026-03-19 MSG-05 완료 — war_startup/summary Silent, tracker SL+급락만 긴급
# 2026-03-19 MSG-06 완료 — _job_preclose_report Silent, _send_preclose_brief 통합
# 2026-03-19 MSG-07 완료 — 장후 job들 log_event 추가 (_send_daily_closing 이미 존재)
# 2026-03-19 MSG-08 완료 — /ㄹ (이벤트로그) 명령어 추가
