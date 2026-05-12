# ============================================
# FLOWX 단타봇 — 미국장 연동 통합 안내
# ============================================

## 파일 배치

단타봇 루트 폴더에 아래 3개 파일 추가:

  단타봇/
  ├── us_overnight_filter.py    ← 갭 예측 + 진입 모드
  ├── us_relay_map.py           ← US→KR 릴레이 매핑
  └── upload_daytrading_us.py   ← Supabase 업로드


## Supabase 테이블 생성

Supabase SQL Editor에 아래 붙여넣기:

  CREATE TABLE daytrading_us_overnight (
    id            BIGSERIAL PRIMARY KEY,
    date          DATE NOT NULL UNIQUE,
    mode          TEXT,
    gap_signal    TEXT,
    gap_est_pct   NUMERIC,
    soxx_alert    BOOLEAN,
    risk_level    INT,
    risk_score    INT,
    watch_sectors JSONB DEFAULT '[]',
    avoid_sectors JSONB DEFAULT '[]',
    relay_picks   JSONB DEFAULT '[]',
    reasons_bad   JSONB DEFAULT '[]',
    reasons_good  JSONB DEFAULT '[]',
    reason        TEXT,
    nasdaq_change NUMERIC,
    soxx_change   NUMERIC,
    vix           NUMERIC,
    dxy           NUMERIC,
    us_3y_yield   NUMERIC,
    fear_greed    INT,
    fear_greed_label TEXT,
    kr_impact     TEXT,
    risk_flags    JSONB DEFAULT '[]',
    created_at    TIMESTAMPTZ DEFAULT NOW()
  );


## cron 스케줄

  # 08:05 — 정보봇 수집(08:00) 완료 후 단타봇 분석 실행
  5 8 * * 1-5 cd /path/to/단타봇 && python upload_daytrading_us.py

  # 정보봇 cron (참고)
  0 8 * * 1-5 cd /path/to/정보봇 && python us_market_collector.py


## 기존 단타봇 코드 연동 방법

단타봇의 아침 루틴 (morning() 함수 또는 daily cron)에 아래 추가:

  # us_overnight_filter 결과를 읽어서 진입 모드 적용
  from us_overnight_filter import run as get_us_mode

  us_report = get_us_mode()
  MODE = us_report.get("mode", "NORMAL")   # AGGRESSIVE/NORMAL/DEFENSIVE/HALT

  if MODE == "HALT":
      # 오늘 진입 금지
      send_telegram("🔴 진입 HALT — 미국장 극단 위험")
      exit()

  elif MODE == "DEFENSIVE":
      # 조건 강화
      MIN_GRADE = "A+"     # 기존 A → A+로 상향
      STOP_LOSS = -1.5     # 기존 -2.0 → -1.5로 타이트
      MAX_SLOTS = 2        # 기존 3 → 2로 축소

  elif MODE == "AGGRESSIVE":
      # 조건 완화
      MIN_GRADE = "A"      # 기존 A → A 유지 (빠른 진입)
      STOP_LOSS = -2.5     # 기존보다 여유
      MAX_SLOTS = 4        # 기존 3 → 4로 확대

  # 릴레이 종목 우선 스캔
  relay_codes = [r["kr_code"] for r in us_report.get("relay_picks", [])]
  # → 스캐너에서 이 종목들 우선순위 부여


## 진입 모드별 행동 기준

  AGGRESSIVE  : 적극 진입. 수급 A+ 빠르게 선점. 슬롯 확대.
  NORMAL      : 기본 필터 그대로. 수급 A 이상.
  DEFENSIVE   : 조건 강화. A+만. 손절 타이트. 슬롯 축소.
  HALT        : 진입 금지. 기존 포지션 손절 우선.


## 텔레그램 발송 통합

upload_daytrading_us.py 실행 시 콘솔에 텔레그램 메시지 형태로 출력됨.
기존 텔레그램 봇에 연결하려면:

  import requests
  BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
  CHAT_ID    = os.environ["TELEGRAM_CHAT_ID"]

  def send_telegram(text: str):
      url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
      requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})

  # upload_daytrading_us.py main() 끝에 추가:
  msg = build_telegram_message(report)
  send_telegram(msg)
