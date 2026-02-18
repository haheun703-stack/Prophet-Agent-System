# 🔮 Prophet Agent v1.0 - "예언자"

> "3개월 뒤 포물선이 시작될 종목을 미리 찾는다"

## 철학

기존 시스템 "포물선의 초점 v8.1"이 **"언제 들어갈지"** (타이밍)를 판단한다면,  
예언자는 **"어디가 터질지"** (종목 선정)를 3개월 전에 예측한다.

서보성 핵심 원칙: **"주가는 EPS에 수렴한다"**

## 구조

```
prophet-agent/
├── config.yaml              # 설정 (API키, 가중치, 임계값)
├── main.py                  # 진입점
├── requirements.txt
├── collectors/              # 🔧 데이터 수집 파이프 (WHERE)
│   ├── pykrx_pipe.py       #   한국 시장 핵심 (OHLCV, 수급, 공매도)
│   ├── dart_pipe.py         #   공시, 재무제표, EPS
│   ├── yahoo_pipe.py        #   글로벌 매크로 (VIX, SOXX, 환율)
│   ├── naver_pipe.py        #   센티먼트, 신용잔고
│   └── whale_pipe.py        #   고래 추적 (연기금, 외국인)
├── predictors/              # 🧠 예측 두뇌 (WHAT)
│   ├── eps_divergence.py    #   ⭐ EPS 괴리도 (핵심)
│   ├── credit_danger.py     #   🚫 신용잔고 위험 (안전장치)
│   ├── dividend_floor.py    #   💰 배당수익률 바닥
│   ├── liquidation_floor.py #   🔒 반대매매 바닥
│   ├── whale_tracker.py     #   🐋 고래 추적
│   └── chicken_survivor.py  #   🐔 치킨게임 종료
├── synthesizer/
│   └── prophet_score.py     # 🔮 종합 판정 (100점 만점)
└── output/
    └── telegram_alert.py    # 📢 알림
```

## 빠른 시작

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. config.yaml에 API키 설정
#    - DART API키: https://opendart.fss.or.kr
#    - Telegram Bot Token

# 3. 단일 종목 분석
python main.py --ticker 005930

# 4. 전종목 스캔
python main.py --scan

# 5. 실시간 모니터링
python main.py --monitor
```

## 스코어링 체계

| 예측기 | 점수 | 역할 |
|--------|------|------|
| EPS 괴리도 | 0~30 | ⭐ 핵심 공격 |
| 신용 안전도 | -50~0 | 🚫 비상 브레이크 |
| 배당 바닥 | 0~15 | 💰 가치 바닥 |
| 고래 진입 | 0~15 | 🐋 스마트머니 |
| 치킨게임 종료 | 0~10 | 🐔 독과점 |
| 반대매매 바닥 | 0~5 | 🔒 절대 바닥 |

**판정 기준:**
- 80+ = 🔮🔮🔮 포물선 임박
- 60+ = 🔮🔮 가능성 높음  
- 40+ = 🔮 관심
- 음수 = 🚫 절대 금지

## Claude Code 개발 순서

```
Phase 1: pykrx_pipe.py + dart_pipe.py 실제 데이터 연동 테스트
Phase 2: eps_divergence.py 실전 로직 완성 → 삼성전자로 검증
Phase 3: credit_danger.py 신용잔고 크롤링 완성
Phase 4: synthesizer 통합 테스트
Phase 5: 포물선의 초점 v8.1과 API 연결
```
