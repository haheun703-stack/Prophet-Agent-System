# 정보봇(JGIS) AWS 배포 가이드

> **발신**: 단타봇 (Prophet Agent System)
> **수신**: 정보봇 (JGIS Info Bot)
> **날짜**: 2026-04-05
> **목적**: 정보봇의 수급 데이터 수집기를 AWS에 배포하는 방법 안내

---

## 현재 상황 요약

**이미 돌아가고 있는 AWS 서버가 있습니다!**

| 항목 | 값 |
|------|-----|
| 서비스 | AWS Lightsail |
| 인스턴스 | Ubuntu 22.04 LTS (x86_64) |
| 요금 | $12/월 |
| IP | `13.209.153.221` |
| SSH 키 | `_tmp_zips/lightsail_60gb.pem` |
| OS | Ubuntu 22.04.5 LTS |
| Python | 3.11.0rc1 (venv) |

### 현재 돌아가는 서비스 (3개)

```
1. bodyhunter-bot    — 단타봇 텔레그램 봇 (메인)
2. quantum-scheduler — 퀀트봇 스케줄러
3. jgis-scheduler    — ★ 정보봇 스케줄러 (이미 배포됨!)
```

**정보봇은 이미 이 서버에서 돌아가고 있습니다.**

---

## 서버 구조도

```
/home/ubuntu/
├── bodyhunter/           ← 단타봇 repo (Prophet-Agent-System)
│   ├── venv/             ← Python 3.11 가상환경 (3봇 공유)
│   ├── .env              ← 환경변수 (3봇 공유)
│   ├── run_bot.py        ← 단타봇 실행 파일
│   └── scalper-agent/
│       └── data_store/
│           └── flow/     ← ★ 수급 데이터 (2,584 종목)
│               ├── 005930_investor.csv     ← 투자자수급
│               ├── 005930_foreign_exh.csv  ← 외인소진율
│               └── ...
│
├── jgis/                 ← 정보봇 repo (Global-Stock-Overview-Scripter)
│   ├── .env → ../bodyhunter/.env  ← 심볼릭 링크 (공유)
│   ├── src/
│   │   └── scheduler.py  ← 정보봇 스케줄러 실행 파일
│   └── data/             ← 정보봇 자체 데이터
│       ├── blackrock_*.json
│       ├── cftc_cot_*.json
│       └── ...
│
└── quantum-master/       ← 퀀트봇 repo
```

### 핵심 포인트

1. **가상환경(venv)**: `bodyhunter/venv`를 3봇이 공유
2. **.env 파일**: `bodyhunter/.env`를 3봇이 공유 (심볼릭 링크)
3. **수급 데이터**: `bodyhunter/scalper-agent/data_store/flow/`에 저장
   - 단타봇의 COO 파이프라인이 매일 16:00에 수집
   - 정보봇이 이 데이터를 읽어서 분석

---

## .env에 들어있는 키 목록

```
ANTHROPIC_API_KEY
KIS_APP_KEY / KIS_APP_SECRET / KIS_ACC_NO
TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
SUPABASE_URL / SUPABASE_KEY
FINNHUB_API_KEY / DART_API_KEY
XAI_API_KEY / OPENAI_API_KEY / PERPLEXITY_API_KEY
BOK_API_KEY / TAVILY_API_KEY / ALPHA_VANTAGE_API_KEY
KRX_ID / KRX_PW
```

---

## 정보봇 systemd 서비스 설정

파일 위치: `/etc/systemd/system/jgis-scheduler.service`

```ini
[Unit]
Description=JGIS Info Bot Scheduler (44 Jobs)
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/jgis
Environment=PYTHONIOENCODING=utf-8
Environment=PYTHONPATH=/home/ubuntu/jgis
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/home/ubuntu/bodyhunter/.env
ExecStart=/home/ubuntu/bodyhunter/venv/bin/python3.11 src/scheduler.py
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## 배포 명령어 (코드 업데이트 시)

### SSH 접속
```bash
ssh -i "_tmp_zips/lightsail_60gb.pem" ubuntu@13.209.153.221
```

### 코드 업데이트 + 재시작
```bash
cd ~/jgis && git pull && sudo systemctl restart jgis-scheduler
```

### 상태 확인
```bash
sudo systemctl status jgis-scheduler
```

### 로그 확인
```bash
# 최근 로그 100줄
sudo journalctl -u jgis-scheduler -n 100 --no-pager

# 실시간 로그
sudo journalctl -u jgis-scheduler -f
```

---

## 수급 데이터 접근 방법

정보봇이 수급 데이터를 사용하려면, 단타봇의 데이터 경로를 참조하면 됩니다:

```python
# 투자자수급 CSV 경로
FLOW_DIR = "/home/ubuntu/bodyhunter/scalper-agent/data_store/flow"

# 투자자수급 읽기
import pandas as pd
df = pd.read_csv(f"{FLOW_DIR}/005930_investor.csv")
# 컬럼: 날짜, 기관_금액, 기타법인_금액, 개인_금액, 외국인_금액,
#       기관_수량, 기타법인_수량, 개인_수량, 외국인_수량, 종가, 전일대비

# 외인소진율 읽기
df = pd.read_csv(f"{FLOW_DIR}/005930_foreign_exh.csv")
# 컬럼: 날짜, 상장주식수, 보유수량, 소진율, 한도수량, 한도소진율

# 국적별수급 (별도 디렉토리)
NAT_DIR = "/home/ubuntu/bodyhunter/scalper-agent/data_store/nationality"
# 파일명: {종목코드}_{날짜}.csv
# 컬럼: 국가명, 거래량
```

### 데이터 현황 (2026-04-05 기준)

| 데이터 | 파일 수 | 마지막 날짜 | 경로 |
|--------|---------|-----------|------|
| 투자자수급 | 2,584 | 4/3 | `flow/*_investor.csv` |
| 외인소진율 | 2,611 | 4/3 | `flow/*_foreign_exh.csv` |
| 국적별수급 | 6,156 | 4/3 | `nationality/` |
| 공매도잔고 | 3,134 | 2/24 (KRX 중단) | `short/` |

### 데이터 갱신 주기
- **매일 16:00~16:25**: 단타봇 COO가 자동 수집
- **약 22분** 소요 (2,264종목 투자자수급 + 국적별수급 + ETF)
- 장이 쉬는 날(주말/공휴일)에는 수집 안 함

---

## 새 패키지 설치가 필요한 경우

```bash
# bodyhunter venv 사용 (3봇 공유)
/home/ubuntu/bodyhunter/venv/bin/pip install 패키지명

# 또는 jgis 전용 venv를 만들 수도 있음 (현재는 공유)
```

---

## 자주 묻는 질문

### Q: EC2 인스턴스를 새로 만들어야 하나?
**아닙니다.** 이미 Lightsail 인스턴스에서 정보봇이 돌아가고 있습니다.
"EC2 인스턴스 있음"을 선택하세요.

### Q: 수급 데이터를 직접 수집해야 하나?
**아닙니다.** 단타봇이 매일 16:00에 자동 수집합니다.
정보봇은 `data_store/flow/` 경로에서 읽기만 하면 됩니다.

### Q: .env 파일은 어디서 가져오나?
이미 `bodyhunter/.env`에 모든 API 키가 있고,
jgis는 심볼릭 링크로 연결되어 있습니다.
새 키가 필요하면 `bodyhunter/.env`에 추가하면 3봇 모두 사용 가능합니다.

### Q: 서버가 꺼지면?
systemd가 자동 재시작합니다 (`Restart=on-failure`).
서버 자체가 리부트되면 `sudo systemctl enable jgis-scheduler`로 등록되어 있어 자동 시작됩니다.

### Q: 메모리가 부족하면?
현재 1.9GB 중 839MB 가용. 3봇 합산 약 800MB 사용 중.
부족하면 Lightsail 인스턴스 업그레이드($12→$24, 4GB RAM).

---

## 요약: 정보봇이 해야 할 것

1. **"EC2 인스턴스 있음"** 선택
2. IP: `13.209.153.221`, SSH 키: `_tmp_zips/lightsail_60gb.pem`
3. 코드 변경 시: `cd ~/jgis && git pull && sudo systemctl restart jgis-scheduler`
4. 수급 데이터: `/home/ubuntu/bodyhunter/scalper-agent/data_store/flow/` 에서 읽기
5. 환경변수: `/home/ubuntu/bodyhunter/.env` (심볼릭 링크로 공유)
