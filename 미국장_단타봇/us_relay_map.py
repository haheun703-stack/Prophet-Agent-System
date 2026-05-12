"""
============================================
FLOWX 단타봇 — US→KR 릴레이 매핑
파일명: us_relay_map.py
============================================

미국 주요 종목 등락 → 한국 연관 종목 자동 연결
"NVDA +5% → SK하이닉스, 삼성전자 HBM 수혜"
"TSLA +4% → LG에너지솔루션, 삼성SDI"

데이터 소스: us_market_daily (Supabase) + 한투API
============================================
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("us_relay_map")


# ════════════════════════════════════════════════════════════
# 릴레이 매핑 테이블 (US 티커 → 한국 연관 종목)
# ════════════════════════════════════════════════════════════

RELAY_MAP = [
    # ── 반도체 / AI ──────────────────────────────────────
    {
        "us_ticker":   "NVDA",
        "us_name":     "엔비디아",
        "theme":       "AI반도체",
        "threshold":   2.0,       # 이 등락률 이상일 때만 릴레이 발동
        "kr_stocks": [
            {"code": "000660", "name": "SK하이닉스",   "reason": "HBM 최대 공급사"},
            {"code": "005930", "name": "삼성전자",     "reason": "HBM3E 공급사"},
            {"code": "042700", "name": "한미반도체",   "reason": "HBM 본딩 장비"},
        ],
    },
    {
        "us_ticker":   "AMD",
        "us_name":     "AMD",
        "theme":       "AI반도체",
        "threshold":   2.5,
        "kr_stocks": [
            {"code": "000660", "name": "SK하이닉스",  "reason": "DDR5/HBM 공급"},
            {"code": "005930", "name": "삼성전자",    "reason": "메모리 공급"},
        ],
    },
    {
        "us_ticker":   "AVGO",
        "us_name":     "브로드컴",
        "theme":       "AI네트워킹",
        "threshold":   2.0,
        "kr_stocks": [
            {"code": "000660", "name": "SK하이닉스",  "reason": "HBM 공급"},
            {"code": "086520", "name": "에코프로",    "reason": "PCB 소재"},
        ],
    },
    # ── 전기차 / 배터리 ────────────────────────────────────
    {
        "us_ticker":   "TSLA",
        "us_name":     "테슬라",
        "theme":       "전기차배터리",
        "threshold":   3.0,
        "kr_stocks": [
            {"code": "373220", "name": "LG에너지솔루션", "reason": "테슬라 배터리 공급사"},
            {"code": "006400", "name": "삼성SDI",        "reason": "EV 배터리"},
            {"code": "051910", "name": "LG화학",         "reason": "배터리 소재"},
            {"code": "086520", "name": "에코프로",       "reason": "양극재"},
        ],
    },
    {
        "us_ticker":   "GM",
        "us_name":     "GM",
        "theme":       "전기차배터리",
        "threshold":   3.0,
        "kr_stocks": [
            {"code": "373220", "name": "LG에너지솔루션", "reason": "GM 배터리 파트너"},
        ],
    },
    # ── 디스플레이 / OLED ──────────────────────────────────
    {
        "us_ticker":   "AAPL",
        "us_name":     "애플",
        "theme":       "디스플레이",
        "threshold":   2.0,
        "kr_stocks": [
            {"code": "034220", "name": "LG디스플레이", "reason": "OLED 공급사"},
            {"code": "005930", "name": "삼성전자",     "reason": "부품/메모리"},
        ],
    },
    # ── 방산 ──────────────────────────────────────────────
    {
        "us_ticker":   "LMT",
        "us_name":     "록히드마틴",
        "theme":       "방산",
        "threshold":   2.0,
        "kr_stocks": [
            {"code": "272210", "name": "한화에어로스페이스", "reason": "F-35 부품 공급"},
            {"code": "047810", "name": "한국항공우주(KAI)",  "reason": "항공 방산"},
        ],
    },
    {
        "us_ticker":   "RTX",
        "us_name":     "레이시온",
        "theme":       "방산",
        "threshold":   2.0,
        "kr_stocks": [
            {"code": "272210", "name": "한화에어로스페이스", "reason": "방산 수혜"},
            {"code": "012750", "name": "에스원",            "reason": "보안 방산"},
        ],
    },
    # ── 에너지 / 정유 ──────────────────────────────────────
    {
        "us_ticker":   "XOM",
        "us_name":     "엑손모빌",
        "theme":       "에너지정유",
        "threshold":   2.5,
        "kr_stocks": [
            {"code": "010950", "name": "S-Oil",     "reason": "정유 동반 수혜"},
            {"code": "096770", "name": "SK이노베이션", "reason": "정유/화학"},
        ],
    },
    # ── 조선 / LNG ────────────────────────────────────────
    {
        "us_ticker":   "LNG",   # Cheniere Energy
        "us_name":     "LNG(체니어)",
        "theme":       "조선LNG",
        "threshold":   3.0,
        "kr_stocks": [
            {"code": "009540", "name": "HD한국조선해양", "reason": "LNG선 건조"},
            {"code": "010140", "name": "삼성중공업",    "reason": "LNG선"},
            {"code": "042660", "name": "대우조선해양",  "reason": "LNG선"},
        ],
    },
]


# ════════════════════════════════════════════════════════════
# 릴레이 종목 추출
# ════════════════════════════════════════════════════════════

def get_relay_picks(us_data: dict, top_n: int = 5) -> list:
    """
    미국장 데이터에서 릴레이 조건 충족 종목 추출.

    us_data: us_market_daily row (또는 yfinance 데이터 dict)
    Returns: [
      {
        "us_ticker":  "NVDA",
        "us_name":    "엔비디아",
        "us_change":  3.5,
        "theme":      "AI반도체",
        "kr_code":    "000660",
        "kr_name":    "SK하이닉스",
        "reason":     "HBM 최대 공급사",
        "priority":   1,
      }, ...
    ]
    """
    # us_data에서 종목별 등락률 추출
    # 방법 1: 미리 수집된 개별 종목 등락률 (yfinance 보완 수집)
    # 방법 2: 섹터 ETF로 대리 판단
    stock_changes = _get_stock_changes(us_data)

    results = []
    for mapping in RELAY_MAP:
        ticker  = mapping["us_ticker"]
        thresh  = mapping["threshold"]
        change  = stock_changes.get(ticker)

        if change is None:
            # 개별 종목 데이터 없으면 섹터 ETF로 대리
            change = _fallback_sector_change(ticker, us_data)

        if change is None:
            continue

        # 임계값 체크 (절대값 — 급락도 릴레이 발동)
        if abs(change) < thresh:
            continue

        # 방향 필터 (하락 릴레이는 회피용)
        direction = "UP" if change >= 0 else "DOWN"

        priority = int(abs(change) * 10)  # 등락률 클수록 우선순위 높음

        for kr in mapping["kr_stocks"]:
            results.append({
                "us_ticker":  ticker,
                "us_name":    mapping["us_name"],
                "us_change":  round(change, 2),
                "direction":  direction,
                "theme":      mapping["theme"],
                "kr_code":    kr["code"],
                "kr_name":    kr["name"],
                "reason":     kr["reason"],
                "priority":   priority,
            })

    # 우선순위 정렬 + 중복 제거 (같은 한국 종목)
    seen_kr = set()
    deduped = []
    for r in sorted(results, key=lambda x: x["priority"], reverse=True):
        kr_code = r["kr_code"]
        if kr_code not in seen_kr:
            seen_kr.add(kr_code)
            deduped.append(r)

    return deduped[:top_n]


def _get_stock_changes(us_data: dict) -> dict:
    """
    us_market_daily에서 개별 종목 등락률 추출.
    정보봇이 sector_etf 외에 개별 종목도 수집하면 여기서 꺼냄.
    현재는 빈 dict 반환 → 섹터 ETF 대리로 폴백.
    """
    # TODO: 정보봇에서 NVDA/TSLA/AAPL 등 개별 종목 등락률도
    #       us_market_daily.individual_stocks JSONB에 저장하면 여기서 꺼냄
    individual = us_data.get("individual_stocks") or {}
    return individual


def _fallback_sector_change(ticker: str, us_data: dict) -> float | None:
    """
    개별 종목 데이터 없을 때 섹터 ETF로 대리 판단.
    NVDA/AMD → XLK (기술) 등락률 사용
    """
    sector_etf = us_data.get("sector_etf") or {}

    fallback = {
        "NVDA": "XLK", "AMD": "XLK", "AVGO": "XLK",
        "AAPL": "XLK", "MSFT": "XLK",
        "TSLA": "XLY", "GM": "XLY",
        "LMT":  "XLI", "RTX": "XLI",
        "XOM":  "XLE",
        "LNG":  "XLE",
    }
    etf = fallback.get(ticker)
    if etf and etf in sector_etf:
        return sector_etf[etf]
    return None


# ════════════════════════════════════════════════════════════
# 테스트용
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # 테스트 데이터
    test_us = {
        "date": "2026-04-08",
        "nasdaq_change":  1.8,
        "soxx_change":    3.2,
        "sp500_change":   1.2,
        "vix":            18.5,
        "dxy":            101.2,
        "us_3y_yield":    4.1,
        "fear_greed":     55,
        "fear_greed_label": "Neutral",
        "sector_etf": {
            "XLK": 2.1, "XLF": 0.5, "XLE": -0.3,
            "XLY": 1.8, "XLI": 0.4, "XLV": 0.2,
        },
        "risk_flags": [],
        "kr_impact": "나스닥 +1.8% | 반도체 강세",
    }

    picks = get_relay_picks(test_us)
    print(f"\n릴레이 종목 {len(picks)}개:")
    for p in picks:
        print(f"  {p['us_ticker']} {p['us_change']:+.1f}% → {p['kr_name']} ({p['kr_code']}) | {p['reason']}")
