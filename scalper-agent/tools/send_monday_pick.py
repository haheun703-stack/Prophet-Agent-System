# -*- coding: utf-8 -*-
"""
3/16(월) MOMENTUM 진입 후보 텔레그램 전송
"""

import os
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(ENV_PATH)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send(text: str):
    if not TOKEN or not CHAT_ID:
        print("TELEGRAM_BOT_TOKEN / CHAT_ID not set")
        return False
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    })
    if resp.ok:
        print("Telegram OK")
        return True
    else:
        print(f"Telegram FAIL: {resp.text}")
        return False


def build_message():
    lines = []
    lines.append("<b>[MOMENTUM v2] 3/16(월) 진입 후보</b>")
    lines.append("3/13 종가 기준 | THRESHOLD 0.40")
    lines.append("")

    lines.append("<b>--- TOP 5 추천 (매집 패턴 우선) ---</b>")
    lines.append("")

    # 1. 롯데렌탈
    lines.append("<b>1. 롯데렌탈 (089860) 32,400원</b>")
    lines.append("   Score 0.85 | 5일 연속매수 + 조용한 매집")
    lines.append("   5일 +2.2% (아직 안 올랐음!)")
    lines.append("   SL 31,266 / TP 35,640")
    lines.append("")

    # 2. 한일홀딩스
    lines.append("<b>2. 한일홀딩스 (003300) 17,710원</b>")
    lines.append("   Score 0.80 | 9일 연속매수 (업계 최장)")
    lines.append("   5일 +5.2% | 비금속 섹터")
    lines.append("   SL 17,090 / TP 19,481")
    lines.append("")

    # 3. HLB제약
    lines.append("<b>3. HLB제약 (047920) 15,010원</b>")
    lines.append("   Score 0.70 | 가격변동 없이 조용한 매집")
    lines.append("   5일 -0.6% | 제약 섹터 피어")
    lines.append("   SL 14,484 / TP 16,511")
    lines.append("")

    # 4. 아이에스동서
    lines.append("<b>4. 아이에스동서 (010780) 33,450원</b>")
    lines.append("   Score 0.75 | 6일 연속매수 + 건설 섹터 동반")
    lines.append("   5일 +15.3% | 비금속")
    lines.append("   SL 32,279 / TP 36,795")
    lines.append("")

    # 5. 티씨케이
    lines.append("<b>5. 티씨케이 (064760) 232,500원</b>")
    lines.append("   Score 0.70 | 낙폭 후 기관 3일 매집 시작")
    lines.append("   5일 -10.6% | 반도체 회복 테마")
    lines.append("   SL 224,362 / TP 255,750")
    lines.append("")

    lines.append("<b>--- 건설 섹터 후보 ---</b>")
    lines.append("")

    lines.append("<b>6. 태영건설 (009410) 1,832원</b>")
    lines.append("   Score 0.60 | 3일 연속 외인매수 + 건설 동반")
    lines.append("   5일 +5.0% | 워크아웃 졸업 이슈")
    lines.append("   SL 1,767 / TP 2,015")
    lines.append("")

    lines.append("<b>7. 현대건설 (000720) 164,400원</b>")
    lines.append("   Score 0.50 | 2일 연속매수 시작")
    lines.append("   5일 +4.8% | 대형 건설주 안정적")
    lines.append("   SL 158,646 / TP 180,840")
    lines.append("")

    lines.append("<b>--- 고점 추격 주의 (WATCH) ---</b>")
    lines.append("")
    lines.append("HJ중공업(1.00/+16%) SK이터닉스(1.00/+44%)")
    lines.append("대우건설(0.95/+40%) 노타(0.90/+37%)")
    lines.append("-> 이미 급등, 월요일 차익실현 위험")
    lines.append("")

    lines.append("<b>--- 전략 ---</b>")
    lines.append("MOMENTUM 진입: MACD 스킵, 조건 2/6")
    lines.append("SL -3.5% | 트레일링 +1.5%->-2%")
    lines.append("최대 보유 5일 | 수급 이탈시 즉시 청산")

    return "\n".join(lines)


if __name__ == "__main__":
    msg = build_message()
    print(msg)
    print("\n" + "=" * 50)
    send(msg)
