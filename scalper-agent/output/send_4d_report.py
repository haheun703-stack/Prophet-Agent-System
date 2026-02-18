# -*- coding: utf-8 -*-
"""Body Hunter v3 — 6D 디스크법 리포트 텔레그램 전송

TOP 5 추천 + 특이종목 태그 방식
- 종합점수(composite_score)로 전종목 순위
- TOP 5만 상세 추천
- 나머지 BUY+ 종목은 특이태그만
- ENTER/WATCH는 간략 리스트
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 설정
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 종목명 매핑
from data.kis_collector import UNIVERSE
NAMES = {code: info[0] for code, info in UNIVERSE.items()}


def send(text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=15)
    return resp.status_code == 200


def energy_icon(grade):
    """5D 사냥 에너지 등급 → 아이콘"""
    if grade == "EXPLOSIVE":
        return "\U0001F3AF\U0001F525"  # 🎯🔥
    elif grade == "HUNTABLE":
        return "\U0001F3AF"            # 🎯
    elif grade == "SLUGGISH":
        return "\U0001F40C"            # 🐌
    return ""


def _get_tags(f):
    """종목 특이점 태그 리스트 반환"""
    tags = []
    m = f.momentum
    th = f.tech_health

    # 기관 폭풍매수
    if m.inst_streak >= 5:
        tags.append(f"\U0001F525기관{m.inst_streak}일연속")
    if abs(m.inst_streak_amount) >= 500:
        tags.append(f"\U0001F4B0기관{m.inst_streak_amount:+.0f}억")

    # 외인 전환
    if m.foreign_inflection == "UP_TURN":
        tags.append("\u2191외인전환")

    # 6D 기술 S등급
    if th and th.tech_grade == "S":
        tags.append(f"\U0001F4AA기술S({th.tech_score:.0f})")

    # EXPLOSIVE 에너지
    if f.stability_grade == "EXPLOSIVE":
        tags.append("\U0001F3AF\U0001F525폭발")

    # 6D 가감점 상향
    if f._base_action != f.action:
        ranks = f._ACTION_RANKS
        diff = ranks.index(f._base_action) - ranks.index(f.action)
        if diff > 0:
            tags.append(f"\u2191{diff}\uB2E8\uACC4\uC0C1\uD5A5")

    # 밸류에이션 경고
    if f.valuation_warning:
        tags.append(f"\u26A0{f.valuation_warning}")

    return tags


def generate_report():
    """6D 스캔 → TOP 5 추천 + 나머지 태그

    1장: 헤더 + TOP 5 상세
    2장: 나머지 BUY+ (태그 한줄씩)
    3장: ENTER/WATCH/CAUTION 간략
    = 최대 3장
    """
    from data.supply_analyzer import SupplyAnalyzer

    analyzer = SupplyAnalyzer()

    # ETF 제외
    exclude_prefixes = ("069500", "371160", "102780", "305720")
    codes = [c for c in UNIVERSE.keys()
             if c not in exclude_prefixes
             and not c.startswith("018880") and not c.startswith("011210")]

    fulls = analyzer.scan_all_full(codes)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    date_str = fulls[0].score.date if fulls else "?"

    # BUY 이상만 추출
    buy_plus = [f for f in fulls if f.action in ("STRONG_BUY", "BUY")]

    # ━━━ BUY+ 종목 뉴스 수집 (네이버증권 + Grok API) ━━━
    from data.news_collector import NewsCollector
    import logging
    nc = NewsCollector()
    logger = logging.getLogger("BH.Report")

    # 1단계: BUY+ 전체 — 네이버 키워드 감성 (API 호출 없음, 빠름)
    for f in buy_plus:
        code = f.score.code
        name = NAMES.get(code, code)
        try:
            result = nc.get_news_score(code, name=name, use_grok=False)
            f.news_score = result["score"]
            f.news_summary = result.get("summary", "")
        except Exception as e:
            logger.warning(f"뉴스 수집 실패 {code}: {e}")

    # 1차 정렬 (뉴스 반영된 종합점수)
    buy_plus.sort(key=lambda f: f.composite_score, reverse=True)

    # 2단계: TOP 10 — Grok API 상세 감성분석
    for f in buy_plus[:10]:
        code = f.score.code
        name = NAMES.get(code, code)
        try:
            result = nc.get_news_score(code, name=name, use_grok=True)
            f.news_score = result["score"]
            f.news_summary = result.get("summary", "")
        except Exception as e:
            logger.warning(f"Grok 분석 실패 {code}: {e}")

    # 최종 정렬 (Grok 반영)
    buy_plus.sort(key=lambda f: f.composite_score, reverse=True)

    enters = [f for f in fulls if f.action == "ENTER"]
    cautions = [f for f in fulls if f.action == "CAUTION"]
    watches = [f for f in fulls if f.action == "WATCH"]
    skips = [f for f in fulls if f.action == "SKIP"]

    # TOP 5 (적자 경고 없는 것 우선, 있으면 뒤로)
    top5_clean = [f for f in buy_plus if not f.valuation_warning]
    top5_warn = [f for f in buy_plus if f.valuation_warning]
    top5_pool = top5_clean + top5_warn
    top5 = top5_pool[:5]
    rest = [f for f in buy_plus if f not in top5]

    msgs = []

    # ━━━ 1장: 헤더 + TOP 5 상세 ━━━
    lines = [
        f"\U0001F52E Body Hunter v3 | {now}",
        f"\U0001F4CA 6D \uC2A4\uCE94 {len(fulls)}\uC885\uBAA9 ({date_str})",
        f"BUY+: {len(buy_plus)} | ENTER: {len(enters)} | WATCH: {len(watches)} | SKIP: {len(skips)}",
        "",
        "\u2501" * 28,
        f"\U0001F3C6 \uC624\uB298\uC758 TOP 5 \uCD94\uCC9C",
        "\u2501" * 28,
    ]

    for i, f in enumerate(top5, 1):
        s = f.score
        m = f.momentum
        th = f.tech_health
        code = s.code
        name = NAMES.get(code, code)
        eicon = energy_icon(f.stability_grade)

        # 기관 수급
        if m.inst_streak > 0:
            inst = f"\uAE30\uAD00+{m.inst_streak}\uC77C({m.inst_streak_amount:+.0f}\uC5B5)"
        elif m.inst_streak < 0:
            inst = f"\uAE30\uAD00{m.inst_streak}\uC77C({m.inst_streak_amount:+.0f}\uC5B5)"
        else:
            inst = "\uAE30\uAD00\uC911\uB9BD"

        tech = f"{th.tech_grade}({th.tech_score:.0f})" if th else "-"
        warn = f" \u26A0{f.valuation_warning}" if f.valuation_warning else ""

        # 메달 이모지
        medal = ["\U0001F947", "\U0001F948", "\U0001F949", "4\uFE0F\u20E3", "5\uFE0F\u20E3"][i-1]

        lines.append("")
        lines.append(f"{medal} {name}({code}) {eicon}")
        lines.append(f"   \uC885\uD569: {f.composite_score:.0f}\uC810 | {f.action}")
        lines.append(
            f"   3D:{s.grade}({s.total_score:.0f}) 4D:{m.signal}({m.momentum_score:.0f}) "
            f"6D:{tech}"
        )
        lines.append(f"   {inst} | \uC678\uC778:{m.foreign_inflection}{warn}")

        # 뉴스 감성
        if f.news_score != 0 or f.news_summary:
            n_emoji = "\U0001F4C8" if f.news_score > 0 else ("\U0001F4C9" if f.news_score < 0 else "\U0001F4CA")
            news_line = f"   {n_emoji} \uB274\uC2A4: {f.news_score:+.0f}\uC810"
            if f.news_summary:
                news_line += f" | {f.news_summary[:25]}"
            lines.append(news_line)

        # 특이태그
        tags = _get_tags(f)
        if tags:
            lines.append(f"   {' '.join(tags)}")

    lines.append("")
    lines.append("3D=\uC218\uAE09 | 4D=\uBAA8\uBA58\uD140 | 5D=\uC5D0\uB108\uC9C0 | 6D=\uAE30\uC220 | \uB274\uC2A4=\uB124\uC774\uBC84+Grok")
    msgs.append("\n".join(lines))

    # ━━━ 2장: 나머지 BUY+ (태그 한줄씩) ━━━
    if rest:
        rest_lines = [
            f"\u2B50 \uAE30\uD0C0 BUY+ ({len(rest)}\uAC1C)",
            "\u2500" * 28,
        ]
        for f in rest[:20]:
            name = NAMES.get(f.score.code, f.score.code)
            eicon = energy_icon(f.stability_grade)
            tags = _get_tags(f)
            tag_str = " ".join(tags) if tags else ""
            rest_lines.append(
                f"\u251C {name} ({f.composite_score:.0f}\uC810) {eicon} {tag_str}"
            )
        if len(rest) > 20:
            rest_lines.append(f"... \uC678 {len(rest) - 20}\uAC1C")
        msgs.append("\n".join(rest_lines))

    # ━━━ 3장: ENTER + WATCH + CAUTION 간략 ━━━
    other_lines = []
    if enters:
        enters.sort(key=lambda f: f.composite_score, reverse=True)
        other_lines.append(f"\U0001F539 ENTER ({len(enters)}\uAC1C)")
        for f in enters[:10]:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(f"  {name}({f.composite_score:.0f})")
        if len(enters) > 10:
            other_lines.append(f"  ... \uC678 {len(enters) - 10}\uAC1C")

    if watches:
        other_lines.append(f"\n\U0001F50D WATCH ({len(watches)}\uAC1C)")
        for f in watches[:5]:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(f"  {name}")

    if cautions:
        other_lines.append(f"\n\u26A0\uFE0F CAUTION ({len(cautions)}\uAC1C)")
        for f in cautions[:5]:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(f"  {name}")

    if other_lines:
        msgs.append("\n".join(other_lines))

    return msgs


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    print("6D 리포트 생성 중...")
    msgs = generate_report()

    print(f"\n총 {len(msgs)}개 메시지 생성됨\n")
    for i, msg in enumerate(msgs, 1):
        print(f"--- Part {i} ({len(msg)}ch) ---")
        print(msg)
        print()

    # 텔레그램 전송
    if "--send" in sys.argv:
        print("텔레그램 전송 중...")
        for i, msg in enumerate(msgs, 1):
            ok = send(msg)
            print(f"  Part {i}/{len(msgs)}: {'OK' if ok else 'FAIL'} ({len(msg)}ch)")
            if i < len(msgs):
                time.sleep(0.8)
        print("전송 완료!")
    else:
        print("텔레그램 전송: python output/send_4d_report.py --send")
