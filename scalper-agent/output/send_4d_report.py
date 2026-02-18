# -*- coding: utf-8 -*-
"""Body Hunter v3 — 5D 디스크법 리포트 텔레그램 전송

동적 생성: scan_all_full() 결과를 받아 자동 포맷팅 + 텔레그램 전송
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
    return ""                          # MODERATE/UNKNOWN → 없음


def grade_emoji(action):
    """action → 등급 이모지"""
    if action == "STRONG_BUY":
        return "\U0001F525"  # 🔥
    elif action == "BUY":
        return "\u2B50"      # ⭐
    elif action == "ENTER":
        return "\U0001F539"  # 🔹
    elif action == "CAUTION":
        return "\u26A0\uFE0F"  # ⚠️
    elif action == "WATCH":
        return "\U0001F50D"    # 🔍
    return "\u26D4"            # ⛔


def action_to_tier(action):
    """action → 등급명"""
    m = {
        "STRONG_BUY": "S등급",
        "BUY": "A등급",
        "ENTER": "B등급",
        "CAUTION": "C등급 (함정주의)",
        "WATCH": "관찰종목",
    }
    return m.get(action, "SKIP")


def format_stock_line(f, idx=0):
    """SupplyFull → 한 줄 요약"""
    s = f.score
    m = f.momentum
    code = s.code
    name = NAMES.get(code, code)
    eicon = energy_icon(f.stability_grade)
    th = f.tech_health

    # 6D 기술등급
    tech = f"{th.tech_grade}({th.tech_score:.0f})" if th else "-"

    # 기관 수급
    if m.inst_streak > 0:
        inst = f"기관+{m.inst_streak}일({m.inst_streak_amount:+.0f}억)"
    elif m.inst_streak < 0:
        inst = f"기관{m.inst_streak}일({m.inst_streak_amount:+.0f}억)"
    else:
        inst = "기관중립"

    # 가감점 화살표
    arrow = ""
    base = f._base_action
    final = f.action
    if final != base:
        ranks = f._ACTION_RANKS
        diff = ranks.index(base) - ranks.index(final)
        arrow = "↑" * diff if diff > 0 else "↓" * (-diff)

    # 밸류 경고
    warn = f" ⚠{f.valuation_warning}" if f.valuation_warning else ""

    num = f"{idx}." if idx else "•"
    return (
        f"{num} {name}({code}) {eicon}\n"
        f"   3D:{s.grade}({s.total_score:.0f}) 4D:{m.signal}({m.momentum_score:.0f}) "
        f"6D:{tech} {arrow}\n"
        f"   {inst} | 외인:{m.foreign_inflection}{warn}"
    )


def _build_group_message(title, stocks, max_items=20):
    """등급별 종목 리스트 → 하나의 메시지 텍스트

    텔레그램 4096자 제한 고려하여 max_items로 제한
    """
    lines = [title, "\u2501" * 28]
    for i, f in enumerate(stocks[:max_items], 1):
        lines.append(format_stock_line(f, i))
    if len(stocks) > max_items:
        lines.append(f"\n... 외 {len(stocks) - max_items}개")
    return "\n".join(lines)


def generate_report():
    """6D 스캔 실행 후 텔레그램 메시지 리스트 생성

    등급별 묶음 전송: 헤더 1장 + S등급 1장 + A등급 1장 + B/WATCH 1장 + 요약 1장
    = 최대 5장
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

    # 그룹핑
    strong_buys = [f for f in fulls if f.action == "STRONG_BUY"]
    buys = [f for f in fulls if f.action == "BUY"]
    enters = [f for f in fulls if f.action == "ENTER"]
    cautions = [f for f in fulls if f.action == "CAUTION"]
    watches = [f for f in fulls if f.action == "WATCH"]
    skips = [f for f in fulls if f.action == "SKIP"]

    # 에너지 통계
    stab_counts = {}
    for f in fulls:
        g = f.stability_grade
        stab_counts[g] = stab_counts.get(g, 0) + 1

    msgs = []

    # ━━━ 1장: 헤더 + 요약 통계 ━━━
    header_lines = [
        f"\U0001F52E Body Hunter v3 (6D) | {now}",
        "\u2501" * 28,
        "",
        f"\U0001F4CA 6D \uC218\uAE09 \uC2A4\uCE94 ({len(fulls)}\uC885\uBAA9 | {date_str})",
        f"\U0001F525 STRONG_BUY: {len(strong_buys)}",
        f"\u2B50 BUY: {len(buys)}",
        f"\U0001F539 ENTER: {len(enters)}",
        f"\u26A0\uFE0F CAUTION: {len(cautions)} | \U0001F50D WATCH: {len(watches)}",
        f"\u26D4 SKIP: {len(skips)}",
        "",
        "\uC5D0\uB108\uC9C0: " + " / ".join(
            f"{g}({stab_counts.get(g, 0)})"
            for g in ["EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH"]
            if stab_counts.get(g, 0) > 0
        ),
        "",
        "\U0001F3AF\U0001F525=\uD3ED\uBC1C | \U0001F3AF=\uC0AC\uB0E5\uAC10 | \u2191=6D/\uB274\uC2A4 \uC0C1\uD5A5",
    ]
    msgs.append("\n".join(header_lines))

    # ━━━ 2장: S등급 (STRONG_BUY) ━━━
    if strong_buys:
        msgs.append(_build_group_message(
            f"\U0001F525 S\uB4F1\uAE09 STRONG_BUY ({len(strong_buys)}\uAC1C)",
            strong_buys, max_items=15
        ))

    # ━━━ 3장: A등급 (BUY) ━━━
    if buys:
        msgs.append(_build_group_message(
            f"\u2B50 A\uB4F1\uAE09 BUY ({len(buys)}\uAC1C)",
            buys, max_items=20
        ))

    # ━━━ 4장: B등급 + WATCH + CAUTION ━━━
    other_lines = []
    if enters:
        other_lines.append(f"\U0001F539 B\uB4F1\uAE09 ENTER ({len(enters)}\uAC1C)")
        other_lines.append("\u2500" * 28)
        for i, f in enumerate(enters[:15], 1):
            name = NAMES.get(f.score.code, f.score.code)
            eicon = energy_icon(f.stability_grade)
            th = f.tech_health
            tech = f"6D:{th.tech_grade}({th.tech_score:.0f})" if th else ""
            other_lines.append(
                f"{i}. {name} | 3D:{f.score.grade}({f.score.total_score:.0f}) "
                f"4D:{f.momentum.signal}({f.momentum.momentum_score:.0f}) {tech} {eicon}"
            )
        if len(enters) > 15:
            other_lines.append(f"... \uC678 {len(enters) - 15}\uAC1C")

    if watches:
        other_lines.append("")
        other_lines.append(f"\U0001F50D \uAD00\uCC30 WATCH ({len(watches)}\uAC1C)")
        for f in watches[:10]:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(
                f"\u251C {name} | 3D:{f.score.grade}({f.score.total_score:.0f}) "
                f"4D:{f.momentum.signal}({f.momentum.momentum_score:.0f})"
            )

    if cautions:
        other_lines.append("")
        other_lines.append(f"\u26A0\uFE0F \uD568\uC815\uC8FC\uC758 CAUTION ({len(cautions)}\uAC1C)")
        for f in cautions[:10]:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(f"\u251C {name}: {f.score.grade}/{f.momentum.signal}")

    if other_lines:
        msgs.append("\n".join(other_lines))

    # ━━━ 5장: 전략 요약 (S+A 한줄씩) ━━━
    summary_lines = [
        "\u2501" * 28,
        f"\U0001F4CB \uC624\uB298\uC758 \uC804\uB7B5 \uC694\uC57D ({len(strong_buys)+len(buys)}\uC885\uBAA9)",
        "\u2501" * 28,
    ]
    all_active = strong_buys + buys
    for i, f in enumerate(all_active[:20], 1):
        name = NAMES.get(f.score.code, f.score.code)
        eic = energy_icon(f.stability_grade)
        tier = "S" if f.action == "STRONG_BUY" else "A"
        # 가감점 표시
        arrow = ""
        if f._base_action != f.action:
            ranks = f._ACTION_RANKS
            diff = ranks.index(f._base_action) - ranks.index(f.action)
            arrow = "\u2191" * diff if diff > 0 else "\u2193" * (-diff)
        summary_lines.append(f"{i}. [{tier}] {name} {f.risk_label} {eic}{arrow}")

    if len(all_active) > 20:
        summary_lines.append(f"... \uC678 {len(all_active) - 20}\uAC1C")

    summary_lines.append("")
    summary_lines.append("3D=\uC218\uAE09 | 4D=\uBAA8\uBA58\uD140 | 5D=\uC5D0\uB108\uC9C0 | 6D=\uAE30\uC220")
    summary_lines.append(f"\U0001F52E Body Hunter v3 | 6D \uB514\uC2A4\uD06C\uBC95")

    msgs.append("\n".join(summary_lines))

    return msgs


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    print("5D 리포트 생성 중...")
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
