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


def format_stock_block(f, rank_note=""):
    """SupplyFull → 텔레그램 메시지 블록"""
    s = f.score
    m = f.momentum
    st = f.stability
    code = s.code
    name = NAMES.get(code, code)
    action = f.action

    # 5D 에너지
    sg = f.stability_grade
    eicon = energy_icon(sg)
    energy_line = ""
    if st:
        energy_line = f"5D: {sg}({st.stability_score:.0f})"
        if st.atr_pct > 0:
            energy_line += f" | ATR:{st.atr_pct:.1f}%"
        if st.smart_money_ratio != 0:
            energy_line += f" | SM:{st.smart_money_ratio:+.1f}%"
        if st.signal_count > 0:
            energy_line += f" | \uC2E0\uD638:{st.signal_count}/4"

    # 가격 정보 (daily CSV에서)
    from data.supply_analyzer import DAILY_DIR
    import pandas as pd
    price_line = ""
    try:
        daily_path = DAILY_DIR / f"{code}.csv"
        if daily_path.exists():
            df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
            # 한글 컬럼 → 영문 매핑
            if "종가" in df.columns:
                df = df.rename(columns={"종가": "close"})
            if len(df) > 0 and "close" in df.columns:
                last = df.iloc[-1]
                price = int(last["close"])
                price_line = f"\U0001F4B0 {price:,}\uC6D0 ({df.index[-1].date()})"
    except Exception:
        pass

    # 수급 정보
    flow_lines = []
    if m.inst_streak > 0:
        fire = "\U0001F525" * min(4, max(1, abs(m.inst_streak) // 2))
        flow_lines.append(f"\u251C \uAE30\uAD00: {m.inst_streak}일 \uC5F0\uC18D\uB9E4\uC218 ({m.inst_streak_amount:+.0f}\uC5B5) {fire}")
    elif m.inst_streak < 0:
        flow_lines.append(f"\u251C \uAE30\uAD00: {abs(m.inst_streak)}일 \uC5F0\uC18D\uB9E4\uB3C4 ({m.inst_streak_amount:+.0f}\uC5B5)")
    else:
        flow_lines.append(f"\u251C \uAE30\uAD00: \uC911\uB9BD")

    inflection_icon = "\u2191" if m.foreign_inflection == "UP_TURN" else ("\u2193" if m.foreign_inflection == "DOWN_TURN" else "\u2192")
    flow_lines.append(f"\u251C \uC678\uC778\uBCC0\uACE1: {m.foreign_inflection} {inflection_icon}")

    contra_mark = "\u2705" if m.retail_contrarian else ""
    flow_lines.append(f"\u2514 \uAC1C\uC778\uC5ED\uC9C0\uD45C: {'O' if m.retail_contrarian else 'X'} {contra_mark}")

    # 조합
    emoji = grade_emoji(action)
    tier = action_to_tier(action)
    title = f"{emoji} {tier} \u2014 {name} ({code})"
    if rank_note:
        title += f" {rank_note}"
    if eicon:
        title += f" {eicon}"

    lines = [
        "\u2501" * 28,
        title,
        "\u2501" * 28,
        f"{f.risk_label} | 3D: {s.grade}({s.total_score:.0f}) | 4D: {m.signal}({m.momentum_score:.0f})",
    ]
    if energy_line:
        lines.append(energy_line)
    if price_line:
        lines.append(price_line)
    lines.append("")
    lines.append("\U0001F4CA \uC218\uAE09 \uC0C1\uC138")
    lines.extend(flow_lines)

    return "\n".join(lines)


def generate_report():
    """5D 스캔 실행 후 텔레그램 메시지 리스트 생성"""
    from data.supply_analyzer import SupplyAnalyzer

    analyzer = SupplyAnalyzer()

    # ETF, 순환매 ETF 제외
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

    # ━━━ Part 1: Header ━━━
    header_lines = [
        f"\U0001F52E Body Hunter v3 (5D \uB514\uC2A4\uD06C\uBC95) | {now}",
        "\u2501" * 28,
        "",
        f"\U0001F4CA 5D \uC218\uAE09 \uC2A4\uCE94 ({len(fulls)}\uC885\uBAA9 | {date_str})",
        f"\u251C STRONG_BUY: {len(strong_buys)} | BUY: {len(buys)} | ENTER: {len(enters)}",
        f"\u251C WATCH: {len(watches)} | CAUTION: {len(cautions)} | SKIP: {len(skips)}",
        f"\u2514 \uC5D0\uB108\uC9C0: " + " / ".join(
            f"{g}({stab_counts.get(g, 0)})"
            for g in ["EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH"]
            if stab_counts.get(g, 0) > 0
        ),
        "",
        "\U0001F3AF\U0001F525=\uD3ED\uBC1C\uC801 | \U0001F3AF=\uC0AC\uB0E5\uAC10 | \U0001F40C=\uB454\uAC10",
    ]
    msgs.append("\n".join(header_lines))

    # ━━━ Part 2+: STRONG_BUY 종목들 (각각 별도 메시지) ━━━
    for f in strong_buys:
        msgs.append(format_stock_block(f))

    # ━━━ BUY 종목들 ━━━
    for i, f in enumerate(buys):
        note = ""
        if i == 0 and f.momentum.momentum_score >= 90:
            note = "\U0001F3C6 4D 1\uC704"
        msgs.append(format_stock_block(f, note))

    # ━━━ ENTER + CAUTION + WATCH (하나로 묶기) ━━━
    other_lines = []
    if enters:
        other_lines.append("\u2500" * 28)
        other_lines.append(f"\U0001F539 B\uB4F1\uAE09 (ENTER)")
        other_lines.append("\u2500" * 28)
        for f in enters:
            name = NAMES.get(f.score.code, f.score.code)
            shield = energy_icon(f.stability_grade)
            stab = f.stability.stability_score if f.stability else 0
            other_lines.append(
                f"{name}({f.score.code}) {f.risk_label} "
                f"| 3D:{f.score.grade}({f.score.total_score:.0f}) "
                f"4D:{f.momentum.signal}({f.momentum.momentum_score:.0f}) "
                f"5D:{f.stability_grade}({stab:.0f}) {shield}"
            )

    if cautions:
        other_lines.append("")
        other_lines.append(f"\u26A0\uFE0F C\uB4F1\uAE09 (\uD568\uC815\uC8FC\uC758)")
        for f in cautions:
            name = NAMES.get(f.score.code, f.score.code)
            other_lines.append(f"\u251C {name}: CAUTION ({f.score.grade}/{f.momentum.signal})")

    if watches:
        other_lines.append("")
        other_lines.append(f"\U0001F50D \uAD00\uCC30\uC885\uBAA9 (WATCH)")
        for f in watches:
            name = NAMES.get(f.score.code, f.score.code)
            shield = energy_icon(f.stability_grade)
            other_lines.append(
                f"\u251C {name}({f.score.code}) "
                f"3D:{f.score.grade}({f.score.total_score:.0f}) "
                f"4D:{f.momentum.signal}({f.momentum.momentum_score:.0f}) {shield}"
            )

    if other_lines:
        msgs.append("\n".join(other_lines))

    # ━━━ 전략 요약 ━━━
    summary_lines = [
        "\u2501" * 28,
        f"\U0001F4CB \uC624\uB298\uC758 \uC804\uB7B5 \uC694\uC57D",
        "\u2501" * 28,
    ]

    all_active = strong_buys + buys
    for i, f in enumerate(all_active, 1):
        name = NAMES.get(f.score.code, f.score.code)
        eic = energy_icon(f.stability_grade)
        tier = "S" if f.action == "STRONG_BUY" else "A"
        summary_lines.append(f"{i}\uFE0F\u20E3 {name} [{tier}] {f.risk_label} {eic}")

    summary_lines.append("")
    summary_lines.append("V = \u03C0\u222B[f(x)]\u00B2dx")
    summary_lines.append("3D=\uBC18\uC9C0\uB984 | 4D=\uD31D\uCC3D\uC18D\uB3C4 | 5D=\uC5D0\uB108\uC9C0(\uC0AC\uB0E5\uC801\uD569\uB3C4)")
    summary_lines.append("")
    summary_lines.append(f"\U0001F52E Body Hunter v3 | 3D+4D+5D \uB514\uC2A4\uD06C\uBC95")

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
