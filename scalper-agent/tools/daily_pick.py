# -*- coding: utf-8 -*-
"""
일간 추천종목 스캐너 v3 + ETF 시그널 → 텔레그램 전송
설정: 점수 30+, TOP 10, 손절 -5%
양식: send_4d_report.py와 동일 (메달+구분선+플레인텍스트)
"""
import sys, io, json, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
import glob as gl
from datetime import datetime
import requests

from backtest.full_period_backtest import score_stock


TOP_N = 10
MIN_SCORE = 30
STOP_LOSS = -5.0

# 메달 이모지 (send_4d_report.py 동일)
MEDALS = [
    "\U0001F947",  # 🥇
    "\U0001F948",  # 🥈
    "\U0001F949",  # 🥉
    "4\uFE0F\u20E3",  # 4️⃣
    "5\uFE0F\u20E3",  # 5️⃣
    "6\uFE0F\u20E3",  # 6️⃣
    "7\uFE0F\u20E3",  # 7️⃣
    "8\uFE0F\u20E3",  # 8️⃣
    "9\uFE0F\u20E3",  # 9️⃣
    "\U0001F51F",  # 🔟
]

TG_MAX = 4096


def load_env():
    """환경변수에서 텔레그램 정보 로드"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    env_candidates = [
        os.path.join(os.path.dirname(__file__), "..", ".env"),
        os.path.join(os.path.dirname(__file__), "..", "..", ".env"),
    ]
    env_file = next((f for f in env_candidates if os.path.exists(f)), None)
    if (not token or not chat_id) and env_file:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith("TELEGRAM_BOT_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("TELEGRAM_CHAT_ID="):
                    chat_id = line.split("=", 1)[1].strip().strip('"').strip("'")

    return token, chat_id


def send_telegram(token, chat_id, text):
    """텔레그램 메시지 전송 (플레인텍스트, parse_mode 없음)"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    resp = requests.post(url, json=payload, timeout=15)
    return resp.status_code == 200


def split_message(text, limit=TG_MAX):
    """텔레그램 4096자 제한 분할"""
    if len(text) <= limit:
        return [text]
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > limit - 50:
            chunks.append(current)
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current)
    return chunks


def sto_zone_icon(k):
    """STO 영역별 아이콘"""
    if k >= 90:
        return "\U0001F525"   # 🔥 극과매수
    elif k >= 80:
        return "\u2B06"       # ⬆ 과매수
    elif k >= 60:
        return "\u2197"       # ↗ 고구간
    elif k >= 40:
        return "\u27A1"       # ➡ 중립
    elif k >= 20:
        return "\u2198"       # ↘ 저구간
    else:
        return "\u2744"       # ❄ 과매도


def sto_zone_label(k):
    if k >= 90: return "극과매수"
    elif k >= 80: return "과매수"
    elif k >= 60: return "고구간"
    elif k >= 40: return "중립"
    elif k >= 20: return "저구간"
    else: return "과매도"


ETF_TOP_N = 5


def load_etf_scan():
    """최신 ETF 스캔 결과 CSV 로드 → TOP 5 시그널"""
    csv_files = sorted(gl.glob('results/etf_scan_*.csv'), reverse=True)
    if not csv_files:
        return None

    df = pd.read_csv(csv_files[0])
    with_signal = df[df['has_signal'] == True].head(ETF_TOP_N)
    if with_signal.empty:
        return None

    return with_signal, os.path.basename(csv_files[0])


def format_etf_message(etf_df, csv_name):
    """ETF 시그널 텔레그램 메시지 생성 (플레인텍스트)"""
    lines = [
        "\u2501" * 28,
        "\U0001F4CA ETF \uC774\uD0C8 \uC2DC\uADF8\uB110 TOP 5",
        "\u2501" * 28,
    ]

    for i, (_, row) in enumerate(etf_df.iterrows()):
        medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
        direction = row['direction']
        dir_icon = "\u2B06" if direction == "LONG" else "\u2B07"

        lines.append("")
        lines.append(f"{medal} {row['name']} {dir_icon}{direction}")
        lines.append(
            f"   \uC810\uC218: {row['score']:.0f}\uC810 | "
            f"{row['close']:,.0f}\uC6D0 | "
            f"\uAC70\uB798\uB7C9:{row['vol_ratio']:.1f}x"
        )
        if row['entry'] > 0:
            lines.append(
                f"   \uC9C4\uC785: {row['entry']:,.0f} "
                f"\u2192 SL:{row['sl']:,.0f} / TP:{row['tp']:,.0f}"
            )
        lines.append(
            f"   NAV\uAD34\uB9AC: {row['nav_gap_pct']:+.2f}% | "
            f"5\uC77C\uC218\uC775: {row['return_5d']:+.1f}%"
        )

    lines.append("")
    lines.append("\u2501" * 28)
    lines.append("Body Hunter v2.3 | FCR\uC774\uD0C8+\uAC70\uB798\uB7C9\uC11C\uC9C0")
    lines.append("\u26A0 SL/TP = \uC804\uC77CFCR \uAE30\uBC18, \uC190\uC808 \uD544\uC218")

    return "\n".join(lines)


def run_scan():
    """오늘 기준 스캐너 v3 실행 → TOP N 추천"""
    csv_files = gl.glob('data_store/daily/*.csv')
    with open('data_store/universe.json', 'r', encoding='utf-8') as f:
        uni = json.load(f)

    picks = []

    for fpath in csv_files:
        code = os.path.basename(fpath).replace('.csv', '')
        if code not in uni:
            continue

        df = pd.read_csv(fpath).sort_values('날짜').reset_index(drop=True)
        if len(df) < 65:
            continue

        c = df['종가'].values.astype(float)
        h = df['고가'].values.astype(float)
        l = df['저가'].values.astype(float)
        v = df['거래량'].values.astype(float)
        o = df['시가'].values.astype(float)

        result = score_stock(c, h, l, v, o)
        if result is None:
            continue

        sc, sigs, det = result
        if sc < MIN_SCORE:
            continue

        last_date = df['날짜'].iloc[-1]
        last_close = int(c[-1])
        last_change = (c[-1] / c[-2] - 1) * 100 if len(c) >= 2 else 0

        picks.append({
            'code': code,
            'name': uni[code]['name'],
            'sector': uni[code].get('sub_sector', uni[code].get('sector', '')),
            'score': sc,
            'signals': sigs,
            'sto_k': det.get('sto_k', 0),
            'sto_d': det.get('sto_d', 0),
            'vr': det.get('vr', 0),
            'bb_pct': det.get('bb_pct', 0),
            'drop20': det.get('drop20', 0),
            'close': last_close,
            'change': last_change,
            'date': last_date,
        })

    picks.sort(key=lambda x: -x['score'])
    top = picks[:TOP_N]
    rest = picks[TOP_N:30]  # 11~30위

    return top, rest, len(picks)


def format_messages(top, rest, total_picks):
    """
    텔레그램 메시지 생성 (send_4d_report.py 양식 준수)

    1장: 헤더 + TOP 10 상세
    2장: 11~30위 간략 + 요약통계
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    data_date = top[0]['date'] if top else "?"

    msgs = []

    # ━━━ 1장: 헤더 + TOP 10 상세 ━━━
    lines = [
        f"\U0001F52E \uC2A4\uCE90\uB108v3 | {now}",
        f"\U0001F4CA {data_date} \uAE30\uC900 | {total_picks}\uC885\uBAA9 \uC120\uC815",
        f"\uC124\uC815: {MIN_SCORE}\uC810+ / TOP{TOP_N} / \uC190\uC808{STOP_LOSS}%",
        "",
        "\u2501" * 28,
        f"\U0001F3C6 \uB0B4\uC77C TOP {len(top)} \uCD94\uCC9C",
        "\u2501" * 28,
    ]

    for i, p in enumerate(top):
        medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
        k = p['sto_k']
        zone_icon = sto_zone_icon(k)
        zone_label = sto_zone_label(k)

        # 핵심 패턴 태그 (★ 시그널만)
        star_sigs = [s.replace('★', '') for s in p['signals'] if s.startswith('★')]
        # STO 주요 시그널
        sto_sigs = [s for s in p['signals']
                    if ('골든' in s or '스프레드' in s or '상승전환' in s or '다이버전스' in s)
                    and not s.startswith('★')]

        tags = []
        if star_sigs:
            tags.extend([f"\U0001F31F{s}" for s in star_sigs[:2]])  # 🌟패턴명
        if sto_sigs:
            tags.extend(sto_sigs[:2])
        if '정배열' in p['signals']:
            tags.append("\u2191\uC815\uBC30\uC5F4")  # ↑정배열
        if '역배열' in p['signals']:
            tags.append("\u2193\uC5ED\uBC30\uC5F4")  # ↓역배열
        if p['vr'] >= 3:
            tags.append(f"\U0001F4A5\uAC70\uB798\uB7C9{p['vr']:.1f}x")  # 💥거래량
        elif p['vr'] >= 1.5:
            tags.append(f"\U0001F4C8\uAC70\uB798\uB7C9{p['vr']:.1f}x")  # 📈거래량

        tag_str = " ".join(tags) if tags else ""

        # 등락 이모지
        if p['change'] >= 5:
            ch_icon = "\U0001F534"  # 🔴 강세
        elif p['change'] >= 2:
            ch_icon = "\U0001F7E0"  # 🟠 상승
        elif p['change'] <= -3:
            ch_icon = "\U0001F535"  # 🔵 하락
        else:
            ch_icon = "\u26AA"     # ⚪ 보합

        lines.append("")
        lines.append(f"{medal} {p['name']}({p['code']}) {zone_icon}")
        lines.append(f"   \uC810\uC218: {p['score']}\uC810 | {p['close']:,}\uC6D0 ({p['change']:+.1f}%) {ch_icon}")
        lines.append(f"   STO: K{k:.0f}/D{p['sto_d']:.0f} ({zone_label}) | VR:{p['vr']:.1f}x | BB:{p['bb_pct']:.0f}%")
        lines.append(f"   \uC139\uD130: {p['sector']}")
        if tag_str:
            lines.append(f"   {tag_str}")

    lines.append("")
    lines.append("\u2501" * 28)
    lines.append("STO=\uC2A4\uD1A0\uCE90\uC2A4\uD2F1(14,3,3) | VR=\uAC70\uB798\uB7C9\uBE44 | BB=\uBCFC\uB9B0\uC800%")
    lines.append(f"\u26A0 \uC190\uC808: \uC9C4\uC785\uAC00 \uB300\uBE44 {STOP_LOSS}% \uB3C4\uB2EC\uC2DC \uC989\uC2DC \uCCAD\uC0B0")
    msgs.append("\n".join(lines))

    # ━━━ 2장: 11~30위 간략 + 통계 ━━━
    if rest:
        rest_lines = [
            f"\u2B50 \uAE30\uD0C0 \uCD94\uCC9C ({len(rest)}\uAC1C)",
            "\u2500" * 28,
        ]
        for p in rest:
            star = [s.replace('★', '') for s in p['signals'] if s.startswith('★')]
            star_str = f" \U0001F31F{'|'.join(star[:1])}" if star else ""
            zone_icon = sto_zone_icon(p['sto_k'])
            rest_lines.append(
                f"\u251C {p['name']} ({p['score']}\uC810) "
                f"STO:{p['sto_k']:.0f} {zone_icon} "
                f"{p['close']:,}\uC6D0({p['change']:+.1f}%){star_str}"
            )

        # 섹터 분포
        sector_cnt = {}
        for p in (top + rest):
            sec = p['sector']
            sector_cnt[sec] = sector_cnt.get(sec, 0) + 1
        top_sectors = sorted(sector_cnt.items(), key=lambda x: -x[1])[:5]

        rest_lines.append("")
        rest_lines.append(f"\U0001F4CA \uC139\uD130 \uBD84\uD3EC (TOP5)")
        rest_lines.append("\u2500" * 28)
        for sec, cnt in top_sectors:
            rest_lines.append(f"  {sec}: {cnt}\uC885\uBAA9")

        rest_lines.append("")
        rest_lines.append(f"\U0001F4C8 \uBC31\uD14c\uC2A4\uD2B8 \uC131\uACFC (8\uAC1C\uC6D4)")
        rest_lines.append(f"  \uC218\uC775: +128.2% | MDD: -10.7%")
        rest_lines.append(f"  \uC0E4\uD504\uBE44\uC728: 3.84 | \uC2B9\uB960: 52.3%")
        msgs.append("\n".join(rest_lines))

    return msgs


def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    print("=== 스캐너 v3 일간 추천 ===")
    token, chat_id = load_env()

    if not token or not chat_id:
        print("\u26A0 TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 미설정")
        send = False
    else:
        send = True
        print(f"텔레그램: chat_id={chat_id[:4]}...")

    top, rest, total = run_scan()

    print(f"\n선정: {total}종목 → TOP {len(top)}")
    print("-" * 60)

    for i, p in enumerate(top, 1):
        sigs = [s for s in p['signals'] if s.startswith('★')]
        sig_str = ', '.join(sigs) if sigs else ', '.join(p['signals'][:2])
        print(f"  {i:>2}. {p['name']:10s} ({p['code']}) 점수:{p['score']:>3} "
              f"STO:{p['sto_k']:.0f} {p['close']:>8,}원 ({p['change']:+.1f}%) {sig_str}")

    msgs = format_messages(top, rest, total)

    # ━━━ ETF 스캔 결과 로드 ━━━
    etf_result = load_etf_scan()
    if etf_result:
        etf_df, csv_name = etf_result
        etf_msg = format_etf_message(etf_df, csv_name)
        msgs.append(etf_msg)
        print(f"\nETF 시그널: {len(etf_df)}개 ({csv_name})")
        for _, row in etf_df.iterrows():
            print(f"  {row['name'][:20]:20s} {row['direction']:5s} "
                  f"점수:{row['score']:.0f} {row['close']:>10,.0f}원")
    else:
        print("\nETF 스캔 결과 없음 (results/etf_scan_*.csv)")

    if send:
        print(f"\n텔레그램 전송중... ({len(msgs)}장)")
        for i, msg in enumerate(msgs, 1):
            chunks = split_message(msg)
            for chunk in chunks:
                ok = send_telegram(token, chat_id, chunk)
                print(f"  Part {i} ({len(chunk)}ch): {'OK' if ok else 'FAIL'}")
                if not ok:
                    print(f"    전송 실패")
                time.sleep(0.8)
        print("\u2705 전송 완료!")
    else:
        print("\n--- 텔레그램 미전송 (토큰 미설정) ---")
        for i, msg in enumerate(msgs, 1):
            print(f"\n--- Part {i} ({len(msg)}ch) ---")
            print(msg)


if __name__ == "__main__":
    main()
