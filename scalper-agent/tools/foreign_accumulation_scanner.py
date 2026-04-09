"""
외국인 선행 매집 감지기 (Foreign Accumulation Scanner)
=======================================================

목적: SK텔레콤(4/2~4/7 조용한 축적 → 4/8~4/9 폭발) 같은
      외국인 선행 매집 패턴을 전체 유니버스에서 감지

감지 조건:
1. 외국인 최근 3~5일 순매수 우세 (3일 중 2일+ 양수)
2. 외국인 누적 금액 > 양수 (확실한 매집)
3. 주가 변동률 합계가 조용함 (±7% 이내) — "조용한 축적"
4. 거래량 급등 없음 (상위권이지만 아직 폭발 전)
5. 시총 2,000억 이상 (소형주 노이즈 제외)

스코어:
- 연속 매수일수 (최대 +20)
- 외국인 금액 / 시총 비율 (최대 +25)
- 조용함 점수 (낮은 변동률일수록 +10)
- 기관 합류 조짐 (마지막 날 기관 양수 +10)
- 개인 이탈 (개인 음수면 +5)

사용: python3 tools/foreign_accumulation_scanner.py [--top 15] [--nxt-send]
"""
from __future__ import annotations
import json
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd


# ─────────────────────────────────────────────
# 경로
# ─────────────────────────────────────────────
BASE = Path(__file__).resolve().parents[1]
FLOW_DIR = BASE / "data_store" / "flow"
DAILY_DIR = BASE / "data_store" / "daily"
UNIVERSE_PATH = BASE / "data_store" / "universe.json"
OUT_PATH = BASE / "data_store" / "foreign_accumulation.json"


# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
LOOKBACK_DAYS = 5         # 최근 N일 분석
MIN_BUY_DAYS = 3          # 최소 매수일수 (5일 중 3일+)
QUIET_THRESHOLD = 7.0     # 주가 변동률 합계 (±% 이내면 조용)
MIN_MCAP = 2000           # 시총 하한 (억원)
MAX_MCAP = 500000         # 시총 상한 (초대형주 제외, 삼성전자급)


def load_universe() -> dict:
    """universe.json 로드"""
    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_investor_flow(code: str) -> pd.DataFrame | None:
    """investor.csv 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        if len(df) < LOOKBACK_DAYS:
            return None
        # 컬럼 체크
        required = ["date", "외국인_수량", "기관_수량", "개인_수량",
                    "외국인_금액", "기관_금액", "개인_금액", "종가"]
        if not all(c in df.columns for c in required):
            return None
        return df.tail(LOOKBACK_DAYS).copy()
    except Exception:
        return None


def score_stock(code: str, name: str, mcap: float, flow: pd.DataFrame) -> dict | None:
    """종목 스코어링"""
    try:
        # 1) 외국인 매수일수
        foreign_amt = flow["외국인_금액"].astype(float).values  # 백만원
        foreign_qty = flow["외국인_수량"].astype(float).values
        buy_days = int((foreign_amt > 0).sum())

        if buy_days < MIN_BUY_DAYS:
            return None

        # 2) 외국인 누적 순매수 금액 (백만원 → 억원)
        foreign_total = float(foreign_amt.sum()) / 100.0  # 억원
        if foreign_total <= 0:
            return None

        # 3) 외국인 금액 / 시총 비율
        ratio = (foreign_total / mcap) * 100  # %

        # 4) 주가 변동률 — "조용함" 체크
        closes = flow["종가"].astype(float).values
        if closes[0] <= 0:
            return None
        total_change_pct = ((closes[-1] / closes[0]) - 1) * 100
        # 조용하려면 절대값이 낮아야 함
        if abs(total_change_pct) > QUIET_THRESHOLD:
            # 이미 많이 움직인 것 — 바스켓 매집 단계 아님
            return None

        # 5) 일별 변동률 합계 (절대값)
        daily_volatility = 0.0
        for i in range(1, len(closes)):
            if closes[i-1] > 0:
                daily_volatility += abs((closes[i] / closes[i-1]) - 1) * 100

        # 6) 기관 합류 조짐 (마지막 2일 중 1일+ 양수)
        inst_amt = flow["기관_금액"].astype(float).values
        inst_joining = int((inst_amt[-2:] > 0).sum())

        # 7) 개인 이탈 (개인 음수 비율)
        indiv_amt = flow["개인_금액"].astype(float).values
        indiv_out_days = int((indiv_amt < 0).sum())

        # ─── 스코어 계산 ───
        score = 0.0

        # A) 매수일수 (최대 +20)
        score += buy_days * 4  # 3일=12, 4일=16, 5일=20

        # B) 외국인 금액/시총 비율 (최대 +25)
        if ratio >= 1.0:
            score += 25
        elif ratio >= 0.5:
            score += 20
        elif ratio >= 0.3:
            score += 15
        elif ratio >= 0.15:
            score += 10
        else:
            score += 5

        # C) 조용함 보너스 (최대 +15)
        # 변동률 합계가 ±3% 이내면 최고점
        quiet_score = max(0, 15 - abs(total_change_pct) * 2)
        score += quiet_score

        # D) 기관 합류 (최대 +10)
        score += inst_joining * 5

        # E) 개인 이탈 (최대 +5)
        if indiv_out_days >= 3:
            score += 5
        elif indiv_out_days >= 2:
            score += 3

        # 금액 단위 조정 (백만원 → 억원) 확인:
        # CSV는 백만원 단위 (flow_collector 기준)
        # → foreign_total은 이미 /100 했으므로 억원

        return {
            "code": code,
            "name": name,
            "mcap_억": round(mcap, 0),
            "score": round(score, 1),
            "buy_days": buy_days,
            "foreign_total_억": round(foreign_total, 1),
            "ratio_mcap_%": round(ratio, 3),
            "price_change_%": round(total_change_pct, 2),
            "quiet_score": round(quiet_score, 1),
            "inst_joining": inst_joining,
            "indiv_out_days": indiv_out_days,
            "close_start": int(closes[0]),
            "close_end": int(closes[-1]),
            "dates": flow["date"].tolist(),
            "foreign_daily_억": [round(x/100, 1) for x in foreign_amt.tolist()],
            "inst_daily_억": [round(x/100, 1) for x in inst_amt.tolist()],
        }
    except Exception as e:
        return None


def scan_universe(top_n: int = 15) -> list[dict]:
    """전체 유니버스 스캔"""
    uni = load_universe()
    print(f"📊 유니버스 종목 수: {len(uni)}")

    candidates = []
    skipped_no_flow = 0
    skipped_mcap = 0
    skipped_score = 0

    for code, info in uni.items():
        if not isinstance(info, dict):
            continue
        name = info.get("name", "")
        mcap = info.get("cap_억", 0) or info.get("market_cap_억", 0) or info.get("mcap", 0) or 0

        if mcap < MIN_MCAP or mcap > MAX_MCAP:
            skipped_mcap += 1
            continue

        flow = load_investor_flow(code)
        if flow is None:
            skipped_no_flow += 1
            continue

        result = score_stock(code, name, mcap, flow)
        if result is None:
            skipped_score += 1
            continue

        candidates.append(result)

    print(f"  - 시총필터 제외: {skipped_mcap}")
    print(f"  - flow 없음: {skipped_no_flow}")
    print(f"  - 스코어 미달: {skipped_score}")
    print(f"  ✅ 후보 감지: {len(candidates)}")

    # 스코어 순 정렬
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:top_n]


def format_telegram_message(candidates: list[dict]) -> str:
    """텔레그램 메시지 포맷"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"🎯 <b>외국인 선행 매집 감지</b>",
        f"📅 {now}",
        f"━━━━━━━━━━━━━━━━━━",
        f"💡 SK텔레콤(4/2~4/7 조용한 축적→4/8~4/9 폭발) 패턴",
        f"",
    ]

    for i, c in enumerate(candidates, 1):
        emoji = "🏆" if i <= 3 else "💎" if i <= 7 else "⭐"
        lines.append(
            f"{emoji} <b>{i}. {c['name']}</b> ({c['code']}) — <b>{c['score']}점</b>"
        )
        lines.append(
            f"  └ 외국인 {c['buy_days']}/{LOOKBACK_DAYS}일 +{c['foreign_total_억']:.0f}억 "
            f"(시총대비 {c['ratio_mcap_%']:.2f}%)"
        )
        lines.append(
            f"  └ 주가 {c['close_start']:,} → {c['close_end']:,} "
            f"({c['price_change_%']:+.1f}% 조용함)"
        )
        if c['inst_joining'] > 0:
            lines.append(
                f"  └ 🔥 기관 합류 조짐 (최근 2일 중 {c['inst_joining']}일)"
            )
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️ 참고: 조용한 축적 구간 — 아직 폭발 전")
    lines.append("⚠️ NXT 야간 상위권 후보로 검토 권장")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=15, help="TOP N 후보")
    parser.add_argument("--nxt-send", action="store_true", help="NXT 텔레그램 전송")
    parser.add_argument("--save", action="store_true", help="JSON 저장")
    args = parser.parse_args()

    print("🔍 외국인 선행 매집 감지기 시작")
    print(f"   Lookback: {LOOKBACK_DAYS}일, 최소 매수일: {MIN_BUY_DAYS}일")
    print(f"   조용함 임계값: ±{QUIET_THRESHOLD}%, 시총: {MIN_MCAP}~{MAX_MCAP}억")
    print()

    candidates = scan_universe(top_n=args.top)

    print()
    print("=" * 80)
    print(f"🏆 TOP {len(candidates)} 외국인 선행 매집 종목")
    print("=" * 80)
    for i, c in enumerate(candidates, 1):
        print(f"{i:2}. [{c['score']:5.1f}] {c['name']:20} ({c['code']}) "
              f"| 매수 {c['buy_days']}/{LOOKBACK_DAYS}일 "
              f"| +{c['foreign_total_억']:7.1f}억 "
              f"| {c['ratio_mcap_%']:5.2f}% "
              f"| 주가 {c['price_change_%']:+5.1f}% "
              f"| 기관합류 {c['inst_joining']}")

    if args.save:
        out = {
            "updated": datetime.now().isoformat(),
            "config": {
                "lookback_days": LOOKBACK_DAYS,
                "min_buy_days": MIN_BUY_DAYS,
                "quiet_threshold": QUIET_THRESHOLD,
                "min_mcap": MIN_MCAP,
                "max_mcap": MAX_MCAP,
            },
            "candidates": candidates,
        }
        OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n💾 저장: {OUT_PATH}")

    if args.nxt_send:
        try:
            from bot.telegram_bot import send_telegram_message
            msg = format_telegram_message(candidates)
            send_telegram_message(msg, parse_mode="HTML")
            print(f"\n📤 텔레그램 전송 완료")
        except Exception as e:
            print(f"\n⚠️ 텔레그램 전송 실패: {e}")
            # Fallback
            try:
                from bot.telegram_bot import send_message
                send_message(msg)
                print("📤 텔레그램 전송 완료 (fallback)")
            except Exception as e2:
                print(f"⚠️ 재시도 실패: {e2}")

    return candidates


if __name__ == "__main__":
    main()
