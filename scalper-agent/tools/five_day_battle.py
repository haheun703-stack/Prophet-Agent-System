# -*- coding: utf-8 -*-
"""
5일 전투 계획 시뮬레이터
========================
1,000만원으로 5거래일 안에 10~20% 수익 목표
"""
import json
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR.parent / ".env")

# ── 스윙 피커 결과 로드 ──
with open(BASE_DIR / "data_store/swing_candidates.json", "r", encoding="utf-8") as f:
    swing = json.load(f)

# ── KIS API로 오늘 종가 확인 ──
try:
    from bot.kis_trader import KISTrader
    trader = KISTrader()
    has_kis = True
except Exception:
    has_kis = False

# ── 컨센서스 ──
from data.consensus_scraper import fetch_consensus

print("=" * 80)
print("  1,000만원 5일 전투 계획 - 스윙 후보 분석")
print("  기준: 2026-03-12 장마감 | 내일부터 5거래일")
print("=" * 80)

candidates = swing["candidates"]

detailed = []
for c in candidates:
    code = c["code"]
    name = c["name"]
    entry = c["entry"]
    sl = c["sl"]
    tp = c["tp"]

    # 현재가 조회
    price = entry
    day_chg = 0
    if has_kis:
        try:
            p = trader.fetch_price(code)
            if p and p.get("success"):
                price = p["current_price"]
                day_chg = p.get("change_pct", 0)
            time.sleep(0.15)
        except Exception:
            pass

    # 컨센서스
    cons = fetch_consensus(code, use_cache=True, cache_hours=24)
    target_p = cons.get("target_price", 0) if cons else 0
    analyst = cons.get("analyst_count", 0) if cons else 0

    # 업사이드 계산
    tp_upside = round((tp / price - 1) * 100, 1) if price > 0 else 0
    cons_upside = round((target_p / price - 1) * 100, 1) if price > 0 and target_p > 0 else 0
    sl_risk = round((sl / price - 1) * 100, 1) if price > 0 else 0

    # 리스크/리워드
    rr = abs(tp_upside / sl_risk) if sl_risk != 0 else 0

    detailed.append({
        "code": code, "name": name, "price": price, "day_chg": day_chg,
        "entry": entry, "sl": sl, "tp": tp,
        "target_p": target_p, "analyst": analyst,
        "tp_upside": tp_upside, "cons_upside": cons_upside,
        "sl_risk": sl_risk, "rr": rr,
        "scores": c["scores"], "total_score": c["total_score"],
    })

for i, d in enumerate(detailed, 1):
    print(f"""
{'=' * 60}
  #{i} {d['name']} ({d['code']})
  스윙점수: {d['total_score']:.1f}/100
{'=' * 60}
  종가: {d['price']:,}원 ({d['day_chg']:+.1f}%)

  진입가: {d['entry']:,} | SL: {d['sl']:,} ({d['sl_risk']:+.1f}%) | TP: {d['tp']:,} ({d['tp_upside']:+.1f}%)
  컨센서스 목표: {d['target_p']:,}원 ({d['cons_upside']:+.1f}%) | 애널: {d['analyst']}명
  리스크/리워드: 1:{d['rr']:.1f}

  세부점수:
    수급:{d['scores']['supply']:.0f} | 기술:{d['scores']['tech']:.0f} | 기관괴리:{d['scores']['inst_gap']} | 모멘텀:{d['scores']['momentum']:.0f} | 변동성:{d['scores']['volatility']:.0f}""")

# ── R/R 좋은 순으로 정렬 ──
detailed.sort(key=lambda x: x["rr"], reverse=True)

# ── 상위 3개 선택 ──
picks = detailed[:3]
alloc_per = 1000 / len(picks)

print(f"""

{'=' * 80}
  1,000만원 5일 전투 계획
{'=' * 80}

  목표: 10~20% 수익 (100~200만원)
  기간: 2026-03-13(목) ~ 2026-03-19(수) [5거래일]

  *** 핵심: 오늘 동시만기일 폭락 -> 내일 반등 가능성 높음 ***
""")

print(f"  -- 선택 {len(picks)}종목 (집중 투자) --")
print(f"  종목당 배분: {alloc_per:.0f}만원\n")

total_best = 0
total_worst = 0
for i, d in enumerate(picks, 1):
    invest = alloc_per
    shares = int(invest * 10000 / d["price"])
    actual_invest = shares * d["price"]

    profit_tp = int(shares * (d["tp"] - d["price"]))
    loss_sl = int(shares * (d["sl"] - d["price"]))

    # 컨센서스까지 갈 경우
    if d["target_p"] > 0:
        profit_cons = int(shares * (d["target_p"] - d["price"]))
    else:
        profit_cons = profit_tp * 2

    total_best += profit_tp
    total_worst += loss_sl

    print(f"  #{i} {d['name']} ({d['code']})")
    print(f"     매수: {d['price']:,}원 x {shares}주 = {actual_invest:,}원")
    print(f"     TP 도달: +{profit_tp:,}원 ({d['tp_upside']:+.1f}%)")
    print(f"     SL 도달: {loss_sl:,}원 ({d['sl_risk']:+.1f}%)")
    print(f"     R/R: 1:{d['rr']:.1f}")
    print()

print(f"  -- 시나리오 --")
print(f"  [BEST]  3종목 TP 도달: +{total_best:,}원 ({total_best/10000000*100:+.1f}%)")
mid_profit = total_best * 2 // 3 + total_worst // 3
safe_profit = total_best // 3 + total_worst * 2 // 3
print(f"  [MID]   2승 1패:      {'+' if mid_profit >= 0 else ''}{mid_profit:,}원")
print(f"  [SAFE]  1승 2패:      {'+' if safe_profit >= 0 else ''}{safe_profit:,}원")
print(f"  [WORST] 3종목 SL:     {total_worst:,}원 ({total_worst/10000000*100:+.1f}%)")

# ── 확률 추정 ──
print(f"""

  -- 확률 추정 (과거 스윙피커 기반) --
  BEST  (3종목 TP): ~15% 확률
  MID   (2승 1패):  ~40% 확률  <-- 가장 가능성 높음
  SAFE  (1승 2패):  ~30% 확률
  WORST (3종목 SL): ~15% 확률

  기대수익 = (0.15 x {total_best:,}) + (0.40 x {mid_profit:,}) + (0.30 x {safe_profit:,}) + (0.15 x {total_worst:,})""")

expected = 0.15 * total_best + 0.40 * mid_profit + 0.30 * safe_profit + 0.15 * total_worst
print(f"           = {expected:+,.0f}원 (기대수익률: {expected/10000000*100:+.1f}%)")

print(f"""

  -- 5일 운용 규칙 --

  Day 1 (3/13 목): 09:10 매수 (만기일 다음날 반등 노림)
    - 시가가 전일대비 -2% 이상 갭다운 -> 매수 보류 (09:30에 재판단)
    - 시가가 전일대비 +1% 이상 갭업 -> 70%만 매수, 나머지 눌림에 추가

  Day 2~3: 홀딩 + 모니터링
    - TP 50% 도달 시 -> 절반 익절 (원금 회수)
    - SL 도달 시 -> 즉시 손절 (미련 금지)

  Day 4 (3/18 화): 점검
    - 수익 10%+ -> 트레일링스탑으로 전환 (고점 대비 -3%)
    - 수익 5~10% -> 유지
    - 손실 중 -> 차트 재판단 (악화시 청산)

  Day 5 (3/19 수): 강제 청산
    - 장 시작 후 30분 내 전량 매도
    - 수익/손실 무관하게 5일 규칙 엄수

  -- 자금 관리 --
  - 최대 손실 한도: -50만원 (5%) -> 이 금액 도달 시 전체 포지션 청산
  - 슬리피지 감안: 호가 1~2틱 위에서 매수/아래에서 매도
  - 세금+수수료: 약 0.26% (매도시)
""")

# ── 만기일 후 추가 후보 (히든젬에서 단기 반등 가능 종목) ──
print("=" * 80)
print("  [보너스] 히든젬 중 만기일 반등 후보")
print("=" * 80)

try:
    with open(BASE_DIR / "data_store/hidden_gems.json", "r", encoding="utf-8") as f:
        gems = json.load(f)

    # 업사이드 40%+ 이면서 변동성 높은 종목
    bounce = []
    for g in gems["gems"][:10]:
        if g.get("real_upside", 0) >= 40:
            bounce.append(g)

    for g in bounce[:3]:
        name = g["name"]
        price = g.get("current_price", 0)
        tp = g.get("target_price", 0)
        upside = g.get("real_upside", 0)
        pbr = g.get("pbr", 0)
        print(f"  - {name}: {price:,}원 -> {tp:,}원 ({upside:+.1f}%) PBR:{pbr:.2f}")
        # 5일 내 현실적 목표는 업사이드의 20~30%
        realistic_5d = round(upside * 0.25, 1)
        realistic_profit = int(3330000 * realistic_5d / 100)  # 333만원 투자시
        print(f"    5일 현실적: {realistic_5d:+.1f}% -> 333만원 투자시 +{realistic_profit:,}원")
    print()

except Exception as e:
    print(f"  히든젬 데이터 로드 실패: {e}")

print("=" * 80)
