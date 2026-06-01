# -*- coding: utf-8 -*-
"""트레일 고점 해상도 — 휩쏘 측정 shadow (6/1 반보, 라이브 무접촉).

배경: realtime_monitor._update_trailing(line 401-402)는 30초 폴링 현재가로 high_since_entry를
매번 갱신 → 단 1회 장중 스파이크가 손절선을 영구히 끌어올려, 추세 멀쩡해도 눌림에서 잘림(휩쏘).
#2(분봉 실측, 노트북)가 이 해상도 artifact를 드러냄: 일봉고점 5.8x → 분봉고점 0.5x 헤드룸 붕괴.

이 모듈 = ★라이브 동작 안 건드림★. 현재 트레일(tick고점)과 대안 트레일(smoothed고점/버퍼)을
같은 가격경로에 나란히 돌려 **휩쏘 차이를 측정**만 함. 증거 쌓이면 그때 사장님 결정으로 flip.

핵심 가설: 30초 raw max 대신 **K폴 스무딩 고점**을 쓰면 1회 스파이크가 손절선을 못 끌어올림
→ 휩쏘 감소. 단 추세반전 시엔 약간 늦게 잘림(트레이드오프). 실데이터로 K·buffer 튜닝은 노트북.

사용:
  python bot/trailing_shadow_6_1.py --selftest    # 로직 검증(데이터 불필요) → 5/5 PASS
  (실측: 노트북에서 분봉/30초 경로로 compare_modes 호출 → 휩쏘율 old vs new)
"""
from __future__ import annotations
import argparse


# 라이브 현행 값 미러 (realtime_monitor): 추후 flip 시 전부 SAJANG 경유 (shadow 단계라 모듈상수)
TRAIL_PCT = 3.0            # 고점 -3% (사장님 5/25 일관 룰)
ARM_PCT = 10.0            # 트레일 발동 수익% (_trailing_start_pct)
SMOOTH_K = 5             # 스무딩 고점 윈도우(폴 수) — 대안. 실데이터로 튜닝(노트북)
BUFFER_PCT = 0.0         # 추가 손절 여유%(ATR 대용). 0=스무딩만. 실데이터로 튜닝


def live_trail_sl(high_watermark: float, trail_pct: float = TRAIL_PCT) -> float:
    """현행 라이브: SL = 고점 × (1 - trail%). (realtime_monitor line 414 동일)"""
    return high_watermark * (1 - trail_pct / 100.0)


def smoothed_high(prices: list, k: int = SMOOTH_K) -> list:
    """각 시점까지의 '스무딩 고점' 시퀀스. 고점을 raw max가 아니라 K폴 이동평균의 max로.

    → 1회 스파이크는 K창 평균을 거의 못 올림 → 고점 미갱신 → 손절선 안 끌려올라감.
    """
    out = []
    hw = prices[0]
    for i in range(len(prices)):
        window = prices[max(0, i - k + 1): i + 1]
        sm = sum(window) / len(window)
        hw = max(hw, sm)
        out.append(hw)
    return out


def confirmed_high(prices: list, n: int = 3) -> list:
    """시간확인 고점: '최근 n폴 동안 held된 레벨'(=윈도 min)만 고점으로 인정.

    1폴 스파이크는 윈도 min을 못 올림(나머지 n-1폴이 낮음) → 고점 미갱신 → 손절선 안 끌림.
    스무딩과 차이: 지속 추세는 n폴 후 '완전히' 따라잡음(평균으로 영구히 깎이지 않음) → 지연 페널티↓.
    """
    out = []
    hw = prices[0]
    for i in range(len(prices)):
        window = prices[max(0, i - n + 1): i + 1]
        level = min(window)        # n폴 연속 이 레벨 이상 유지됨 = 확인된 고점 후보
        hw = max(hw, level)
        out.append(hw)
    return out


def simulate_trail(path: list, entry: float, mode: str,
                   trail_pct: float = TRAIL_PCT, arm_pct: float = ARM_PCT,
                   k: int = SMOOTH_K, buffer_pct: float = BUFFER_PCT) -> dict:
    """가격경로에 트레일 1회 시뮬. mode='tick'(현행) | 'smoothed' | 'confirmed'(시간확인).

    반환: exit_idx/exit_price/exited + max_after_exit(청산 후 최고가 — 휩쏘 판정용).
    트레일은 수익 arm_pct 도달 후에만 발동(라이브 동일). 손절선은 위로만 래칫.
    """
    if mode == "smoothed":
        highs = smoothed_high(path, k)
    elif mode == "confirmed":
        highs = confirmed_high(path, k)
    else:  # tick = 현행 라이브 (매 폴 raw max)
        highs = []
        hw = path[0]
        for p in path:
            hw = max(hw, p)
            highs.append(hw)

    sl = 0.0
    armed = False
    for i, p in enumerate(path):
        pnl = (p - entry) / entry * 100
        if pnl >= arm_pct:
            armed = True
        if armed:
            cand = highs[i] * (1 - (trail_pct + buffer_pct) / 100.0)
            sl = max(sl, cand)          # 손절선 위로만 래칫(라이브 line 415)
            if p <= sl:
                after = max(path[i + 1:]) if i + 1 < len(path) else p
                return {"exited": True, "exit_idx": i, "exit_price": p,
                        "exit_pnl": pnl, "max_after_exit": after}
    return {"exited": False, "exit_idx": None, "exit_price": path[-1],
            "exit_pnl": (path[-1] - entry) / entry * 100, "max_after_exit": path[-1]}


def compare_modes(path: list, entry: float, **kw) -> dict:
    """현행(tick) vs 대안(smoothed) 나란히 + 휩쏘 판정.

    휩쏘 = 청산했는데 이후 가격이 청산가보다 trail_pct 이상 회복(잘렸는데 추세 살아있었음).
    """
    tp = kw.get("trail_pct", TRAIL_PCT)
    tick = simulate_trail(path, entry, "tick", **kw)
    smooth = simulate_trail(path, entry, "smoothed", **kw)

    def whipsawed(r):
        if not r["exited"]:
            return False
        return r["max_after_exit"] >= r["exit_price"] * (1 + tp / 100.0)

    return {"tick": tick, "smoothed": smooth,
            "tick_whipsaw": whipsawed(tick), "smoothed_whipsaw": whipsawed(smooth)}


def _selftest() -> int:
    """합성 경로로 핵심 로직 검증(데이터 불필요)."""
    checks = []
    entry = 100.0

    # 경로: +10%(발동)→ 잠깐 +20% 스파이크 1폴 → +12% 복귀 다수 → 눌림 +12.5%로 회복·상승
    # tick: 스파이크 120이 고점→SL=116.4→복귀 112≤116.4 휩쏘. smoothed: 평균이라 120 무시→생존
    path = [100, 105, 110, 112, 112, 112, 120, 112, 112, 112, 113, 125, 130]

    cmp = compare_modes(path, entry)
    # 1) 현행(tick)은 스파이크 후 복귀에서 휩쏘 청산
    checks.append(("tick 휩쏘 청산", cmp["tick"]["exited"] and cmp["tick_whipsaw"]))
    # 2) 스무딩은 스파이크 무시 → 생존(추세 끝까지)
    checks.append(("smoothed 생존(휩쏘X)", not cmp["smoothed_whipsaw"]))
    # 3) 스무딩 최종이 tick 청산가보다 높음(휩쏘 안 당해 더 벎)
    checks.append(("smoothed 결과 우위",
                   cmp["smoothed"]["exit_price"] > cmp["tick"]["exit_price"]))
    # 4) 스무딩 고점은 스파이크(120)에 안 끌려올라감
    sh = smoothed_high(path, SMOOTH_K)
    checks.append(("스무딩 고점 < 스파이크", max(sh) < 120))
    # 5) 추세 없는 평탄 경로는 둘 다 청산 안 함(거짓 휩쏘 0)
    flat = [100, 101, 100, 101, 100, 101, 100]
    cf = compare_modes(flat, entry)
    checks.append(("평탄=청산X 둘다", not cf["tick"]["exited"] and not cf["smoothed"]["exited"]))

    # 6) 시간확인(confirmed): 단일스파이크(120) 거름 → 고점<120 & 휩쏘X 생존
    ch = confirmed_high(path, 3)
    conf = simulate_trail(path, entry, "confirmed", k=3)
    conf_wh = conf["exited"] and conf["max_after_exit"] >= conf["exit_price"] * (1 + TRAIL_PCT / 100)
    checks.append(("confirmed 스파이크 거름(고점<120)", max(ch) < 120))
    checks.append(("confirmed 휩쏘X 생존", not conf_wh))

    ok = sum(1 for _, c in checks if c)
    for name, c in checks:
        print(f"  [{'PASS' if c else 'FAIL'}] {name}")
    print(f"\n셀프테스트: {ok}/{len(checks)} PASS")
    print(f"  (tick 청산 {cmp['tick']['exit_price']:.0f}(+{cmp['tick']['exit_pnl']:.0f}%) "
          f"vs smoothed 청산 {cmp['smoothed']['exit_price']:.0f}(+{cmp['smoothed']['exit_pnl']:.0f}%))")
    return 0 if ok == len(checks) else 1


# ── 실데이터 러너 (노트북 6/1) — 봇 close 경로로 현행 vs 스무딩 휩쏘율 실측 ──

def _run_real() -> int:
    import glob
    import os
    import statistics
    import sys as _sys
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent.parent
    _sys.path.insert(0, str(root / "scalper-agent" / "tools"))
    try:
        from reentry_cost_sensitivity_6_1 import _load, _events, HOLD_K
        from reentry_intraday_precision_6_1 import _load_min
    except Exception as e:
        print(f"[!] 분봉/일봉 로더 import 실패: {e}", file=_sys.stderr)
        return 2
    daily_dir = root / "stock_data_daily"
    min_dir = root / "scalper-agent" / "data_store" / "1min"
    min_files = {os.path.basename(f).replace(".csv", ""): f
                 for f in glob.glob(str(min_dir / "*.csv"))}
    if not min_files:
        print(f"[!] 분봉 없음: {min_dir} — 노트북/VPS 에서 실행. 로직검증=--selftest", file=_sys.stderr)
        return 2
    K_GRID = [3, 5, 10]
    n = 0
    tick_wh = 0
    tick_pnls = []
    sm_wh = {k: 0 for k in K_GRID}
    sm_pnls = {k: [] for k in K_GRID}
    cf_wh = {k: 0 for k in K_GRID}
    cf_pnls = {k: [] for k in K_GRID}
    for f in glob.glob(str(daily_dir / "*.csv")):
        code = os.path.basename(f).replace(".csv", "").split("_")[-1]
        if code not in min_files:
            continue
        rows, dates = _load(f)
        if len(rows) < 30:
            continue
        mbars = _load_min(min_files[code])
        if not mbars:
            continue
        mdates = sorted({b[0] for b in mbars})
        mlo, mhi = mdates[0], mdates[-1]
        for e in _events(rows):
            end = min(e + HOLD_K, len(rows) - 1)
            wd = [dates[d] for d in range(e + 1, end + 1)]
            if not wd or not (mlo <= wd[0] and wd[-1] <= mhi):
                continue
            wset = set(wd)
            path = [b[5] for b in mbars if b[0] in wset]   # 봇 폴 근사 = 1분봉 close 시퀀스
            if len(path) < 10:
                continue
            entry = rows[e][3]
            n += 1
            t = simulate_trail(path, entry, "tick")
            tick_pnls.append(t["exit_pnl"])
            if t["exited"] and t["max_after_exit"] >= t["exit_price"] * (1 + TRAIL_PCT / 100):
                tick_wh += 1
            for k in K_GRID:
                s = simulate_trail(path, entry, "smoothed", k=k)
                sm_pnls[k].append(s["exit_pnl"])
                if s["exited"] and s["max_after_exit"] >= s["exit_price"] * (1 + TRAIL_PCT / 100):
                    sm_wh[k] += 1
                cs = simulate_trail(path, entry, "confirmed", k=k)
                cf_pnls[k].append(cs["exit_pnl"])
                if cs["exited"] and cs["max_after_exit"] >= cs["exit_price"] * (1 + TRAIL_PCT / 100):
                    cf_wh[k] += 1
    if n == 0:
        print("[!] 비교가능 이벤트 0건", file=_sys.stderr)
        return 2
    print(f"트레일 휩쏘 실측: {n} 이벤트 (봇 close 경로 근사, 분봉 ~6주)\n")
    print(f"  {'방식':<16}{'휩쏘율':>9}{'평균실현수익%':>14}")
    tmean = statistics.mean(tick_pnls)
    print(f"  {'현행 tick고점':<14}{tick_wh / n * 100:>8.1f}%{tmean:>14.2f}")
    for k in K_GRID:
        print(f"  {'스무딩 K=' + str(k):<14}{sm_wh[k] / n * 100:>8.1f}%"
              f"{statistics.mean(sm_pnls[k]):>14.2f}")
    for k in K_GRID:
        print(f"  {'시간확인 N=' + str(k):<13}{cf_wh[k] / n * 100:>8.1f}%"
              f"{statistics.mean(cf_pnls[k]):>14.2f}")
    print()
    twr = tick_wh / n * 100
    # 합격 = 휩쏘 줄이면서 실현수익 안 깎임(≥ 현행) — 데이터가 판정, 예측X
    print("판정 (합격조건 = 휩쏘↓ AND 실현수익 ≥ 현행, 데이터가 판정):")
    print(f"  현행 tick: 휩쏘 {twr:.1f}% / 수익 {tmean:+.2f}%")

    def _judge(label, wh, pnls):
        for k in K_GRID:
            w = wh[k] / n * 100
            p = statistics.mean(pnls[k])
            ok = (w < twr) and (p >= tmean - 0.01)
            print(f"  {label} {k}: 휩쏘 {w:.1f}% / 수익 {p:+.2f}% "
                  f"→ {'합격(휩쏘↓·수익유지)' if ok else ('수익깎임' if w < twr else '휩쏘안줆')}")
    _judge("스무딩", sm_wh, sm_pnls)
    _judge("시간확인", cf_wh, cf_pnls)
    print("  → 합격 mode 있으면 트레일 flip 후보(사장님 결정+게이트+매도회귀). 없으면 -3%/구조 재검(사장님).")
    print("★ thin(분봉 ~6주·단일regime) + close경로(봇 30초보다 거침=휩쏘 과소가능) → 방향성·상대비교만.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    return _run_real()


if __name__ == "__main__":
    raise SystemExit(main())
