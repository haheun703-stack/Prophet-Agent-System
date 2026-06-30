# -*- coding: utf-8 -*-
"""페이퍼 전용 포트폴리오 시뮬 (6/27 신설) — 봇 OFF여도 매일 도는 가상 매매 루프.

배경:
  - 봇 OFF(AUTO_TRADE_DISABLED)로 실제 paper_portfolio 매매가 5/29부터 정지.
  - paper_3type ledger(봇 무관·매일 nightly ④ 생성)로 가상 포트폴리오를 복리로 굴려
    breadth 게이트 효과를 "실계좌처럼" 누적수익률·MDD로 검증한다.
  - "관측만"이 아니라 현행(baseline) vs breadth게이트 두 가상계좌를 나란히 굴려 비교.

★ 안전: read-only(ledger 읽기만)·실주문0·라이브0·picks불변·SAJANG 매도헬퍼 미접촉(룰값만 참조).
손절 근사: MAE ≤ -TRAILING_PCT → 손절(-TRAILING), 아니면 forward_d3 보유 청산.
  (일봉 OHLC 정밀 트레일링/재진입은 다음 정밀화 — MVP는 ledger forward+MAE 근사.)
사용법: python -m data.paper_sim_portfolio
"""
from __future__ import annotations

import csv as _csv
import json
import logging
from datetime import datetime
from pathlib import Path

from data.paper_rule_shadow import _cohorts
from data.sajang_rules import SAJANG

logger = logging.getLogger("BH.PaperSim")

OUT_PATH = Path(__file__).resolve().parent.parent / "data_store" / "paper_sim_portfolio.json"
DAILY_DIR = Path(__file__).resolve().parent.parent / "data_store" / "daily"

TOP_K = 7              # 매일 진입 종목 수(단타봇 09:15 5 + 14:50 2 가정)
INVEST_RATIO = 0.7     # 자본 투입 비율(SAJANG 30% 현금보유 룰 반영)
MEONGBUN_SRS_TH = 10.0  # 명분 근사 임계 — sector_rotation_score(섹터강도) 이상만 진입
                        # (2순위 6/30 A: '명분 있는 끼' 대리지표. 진짜 catalyst 명분등급 결합은 B 축적 후)


def _load_post_bars(ticker: str, entry_date: str, max_days: int = 5) -> list:
    """daily/{ticker}.csv(한글헤더 날짜/시가/고가/저가/종가)에서
    entry_date 다음 영업일부터 max_days개 OHLC 일봉을 시간순(오름차순)으로 반환.
    파일 없거나 파싱 실패 시 빈 리스트(→ 호출부에서 MFE 근사 fallback)."""
    p = DAILY_DIR / f"{ticker}.csv"
    if not p.exists():
        return []
    ed = str(entry_date).replace("-", "")
    rows = []
    try:
        with p.open(encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                d = str(row.get("날짜", "")).replace("-", "")
                if not d or d <= ed:
                    continue
                try:
                    rows.append({
                        "date": row.get("날짜"),
                        "open": int(float(row.get("시가") or 0)),
                        "high": int(float(row.get("고가") or 0)),
                        "low": int(float(row.get("저가") or 0)),
                        "close": int(float(row.get("종가") or 0)),
                    })
                except (ValueError, TypeError):
                    continue
    except Exception:  # noqa: BLE001
        return []
    rows.sort(key=lambda x: str(x["date"]).replace("-", ""))  # 시간순 보장
    return rows[:max_days]


def _stock_pnl(c) -> float:
    """종목 청산 pnl(%) — OHLC 일봉 정밀 트레일링(고점추적·순서보존).
    daily CSV가 있으면 simulate_trailing_with_ohlc로 진입가 대비 정밀 청산가를 산출
    (MFE/MAE 시간순서 무시 낙관편향 제거). CSV 없거나 부족하면 MFE/MAE 근사 fallback."""
    ticker = c.get("ticker")
    ed = c.get("virtual_entry_date")
    ep = c.get("virtual_entry_price")
    if ticker and ed and ep and ep > 0:
        post_bars = _load_post_bars(str(ticker), str(ed), max_days=5)
        if post_bars:
            from tools.portfolio_daily_simulator_5_24 import simulate_trailing_with_ohlc
            sim = simulate_trailing_with_ohlc(
                int(ep), post_bars,
                max_hold_days=5, sl_pct=SAJANG.TRAILING_PCT / 100.0,
            )
            if sim.get("exited") and sim.get("exit_reason") != "no_data":
                return float(sim.get("pnl_pct", 0.0))
    return _stock_pnl_mfe_approx(c)


def _stock_pnl_mfe_approx(c) -> float:
    """MFE/MAE 근사 fallback(구 MVP). OHLC CSV 미존재/부족 종목 전용:
    +TRAIL 고점 도달 → 고점-TRAIL 익절 / 그 전 MAE≤-TRAIL → -TRAIL 손절 / else d3 만기.
    (★MFE/MAE 시간순서 무시 = 낙관편향. CSV 있으면 _stock_pnl이 OHLC 정밀로 대체)"""
    trail = SAJANG.TRAILING_PCT
    mfe = c.get("MFE")
    mae = c.get("MAE")
    if mfe is not None and mfe >= trail:
        return round(mfe - trail, 2)       # 고점 트레일링 익절(이익 구간)
    if mae is not None and mae <= -trail:
        return -trail                      # 진입가 -TRAIL 손절
    d3 = c.get("forward_d3")
    return d3 if d3 is not None else 0.0    # 보유 만기 청산(d3)


def _stock_pnl_reentry(c) -> float:
    """종목 청산 pnl(%) — 손절+재진입(reentry_shadow._simulate·SAJANG 단일진실).
    손절 후 reclaim(종가≥직전 손절선)+should_reenter면 풀비중 재진입 → leg 복리 합성.
    일봉 부족/미해결(None) 시 _stock_pnl(손절만 OHLC) graceful fallback.
    (2번 6/30 — 검증된 '손절+재진입 > 버티기 +5~6%p'를 페이퍼에 반영)"""
    ticker = c.get("ticker")
    ed = c.get("virtual_entry_date")
    if ticker and ed:
        try:
            from data.sector_reversal_shadow import _daily_rows
            from data.reentry_shadow import _simulate
            rows = _daily_rows(str(ticker))
            edk = str(ed)[:10]
            di = next((i for i, r in enumerate(rows) if str(r[0])[:10] == edk), None)
            if di is not None:
                sim = _simulate(rows, di, c.get("kki_grade", "B"))
                if sim and sim.get("stop_reenter_ret") is not None:
                    return float(sim["stop_reenter_ret"])
        except Exception:  # noqa: BLE001
            pass
    return _stock_pnl(c)  # fallback: 손절만 OHLC


def simulate(cohorts: dict, gate_fn, top_k: int = TOP_K,
             invest_ratio: float = INVEST_RATIO, pnl_fn=None) -> dict:
    """가상 포트폴리오 복리 시뮬. gate_fn(c)=True인 후보만 진입.
    pnl_fn=None → _stock_pnl(손절만 OHLC). _stock_pnl_reentry → 손절+재진입."""
    if pnl_fn is None:
        pnl_fn = _stock_pnl
    cash = 1.0
    peak = 1.0
    mdd = 0.0
    win_days = n_days = 0
    trades = wins = 0
    for date in sorted(cohorts):
        picks = [c for c in cohorts[date] if gate_fn(c)]
        if not picks:
            continue
        sel = picks[:top_k]
        pnls = [pnl_fn(c) for c in sel]
        for p in pnls:
            trades += 1
            if p > 0:
                wins += 1
        day_ret = (sum(pnls) / len(pnls)) / 100.0 * invest_ratio
        cash *= (1 + day_ret)
        n_days += 1
        if day_ret > 0:
            win_days += 1
        peak = max(peak, cash)
        mdd = min(mdd, cash / peak - 1)
    return {
        "final_equity": round(cash, 4),
        "total_return_pct": round((cash - 1) * 100, 2),
        "mdd_pct": round(mdd * 100, 2),
        "n_days": n_days,
        "win_day_pct": round(100 * win_days / n_days, 0) if n_days else None,
        "trades": trades,
        "win_pct": round(100 * wins / trades, 0) if trades else None,
    }


def build_paper_sim(save: bool = True) -> dict:
    """현행(baseline) vs breadth게이트 두 가상 포트폴리오 복리 비교."""
    cohorts = _cohorts()
    if not cohorts:
        logger.info("[paper_sim] 코호트 없음 — skip")
        return {"cohorts": 0}

    # 검수 M2 투명화: OHLC 정밀 적용 vs MFE 근사 fallback 종목 수 카운트
    n_total = n_ohlc = 0
    for _picks in cohorts.values():
        for _c in _picks:
            n_total += 1
            _tk, _ed, _ep = _c.get("ticker"), _c.get("virtual_entry_date"), _c.get("virtual_entry_price")
            if _tk and _ed and _ep and _ep > 0 and _load_post_bars(str(_tk), str(_ed), 5):
                n_ohlc += 1

    base = simulate(cohorts, lambda c: True)
    gated = simulate(cohorts, lambda c: c.get("_breadth_state") == "BROAD_UP")
    # 2순위(6/30 A): breadth(언제 사나=시장폭) + 명분근사(뭘 사나=sector_rotation_score 섹터강도 대리) 결합
    gated_mb = simulate(
        cohorts,
        lambda c: c.get("_breadth_state") == "BROAD_UP"
        and (c.get("sector_rotation_score") or 0) >= MEONGBUN_SRS_TH,
    )
    # 2번(6/30): 손절+재진입 — 현행(손절만) vs 재진입 / breadth+재진입 직접비교
    reentry = simulate(cohorts, lambda c: True, pnl_fn=_stock_pnl_reentry)
    breadth_reentry = simulate(
        cohorts, lambda c: c.get("_breadth_state") == "BROAD_UP",
        pnl_fn=_stock_pnl_reentry,
    )

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cohorts": len(cohorts),
        "config": {"top_k": TOP_K, "invest_ratio": INVEST_RATIO,
                   "stop_model": f"OHLC 정밀 트레일링(고점추적·순서보존·SL -{SAJANG.TRAILING_PCT}%·max_hold 5d) "
                                 f"+ CSV없으면 MFE/MAE 근사 fallback",
                   "coverage": f"OHLC정밀 {n_ohlc}/{n_total}종목 (나머지 MFE근사 fallback)",
                   "meongbun_proxy": f"sector_rotation_score>={MEONGBUN_SRS_TH} (섹터강도=명분 근사·2순위 A)"},
        "baseline": base,
        "breadth_gated": gated,
        "breadth_meongbun_gated": gated_mb,
        "reentry": reentry,
        "breadth_reentry": breadth_reentry,
        "note": "페이퍼 전용 가상 포트폴리오(record-only·봇무관·매일). 실주문0·라이브0. "
                "OHLC 일봉 정밀 트레일링 적용(6/30 정밀화) — MFE/MAE 시간순서무시 낙관편향 제거. "
                "단 미투입현금(invest_ratio 0.7 근사)·재진입은 아직 미반영 → 절대수익은 보수적 참고용, "
                "상대비교가 본 신호(절대수익=생존편향). "
                "breadth_gated=시장폭(언제), breadth_meongbun_gated=시장폭+섹터강도(언제+뭘 근사·2순위 A 관측축적). "
                "★명분근사=sector_rotation_score 대리 — 진짜 catalyst 명분등급 결합은 B 축적 후. "
                "CSV 미존재 종목만 MFE/MAE 근사 fallback(_stock_pnl_mfe_approx). "
                "reentry/breadth_reentry=손절+재진입(reentry_shadow._simulate·SAJANG 단일진실·일봉부족시 손절만 fallback). "
                "★2번(6/30): 재진입이 손절만 대비 수익 견인 — VPS 실측 baseline-6.24→reentry+0.75(+6.99%p)·breadth+재진입 +3.17%(승71%).",
    }
    if save:
        try:
            OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[paper_sim] 저장 실패: {e}")

    logger.info(
        f"[paper_sim] baseline {base['total_return_pct']}%(승{base['win_pct']}%) "
        f"vs breadth {gated['total_return_pct']}%(승{gated['win_pct']}%) "
        f"vs breadth+명분 {gated_mb['total_return_pct']}%(승{gated_mb['win_pct']}%·{gated_mb['trades']}거래)"
    )
    return out


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    r = build_paper_sim()
    if r.get("cohorts"):
        b, g = r["baseline"], r["breadth_gated"]
        m = r.get("breadth_meongbun_gated", {})
        print(f"\n=== 페이퍼 전용 가상 포트폴리오 ({r['cohorts']}일) ===")
        print(f"현행:         누적 {b['total_return_pct']:+}% | MDD {b['mdd_pct']}% | 승률 {b['win_pct']}% | {b['trades']}거래")
        print(f"breadth:      누적 {g['total_return_pct']:+}% | MDD {g['mdd_pct']}% | 승률 {g['win_pct']}% | {g['trades']}거래")
        print(f"breadth+명분: 누적 {m.get('total_return_pct')}% | 승률 {m.get('win_pct')}% | {m.get('trades')}거래")
        re = r.get("reentry", {})
        br = r.get("breadth_reentry", {})
        print(f"재진입:       누적 {re.get('total_return_pct')}% | 승률 {re.get('win_pct')}%")
        print(f"breadth+재진입: 누적 {br.get('total_return_pct')}% | 승률 {br.get('win_pct')}%")
        print("\n★ record-only·봇무관·실주문0. OHLC 트레일링 + 명분근사 + 손절재진입 관측.")
