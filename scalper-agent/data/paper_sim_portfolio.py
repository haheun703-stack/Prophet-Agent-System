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


# ── 7/4 진실회복: 재진입 kki 버그 fix ──────────────────────────────
# 종전: _simulate(rows, di, c.get("kki_grade", "B")) — ledger 후보에 kki_grade 필드가 없어
# 항상 "B"(SAJANG._KKI_GRADE_RANK에 없는 값=rank 0)가 전달 → should_reenter 항상 False
# → "재진입" 코호트가 실제 재진입 0회(실체=보유 3일 단축 청산 모델)였음.
# fix: 진입일 기준 끼를 일봉에서 직접 계산(SAJANG.kki_grade·score_kki 단일진실 재사용,
# early_variant_shadow와 동일 입력 구성·consecutive_limit=0 단순화 동일). 계산 불가 시
# SLUGGISH(재진입 불허=보수적)로 종전 실효 동작과 동일하게 fallback.

_UNIVERSE_CAP: dict | None = None
_KKI_CACHE: dict = {}
# 7/4 검수 fix: 재진입 코호트 4개가 같은 후보를 재평가해도 (ticker,진입일) 고유 단위로만
# 집계(set/dict) — 종전 int 누적은 최대 4배 인플레이션이었음. _simulate는 결정적이라
# 같은 키의 n_reentries는 항상 동일값(dict 덮어쓰기 안전).
_REENTRY_STATS = {"sims": set(), "with_reentry": set(), "reentries": {},
                  "kki_computed": set(), "kki_fallback": set()}


def _reset_reentry_stats():
    _REENTRY_STATS["sims"] = set()
    _REENTRY_STATS["with_reentry"] = set()
    _REENTRY_STATS["reentries"] = {}
    _REENTRY_STATS["kki_computed"] = set()
    _REENTRY_STATS["kki_fallback"] = set()


def _reentry_stats_out() -> dict:
    return {
        "unique_evaluated": len(_REENTRY_STATS["sims"]),
        "unique_with_reentry": len(_REENTRY_STATS["with_reentry"]),
        "total_reentries": sum(_REENTRY_STATS["reentries"].values()),
        "kki_computed": len(_REENTRY_STATS["kki_computed"]),
        "kki_fallback": len(_REENTRY_STATS["kki_fallback"]),
    }


def _cap_억(ticker: str) -> float:
    """universe.json 시총(억) — 회전율(kki F3)용. 없으면 0(보수적)."""
    global _UNIVERSE_CAP
    if _UNIVERSE_CAP is None:
        try:
            u = json.loads((DAILY_DIR.parent / "universe.json").read_text(encoding="utf-8"))
            _UNIVERSE_CAP = {k: float(v.get("cap_억") or 0) for k, v in u.items()}
        except Exception:  # noqa: BLE001
            _UNIVERSE_CAP = {}
    return _UNIVERSE_CAP.get(str(ticker), 0.0)


def _kki_grade_at(ticker: str, entry_date: str) -> str | None:
    """진입일 기준 끼 등급 — daily CSV(OHLCV)만으로 SAJANG 단일진실 재계산.
    바 부족(<22)/파일 없음 → None (호출부에서 SLUGGISH 보수 처리)."""
    key = (str(ticker), str(entry_date)[:10])
    if key in _KKI_CACHE:
        return _KKI_CACHE[key]
    grade = None
    try:
        from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
        from data.watchlist_continuation import _atr_pct
        p = DAILY_DIR / f"{ticker}.csv"
        if p.exists():
            # 7/4 검수 fix: 행 단위 원자 파싱(전 필드 성공 시에만 동시 append) + 날짜 정렬 —
            # 종전 dates 선append는 손상 행 1개로 이후 전체 인덱스가 조용히 어긋나는 잠복 결함.
            bars = []
            with p.open(encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    try:
                        d = str(row.get("날짜", ""))[:10]
                        h = float(row.get("고가") or 0)
                        lo = float(row.get("저가") or 0)
                        cl = float(row.get("종가") or 0)
                        v = float(row.get("거래량") or 0)
                    except (ValueError, TypeError):
                        continue
                    if d:
                        bars.append((d, h, lo, cl, v))
            bars.sort(key=lambda x: x[0])
            dates = [b[0] for b in bars]
            highs = [b[1] for b in bars]
            lows = [b[2] for b in bars]
            closes = [b[3] for b in bars]
            vols = [b[4] for b in bars]
            edk = str(entry_date)[:10]
            i = next((k for k in range(len(dates) - 1, -1, -1) if dates[k] == edk), None)
            if i is not None and i >= 21:
                atrp = _atr_pct(highs, lows, closes, i)
                surge, limit = count_surge_limit_days(closes[:i + 1])
                h0, l0, c0, v0 = highs[i], lows[i], closes[i], vols[i]
                cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
                vma = sum(vols[i - 20:i]) / 20.0
                volx = v0 / vma if vma > 0 else 0.0
                tv_억 = c0 * v0 / 1e8
                cap = _cap_억(ticker)
                st = LimitUpStock(
                    code=str(ticker), name="", close=int(c0),
                    volume_ratio=round(volx, 2), trading_value_억=round(tv_억, 1),
                    turnover_pct=round(tv_억 / cap * 100, 2) if cap else 0.0,
                    close_strength=round(cs, 3), consecutive_limit=0,
                )
                grade = SAJANG.kki_grade(score_kki(st, atrp, surge, limit))
    except Exception:  # noqa: BLE001
        grade = None
    _KKI_CACHE[key] = grade
    return grade


def _stock_pnl_reentry(c) -> float:
    """종목 청산 pnl(%) — 손절+재진입(reentry_shadow._simulate·SAJANG 단일진실).
    손절 후 reclaim(종가≥직전 손절선)+should_reenter면 풀비중 재진입 → leg 복리 합성.
    끼 등급은 진입일 기준 실계산(_kki_grade_at·7/4 fix). 계산 불가 시 SLUGGISH(재진입 불허).
    일봉 부족/미해결(None) 시 _stock_pnl(손절만 OHLC) graceful fallback."""
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
                key = (str(ticker), edk)
                kg = c.get("kki_grade") or _kki_grade_at(str(ticker), edk)
                if kg:
                    _REENTRY_STATS["kki_computed"].add(key)
                else:
                    _REENTRY_STATS["kki_fallback"].add(key)
                sim = _simulate(rows, di, kg or "SLUGGISH")
                if sim and sim.get("stop_reenter_ret") is not None:
                    _REENTRY_STATS["sims"].add(key)
                    nre = int(sim.get("n_reentries") or 0)
                    if nre > 0:
                        _REENTRY_STATS["with_reentry"].add(key)
                        _REENTRY_STATS["reentries"][key] = nre
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

    # 7/4 진실회복: 재진입 통계 리셋(끼 실계산 fix 검증용 — build마다 초기화·고유키 집계)
    _reset_reentry_stats()

    base = simulate(cohorts, lambda c: True)
    # ★7/4 정직 라벨: 아래 breadth/명분 게이트는 "당일(D0) 종가" 값 = 장마감 후에야 아는 정보.
    # T0_CLOSE(당일종가 진입) 프레임 안에서만 정합 — 아침 진입에 매핑하면 look-ahead(이론상한).
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
    # 3중 결합(6/30): breadth(언제)+명분(뭘·섹터강도)+재진입(어떻게)
    breadth_mb_reentry = simulate(
        cohorts,
        lambda c: c.get("_breadth_state") == "BROAD_UP"
        and (c.get("sector_rotation_score") or 0) >= MEONGBUN_SRS_TH,
        pnl_fn=_stock_pnl_reentry,
    )

    # ── 7/4 신설: 전일(D-1) breadth 게이트 = 구현가능판(look-ahead 0) ──
    # 레짐검증(63거래일): 전일 breadth≤0.45 → 익일 DOWN precision 51.6%(base 42.2%)·
    # -3% 손절터치 26~28% 감소. UP 예측은 안 됨 → 이 게이트는 "회피 전용" 엣지.
    # 임계 단일진실 = market_regime_gate.NO_GO_BREADTH (검수 M1)
    from data.market_regime_gate import NO_GO_BREADTH as _d1_th

    def _d1_gate(c):
        b = c.get("_d1_breadth_pct")
        return not (b is not None and b <= _d1_th)

    d1_gated = simulate(cohorts, _d1_gate)
    d1_reentry = simulate(cohorts, _d1_gate, pnl_fn=_stock_pnl_reentry)

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
        "breadth_meongbun_reentry": breadth_mb_reentry,
        "d1_breadth_gated": d1_gated,
        "d1_breadth_reentry": d1_reentry,
        "reentry_stats": _reentry_stats_out(),
        "note": "페이퍼 전용 가상 포트폴리오(record-only·봇무관·매일). 실주문0·라이브0. "
                "OHLC 일봉 정밀 트레일링(6/30) — MFE/MAE 낙관편향 제거. invest_ratio 0.7 근사. "
                "절대수익은 보수적 참고용, 상대비교가 본 신호(절대수익=생존편향). "
                "★7/4 정직 라벨: breadth_gated/breadth_meongbun_*는 D0(당일종가) 정보 게이트 = "
                "T0_CLOSE 프레임 한정 정합·아침 진입 매핑 시 look-ahead → '이론상한'으로만 읽을 것. "
                "구현가능판은 d1_breadth_*(전일 breadth≤0.45 스킵·look-ahead 0·회피 전용 엣지, "
                "7/4 레짐검증 63거래일: DOWN precision 51.6% vs base 42.2%·손절터치 -26~28%·UP예측 불가). "
                "★7/4 재진입 fix: 종전 kki_grade 미존재→'B'→should_reenter 항상 False(재진입 0회) 버그 수정 — "
                "진입일 끼를 일봉에서 실계산(SAJANG 단일진실). reentry_stats로 실제 재진입 횟수 투명 공개. "
                "종전 '재진입 +6.99%p' 주장은 재진입 아닌 보유3일 단축 효과였음(기록 정정). "
                "명분근사=sector_rotation_score 대리(D0값) — 진짜 catalyst 명분등급 결합은 B 축적 후. "
                "CSV 미존재 종목만 MFE/MAE 근사 fallback(_stock_pnl_mfe_approx).",
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
        bmr = r.get("breadth_meongbun_reentry", {})
        print(f"★3중(breadth+명분+재진입): 누적 {bmr.get('total_return_pct')}% | 승률 {bmr.get('win_pct')}% (D0정보=이론상한)")
        d1g = r.get("d1_breadth_gated", {})
        d1r = r.get("d1_breadth_reentry", {})
        rs = r.get("reentry_stats", {})
        print(f"★구현가능 D-1게이트:  누적 {d1g.get('total_return_pct')}% | 승률 {d1g.get('win_pct')}% | {d1g.get('trades')}거래")
        print(f"★구현가능 D-1+재진입: 누적 {d1r.get('total_return_pct')}% | 승률 {d1r.get('win_pct')}% | {d1r.get('trades')}거래")
        print(f"재진입 실통계(고유 종목·진입일): 평가 {rs.get('unique_evaluated')}건 중 재진입 발생 "
              f"{rs.get('unique_with_reentry')}건 (총 {rs.get('total_reentries')}회 / "
              f"끼 실계산 {rs.get('kki_computed')}·fallback {rs.get('kki_fallback')})")
        print("\n★ record-only·봇무관·실주문0. D0 게이트=이론상한 / d1_*=구현가능판(7/4 정직화).")
