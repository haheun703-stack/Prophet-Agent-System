# -*- coding: utf-8 -*-
"""rotation_paper_scan — B/C 3-Type paper 스캐너 (6/6 구현, read-only 기록 전용).

B: ROTATION_PULLBACK_BUY  강한 섹터/그룹 안에서 눌림 매수
   넓게 병행: 고점대비 -3/-5/-7% · ma10/ma20 지지 · 거래대금 유지 전부 태깅·기록.
C: ROTATION_RIDE_BUY      강한 섹터/그룹 안에서 올라타기
   넓게 병행: 당일 +5/+7% 돌파 · 강한양봉+거래대금 증가 · 전고점/신고가 돌파 전부 태깅·기록.

설계 승인: docs/02-design/paper_training_3type_6_3.md (6/3 8장 '전부 승인 확정').

★ 원칙(불변): 실주문 0 / KIS 주문함수 호출 0 / scheduler·systemd 무접촉 / SAJANG 무변경(상수추가 X)
  / picks·asset_pool 불변 / 봇 OFF. record_order_intent 없음(주문 자체 없음). 기록 전용.
★ pykrx 우회: sector_relay는 SECTORS(섹터/그룹 구조)·_classify_status(분류 단일진실)만 재사용.
  모멘텀은 stock_data_daily/*.csv(KIS 일봉, load_daily)로 직접 계산 — pykrx 영구 막힘(6/4 재확인) 회피.
★★ 넓게 병행 기록: B/C 진입 파라미터 단일 확정 X → forward로 어느 임계가 돈 되는지 판정(A sdart 일관).
   "검증 없이 손대지 말것"(사장님 영구룰) — flip(선정 변경)은 관측 후 사장님 결정.

사용: python tools/rotation_paper_scan_6_6.py [--asof YYYY-MM-DD] [--selftest]
  --asof 없으면 오늘. 데이터가 그 날 이전까지면 자동으로 직전 거래일 기준(lookahead 없음).
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import date
from pathlib import Path

SA = Path(__file__).resolve().parent.parent
ROOT = SA.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
sys.path.insert(0, str(SA))
sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from data.paper_3type_ledger import new_ledger  # noqa: E402
from leader_prospective_scan_6_2 import load_daily, _ma, _is_excluded  # noqa: E402
# 6/8 관측 레이어(가격구조 feature/label) — 진입 조건 변경 아님, 기록·태그만(hard gate 0).
from price_structure_features_6_8 import (  # noqa: E402
    price_structure_labels, half_pullback, breakout, sector_peer_sync,
    gap_open_features, candle_shape_features)

# SECTORS·status 분류 = sector_relay 단일진실 재사용.
# (sector_relay가 `from pykrx import stock`을 모듈로드 시 실행하나, 우리는 데이터조회 함수는 호출 X.
#  import 자체는 패키지 설치만으로 성공 → 우회 성립. 실패 시 명시적 안내.)
try:
    from data.sector_relay import SECTORS, _classify_status  # noqa: E402
    _SECTOR_SRC = "sector_relay(단일진실)"
except Exception as e:  # pragma: no cover - 환경 의존
    SECTORS = {}
    _classify_status = None
    _SECTOR_SRC = f"import_failed:{e}"

STRONG = ("HOT", "WARMING", "RELAY")   # '강한 섹터/그룹' = 설계서 8장 status 넓게
HOLD_CAP = 40
FWD_H = (1, 3, 5, 10, 20)
TYPE_SEED = {"B": 35.0, "C": 35.0}     # 설계서 5장 A30/B35/C35 (A는 sdart 별도)
TP_TOUCH_LEVELS = (5.0, 10.0)          # 6/8 관측용 터치 레벨(★익절 아님 — 트레일링 only 영구룰 유지)

# data row 인덱스: 0=date 1=open 2=high 3=low 4=close 5=volume


# ── forward 부품 (sdart_shadow_record 동일 로직 — entry 기준, 미래없으면 reached=False) ──
def raw_fwd(d, i, entry):
    r = {}
    for h in FWD_H:
        k = i + h
        r[f"d{h}"] = round((d[k][4] - entry) / entry * 100, 2) if k < len(d) and entry > 0 else None
    return r


def mfe_mae(d, i, entry):
    path = d[i + 1:min(len(d), i + 1 + HOLD_CAP)]
    if not path or entry <= 0:
        return None, None
    return (round(max((x[2] - entry) / entry * 100 for x in path), 2),
            round(min((x[3] - entry) / entry * 100 for x in path), 2))


_MISS = {"date": None, "ret_pct": None, "reached": False}


def _hit(d, k, entry, px):
    return {"date": d[k][0], "ret_pct": round((px - entry) / entry * 100, 2), "reached": True}


def trail_stop(d, i, entry, drop_pct):
    """고점(peak) 대비 drop_pct% 트레일 손절."""
    if entry <= 0:
        return dict(_MISS)
    peak = entry
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        x = d[k]
        peak = max(peak, x[2])
        stop = peak * (1 - drop_pct / 100.0)
        if x[3] <= stop:
            px = min(x[1], stop) if x[1] <= stop else stop
            return _hit(d, k, entry, px)
    return dict(_MISS)


def exit_ma(d, i, entry, n):
    """종가가 maN 이탈 시 청산."""
    if entry <= 0:
        return dict(_MISS)
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        if d[k][4] < _ma(d, k, n):
            return _hit(d, k, entry, d[k][4])
    return dict(_MISS)


def tp_touch(d, i, entry, levels=TP_TOUCH_LEVELS):
    """★ 관측용 — +lvl% 가격에 '닿았는지'만 기록(익절 아님). 사장님 6/8: 트레일링 only
    영구룰 유지 — 그 자리서 팔았다고 가정 X, 청산은 would_stop/would_exit 중심. 미래없으면 reached False.
    would_take_profit(고정 TP 부활 오해) 금지 → tp_touch_observer(터치 관측)로 대체."""
    out = {}
    end = min(len(d), i + 1 + HOLD_CAP)
    for lvl in levels:
        if entry <= 0:
            out[f"+{lvl:g}%"] = {"reached": False, "date": None}
            continue
        target = entry * (1 + lvl / 100.0)
        hit = next((d[k][0] for k in range(i + 1, end) if d[k][2] >= target), None)
        out[f"+{lvl:g}%"] = {"reached": hit is not None, "date": hit}
    return out


# ── 종목/섹터 모멘텀 (pykrx 우회: 우리 일봉으로 직접 계산, sector_relay 로직 동일) ──
def _stock_state(d, i):
    """d[i] 기준 모멘텀. i<5면 None."""
    if i < 5:
        return None
    close = d[i][4]
    prev = d[i - 1][4]
    if close <= 0 or prev <= 0:
        return None
    c1d = (close / prev - 1) * 100
    c5d = (close / d[i - 5][4] - 1) * 100 if d[i - 5][4] > 0 else 0.0
    vol = d[i][5]
    vols = [d[k][5] for k in range(max(0, i - 5), i)]   # 직전5일(당일 제외) = sector_relay와 동일
    avg = sum(vols) / len(vols) if vols else vol
    vr = vol / avg if avg > 0 else 0.0
    return {"close": close, "c1d": c1d, "c5d": c5d, "vr": vr}


def _code_to_path():
    """stock_data_daily/{name}_{code}.csv → {code: path}."""
    m = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        code = os.path.basename(f)[:-4].rsplit("_", 1)[-1]
        m[code] = f
    return m


def scan_sectors(c2p, asof):
    """12섹터(SECTORS)를 우리 일봉으로 스캔 → status 판정. 반환 list[dict]."""
    out = []
    for sid, sec in SECTORS.items():
        stocks = []
        for tier, lst in (("leader", sec.get("leaders", [])),
                          ("mid", sec.get("mid", [])),
                          ("sobujan", sec.get("sobujan", []))):
            for code, name in lst:
                p = c2p.get(code) or c2p.get(str(code).zfill(6))
                if not p:
                    continue
                d = load_daily(p)
                if len(d) < 6:
                    continue
                i = next((j for j in range(len(d) - 1, -1, -1) if d[j][0] <= asof), None)
                if i is None or i < 5:
                    continue
                st = _stock_state(d, i)
                if not st:
                    continue
                stocks.append({"code": code, "name": name, "tier": tier,
                               "d": d, "i": i, **st})
        if not stocks:
            continue
        n = len(stocks)
        momentum_5d = sum(s["c5d"] for s in stocks) / n
        vol_ratio = sum(s["vr"] for s in stocks) / n
        breadth = sum(1 for s in stocks if s["c1d"] > 0) / n
        leaders = [s for s in stocks if s["tier"] == "leader"]
        sob = [s for s in stocks if s["tier"] == "sobujan"]
        la = sum(s["c5d"] for s in leaders) / len(leaders) if leaders else 0.0
        soa = sum(s["c5d"] for s in sob) / len(sob) if sob else 0.0
        relay_gap = la - soa
        status = _classify_status(momentum_5d, vol_ratio, breadth, relay_gap)
        out.append({
            "sid": sid, "name": sec["name"], "status": status,
            "momentum_5d": round(momentum_5d, 2), "vol_ratio": round(vol_ratio, 2),
            "breadth": round(breadth, 2), "relay_gap": round(relay_gap, 2),
            "n": n, "stocks": stocks,
        })
    return out


# ── B/C feature (넓게 병행: 모든 임계 동시 태깅) ──
def pullback_features(d, i):
    """B: 최근10일 고점 대비 눌림 + ma 지지 + 거래대금."""
    close = d[i][4]
    hi10 = max(x[2] for x in d[max(0, i - 10):i + 1])
    draw = (close / hi10 - 1) * 100 if hi10 > 0 else 0.0   # 음수 = 눌림 깊이
    ma10 = _ma(d, i, 10)
    ma20 = _ma(d, i, 20)
    turn = close * d[i][5] / 1e8
    return {
        "high10": round(hi10, 2), "drawdown_pct": round(draw, 2),
        "pullback_3": draw <= -3.0, "pullback_5": draw <= -5.0, "pullback_7": draw <= -7.0,
        "ma10_support": close >= ma10, "ma20_support": close >= ma20,
        "ma10": round(ma10, 2), "ma20": round(ma20, 2), "turnover_억": round(turn, 1),
    }


def ride_features(d, i):
    """C: 당일 강세/돌파. i<1이면 None(방어 — 호출처 보장에 미의존)."""
    if i < 1:
        return None
    close = d[i][4]
    prev = d[i - 1][4]
    op = d[i][1]
    high = d[i][2]
    chg = (close / prev - 1) * 100 if prev > 0 else 0.0
    past = d[max(0, i - 120):i]                                 # 직전120일(당일 제외)
    hi_close = max((x[4] for x in past), default=close)         # 과거 종가 최고
    hi_high = max((x[2] for x in past), default=high)           # 과거 장중 최고
    body = (close - op) / op * 100 if op > 0 else 0.0
    vols = [d[k][5] for k in range(max(0, i - 5), i)]
    avg = sum(vols) / len(vols) if vols else d[i][5]
    vr = d[i][5] / avg if avg > 0 else 0.0
    turn = close * d[i][5] / 1e8
    # 넓게 병행: 종가신고가 / 장중신고가 둘 다 기록 → forward로 어느 정의가 돈 되는지 판정
    nh_close = bool(close >= hi_close)
    nh_intraday = bool(high >= hi_high)
    return {
        "chg_pct": round(chg, 2), "break_5": chg >= 5.0, "break_7": chg >= 7.0,
        "strong_bull": bool(body >= 3.0 and vr >= 1.5),
        "new_high_close": nh_close, "new_high_intraday": nh_intraday,
        "new_high": bool(nh_close or nh_intraday),
        "body_pct": round(body, 2), "vol_ratio": round(vr, 2), "turnover_억": round(turn, 1),
        "high_close_120d": round(hi_close, 2), "high_120d": round(hi_high, 2),
    }


def collect_candidates(sectors):
    """강한 섹터 종목 → B/C 후보(type, kwargs) 리스트. capital은 2-pass에서 채움."""
    cands = []
    for sec in sectors:
        if sec["status"] not in STRONG:
            continue
        for s in sec["stocks"]:
            if _is_excluded(s["name"]):
                continue
            d, i = s["d"], s["i"]
            entry = d[i][4]
            mfe, mae = mfe_mae(d, i, entry)
            ps = price_structure_labels(d, i)              # 6/8: 주봉/월/반기 시가·첫봉투봉·연간과열
            base = dict(
                ticker=s["code"], name=s["name"], sector=sec["name"], group=s["tier"],
                virtual_entry_date=d[i][0], virtual_entry_price=round(entry, 2),
                entry_basis="T0_CLOSE",                    # 6/8 메타: B/C 현재 기준 명시(로직 불변)
                market_regime=sec["status"],
                sector_rotation_score=sec["momentum_5d"], group_rotation_score=sec["breadth"],
                MFE=mfe, MAE=mae, holding_days=0, supply=None,
                raw_fwd=raw_fwd(d, i, entry),
                sector_detail={"status": sec["status"], "momentum_5d": sec["momentum_5d"],
                               "vol_ratio": sec["vol_ratio"], "breadth": sec["breadth"],
                               "relay_gap": sec["relay_gap"], "tier": s["tier"]},
            )
            base.update(ps)                                # 가격구조 라벨(flat, hard gate 0)
            base.update(sector_peer_sync(sec["stocks"], s["code"]))   # 섹터 동조화(12-섹터 근사)
            base["tp_touch_observer"] = tp_touch(d, i, entry)   # 6/8 관측용 터치(★익절 아님)
            base["tp_touch_level_pct"] = list(TP_TOUCH_LEVELS)
            base["tp_touch_only"] = True                   # 트레일링 only 유지 — 터치 관측일뿐
            base.update(gap_open_features(d, i, ps.get("weekly_open")))   # 6/8 갭/시가 위치
            base.update(candle_shape_features(d, i))                       # 6/8 꼬리/종가 위치
            # B: 눌림 (-3% 이상 도달 = 후보)
            pf = pullback_features(d, i)
            if pf["pullback_3"]:
                kw = dict(base)
                depth = [t for t, ok in (("-3", pf["pullback_3"]), ("-5", pf["pullback_5"]),
                                         ("-7", pf["pullback_7"])) if ok]
                kw["entry_reason"] = (f"pullback {pf['drawdown_pct']}% [{'/'.join(depth)}] "
                                      f"ma10지지:{pf['ma10_support']} ma20지지:{pf['ma20_support']} "
                                      f"거래대금:{pf['turnover_억']}억")
                kw["features"] = pf
                kw.update(half_pullback(d, i, ps["weekly_open_state"],
                                        ps["half_year_open_state"]))   # 6/8: 하프 눌림(B 핵심)
                kw["would_stop"] = {"trail3": trail_stop(d, i, entry, 3.0),
                                    "trail5": trail_stop(d, i, entry, 5.0),
                                    "trail7": trail_stop(d, i, entry, 7.0)}
                kw["would_exit"] = {"ma10": exit_ma(d, i, entry, 10),
                                    "ma20": exit_ma(d, i, entry, 20)}
                cands.append(("B", kw, -pf["drawdown_pct"]))   # 정렬키: 깊은 눌림 우선
            # C: 돌파/강세
            rf = ride_features(d, i)
            if rf and (rf["break_5"] or rf["new_high"] or rf["strong_bull"]):
                kw = dict(base)
                tags = [t for t, ok in (("+5", rf["break_5"]), ("+7", rf["break_7"]),
                                        ("강한양봉", rf["strong_bull"]),
                                        ("종가신고가", rf["new_high_close"]),
                                        ("장중신고가", rf["new_high_intraday"])) if ok]
                kw["entry_reason"] = (f"ride {rf['chg_pct']}% [{'/'.join(tags)}] "
                                      f"양봉:{rf['body_pct']}% 거래량:{rf['vol_ratio']}x "
                                      f"거래대금:{rf['turnover_억']}억")
                kw["features"] = rf
                kw.update(breakout(d, i, sec["status"]))               # 6/8: 돌파(C 핵심)
                kw["would_stop"] = {"trail3": trail_stop(d, i, entry, 3.0),
                                    "trail5": trail_stop(d, i, entry, 5.0),
                                    "trail10": trail_stop(d, i, entry, 10.0)}
                kw["would_exit"] = {"ma5": exit_ma(d, i, entry, 5),
                                    "ma10": exit_ma(d, i, entry, 10)}
                cands.append(("C", kw, rf["chg_pct"]))         # 정렬키: 강한 돌파 우선
    return cands


def record_bc_into(led, cands):
    """cands(B/C)를 기존 ledger 인스턴스에 record (save 안 함 — 통합러너가 A와 함께 저장).
    capital 균등배분(2-pass): type별 시드(B35/C35)를 후보 수로 나눔."""
    by_type = {"B": [], "C": []}
    for t, kw, key in cands:
        by_type[t].append((key, kw))
    for t, lst in by_type.items():
        lst.sort(key=lambda x: x[0], reverse=True)
        n = len(lst)
        per = round(TYPE_SEED[t] / n, 2) if n else 0.0
        pct = round(100.0 / n, 1) if n else 0.0
        bucket = f"{t}_{int(TYPE_SEED[t])}"        # 6/8 메타: B_35 / C_35 (capital_bucket 명시)
        for _, kw in lst:
            kw["capital_allocated"] = per
            kw["position_size_pct"] = pct
            kw["capital_bucket"] = bucket
            kw["variant"] = t                      # 6/8 메타: variant=B/C 판정 라벨
            led.record(t, **kw)


def record_to_ledger(asof, cands):
    """B/C 단독 실행: 새 ledger 생성 → record → save(별도 파일 'bc_only').
    ★ S-2: 통합러너의 ledger_{date}.json(A/B/C)를 덮어쓰지 않게 분리 — 통합 ledger 무손상 ★.
    반환 (ledger, path)."""
    led = new_ledger(asof)
    record_bc_into(led, cands)
    path = led.save(suffix="bc_only")
    return led, path


# ─────────────────────────── selftest (합성 데이터, 네트워크 0) ───────────────────────────
def _synth_daily(base=10000.0, n=130, last_chg=0.0, drawdown=0.0):
    """완만상승 130일 일봉 합성. last_chg=마지막날 등락(%), drawdown=고점대비 눌림(%)."""
    d = []
    px = base
    for k in range(n - 1):
        px *= 1.003
        d.append((f"2026-01-{k:04d}", px, px * 1.01, px * 0.99, px, 100000))
    if drawdown:                       # 마지막날을 직전고점에서 drawdown% 끌어내림
        peak = max(x[2] for x in d[-10:])
        c = peak * (1 + drawdown / 100.0)
        d.append(("2026-06-04", c * 1.001, c * 1.002, c * 0.995, c, 90000))
    else:
        prev = d[-1][4]
        c = prev * (1 + last_chg / 100.0)
        o = prev if last_chg >= 0 else prev * 1.001
        d.append(("2026-06-04", o, max(o, c) * 1.001, min(o, c) * 0.99, c, 250000))
    return d


def selftest():
    ok = []
    ok.append(("T0 SECTORS·_classify_status import", bool(SECTORS) and _classify_status is not None))
    # 합성: C 후보(마지막날 +6%)
    dc = _synth_daily(last_chg=6.0)
    rf = ride_features(dc, len(dc) - 1)
    ok.append(("T1 ride +6% → break_5", rf["break_5"] is True and rf["break_7"] is False))
    # +6% 마지막날(완만상승 끝)은 종가신고가 = 직전 종가max 갱신 / new_high 키 분리 존재
    ok.append(("T1b new_high 종가·장중 분리 기록",
               "new_high_close" in rf and "new_high_intraday" in rf
               and rf["new_high"] == (rf["new_high_close"] or rf["new_high_intraday"])))
    ok.append(("T1c ride_features i<1 방어 → None", ride_features(dc, 0) is None))
    # 합성: B 후보(고점대비 -5% 눌림)
    db = _synth_daily(drawdown=-5.0)
    pf = pullback_features(db, len(db) - 1)
    ok.append(("T2 pullback -5% → pb3·pb5 True pb7 False",
               pf["pullback_3"] and pf["pullback_5"] and not pf["pullback_7"]))
    # forward 부품: 미래 없는 마지막날 → reached False
    entry = dc[-1][4]
    ok.append(("T3 trail_stop 미래없음 → reached False",
               trail_stop(dc, len(dc) - 1, entry, 3.0)["reached"] is False))
    ok.append(("T4 raw_fwd 미래없음 → d1 None", raw_fwd(dc, len(dc) - 1, entry)["d1"] is None))
    # forward: 중간 인덱스(미래 존재)
    i_mid = len(dc) - 5
    rfm = raw_fwd(dc, i_mid, dc[i_mid][4])
    ok.append(("T5 raw_fwd 미래있음 → d3 float", isinstance(rfm["d3"], float)))
    # _stock_state
    st = _stock_state(dc, len(dc) - 1)
    ok.append(("T6 _stock_state c1d ~ +6%", st is not None and 5.5 < st["c1d"] < 6.5))
    # ledger 통합 + extra 보존
    led = new_ledger("2026-06-04")
    led.record("C", ticker="000000", name="합성종목", sector="테스트", group="leader",
               virtual_entry_price=entry, entry_reason="ride +6%", market_regime="HOT",
               features=rf, raw_fwd=rfm, would_stop={"trail3": dict(_MISS)})
    row = led.candidates["C"][0]
    ok.append(("T7 ledger 17필드 기록", row["ticker"] == "000000" and row["type"] == "C"
               and row["signal_source"] == "paper:3type:C_ROTATION_RIDE"))
    ok.append(("T8 ledger extra 넓게병행 보존",
               "extra" in row and "features" in row["extra"] and "raw_fwd" in row["extra"]))
    # _classify_status 단일진실 동작
    if _classify_status:
        ok.append(("T9 _classify_status HOT", _classify_status(5.0, 2.0, 0.8, 1.0) == "HOT"))
        ok.append(("T10 _classify_status COLD", _classify_status(0.0, 1.0, 0.3, 0.0) == "COLD"))
    # S-2: rotation 단독 save(bc_only)가 통합 ledger(A 포함)를 덮어쓰지 않음 (날짜 2099=실관측 무간섭)
    import json as _json
    integ = new_ledger("2099-01-01")
    integ.record("A", ticker="000001", name="A합성", virtual_entry_price=100.0)
    integ.record("B", ticker="000002", name="B합성", virtual_entry_price=200.0)
    integ_path = integ.save()                     # ledger_2099-01-01.json (통합)
    bc = new_ledger("2099-01-01")
    bc.record("B", ticker="000003", name="B단독", virtual_entry_price=300.0)
    bc_path = bc.save(suffix="bc_only")           # ledger_2099-01-01_bc_only.json (단독)
    integ_after = _json.loads(integ_path.read_text(encoding="utf-8"))
    ok.append(("T11 S-2 단독save=별도파일(통합 무손상)",
               bc_path != integ_path and bc_path.name.endswith("_bc_only.json")))
    ok.append(("T12 S-2 통합 ledger A·B records 보존",
               integ_after["summary"]["A"] == 1 and integ_after["summary"]["B"] == 1))
    integ_path.unlink(missing_ok=True)
    bc_path.unlink(missing_ok=True)
    # 6/8 tp_touch 관측(★익절 아님 — 트레일링 only 유지, 닿았는지만)
    d_t = [("2026-01-01", 100, 100, 100, 100, 1), ("2026-01-02", 100, 100, 100, 100, 1),
           ("2026-01-03", 100, 100, 100, 100, 1), ("2026-01-06", 100, 106, 100, 105, 1)]
    tt = tp_touch(d_t, 2, 100.0)
    ok.append(("T13 tp_touch +5%도달·+10%미달(터치관측만)",
               tt["+5%"]["reached"] is True and tt["+10%"]["reached"] is False))
    ok.append(("T14 tp_touch 미래없음 → reached False",
               tp_touch(d_t, 3, 105.0)["+5%"]["reached"] is False))
    # 6/8 메타: record_bc_into capital_bucket(B_35/C_35)·variant(B/C)
    led_m = new_ledger("2026-06-04")
    record_bc_into(led_m, [("B", {"ticker": "1", "name": "b", "virtual_entry_price": 100}, 1.0),
                           ("C", {"ticker": "2", "name": "c", "virtual_entry_price": 200}, 1.0)])
    rb = led_m.candidates["B"][0]; rc = led_m.candidates["C"][0]
    ok.append(("T15 capital_bucket/variant (B_35·C_35 / B·C)",
               rb["capital_bucket"] == "B_35" and rb["variant"] == "B"
               and rc["capital_bucket"] == "C_35" and rc["variant"] == "C"))
    print("rotation_paper_scan 셀프테스트:")
    for nme, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
    return all(p for _, p in ok)


def _print_summary(asof, sectors, cands, path):
    strong = [s for s in sectors if s["status"] in STRONG]
    nb = sum(1 for t, _, _ in cands if t == "B")
    nc = sum(1 for t, _, _ in cands if t == "C")
    print("=" * 100)
    print(f"rotation_paper_scan — B/C ROTATION paper (asof {asof}, 넓게 병행 기록)")
    print(f"섹터소스: {_SECTOR_SRC} / 모멘텀: stock_data_daily(KIS 일봉, pykrx 우회)")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / 주문함수 0 "
          f"/ picks·asset_pool 불변 / SAJANG·scheduler·systemd 무변경")
    print(f"스캔 섹터 {len(sectors)} / 강한(HOT·WARMING·RELAY) {len(strong)} → B {nb}건 · C {nc}건")
    print(f"★ 단독 ledger(B/C only): {path}")
    print("  ※ A/B/C 통합 ledger는 paper_3type_daily_run_6_6.py 사용(이 단독 실행은 통합 ledger 무손상)")
    print("=" * 100)
    for s in sorted(sectors, key=lambda x: (x["status"] not in STRONG, -x["momentum_5d"])):
        mark = "★" if s["status"] in STRONG else " "
        print(f" {mark}[{s['status']:<8}] {s['name']:<10} 5D:{s['momentum_5d']:+.1f}% "
              f"거래:{s['vol_ratio']:.1f}x 상승:{s['breadth']:.0%} relay_gap:{s['relay_gap']:+.1f} (n={s['n']})")
    for t, label in (("B", "ROTATION_PULLBACK"), ("C", "ROTATION_RIDE")):
        rows = [kw for tt, kw, _ in cands if tt == t]
        if rows:
            print(f"\n[{t} · {label}] {len(rows)}건")
            for kw in rows:
                print(f"   {kw['name']:<14}({kw['sector']}/{kw['group']}) "
                      f"{kw['virtual_entry_price']:,.0f}원 — {kw['entry_reason']}")
    print("\n★★ shadow=기록만(주문 0). B/C 진입 임계는 forward ≥7거래일 누적 후 6/12 1차 판정 → 사장님 결정.")
    print("★★ 정직: B/C 백테근거 없음(A는 45건) → forward 자체가 1차 검증. 절대수익 신뢰 X. thin(7거래일).")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=date.today().isoformat())
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if not SECTORS or _classify_status is None:
        print(f"[중단] sector_relay import 실패: {_SECTOR_SRC}")
        sys.exit(2)
    try:                                   # zero-pad 보장(문자열 날짜 비교 오판 방지)
        asof = date.fromisoformat(a.asof).isoformat()
    except ValueError:
        print(f"[중단] --asof 형식 오류: {a.asof} (YYYY-MM-DD 필요)")
        sys.exit(2)
    c2p = _code_to_path()
    sectors = scan_sectors(c2p, asof)
    cands = collect_candidates(sectors)
    _, path = record_to_ledger(asof, cands)
    _print_summary(asof, sectors, cands, path)


if __name__ == "__main__":
    main()
