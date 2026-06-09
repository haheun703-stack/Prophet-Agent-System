# -*- coding: utf-8 -*-
"""Sector reversal shadow recorder (6/9 사장님 지시) — 6/10~6/12 자동 적재.

배경: 강한섹터(HOT) 판정이 momentum_5d(5일창)이라 급락 후 V자 당일반전을
하루 늦게 잡는다(6/9 반도체 RS+7.7·거래대금1위인데 5d=COLD 미선정).
→ HOT_5D(기존 5일 선정)와 REVERSAL_D0(당일 급반등·HOT 미선정)을 둘 다 shadow로
기록하고, 6/12에 forward 수익을 비교해 "다음주 실전 보조점수(+5) 반영 여부"를 판정한다.

★★★ 안전 불변식 (사장님 6/9) ★★★
- 실매수 로직 변경 0 / HOT 섹터 판정 변경 0 / 후보 tier 변경 0
- sector_relay는 read-only로만 사용 (scan_all_sectors)
- 출력은 별도 누적 json(sector_reversal_shadow.json) — ledger/picks/SAJANG/order path 무접촉
- forward/verdict는 placeholder(None)로 시작 → update_forward()가 일봉 쌓인 만큼 채움

실행(러너):
  python -c "from data.sector_reversal_shadow import build_shadow,update_forward; build_shadow('2026-06-09'); update_forward()"
"""
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
DAILY_DIR = BASE / "data_store" / "daily"
SHADOW_OUT = BASE / "data_store" / "sector_reversal_shadow.json"

# paper B/C가 실제 "강한섹터"로 선정하는 5일 status (HOT 단독 아님 — 메모리: HOT+WARMING+RELAY 넓게).
# 6/9처럼 폭락 직후엔 순수 HOT이 0개라, paper 선정 기준과 맞춰야 HOT_5D vs REVERSAL_D0 비교가 성립.
SELECTED_5D_STATUSES = ("HOT", "WARMING", "RELAY")
# REVERSAL_D0 판정 임계 (당일 급반등 + 5일 미선정)
REV_RS_MIN = 3.0        # 시장(섹터유니버스 평균) 대비 상대강도 +3.0%p 이상
REV_BREADTH_MIN = 0.6   # 당일 상승종목 비율 0.6 이상
FWD_OFFSETS = (1, 3, 5)
MFE_MAE_WINDOW = 5      # D+1~D+5


def _daily_rows(code: str):
    """일봉 csv → [(date, close, high, low), ...] (거래일순, read-only)."""
    f = DAILY_DIR / f"{code}.csv"
    if not f.exists():
        return []
    try:
        lines = f.read_text(encoding="utf-8").strip().split("\n")[1:]  # skip header
        out = []
        for ln in lines:
            p = ln.split(",")
            if len(p) >= 5:
                out.append((p[0], float(p[4]), float(p[2]), float(p[3])))  # date,close,high,low
        return out
    except Exception:
        return []


def _turnover(code: str, asof: str) -> float:
    f = DAILY_DIR / f"{code}.csv"
    if not f.exists():
        return 0.0
    try:
        p = f.read_text(encoding="utf-8").strip().split("\n")[-1].split(",")
        if p[0] == asof and len(p) >= 6:
            return float(p[4]) * float(p[5])  # 종가 × 거래량
    except Exception:
        pass
    return 0.0


def _load() -> dict:
    if SHADOW_OUT.exists():
        try:
            return json.loads(SHADOW_OUT.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"records": [], "note": "sector reversal shadow (관측 전용, 매수 무접촉)"}


def _save(data: dict) -> None:
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SHADOW_OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def build_shadow(asof: str, save: bool = True) -> dict:
    """asof 당일의 HOT_5D / REVERSAL_D0 섹터를 shadow로 기록(누적 merge).

    forward 필드는 None으로 시작 — update_forward()가 나중에 채운다.
    """
    from data.sector_relay import scan_all_sectors  # read-only

    sectors = scan_all_sectors(asof)
    base = []
    for st in sectors:
        if not getattr(st, "stocks", None):
            continue
        ranked = sorted(st.stocks, key=lambda s: -s.change_1d)
        d0 = sum(s.change_1d for s in st.stocks) / len(st.stocks)
        turn = sum(_turnover(s.code, asof) for s in st.stocks)
        base.append({
            "sector": st.sector_name,
            "d0_return": round(d0, 2),
            "breadth_1d": round(st.breadth, 2),
            "volume_ratio": round(st.vol_ratio, 2),
            "momentum_5d": round(st.momentum_5d, 2),
            "status_5d": st.status,
            "candidate_count": len(st.stocks),
            "representative_codes": [s.code for s in ranked[:3]],
            "_turnover": turn,
        })

    if not base:
        return {"date": asof, "records": [], "note": "no sectors"}

    # market proxy = 섹터 유니버스 d0 평균 (코스피 지수 일봉 부재 → 근사)
    market_return = round(sum(b["d0_return"] for b in base) / len(base), 2)
    for i, b in enumerate(sorted(base, key=lambda x: -x["_turnover"])):
        b["turnover_rank"] = i + 1

    records = []
    for b in base:
        rs = round(b["d0_return"] - market_return, 2)
        selected = b["status_5d"] in SELECTED_5D_STATUSES   # paper 5일 선정(HOT/WARMING/RELAY)
        reversal = (not selected) and rs >= REV_RS_MIN and b["breadth_1d"] >= REV_BREADTH_MIN
        if not (selected or reversal):
            continue  # 두 그룹(HOT_5D / REVERSAL_D0)만 기록
        records.append({
            "date": asof,
            "sector": b["sector"],
            "source_type": "HOT_5D" if selected else "REVERSAL_D0",
            "d0_return": b["d0_return"],
            "market_return": market_return,
            "relative_strength": rs,
            "breadth_1d": b["breadth_1d"],
            "turnover_rank": b["turnover_rank"],
            "volume_ratio": b["volume_ratio"],
            "momentum_5d": b["momentum_5d"],
            "status_5d": b["status_5d"],
            "selected_by_5d_hot": selected,
            "candidate_count": b["candidate_count"],
            "representative_codes": b["representative_codes"],
            "forward_d1": None, "forward_d3": None, "forward_d5": None,
            "mfe": None, "mae": None, "verdict": None,
        })

    if save:
        data = _load()
        idx = {(r["date"], r["sector"], r["source_type"]): i for i, r in enumerate(data["records"])}
        for rec in records:
            k = (rec["date"], rec["sector"], rec["source_type"])
            if k in idx:
                old = data["records"][idx[k]]
                # 기존 forward/verdict 보존 (D0 라벨만 재계산)
                for fld in ("forward_d1", "forward_d3", "forward_d5", "mfe", "mae", "verdict"):
                    rec[fld] = old.get(fld)
                data["records"][idx[k]] = rec
            else:
                data["records"].append(rec)
        _save(data)

    return {"date": asof, "market_return": market_return, "records": records}


def update_forward(save: bool = True) -> int:
    """기록된 record의 forward_d1/d3/d5 + mfe/mae를 일봉으로 채운다.

    representative_codes 평균 수익. base date 이후 N거래일 일봉이 있어야 채움.
    이미 forward_d5가 채워진 record는 skip. 반환=새로 채운 record 수.
    """
    data = _load()
    filled = 0
    for rec in data["records"]:
        if rec.get("forward_d5") is not None:
            continue
        codes = rec.get("representative_codes") or []
        if not codes:
            continue
        base_date = rec["date"]
        fwd = {}
        ok_any = False
        for n in FWD_OFFSETS:
            rets = []
            for c in codes:
                rows = _daily_rows(c)
                di = next((i for i, r in enumerate(rows) if r[0] == base_date), None)
                if di is None or di + n >= len(rows):
                    continue
                bc = rows[di][1]
                if bc > 0:
                    rets.append((rows[di + n][1] / bc - 1) * 100)
            if rets:
                fwd[n] = round(sum(rets) / len(rets), 2)
                ok_any = True
        # mfe/mae over D+1..D+WINDOW
        mfes, maes = [], []
        for c in codes:
            rows = _daily_rows(c)
            di = next((i for i, r in enumerate(rows) if r[0] == base_date), None)
            if di is None:
                continue
            bc = rows[di][1]
            window = rows[di + 1: di + 1 + MFE_MAE_WINDOW]
            if not window or bc <= 0:
                continue
            mfes.append((max(r[2] for r in window) / bc - 1) * 100)
            maes.append((min(r[3] for r in window) / bc - 1) * 100)
        if ok_any:
            rec["forward_d1"] = fwd.get(1)
            rec["forward_d3"] = fwd.get(3)
            rec["forward_d5"] = fwd.get(5)
            if mfes:
                rec["mfe"] = round(max(mfes), 2)
            if maes:
                rec["mae"] = round(min(maes), 2)
            filled += 1
    if save and filled:
        _save(data)
    return filled
