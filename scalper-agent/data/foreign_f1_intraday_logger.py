# -*- coding: utf-8 -*-
"""외국인 장중 잠정 순매수 시간대 패턴 로거 (F1) — 2026-06-11 사장님 + Claude Fable 5 협업.

배경 (Fable 5 외부조언 → 단타봇 데이터 검증):
  "오늘 +2~5% 종목, 내일 들고 갈까"의 게이트 질문. 외인 단타형(오전 매수 → 오후 반납)과
  매집형(오후까지 유지 / 종가 동시호가 추가매수)을 **장중 외인 잠정 순매수 시간대 추이**로
  구분 → 익일 forward로 "반납형이 정말 익일 약한가"를 검증(예측 아닌 게이트).

★★★ 안전 불변식 (사장님 6/11) ★★★
- **read-only 수집기** — 매매 경로·기존 intraday_supply_tracker 로직 무접촉.
- 주문/picks/SAJANG/게이트 미참조. F1 전용 관측 ledger에만 기록(기존 flow 캐시도 무접촉, 저장 0).
- ledger 쓰기 3원칙(is_final): ① (date,code) 멱등 ② 확정(is_final=True) record 보존(스냅샷 덮어쓰기 금지)
  ③ 잠정→확정은 finalize()에서만 전환.

설계 (사장님 + Fable 5 6/11 확정):
- **선정**: 11:00 스냅샷 직전, fetch_ranking_fluctuation 등락률 +3%↑ 상위 N(=30)
  + 당일 paper 후보 무조건 합류(③-lite). **11:00 확정 리스트 고정 — 13:00/14:30/15:30 동일 추적,
  중도 교체 금지**(오후 진입 종목은 놓침 = 의도). 백필 불가라 14:30 선정 시 11:00 데이터 부재.
- **스냅샷**: 11:00/13:00/14:30/15:30 4회. 종목별 외인 당일 누적 잠정(_fetch_investor_api, 저장 0).
- **지표**: retention_1430_1100 = 외인잠정(14:30)/외인잠정(11:00).
  ★ 단타봇 보강: "11:00 = 오전 *기준점*, 피크 아님"(09:30 백필 불가) /
                 ★ 11:00 순매수 > 0 일 때만 유효, ≤0은 FOREIGN_NET_SELL_AT_1100으로 분리(통계 보호).
  close_buy_1530_1430 = 외인잠정(15:30)/외인잠정(14:30) (종가 동시호가 추가매수 단서).
- **라벨**: ACCUMULATION / PARTIAL_RETENTION / REVERSAL_AFTERNOON / FOREIGN_NET_SELL_AT_1100 / INCOMPLETE.
- **forward**: 익일 D+1 종가 수익(단타 1일 게이트). ★ 일봉 측정 = 장중 휩쏘 과소평가(5/31 메타교훈) caveat.

★ 데이터 전제 최종 확인 = check_f1_ledger_today()(preflight)가 6/12 첫 가동 시 "찍힘"을 자동 판정.
  "연결됨"이 아니라 "찍힘" — 외인 잠정이 장중 시간대에 실제 충전되는지를 시스템이 스스로 검증.

실행(러너, 봇 스케줄에서 호출):
  11:00  select_and_snapshot_1100(trader)
  13:00/14:30/15:30  snapshot(trader, "1300"|"1430"|"1530")
  마감후  finalize(asof);  익일  update_forward()
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
BASE = Path(__file__).resolve().parent.parent
F1_DIR = BASE / "data_store" / "foreign_f1"
PAPER_DIR = BASE / "data_store" / "paper_3type"
DAILY_DIR = BASE / "data_store" / "daily"

SELECT_CHANGE_MIN = 3.0     # 11:00 등락률 +3%↑ (Fable5 O게이트 모집단)
SELECT_TOP_N = 30
SNAP_LABELS = ("1100", "1300", "1430", "1530")
RETENTION_ACC = 0.9         # 유지율 ≥ 0.9 = 매집(오후 유지/증가)
RETENTION_PARTIAL = 0.5     # 0.5~0.9 = 부분유지
FWD_OFFSET = 1              # 익일 D+1


# ─────────────────────────── ledger I/O (is_final 3원칙) ───────────────────────────

def _today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _ledger_path(asof: str) -> Path:
    return F1_DIR / f"f1_{asof}.json"


def _load(asof: str) -> dict:
    p = _ledger_path(asof)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"date": asof, "selected_at": None, "records": [],
            "note": "foreign intraday F1 (read-only 관측, 매매 무접촉)"}


def _save(asof: str, data: dict) -> None:
    F1_DIR.mkdir(parents=True, exist_ok=True)
    data["updated_at"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    _ledger_path(asof).write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _rec_index(data: dict) -> dict:
    return {r["code"]: i for i, r in enumerate(data["records"])}


# ─────────────────────────── 데이터 소스 (read-only) ───────────────────────────

def _paper_candidate_codes(asof: str) -> set:
    """당일 paper 후보 코드 (합류용, 구조 불확실 대비 방어적). 없으면 빈 set."""
    p = PAPER_DIR / f"ledger_{asof}.json"
    if not p.exists():
        return set()
    out = set()
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        recs = d.get("records", []) if isinstance(d, dict) else (d if isinstance(d, list) else [])
        for r in recs:
            if not isinstance(r, dict):
                continue
            c = r.get("code") or r.get("종목코드") or r.get("stock_code")
            if c:
                out.add(str(c).zfill(6))
    except Exception as e:
        logger.warning(f"[F1] paper 후보 로드 실패: {e}")
    return out


def _foreign_net_today(session: Tuple[str, dict], code: str, asof: str) -> Optional[int]:
    """종목 외인 당일 누적 잠정 순매수 금액(원). read-only(저장 0). None = 미가용/당일행 부재.

    None 반환 = '장중 당일 외인 잠정 미충전' 신호 → preflight가 6/12에 데이터 전제 판정.
    """
    try:
        from data.flow_collector import _fetch_investor_api
        base_url, headers = session
        h = dict(headers)
        h["tr_id"] = "FHKST01010900"
        df = _fetch_investor_api(base_url, h, code)
        if df is None or len(df) == 0:
            return None
        for idx in df.index:
            if str(idx).startswith(asof):
                v = df.loc[idx].get("외국인_금액")
                return int(v) if v is not None else None
        return None  # 당일 행 부재 = 장중 미충전
    except Exception as e:
        logger.warning(f"[F1] 외인 조회 실패 {code}: {e}")
        return None


def _daily_closes(code: str) -> List[Tuple[str, float]]:
    """일봉 csv → [(date, close), ...] 거래일순 (forward용, read-only)."""
    f = DAILY_DIR / f"{code}.csv"
    if not f.exists():
        return []
    try:
        lines = f.read_text(encoding="utf-8").strip().split("\n")[1:]
        out = []
        for ln in lines:
            p = ln.split(",")
            if len(p) >= 5:
                out.append((p[0], float(p[4])))
        return out
    except Exception:
        return []


# ─────────────────────────── 핵심 로직 ───────────────────────────

def _classify(f1100: Optional[int], f1430: Optional[int]) -> Tuple[Optional[float], str]:
    """retention(14:30/11:00) + 라벨. 11:00>0 일 때만 유지율 유효(부호 분리)."""
    if f1100 is None or f1430 is None:
        return None, "INCOMPLETE"
    if f1100 <= 0:
        # 11:00에 이미 외인 순매도 = 애초에 매집 아님 → 유지율 무의미, 분리
        return None, "FOREIGN_NET_SELL_AT_1100"
    retention = round(f1430 / f1100, 3)
    if retention >= RETENTION_ACC:
        label = "ACCUMULATION"
    elif retention >= RETENTION_PARTIAL:
        label = "PARTIAL_RETENTION"
    else:
        label = "REVERSAL_AFTERNOON"
    return retention, label


def select_and_snapshot_1100(trader, asof: Optional[str] = None,
                             session: Optional[Tuple[str, dict]] = None,
                             top_n: int = SELECT_TOP_N, save: bool = True) -> dict:
    """11:00 — 선정(등락률 +3%↑ 상위 N + paper 후보 합류) + 첫 외인 스냅샷. 리스트 고정.

    이미 selected_at 이 있으면(재실행) 선정 보존(중도 교체 금지) — 멱등.
    """
    asof = asof or _today_kst()
    data = _load(asof)

    # 선정 1회만 — 이미 선정됐으면 보존(고정 원칙)
    if data.get("selected_at"):
        logger.info(f"[F1] {asof} 이미 선정됨(selected_at={data['selected_at']}) — 11:00 스냅샷만 갱신")
    else:
        gainers = []
        try:
            ranked = trader.fetch_ranking_fluctuation(market="J", top_n=top_n * 2)
            gainers = [g for g in (ranked or []) if g.get("change_rate", 0) >= SELECT_CHANGE_MIN][:top_n]
        except Exception as e:
            logger.warning(f"[F1] 등락률 순위 조회 실패: {e}")
        gainer_codes = {str(g["code"]).zfill(6): g for g in gainers if g.get("code")}
        paper_codes = _paper_candidate_codes(asof)

        all_codes = set(gainer_codes) | paper_codes
        records = []
        for code in sorted(all_codes):
            g = gainer_codes.get(code, {})
            src = ("both" if (code in gainer_codes and code in paper_codes)
                   else "gainer" if code in gainer_codes else "paper_candidate")
            records.append({
                "code": code,
                "name": g.get("name", ""),
                "change_at_select": g.get("change_rate"),  # 11:00 등락률(후보-only는 None)
                "source": src,
                "foreign_net": {},          # {snap_label: 외인 잠정 금액}
                "retention_1430_1100": None,
                "close_buy_1530_1430": None,
                "f1_label": None,
                "is_final": False,
                "forward_d1": None,
            })
        data["selected_at"] = "1100"
        data["records"] = records
        logger.info(f"[F1] {asof} 선정: gainer{len(gainer_codes)} + 후보{len(paper_codes)} = {len(records)}종목 (고정)")

    if session is not None:
        _snapshot_into(data, asof, "1100", session)
    if save:
        _save(asof, data)
    return data


def _snapshot_into(data: dict, asof: str, snap_label: str,
                   session: Tuple[str, dict]) -> int:
    """고정 리스트 종목들의 외인 당일 잠정을 snap_label 슬롯에 기록. is_final record는 skip. 반환=찍힌 수."""
    filled = 0
    for rec in data["records"]:
        if rec.get("is_final"):   # 확정 보존(3원칙 ②)
            continue
        fn = _foreign_net_today(session, rec["code"], asof)
        if fn is not None:
            rec.setdefault("foreign_net", {})[snap_label] = fn
            filled += 1
    return filled


def snapshot(trader, snap_label: str, asof: Optional[str] = None,
             session: Tuple[str, dict] = None, save: bool = True) -> int:
    """13:00/14:30/15:30 — 고정 리스트 외인 스냅샷 추가(중도 교체 0). 반환=찍힌 수."""
    asof = asof or _today_kst()
    if snap_label not in SNAP_LABELS:
        raise ValueError(f"snap_label must be in {SNAP_LABELS}")
    if session is None:
        from data.flow_collector import _get_kis_session
        session = _get_kis_session()
        if session is None:
            logger.error("[F1] KIS 세션 없음 — 스냅샷 스킵")
            return 0
    data = _load(asof)
    if not data.get("selected_at"):
        logger.warning(f"[F1] {asof} 선정 전 — {snap_label} 스냅샷 스킵(11:00 select 먼저)")
        return 0
    filled = _snapshot_into(data, asof, snap_label, session)
    if save:
        _save(asof, data)
    logger.info(f"[F1] {asof} {snap_label} 스냅샷: {filled}/{len(data['records'])}종목 외인 충전")
    return filled


def finalize(asof: Optional[str] = None, save: bool = True) -> int:
    """마감후 — retention/close_buy/라벨 계산 + is_final 전환(잠정→확정, 3원칙 ③). 반환=확정 수."""
    asof = asof or _today_kst()
    data = _load(asof)
    n = 0
    for rec in data["records"]:
        if rec.get("is_final"):
            continue
        fn = rec.get("foreign_net", {})
        retention, label = _classify(fn.get("1100"), fn.get("1430"))
        rec["retention_1430_1100"] = retention
        rec["f1_label"] = label
        f1430, f1530 = fn.get("1430"), fn.get("1530")
        rec["close_buy_1530_1430"] = (
            round(f1530 / f1430, 3) if (f1430 and f1430 > 0 and f1530 is not None) else None)
        rec["is_final"] = True
        n += 1
    if save and n:
        _save(asof, data)
    logger.info(f"[F1] {asof} finalize: {n}종목 확정")
    return n


def update_forward(asof: Optional[str] = None, save: bool = True) -> int:
    """익일+ — forward_d1(D+1 종가 수익) 채움. 이미 채워진 record skip. 반환=새로 채운 수."""
    asof = asof or _today_kst()
    data = _load(asof)
    filled = 0
    for rec in data["records"]:
        if rec.get("forward_d1") is not None:
            continue
        closes = _daily_closes(rec["code"])
        di = next((i for i, (d, _) in enumerate(closes) if d == asof), None)
        if di is None or di + FWD_OFFSET >= len(closes):
            continue
        base_c = closes[di][1]
        if base_c > 0:
            rec["forward_d1"] = round((closes[di + FWD_OFFSET][1] / base_c - 1) * 100, 2)
            filled += 1
    if save and filled:
        _save(asof, data)
    return filled


# ─────────────────────────── preflight 자가감지 (단서②: "찍힘" 검증) ───────────────────────────

def check_f1_ledger_today(asof: Optional[str] = None) -> dict:
    """preflight — F1 ledger 오늘자가 '찍혔는지' 자가감지. "연결됨"이 아니라 "찍힘"을 시스템이 확인.

    반환 {ok, reason, records, with_foreign, selected_at}.
    ok=False = 연결됐다 믿었는데 빈 ledger / 외인 미충전 → 데이터 전제 미충족 경보.
    """
    asof = asof or _today_kst()
    p = _ledger_path(asof)
    if not p.exists():
        return {"ok": False, "reason": "F1 ledger 파일 없음(스케줄 미가동/미연결)",
                "asof": asof, "records": 0, "with_foreign": 0, "selected_at": None}
    data = _load(asof)
    recs = data.get("records", [])
    if not recs:
        return {"ok": False, "reason": "F1 ledger 종목 0(선정 미실행)",
                "asof": asof, "records": 0, "with_foreign": 0,
                "selected_at": data.get("selected_at")}
    with_foreign = sum(1 for r in recs if r.get("foreign_net"))
    if with_foreign == 0:
        return {"ok": False, "reason": "외인 잠정 0건 충전 — 장중 당일 잠정 미가용(스냅샷 시각/TR 교정 필요)",
                "asof": asof, "records": len(recs), "with_foreign": 0,
                "selected_at": data.get("selected_at")}
    return {"ok": True, "reason": "F1 찍힘 정상", "asof": asof,
            "records": len(recs), "with_foreign": with_foreign,
            "selected_at": data.get("selected_at")}


# ─────────────────────────── 셀프테스트 (순수 로직, KIS 무관) ───────────────────────────

def _selftest() -> bool:
    ok = []

    # T1 retention/label — 매집형
    r, l = _classify(1000, 950)
    ok.append(("T1 ACCUMULATION(0.95)", abs(r - 0.95) < 1e-6 and l == "ACCUMULATION"))
    # T2 반납형
    r, l = _classify(1000, 300)
    ok.append(("T2 REVERSAL_AFTERNOON(0.30)", abs(r - 0.30) < 1e-6 and l == "REVERSAL_AFTERNOON"))
    # T3 부분유지
    r, l = _classify(1000, 700)
    ok.append(("T3 PARTIAL_RETENTION(0.70)", abs(r - 0.70) < 1e-6 and l == "PARTIAL_RETENTION"))
    # T4 ★ 11:00 순매도 분리(부호 보호)
    r, l = _classify(-500, 300)
    ok.append(("T4 FOREIGN_NET_SELL_AT_1100(11:00<0)", r is None and l == "FOREIGN_NET_SELL_AT_1100"))
    # T5 미완(스냅샷 부재)
    r, l = _classify(1000, None)
    ok.append(("T5 INCOMPLETE(1430 부재)", r is None and l == "INCOMPLETE"))
    # T6 11:00=0 경계 → 순매도 분리
    r, l = _classify(0, 100)
    ok.append(("T6 11:00=0 → SELL_AT_1100", r is None and l == "FOREIGN_NET_SELL_AT_1100"))

    # T7 is_final 3원칙 — 확정 record는 스냅샷 갱신 skip
    data = {"date": "2026-06-12", "selected_at": "1100", "records": [
        {"code": "000660", "is_final": True, "foreign_net": {"1100": 999}},
        {"code": "058470", "is_final": False, "foreign_net": {"1100": 100}},
    ]}
    # _snapshot_into는 module-global _foreign_net_today를 부르므로 globals() 패치(KIS 무호출)
    orig = globals()["_foreign_net_today"]
    globals()["_foreign_net_today"] = lambda s, c, a: 777
    try:
        filled = _snapshot_into(data, "2026-06-12", "1300", ("u", {}))
    finally:
        globals()["_foreign_net_today"] = orig
    final_rec = data["records"][0]["foreign_net"]
    nonfinal_rec = data["records"][1]["foreign_net"]
    ok.append(("T7 확정 record 보존(skip)", "1300" not in final_rec and final_rec["1100"] == 999))
    ok.append(("T7b 잠정 record 갱신", nonfinal_rec.get("1300") == 777 and filled == 1))

    # T8 preflight — 빈 ledger 감지
    res = check_f1_ledger_today("1999-01-01")
    ok.append(("T8 preflight 빈 ledger ok=False", res["ok"] is False))

    passed = sum(1 for _, v in ok if v)
    for name, v in ok:
        print(f"  [{'PASS' if v else 'FAIL'}] {name}")
    print(f"\n  F1 셀프테스트: {passed}/{len(ok)} PASS")
    return passed == len(ok)


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        sys.exit(0 if _selftest() else 1)
    print("F1 logger — 러너에서 select_and_snapshot_1100 / snapshot / finalize / update_forward 호출")
