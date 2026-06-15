# -*- coding: utf-8 -*-
"""F1 장중 외인 TR 검증 프로브 (6/15 일회성, read-only).

목적: 버그②(외인충전 0) 근본 규명 — 장중(09:05+)에 실호출로 2가지 확정.
  (A) 현재 F1이 쓰는 inquire-investor(FHKST01010900, 일별 가집계)가 장중 당일 외인을
      0/부재로 주는지 = 버그②의 원인 확정.
  (B) 대체 후보 fetch_foreign_inst_total(FHPTJ04400000, 장중 추정 가집계)이 장중 외인
      순매수/순매도 상위를 실제로 주는지 + F1 고정 20종목이 랭킹에 드는지(교집합).

안전: 조회 전용(KIS read-only)·주문 0·매도 무접촉·저장 0(stdout만). 어떤 예외도 비차단.
실행: (장중 09:05+) PYTHONIOENCODING=utf-8 python tools/f1_intraday_tr_probe_6_15.py
"""
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# F1 6/12 코호트 고정 20종목 (ved=6/12)
F1_CODES = [
    "000660", "000720", "005930", "006360", "035420", "035720", "042700",
    "051900", "058470", "090430", "095340", "192820", "237880", "240810",
    "251270", "259960", "294870", "357780", "375500", "402340",
]


def _fmt_won(v):
    try:
        return f"{int(v)/1e8:+.1f}억"
    except Exception:
        return str(v)


def probe_fhptj():
    """(B) FHPTJ04400000 장중 외인 추정 가집계 — 순매수/순매도 상위 랭킹."""
    from bot.kis_trader import KISTrader
    trader = KISTrader({})
    out = {"buy": {}, "sell": {}, "err": None}
    try:
        buy = trader.fetch_foreign_inst_total(target="1", sort_cls="0") or []   # 외인 순매수 상위
        sell = trader.fetch_foreign_inst_total(target="1", sort_cls="1") or []  # 외인 순매도 상위
        out["buy"] = {str(r.get("code", "")).zfill(6): r.get("amount") for r in buy if r.get("code")}
        out["sell"] = {str(r.get("code", "")).zfill(6): r.get("amount") for r in sell if r.get("code")}
        print(f"[B] FHPTJ04400000 장중 추정 — 외인 순매수상위 {len(buy)}종목 / 순매도상위 {len(sell)}종목")
        for label, arr in (("순매수", buy[:5]), ("순매도", sell[:5])):
            for r in arr:
                print(f"    {label} {str(r.get('code','')).zfill(6)} {str(r.get('name',''))[:10]:10} "
                      f"{_fmt_won(r.get('amount'))} (chg {r.get('change_rate')})")
    except Exception as e:
        out["err"] = str(e)
        print(f"[B] FHPTJ04400000 호출 실패: {e}")
    return out


def probe_inquire_investor():
    """(A) inquire-investor(FHKST01010900) — F1 현행 소스. 장중 당일 외인값 확인."""
    from data.foreign_f1_intraday_logger import _foreign_net_today
    from data.flow_collector import _get_kis_session
    asof = date.today().isoformat()
    session = _get_kis_session()
    res = {}
    if session is None:
        print("[A] KIS 세션 없음 — inquire-investor 스킵")
        return res
    nonzero = 0
    for code in F1_CODES:
        try:
            v = _foreign_net_today(session, code, asof)
        except Exception as e:
            v = f"ERR:{e}"
        res[code] = v
        if isinstance(v, int) and v != 0:
            nonzero += 1
    print(f"[A] inquire-investor 장중 당일 외인 — {len(F1_CODES)}종목 중 값≠0: {nonzero}개 "
          f"(0/None이면 버그② 원인 = 일별 TR 장중 미집계 확정)")
    sample = {c: res[c] for c in F1_CODES[:6]}
    print(f"    샘플: {sample}")
    return res


def main():
    print(f"===== F1 장중 외인 TR 프로브 {date.today().isoformat()} =====")
    b = probe_fhptj()
    print("")
    a = probe_inquire_investor()
    print("")
    # 교집합 — F1 20종목 중 장중 추정 랭킹(매수∪매도)에 든 종목
    ranked = set(b.get("buy", {})) | set(b.get("sell", {}))
    hit = [c for c in F1_CODES if c in ranked]
    print(f"[교집합] F1 20종목 중 FHPTJ 장중 랭킹 포함: {len(hit)}개 → {hit}")
    print("===== 결론 판정 가이드 =====")
    print("  - [A]가 값≠0 0개 → 버그② 원인 = inquire-investor 장중 미집계 확정")
    print("  - [B]가 종목 다수 반환 → 장중 추정 TR 가용 (대체 소스 검증)")
    print("  - [교집합] ≥1 → F1 랭킹 교집합 방식 실현 가능 / 0 → 종목 커버리지 한계 추가검토")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001
        print(f"프로브 예외(무시): {e}")
        sys.exit(0)
