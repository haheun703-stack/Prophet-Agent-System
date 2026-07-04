# -*- coding: utf-8 -*-
"""leader_catalyst_extend — STEADY × S급 DART 명분 기간확대 (6/2, read-only).

사장님 지시: S급 명분 forward 후보 인정, 단 18건 thin → 기간확대 먼저(실전/스케줄 배선 금지).
범위 2025-01-01~2026-06-02. STEADY 신고가돌파 전체 × DART 본문 S/A/B/제외 재분류.
S급(매출대비 큰 공급계약·대규모 자사주·수치 큰 확정성 호재)이 median+/승률55%+ 유지되나.

★ 순수 read-only: 실주문 0 / DART 조회만 / SAJANG·scheduler·systemd 무변경 / 봇 OFF.
★ 재개가능: DART list 월별 청크 캐시(중단시 재실행=남은 청크만). document.xml rcept별 캐시.
보고: S/A/B/제외 건수 · D+1/3/5/10/20 median · 승률 · MAE · 업종/시총 편중 · 상하위 TOP5 · lookahead 증빙.
사용: python tools/leader_catalyst_extend_6_2.py [--refetch] [--selftest]
"""
from __future__ import annotations

import argparse
import calendar
import json
import glob
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
CACHE_DIR = SA / "data_store"
CHUNK_DIR = CACHE_DIR / "dart_chunks"
UNIVERSE = CACHE_DIR / "universe.json"
sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import detect, load_daily, _is_excluded, STEADY_HIGH_N  # noqa: E402
from leader_catalyst_deep_6_2 import grade_disclosure, fetch_doc, is_pos, ride_exit, _median, _avg, _key  # noqa: E402

WIN_BGN, WIN_END = "2025-01-01", "2026-05-05"     # STEADY 진입(공시+fwd20 확보)
FETCH_BGN, FETCH_END = (2025, 1), (2026, 6)
CAT_LOOKBACK = 3
GRADE_RANK = {"S": 3, "A": 2, "B": 1, "제외": 0, "없음": -1}


def month_chunks(s, e):
    out = []
    y, m = s
    while (y, m) <= e:
        last = calendar.monthrange(y, m)[1]
        out.append((f"{y}{m:02d}01", f"{y}{m:02d}{last:02d}"))
        m += 1
        if m > 12:
            m = 1; y += 1
    return out


def fetch_list_resumable(refetch=False):
    """월별 청크 캐시(재개가능). 반환 [{date,code,nm,rcept}]."""
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    K = _key()
    chunks = month_chunks(FETCH_BGN, FETCH_END)
    out = []
    for ty in ("B", "I"):
        for b, e in chunks:
            cf = CHUNK_DIR / f"{ty}_{b}.json"
            if cf.exists() and not refetch:
                out += json.loads(cf.read_text(encoding="utf-8")); continue
            rows = []
            page = 1
            while True:
                params = {"crtfc_key": K, "bgn_de": b, "end_de": e, "pblntf_ty": ty,
                          "page_no": str(page), "page_count": "100"}
                try:
                    data = json.loads(urllib.request.urlopen(
                        "https://opendart.fss.or.kr/api/list.json?" + urllib.parse.urlencode(params),
                        timeout=20).read().decode("utf-8"))
                except Exception as ex:  # noqa: BLE001
                    print(f"  [!] {ty}{b} p{page}: {ex}"); break
                st = str(data.get("status"))
                if st == "013":
                    break
                if st != "000":
                    print(f"  [!] {ty}{b} status={st}"); break
                for it in data.get("list", []):
                    sc = (it.get("stock_code") or "").strip()
                    if sc and len(sc) == 6:
                        rows.append({"date": it.get("rcept_dt", "")[:8], "code": sc,
                                     "nm": (it.get("report_nm") or "").strip(),
                                     "rcept": it.get("rcept_no", "")})
                if page >= int(data.get("total_page", 1)):
                    break
                page += 1; time.sleep(0.1)
            cf.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            out += rows
            print(f"  DART {ty} {b}: {len(rows)}건")
    return out


def load_universe():
    if not UNIVERSE.exists():
        return {}
    u = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    return u if isinstance(u, dict) else {}


def fwd5(d, i):
    e = d[i][4]; r = {}
    for h in (1, 3, 5, 10, 20):
        k = i + h
        r[h] = (d[k][4] - e) / e * 100 if k < len(d) else None
    path = d[i + 1:min(len(d), i + 21)]
    mae = min((x[3] - e) / e * 100 for x in path) if path else 0.0
    return r, mae


def run(refetch):
    print("DART list 수집(월별청크, 재개가능) ...")
    discl = fetch_list_resumable(refetch)
    K = _key()
    pos_idx = defaultdict(lambda: defaultdict(list))
    for r in discl:
        if is_pos(r["nm"]):
            dd = r["date"]
            if len(dd) == 8:
                ds = f"{dd[:4]}-{dd[4:6]}-{dd[6:8]}"
                pos_idx[r["code"]][ds].append((r["nm"], r["rcept"]))
    print(f"  POS 공시 종목 {len(pos_idx)}개 / 총공시 {len(discl)}")
    uni = load_universe()

    dpaths = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        dpaths[os.path.basename(f)[:-4].split("_")[-1]] = f

    rows = []
    n_steady = 0
    for code, dp in dpaths.items():
        cm = pos_idx.get(code) or pos_idx.get(code.zfill(6))
        if not cm:
            continue
        name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(dp)
        if len(d) < STEADY_HIGH_N + 25:
            continue
        dates = [x[0] for x in d]
        for i in range(STEADY_HIGH_N, len(d) - 21):
            if dates[i] < WIN_BGN or dates[i] > WIN_END:
                continue
            if detect(d, i) != "STEADY":
                continue
            n_steady += 1
            best = ("없음", "없음", "", 0)
            for k in range(max(0, i - CAT_LOOKBACK), i + 1):
                for nm, rc in cm.get(dates[k], []):
                    kind, g, det = grade_disclosure(nm, fetch_doc(rc, K) if rc else "")
                    if GRADE_RANK.get(g, -1) > GRADE_RANK.get(best[1], -1):
                        best = (kind, g, det, k)
            if best[1] in ("없음",):
                continue
            fr, mae = fwd5(d, i)
            net10 = ride_exit(d, i, "ma10")[0]
            ui = uni.get(code) or uni.get(code.zfill(6)) or {}
            rows.append({"code": code, "name": name, "date": dates[i], "grade": best[1],
                         "kind": best[0], "detail": best[2], "fwd": fr, "mae": mae,
                         "net10": net10, "sector": ui.get("sector", "?"), "cap": ui.get("cap_억", 0)})
    return rows, n_steady


def selftest():
    ok = []
    ch = month_chunks((2025, 1), (2025, 3))
    ok.append(("T1 월청크3", len(ch) == 3 and ch[0] == ("20250101", "20250131") and ch[2] == ("20250301", "20250331")))
    ok.append(("T2 윤년2월", month_chunks((2026, 2), (2026, 2))[0] == ("20260201", "20260228")))
    d = [("2025-01-%02d" % (k + 1), 100, 101, 99, 100, 1) for k in range(25)]
    fr, mae = fwd5(d, 0)
    ok.append(("T3 fwd5 D+20키", 20 in fr and 1 in fr))
    ok.append(("T4 grade import", grade_disclosure("단일판매ㆍ공급계약체결", "계약금액 100 매출액 500")[1] == "S"))
    print("catalyst extend 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def _cap_bucket(c):
    if c >= 10000:
        return "대형(≥1조)"
    if c >= 1000:
        return "중형(1천억~1조)"
    if c > 0:
        return "소형(<1천억)"
    return "미상"


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    rows, n_steady = run(a.refetch)
    by = defaultdict(list)
    for r in rows:
        by[r["grade"]].append(r); by["ALL호재"].append(r)
    print("=" * 100)
    print(f"leader_catalyst_extend — STEADY × S급 DART 명분 기간확대 ({WIN_BGN}~{WIN_END})")
    print(f"STEADY 후보 {n_steady} 중 호재공시有 {len(rows)}건 (청산 ma10)")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / DART 조회만 / SAJANG·scheduler·systemd 무변경")
    print(f"★ lookahead 없음: 점화탐지=data[:i]만 / DART 접수일≤T0(T-3..T0) / 진입=T0종가 / fwd만 미래")
    print("=" * 100)
    print(f"\n{'등급':<8}{'건수':>5}{'D+1':>7}{'D+3':>7}{'D+5':>7}{'D+10':>7}{'D+20':>7}{'median(ma10)':>13}{'승률%':>7}{'MAE평균':>9}")
    for g in ("S", "A", "B", "제외", "ALL호재"):
        rs = by.get(g)
        if not rs:
            continue
        nets = [r["net10"] for r in rs]
        wr = 100 * sum(1 for x in nets if x > 0) / len(nets)
        def md(h):
            v = [r["fwd"][h] for r in rs if r["fwd"][h] is not None]
            return _median(v)
        print(f"{g:<8}{len(rs):>5}{md(1):>7}{md(3):>7}{md(5):>7}{md(10):>7}{md(20):>7}"
              f"{_median(nets):>13}{wr:>7.0f}{_avg([r['mae'] for r in rs]):>9}")
    # S급 편중
    S = by.get("S", [])
    if S:
        sec = Counter(r["sector"] for r in S)
        cap = Counter(_cap_bucket(r["cap"]) for r in S)
        print(f"\n[S급 {len(S)}건 업종 편중] {sec.most_common(6)}")
        print(f"[S급 시총 편중] {dict(cap)}")
    # 상하위 TOP5 (D+20)
    rv = [r for r in rows if r["fwd"][20] is not None]
    rv.sort(key=lambda r: r["fwd"][20], reverse=True)
    print("\n[상위 D+20 TOP5]")
    for r in rv[:5]:
        print(f"  {r['grade']:<3}{r['name']:<14}{r['date']} {r['kind']}/{r['detail'][:18]} D+20 {r['fwd'][20]:.1f}%")
    print("[하위 D+20 TOP5]")
    for r in rv[-5:]:
        print(f"  {r['grade']:<3}{r['name']:<14}{r['date']} {r['kind']}/{r['detail'][:18]} D+20 {r['fwd'][20]:.1f}%")
    # 판정
    print("\n── 판정 (S급 median+ & 승률≥55% 유지?) ──")
    for g in ("S", "A"):
        rs = by.get(g)
        if not rs:
            print(f"  {g}급 0"); continue
        nets = [r["net10"] for r in rs]; mdv = _median(nets)
        wr = 100 * sum(1 for x in nets if x > 0) / len(nets)
        v = "유지(forward 가치 확실)" if (g == "S" and mdv > 0 and wr >= 55) else ("유지" if mdv > 0 and wr >= 55 else "약화")
        print(f"  {g}급 {len(rs)}건: median {mdv} / 승률 {wr:.0f}% → {v}")
    print("\n★★ 정직: 생존편향(과대,상대만)·일봉·본문추출 일부실패. 실전/스케줄 배선 금지(forward 본검증 선행).")


if __name__ == "__main__":
    main()
