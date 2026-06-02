# -*- coding: utf-8 -*-
"""leader_catalyst_deep — STEADY × DART 본문 심화 (6/2, read-only).

사장님 지시: 첫 양전환(완만추세형 신고가돌파 × 호재공시 median +0.92, 승률54%)이
진짜 알파인지 ★DART 본문 파싱+강도 점수화(S/A/B/제외)★로 단단히 검증. STEADY에만 결합.
급등형 명분 제외. 뉴스/Tavily/종합매체 미적용(신호 흐리지 않게).

본문 추출(document.xml, 검증됨): 공급계약→계약금액/매출액→매출대비비율 / 자사주→취득금액 / 정정→제외.
강도: S(매출대비≥10% 공급계약·대규모 자사주) / A(매출대비3~10%·실적/배당/소각/무상) / B(소형 호재) / 제외(정정·소액).

★ 순수 read-only: 실주문 0 / DART 조회만 / SAJANG·scheduler·systemd 무변경 / 봇 OFF.
★ 판정: S/A급 median + 유지 & 승률≥55% → forward shadow 후보. 약하면 +0.92=키워드 노이즈.
사용: python tools/leader_catalyst_deep_6_2.py [--refetch] [--selftest]
"""
from __future__ import annotations

import argparse
import io
import json
import glob
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
CACHE_DIR = SA / "data_store"
DOC_DIR = CACHE_DIR / "dart_docs"
sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import detect, load_daily, _is_excluded, STEADY_HIGH_N  # noqa: E402

WIN_BGN, WIN_END = "2026-02-04", "2026-05-05"   # STEADY 진입 윈도(공시 있고 fwd 확보)
CHUNKS = [("20260204", "20260228"), ("20260301", "20260331"),
          ("20260401", "20260430"), ("20260501", "20260531")]
CAT_LOOKBACK = 3
HOLD_CAP = 40
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033

POS_KW = ("공급계약", "수주", "자기주식취득", "자사주", "무상증자", "단일판매",
          "영업실적", "잠정실적", "현금ㆍ현물배당", "이익소각", "주식소각", "시설투자", "타법인주식")
NEG_KW = ("유상증자", "전환사채", "신주인수권", "교환사채", "감자", "관리종목",
          "상장폐지", "불성실공시", "횡령", "배임", "거래정지")


def _key():
    for p in (SA / ".env", ROOT / ".env"):
        if p.exists():
            for l in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                if l.startswith("DART_API_KEY") and "=" in l:
                    return l.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def fetch_list(refetch=False):
    """list B+I 6개월, rcept_no/corp_code 포함. 캐시."""
    cache = CACHE_DIR / "dart_listdeep_2026.json"
    if cache.exists() and not refetch:
        return json.loads(cache.read_text(encoding="utf-8"))
    K = _key()
    out = []
    for ty in ("B", "I"):
        for b, e in CHUNKS:
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
                        out.append({"date": it.get("rcept_dt", "")[:8], "code": sc,
                                    "nm": (it.get("report_nm") or "").strip(),
                                    "rcept": it.get("rcept_no", ""), "corp": it.get("corp_code", "")})
                if page >= int(data.get("total_page", 1)):
                    break
                page += 1; time.sleep(0.1)
    cache.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return out


def fetch_doc(rcept, K):
    """document.xml → 태그제거 본문 텍스트. rcept별 캐시."""
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    cp = DOC_DIR / f"{rcept}.txt"
    if cp.exists():
        return cp.read_text(encoding="utf-8", errors="ignore")
    try:
        raw = urllib.request.urlopen(
            f"https://opendart.fss.or.kr/api/document.xml?crtfc_key={K}&rcept_no={rcept}", timeout=30).read()
        if raw[:2] != b"PK":
            cp.write_text("", encoding="utf-8"); return ""
        z = zipfile.ZipFile(io.BytesIO(raw))
        body = z.read(z.namelist()[0])
        txt = None
        for enc in ("utf-8", "euc-kr", "cp949"):
            try:
                txt = body.decode(enc); break
            except UnicodeDecodeError:
                continue
        txt = re.sub(r"<[^>]+>", " ", txt or "")
        txt = re.sub(r"\s+", " ", txt)
        cp.write_text(txt, encoding="utf-8")
        time.sleep(0.05)
        return txt
    except Exception:  # noqa: BLE001
        return ""


def _num_after(txt, kw, span=45):
    m = re.search(rf"{kw}[^0-9\-]{{0,{span}}}([0-9][0-9,]+)", txt)
    return float(m.group(1).replace(",", "")) if m else None


def grade_disclosure(nm, txt):
    """공시 본문 → (kind, grade S/A/B/제외, detail)."""
    if "정정" in nm:                       # [기재정정] 등 이미 알려진 정정
        return "정정", "제외", "정정공시"
    # 공급계약/수주
    if ("공급계약" in nm) or ("수주" in nm) or ("단일판매" in nm):
        amt = _num_after(txt, "계약금액")
        sales = _num_after(txt, "매출액")
        ratio = (amt / sales) if (amt and sales) else None
        if ratio is None:
            return "공급계약", "B", "금액/매출 미추출"
        if ratio >= 0.10:
            g = "S"
        elif ratio >= 0.03:
            g = "A"
        elif ratio >= 0.01:
            g = "B"
        else:
            g = "제외"      # 소액
        return "공급계약", g, f"매출대비 {ratio*100:.1f}%"
    # 자사주 취득
    if ("자기주식취득" in nm) or ("자사주" in nm):
        amt = _num_after(txt, "취득예정금액") or _num_after(txt, "취득금액")
        if amt is None:
            return "자사주", "B", "금액 미추출"
        if amt >= 5e10:
            g = "S"
        elif amt >= 1e10:
            g = "A"
        else:
            g = "B"
        return "자사주", g, f"취득 {amt/1e8:.0f}억"
    # 실적/배당/소각/무상 = A, 투자/타법인 = B
    if ("실적" in nm):
        return "실적", "A", "실적공시"
    if ("배당" in nm) or ("소각" in nm) or ("무상증자" in nm):
        return "주주환원", "A", nm[:14]
    if ("시설투자" in nm) or ("타법인" in nm):
        return "투자", "B", nm[:14]
    return "기타", "B", nm[:14]


def is_pos(nm):
    return any(k in nm for k in POS_KW) and not any(k in nm for k in NEG_KW)


def ride_exit(d, i, kind):
    """STEADY 라이드 청산: ma5/ma10/atr/trail7/trail10 → net%."""
    entry = d[i][4]; peak = entry
    a = max(1, i - 14)
    atr0 = sum(max(d[k][2]-d[k][3], abs(d[k][2]-d[k-1][4]), abs(d[k][3]-d[k-1][4])) for k in range(a, i+1)) / max(1, i+1-a)
    end = min(len(d), i + 1 + HOLD_CAP)
    exit_px = d[end-1][4] if end-1 > i else entry; exit_k = end-1
    for k in range(i+1, end):
        x = d[k]; peak = max(peak, x[2])
        stop = None
        if kind == "trail7":
            stop = peak*(1-0.07)
        elif kind == "trail10":
            stop = peak*(1-0.10)
        elif kind == "atr":
            stop = peak - 2.5*atr0
        if stop is not None and x[3] <= stop:
            exit_px = min(x[1], stop) if x[1] <= stop else stop; exit_k = k; break
        if kind in ("ma5", "ma10"):
            n = 5 if kind == "ma5" else 10
            ma = sum(d[j][4] for j in range(max(0, k-n+1), k+1)) / min(n, k+1)
            if x[4] < ma:
                exit_px = x[4]; exit_k = k; break
    net = (int(BUDGET//entry)*exit_px*(1-SELL_COST) - int(BUDGET//entry)*entry*(1+BUY_COST))/BUDGET*100
    return net, exit_k - i


def fwd_rets(d, i):
    e = d[i][4]
    r = {}
    for h in (1, 3, 5, 10):
        k = i + h
        r[h] = (d[k][4]-e)/e*100 if k < len(d) else None
    path = d[i+1:min(len(d), i+1+20)]
    mfe = max((x[2]-e)/e*100 for x in path) if path else 0
    mae = min((x[3]-e)/e*100 for x in path) if path else 0
    return r, mfe, mae


def _median(x):
    if not x:
        return 0.0
    s = sorted(x); n = len(s)
    return round(s[n//2] if n % 2 else (s[n//2-1]+s[n//2])/2, 2)


def _avg(x):
    return round(sum(x)/len(x), 2) if x else 0.0


def run(refetch):
    print("DART list 로드(rcept 포함) ...")
    discl = fetch_list(refetch)
    K = _key()
    # 종목·날짜별 POS 공시 인덱스 (nm, rcept)
    pos_idx = defaultdict(lambda: defaultdict(list))
    for r in discl:
        if is_pos(r["nm"]):
            dd = r["date"]
            if len(dd) == 8:
                ds = f"{dd[:4]}-{dd[4:6]}-{dd[6:8]}"
                pos_idx[r["code"]][ds].append((r["nm"], r["rcept"]))
    print(f"  POS 공시 종목 {len(pos_idx)}개")

    dpaths = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        dpaths[os.path.basename(f)[:-4].split("_")[-1]] = f

    GRADE_RANK = {"S": 3, "A": 2, "B": 1, "제외": 0}
    rows = []        # STEADY 후보(호재공시 있는 것만): grade, fwd, exits
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
            # T-3..T0 POS 공시 수집 → 최강 등급
            best = ("없음", "없음", "", 0)
            for k in range(max(0, i - CAT_LOOKBACK), i + 1):
                for nm, rc in cm.get(dates[k], []):
                    txt = fetch_doc(rc, K) if rc else ""
                    kind, g, det = grade_disclosure(nm, txt)
                    if GRADE_RANK.get(g, 0) > GRADE_RANK.get(best[1], 0):
                        best = (kind, g, det, k)
            if best[1] == "없음":
                continue
            fr, mfe, mae = fwd_rets(d, i)
            exits = {ek: ride_exit(d, i, ek)[0] for ek in ("ma5", "ma10", "atr", "trail7", "trail10")}
            rows.append({"code": code, "name": name, "date": dates[i],
                         "grade": best[1], "kind": best[0], "detail": best[2],
                         "fwd": fr, "mfe": mfe, "mae": mae, "exits": exits,
                         "hold": ride_exit(d, i, "ma10")[1]})
    return rows, n_steady


def selftest():
    ok = []
    k, g, det = grade_disclosure("단일판매ㆍ공급계약체결", "계약금액 100 매출액 500")
    ok.append(("T1 공급계약 매출대비20%→S", g == "S"))
    k, g, _ = grade_disclosure("[기재정정]단일판매ㆍ공급계약체결", "계약금액 100 매출액 500")
    ok.append(("T2 정정→제외", g == "제외"))
    k, g, _ = grade_disclosure("주요사항보고서(자기주식취득결정)", "취득예정금액 60,000,000,000")
    ok.append(("T3 자사주 600억→S", g == "S"))
    k, g, _ = grade_disclosure("현금ㆍ현물배당결정", "")
    ok.append(("T4 배당→A", g == "A"))
    ok.append(("T5 POS/NEG", is_pos("단일판매공급계약") and not is_pos("유상증자결정")))
    ok.append(("T6 num_after", _num_after("계약금액 1,234,567 원", "계약금액") == 1234567))
    print("catalyst deep 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    rows, n_steady = run(a.refetch)
    print("=" * 96)
    print(f"leader_catalyst_deep — STEADY × DART 본문 심화 (2026-02~05, 청산 ma10 기준)")
    print(f"STEADY 후보 {n_steady} 중 호재공시有 {len(rows)}건")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / DART 조회만 / SAJANG·scheduler·systemd 무변경")
    print("=" * 96)
    by = defaultdict(list)
    for r in rows:
        by[r["grade"]].append(r)
        by["ALL호재"].append(r)
    print(f"\n{'등급':<8}{'건수':>5}{'D+1':>7}{'D+3':>7}{'D+5':>7}{'D+10':>7}{'median(ma10)':>13}{'승률%':>7}{'MFE':>7}{'MAE':>8}{'휩쏘%':>7}")
    for g in ("S", "A", "B", "ALL호재"):
        rs = by.get(g)
        if not rs:
            continue
        nets = [r["exits"]["ma10"] for r in rs]
        wr = 100*sum(1 for x in nets if x > 0)/len(nets)
        whip = 100*sum(1 for r in rs if r["hold"] <= 2 and r["exits"]["ma10"] < 0)/len(rs)
        def fa(h):
            v = [r["fwd"][h] for r in rs if r["fwd"][h] is not None]
            return _avg(v)
        print(f"{g:<8}{len(rs):>5}{fa(1):>7}{fa(3):>7}{fa(5):>7}{fa(10):>7}{_median(nets):>13}{wr:>7.0f}"
              f"{_avg([r['mfe'] for r in rs]):>7}{_avg([r['mae'] for r in rs]):>8}{whip:>7.0f}")
    print(f"\n[exit별 median net% — S/A급]")
    sa = [r for r in rows if r["grade"] in ("S", "A")]
    if sa:
        print(f"  S/A {len(sa)}건: " + " ".join(f"{ek} {_median([r['exits'][ek] for r in sa])}" for ek in ("ma5", "ma10", "atr", "trail7", "trail10")))
    # 판정
    print("\n── 판정 ──")
    for g in ("S", "A"):
        rs = by.get(g)
        if not rs:
            print(f"  {g}급: 표본 0"); continue
        nets = [r["exits"]["ma10"] for r in rs]
        md = _median(nets); wr = 100*sum(1 for x in nets if x > 0)/len(nets)
        verdict = "forward shadow 후보" if (md > 0 and wr >= 55) else "약함(노이즈 의심)"
        print(f"  {g}급 {len(rs)}건: median {md} / 승률 {wr:.0f}% → {verdict}")
    print("\n★ S/A급 median+ & 승률≥55% → forward 후보 / 약해지면 +0.92=키워드노이즈.")
    print("★★ 정직: 표본 thin(기간확대=다음)·본문추출 일부 실패가능·생존편향·일봉. 절대치X. forward가 본검증.")


if __name__ == "__main__":
    main()
