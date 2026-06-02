# -*- coding: utf-8 -*-
"""leader_catalyst_enrich — 재료/명분(DART 공시) 결합 테스트 (6/2, read-only).

기계·4주체수급 둘 다 median 음수 → 사장님 "DART 재료/명분 레버(즉시)" 선택.
질문: ★호재 공시(명분) 있는 leader 후보가 음수 median을 양으로 끌어올리는가★.

데이터: DART OpenAPI list.json (주요사항보고 B + 거래소공시 I), 2026-02~06 윈도.
  ★ list.json에 stock_code 직접 제공 → 종목 직접 매핑. 1회 수집 후 캐시(data_store/dart_cache_*.json).
  ★ walk-forward: 공시 접수일 ≤ T0 만 사용(T-3..T0 윈도). report_nm 키워드로 호재/악재 분류.
  ★ 한계: report_nm만(본문 미파싱) → 키워드 분류 거칠음. 실적=중립처리. 생존편향·일봉.

★ 순수 read-only: 실주문 0 / SAJANG·scheduler·systemd 무변경 / 봇 OFF / DART는 조회만.
사용: python tools/leader_catalyst_enrich_6_2.py [--exit ma10] [--refetch] [--selftest]
"""
from __future__ import annotations

import argparse
import json
import glob
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
CACHE_DIR = SA / "data_store"
sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import detect, load_daily, fwd_metrics, _is_excluded, STEADY_HIGH_N  # noqa: E402
from leader_exit_refine_6_2 import exit_variant  # noqa: E402

BGN, END = "20260204", "20260602"
CAT_LOOKBACK = 3          # 호재공시 T-3..T0
LEADER_MFE = 20.0

POS_KW = ("공급계약", "수주", "자기주식취득", "자기주식 취득", "자사주", "무상증자", "단일판매")
NEG_KW = ("유상증자", "전환사채", "신주인수권", "교환사채", "감자", "관리종목", "상장폐지",
          "불성실공시", "횡령", "배임", "거래정지", "감사의견")


def _read_env_key():
    for p in (SA / ".env", ROOT / ".env"):
        if p.exists():
            for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("DART_API_KEY") and "=" in line:
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def fetch_dart(refetch=False):
    """list.json 주요사항보고(B)+거래소공시(I) 윈도 수집 → [{date, code, nm}]. 캐시."""
    cache = CACHE_DIR / f"dart_cache_{BGN}_{END}.json"
    if cache.exists() and not refetch:
        return json.loads(cache.read_text(encoding="utf-8"))
    key = _read_env_key()
    if not key:
        raise RuntimeError("DART_API_KEY 없음")
    # ★ DART list.json 범위 ≤3개월 → 월별 청크로 분할
    chunks = [("20260204", "20260228"), ("20260301", "20260331"),
              ("20260401", "20260430"), ("20260501", "20260531"), ("20260601", "20260602")]
    out = []
    for ty in ("B", "I"):
        ty_pages = 0
        for b, e in chunks:
            page = 1
            while True:
                params = {"crtfc_key": key, "bgn_de": b, "end_de": e,
                          "pblntf_ty": ty, "page_no": str(page), "page_count": "100"}
                url = "https://opendart.fss.or.kr/api/list.json?" + urllib.parse.urlencode(params)
                try:
                    with urllib.request.urlopen(url, timeout=20) as r:
                        data = json.loads(r.read().decode("utf-8"))
                except Exception as ex:  # noqa: BLE001
                    print(f"  [!] {ty} {b} p{page} 실패: {ex}"); break
                st = str(data.get("status"))
                if st == "013":          # 조회 데이터 없음
                    break
                if st != "000":
                    print(f"  [!] {ty} {b} status={st} {data.get('message')}"); break
                for it in data.get("list", []):
                    sc = (it.get("stock_code") or "").strip()
                    if sc and len(sc) == 6:
                        out.append({"date": it.get("rcept_dt", "")[:8], "code": sc,
                                    "nm": (it.get("report_nm") or "").strip()})
                tp = int(data.get("total_page", 1))
                ty_pages += 1
                if page >= tp:
                    break
                page += 1
                time.sleep(0.12)
        print(f"  DART {ty}: {ty_pages}페이지 수집")
    cache.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return out


def classify(nm):
    if any(k in nm for k in NEG_KW):
        return "NEG"
    if any(k in nm for k in POS_KW):
        return "POS"
    return "NEU"


def build_index(discl):
    """{code: {yyyy-mm-dd: set(class)}}."""
    idx = defaultdict(lambda: defaultdict(set))
    for r in discl:
        d = r["date"]
        if len(d) == 8:
            ds = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            idx[r["code"]][ds].add(classify(r["nm"]))
    return idx


def cat_in_window(idx, code, dates, i):
    """T-CAT_LOOKBACK..T0 내 공시 클래스 집합."""
    s = set()
    cm = idx.get(code) or idx.get(code.zfill(6))
    if not cm:
        return s
    for k in range(max(0, i - CAT_LOOKBACK), i + 1):
        s |= cm.get(dates[k], set())
    return s


def _median(x):
    if not x:
        return 0.0
    s = sorted(x); n = len(s)
    return round(s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2, 2)


def _avg(x):
    return round(sum(x) / len(x), 2) if x else 0.0


def run(exit_kind, refetch):
    print("DART 공시 수집/로드 ...")
    discl = fetch_dart(refetch)
    idx = build_index(discl)
    print(f"  공시 {len(discl)}건 / 종목 {len(idx)}개 (POS {sum(1 for r in discl if classify(r['nm'])=='POS')} · "
          f"NEG {sum(1 for r in discl if classify(r['nm'])=='NEG')})")

    dpaths = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        dpaths[os.path.basename(f)[:-4].split("_")[-1]] = f

    acc = {}
    for code, dp in dpaths.items():
        name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(dp)
        if len(d) < STEADY_HIGH_N + 25:
            continue
        dates = [x[0] for x in d]
        for i in range(STEADY_HIGH_N, len(d) - 21):
            if dates[i] < "2026-02-04" or dates[i] > "2026-05-05":   # 공시 윈도 & fwd 확보
                continue
            t = detect(d, i)
            if not t:
                continue
            fm = fwd_metrics(d, i)
            if not fm:
                continue
            cats = cat_in_window(idx, code, dates, i)
            if "POS" in cats:
                cl = "호재공시"
            elif "NEG" in cats:
                cl = "악재공시"
            else:
                cl = "공시없음/중립"
            net, hold = exit_variant(d, i, exit_kind)
            lead = 1 if fm["mfe"] >= LEADER_MFE else 0
            for key in ((t, cl), (t, "ALL"), ("ALL", cl), ("ALL", "ALL")):
                a = acc.setdefault(key, {"net": [], "lead": 0, "n": 0})
                a["net"].append(net); a["lead"] += lead; a["n"] += 1
    return acc


def selftest():
    ok = []
    ok.append(("T1 호재분류", classify("단일판매ㆍ공급계약체결") == "POS" and classify("자기주식취득결정") == "POS"))
    ok.append(("T2 악재분류", classify("유상증자결정") == "NEG" and classify("전환사채발행결정") == "NEG"))
    ok.append(("T3 중립", classify("분기보고서") == "NEU"))
    idx = build_index([{"date": "20260310", "code": "005930", "nm": "단일판매공급계약체결"}])
    s = cat_in_window(idx, "005930", ["2026-03-08", "2026-03-09", "2026-03-10"], 2)
    ok.append(("T4 윈도조인", "POS" in s))
    ok.append(("T5 median", _median([5, 1, 3]) == 3))
    print("catalyst enrich 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--exit", default="ma10")
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    acc = run(a.exit, a.refetch)
    print("=" * 90)
    print(f"leader_catalyst_enrich — 재료/명분(DART) 결합 (2026-02~05, 청산={a.exit})")
    print(f"★ 질문: 호재공시(명분) 있는 후보가 음수 median을 양으로 끌어올리는가")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / DART 조회만 / SAJANG·scheduler·systemd 무변경")
    print("=" * 90)
    for t in ("SURGE", "STEADY", "ALL"):
        print(f"\n■ {t}")
        print(f"  {'명분클래스':<16}{'건수':>6}{'median%':>9}{'mean%':>8}{'precision%':>11}{'승률%':>7}")
        for cl in ("호재공시", "공시없음/중립", "악재공시", "ALL"):
            a2 = acc.get((t, cl))
            if not a2 or a2["n"] == 0:
                continue
            nets = a2["net"]; prec = 100 * a2["lead"] / a2["n"]
            wr = 100 * sum(1 for x in nets if x > 0) / len(nets)
            star = " ★median+" if _median(nets) > 0 else ""
            print(f"  {cl:<16}{a2['n']:>6}{_median(nets):>9}{_avg(nets):>8}{prec:>11.0f}{wr:>7.0f}{star}")
    print("\n★★ 판정: 호재공시 median이 공시없음보다 유의하게↑(특히 양전환)이면 = 명분이 레버.")
    print("   차이없으면 = report_nm 키워드론 부족(본문·신선도·재료강도 필요) or 명분도 단독 레버 아님.")
    print("   정직: report_nm만(본문미파싱)·생존편향·일봉·실적중립처리. 절대치X 클래스간 상대·median부호.")


if __name__ == "__main__":
    main()
