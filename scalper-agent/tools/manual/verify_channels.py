# -*- coding: utf-8 -*-
"""verify_channels.py — 기초데이터 채널 전수 계수 (read-only·매매 무접촉)

[F-81] (8/1 등재 → 8/5 승격) 채널 전수 계수 도구화.
7/31·8/1·8/5 세 번의 적재 확인에서 매번 scratchpad에 새로 짰고, **새로 짰기 때문에 틀렸다.**

왜 이 도구가 필요한가 (설계 근거 = 실제로 당한 사고 3건)
--------------------------------------------------------------------------
[F-82] 8/1 — 날짜 표기 3종(`YYYY-MM-DD` / `YYYYMMDD` / BOM 선두)을 즉석 스크립트가
       못 읽어 ④ 시장11주체·⑧ 순위를 "최신 없음"으로 오판. 검증기가 맞고 단타봇이 틀렸다.
[F-82] 8/5 — ticks의 price를 **index 2(`change`)로 하드코딩**해 유효율을 64.84%로 오산
       (정본 `ticks_health`는 100.0%). 허위 경보 직전에 정본 대조로 걸렀다.
       → **처방: 컬럼은 절대 인덱스로 찾지 않는다. 헤더 이름으로만 찾는다.**
[F-88] 8/5 — 거래정지 종목은 daily만 멈추고 flow/short는 최대 6주+ 계속 기록된다.
       → **처방: 커버리지를 전체 분모와 활성 분모 양쪽으로 낸다.**

단일진실 위임 (로직 복제 0건 — [F-37] 교훈)
--------------------------------------------------------------------------
  - 유니버스        : `data.data_verifier._all_universe_codes`
  - 장중틱 판정      : `tools.playbook_shadow.ticks_health`
  이 도구는 **세는 일만** 한다. 판정 임계·유니버스·틱 건강도를 자체 정의하지 않는다.

안전 불변식
--------------------------------------------------------------------------
  read-only · 실주문 0 · 매도 무접촉 · CSV 쓰기 0 · KRX 무접촉(nationality 계열 제외)

Usage:
    python3.11 -X utf8 tools/manual/verify_channels.py --date 2026-08-04
    python3.11 -X utf8 tools/manual/verify_channels.py --date 2026-08-03 2026-08-04
    python3.11 -X utf8 tools/manual/verify_channels.py --date 2026-08-04 --selftest
"""

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Sequence

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # scalper-agent/
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from data.data_verifier import _all_universe_codes  # noqa: E402  (유니버스 정본)

DS = BASE_DIR / "data_store"

# 자기검증 앵커 — 8/1·8/5 업무일지에 기록된 실측치. 이 값이 재현되지 않으면
# 도구가 바뀐 것이거나 데이터가 바뀐 것이므로 **결과를 인용하기 전에 멈춘다.**
SELFTEST_ANCHORS = {
    "2026-07-31": {"ticks_rows": 47_978, "ticks_pct": 100.00, "daily_ok": 2_492},
    "2026-08-03": {"ticks_rows": 47_720, "ticks_pct": 100.00, "daily_ok": 2_491},
    "2026-08-04": {"ticks_rows": 50_400, "ticks_pct": 100.00, "daily_ok": 2_490},
}


# ──────────────────────────────────────────────────────────────────────
# 공용 판독기 — [F-82] 처방. 모든 채널 읽기는 이 두 함수만 통과한다.
# ──────────────────────────────────────────────────────────────────────
def norm_date(s: str) -> str:
    """날짜 표기 3종을 `YYYYMMDD`로 정규화 (BOM·공백 제거 포함)."""
    s = s.lstrip("﻿").strip().strip('"')
    return s.replace("-", "").replace("/", "")


def read_header(path: Path) -> Optional[List[str]]:
    """헤더 행을 컬럼명 리스트로. utf-8-sig로 BOM 흡수."""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            line = fh.readline()
    except OSError:
        return None
    if not line:
        return None
    return [c.strip().lstrip("﻿") for c in line.rstrip("\n").split(",")]


def col(header: Sequence[str], name: str) -> Optional[int]:
    """★컬럼은 이름으로만 찾는다 (8/5 index 하드코딩 사고의 직접 처방)."""
    try:
        return list(header).index(name)
    except ValueError:
        return None


def find_row(path: Path, target: str, tail_bytes: int = 8192) -> Optional[List[str]]:
    """정규화된 target 날짜의 행을 필드 리스트로 반환. 없으면 None.

    꼬리 tail_bytes만 읽는다(누적 CSV가 수십 MB이므로). 기초데이터는 시간순
    append라 최근 날짜는 항상 꼬리에 있다.
    """
    if not path.exists():
        return None
    tgt = norm_date(target)
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - tail_bytes))
            raw = fh.read()
    except OSError:
        return None
    for line in reversed(raw.decode("utf-8-sig", errors="replace").splitlines()):
        if not line:
            continue
        fields = line.split(",")
        if norm_date(fields[0]) == tgt:
            return [f.strip() for f in fields]
    return None


def last_date(path: Path, tail_bytes: int = 8192) -> Optional[str]:
    """마지막 데이터 행의 날짜(정규화 전 원문 표기)."""
    if not path.exists():
        return None
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - tail_bytes))
            raw = fh.read()
    except OSError:
        return None
    for line in reversed(raw.decode("utf-8-sig", errors="replace").splitlines()):
        head = norm_date(line.split(",")[0])
        if head.isdigit() and len(head) == 8:
            return line.split(",")[0].lstrip("﻿").strip()
    return None


def num(fields: Sequence[str], header: Sequence[str], name: str) -> Optional[float]:
    """헤더 이름으로 찾은 컬럼의 수치. 공란·비수치는 None(= soft-fail 행 구분)."""
    i = col(header, name)
    if i is None or i >= len(fields):
        return None
    try:
        return float(fields[i])
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────
# 채널별 계수
# ──────────────────────────────────────────────────────────────────────
def census_daily(codes: List[str], d: str) -> Dict:
    """① 종가 OHLCV. 활성 종목 집합을 여기서 정의한다([F-88]: 정지 판별은 daily 단독)."""
    ok, zero, miss = 0, 0, 0
    active: List[str] = []
    zero_codes: List[str] = []
    for c in codes:
        p = DS / "daily" / f"{c}.csv"
        row = find_row(p, d)
        if row is None:
            miss += 1
            continue
        hdr = read_header(p) or []
        close = num(row, hdr, "종가")
        if close is None or close <= 0:
            zero += 1
            zero_codes.append(c)
        else:
            ok += 1
            active.append(c)
    return {"ok": ok, "zero": zero, "miss": miss, "active": active, "zero_codes": zero_codes}


def census_pair(codes: List[str], active: List[str], d: str,
                subdir: str, suffix: str) -> Dict:
    """전체 분모·활성 분모 양쪽 커버리지([F-88] 처방)."""
    hit = {c for c in codes if find_row(DS / subdir / f"{c}{suffix}.csv", d) is not None}
    act = set(active)
    return {
        "all_hit": len(hit), "all_n": len(codes),
        "act_hit": len(hit & act), "act_n": len(act),
        "stale_hit": len(hit - act),   # daily는 죽었는데 이 채널만 살아있는 종목 수
    }


def census_investor_placeholder(codes: List[str], d: str) -> Dict:
    """② 투자자수급 placeholder — 8/1 [F-49] 정정 판정식.

    "수급 0 = 실패"가 아니라 **종가·전일대비까지 0인 행**만 placeholder로 센다
    (거래정지 종목의 수급 0은 KIS가 준 실제값이므로 실패가 아니다).
    """
    ph, rows = 0, 0
    ph_codes: List[str] = []
    for c in codes:
        p = DS / "flow" / f"{c}_investor.csv"
        row = find_row(p, d)
        if row is None:
            continue
        rows += 1
        hdr = read_header(p) or []
        close, chg = num(row, hdr, "종가"), num(row, hdr, "전일대비")
        if close == 0 and chg == 0:
            ph += 1
            ph_codes.append(c)
    return {"rows": rows, "placeholder": ph, "codes": ph_codes[:10]}


def census_credit(codes: List[str]) -> Dict:
    """⑤ 신용잔고 — T+지연 채널이라 '해당일 존재'가 아니라 최신일 분포로 본다."""
    dist: Counter = Counter()
    for c in codes:
        ld = last_date(DS / "credit" / f"{c}_credit_bal.csv")
        if ld:
            dist[norm_date(ld)] += 1
    return {"top": dist.most_common(3), "stored": sum(dist.values())}


def census_market(d: str) -> Dict:
    """⑥ 시장 11주체 — kospi·kosdaq 양쪽([F-54] 사각 상시 개방)."""
    out = {}
    for mk in ("kospi", "kosdaq"):
        p = DS / "flow_market" / f"{mk}_investor.csv"
        out[mk] = {"has": find_row(p, d) is not None, "last": last_date(p)}
    return out


def census_ranking(d: str) -> Dict:
    """⑦ 순위 스냅샷 6종 — [F-53] night_futures 상시 미생산 가시화."""
    names = ("volume", "fluctuation", "strength", "foreign_inst", "uplowprice", "night_futures")
    out = {}
    for n in names:
        p = DS / "ranking_snapshots" / f"{n}.csv"
        out[n] = {"exists": p.exists(), "has": find_row(p, d, 65536) is not None,
                  "last": last_date(p, 65536)}
    return out


def census_ticks(d: str) -> Dict:
    """⑧ 장중틱 — 전수 계수(자체) + 정본 ticks_health 판정(위임) 양쪽.

    ★두 값을 나란히 내는 것이 이 도구의 핵심 안전장치다. 8/5에 자체 계수가
    64.84%를 냈을 때 정본이 100.0%였고, **정본이 옳았다.** 불일치 시 경고한다.
    """
    ymd = norm_date(d)
    tdir = DS / "ticks" / ymd
    res: Dict = {"dir": tdir.exists(), "files": 0, "rows": 0, "good": 0, "pct": 0.0,
                 "health": None, "agree": None}
    if not tdir.exists():
        return res
    files = sorted(tdir.glob("*.csv"))
    res["files"] = len(files)
    price_idx: Optional[int] = None
    for fp in files:
        hdr = read_header(fp)
        if not hdr:
            continue
        if price_idx is None:
            price_idx = col(hdr, "price")     # ★이름으로 조회 — 인덱스 하드코딩 금지
            if price_idx is None:
                res["error"] = "ticks CSV에 'price' 컬럼 없음 — 스키마 변경 의심"
                return res
        try:
            with open(fp, "r", encoding="utf-8-sig", errors="replace") as fh:
                next(fh, None)
                for line in fh:
                    fields = line.split(",")
                    if len(fields) <= price_idx:
                        continue
                    res["rows"] += 1
                    try:
                        if float(fields[price_idx]) > 0:
                            res["good"] += 1
                    except ValueError:
                        pass
        except OSError:
            continue
    res["pct"] = round(res["good"] / res["rows"] * 100, 2) if res["rows"] else 0.0

    try:
        import tools.playbook_shadow as pb   # 정본 위임
        # deep=True — 이 도구는 진단용(하루 1일치)이라 전수 생존성 스캔(~2초)을 켠다.
        # 백테스트류는 90여 일 루프라 기본값 False 유지([F-123]).
        h = pb.ticks_health(ymd, deep=True)
        res["health"] = h
        if h and h.get("usable_pct") is not None:
            res["agree"] = abs(float(h["usable_pct"]) - res["pct"]) <= 1.0
    except Exception as e:  # pragma: no cover - 정본 부재 시에도 계수는 살린다
        res["health"] = {"error": str(e)}
    return res


# ──────────────────────────────────────────────────────────────────────
def run(d: str, codes: List[str]) -> Dict:
    print("=" * 68)
    print(f"■ {d} 채널 전수 계수 (유니버스 {len(codes)}종 · read-only)")
    print("=" * 68)

    dly = census_daily(codes, d)
    active = dly["active"]
    n = len(codes)
    print(f"① 종가(daily)      {dly['ok']}/{n} ({dly['ok']/n*100:.1f}%) · 종가0 {dly['zero']}건 · 행부재 {dly['miss']}건")
    if dly["zero_codes"]:
        print(f"   ★종가0 종목: {dly['zero_codes'][:10]}")

    inv = census_pair(codes, active, d, "flow", "_investor")
    ph = census_investor_placeholder(codes, d)
    print(f"② 투자자수급        전체 {inv['all_hit']}/{inv['all_n']} ({inv['all_hit']/n*100:.1f}%) · "
          f"활성 {inv['act_hit']}/{inv['act_n']} ({inv['act_hit']/max(1,inv['act_n'])*100:.2f}%) · "
          f"정지종목 행 {inv['stale_hit']}건 · placeholder {ph['placeholder']}건")
    if ph["placeholder"]:
        print(f"   ★placeholder(종가·전일대비 0): {ph['codes']}")

    fex = census_pair(codes, active, d, "flow", "_foreign_exh")
    print(f"③ 외국인소진율      전체 {fex['all_hit']}/{n} ({fex['all_hit']/n*100:.1f}%) · "
          f"활성 {fex['act_hit']}/{fex['act_n']} ({fex['act_hit']/max(1,fex['act_n'])*100:.2f}%)")

    sh = census_pair(codes, active, d, "short", "_short_bal")
    print(f"④ 공매도            전체 {sh['all_hit']}/{n} ({sh['all_hit']/n*100:.1f}%) · "
          f"활성 {sh['act_hit']}/{sh['act_n']} ({sh['act_hit']/max(1,sh['act_n'])*100:.2f}%) · "
          f"정지종목 행 {sh['stale_hit']}건")

    cr = census_credit(codes)
    print(f"⑤ 신용잔고(T+지연)  최신일 분포 {cr['top']} · 저장 {cr['stored']}종")

    mk = census_market(d)
    for name, v in mk.items():
        mark = "✅" if v["has"] else "🚨"
        print(f"⑥ 시장11주체 {name:6s} {mark} 해당일 행 {'있음' if v['has'] else '없음'} (최신 {v['last']})")

    rk = census_ranking(d)
    for name, v in rk.items():
        if not v["exists"]:
            print(f"⑦ 순위 {name:13s} ⚠️ 파일 부재 (기존 [F-53] 상시 미생산 여부 확인)")
        else:
            print(f"⑦ 순위 {name:13s} {'✅' if v['has'] else '🚨'} 해당일 {'있음' if v['has'] else '없음'} (최신 {v['last']})")

    tk = census_ticks(d)
    if not tk["dir"]:
        print("⑧ 장중틱            🚨 디렉터리 부재")
    elif tk.get("error"):
        print(f"⑧ 장중틱            🚨 {tk['error']}")
    else:
        hv = (tk["health"] or {}).get("usable_pct")
        agree = "일치" if tk["agree"] else "★불일치 — 정본을 신뢰할 것"
        print(f"⑧ 장중틱            {tk['files']}파일 {tk['rows']:,}행 · 전수 price>0 {tk['pct']:.2f}% "
              f"| 정본 ticks_health {hv}% ({(tk['health'] or {}).get('verdict')}) → {agree}")
        _h = tk["health"] or {}
        if _h.get("live_files") is not None:
            # [F-123] price>0 100%는 정지종목 박제를 정상으로 센다 — '움직인 종목'을 병기.
            print(f"⑧-2 틱 생존성       살아있음 {_h['live_files']}/{tk['files']} "
                  f"({_h['live_pct']:.2f}%) · 박제(정지 추정) {_h['frozen_files']}건 "
                  f"— 판정 미반영·진단 전용")

    print()
    return {"daily": dly, "investor": inv, "ticks": tk}


def selftest(results: Dict[str, Dict]) -> int:
    """앵커 재현 검증 — 인용 前 자기검증(8/1 lynch_concentration 방식)."""
    print("=" * 68)
    print("■ 자기검증 — 업무일지 기록 앵커 재현")
    print("=" * 68)
    bad = 0
    for d, r in results.items():
        a = SELFTEST_ANCHORS.get(d)
        if not a:
            print(f"  {d} — 앵커 없음 (skip)")
            continue
        checks = [
            ("ticks_rows", r["ticks"]["rows"], a["ticks_rows"]),
            ("ticks_pct", r["ticks"]["pct"], a["ticks_pct"]),
            ("daily_ok", r["daily"]["ok"], a["daily_ok"]),
        ]
        for name, got, want in checks:
            mark = "✅" if got == want else "🚨"
            if got != want:
                bad += 1
            print(f"  {mark} {d} {name:11s} 실측 {got} / 기록 {want}")
    print(f"\n{'✅ 앵커 전부 재현 — 결과 인용 가능' if bad == 0 else f'🚨 {bad}건 불일치 — 인용 前 원인 규명 필수'}")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description="기초데이터 채널 전수 계수 (read-only)")
    ap.add_argument("--date", nargs="+", required=True, help="기준일 (YYYY-MM-DD, 복수 가능)")
    ap.add_argument("--selftest", action="store_true", help="업무일지 앵커 재현 검증")
    args = ap.parse_args()

    codes = _all_universe_codes()
    if not codes:
        print("🚨 유니버스 로드 실패 — 계수 불가")
        return 1

    results = {d: run(d, codes) for d in args.date}
    if args.selftest:
        return 1 if selftest(results) else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
