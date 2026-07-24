# -*- coding: utf-8 -*-
"""S-2 재설계 — 동적 명분 클러스터 수확 백테스트 (7/24 사장님 승인 "1번 재설계").

[왜 재설계인가 — 7/24 D-DAY 판정]
구 ⑯(cluster_harvest_paper.py)은 클러스터를 INFRA_KW(건설/전선/전력) 종목명 키워드로
하드코딩했다. 6월말 건설 테마가 식은 7/7 이후 13거래일 신호 0 → 표본 9건(승률 11.1%
합 -18.0%p)이 전부 단일 테마·단일 국면(급등 끝물)에 갇혔다. 그동안 시장에는
HLB 9종목(7/15)·로봇 4종목(7/22)·형지 3종목(7/16) 같은 클러스터가 계속 있었다.
→ 가설이 기각된 게 아니라 "검증조차 안 된" 상태. 테마 무관 동적 감지로 재설계한다.

[구 구현에서 추가 발견한 결함 — v2에서 차단]
우선주를 별개 종목으로 세어 가짜 클러스터를 만들었다. 예: 6/29 "금호건설우/금호건설/
남화토건" = 실제 2社인데 MIN_CLUSTER=3을 충족. 구 신호 4회 중 3회가 이 중복으로 성립.
→ v2는 우선주 접미사를 정규화한 뒤 회사 단위로 센다(_base_name).

★★★ 판정 기준 — 백테스트 실행 "전"에 고정한다 (7/21 교훈: 관측 전에 판정 방법부터) ★★★
    이 상수들은 결과를 본 뒤 바꾸지 않는다. 바꾸려면 사장님 승인 + 사유를 커밋에 남긴다.
    JUDGE_PRIMARY : 실행가능 순누적(exec_sum_net) > 0        ← 주기준
    JUDGE_MIN_N   : 체결 30건 미만이면 "판정 유예"(표본부족)  ← 무표본을 성공/실패로 위장 금지
    JUDGE_LIFT    : 전략군 avg_net > 대조군 avg_net           ← 클러스터 소속이 실제로 기여해야 채택
    세 조건을 모두 만족해야 GO 후보. 하나라도 실패 = 폐기 권고(7/23 교훈: 지표 선택이 곧 판정).

[실행 가능성 규약 — 사장님 자금 룰과 대조 (7/23 교훈)]
무제한 합산은 실행 불가능한 숫자놀음이다. 아래 제약 하의 체결만 판정에 쓴다.
    DAILY_NEW_MAX=3   하루 신규 진입 상한 (top_k)
    CONCURRENT_MAX=5  동시 보유 상한 (30% 현금보유 + split_cash 하에서 가능한 분산)
    COST_RT=0.2       왕복 비용 %p — ⑲-3 v2 페이퍼 규약 미러링(새 규약 만들지 않음)

[look-ahead 0]
D일 종가 확정 후 클러스터 판정 → D+1 시가 진입. 청산은 진입 다음날부터 OHLC로 체크.
구 ⑯의 검증된 replay 규약(SL 우선 → TP → 타임스톱)을 그대로 미러링한다.

★★★ 안전 불변식 ★★★
- read-only 분석 도구. 매수/매도/picks/SAJANG/order_intent 0 접촉. 실주문 0.
- 산출물은 data_store/cluster_v2_backtest.json 하나. 구 ⑯ 장부 무접촉.
- 이 백테스트 결과만으로 라이브 전환 금지 — 판정 → 사장님 결정 → 그 다음.

실행: python tools/cluster_v2_backtest.py [--months 12] [--k 3] [--verify-limitup]
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DS = BASE / "data_store"
DAILY = DS / "daily"
UNIVERSE = DS / "universe.json"
THEME_MAP = DS / "theme_map.json"
UPLOW = DS / "ranking_snapshots" / "uplowprice.csv"
OUT = DS / "cluster_v2_backtest.json"

# ── 판정 기준 (사전 고정 — 결과 보고 변경 금지) ──────────────────────────
JUDGE_MIN_N = 30
JUDGE_PRIMARY = "exec_sum_net > 0"
JUDGE_LIFT = "strategy avg_net > control avg_net"

# ── 실행 가능성 제약 (사장님 자금 룰 대조) ──────────────────────────────
DAILY_NEW_MAX = 3
CONCURRENT_MAX = 5
COST_RT = 0.2

# ── 매매 규약 (구 ⑯ 검증판 미러링) ──────────────────────────────────────
DEF_TP, DEF_SL, DEF_HOLD, DEF_K = 6.0, -3.0, 3, 3
MIN_CLUSTER = 3            # 회사 단위(우선주 dedup 후) 상한가 >=3 = 클러스터 hot
FOLLOW_LO, FOLLOW_HI = 0.0, 12.0
LIMIT_UP_CHG = 29.0
MIN_PREFIX = 2             # 그룹명 클러스터 최소 공통 접두어 길이

# 우선주/특수 접미사 — 같은 회사를 여러 종목으로 세지 않기 위한 정규화
_PREF_SUFFIX = ("우B", "우C", "1우", "2우B", "2우", "3우B", "3우", "우")


def _base_name(name: str) -> str:
    """'금호건설우'→'금호건설', 'CJ씨푸드1우'→'CJ씨푸드'. 회사 단위 카운트용."""
    n = (name or "").strip()
    for s in _PREF_SUFFIX:
        if len(n) > len(s) + 1 and n.endswith(s):
            return n[: -len(s)]
    return n


def _load_json(p, default):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return default


def _load_daily_window(code, start):
    """start 이후 행만 {date: (o,h,l,c)}. 메모리 절약(VPS RAM 1.9GB)."""
    f = DAILY / f"{code}.csv"
    out = {}
    if not f.exists():
        return out
    try:
        lines = f.read_text(encoding="utf-8").splitlines()[1:]
    except Exception:  # noqa: BLE001
        return out
    for ln in lines:
        p = ln.split(",")
        if len(p) < 5 or p[0] < start:
            continue
        try:
            out[p[0]] = (float(p[1]), float(p[2]), float(p[3]), float(p[4]))
        except ValueError:
            continue
    return out


def _name_book():
    """종목명 사전 — universe.json 우선, uplowprice.csv로 보강.

    ★universe.json만 쓰면 prune된 종목·신규상장이 통째로 빠진다(1차 실행에서
      상한가 재현율 80%·놓친 27종목 전부 universe 밖으로 확인). 이름을 못 구해도
      코드로 대체해 스캔에는 반드시 포함시킨다(테마 클러스터는 코드 기반이라 유효).
    """
    uni = _load_json(UNIVERSE, {})
    book = {c: (uni[c].get("name", c) if isinstance(uni[c], dict) else c) for c in uni}
    if UPLOW.exists():
        try:
            for ln in UPLOW.read_text(encoding="utf-8-sig").splitlines()[1:]:
                p = ln.split(",")
                if len(p) >= 3 and p[1] not in book:
                    book[p[1]] = p[2]
        except Exception:  # noqa: BLE001
            pass
    return uni, book


def _scan(start):
    """전 종목 1패스: 일봉 윈도우 + 상한가일 추출.

    ★순회 대상은 universe가 아니라 daily/*.csv 전체 — 커버리지 누락이 클러스터
      과소카운트로 이어져 신호 자체를 사라지게 하기 때문(1차 실행 결함 fix).
    등락률은 CSV 7번째 컬럼을 쓰지 않고 전일 종가 대비로 직접 계산한다
    (컬럼 결측·정의 차이에 의존하지 않기 위함 — --verify-limitup으로 교차검증).
    """
    uni, name_of = _name_book()
    px = {}                       # {code: {date: (o,h,l,c)}}
    limitups = defaultdict(list)  # {date: [(code, name, chg)]}
    chg_of = {}                   # {code: {date: chg_pct}}
    for f in sorted(DAILY.glob("*.csv")):
        code = f.stem
        dd = _load_daily_window(code, start)
        if not dd:
            continue
        px[code] = dd
        days = sorted(dd)
        cmap = {}
        for i in range(1, len(days)):
            prev_c = dd[days[i - 1]][3]
            if prev_c <= 0:
                continue
            chg = (dd[days[i]][3] / prev_c - 1) * 100
            cmap[days[i]] = chg
            if chg >= LIMIT_UP_CHG:
                limitups[days[i]].append((code, name_of.get(code, code), round(chg, 2)))
        chg_of[code] = cmap
    return uni, name_of, px, chg_of, limitups


def _clusters_of_day(ups, code_to_themes, name_of):
    """상한가 종목들 → 클러스터 목록. 회사 단위(우선주 dedup)로 MIN_CLUSTER 이상만.

    두 경로를 모두 본다:
      (a) theme_map 공통 테마 — 302테마 사전 매핑(4/10 스냅샷)
      (b) 종목명 공통 접두어 — 계열사 클러스터(HLB*, 형지*)·신규 테마 커버
    반환: [(kind, key, [codes])]
    """
    out = []
    # (a) 테마
    by_theme = defaultdict(list)
    for code, name, _chg in ups:
        for t in code_to_themes.get(code, []):
            by_theme[(t.get("theme_code"), t.get("theme_name"))].append(code)
    for (tc, tn), codes in by_theme.items():
        if len({_base_name(name_of.get(c, c)) for c in codes}) >= MIN_CLUSTER:
            out.append(("theme", f"{tc}:{tn}", sorted(set(codes))))
    # (b) 그룹명 접두어
    by_pref = defaultdict(list)
    for code, name, _chg in ups:
        b = _base_name(name)
        for ln in range(MIN_PREFIX, min(len(b), 5) + 1):
            by_pref[b[:ln]].append(code)
    for pref, codes in by_pref.items():
        if len({_base_name(name_of.get(c, c)) for c in codes}) >= MIN_CLUSTER:
            out.append(("group", pref, sorted(set(codes))))
    return out


def _cluster_members(cl_kind, cl_key, themes, name_of, uni):
    """클러스터 소속 종목 전체(후발주 후보 풀)."""
    if cl_kind == "theme":
        tc = cl_key.split(":", 1)[0]
        return list((themes.get(tc, {}) or {}).get("codes", []))
    return [c for c in uni if _base_name(name_of.get(c, c)).startswith(cl_key)]


def _followers(members, up_codes, px, chg_of, D, k, name_of=None, taken=None):
    """후발주 = 소속 종목 중 상한가 아님 · D일 등락 0~+12%(아직 안 터짐). 상승폭 큰 순 top k.

    ★회사 단위 dedup: 보통주와 우선주를 둘 다 담으면 분산이 아니라 같은 회사 2배 베팅이다
      (1차 실행에서 '금호석유화학 + 금호석유화학우' 동시 선정 확인). taken은 호출자가
      여러 클러스터에 걸쳐 누적 dedup하기 위해 넘기는 회사명 집합.
    """
    cands = []
    for c in members:
        if c in up_codes or c not in px:
            continue
        chg = (chg_of.get(c) or {}).get(D)
        if chg is None or not (FOLLOW_LO <= chg <= FOLLOW_HI):
            continue
        cands.append((c, round(chg, 2)))
    cands.sort(key=lambda x: (-x[1], x[0]))   # 결정적 정렬(동률은 코드순)
    seen = taken if taken is not None else set()
    out = []
    for c, ch in cands:
        b = _base_name((name_of or {}).get(c, c))
        if b in seen:
            continue
        seen.add(b)
        out.append((c, ch))
        if len(out) >= k:
            break
    return out


def _replay(signals, px, days, tp, sl, hold, tag):
    """결정적 replay — pending(D 결정) → D+1 시가 진입 → OHLC 청산.

    제약: 하루 신규 <= DAILY_NEW_MAX, 동시보유 <= CONCURRENT_MAX (사장님 자금 룰 대조).
    비용: 왕복 COST_RT %p 차감(net). 구 ⑯ 규약 미러링 + 7/23 실행가능성 규약 결합.
    """
    idx = {d: i for i, d in enumerate(days)}
    open_pos, closed, skipped = [], [], defaultdict(int)
    for i, D in enumerate(days):
        # 1) 전일 결정분 진입
        for code, nm, dday, chgd, ck in signals.get(days[i - 1], []) if i > 0 else []:
            if len(open_pos) >= CONCURRENT_MAX:
                skipped["concurrent_cap"] += 1
                continue
            if sum(1 for p in open_pos if p["entry_day"] == D) >= DAILY_NEW_MAX:
                skipped["daily_cap"] += 1
                continue
            dd = px.get(code) or {}
            if D not in dd or dd[D][0] <= 0:
                skipped["no_fill"] += 1
                continue
            e = dd[D][0]
            open_pos.append({"code": code, "name": nm, "decide_day": dday, "entry_day": D,
                             "entry": e, "chg_decide": chgd, "cluster": ck,
                             "tp_px": e * (1 + tp / 100), "sl_px": e * (1 + sl / 100),
                             "exit_by_idx": i + hold})
        # 2) 청산 (SL 우선 → TP → 타임스톱)
        still = []
        for pos in open_pos:
            dd = px.get(pos["code"]) or {}
            if D not in dd:
                still.append(pos)
                continue
            o, h, l, c = dd[D]
            ret = why = None
            if D != pos["entry_day"]:
                if l <= pos["sl_px"]:
                    ret, why = sl, "SL"
                elif h >= pos["tp_px"]:
                    ret, why = tp, "TP"
            if ret is None and i >= pos["exit_by_idx"]:
                ret = (c / pos["entry"] - 1) * 100 if pos["entry"] > 0 else 0.0
                why = "HOLD"
            if ret is not None:
                closed.append({"decide_day": pos["decide_day"], "entry_day": pos["entry_day"],
                               "exit_day": D, "code": pos["code"], "name": pos["name"],
                               "cluster": pos["cluster"], "ret": round(ret, 2),
                               "net": round(ret - COST_RT, 2), "why": why})
            else:
                still.append(pos)
        open_pos = still
    nets = [t["net"] for t in closed]
    wins = [n for n in nets if n > 0]
    return {"tag": tag, "n": len(closed), "wins": len(wins),
            "win_pct": round(100 * len(wins) / len(nets), 1) if nets else None,
            "sum_net": round(sum(nets), 2), "avg_net": round(sum(nets) / len(nets), 3) if nets else None,
            "skipped": dict(skipped), "trades": closed}


def verify_limitup(limitups, trading_days=None):
    """daily 역산 상한가를 uplowprice.csv(19일 실측)와 교차검증 — 역산 신뢰성 근거.

    ★비거래일 제외: uplowprice에는 토요일 수집분(예: 2026-06-27)이 섞여 있는데
      일봉에는 그 날짜가 없다. 대조에 넣으면 재현율이 구조적으로 깎여
      "역산이 부실하다"는 잘못된 결론이 난다(1차 실행 오탐).
    """
    if not UPLOW.exists():
        return {"error": "uplowprice.csv 없음"}
    ref = defaultdict(set)
    try:
        for ln in UPLOW.read_text(encoding="utf-8-sig").splitlines()[1:]:
            p = ln.split(",")
            if len(p) >= 5:
                try:
                    if float(p[4]) >= LIMIT_UP_CHG:
                        ref[p[0]].add(p[1])
                except ValueError:
                    pass
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:120]}
    rows, tot_ref, tot_hit, nontrading = [], 0, 0, []
    tdset = set(trading_days or [])
    for d in sorted(ref):
        if tdset and d not in tdset:
            nontrading.append(d)      # 토요일 스냅샷 등 — 대조 대상 아님
            continue
        got = {c for c, _n, _g in limitups.get(d, [])}
        hit = len(ref[d] & got)
        tot_ref += len(ref[d])
        tot_hit += hit
        rows.append({"date": d, "ref": len(ref[d]), "derived": len(got), "match": hit,
                     "missed": sorted(ref[d] - got)[:5]})
    return {"days": len(rows), "ref_total": tot_ref, "matched": tot_hit,
            "recall_pct": round(100 * tot_hit / tot_ref, 1) if tot_ref else None,
            "excluded_nontrading": nontrading, "rows": rows}


def run(months=12, k=DEF_K, tp=DEF_TP, sl=DEF_SL, hold=DEF_HOLD, do_verify=False, save=True):
    start = (datetime.now() - timedelta(days=int(months * 30.5))).strftime("%Y-%m-%d")
    uni, name_of, px, chg_of, limitups = _scan(start)
    if not px:
        return {"error": "일봉 입력 부족"}
    tmj = _load_json(THEME_MAP, {})
    themes = tmj.get("themes", {}) or {}
    code_to_themes = tmj.get("code_to_themes", {}) or {}
    days = sorted({d for dd in px.values() for d in dd})

    sig_strat, sig_ctrl = defaultdict(list), defaultdict(list)
    cluster_days = []
    for D in days:
        ups = limitups.get(D) or []
        if len(ups) < MIN_CLUSTER:
            continue
        cls = _clusters_of_day(ups, code_to_themes, name_of)
        if not cls:
            continue
        up_codes = {c for c, _n, _g in ups}
        picked, in_cluster, taken = [], set(), set()
        for kind, key, codes in cls:
            members = _cluster_members(kind, key, themes, name_of, uni)
            in_cluster.update(members)
            if len(picked) >= k:
                continue   # 이미 K개 확보 — 나머지 클러스터는 소속집합만 기록(대조군 정의용)
            for c, ch in _followers(members, up_codes, px, chg_of, D,
                                    k - len(picked), name_of, taken):
                picked.append((c, name_of.get(c, c), D, ch, f"{kind}:{key}"))
        if picked:
            sig_strat[D] = picked
            cluster_days.append({"date": D, "limitups": len(ups),
                                 "clusters": [f"{a}:{b}({len(c)})" for a, b, c in cls][:6],
                                 "picks": [p[1] for p in picked]})
            # 대조군: 같은 날 · 클러스터 밖 종목 · 동일 필터·동일 dedup top k
            outside = [c for c in px if c not in in_cluster]
            sig_ctrl[D] = [(c, name_of.get(c, c), D, ch, "control")
                           for c, ch in _followers(outside, up_codes, px, chg_of, D,
                                                   k, name_of, set())]

    strat = _replay(sig_strat, px, days, tp, sl, hold, "cluster_v2")
    ctrl = _replay(sig_ctrl, px, days, tp, sl, hold, "control_noncluster")

    # ── 판정 (사전 고정 기준으로만) ──────────────────────────────────
    n_ok = strat["n"] >= JUDGE_MIN_N
    primary_ok = strat["sum_net"] > 0
    lift_ok = (strat["avg_net"] is not None and ctrl["avg_net"] is not None
               and strat["avg_net"] > ctrl["avg_net"])
    if not n_ok:
        verdict = "판정 유예(표본 부족)"
    elif primary_ok and lift_ok:
        verdict = "GO 후보 — 사장님 결정 필요"
    else:
        verdict = "미달 — 폐기 권고"
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "window": {"start": start, "end": days[-1] if days else None,
                   "trading_days": len(days), "codes": len(px)},
        "params": {"tp": tp, "sl": sl, "hold": hold, "k": k, "min_cluster": MIN_CLUSTER,
                   "follow_range": [FOLLOW_LO, FOLLOW_HI], "cost_rt": COST_RT,
                   "daily_new_max": DAILY_NEW_MAX, "concurrent_max": CONCURRENT_MAX},
        "judge_criteria": {"primary": JUDGE_PRIMARY, "min_n": JUDGE_MIN_N, "lift": JUDGE_LIFT,
                           "fixed_before_run": True},
        "judge": {"n_ok": n_ok, "primary_ok": primary_ok, "lift_ok": lift_ok, "verdict": verdict},
        "strategy": {a: b for a, b in strat.items() if a != "trades"},
        "control": {a: b for a, b in ctrl.items() if a != "trades"},
        "signal_days": len(cluster_days),
        "cluster_days": cluster_days[-40:],
        "trades": strat["trades"],
        "note": "read-only 백테스트 / 매수·매도·picks·SAJANG·order 무접촉 / 관측 없이 flip 금지",
    }
    if do_verify:
        result["limitup_verify"] = verify_limitup(limitups, days)
    if save:
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUT)
    return result


def format_report(r):
    if r.get("error"):
        return f"[cluster_v2] {r['error']}"
    w, s, c, j = r["window"], r["strategy"], r["control"], r["judge"]
    L = [f"=== S-2 재설계 백테스트 (동적 클러스터) — {w['start']}~{w['end']} "
         f"{w['trading_days']}거래일 · {w['codes']}종목 ===",
         f"신호일 {r['signal_days']}일 · 제약: 신규<= {r['params']['daily_new_max']}/일 "
         f"· 동시<= {r['params']['concurrent_max']} · 비용 {r['params']['cost_rt']}%p",
         f"[전략] {s['n']}건 승률 {s['win_pct']}% 순누적 {s['sum_net']:+.2f}%p 평균 {s['avg_net']}",
         f"[대조] {c['n']}건 승률 {c['win_pct']}% 순누적 {c['sum_net']:+.2f}%p 평균 {c['avg_net']}",
         f"판정: 표본{'✅' if j['n_ok'] else '❌'} 주기준{'✅' if j['primary_ok'] else '❌'} "
         f"리프트{'✅' if j['lift_ok'] else '❌'} → {j['verdict']}"]
    if s.get("skipped"):
        L.append(f"  (제약으로 스킵: {s['skipped']})")
    v = r.get("limitup_verify")
    if v and not v.get("error"):
        L.append(f"  [역산검증] uplowprice {v['days']}일 대조 — 재현율 {v['recall_pct']}% "
                 f"({v['matched']}/{v['ref_total']})")
    for d in r.get("cluster_days", [])[-8:]:
        L.append(f"  {d['date']} 상한가{d['limitups']} → {','.join(d['clusters'][:3])} "
                 f"→ {','.join(d['picks'])}")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=float, default=12)
    ap.add_argument("--k", type=int, default=DEF_K)
    ap.add_argument("--tp", type=float, default=DEF_TP)
    ap.add_argument("--sl", type=float, default=DEF_SL)
    ap.add_argument("--hold", type=int, default=DEF_HOLD)
    ap.add_argument("--verify-limitup", action="store_true")
    ap.add_argument("--no-save", action="store_true")
    a = ap.parse_args()
    try:
        r = run(months=a.months, k=a.k, tp=a.tp, sl=a.sl, hold=a.hold,
                do_verify=a.verify_limitup, save=not a.no_save)
        print(format_report(r))
    except Exception as e:  # noqa: BLE001
        print(f"[cluster_v2] ERROR (fail-soft·read-only): {str(e)[:200]}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
