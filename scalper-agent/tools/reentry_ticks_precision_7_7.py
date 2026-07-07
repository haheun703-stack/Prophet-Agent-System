# -*- coding: utf-8 -*-
"""재진입 숙제#2 계승 — ticks(~20분) 해상도 정밀 재검증 (7/7).

배경: 6/1 분봉정밀(docs/03-analysis/reentry_intraday_precision_6_1.md)이 옛 1분봉
(2~3월·335이벤트·다른 코호트)에서 헤드룸 붕괴(NO-GO)를 판정. 1분봉 수집은 4/4 종료라
그 방식 재현 불가 → 본 도구는 **현 forward 코호트**(reentry_shadow.json resolved·6/16~)를
ticks(~20분·전종목·3/24~·하루 ~19관측)로 장중 재현 —
"일봉 통과(비용0.5 lift +2.82%p)가 장중 해상도에서 살아남는가"를 같은 코호트로 정면 검증.

★ 사전 판정 기준(분석 전 고정·이동 금지): 비용 0.5%/leg 차감 시
  ① ticks 해상도 lift 전체 평균 ≥ +1.5%p AND ② 재진입 발생분 lift 중앙값 > 0 → 생존(PASS)
  미달 → 재진입 라이브 NO-GO 유지(6/1 판정 계승·shadow 관측만 지속).

★★★ 안전 불변식 ★★★
- 순수 read-only 분석: 일봉 csv·ticks csv·reentry_shadow.json 읽기만.
- 주문/픽/recommendation/SAJANG 값/order_intent 0 접촉. SAJANG 헬퍼(트레일/재진입 룰) 읽기 참조만.
- 출력: stdout + data_store/reentry_ticks_precision_7_7.json (record-only 분석 산출물).

★ 정직 한계:
- ticks ~20분 관측은 봇 실제(30초 폴 래칫)보다 거칠어 휩쏘 **과소** 측정 —
  "ticks에서 붕괴 → 실전 더 나쁨(확정적)" / "ticks 생존 → 30초 대비 여전히 낙관 여지".
- 체결가 = 관측가(관측 시점 이미 선 아래면 그 가격 — 손절선 체결 낙관 배제).
- ticks 없는 날은 일봉 근사 폴백(fallback_days 카운트로 정직 표기).
- 생존편향(상폐 미포함) — 절대수익 X, 일봉 vs ticks **상대 비교만** 신뢰.

실행: python tools/reentry_ticks_precision_7_7.py [--selftest]
"""
import csv
import json
import sys
from pathlib import Path
from statistics import mean, median

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.sajang_rules import SAJANG                      # noqa: E402 — 룰 읽기 참조만
from data.sector_reversal_shadow import _daily_rows       # noqa: E402 — 일봉 단일진실 재사용

STORE = BASE / "data_store"
TICKS_DIR = STORE / "ticks"
REENTRY = STORE / "reentry_shadow.json"
OUT = STORE / "reentry_ticks_precision_7_7.json"

COST_GRID = (0.0, 0.5, 0.7)
JUDGE_COST = 0.5          # 사전 판정 기준 비용
JUDGE_LIFT_ALL = 1.5      # ① 전체 lift 평균 하한 (%p)


def _ticks_prices(day: str, code: str) -> list:
    """해당 거래일(YYYY-MM-DD)의 ~20분 관측가 리스트(시간순). 없으면 []."""
    f = TICKS_DIR / day.replace("-", "") / f"{code}.csv"
    if not f.exists():
        return []
    out = []
    try:
        with open(f, encoding="utf-8", errors="replace", newline="") as fh:
            for row in csv.DictReader(fh):        # 헤더명 접근 (playbook_shadow 패턴·CSV-001 준수)
                try:
                    p = float(row.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if p > 0:                          # 간헐 price=0 불량행 필터 (playbook ③ 교훈)
                    out.append(p)
    except Exception:
        return []
    return out


def _leg_stop_line(cur_entry: float, peak: float) -> float:
    """reentry_shadow._leg_stop_line과 동일 (SAJANG 단일진실)."""
    gain_at_peak = (peak / cur_entry - 1) * 100.0 if cur_entry > 0 else 0.0
    if SAJANG.is_trailing_activated(gain_at_peak):
        return SAJANG.get_trailing_sl(peak)
    return SAJANG.get_normal_sl(cur_entry)


def simulate_ticks(rows, di: int, kki_grade: str, code: str = None,
                   ticks_fn=None, thesis_ok: bool = True):
    """reentry_shadow._simulate의 ticks 해상도판 — 룰 동일, 손절 감시만 장중 관측.

    rows: [(date, close, high, low)] 거래일순. di: 진입(T0·종가 매수) 인덱스.
    손절 감시: T0+1부터 각 날 ticks 관측가 순회 — 관측가 ≤ 손절선 → 그 관측가로 체결(낙관 배제).
      peak watermark도 관측가로만 갱신(20분 해상도 자기일관). ticks 없는 날=일봉 근사 폴백.
    재진입: 설계 §2.1 그대로 EOD 규율 — 일봉 종가 reclaim + SAJANG.should_reenter (장중 재진입 금지).
    반환: dict | None(bar 부족 미해결).
    """
    n = len(rows)
    if di < 0 or di >= n:
        return None
    entry0 = rows[di][1]
    if entry0 <= 0:
        return None

    def _obs(day: str) -> list:
        if ticks_fn is not None:
            return ticks_fn(day, code)
        return _ticks_prices(day, code) if code else []

    legs = []
    re_count = 0
    cur_idx = di
    cur_entry = entry0
    fallback_days = 0
    n_obs = 0

    while True:
        deadline = SAJANG.effective_hold_days(kki_grade)
        peak = rows[cur_idx][2]                    # T0 고가 watermark (일봉 sim과 동일 시작)

        stop_idx = None
        stop_price = None
        j = cur_idx + 1
        end = cur_idx + deadline
        while j <= end:
            if j >= n:
                return None                        # bar 부족 → 미해결
            day = rows[j][0]
            obs = _obs(day)
            if obs:
                n_obs += len(obs)
                for p in obs:
                    sl = _leg_stop_line(cur_entry, peak)
                    if p <= sl:
                        stop_idx = j
                        stop_price = p             # 관측가 체결 (선 아래 관측=그 가격)
                        break
                    peak = max(peak, p)
                if stop_idx is not None:
                    break
            else:
                # ticks 없는 날 — 일봉 근사 폴백 (reentry_shadow._simulate와 동일)
                fallback_days += 1
                sl = _leg_stop_line(cur_entry, peak)
                if rows[j][3] <= sl:
                    stop_idx = j
                    stop_price = min(sl, rows[j][2])
                    break
                peak = max(peak, rows[j][2])
            j += 1

        if stop_idx is None:
            if end >= n:
                return None
            exit_price = rows[end][1]
            legs.append((cur_entry, exit_price, (exit_price / cur_entry - 1) * 100.0, "HOLD_EXIT"))
            break

        legs.append((cur_entry, stop_price, (stop_price / cur_entry - 1) * 100.0, "STOP"))
        if re_count >= SAJANG.REENTRY_MAX:
            break

        # EOD reclaim 재진입 (설계 §2.1 — 당일 종가·일봉과 동일 규율)
        reentered = False
        unresolved = False
        r = stop_idx
        while True:
            if r >= n:
                unresolved = True
                break
            day_offset = r - di
            if day_offset > SAJANG.effective_hold_days(kki_grade):
                break
            if SAJANG.should_reenter(rows[r][1], stop_price, kki_grade, re_count, day_offset, thesis_ok):
                re_count += 1
                cur_idx = r
                cur_entry = rows[r][1]
                reentered = True
                break
            r += 1

        if unresolved:
            return None
        if not reentered:
            break

    comp = 1.0
    for (_, _, ret, _) in legs:
        comp *= (1 + ret / 100.0)
    return {
        "stop_reenter_ret": round((comp - 1) * 100.0, 2),
        "stop_only_ret": round(legs[0][2], 2),
        "n_reentries": re_count,
        "n_legs": len(legs),
        "fallback_days": fallback_days,
        "n_obs": n_obs,
    }


def _stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "avg": round(mean(xs), 2), "med": round(median(xs), 2),
            "win_pct": round(100 * sum(1 for x in xs if x > 0) / len(xs), 1)}


def run() -> dict:
    if not REENTRY.exists():
        print("[ticks_precision] reentry_shadow.json 없음 — 중단")
        return {}
    records = [r for r in json.loads(REENTRY.read_text(encoding="utf-8"))["records"]
               if r.get("stop_reenter_ret") is not None]
    print(f"[ticks_precision] 코호트 {len(records)}건 (일봉 resolved) 로드")

    rows_cache = {}
    results = []
    skipped = 0
    for rec in records:
        code, grade = rec["code"], rec.get("kki_grade", "SLUGGISH")
        if code not in rows_cache:
            rows_cache[code] = _daily_rows(code)
        rows = rows_cache[code]
        if not rows:
            skipped += 1
            continue
        di = next((k for k, r in enumerate(rows) if r[0] == rec["date"]), None)
        if di is None:
            skipped += 1
            continue
        sim = simulate_ticks(rows, di, grade, code=code)
        if sim is None:
            skipped += 1
            continue
        results.append({
            "date": rec["date"], "code": code, "variant": rec.get("variant"),
            "kki_grade": grade,
            "daily": {"sr": rec["stop_reenter_ret"], "so": rec["stop_only_ret"],
                      "lift": rec["reentry_lift"], "n_legs": rec.get("n_legs", 1),
                      "n_re": rec.get("n_reentries", 0)},
            "ticks": sim,
        })

    print(f"[ticks_precision] ticks 재현 {len(results)}건 / skip {skipped}건 "
          f"(폴백일 총 {sum(r['ticks']['fallback_days'] for r in results)})")

    # ── 비교 요약 (같은 레코드 부분집합 — apples to apples) ──
    summary = {"n": len(results), "cost_grid": {}}
    for c in COST_GRID:
        d_lift = [r["daily"]["lift"] - c * (r["daily"]["n_legs"] - 1) for r in results]
        t_lift = [r["ticks"]["stop_reenter_ret"] - c * r["ticks"]["n_legs"]
                  - (r["ticks"]["stop_only_ret"] - c) for r in results]
        d_re = [v for r, v in zip(results, d_lift) if r["daily"]["n_re"] > 0]
        t_re = [v for r, v in zip(results, t_lift) if r["ticks"]["n_reentries"] > 0]
        d_net = [r["daily"]["sr"] - c * r["daily"]["n_legs"] for r in results]
        t_net = [r["ticks"]["stop_reenter_ret"] - c * r["ticks"]["n_legs"] for r in results]
        summary["cost_grid"][f"{c:.1f}"] = {
            "daily": {"lift_all": _stats(d_lift), "lift_re": _stats(d_re), "net": _stats(d_net)},
            "ticks": {"lift_all": _stats(t_lift), "lift_re": _stats(t_re), "net": _stats(t_net)},
        }

    # 조기손절(휩쏘) 관측: ticks가 일봉보다 첫 leg 손절이 나빠진 정도
    so_diff = [r["ticks"]["stop_only_ret"] - r["daily"]["so"] for r in results]
    stop_earlier = sum(1 for d in so_diff if d < -0.01)
    summary["whipsaw"] = {
        "stop_only_diff": _stats(so_diff),
        "ticks_worse_n": stop_earlier,
        "note": "ticks 첫leg 손절이 일봉 대비 악화된 건수/폭 — 20분 해상도 조기손절 효과",
    }
    re_daily = sum(1 for r in results if r["daily"]["n_re"] > 0)
    re_ticks = sum(1 for r in results if r["ticks"]["n_reentries"] > 0)
    summary["reentry_counts"] = {"daily": re_daily, "ticks": re_ticks}

    # ── 사전 판정 (고정 기준) ──
    jc = summary["cost_grid"][f"{JUDGE_COST:.1f}"]["ticks"]
    ok1 = (jc["lift_all"].get("avg") or -999) >= JUDGE_LIFT_ALL
    ok2 = (jc["lift_re"].get("med") or -999) > 0
    verdict = "PASS(장중 생존)" if (ok1 and ok2) else "FAIL(장중 붕괴 — 6/1 판정 유지)"
    summary["verdict"] = {
        "criteria": f"cost {JUDGE_COST}/leg: ①lift전체 avg≥{JUDGE_LIFT_ALL} ②재진입발생 lift med>0 (사전고정)",
        "check": {"①lift_all_avg": jc["lift_all"].get("avg"), "②lift_re_med": jc["lift_re"].get("med"),
                  "ok1": ok1, "ok2": ok2},
        "verdict": verdict,
    }

    out = {"summary": summary, "results": results,
           "note": ("record-only 분석(7/7) — 일봉 vs ticks(~20분) 같은 코호트 정면비교. "
                    "read-only·주문/픽/SAJANG 무접촉. ticks는 봇 30초 폴보다 거칠어 휩쏘 과소 측정.")}
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(OUT)

    # ── 출력 ──
    for c in COST_GRID:
        g = summary["cost_grid"][f"{c:.1f}"]
        print(f"\n=== cost {c}%/leg ===")
        for k in ("daily", "ticks"):
            print(f"  [{k:5s}] lift전체 {g[k]['lift_all']} | lift재진입 {g[k]['lift_re']} | net {g[k]['net']}")
    print(f"\n휩쏘: 첫leg 손절 ticks 악화 {stop_earlier}/{len(results)}건, diff {summary['whipsaw']['stop_only_diff']}")
    print(f"재진입 발생: daily {re_daily} vs ticks {re_ticks}")
    print(f"\n★ 판정({summary['verdict']['criteria']}): {verdict}")
    print("★ read-only·record-only — 관측 없이 flip 금지·flip=사장님 결정")
    return out


def _selftest() -> int:
    """합성 데이터 3 불변식 — ①ticks 없으면 일봉 폴백과 동일 ②장중 조기손절 포착 ③EOD reclaim 재진입."""
    # 합성 일봉: T0 진입 100 — 재진입 leg들이 hold 윈도우를 다 채울 만큼 길게(d8)
    rows = [("d0", 100.0, 101.0, 99.0),
            ("d1", 106.0, 107.0, 100.0),
            ("d2", 103.0, 106.5, 102.0),
            ("d3", 104.5, 105.0, 102.5),
            ("d4", 105.0, 106.0, 103.0),
            ("d5", 104.0, 105.0, 103.0),
            ("d6", 105.5, 106.0, 104.0),
            ("d7", 105.0, 106.0, 104.0),
            ("d8", 105.0, 106.0, 104.0)]
    grade = "MODERATE"   # hold 3일

    # ① ticks 전무 → 폴백 경로 = reentry_shadow._simulate와 동일 결과
    from data.reentry_shadow import _simulate as daily_sim
    no_ticks = lambda day, code: []
    a = simulate_ticks(rows, 0, grade, ticks_fn=no_ticks)
    b = daily_sim(rows, 0, grade)
    assert a and b and abs(a["stop_reenter_ret"] - b["stop_reenter_ret"]) < 1e-9, f"①폴백 불일치 {a} vs {b}"

    # ② 장중 조기손절: d1 장중 108 찍고 103으로 급락 — ticks 트레일선(고점108-3%)에 걸림.
    #    일봉 sim은 d1 무손절(low 100 > 진입가-3%=97) → ticks가 하루 일찍 잡는 것 검증.
    intraday = {"d1": [102.0, 108.0, 103.0, 106.0]}
    t = lambda day, code: intraday.get(day, [])
    a2 = simulate_ticks(rows, 0, grade, ticks_fn=t)
    assert a2 and a2["stop_only_ret"] == round((103.0 / 100 - 1) * 100, 2), f"②조기손절 실패 {a2}"

    # ③ EOD reclaim: d1 손절(103) 후 d1 종가 106 ≥ 103 → 당일 종가 재진입(re_count 1+)
    assert a2["n_reentries"] >= 1 and a2["n_legs"] >= 2, f"③재진입 실패 {a2}"

    print("[selftest] 3/3 PASS (①일봉폴백 동일 ②장중 조기손절 ③EOD reclaim 재진입)")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    try:
        run()
    except Exception as e:  # 분석 도구 — 예외도 exit 0
        print(f"[ticks_precision] 치명 예외: {e}")
    sys.exit(0)
