# -*- coding: utf-8 -*-
"""s6_sector_reversal_judge.py — S-6(sector_reversal shadow) 판정 도구 (D-DAY 2026-08-07)

배경: 7/21에 S-3~5를 **판정 방법이 없어서** 잃었다([[feedback-observe-before-judge-method-7-21]]).
S-6는 대장에 `metric: null`(수동 판정)로만 등록돼 있어 같은 자리에 서 있었다.
7/31 전체검수에서 미리 판정 방법을 코드로 고정한다 — D-DAY에 이 스크립트 출력이 곧 근거다.

판정 규약 (7/24 S-2 폐기에서 확립한 **대조군 리프트** 방식):
  절대 수익률은 생존편향·시장 레짐에 오염되므로 신뢰하지 않는다. 신뢰하는 것은
  ①신호 등급별 단조성 ②선정 vs 비선정 리프트 ③시장 대비 초과수익 — 셋의 일치.

  PASS 조건(셋 다 충족): 등급 단조 정방향 AND 선정 리프트 > 0 AND 초과수익 > 0
  그 외 = 미달 → on_fail("폐기 또는 게이트 결합") 판단을 사장님께 상신.

★ 안전: read-only(shadow 장부 읽기만)·실주문 0·매수/매도/SAJANG/picks 무접촉.
★ 자동 폐기 없음 — 폐기/연장 결정은 사장님.

실행: python3.11 -X utf8 tools/manual/s6_sector_reversal_judge.py
      python3.11 -X utf8 tools/manual/s6_sector_reversal_judge.py --horizon forward_d3
"""
import argparse
import json
import statistics
import sys
from datetime import date
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent      # scalper-agent/
SHADOW = BASE / "data_store" / "sector_reversal_shadow.json"

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# 신호 강도 순서(강 → 약). 정방향이라면 이 순서로 수익이 단조 감소해야 한다.
# ★8/5 전체검수 — "COLD"가 빠져 있어 ①에서 조용히 누락됐다(대조군 ②엔 포함).
STATUS_ORDER = ["HOT", "RELAY", "WARMING", "COOLING", "COLD"]
MIN_BUCKET_N = 10          # 이보다 작은 버킷은 방향성 근거로 쓰지 않는다(7/27 소표본 교훈)

# ★8/5 전체검수 HIGH — **측정 불가가 '미달'로 변환되는 통로를 막는다.**
#   기존 판정식 `ok = mono_fwd and (lift or 0) > 0 and ...`은 산출 불가(None)를
#   전부 False로 접어 넣어 **항상 폐기 쪽으로 기우는 비관 편향**이었다. S-1 경로엔
#   MIN_JUDGE_N 가드가 있는데(observe_v2_paper.py:358·strategy_deadline_check.py:78)
#   S-6 경로에만 없었다. 상수는 정본에서 import한다(로컬 복제 금지·[F-37] 교훈).
try:
    sys.path.insert(0, str(BASE / "tools"))
    from observe_v2_paper import MIN_JUDGE_N      # noqa: E402
except Exception:                                  # pragma: no cover
    MIN_JUDGE_N = 10

# 로컬 노트북은 VPS 미러인데 shadow/paper는 **의도적 sync 제외**다
# (sync_from_vps.py:111 · 6/10 결정 "판정은 VPS에서 직접"). 그 결과 노트북엔
# 낡은 사본이 남아 있고, 그걸 모르고 여기서 돌리면 **틀린 판정이 나온다**
# (8/5 실증: 로컬 5건/6-10 vs VPS 168건/8-04). 신선도를 판정 전에 본다.
STALE_DAYS_MAX = 5


def _agg(rows, key):
    v = [x[key] for x in rows if isinstance(x.get(key), (int, float))]
    if not v:
        return 0, None, None
    return len(v), statistics.mean(v), statistics.median(v)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", default="forward_d5",
                    choices=["forward_d1", "forward_d3", "forward_d5"])
    args = ap.parse_args()
    H = args.horizon

    try:
        doc = json.loads(SHADOW.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"[S-6] shadow 장부 로드 실패: {e} — 판정 불가(수집 복구 후 재실행)")
        return 0
    rec = doc.get("records") or []
    if not rec:
        print("[S-6] 레코드 0건 — 판정 불가")
        return 0

    # 장부 신선도 — 로컬 낡은 사본으로 판정하는 사고를 막는다(위 STALE_DAYS_MAX 주석)
    _stale_days = None
    try:
        _u = str(doc.get("updated_at") or "")[:10]
        if _u:
            _stale_days = (date.today() - date.fromisoformat(_u)).days
    except Exception:  # noqa: BLE001 — 신선도 판독 실패가 판정을 막지는 않는다
        _stale_days = None

    days = sorted({x.get("date") for x in rec if x.get("date")})
    n_all, mean_all, med_all = _agg(rec, H)
    print(f"■ S-6 sector_reversal shadow 판정 ({H})")
    print(f"  표본 {len(rec)}건 / 관측일 {len(days)}일 ({days[0]} ~ {days[-1]})"
          f" / {H} 충전 {n_all}건")
    print(f"  updated_at: {doc.get('updated_at')}")
    print()

    # ── ① 신호 등급별 단조성 ──
    print("① 신호 등급별 (강→약) — 정방향이면 수익이 단조 감소해야 한다")
    means = []
    for s in STATUS_ORDER:
        n, m, md = _agg([x for x in rec if x.get("status_5d") == s], H)
        flag = "" if n >= MIN_BUCKET_N else "  ⚠소표본(방향성 근거 제외)"
        print(f"   {s:8s} n={n:3d}  평균 {m:+7.2f}  중앙 {md:+7.2f}{flag}"
              if n else f"   {s:8s} n=  0")
        if n >= MIN_BUCKET_N and m is not None:
            means.append((s, m))
    mono_fwd = len(means) >= 3 and all(means[i][1] >= means[i + 1][1] for i in range(len(means) - 1))
    mono_rev = len(means) >= 3 and all(means[i][1] <= means[i + 1][1] for i in range(len(means) - 1))
    verdict1 = "정방향 단조 ✅" if mono_fwd else ("★역단조(강할수록 나쁨) ❌" if mono_rev else "무단조 ❌")
    print(f"   → {verdict1}  (유효 버킷 {[s for s, _ in means]})")
    print()

    # ── ② 선정 리프트 (대조군) ──
    n_t, m_t, _ = _agg([x for x in rec if x.get("selected_by_5d_hot")], H)
    n_f, m_f, _ = _agg([x for x in rec if not x.get("selected_by_5d_hot")], H)
    print("② 선정 리프트 — 선정 로직이 결과를 개선하는가 (7/24 S-2 방식)")
    print(f"   선정   n={n_t:3d}  평균 {m_t:+7.2f}" if n_t else "   선정   n=0")
    print(f"   비선정 n={n_f:3d}  평균 {m_f:+7.2f}" if n_f else "   비선정 n=0")
    lift = (m_t - m_f) if (m_t is not None and m_f is not None) else None
    small = (n_f < MIN_BUCKET_N)
    print(f"   → 리프트 {lift:+.2f}%p  {'✅' if (lift or 0) > 0 else '❌'}"
          f"{'  ⚠대조군 소표본' if small else ''}" if lift is not None else "   → 리프트 산출 불가")
    print()

    # ── ③ 시장 대비 초과수익 ──
    ex = [x[H] - x["market_return"] for x in rec
          if isinstance(x.get(H), (int, float)) and isinstance(x.get("market_return"), (int, float))]
    print("③ 시장 대비 초과수익 — 하락장 베타가 아닌지 확인")
    if ex:
        print(f"   n={len(ex)}  평균 {statistics.mean(ex):+.2f}  중앙 {statistics.median(ex):+.2f}"
              f"  {'✅' if statistics.mean(ex) > 0 else '❌'}")
    else:
        print("   산출 불가(market_return 결손)")
    print()

    # ── 종합 ──
    v = [x[H] for x in rec if isinstance(x.get(H), (int, float))]
    wr = 100 * sum(1 for x in v if x > 0) / len(v) if v else 0
    print(f"④ 승률({H} > 0): {sum(1 for x in v if x > 0)}/{len(v)} = {wr:.1f}%")
    print()
    # ── ★판정 가능 여부를 먼저 확정한다 (측정 불가 ≠ 미달) ──
    blockers = []
    if n_all < MIN_JUDGE_N:
        blockers.append(f"{H} 충전 표본 {n_all}건 < {MIN_JUDGE_N}건")
    if len(means) < 3:
        blockers.append(f"유효 등급 버킷 {len(means)}개 < 3개 (단조성 산출 불가)")
    if lift is None:
        blockers.append("대조군 리프트 산출 불가(선정/비선정 한쪽이 0건)")
    if not ex:
        blockers.append("초과수익 표본 0건")
    if _stale_days is not None and _stale_days > STALE_DAYS_MAX:
        blockers.append(f"장부가 {_stale_days}일 낡음(updated_at {doc.get('updated_at')}) "
                        f"— 로컬 미러일 가능성. VPS 원본으로 재실행할 것")

    print("=" * 66)
    if blockers:
        # ★여기서 '미달'을 출력하면 안 된다. 못 잰 것을 못 잰다고 말해야 한다.
        print("판정: ⏸ **판정 불가** — 측정 불가는 미달이 아니다")
        for b in blockers:
            print(f"   · {b}")
        print("→ on_fail 상신하지 않음. 증거 보강 후 재실행 (자동 폐기 없음·결정은 사장님)")
        print("=" * 66)
        return 0

    ok = mono_fwd and (lift or 0) > 0 and bool(ex) and statistics.mean(ex) > 0
    print(f"판정: {'✅ 충족' if ok else '❌ 미달'}  "
          f"(전체 평균 {mean_all:+.2f}%p · 승률 {wr:.1f}%)")
    if not ok:
        print("on_fail = '폐기(nightly ③ 제외) 또는 게이트 결합' → ★결정은 사장님★ (자동 폐기 없음)")
        if mono_rev:
            print("참고: 역단조는 '신호를 반대로 쓰는' 가설을 시사하나, 소표본 방향성 법칙화 금지"
                  "(7/27 교훈) — 별도 검증 전 라이브 반영 금지.")
    print("=" * 66)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 — 판정 도구: 조용한 실패 금지·그러나 차단도 금지
        print(f"[S-6] 치명 예외: {e}")
        raise SystemExit(0)
