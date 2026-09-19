# -*- coding: utf-8 -*-
"""[F-238] 외국인소진율 2차 원천 교차검증 — 네이버 캐시 vs KIS 스냅샷. read-only.

[배경] 9/11~9/18 사고의 근본은 **원천이 하나뿐**이라는 것이었다. 네이버가 페이지를
폐지하자 채널이 통째로 죽었고, 세션이 없어 12일 뒤에야 알았다. KRX는 사장님 절대룰이라
대안이 아니다. 남은 2차 원천이 KIS 현재가 API(`hts_frgn_ehrt`)다.

★★**이 도구가 잡는 것과 못 잡는 것 — 정직하게**
  잡는다  : 조용한 **값 오염**(재탕·잘못된 파싱·스케일 오류). [F-192] nationality가
            6/19 스냅샷을 77일간 재발행하던 유형이 여기 걸린다.
  못 잡는다: **단기 스테일**. 소진율은 느리게 변해서 8영업일 멈춰도 삼성전자 기준
            0.26%p(46.71→46.45)밖에 안 벌어진다 — 즉 **이번 사고는 이 도구로 못 잡았다.**
            그건 커버리지(활성 대비 적재율)가 잡는 영역이고 이미 20:10/08:30이 한다.
  ⇒ 그래서 **자동 배선을 하지 않는다.** 매일 돌릴 근거(잡히는 사례)가 아직 없다.
     수동 도구로 두고, 실제로 잡는 사례가 나오면 그때 승격한다([F-25] 정신).

[정상 대역 — 9/19 실측 119종]
  |KIS − 네이버| 중앙 **0.000%p** · p90 0.120 · p95 0.440 · p99 0.750 · 최대 2.090
  0.5%p 초과 2.5%(3종) · 1.0%p 초과 0.8%(1종)
  ⇒ 기본 임계: 중앙값 0.30%p 초과 **또는** 0.5%p 초과 종목이 20% 이상이면 경고.
    (개별 종목 차이가 아니라 **전체 경향**을 본다 — 한 종목은 늘 튄다)

★ record-only: 파일 쓰기 0 · 매수/매도/SAJANG/picks/order 무접촉 · KRX 무접촉.
★ KIS 스냅샷은 휴장일·장전에 전일 값을 준다 → **비교에만** 쓰고 캐시에 쓰지 않는다([F-170]).

실행:
    python3.11 -X utf8 tools/manual/foreign_exh_crosscheck.py --sample 120
"""
import argparse
import csv
import glob
import os
import random
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

MEDIAN_ALERT = 0.30        # %p — 전체가 틀어졌는지
OUTLIER_LIMIT = 0.50       # %p — '크게 다름'의 정의
OUTLIER_RATIO_ALERT = 0.20  # 표본 대비 비율


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=120, help="표본 종목 수")
    ap.add_argument("--seed", type=int, default=919)
    args = ap.parse_args()

    from data.flow_collector import (FLOW_DIR, _get_kis_session,
                                     _fetch_foreign_rate_kis_snapshot)

    sess = _get_kis_session()
    if not sess:
        print("[crosscheck] KIS 세션 실패 — 판정 불가(SKIP). 토큰 발급 제한일 수 있음")
        return 0
    base, headers = sess

    files = sorted(glob.glob(str(FLOW_DIR / "*_foreign_exh.csv")))
    if not files:
        print("[crosscheck] 캐시 없음 — SKIP")
        return 0
    random.seed(args.seed)
    sample = random.sample(files, min(args.sample, len(files)))

    diffs = []
    fails = 0
    both_zero = 0
    worst = []
    for f in sample:
        code = os.path.basename(f).split("_")[0]
        try:
            rows = list(csv.DictReader(open(f, encoding="utf-8")))
            nav = float(rows[-1]["소진율"]) if rows else None
        except Exception:
            nav = None
        if nav is None:
            continue
        snap = _fetch_foreign_rate_kis_snapshot(base, headers, code)
        if not snap:
            fails += 1
            time.sleep(0.06)
            continue
        kis = float(snap.get("소진율") or 0)
        if kis == 0 and nav == 0:
            both_zero += 1
        d = abs(kis - nav)
        diffs.append(d)
        worst.append((d, code, nav, kis))
        time.sleep(0.06)

    n = len(diffs)
    if n < 20:
        print(f"[crosscheck] 비교쌍 {n}건 — 표본 부족으로 판정하지 않음(SKIP). 실패 {fails}")
        return 0

    diffs.sort()
    median = diffs[n // 2]
    outliers = sum(1 for d in diffs if d > OUTLIER_LIMIT)
    ratio = outliers / n
    worst.sort(reverse=True)

    print(f"[crosscheck] 비교 {n}종 (조회 실패 {fails} · 양쪽 0.00 {both_zero})")
    print(f"  |KIS − 네이버| 중앙 {median:.3f}%p · "
          f"{OUTLIER_LIMIT}%p 초과 {outliers}종({ratio*100:.1f}%) · 최대 {diffs[-1]:.3f}")
    print("  상위 차이:", ", ".join(f"{c} {nv:.2f}→{k:.2f}" for _d, c, nv, k in worst[:5]))

    bad = median > MEDIAN_ALERT or ratio > OUTLIER_RATIO_ALERT
    if bad:
        print(f"  🚨 정상 대역 이탈 — 중앙 {median:.3f}(임계 {MEDIAN_ALERT}) · "
              f"이탈비율 {ratio*100:.1f}%(임계 {OUTLIER_RATIO_ALERT*100:.0f}%)")
        print("     → 네이버 파서/원천 또는 KIS 중 한쪽이 틀어졌다. 원본 응답부터 확인할 것.")
        return 1
    print("  ✅ 두 원천이 같은 말을 한다 (9/19 실측 정상 대역: 중앙 0.000 · 이탈 2.5%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
