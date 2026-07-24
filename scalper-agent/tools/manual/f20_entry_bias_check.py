# -*- coding: utf-8 -*-
"""[F-20] 진입 낙관편향 대조 — signal_time 기준 vs detected_at 기준 (7/24).

★★★ 경고 — 이 도구의 출력을 판정에 쓰지 말 것 (7/24 실행 후 확인된 한계) ★★★
  실행 결과 -20.75%p → -36.80%p(-16.05%p)가 나왔으나 **대부분이 아티팩트**다.
  ticks 해상도가 중앙값 20.4분인데 신호→인지 지연은 중앙값 2.7분이라, detected_at
  다음 관측행으로 옮기면 항상 '칸 1개'(=약 20분)를 통째로 미는 결과가 된다.
  실측 예(7/22 017670): 신호 09:01:22 → 기존진입 09:21:35 → 인지 09:25:02 →
  보정진입 09:41:54. 실제로 살 수 있던 09:25 가격은 09:21도 09:41도 아니다.
  즉 기존 장부는 '인지 3분 前' 가격(약간 낙관), 이 보정은 '인지 18분 後' 가격(과도한 비관).
  → 편향의 **존재**는 확증(30건 중 29건)했지만 **크기는 현 데이터로 측정 불가**다.
  근본 처방은 측정 도구 수정이 아니라, OBSERVE 러너가 intent 생성 시점의 실시간
  가격을 detected_price로 함께 기록하는 것(추정→실측). 사장님 결정 대기.
  이 파일은 같은 착시를 반복하지 않기 위한 기록으로 보존한다(7/7 해상도 착시 재현 사례).


문제(7/23 Tier1 M-1에서 등재):
  ⑲-3 페이퍼는 진입을 `signal_time 다음 관측행`으로 잡는다. 그런데 봇(러너)은 5분
  폴링이라 신호를 `detected_at`에야 인지한다. 즉 장부는 **봇이 아직 모르던 시각의
  가격**으로 체결하고 있다. 판정 표본 25건 중 24건이 entry_time < detected_at였다.
  게다가 상한 뷰는 '가장 이른 5건'을 취하므로, 인지 지연이 큰 체결만 골라 담는다.

이 도구가 하는 일:
  같은 체결 건에 대해 진입행만 `detected_at 다음 관측행`으로 바꿔 재계산하고,
  상한 CAP_VIEW_N건 기준 순누적을 비교한다. 편향의 부호와 크기를 정량화한다.

★한계(정직 표기): 진입 시각이 바뀌면 CB 발동 시각과 체결 순서도 달라질 수 있는데,
  이 대조는 **장부에 이미 남은 체결 건**만 재계산한다(CB로 잘린 건·no_fill은 미재현).
  따라서 결과는 "편향의 방향과 대략적 크기"이지 완전한 재정산이 아니다.
  완전 재정산이 필요하면 원본 intents로 settle_day 전체를 다시 돌려야 한다.

read-only: 장부·ticks 무접촉. 출력만 한다.
실행: python tools/manual/f20_entry_bias_check.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent
for p in (str(BASE), str(BASE / "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001
    pass

import playbook_shadow as pb          # noqa: E402
from observe_v2_paper import _load_ledger, CAP_VIEW_N, TICKS_BAD  # noqa: E402


def _requote(day, code, after_t):
    """after_t 다음 관측행으로 진입해 청산까지 재시뮬 → (entry_time, ret) 또는 None."""
    rows = pb._read_ticks(day, code)
    if not rows:
        return None
    ei = next((i for i, r in enumerate(rows) if r[0] > after_t), None)
    if ei is None:
        return None
    entry = rows[ei][1]
    ret, why, exit_t = pb._simulate_exit(rows, ei, entry)
    if ret is None:
        return None
    return {"entry_time": rows[ei][0], "entry_price": entry,
            "ret": round(ret, 2), "net": round(ret - pb.COST_RT, 2),
            "why": why, "exit_time": exit_t}


def main():
    led = _load_ledger()
    days = led.get("days", {})
    print(f"[F-20] 진입 낙관편향 대조 — 상한 {CAP_VIEW_N}건/일 · 유효일(ticks OK)만")
    print("=" * 78)
    hdr = f"{'일자':<10}{'건':>3}{'지연중앙':>9}  {'기존net':>9}{'보정net':>9}{'차이':>9}"
    print(hdr)
    print("-" * 78)

    tot = {"n": 0, "old": 0.0, "new": 0.0, "old_w": 0, "new_w": 0,
           "lost": 0, "early": 0, "delays": []}
    for day in sorted(days):
        d = days[day]
        verdict = ((d.get("summary") or {}).get("ticks_health") or {}).get("verdict")
        if verdict != "OK":
            continue                      # 수집장애일 제외 — 판정 규약과 동일
        cap = (d.get("trades") or [])[:CAP_VIEW_N]
        if not cap:
            continue
        o_sum = n_sum = 0.0
        o_w = n_w = 0
        cnt = lost = 0
        delays = []
        for t in cap:
            det = t.get("detected_at")
            o_net = t.get("net") or 0.0
            o_sum += o_net
            o_w += 1 if o_net > 0 else 0
            cnt += 1
            if not det:
                n_sum += o_net           # detected_at 미기입 → 보정 불가·기존값 유지
                n_w += 1 if o_net > 0 else 0
                continue
            # detected_at은 'HH:MM:SS' 또는 'YYYY-MM-DD HH:MM:SS' 둘 다 올 수 있다
            det_t = det.split(" ")[-1]
            if t.get("entry_time") and t["entry_time"] < det_t:
                tot["early"] += 1
                delays.append(_secs(det_t) - _secs(t["entry_time"]))
            r = _requote(day, t.get("code") or "", det_t)
            if r is None:
                lost += 1                # 인지 시각 이후 관측행 없음 = 실제론 체결 불가
                continue
            n_sum += r["net"]
            n_w += 1 if r["net"] > 0 else 0
        med = _median(delays)
        print(f"{day:<10}{cnt:>3}{(f'{med//60:.0f}분' if med else '-'):>9}  "
              f"{o_sum:>+9.2f}{n_sum:>+9.2f}{n_sum - o_sum:>+9.2f}"
              + (f"   (체결불가 {lost})" if lost else ""))
        tot["n"] += cnt
        tot["old"] += o_sum
        tot["new"] += n_sum
        tot["old_w"] += o_w
        tot["new_w"] += n_w
        tot["lost"] += lost
        tot["delays"] += delays

    print("-" * 78)
    n = tot["n"]
    print(f"{'합계':<10}{n:>3}{'':>9}  {tot['old']:>+9.2f}{tot['new']:>+9.2f}"
          f"{tot['new'] - tot['old']:>+9.2f}")
    if n:
        print(f"\n승률   기존 {100*tot['old_w']/n:.1f}%  →  보정 {100*tot['new_w']/n:.1f}%")
        print(f"건당   기존 {tot['old']/n:+.3f}%p  →  보정 {tot['new']/n:+.3f}%p")
    med = _median(tot["delays"])
    print(f"\n신호→인지 지연: {tot['early']}/{n}건이 인지 前 체결 · 중앙값 "
          f"{med/60:.1f}분" if med else "\n지연 표본 없음")
    if tot["lost"]:
        print(f"★ 보정 시 체결 불가 {tot['lost']}건 — 인지 시각 이후 관측행이 없어 "
              f"실제로는 못 샀을 건(기존 장부엔 수익/손실로 계상됨)")
    print("\n※ 한계: 장부에 남은 체결만 재계산(CB로 잘린 건·no_fill 미재현). "
          "부호와 크기 파악용.")
    return 0


def _secs(hms):
    try:
        p = [int(x) for x in hms.split(":")]
        return p[0] * 3600 + p[1] * 60 + (p[2] if len(p) > 2 else 0)
    except Exception:  # noqa: BLE001
        return 0


def _median(xs):
    if not xs:
        return 0
    s = sorted(xs)
    m = len(s) // 2
    return s[m] if len(s) % 2 else (s[m - 1] + s[m]) / 2


if __name__ == "__main__":
    sys.exit(main())
