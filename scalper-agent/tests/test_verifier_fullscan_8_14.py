# -*- coding: utf-8 -*-
"""test_verifier_fullscan_8_14.py — 채널 검증 표본→전수 전환 회귀 ([F-33]·[F-164])

배경(8/14 데이터 점검 중 확정):
  - `_verify_short_kis`/`_verify_credit_kis` 가 `_load_universe_codes(5)`(활성필터 없음)
    + `best = max(dates)` 였다 → **5종 중 1종만 신선해도 PASS**. 7/30 [F-33] 등재 후
    15일 미소진.
  - `_verify_investor_flow` 는 활성 표본 10종. 표본 n=10이 결손률 p를 잡을 확률은
    1-(1-p)^10 → p=2%면 18.3%. 우리가 실제로 겪은 사고(8/11 -6종·8/13 구멍 3종)가
    전부 그 대역이라, **큰 사고만 잡고 겪은 유형엔 눈 감는** 게이트였다.
  - 20:10 요약이 `⚠️`와 `✅ 핵심 채널 모두 정상`을 동시 출력 ([F-164]).

이 테스트가 지키는 것:
  1) 전수 판정이 소규모 결손을 **실제로 센다**
  2) ★음성대조 — 구코드(max/표본)가 **놓치던 케이스**를 신코드가 잡는다
  3) ★재수집 트리거 불변 — investor 소수 결손은 PASS 유지(RETRY_MAP 부작용 0)
  4) 결론 문구가 실상과 어긋나지 않는다(값 불변·문구만)

실행: python -X utf8 tests/test_verifier_fullscan_8_14.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import data.data_verifier as dv  # noqa: E402

PASS_N = 0
FAIL_N = 0


def check(label, cond, extra=""):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {label}")
    else:
        FAIL_N += 1
        print(f"  ❌ {label}  {extra}")


class Fixture:
    """임시 data_store 를 만들고 dv 의 경로 상수를 갈아끼운다."""

    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="verifier_fullscan_"))
        self.daily = self.root / "daily"
        self.flow = self.root / "flow"
        self.short = self.root / "short"
        self.credit = self.root / "credit"
        for d in (self.daily, self.flow, self.short, self.credit):
            d.mkdir(parents=True)
        self._saved = {}

    def install(self):
        self._saved = {
            "STORE_DIR": dv.STORE_DIR, "DAILY_DIR": dv.DAILY_DIR,
            "FLOW_DIR": dv.FLOW_DIR, "SHORT_DIR": dv.SHORT_DIR,
            "_all_universe_codes": dv._all_universe_codes,
        }
        dv.STORE_DIR, dv.DAILY_DIR = self.root, self.daily
        dv.FLOW_DIR, dv.SHORT_DIR = self.flow, self.short
        dv._all_universe_codes = lambda: list(self.codes)

    def restore(self):
        for k, v in self._saved.items():
            setattr(dv, k, v)
        shutil.rmtree(self.root, ignore_errors=True)

    def build(self, n_active, day, prev_days,
              inv_missing=0, short_missing=0,
              credit_day=None, credit_missing=0):
        """활성 n_active 종목을 만들고 지정 수만큼 채널 행을 뺀다."""
        self.codes = [f"{i:06d}" for i in range(1, n_active + 1)]
        rows_hist = "".join(f"{d},100,100,100,100,10,0.0\n" for d in prev_days)
        for i, c in enumerate(self.codes):
            (self.daily / f"{c}.csv").write_text(
                "날짜,시가,고가,저가,종가,거래량,등락률\n" + rows_hist
                + f"{day},100,100,100,100,10,0.0\n", encoding="utf-8")
            inv = "date,종가,외국인_수량\n" + "".join(f"{d},100,1\n" for d in prev_days)
            if i >= inv_missing:
                inv += f"{day},100,1\n"
            (self.flow / f"{c}_investor.csv").write_text(inv, encoding="utf-8")
            sh = "date,close,short_qty\n" + "".join(f"{d},100,1\n" for d in prev_days)
            if i >= short_missing:
                sh += f"{day},100,1\n"
            (self.short / f"{c}_short_bal.csv").write_text(sh, encoding="utf-8")
            if credit_day:
                cr = "date,close,credit_bal\n"
                if i >= credit_missing:
                    cr += f"{credit_day},100,1\n"
                else:
                    # 결손분은 credit_day 보다 **확실히 오래된** 날짜만 보유해야 한다
                    # (prev_days[0]을 쓰면 credit_day와 같은 날일 수 있어 결손이 0이 된다)
                    cr += "2026-07-30,100,1\n"
                (self.credit / f"{c}_credit_bal.csv").write_text(cr, encoding="utf-8")


DAY = "2026-08-14"
PREV = ["2026-08-11", "2026-08-12", "2026-08-13"]


def t1_fullscan_counts():
    print("\n[1] 전수 판정이 소규모 결손을 실제로 센다")
    f = Fixture()
    try:
        f.build(400, DAY, PREV, inv_missing=3)
        f.install()
        codes = dv._active_codes_all(DAY)
        check("활성 전수 = 400종 (표본 아님)", len(codes) == 400, f"got={len(codes)}")
        r = dv._verify_investor_flow(DAY)
        check("결손 3종을 missing 으로 계수", r.get("missing") == 3, f"got={r}")
        check("checked = 400 (활성 전수 분모)", r.get("checked") == 400, f"got={r}")
    finally:
        f.restore()


def t2_trigger_unchanged():
    print("\n[2] ★재수집 트리거 불변 — investor 소수 결손은 PASS 유지")
    f = Fixture()
    try:
        f.build(400, DAY, PREV, inv_missing=2)
        f.install()
        r = dv._verify_investor_flow(DAY)
        check("결손 2종 → PASS (RETRY_MAP 트리거 안 됨)", r["status"] == "PASS", f"got={r}")
        check("그래도 missing=2 로 보인다", r.get("missing") == 2, f"got={r}")
        check("PARTIAL/FAIL 아님 → 06:30 전체 재수집 유발 X",
              r["status"] not in ("PARTIAL", "FAIL"))
    finally:
        f.restore()
    f = Fixture()
    try:
        f.build(400, DAY, PREV, inv_missing=100)
        f.install()
        r = dv._verify_investor_flow(DAY)
        check("다수 결손(100종) → PARTIAL 승격(복구 경로 유지)",
              r["status"] == "PARTIAL", f"got={r}")
    finally:
        f.restore()


def t3_negative_short():
    print("\n[3] ★음성대조 — 구코드(max(dates)+5종표본)가 놓치던 케이스")
    f = Fixture()
    try:
        # 400종 중 399종이 오늘분 결손, 단 1종만 신선 = 사실상 수집 실패
        f.build(400, DAY, PREV, short_missing=399)
        f.install()

        # 구코드 재현: 랜덤 5종 표본 + max(dates)
        old_dates = []
        for c in f.codes[:5]:
            old_dates.append(dv._kis_last_date(f.short / f"{c}_short_bal.csv"))
        # 표본에 신선한 1종(마지막 코드)이 들어간 최악 시나리오를 결정적으로 구성
        worst = [dv._kis_last_date(f.short / f"{f.codes[-1]}_short_bal.csv")] + old_dates[:4]
        old_verdict = "PASS" if max([d for d in worst if d]) >= DAY else "FAIL"
        check("구코드: 1종만 신선해도 PASS (= 놓친다)", old_verdict == "PASS",
              f"got={old_verdict}")

        dv._evening = lambda _t: True     # 저녁 수집 후 조건 고정
        r = dv._verify_short_kis(DAY)
        check("신코드: 같은 상태를 FAIL 로 잡는다", r["status"] == "FAIL", f"got={r}")
        check("신코드가 결손 수를 명시(399)", r.get("missing") == 399, f"got={r}")
    finally:
        f.restore()


def t4_credit_coverage():
    print("\n[4] 신용 — 지연은 정상이나 커버리지 미달이면 PARTIAL")
    f = Fixture()
    try:
        f.build(400, DAY, PREV, credit_day="2026-08-11", credit_missing=40)  # 90%
        f.install()
        r = dv._verify_credit_kis(DAY)
        check("T+3 지연 자체는 통과(FAIL 아님)", r["status"] != "FAIL", f"got={r}")
        check("커버리지 90% → PARTIAL", r["status"] == "PARTIAL", f"got={r}")
        check("latest 는 최신 적재일", r.get("latest") == "2026-08-11", f"got={r}")
    finally:
        f.restore()
    f = Fixture()
    try:
        f.build(400, DAY, PREV, credit_day="2026-08-11", credit_missing=8)   # 98%
        f.install()
        r = dv._verify_credit_kis(DAY)
        check("커버리지 98%(실측 정상대역 96.8~97%) → PASS", r["status"] == "PASS", f"got={r}")
    finally:
        f.restore()
    f = Fixture()
    try:
        f.build(400, DAY, PREV, credit_day="2026-06-01", credit_missing=0)
        f.install()
        r = dv._verify_credit_kis(DAY)
        check("7일 초과 지연은 종전대로 FAIL(회귀)", r["status"] == "FAIL", f"got={r}")
    finally:
        f.restore()


def t5_summary_wording():
    print("\n[5] [F-164] 결론 문구가 실상과 어긋나지 않는다")
    import tools.notify_data_freshness as nf

    base = {
        "daily_ohlcv": {"status": "PASS", "ok": 10, "checked": 10},
        "investor_flow": {"status": "PASS", "ok": 2477, "checked": 2477, "missing": 0},
        "flow_market": {"status": "PASS", "latest": DAY},
        "short_kis": {"status": "PASS", "latest": DAY, "missing": 0},
        "credit_kis": {"status": "PASS", "latest": "2026-08-11", "missing": 0},
        "ticks": {"status": "PASS", "usable_pct": 100.0},
    }
    saved = nf._foreign_exh_status
    try:
        nf._foreign_exh_status = lambda _t: {"status": "PASS", "latest": DAY,
                                             "ok": 10, "checked": 10}
        s = nf.build_summary({"details": dict(base)}, DAY)
        check("전부 깨끗하면 '모두 정상' 유지(기존 문구 회귀)",
              "핵심 채널 모두 정상" in s, s.splitlines()[-2:])

        nf._foreign_exh_status = lambda _t: {"status": "PARTIAL", "latest": DAY,
                                             "ok": 7, "checked": 10}
        s2 = nf.build_summary({"details": dict(base)}, DAY)
        check("★부분수집이면 '모두' 라고 말하지 않는다",
              "핵심 채널 모두 정상" not in s2, s2.splitlines()[-2:])
        check("무엇이 부분인지 병기", "부분수집 7/10" in s2, s2.splitlines()[-2:])
        check("⚠️ 행과 결론이 같은 화면에서 모순되지 않음",
              ("⚠️" in s2) and ("모두 정상" not in s2))

        d3 = dict(base)
        d3["investor_flow"] = {"status": "PASS", "ok": 2475, "checked": 2477, "missing": 2}
        nf._foreign_exh_status = lambda _t: {"status": "PASS", "latest": DAY,
                                             "ok": 10, "checked": 10}
        s3 = nf.build_summary({"details": d3}, DAY)
        check("★소수 결손도 사장님 눈에 보인다(PASS인데 병기)",
              "수급 결손 2종" in s3, s3.splitlines()[-2:])

        # ★8/14 VPS 실측에서 잡힌 회귀 — 신용은 매일 80종 안팎 결손이 정상 대역이라
        # PASS인데 병기하면 매일 같은 경고가 뜬다([F-153] 마모를 새로 만드는 셈).
        d4 = dict(base)
        d4["credit_kis"] = {"status": "PASS", "latest": "2026-08-11",
                            "ok": 2395, "checked": 2477, "missing": 82}
        s4 = nf.build_summary({"details": d4}, DAY)
        check("★신용 정상대역 결손은 매일 띄우지 않는다(마모 방지)",
              "신용" not in s4.splitlines()[-3], s4.splitlines()[-3:])
        check("  그 경우 결론은 '모두 정상' 유지", "핵심 채널 모두 정상" in s4,
              s4.splitlines()[-3:])

        d5 = dict(base)
        d5["credit_kis"] = {"status": "PARTIAL", "latest": "2026-08-11",
                            "ok": 2100, "checked": 2477, "missing": 377}
        s5 = nf.build_summary({"details": d5}, DAY)
        check("반대로 정상대역 이탈(PARTIAL)이면 노출", "신용 커버리지 2100/2477" in s5,
              s5.splitlines()[-3:])
    finally:
        nf._foreign_exh_status = saved


def main():
    print("=" * 70)
    print("채널 검증 표본→전수 전환 회귀 ([F-33]·[F-164]) — 8/14")
    print("=" * 70)
    for fn in (t1_fullscan_counts, t2_trigger_unchanged, t3_negative_short,
               t4_credit_coverage, t5_summary_wording):
        fn()
    print("\n" + "=" * 70)
    print(f"결과: {PASS_N} PASS / {FAIL_N} FAIL")
    print("=" * 70)
    return 1 if FAIL_N else 0


if __name__ == "__main__":
    sys.exit(main())
