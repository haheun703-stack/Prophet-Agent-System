# -*- coding: utf-8 -*-
"""test_nationality_stale_guard_9_7.py — [F-192] nationality_flows 재탕 정지 가드

배경(9/7 운영자 지시서 260907 §2 · 퀀트봇 소유 확인 회신):
  6/22 KRX 접근 차단 이후 `data_store/nationality/` 원천 CSV가 **6/19에 멈춰 있는데**
  `upload_nationality_flows()`는 매일 돌아 `date=오늘`로 같은 스냅샷을 다시 적재해 왔다.
  정보봇 실측 = 매일 1,911행 · 누적 273,698행(Supabase 최대 표) · `countries.date_new`는 6/19 고정.
  **잡은 성공하고 행수도 일정해 로그로는 보이지 않는다** — 퀀트봇 8/11 B-61과 같은 모양.

  ★사장님 절대룰(KRX·nationality 무접촉)과 충돌하지 않는다 — 이 가드는 **수집이 아니라 발행을
    멈춘다.** 오히려 "단타봇 KRX 영구 OFF"를 실제로 이행하는 쪽이다. 수집 재개는 하지 않는다.

이 테스트가 지키는 것:
  1) `_nationality_source_age` — 경계(임계일 정확히 = 통과 / +1일 = stale)·판독불가는 stale
  2) 원천이 낡으면 **upsert 호출 0회** · 반환 False
  3) 원천이 신선하면 종전대로 업로드(회귀 — 가드가 정상 경로를 막지 않는다)
  4) 스킵 시 마커에 `skipped`·`reason` 기록(성공 위장 차단) + `date`는 오늘
     (마커를 안 남기면 AUTO-RECOVERY가 매 사이클 재시도 = 무한 반복)
  5) ★AUTO-RECOVERY가 **같은 함수**를 호출한다 = 복구 경로가 가드를 우회하지 않는다(지시서 §2-3)
  6) ★음성대조 — 가드가 없던 구현(임계 무한대)은 6/19 원천을 그대로 업로드한다

실행: python -X utf8 tests/test_nationality_stale_guard_9_7.py
"""
import json
import sys
import tempfile
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from data import upload_short as us            # noqa: E402

PASS_N = 0
FAIL_N = 0


def _check(name, cond):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {name}")
    else:
        FAIL_N += 1
        print(f"  ❌ {name}")


class _FakeTable:
    def __init__(self, sink):
        self.sink = sink

    def upsert(self, batch, on_conflict=None):
        self.sink.append(list(batch))
        return self

    def execute(self):
        return {"data": []}


class _FakeClient:
    def __init__(self):
        self.upserts = []

    def table(self, _name):
        return _FakeTable(self.upserts)


def test_age_predicate():
    f = us._nationality_source_age
    ref = date(2026, 9, 7)
    _check("6/19 원천 → stale(80일)", f([("000020", "20260619")], ref) == (True, "20260619", 80))
    _check("9/4 원천 → 신선(3일)", f([("000020", "20260904")], ref)[0] is False)
    # 경계: 임계일(4일) 정확히 = 통과 / 5일 = stale
    _check("경계 4일(9/3) → 통과", f([("x", "20260903")], ref) == (False, "20260903", 4))
    _check("경계 5일(9/2) → stale", f([("x", "20260902")], ref) == (True, "20260902", 5))
    _check("빈 목록 → stale(모르면 발행 안 함)", f([], ref) == (True, None, None))
    _check("형식 오류 → stale", f([("x", "abc")], ref) == (True, None, None))
    _check("여러 종목이면 최신일 채택", f([("a", "20260619"), ("b", "20260904")], ref)[1] == "20260904")


def _run_upload(tmp: Path, code_dates, monkey_today=None):
    """가드 경로만 태운다 — 클라이언트·원천 목록·마커 경로를 대체."""
    fake = _FakeClient()
    marker = BASE_DIR / "data_store" / "nationality" / "_last_upload.json"
    before = marker.read_text("utf-8") if marker.exists() else None
    orig = (us._get_client, us._find_all_nationality_codes, us.PREDICTION_PATH)
    try:
        us._get_client = lambda: fake
        us._find_all_nationality_codes = lambda: code_dates
        us.PREDICTION_PATH = tmp / "nope.json"          # prediction 없음 → CSV 경로만
        ok = us.upload_nationality_flows()
        written = json.loads(marker.read_text("utf-8")) if marker.exists() else None
    finally:
        (us._get_client, us._find_all_nationality_codes, us.PREDICTION_PATH) = orig
        if before is not None:
            marker.write_text(before, encoding="utf-8")
    return ok, fake, written


def test_stale_source_skips_upload():
    with tempfile.TemporaryDirectory() as td:
        ok, fake, written = _run_upload(Path(td), [("000020", "20260619")])
        _check("stale 원천 → 반환 False", ok is False)
        _check("stale 원천 → upsert 호출 0회", len(fake.upserts) == 0)
        _check("stale 원천 → 마커에 skipped 기록", (written or {}).get("skipped") is True)


def test_fresh_source_not_blocked():
    """★회귀 — 가드가 정상 경로를 막지 않는다.

    원천 날짜 라벨만 오늘로 바꾸면 가드를 통과해 **끝까지 업로드된다**(upsert 호출 ≥1).
    ★첫 판은 "픽스처에 CSV가 없으니 데이터 없음으로 끝난다"고 단언했다가 실패했다 —
      `_build_countries_detail`은 실제 `data_store/nationality/` CSV를 읽으므로 행이 만들어진다.
      테스트의 가정이 틀렸던 것이고, 실제 동작(가드 통과 → 업로드)이 더 강한 회귀 증거다.
    """
    fresh = date.today().strftime("%Y%m%d")
    with tempfile.TemporaryDirectory() as td:
        ok, fake, written = _run_upload(Path(td), [("000020", fresh)])
        _check("신선 원천 → 가드 미발동(스킵 마커 아님)",
               not (written or {}).get("skipped"))
        _check("신선 원천 → 업로드 정상 진행(upsert 호출됨)", len(fake.upserts) >= 1)
        _check("신선 원천 → 반환 True", ok is True)


def test_marker_records_skip():
    marker = BASE_DIR / "data_store" / "nationality" / "_last_upload.json"
    before = marker.read_text("utf-8") if marker.exists() else None
    try:
        us._write_nationality_marker(0, 0, 0, skipped=True, reason="source_stale:20260619:80d")
        m = json.loads(marker.read_text("utf-8"))
        _check("마커 skipped=True", m.get("skipped") is True)
        _check("마커 reason 기록", "source_stale" in str(m.get("reason", "")))
        _check("마커 count=0", m.get("count") == 0)
        _check("마커 date=오늘(AUTO-RECOVERY 무한재시도 차단)", m.get("date") == str(date.today()))
        # 성공 경로는 skipped 키가 없어야 한다(구분 가능)
        us._write_nationality_marker(1911, 8, 1903)
        m2 = json.loads(marker.read_text("utf-8"))
        _check("성공 마커엔 skipped 없음", "skipped" not in m2 and m2.get("count") == 1911)
    finally:
        if before is not None:
            marker.write_text(before, encoding="utf-8")


def test_recovery_uses_same_function():
    """복구 경로가 가드를 우회하지 않는다 — 같은 함수를 호출하는지 소스로 확인."""
    src = (BASE_DIR / "bot" / "trading_coo.py").read_text("utf-8")
    i = src.index("async def _recover_nationality_flows")
    body = src[i:i + 600]
    _check("_recover_nationality_flows가 upload_nationality_flows 호출",
           "upload_nationality_flows" in body)
    _check("복구 경로에 별도 upsert 없음(우회 통로 0)",
           "nationality_flows\").upsert" not in body and ".upsert(" not in body)
    _check("C25 잡도 같은 함수 사용", "from data.upload_short import upload_nationality_flows" in src)


def test_negative_control_old_behavior():
    """★음성대조 — 가드가 없던 동작(임계 무한대)이면 6/19 원천도 통과한다."""
    orig = us.NATIONALITY_STALE_MAX_DAYS
    try:
        us.NATIONALITY_STALE_MAX_DAYS = 10 ** 6          # 사실상 가드 없음 = 구 동작
        stale, latest, _age = us._nationality_source_age([("000020", "20260619")], date(2026, 9, 7))
        _check("음성대조: 가드 없으면 6/19도 stale 아님", stale is False and latest == "20260619")
    finally:
        us.NATIONALITY_STALE_MAX_DAYS = orig
    stale2, _l, _a = us._nationality_source_age([("000020", "20260619")], date(2026, 9, 7))
    _check("임계 복원 후 다시 stale", stale2 is True)


if __name__ == "__main__":
    for fn in (test_age_predicate, test_stale_source_skips_upload, test_fresh_source_not_blocked,
               test_marker_records_skip, test_recovery_uses_same_function,
               test_negative_control_old_behavior):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
