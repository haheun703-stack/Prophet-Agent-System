# -*- coding: utf-8 -*-
"""test_nationality_retired_9_7.py — [F-198] 국적별 수급 정식 폐기 (배선 제거·코드 보존)

배경(9/7 사장님 "폐기해라"):
  퀀트봇 §3이 3안(㈎KRX재개 / ㈏정식폐기 / ㈐현상유지)을 올렸고 단타봇·퀀트봇 의견이 ㈏로
  일치했으며 사장님이 폐기를 결정하셨다. 확인된 사실:
    - 세 경로 전부 차단: 단타봇 불가(사장님 6/22 룰) · 정보봇 파일은 국적 아님([F-196])
      · 퀀트봇 수집 6/22 cron 비활성
    - `krx_gate.krx_enabled()`가 default False라 크롤러가 **세션 획득 전에 return None**
      → KRX 실호출 0건(절대룰 준수). 그래서 nightly ⑧이 1~3초에 "0건"으로 끝난다.
    - 그럼에도 ⑧ 스텝은 매일 돌며 "⚠ 국적별 스냅샷 0건!" 경고를 냈고 nightly는 ✅로 표시
      → 3개월간 "되살아나지 않을 것을 기다리는 상태" + [F-153] 경보 마모

폐기 범위 = **배선만** ([S-6]·[S-2] 전례):
  제거 — nightly ⑧ 스텝 · C25 잡 등록 · AUTO-RECOVERY `nationality_xray` 감시 항목
  보존 — 함수·모듈·CSV 전부. `nationality_profiler`/`nationality_signal`은
         morning_recommendation·auto_trader·telegram_bot·position_guardian·nightwatch가
         import하므로 삭제 시 동반 사망한다(게이트가 빈 값을 주므로 존치가 안전).

이 테스트가 지키는 것:
  1) nightly STEPS에 "국적별" 스텝이 없다 · 다른 스텝은 그대로
  2) C25 잡 등록이 없다 / AUTO-RECOVERY 감시 항목이 없다
  3) ★코드 보존 — 세 함수와 모듈이 전부 살아 있다(재개 가능)
  4) ★동반 사망 방지 — 소비 5모듈의 import 대상이 실재한다
  5) ★KRX 절대룰 — 크롤러가 게이트를 통과하지 못하면 세션 없이 None을 돌려준다(실호출 0)
  6) ★음성대조 — 폐기 전 상태(스텝·잡·감시 항목 존재)를 문자열로 재현하면 검출된다

실행: python -X utf8 tests/test_nationality_retired_9_7.py
"""
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

PASS_N = 0
FAIL_N = 0

COO = (BASE_DIR / "bot" / "trading_coo.py").read_text("utf-8")
NIGHTLY = (BASE_DIR / "tools" / "run_nightly_pipeline.py").read_text("utf-8")


def _check(name, cond):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {name}")
    else:
        FAIL_N += 1
        print(f"  ❌ {name}")


def _step_names(src: str) -> list:
    """nightly STEPS의 표시 이름만 추출 — 주석 안의 문자열은 세지 않는다."""
    names = []
    for line in src.splitlines():
        st = line.strip()
        if st.startswith("#"):
            continue
        m = re.match(r'\("([^"]+)"\s*,\s*$', st)
        if m:
            names.append(m.group(1))
    return names


def test_nightly_step_removed():
    names = _step_names(NIGHTLY)
    _check("nightly STEPS에 '국적별' 없음", not any("국적별" in n for n in names))
    _check("step3_nationality 호출 없음", "collect_all.step3_nationality()" not in
           "\n".join(l for l in NIGHTLY.splitlines() if not l.strip().startswith("#")))
    # 인접 스텝은 살아 있어야 한다(과다 제거 방지)
    for keep in ("⑦ 11주체", "⑨ F1 forward+preflight", "⑥ 수급 4종"):
        _check(f"인접 스텝 보존: {keep}", keep in names)
    _check("스텝 수 표기 갱신(26)", "**26**" in NIGHTLY)


def test_coo_wiring_removed():
    body = "\n".join(l for l in COO.splitlines() if not l.strip().startswith("#"))
    _check("C25 잡 등록 없음", "C25_nationality_xray" not in body)
    _check("stage3에 _job_nationality_xray_upload 등록 없음",
           "self._job_nationality_xray_upload(context)" not in body)
    _check("AUTO-RECOVERY 감시 항목 없음", '"name": "nationality_xray"' not in body)
    _check("AUTO-RECOVERY recover 등록 없음", "self._recover_nationality_flows," not in body)
    # 다른 감시 항목은 남아야 한다(과다 제거 방지)
    _check("감시 항목 보존: investor_flow", '"name": "investor_flow"' in body)


def test_no_automatic_call_path():
    """★자동 실행 경로 0건 — 배선 제거의 실질 판정.

    C25·AUTO-RECOVERY를 뺀 뒤에도 `run_flowx_upload()`가 **세 번째 호출구**로 남아 있었다
    (← `morning_recommendation.py:3214·4642`, 실매매 추천 파이프라인이 매일 호출).
    [S-6]의 "한쪽만 빼면 재가동 시 되살아난다"를 스스로 저지를 뻔한 자리라 함께 제거했다.
    `__main__` 수동 블록만 남긴다(cron 직접 호출은 crontab 실측 0건).
    """
    src = (BASE_DIR / "data" / "upload_short.py").read_text("utf-8")
    lines = src.splitlines()
    main_at = next(i for i, l in enumerate(lines) if l.startswith("if __name__"))
    auto = [i + 1 for i, l in enumerate(lines[:main_at])
            if "upload_nationality_flows()" in l and not l.strip().startswith("#")
            and not l.lstrip().startswith("def ")]      # 함수 '정의' 줄은 호출이 아니다
    _check(f"__main__ 이전 자동 호출 0건 (실측 {auto})", not auto)
    manual = [i + 1 for i, l in enumerate(lines[main_at:], start=main_at)
              if "upload_nationality_flows()" in l and not l.strip().startswith("#")]
    _check("__main__ 수동 경로 1곳 존치(재현용)", len(manual) == 1)
    # trading_coo는 두 함수를 보존한 채 '등록'만 뺐으므로 그 안의 import는 남는 게 정상
    _check("trading_coo 보존 함수 안의 import 2곳 존치",
           COO.count("from data.upload_short import upload_nationality_flows") == 2)


def test_code_preserved():
    """폐기는 배선 제거다 — 되돌릴 수 있어야 한다."""
    _check("_job_nationality_xray_upload 존치", "async def _job_nationality_xray_upload" in COO)
    _check("_recover_nationality_flows 존치", "async def _recover_nationality_flows" in COO)
    _check("collect_all.step3_nationality 존치",
           "def step3_nationality" in (BASE_DIR / "collect_all.py").read_text("utf-8"))
    from data.upload_short import upload_nationality_flows, _nationality_source_age  # noqa: F401
    _check("upload_nationality_flows import 가능", callable(upload_nationality_flows))
    for mod in ("nationality_signal", "nationality_profiler", "nationality_pictogram",
                "krx_nationality_crawler"):
        _check(f"모듈 파일 존치: {mod}.py", (BASE_DIR / "data" / f"{mod}.py").exists())


def test_consumers_not_broken():
    """소비 모듈이 import하는 심볼이 실재하는가 — 동반 사망 방지([S-6] 교훈)."""
    sig = (BASE_DIR / "data" / "nationality_signal.py").read_text("utf-8")
    prof = (BASE_DIR / "data" / "nationality_profiler.py").read_text("utf-8")
    for sym in ("compare_nationality", "_find_prev_trading_day", "_get_latest_data_date",
                "score_nationality_batch", "collect_daily_snapshots",
                "generate_nationality_report", "save_daily_snapshot", "score_nationality"):
        _check(f"nationality_signal.{sym} 실재", f"def {sym}" in sig)
    for sym in ("calc_nationality_power", "collect_daily_series", "predict_tomorrow_flow",
                "analyze_nationality_behavior"):
        _check(f"nationality_profiler.{sym} 실재", f"def {sym}" in prof)


def test_krx_absolute_rule():
    """★사장님 절대룰 — 폐기 후에도 KRX 실호출 경로가 게이트 뒤에 있다."""
    from data.krx_gate import krx_enabled
    _check("krx_enabled() default False(차단)", krx_enabled() is False)
    crawler = (BASE_DIR / "data" / "krx_nationality_crawler.py").read_text("utf-8")
    # 게이트 검사 뒤에 곧바로 return None 이 오는 블록이 2곳(동기·비동기)
    _check("크롤러에 게이트 차단 2곳", crawler.count("if not krx_enabled():") == 2)
    _check("차단 시 세션 없이 None 반환",
           crawler.count("logger.warning(\"[krx_gate] %s\", krx_block_reason())") == 2)


def test_negative_control():
    """★음성대조 — 폐기 전 형태를 재현하면 위 검사들이 잡는다."""
    old_nightly = NIGHTLY + '\n        ("⑧ 국적별",\n'
    _check("음성대조: 스텝을 되돌리면 검출", any("국적별" in n for n in _step_names(old_nightly)))
    old_coo = COO + '\n            "C25_nationality_xray",\n'
    body = "\n".join(l for l in old_coo.splitlines() if not l.strip().startswith("#"))
    _check("음성대조: C25를 되돌리면 검출", "C25_nationality_xray" in body)


if __name__ == "__main__":
    for fn in (test_nightly_step_removed, test_coo_wiring_removed, test_no_automatic_call_path,
               test_code_preserved, test_consumers_not_broken, test_krx_absolute_rule,
               test_negative_control):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
