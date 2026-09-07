# -*- coding: utf-8 -*-
"""test_picks_levels_f188_f189_9_7.py — [F-188] 청산 룰 병기 + [F-189] 트랙 C SL −15% 제거 + RULE-008 사각 확장

배경(9/7 오후 · 사장님 "순차적으로 작업 진행하자" ①):
  [F-188] picks 표시의 목표가(+5/+8%, 트랙 C +5/+10%)가 사장님 룰(트레일링 −3% only·고정 TP 없음)과
    문구 모순. 결정 = JSON tp1/tp2 키 유지(웹 스키마)·문구 "참고목표"·헤더 1줄 EXIT_RULE_NOTE(SAJANG 파생)
    ·JSON `exit_rule` additive 필드.
  [F-189] 트랙 C(상한가 후속)의 `sl = int(pullback_target * 0.85)` = **−15%** 리터럴이 두 곳.
    RULE-008 정규식이 `0.9x`만 잡아 `0.85`는 사각 → 정규식 `0.[89]x` 확장 + `price_levels_limit_up()`.

이 테스트가 지키는 것:
  1) price_levels_limit_up: sl == SAJANG.get_normal_sl(눌림목가) · ≠ 구 −15% · 밴드/목표는 종전 값
  2) ★소스 대조 — daytrading_picks.py에 구 리터럴(×0.85·×0.965) 없음
  3) EXIT_RULE_NOTE는 SAJANG 값에서 파생(TRAILING_PCT 문자열 포함·리터럴 아님)
  4) format_flowx_post 출력에 EXIT_RULE_NOTE 1회 + "참고목표"(A/C 두 트랙) + 구 "· 목표 " 문구 없음
  5) RULE-008 정규식(정본 로드): `sl = int(x * 0.85)` 매칭 / `SAJANG.get_normal_sl` 비매칭 /
     `entry_low = int(close * 0.985)` 비매칭 · ★음성대조: 구 정규식 `0\\.9[0-9]`는 0.85를 놓친다

실행: python -X utf8 tests/test_picks_levels_f188_f189_9_7.py
"""
import importlib.util
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent            # scalper-agent/
REPO_DIR = BASE_DIR.parent
for p in (str(BASE_DIR), str(BASE_DIR / "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)

from tools import daytrading_picks as dp          # noqa: E402
from data.sajang_rules import SAJANG              # noqa: E402

PASS_N = 0
FAIL_N = 0
_OLD_C_LITERAL = "0." + "85"
_OLD_AB_LITERAL = "0." + "965"


def _check(name, cond):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {name}")
    else:
        FAIL_N += 1
        print(f"  ❌ {name}")


def test_limit_up_levels():
    for close in (100000, 12345, 2570):
        lv = dp.price_levels_limit_up(close)
        pb = int(close * 0.95)
        _check(f"pullback_target({close}) == ×0.95", lv["pullback_target"] == pb)
        _check(f"sl({close}) == SAJANG.get_normal_sl(pullback)", lv["sl"] == SAJANG.get_normal_sl(pb))
        _check(f"sl({close}) != 구 −15%", lv["sl"] != int(pb * float(_OLD_C_LITERAL)))
        _check(f"밴드/목표 종전 동일({close})",
               (lv["entry_low"], lv["entry_high"], lv["tp1"], lv["tp2"])
               == (int(close * 0.93), int(close * 0.97), int(pb * 1.05), int(pb * 1.10)))
    _check("sl(100000) == 92150 (눌림 95,000의 −3%)", dp.price_levels_limit_up(100000)["sl"] == 92150)
    _check("close=0 → 전부 0", all(v == 0 for v in dp.price_levels_limit_up(0).values()))


def test_no_literals_in_source():
    src = Path(dp.__file__).read_text("utf-8")
    _check("소스에 구 −15% 리터럴 없음", _OLD_C_LITERAL not in src)
    _check("소스에 구 −3.5% 리터럴 없음", _OLD_AB_LITERAL not in src)


def test_exit_rule_note_from_sajang():
    note = dp.EXIT_RULE_NOTE
    _check("EXIT_RULE_NOTE에 TRAILING_PCT 값 포함", f"−{SAJANG.TRAILING_PCT:g}%" in note)
    _check("EXIT_RULE_NOTE에 '고정 TP 없음'", "고정 TP 없음" in note)
    _check("FIXED_TP_DISABLED 표기 일치", ("FIXED_TP_DISABLED" in note) == bool(SAJANG.FIXED_TP_DISABLED))


def _dummy_pick(track: str, close: int) -> dict:
    lv = dp.price_levels_limit_up(close) if track.startswith("C_") else dp.price_levels(close)
    base = lv.get("pullback_target", close)
    return {
        "code": "000000", "name": "테스트", "sector": "-", "close_end": close, "mcap_억": 1000,
        "track": track, "final_score": 70, "score": 60, "sector_bonus": 0, "mcap_bonus": 0,
        "ewy_bonus": 0, "wick_pen": 0, "key_reasons": "x",
        "entry_low": lv["entry_low"], "entry_high": lv["entry_high"], "tp1": lv["tp1"], "tp2": lv["tp2"],
        "sl": lv["sl"], "upside_to_tp1_pct": round((lv["tp1"] / base - 1) * 100, 1),
        "etf_alt_code": "", "etf_alt_name": "", "etf_alt_theme": "",
    }


def test_flowx_text():
    picks = [_dummy_pick("A_large", 100000), _dummy_pick("C_상한가후속", 50000)]
    for mode in ("confirmed", "preview"):
        txt = dp.format_flowx_post(picks, {}, mode=mode)
        _check(f"[{mode}] EXIT_RULE_NOTE 정확히 1회", txt.count(dp.EXIT_RULE_NOTE) == 1)
        _check(f"[{mode}] '참고목표' 2회(A·C)", txt.count("참고목표") == 2)
        _check(f"[{mode}] 구 '· 목표 ' 문구 없음", "· 목표 " not in txt)
        _check(f"[{mode}] 헤더 구분선 앞에 위치", txt.index(dp.EXIT_RULE_NOTE) < txt.index("━━━━━━━━"))
    _check("format_telegram_message == flowx 위임", dp.format_telegram_message(picks, {}) == dp.format_flowx_post(picks, {}))


def test_both_save_paths_carry_exit_rule():
    """★9/7 Tier1 MED — picks JSON을 쓰는 경로가 둘이다(CLI `--save` / cron `_job_daytrading_picks`).

    한쪽만 넣으면 운영 JSON엔 없는데 장부는 "했다"가 된다 = [F-110] 모양.
    두 경로의 out 딕셔너리 키 집합을 소스에서 직접 읽어 `exit_rule` 존재를 고정한다.
    """
    def out_keys(src_path: Path, indent: str) -> set:
        src = src_path.read_text("utf-8")
        anchor = 'out = {\n' + indent + '"updated"'
        i = src.index(anchor)
        return set(re.findall(r'"(\w+)":', src[i:src.index("}", i)]))

    coo_path = BASE_DIR / "bot" / "trading_coo.py"
    cli = out_keys(Path(dp.__file__), " " * 12)
    coo = out_keys(coo_path, " " * 16)
    _check("CLI --save out에 exit_rule", "exit_rule" in cli)
    _check("cron _job_daytrading_picks out에 exit_rule", "exit_rule" in coo)
    for k in ("updated", "mode", "ewy_signal", "market_regime"):
        _check(f"두 경로 공통 키 유지: {k}", k in cli and k in coo)
    _check("cron이 EXIT_RULE_NOTE를 import", "EXIT_RULE_NOTE" in coo_path.read_text("utf-8"))


def _load_rule_008():
    spec = importlib.util.spec_from_file_location("_pcc", REPO_DIR / "tools" / "pre_commit_check.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    rules = getattr(mod, "RULES", None) or getattr(mod, "PATTERNS", None)
    if rules is None:
        # 이름을 모르면 모듈 안의 list-of-dict 중 id 키를 가진 것을 찾는다
        for v in vars(mod).values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and "id" in v[0]:
                rules = v
                break
    return next(r for r in rules if r.get("id") == "RULE-008")


def test_rule_008_regex():
    rule = _load_rule_008()
    pat = re.compile(rule["pattern"])
    _check("RULE-008: `sl = int(pb * 0.85)` 매칭", pat.search("        sl = int(pullback_target * 0.85)   # -15%") is not None)
    _check("RULE-008: `sl = int(close * 0.965)` 매칭(종전 유지)", pat.search("        sl = int(close * 0.965)") is not None)
    _check("RULE-008: `stop_loss = int(p * 0.88)` 매칭", pat.search("stop_loss = int(p * 0.88)") is not None)
    _check("RULE-008: SAJANG 헬퍼 비매칭", pat.search('        "sl": SAJANG.get_normal_sl(close),') is None)
    _check("RULE-008: entry_low ×0.985 비매칭(이름이 sl로 안 끝남)", pat.search("        entry_low = int(close * 0.985)") is None)
    _check("RULE-008: tp1 ×1.05 비매칭", pat.search("        tp1 = int(pullback_target * 1.05)") is None)
    old = re.compile(r'(?:stop_loss|[a-zA-Z_]*sl)\s*=\s*.*int\(.*\*\s*0\.9[0-9]')
    _check("★음성대조: 구 정규식은 0.85를 놓친다", old.search("        sl = int(pullback_target * 0.85)") is None)


if __name__ == "__main__":
    for fn in (test_limit_up_levels, test_no_literals_in_source, test_exit_rule_note_from_sajang,
               test_flowx_text, test_both_save_paths_carry_exit_rule, test_rule_008_regex):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
