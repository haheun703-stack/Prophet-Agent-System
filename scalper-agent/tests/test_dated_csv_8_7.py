# -*- coding: utf-8 -*-
"""[F-82] 날짜 CSV 판독 정본 검증 (8/7).

배경: 저장 표기가 채널마다 다른데(`YYYY-MM-DD` / `YYYYMMDD`+BOM) 판독기가 3벌이었고
**서로 반대 방향으로** 정규화했다 — `_kis_last_date`=ISO / `verify_channels.norm_date`
=compact / `_get_last_csv_date`=무변환. 같은 사고가 8/1·8/5·8/7 세 번 났다.

이 테스트가 지키는 것:
  ① 표기가 달라도 같은 날짜면 같은 날짜로 읽힌다 (8/7 사고의 직접 처방)
  ② `compact()`는 compact를 유지한다 — `ticks/20260807` **경로**가 이걸로 만들어진다
  ③ 기존 계약 불변: 꼬리 N행 창·헤더 미매칭·행 없으면 None
  ④ ★음성 대조 — 구 구현이 이 테스트를 통과하지 못함을 같이 증명한다
     (통과하면 테스트가 아무것도 안 지키는 것이다)

실행:
    python -X utf8 tests/test_dated_csv_8_7.py
"""

import sys
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent      # scalper-agent/
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from utils.dated_csv import (  # noqa: E402
    iso, compact, read_dated_csv, last_csv_date, csv_has_date,
)

_fails: list = []
_total = 0


def check(name, got, want):
    global _total
    _total += 1
    ok = got == want
    print(f"  {'✅' if ok else '🚨'} {name}" + ("" if ok else f" — 실측 {got!r} / 기대 {want!r}"))
    if not ok:
        _fails.append(name)


# ── 구 구현 (git 90a7a8d 원문) — 음성 대조 전용 ──────────────────────
def _old_has(csv_path, target, tail_rows=5):
    if not csv_path.exists():
        return False
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in reversed(lines[-tail_rows:]):
            s = line.strip()
            if not s:
                continue
            if s.split(",")[0].strip()[:10] == target:
                return True
        return False
    except Exception:
        return False


def _old_last(csv_path):
    if not csv_path.exists():
        return None
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return None
        last_line = lines[-1].strip()
        if not last_line:
            last_line = lines[-2].strip() if len(lines) > 2 else ""
        if not last_line:
            return None
        return last_line.split(",")[0].strip()[:10]
    except Exception:
        return None


_TMP = Path(tempfile.mkdtemp(prefix="f82_"))


def w(name: str, text: str, bom: bool = False) -> Path:
    p = _TMP / name
    p.write_text(("﻿" if bom else "") + text, encoding="utf-8")
    return p


# ══════════════════════════════════════════════════════════════════
def test_01_iso_흡수():
    print("\n[1] iso() — 표기 3종 + 잡음 흡수")
    check("ISO 그대로", iso("2026-08-07"), "2026-08-07")
    check("compact→ISO", iso("20260807"), "2026-08-07")
    check("슬래시", iso("2026/08/07"), "2026-08-07")
    check("점 표기", iso("2026.08.07"), "2026-08-07")
    check("BOM 선두", iso("﻿20260807"), "2026-08-07")
    check("따옴표", iso('"2026-08-07"'), "2026-08-07")
    check("시각 동반", iso("2026-08-07 15:30:00"), "2026-08-07")
    check("T 구분자", iso("2026-08-07T09:01:00"), "2026-08-07")
    check("앞뒤 공백", iso("  2026-08-07  "), "2026-08-07")


def test_02_해석불가는_None():
    print("\n[2] 해석 불가 → None (예외 아님 · 판정을 막지 않는다)")
    check("헤더 문자열", iso("date"), None)
    check("한글 헤더", iso("날짜"), None)
    check("빈 문자열", iso(""), None)
    check("None 입력", iso(None), None)
    check("자릿수 부족", iso("2026-8-7"), None)
    check("숫자 아님", iso("20260A07"), None)


def test_03_compact_방향_유지():
    print("\n[3] compact() — ticks 디렉터리 경로용 (ISO로 밀면 경로가 깨진다)")
    check("ISO→compact", compact("2026-08-07"), "20260807")
    check("compact 유지", compact("20260807"), "20260807")
    check("BOM+시각", compact("﻿2026-08-07 15:30:00"), "20260807")
    check("경로 조립 가능", (Path("ticks") / compact("2026-08-07")).name, "20260807")
    check("해석불가 None", compact("nope"), None)


def test_04_ISO채널_기존동작_불변():
    print("\n[4] ISO 채널 — 기존 판정과 완전 동일해야 한다")
    p = w("daily.csv", "날짜,시가,종가\n2026-08-05,1,2\n2026-08-06,1,2\n2026-08-07,1,2\n")
    check("당일 True", csv_has_date(p, "2026-08-07"), True)
    check("전일 True", csv_has_date(p, "2026-08-06"), True)
    check("미래일 False", csv_has_date(p, "2026-08-10"), False)
    check("last=ISO", last_csv_date(p), "2026-08-07")
    check("구현과 일치(has)", csv_has_date(p, "2026-08-07"), _old_has(p, "2026-08-07"))
    check("구현과 일치(last)", last_csv_date(p), _old_last(p))


def test_05_compact채널_8_7사고_처방():
    print("\n[5] ★compact 채널(flow_market) — 8/7 사고 지점")
    p = w("kospi_investor.csv", "date,지수종가\n20260805,1\n20260806,2\n20260807,3\n", bom=True)
    check("신: 8/7 찾음", csv_has_date(p, "2026-08-07"), True)
    check("신: 8/6 찾음", csv_has_date(p, "2026-08-06"), True)
    check("신: last=ISO", last_csv_date(p), "2026-08-07")
    check("신: compact 인자도 수용", csv_has_date(p, "20260807"), True)
    # ★음성 대조 — 구 구현은 여기서 반드시 틀려야 한다
    check("★구: 8/7 놓침(=사고 재현)", _old_has(p, "2026-08-07"), False)
    check("★구: last 미정규화", _old_last(p), "20260807")


def test_06_꼬리창_계약_보존():
    print("\n[6] 꼬리 N행 창 — 창 밖 날짜는 여전히 False (7/16 계약)")
    rows = "".join(f"2026-07-{d:02d},1\n" for d in range(1, 21))
    p = w("long.csv", "date,v\n" + rows)
    check("창 안(마지막)", csv_has_date(p, "2026-07-20", tail_rows=5), True)
    check("창 안(-4)", csv_has_date(p, "2026-07-16", tail_rows=5), True)
    check("창 밖", csv_has_date(p, "2026-07-10", tail_rows=5), False)
    check("창 확대하면 찾음", csv_has_date(p, "2026-07-10", tail_rows=20), True)
    check("구현과 일치(창밖)", csv_has_date(p, "2026-07-10", tail_rows=5),
          _old_has(p, "2026-07-10", 5))


def test_07_헤더_빈행_경계():
    print("\n[7] 경계 — 헤더뿐/빈 꼬리행/헤더 오매칭")
    only = w("onlyhdr.csv", "date,v\n")
    check("헤더뿐 → None", last_csv_date(only), None)
    check("헤더뿐 → 구현 일치", last_csv_date(only), _old_last(only))
    blank = w("blank.csv", "date,v\n2026-08-06,1\n2026-08-07,2\n\n\n")
    check("꼬리 빈행 건너뜀", last_csv_date(blank), "2026-08-07")
    check("빈행 있어도 has", csv_has_date(blank, "2026-08-07"), True)
    hdr = w("hdrmatch.csv", "date,v\n2026-08-07,1\n")
    check("헤더는 날짜로 안 읽힘", last_csv_date(hdr), "2026-08-07")
    missing = _TMP / "없는파일.csv"
    check("부재 → None", last_csv_date(missing), None)
    check("부재 → False", csv_has_date(missing, "2026-08-07"), False)


def test_08_read_dated_csv():
    print("\n[8] read_dated_csv — BOM 흡수 + 0번 컬럼 정규화")
    p = w("mixed.csv", "date,지수종가\n20260806,1\n20260807,2\n", bom=True)
    hdr, rows = read_dated_csv(p)
    check("헤더 BOM 제거", hdr[0], "date")
    check("행 날짜 ISO화", [r[0] for r in rows], ["2026-08-06", "2026-08-07"])
    check("다른 컬럼 보존", rows[-1][1], "2")
    hdr2, rows2 = read_dated_csv(p, form="compact")
    check("compact 형식", [r[0] for r in rows2], ["20260806", "20260807"])
    _, none_rows = read_dated_csv(_TMP / "없다.csv")
    check("부재 → 빈 결과", none_rows, [])


def test_09_norm_date_위임_회귀():
    print("\n[9] verify_channels.norm_date — compact 유지(경로 조립 회귀 방지)")
    sys.path.insert(0, str(BASE_DIR / "tools" / "manual"))
    import verify_channels as vc  # noqa: E402
    check("ISO→compact", vc.norm_date("2026-08-07"), "20260807")
    check("compact 유지", vc.norm_date("20260807"), "20260807")
    check("BOM 흡수", vc.norm_date("﻿2026-08-07"), "20260807")
    check("해석불가는 원문", vc.norm_date("date"), "date")


def main() -> int:
    import inspect
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    for t in tests:
        t()
    print("\n" + "=" * 58)
    if _fails:
        print(f"🚨 {_total}건 중 실패 {len(_fails)}건: {', '.join(_fails)}")
        return 1
    print(f"✅ 전건 PASS ({_total}/{_total} · 발견 {len(tests)}함수)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
