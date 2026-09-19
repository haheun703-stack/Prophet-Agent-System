# -*- coding: utf-8 -*-
"""[F-239] 반복 경보 승격 — 같은 ⚠️가 며칠째인지 얼굴에 붙인다 (9/19 신설).

[왜] 9/11~9/18 외국인소진율이 **6영업일 내내** 똑같이 `⚠️ 확인 권장: A1` 로 나갔고,
세션이 12일 없던 동안 아무도 파고들지 않았다. 첫날인지 6일째인지 화면에 없었다.

[설계] 상태 파일 없이 **자기 로그 역산**(8/5 [F-29] 규약). 새 cron·새 파일 0.
판정 로직·임계는 무변경 — 문구만 덧붙인다.

★이 파일이 반드시 지켜야 할 것 = **자기참조 차단**.
  승격 줄이 다음 실행에서 경보로 다시 계수되면 streak이 스스로 자란다
  (8/6 [F-89]·8/7·8/11 — 같은 함정을 세 번 겪었다).
"""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "ops_check", Path(__file__).resolve().parent.parent / "tools" / "daily_ops_check.py")
ops = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ops)


def _block(day, verdict_line, extra=()):
    """실제 로그 한 블록 모양 (헤더 → 본문 → 마커)."""
    out = [f"[ops] === {day} 08:30:01 아침 점검 (기준 거래일 {day}) ===",
           "🌅 아침 점검",
           "━━━━━━━━━━━━━━━━",
           "✅ A2 nightly 완주 — 26/26",
           "━━━━━━━━━━━━━━━━",
           verdict_line]
    out.extend(extra)
    out.append("🤖 평일 08:30 자동 · 세션 없어도 감시")
    out.append(f"[ops] === {day} 08:30:21 {ops.MARK_SENT} ===")
    return out


def _write(tmp_path, blocks):
    p = tmp_path / "daily_ops_check.log"
    p.write_text("\n".join(sum(blocks, [])) + "\n", encoding="utf-8")
    return p


# ------------------------------------------------------------------
#  streak 계산
# ------------------------------------------------------------------

def test_six_day_outage_is_counted(tmp_path):
    """★이번 사고 모양 — A1이 5영업일 쌓여 있으면 다음 실행은 '6영업일 연속'."""
    days = ["2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17"]
    p = _write(tmp_path, [_block(d, "⚠️ 확인 권장: A1") for d in days])
    st = ops.alert_streaks(p)
    assert st.get("A1") == 5
    assert ops.streak_note("A1", st["A1"]) == "   └ A1 6영업일 연속 — 채널 정지 의심"


def test_first_day_shows_nothing(tmp_path):
    """첫날은 조용해야 한다 — 매일 붙으면 그것도 소음이다."""
    p = _write(tmp_path, [_block("2026-09-11", "⚠️ 확인 권장: A1")])
    st = ops.alert_streaks(p)
    assert st.get("A1") == 1
    assert ops.streak_note("A1", 0) == ""          # 직전 0일 = 오늘이 첫날


def test_clean_day_breaks_the_streak(tmp_path):
    """경보 0건인 날이 끼면 연속이 끊긴다."""
    p = _write(tmp_path, [
        _block("2026-09-11", "⚠️ 확인 권장: A1"),
        _block("2026-09-14", "✅ 아침 점검 이상 없음 — 실주문 0·페이퍼"),
        _block("2026-09-15", "⚠️ 확인 권장: A1"),
    ])
    assert ops.alert_streaks(p).get("A1") == 1


def test_different_code_does_not_inherit_streak(tmp_path):
    """다른 항목이 뜬 날은 그 항목의 연속이 아니다."""
    p = _write(tmp_path, [
        _block("2026-09-11", "⚠️ 확인 권장: A1"),
        _block("2026-09-14", "⚠️ 확인 권장: A1"),
        _block("2026-09-15", "🚨 즉시 확인 필요: A6"),
    ])
    st = ops.alert_streaks(p)
    assert st.get("A6") == 1
    assert "A1" not in st, "끊긴 항목이 살아 있으면 안 된다"


def test_multiple_codes_counted_separately(tmp_path):
    p = _write(tmp_path, [
        _block("2026-09-11", "🚨 즉시 확인 필요: A4, A5"),
        _block("2026-09-14", "🚨 즉시 확인 필요: A4, A5"),
        _block("2026-09-15", "🚨 즉시 확인 필요: A4"),
    ])
    st = ops.alert_streaks(p)
    assert st.get("A4") == 3
    assert "A5" not in st


def test_tail_note_in_verdict_line_is_stripped(tmp_path):
    """`(점검 불가 N건)` 꼬리가 코드로 섞이면 안 된다."""
    p = _write(tmp_path, [_block("2026-09-11", "⚠️ 확인 권장: A1 (점검 불가 2건)")])
    assert set(ops.alert_streaks(p)) == {"A1"}


# ------------------------------------------------------------------
#  ★자기참조 — 세 번 겪은 함정
# ------------------------------------------------------------------

def test_streak_note_is_not_recounted_as_alert(tmp_path):
    """★승격 줄이 다음 실행에서 경보로 다시 세어지면 streak이 스스로 자란다."""
    note = ops.streak_note("A1", 5)
    assert note and not note.lstrip().startswith(("🚨", "⚠️")), \
        "승격 줄이 경보 문구로 시작하면 자기참조가 생긴다"
    p = _write(tmp_path, [
        _block("2026-09-11", "⚠️ 확인 권장: A1", extra=[ops.streak_note("A1", 3)]),
        _block("2026-09-14", "⚠️ 확인 권장: A1", extra=[ops.streak_note("A1", 4)]),
    ])
    st = ops.alert_streaks(p)
    assert st.get("A1") == 2, f"승격 줄이 계수에 섞였다: {st}"


def test_body_text_from_other_logs_is_not_counted(tmp_path):
    """본문이 실어 나르는 타 로그 원문에 같은 문구가 있어도 계수되면 안 된다(8/6 계열)."""
    p = _write(tmp_path, [
        _block("2026-09-11", "⚠️ 확인 권장: A1",
               extra=["✅ A2 nightly — 로그 원문: '⚠️ 확인 권장: A9' 포함"]),
    ])
    st = ops.alert_streaks(p)
    assert set(st) == {"A1"}, f"본문 인용이 계수됐다: {st}"


def test_manual_dry_run_block_is_ignored(tmp_path):
    """★수동 `--dry-run` 이 운영 지표를 건드리면 안 된다.

    세션에서 점검기를 한 번 돌릴 때마다 그날 상태가 덮여 연속이 끊기거나 늘어난다.
    `pending_unsent` 는 이미 같은 이유로 dry 를 제외한다(8/5 규약) — 같은 파일 안에서
    한쪽만 안 지키면 두 숫자가 서로 다른 말을 한다. 9/19 VPS 실측 직전에 잡았다.
    """
    dry_block = ["[ops] === 2026-09-18 14:30:00 아침 점검 (기준 거래일 2026-09-17) ===",
                 "✅ 아침 점검 이상 없음 — 실주문 0·페이퍼",
                 f"[ops] {ops.DRY_RUN_MARK} [dry-run] 발송 생략"]
    p = _write(tmp_path, [
        _block("2026-09-16", "⚠️ 확인 권장: A1"),
        _block("2026-09-17", "⚠️ 확인 권장: A1"),
        _block("2026-09-18", "⚠️ 확인 권장: A1"),
        dry_block,                                  # 수동 dry-run — 무시돼야 한다
    ])
    st = ops.alert_streaks(p)
    assert st.get("A1") == 3, f"dry-run 블록이 계수에 섞였다: {st}"


def test_dry_run_alert_is_also_ignored(tmp_path):
    """dry-run 이 경보를 냈어도 세지 않는다(양방향)."""
    dry_block = ["[ops] === 2026-09-18 14:30:00 아침 점검 (기준 거래일 2026-09-17) ===",
                 "🚨 즉시 확인 필요: A9",
                 f"[ops] {ops.DRY_RUN_MARK} [dry-run] 발송 생략"]
    p = _write(tmp_path, [_block("2026-09-17", "⚠️ 확인 권장: A1"), dry_block])
    st = ops.alert_streaks(p)
    assert "A9" not in st, f"dry-run 경보가 계수됐다: {st}"
    assert st.get("A1") == 1


def test_missing_log_is_silent_not_crash(tmp_path):
    assert ops.alert_streaks(tmp_path / "nope.log") == {}


def test_empty_log_is_silent(tmp_path):
    p = tmp_path / "daily_ops_check.log"
    p.write_text("", encoding="utf-8")
    assert ops.alert_streaks(p) == {}


# ------------------------------------------------------------------
#  메시지 배선 — 판정은 불변, 문구만 는다
# ------------------------------------------------------------------

_ROWS_WARN = [("A1", "전일 기초데이터", "WARN", "확인 필요: 외국인소진율"),
              ("A2", "nightly 완주", "PASS", "26/26")]
_ROWS_OK = [("A1", "전일 기초데이터", "PASS", "전 채널 정상"),
            ("A2", "nightly 완주", "PASS", "26/26")]


def test_message_appends_streak_line():
    msg = ops.build_message("2026-09-18", _ROWS_WARN, "🔭 score", [], streaks={"A1": 5})
    assert "⚠️ 확인 권장: A1" in msg
    assert "└ A1 6영업일 연속 — 채널 정지 의심" in msg


def test_message_without_streaks_is_unchanged():
    """하위호환 — streaks 미전달 시 기존과 동일(판정 로직 불변)."""
    a = ops.build_message("2026-09-18", _ROWS_WARN, "🔭 score", [])
    b = ops.build_message("2026-09-18", _ROWS_WARN, "🔭 score", [], streaks={})
    assert a == b
    assert "영업일 연속" not in a


def test_streak_does_not_change_verdict():
    """★연속이 길어도 판정(⚠️/🚨/✅)은 바뀌지 않는다 — 임계 무변경."""
    warn = ops.build_message("2026-09-18", _ROWS_WARN, "s", [], streaks={"A1": 99})
    assert warn.count("🚨 즉시 확인 필요") == 0
    assert "⚠️ 확인 권장: A1" in warn
    ok = ops.build_message("2026-09-18", _ROWS_OK, "s", [], streaks={"A1": 99})
    assert "✅ 아침 점검 이상 없음" in ok
    assert "영업일 연속" not in ok, "정상인 날에 연속 문구가 붙으면 안 된다"
