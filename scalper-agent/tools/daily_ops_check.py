# -*- coding: utf-8 -*-
"""daily_ops_check.py — 아침 점검(A1~A9) 무인화 (F-8 → F-29 승격판)

배경(7/31 사장님 지시: "개선을 우리가 실천·행동할 수 있게 자동화 코드로 적용하자"):
  저녁 20:10 데이터 요약은 cron 자동인데 **아침 점검만 사람(세션) 의존**이라
  7/29 세션 미개설 = A1~A9 하루 통째 공백이 실증됐다([F-29]). 세션이 없어도
  이상이 사장님께 닿아야 한다 → 평일 08:30 cron + 텔레그램 1건.

검사 항목 (DAILY_ROUTINE.md §1과 1:1):
  A1 전일 기초데이터 소급 실측  — DataVerifier(save_result=False) + 핵심 채널 판정
  A2 nightly 파이프라인 완주    — logs/nightly.log 마지막 블록 요약 N/N + ⑳ 완료위장
  A3 20:10 텔레그램 발송 성공   — logs/notify_freshness.log 실행 스탬프 + 발송 결과
  A4 OBSERVE v2 러너 가동       — logs/observe_v2.log 전 거래일 누적 신호
  A5 ⑲-2 대조 + 스코어보드      — nightly 블록의 대조 라인 + 페이퍼 누적(단일진실)
  A6 봇 서비스·안전 불변식      — systemctl + PAPER_ONLY + 자동매매 스위치
  A9 전략 데드라인 대장         — strategy_deadline_check.build_report() 위임

  ※ A7(로컬 미러)은 노트북 파일 상태라 VPS에서 관측 불가 — 세션 시작 시 수동.
    A8(업무일지)은 사람의 기록 행위 — 자동화 대상 아님.

안전 불변식(전량 준수):
  - ★ read-only ★ — 상태 파일 무접촉(DataVerifier save_result=False), 매수/매도/
    picks/SAJANG/주문 0 접촉. 이 도구는 '읽고 알리는' 관측기다.
  - is_trading_day 가드: 휴장일 미발송(신규 job 표준·CLAUDE.md RULE-002)
  - 전 항목 예외 격리 — 한 항목 실패가 나머지 점검·발송을 무산시키지 않는다
    ([F-22]/[F-36]의 '조용한 사망' 재발 방지). 점검 예외로는 절대 죽지 않는다(exit 0).
    단 **텔레그램 발송 실패는 exit 1** — 사장님 미수신을 기계가 알 수 있는 유일한 신호라
    cron MAILTO까지 닿아야 한다(체이닝 금지·단독 cron 라인 전제).
  - **[F-89] 발송 재시도 3회 + 미발송 누적 병기** (8/5) — 첫 평일 8/3에 발송이
    `RemoteDisconnected`로 실패했다. 판정 본문은 정상이었고 A1~A6도 전부 ✅였는데
    **사장님께만 안 갔고, 그 로그를 읽을 세션이 그날 없었다** = 무인화의 전제가
    마지막 1마일에서 깨진 것(7/14 "자동화 마지막 1마일은 사람에 닿는 알림"과 같은 자리).
    누적 카운트는 **자기 로그 역산**으로 구한다 — 상태 파일을 만들면 위의
    '상태 파일 무접촉' 불변식이 깨지기 때문이다.
  - 판정 수치는 기존 단일진실에 위임 — 상수/로직 로컬 복제 금지([F-37] 교훈)

Usage:
    python tools/daily_ops_check.py              # 점검 + 텔레그램 (cron 08:30)
    python tools/daily_ops_check.py --dry-run    # 발송 없이 stdout만 (검수용)
    python tools/daily_ops_check.py --date 2026-07-30   # 기준 거래일 강제(검수)
    python tools/daily_ops_check.py --force      # 휴장일 가드 무시(검수)
"""

import argparse
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent          # scalper-agent/
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(BASE_DIR / "tools") not in sys.path:
    sys.path.insert(0, str(BASE_DIR / "tools"))            # 자매 도구 위임용

# cron 환경엔 셸 export가 없어 config.yaml의 ${TELEGRAM_*}가 빈 값이 된다(7/15 실측·F-2).
try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR.parent / ".env")
except Exception:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

LOGS_DIR = BASE_DIR.parent / "logs"            # /home/ubuntu/bodyhunter/logs
CONFIG_PATH = BASE_DIR / "config.yaml"
BOT_UNIT = "bodyhunter-bot"

_WD = "월화수목금토일"
_MARK = {"PASS": "✅", "WARN": "⚠️", "FAIL": "🚨", "SKIP": "⏭️"}
_RANK = {"PASS": 0, "SKIP": 1, "WARN": 2, "FAIL": 3}

# ── [F-89] 발송 재시도 + 미발송 누적 병기 (8/5 신설) ────────────────────
# 8/3(첫 평일) 발송이 RemoteDisconnected로 실패했고 재시도가 0건이었다.
# 본문·판정은 정상이었는데 사장님께만 안 갔고, 그 로그를 읽을 세션이 그날 없었다.
# ★마커를 상수로 두는 이유: 이 도구가 **자기 로그를 되읽어** 미발송을 센다.
#   기록하는 문자열과 읽는 문자열이 갈리면 카운트가 조용히 0이 된다(단일진실).
SELF_LOG = "daily_ops_check.log"          # cron이 >> 로 받는 파일(2>&1 포함)
MARK_SENT = "텔레그램 발송 완료"
MARK_FAILED = "텔레그램 발송 실패"        # ★최종 실패에만 쓴다(재시도 중간 로그는 다른 문구)
SEND_ATTEMPTS = 3
SEND_BACKOFF = (2, 5)                     # 시도 사이 대기(초) — 최악 7초, cron 무해
_OPS_HEADER_RE = re.compile(r"\[ops\] === (\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2} 아침 점검")

try:
    from data.trading_calendar import is_trading_day, last_trading_day
except Exception:  # pragma: no cover - 캘린더 부재 시 주말만 제외
    def is_trading_day(d=None):
        return (d or date.today()).weekday() < 5

    def last_trading_day(d=None):
        # 정본과 동일 규약: d를 포함하지 않는 '직전' 거래일
        d = (d or date.today()) - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d


# ────────────────────────────────────────────────────────────────
# 로그 유틸 — 전부 read-only
# ────────────────────────────────────────────────────────────────
def _read_lines(path: Path, tail: int = 4000) -> list:
    """로그 꼬리 읽기. 부재/권한 오류는 빈 목록(호출부가 SKIP 판정)."""
    try:
        txt = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    return txt.splitlines()[-tail:]


def _find_ref_block(lines: list, pat, ref: str, skip_marker: str = None) -> tuple:
    """append-only 로그에서 ★기준일 ref의★ 실행 블록 (found, 블록 라인들, 마지막 실행일).

    pat = 실행 헤더 정규식(그룹1 = 기준일 YYYY-MM-DD).
    ★ '마지막 블록'이 아니라 'ref 블록'을 찾는다 — 휴장일 다음 거래일 아침엔
    마지막 블록이 휴장일자라 ref(직전 거래일)와 어긋나 정상을 FAIL로 오탐한다.
    (7/17 제헌절류 캘린더 사고와 같은 자리 — 점검기가 오탐하면 신뢰가 죽는다)
    skip_marker: 그 문자열을 포함한 블록은 건너뛴다(예: 수동 dry-run 실행분)."""
    starts = []          # [(index, day)]
    for i, ln in enumerate(lines):
        m = pat.search(ln)
        if m:
            starts.append((i, m.group(1)))
    if not starts:
        return False, [], None
    last_day = starts[-1][1]
    for pos in range(len(starts) - 1, -1, -1):
        if starts[pos][1] != ref:
            continue
        start = starts[pos][0]
        end = starts[pos + 1][0] if pos + 1 < len(starts) else len(lines)
        block = lines[start:end]
        if skip_marker and any(skip_marker in ln for ln in block):
            continue                 # 수동 실행분 — 정규 실행 증거로 채택하지 않는다
        return True, block, last_day
    return False, [], last_day


_NIGHTLY_HEAD = re.compile(r"야간 통합 관측 파이프라인\s+(\d{4}-\d{2}-\d{2})")
# ★ Tier1 M-1: 캡처는 '실행일'이 아니라 ★기준일★이어야 한다. 실행일로 앵커하면
# 과거일 백필(`--date`는 dry-run이 아니라 실제 발송)이 그날 20:10 정규 발송 증거로
# 위장된다. 정규 20:10은 기준=실행일이라 무영향.
_NOTIFY_HEAD = re.compile(
    r"\[notify\]\s*={3}\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 실행 \(기준 (\d{4}-\d{2}-\d{2})\)")


def _nightly_block(lines: list, ref: str) -> tuple:
    """nightly.log 기준일 블록. 헤더: `===== 야간 통합 관측 파이프라인 YYYY-MM-DD =====`"""
    return _find_ref_block(lines, _NIGHTLY_HEAD, ref)


# ────────────────────────────────────────────────────────────────
# 개별 점검 (모두 (status, detail) 반환 — 예외는 호출부에서 격리)
# ────────────────────────────────────────────────────────────────
def check_a1_freshness(ref: str) -> tuple:
    """A1 전일 기초데이터 소급 실측 — 20:10 요약과 같은 검증기·같은 판정문."""
    from data.data_verifier import DataVerifier
    from notify_data_freshness import build_summary

    # ★ save_result=False — 소급 검증이 상태 파일을 전일자로 되돌려 쓰던 7/30 M-1 사고 방지
    result = DataVerifier(ref, save_result=False).verify_all()
    summary = build_summary(result, ref)
    verdict = ""
    for ln in summary.splitlines():
        if ln.startswith("✅ 핵심 채널") or ln.startswith("⚠️ 확인 필요"):
            verdict = ln
            break
    if verdict.startswith("✅"):
        return "PASS", "핵심 채널 정상"
    # ★ Tier1 M-2: A1은 §1 최상위 항목인데 FAIL 승격 경로가 없어 종가·투자자수급이
    # 무너져도 헤드라인이 ⚠️에 머물렀다. 검증기가 이미 계산해 둔 critical_failures를 쓴다.
    crit = result.get("critical_failures") or []
    if crit:
        return "FAIL", "CRITICAL 결손 " + ", ".join(crit)
    return "WARN", (verdict or "판정문 없음").replace("⚠️ ", "")


def check_a2_nightly(ref: str) -> tuple:
    """A2 nightly 완주 — 기준일 블록의 요약 N/N·⑳ 완료위장 여부."""
    lines = _read_lines(LOGS_DIR / "nightly.log")
    if not lines:
        return "SKIP", "nightly.log 없음(VPS 전용)"
    found, block, last_day = _nightly_block(lines, ref)
    if not found:
        # Tier1 L-2: 헤더 0건(로그 초기화 등)도 '미실행 증거 없음'이라 A5와 심각도 통일
        return "FAIL", f"{ref} 실행 없음 (로그 마지막 {last_day or '없음'} · 미실행 의심)"

    m = None
    for ln in block:
        mm = re.search(r"요약:\s*(\d+)/(\d+)\s*성공", ln)
        if mm:
            m = mm
    if not m:
        return "FAIL", f"{ref} 요약 라인 없음(중단 의심)"
    ok, total = int(m.group(1)), int(m.group(2))
    fresh_ok = any("완료위장 없음" in ln for ln in block)
    if ok == total and fresh_ok:
        return "PASS", f"{ok}/{total} · ⑳ 완료위장 없음"
    if ok != total:
        return "FAIL", f"{ok}/{total} 성공 — 실패 스텝 {total - ok}건"
    return "WARN", f"{ok}/{total} · ⑳ 신선도 미확인"




def check_a3_notify(ref: str) -> tuple:
    """A3 20:10 발송 — 실행 스탬프(F-4 fix) 기준. 스탬프 없으면 mtime 폴백."""
    path = LOGS_DIR / "notify_freshness.log"
    lines = _read_lines(path, tail=400)
    if not lines:
        return "SKIP", "notify_freshness.log 없음(VPS 전용)"

    # 수동 dry-run 실행분은 정규 20:10 발송 증거가 아니다(Tier1 M-1 후단)
    found, block, last_day = _find_ref_block(lines, _NOTIFY_HEAD, ref, skip_marker="[dry-run]")
    if found:
        src = f"스탬프 {ref}"
    elif last_day is not None:
        return "FAIL", f"{ref} 실행 없음 (로그 마지막 {last_day} · 미발송 의심)"
    else:
        # 폴백: 스탬프 도입(7/31 F-4) 이전 실행분 — mtime으로 근사.
        # 스탬프가 하루라도 쌓이면 이 경로는 자연 소멸한다.
        try:
            day = date.fromtimestamp(path.stat().st_mtime).isoformat()
        except Exception:
            return "WARN", "실행 시각 불명(스탬프·mtime 모두 실패)"
        if day != ref:
            return "FAIL", f"마지막 발송 mtime {day} ≠ 기준 {ref} (미발송 의심)"
        block, src = lines[-15:], f"mtime {day}"

    if any("발송 완료" in ln for ln in block):
        return "PASS", f"발송 완료 ({src})"
    if any(("발송 실패" in ln) or ("미설정" in ln) for ln in block):
        return "FAIL", f"발송 실패 — 사장님 미수신 ({src})"
    # 실행 흔적은 있는데 성공/실패 마커가 둘 다 없다 = 중간에 죽었거나 로그 스트림 유실.
    # 사장님 미수신이 확정적이므로 노란불 금지(Tier1 H-1 — 현재 cron은 2>&1 병합 실측 확인,
    # 그 설정에 의존하지 않도록 코드 쪽에서 확정 처리).
    return "FAIL", f"실행 흔적만 있고 발송 결과 없음 — 미수신 의심 ({src})"


def check_a4_observe(ref: str) -> tuple:
    """A4 OBSERVE v2 러너 — 08:30 시점엔 당일 미실행이라 전 거래일 누적으로 판정."""
    lines = _read_lines(LOGS_DIR / "observe_v2.log", tail=1500)
    if not lines:
        return "SKIP", "observe_v2.log 없음(VPS 전용)"
    ymd = ref.replace("-", "")
    pat = re.compile(r"\[observe_v2\]\s*(\d{8}).*누적\s*(\d+)")
    last_day, cum = None, None
    for ln in lines:                       # ★ 기준일 라인만 채택(휴장일 라인 무시)
        m = pat.search(ln)
        if not m:
            continue
        last_day = m.group(1)
        if last_day == ymd:
            cum = int(m.group(2))          # 같은 날 여러 사이클 → 마지막 누적
    if last_day is None:
        return "WARN", "가동 라인 미발견"
    if cum is None:
        return "FAIL", f"{ymd} 가동 없음 (로그 마지막 {last_day} · 러너 정지 의심)"
    if cum == 0:
        # 신호 0 자체는 정상일 수 있으나 [F-38] 결손과 구분이 안 되므로 표면화
        return "WARN", f"{ymd} 누적 0건 — 무신호/결손 구분 확인"
    return "PASS", f"{ymd} 누적 {cum}건"


def check_a5_contrast(ref: str) -> tuple:
    """A5 ⑲-2 대조 — 관측 의도 vs 체결 재현 일치(miss/phantom 0)."""
    lines = _read_lines(LOGS_DIR / "nightly.log")
    if not lines:
        return "SKIP", "nightly.log 없음(VPS 전용)"
    found, block, last_day = _nightly_block(lines, ref)
    if not found:
        return "FAIL", f"{ref} nightly 블록 없음 (마지막 {last_day}) — 대조 미실행"
    # ★ 7/31 전체검수 M-3: 러너 verdict는 PASS/EMPTY/CHECK다 — "FAIL"은 존재하지 않는다
    # (observe_v2_runner.py:195-199). 앞서 PASS|FAIL만 매칭해서, 정작 가장 중요한
    # **CHECK(불일치 발생 = ⑲-2의 존재 이유)** 날을 "스텝 자체 미실행"으로 오진했다.
    # 운영자를 데이터 정합 결함이 아니라 cron 장애로 몰아가는 최악의 오진.
    m, row = None, None
    for ln in block:
        mm = re.search(r"(\d{8}) 대조 — (PASS|CHECK|EMPTY|FAIL): (.+)$", ln)
        if mm:
            m, row = mm, ln
    if not m:
        # run_step 실패행(`⚠ ⑲-2 … exit=N | stderr`)엔 결과 토큰이 없다.
        step = [ln for ln in block if "⑲-2" in ln and "시작" not in ln and "완료" not in ln]
        if step:
            return "FAIL", f"⑲-2 실행됐으나 대조 결과 없음 — {step[-1].strip()[-90:]}"
        return "FAIL", "⑲-2 스텝 자체 미실행 — 판정 증거 미생성"

    day_tok, verdict, detail = m.group(1), m.group(2), m.group(3).strip()
    if day_tok != ref.replace("-", ""):
        return "WARN", f"대조 대상 {day_tok} (기준 {ref})"
    if verdict == "PASS":
        return "PASS", detail.split(" / ")[0]
    if verdict == "EMPTY":
        # 신호 0건 — 정상일 수 있으나 [F-38] 결손과 구분이 안 되므로 표면화
        return "WARN", f"대조 EMPTY(신호 0건) — 무신호/결손 구분 확인 · {detail[:70]}"
    # CHECK/FAIL = miss·phantom·drift 실재 → 판정 표본 오염 가능
    return "FAIL", f"대조 {verdict} — {detail[:110]}"


def check_a6_safety() -> tuple:
    """A6 봇 서비스·안전 불변식 — 서비스 상태 + PAPER_ONLY + 자동매매 스위치."""
    # ★ Tier1 M-5: 스위치 판정은 봇 본체와 같은 _truthy를 써야 한다. `v is True`로 세면
    # `"AUTO_BUY_ENABLED": "true"`(문자열)일 때 봇은 켜진 채 동작하는데 점검은 OFF로
    # 보고한다 — 안전 사실이 실제와 갈리는 [F-37]류 로컬 복제.
    from bot.trade_runtime_config import load_runtime_config, is_paper_only, _truthy

    bits = []
    if sys.platform == "win32":
        svc = "skip(win32)"
    else:
        try:
            r = subprocess.run(["systemctl", "is-active", BOT_UNIT],
                               capture_output=True, text=True, timeout=15)
            svc = (r.stdout or r.stderr).strip() or "unknown"
        except Exception as e:  # noqa: BLE001
            svc = f"unknown({type(e).__name__})"
    bits.append(f"service={svc}")

    paper = is_paper_only()
    bits.append("PAPER_ONLY=True" if paper else "PAPER_ONLY=False")

    cfg = load_runtime_config()
    switches = cfg.get("strategy_switches", {}) or {}
    on = [k for k, v in switches.items() if _truthy(v)]
    bits.append(f"자동매매 스위치 ON {len(on)}/{len(switches)}")

    # 실매매를 막는 최종 관문은 PAPER_ONLY — 이게 풀리면 무조건 사장님께 알린다
    if not paper:
        return "FAIL", "★ PAPER_ONLY=False — 실주문 가능 상태 · " + " · ".join(bits)
    if sys.platform != "win32" and svc != "active":
        return "WARN", " · ".join(bits)
    return "PASS", " · ".join(bits)


def check_a9_deadlines() -> tuple:
    """A9 전략 데드라인 — 단일진실 위임(스코어보드 문구·판정 동일)."""
    from strategy_deadline_check import build_report
    lines, alerts = build_report()
    score = lines[0] if lines else "스코어보드 없음"
    # ★ Tier1 M-3: "⚠ 대장 로드 실패" 행까지 살린다. "["만 필터하면 데드라인 체계가
    # 통째로 멈춘 상태가 detail "0건"으로 표시돼 '활성 전략 0개'와 구분이 안 된다.
    rows = [ln for ln in lines[2:] if ln.startswith(("[", "⚠"))]
    status = "WARN" if alerts else "PASS"
    return status, score, rows


# ────────────────────────────────────────────────────────────────
def run_checks(ref: str) -> tuple:
    """(결과행 목록, 스코어보드, 데드라인 행) — 항목별 예외 격리."""
    plan = [
        ("A1", f"전일({int(ref[5:7])}/{int(ref[8:10])}) 기초데이터", lambda: check_a1_freshness(ref)),
        ("A2", "nightly 완주", lambda: check_a2_nightly(ref)),
        ("A3", "20:10 발송", lambda: check_a3_notify(ref)),
        ("A4", "OBSERVE v2 러너", lambda: check_a4_observe(ref)),
        ("A5", "⑲-2 대조", lambda: check_a5_contrast(ref)),
        ("A6", "봇·안전 불변식", check_a6_safety),
    ]
    rows = []
    for code, title, fn in plan:
        try:
            status, detail = fn()
        except Exception as e:  # noqa: BLE001 — 항목 격리(전체 무산 금지)
            status, detail = "FAIL", f"점검 예외 {type(e).__name__}: {e}"
        # ★ 완료위장 차단 — VPS(운영기)에서 로그 부재는 '정상 스킵'이 아니라 이상이다.
        # 이 승격이 없으면 로그가 통째로 사라진 날 전 항목 SKIP → "이상 없음"이 발송된다
        # ([F-22] '조용한 사망'과 정확히 같은 통로). 노트북(win32)은 VPS 전용 로그가
        # 원래 없으므로 SKIP 유지.
        if status == "SKIP" and sys.platform != "win32":
            status, detail = "WARN", f"{detail} — VPS에선 정상 부재 아님"
        rows.append((code, title, status, detail))

    score, dl_rows = "📊 스코어보드: 평가 불가", []
    try:
        status, score, dl_rows = check_a9_deadlines()
    except Exception as e:  # noqa: BLE001
        status = "FAIL"
        dl_rows = [f"⚠ 데드라인 체크 예외 {type(e).__name__}: {e}"]
    rows.append(("A9", "전략 데드라인", status, f"{len(dl_rows)}건"))
    return rows, score, dl_rows


def build_message(ref: str, rows: list, score: str, dl_rows: list) -> str:
    today = date.today()
    rm, rd = ref[5:7], ref[8:10]
    head = (f"🌅 {today.month}/{today.day}({_WD[today.weekday()]}) 아침 점검 "
            f"(기준 {int(rm)}/{int(rd)} · 자동·read-only)")
    # 미지 상태 문자열의 기본값은 최악이어야 한다 — 0(PASS)이면 초록불 위장 통로(Tier1 L-1)
    worst = max((_RANK.get(s, _RANK["FAIL"]) for _c, _t, s, _d in rows), default=0)

    lines = [head, score, "━━━━━━━━━━━━━━━━"]
    for code, title, status, detail in rows:
        if code == "A9":
            continue
        lines.append(f"{_MARK.get(status, '❓')} {code} {title} — {detail}")
    lines.append("━━━━━━━━━━━━━━━━")
    for r in dl_rows:
        lines.append(f"⏰ {r}")
    lines.append("━━━━━━━━━━━━━━━━")

    bad = [f"{c}" for c, _t, s, _d in rows if s in ("FAIL", "WARN")]
    n_skip = sum(1 for _c, _t, s, _d in rows if s == "SKIP")
    # 점검 불가는 '통과'가 아니다 — 초록불에 반드시 병기(증거 부재 ≠ 이상 없음)
    tail = f" (점검 불가 {n_skip}건)" if n_skip else ""
    if worst >= _RANK["FAIL"]:
        lines.append(f"🚨 즉시 확인 필요: {', '.join(bad)}{tail}")
    elif worst >= _RANK["WARN"]:
        lines.append(f"⚠️ 확인 권장: {', '.join(bad)}{tail}")
    else:
        lines.append(f"✅ 아침 점검 이상 없음 — 실주문 0·페이퍼{tail}")
    lines.append("🤖 평일 08:30 자동 · 세션 없어도 감시")
    return "\n".join(lines)


def pending_unsent(log_path: Path) -> list:
    """직전 성공 발송 이후 쌓인 미발송 실행일 목록 ([F-89]).

    ★상태 파일을 만들지 않는다 — 이 도구의 불변식이 '상태 파일 무접촉'이기 때문이다.
    A2·A3가 이미 로그를 판정 근거로 쓰고 있고, 발송 성공/실패는 이 도구 자신이
    로그에 남기므로 별도 저장소가 필요 없다(desync·손상·원자성 통로를 0으로 유지).

    전방 스캔: 실행 헤더로 '언제'를 기억하고, 최종 실패면 쌓고, 성공을 만나면 비운다.
    로그 부재·판독 실패는 빈 목록(발송 자체를 막지 않는다 — 병기는 부가 정보다).
    """
    if not log_path.exists():
        return []
    try:
        lines = log_path.read_text("utf-8", errors="replace").splitlines()
    except OSError:
        return []
    pending: list = []
    cur = None
    for ln in lines:
        m = _OPS_HEADER_RE.search(ln)
        if m:
            cur = m.group(1)
            continue
        if MARK_SENT in ln:
            pending.clear()
        elif MARK_FAILED in ln:
            pending.append(cur or "?")
    return pending


def pending_notice(pending: list) -> str:
    """미발송 누적 병기 문구 — 실패 사실 자체가 사람에게 닿게 한다.

    ★마커 문자열(MARK_SENT/MARK_FAILED)을 포함하지 않는다. 포함하면 다음 실행의
    pending_unsent가 자기 병기문을 실패로 오독한다(자기참조 오염).
    """
    days = ", ".join(pending[-3:])
    more = f" 외 {len(pending) - 3}건" if len(pending) > 3 else ""
    return (f"⚠️ 지난 {len(pending)}회 미수신({days}{more}) — "
            f"그날 점검 결과는 VPS {SELF_LOG} 에만 남아 있습니다")


def _send_telegram(msg: str) -> int:
    """텔레그램 발송 — 20:10 요약과 동일 경로(검증된 통로).

    이름을 `_send`로 두지 않는 이유: 5/25 `_send` UnboundLocalError 사고 이후
    해당 심볼은 pre-commit RULE-003 감시 대상 패턴이라 혼동을 만들지 않는다.

    [F-89] 8/5 — 단발 발송을 SEND_ATTEMPTS회 재시도로 교체. 8/3 실패 원인은
    `RemoteDisconnected`(연결이 응답 없이 끊김)로 **재시도로 넘어가는 종류**였다.
    """
    import yaml
    from output.telegram_alert import TelegramAlert
    config = None
    if CONFIG_PATH.exists():
        try:
            config = yaml.safe_load(CONFIG_PATH.read_text("utf-8"))
        except Exception as e:  # noqa: BLE001
            print(f"[ops] config.yaml 로드 실패: {e}", file=sys.stderr)
    # 생성 자체도 격리 — config.yaml에 토큰 키가 없으면 KeyError로 죽어 "이상을 다 찾아놓고
    # 메시지만 증발"한다(Tier1 H-2a).
    try:
        tg = TelegramAlert(config)
    except Exception as e:  # noqa: BLE001
        print(f"[ops] TelegramAlert 생성 실패: {e}", file=sys.stderr)
        return 1
    if not tg.enabled:
        print("[ops] 텔레그램 미설정(토큰 없음) — 발송 스킵", file=sys.stderr)
        return 1

    for attempt in range(1, SEND_ATTEMPTS + 1):
        ok = False
        try:
            ok = tg.send_report(msg)
        except Exception as e:  # noqa: BLE001 — 통로가 예외를 던져도 재시도로 흡수
            print(f"[ops] 발송 예외({attempt}/{SEND_ATTEMPTS}): {e}", file=sys.stderr)
        if ok:
            print(f"\n[ops] === {datetime.now():%Y-%m-%d %H:%M:%S} {MARK_SENT} ===")
            return 0
        if attempt < SEND_ATTEMPTS:
            wait = SEND_BACKOFF[attempt - 1]
            print(f"[ops] 발송 재시도 대기 {wait}s ({attempt}/{SEND_ATTEMPTS} 실패)",
                  file=sys.stderr)
            time.sleep(wait)

    print(f"[ops] {MARK_FAILED} — 사장님 미수신 가능(토큰/네트워크 확인·"
          f"{SEND_ATTEMPTS}회 재시도 소진)", file=sys.stderr)
    return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="발송 없이 stdout만")
    ap.add_argument("--date", default=None, help="기준 거래일 YYYY-MM-DD (기본: 직전 거래일)")
    ap.add_argument("--force", action="store_true", help="휴장일 가드 무시(검수용)")
    args = ap.parse_args()

    # --date 오류는 plan 구성(항목 try 밖)에서 터져 무음 exit 0이 된다 → 선검증(Tier1 L-3).
    # ★ 검증만으론 부족 — py3.11 fromisoformat은 "20260730"(하이픈 없는 기본형)도 받아들여
    # 통과시킨 뒤 ref[8:10]이 빈 문자열이 된다(7/31 음성 대조에서 적발). 파싱 후 재직렬화로
    # 표현을 정규화해서 이후 슬라이싱이 항상 성립하게 한다.
    ref_arg = None
    if args.date:
        try:
            ref_arg = date.fromisoformat(args.date).isoformat()
        except ValueError:
            print(f"[ops] --date 형식 오류: {args.date} (YYYY-MM-DD)", file=sys.stderr)
            return 1

    today = date.today()
    if not args.date and not args.force and not is_trading_day(today):
        print(f"[ops] {today} 휴장일 — 점검/발송 생략")
        return 0

    # ★ last_trading_day(d)는 d를 포함하지 않는 '직전' 거래일이다(정본 docstring).
    # 여기에 today-1을 넘기면 이틀 전이 잡혀 A3가 매일 오탐한다(7/31 VPS 실증에서 적발).
    ref = ref_arg or last_trading_day(today).isoformat()
    print(f"[ops] === {datetime.now():%Y-%m-%d %H:%M:%S} 아침 점검 (기준 거래일 {ref}) ===")

    rows, score, dl_rows = run_checks(ref)
    msg = build_message(ref, rows, score, dl_rows)

    # [F-89] 지난 미발송이 있으면 본문에 병기 — 실패 사실 자체가 사람에게 닿아야 한다.
    # 판정 로직(build_message)은 순수하게 두고 발송 직전에만 덧붙인다.
    try:
        pending = pending_unsent(LOGS_DIR / SELF_LOG)
    except Exception as e:  # noqa: BLE001 — 병기는 부가 정보. 실패해도 발송은 간다.
        print(f"[ops] 미발송 누적 판독 실패(무시): {e}", file=sys.stderr)
        pending = []
    if pending:
        msg = f"{msg}\n{pending_notice(pending)}"

    print(msg)

    if args.dry_run:
        print("\n[dry-run] 발송 생략")
        return 0
    return _send_telegram(msg)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 — 관측 도구: cron 차단 금지
        print(f"[ops] 치명 예외(점검 도구 — 무시): {e}")
        raise SystemExit(0)
