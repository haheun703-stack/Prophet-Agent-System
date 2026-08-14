# -*- coding: utf-8 -*-
"""
notify_data_freshness.py — 저녁 수집 완료 후 사장님 텔레그램 '핵심 채널' 요약 발송
================================================================================
7/14 사장님 지시: "종가·csv·각자 맡은 수급 업데이트 확인을, 매번 터미널 말고
                   AWS 자동 스케줄에 맞춰 텔레그램으로 자동 발송하라."

동작:
  - 기존 DataVerifier.verify_all() 결과를 재사용(신규 검증 로직 없음)
  - CHECKLIST에 없는 외국인소진율(flow/{code}_foreign_exh.csv)만 보조 체크(본체 무접촉)
  - 사장님 친화 '핵심 채널' 한눈 요약으로 재구성 → config.yaml 토큰으로 텔레그램 발송

안전 불변식(전량 준수):
  - read-only·record-only·실주문 0·매도 무접촉 (데이터 신선도 조회/발송뿐)
  - is_trading_day 가드: 휴장일 미발송 (신규 job 표준·CLAUDE.md RULE-002)
  - SAJANG 매매룰 무관(매매 코드 아님)

Usage:
    python tools/notify_data_freshness.py                # 오늘자 검증 + 텔레그램 발송
    python tools/notify_data_freshness.py --dry-run      # 발송 없이 stdout만 (검수용)
    python tools/notify_data_freshness.py --date 2026-07-13  # 특정일 기준(검수/백필)
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # scalper-agent/
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# cron 환경엔 셸 export가 없어 config.yaml의 ${TELEGRAM_*} 해석이 빈 값 → 발송 실패
# (7/15 20:10 실측). collect_daily.py와 동일 관례로 프로젝트 루트 .env 로드(없으면 무시).
try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR.parent / ".env")
except Exception:
    pass

# 이모지 포함 출력 — 로케일이 UTF-8이 아닌 환경(win32 cp949·C locale cron)에서도 안전.
# cron은 `-X utf8`로도 실행하나 이중 방어(7/10 win32 no_tg 교훈과 동일 취지).
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from data.data_verifier import (  # noqa: E402
    DataVerifier,
    _load_active_codes,
    _get_last_csv_date,
    _csv_has_date,
    FLOW_DIR,
)
from output.telegram_alert import TelegramAlert  # noqa: E402

try:
    from data.trading_calendar import is_trading_day  # 공휴일 오판 방지(7/1 검수 정정 경로)
except Exception:  # pragma: no cover - 캘린더 부재 시 주말만 제외
    def is_trading_day(d):
        return d.weekday() < 5

CONFIG_PATH = BASE_DIR / "config.yaml"

_MARK = {"PASS": "✅", "PARTIAL": "⚠️", "FAIL": "🚨", "SKIP": "⏭️", "STALE": "🚨", "UNKNOWN": "❓"}
_WD = "월화수목금토일"


def _foreign_exh_status(today: str) -> dict:
    """외국인 소진율(flow/{code}_foreign_exh.csv) 활성종목 샘플 신선도.

    DataVerifier CHECKLIST 미포함 채널이라 요약 전용 보조 체크(본체 무접촉).
    활성종목(daily today)만 표본 → 상폐·정지 종목 오탐 원천 차단(investor fix와 동일 원리).
    cron이 19:40 재수집 이후 실행되는 전제. 상폐 랜덤샘플 1개는 허용."""
    codes = _load_active_codes(10, today)
    if not codes:
        return {"status": "UNKNOWN", "latest": None, "ok": 0, "checked": 0}
    ok = 0
    latest = None
    for c in codes:
        p = FLOW_DIR / f"{c}_foreign_exh.csv"
        d = _get_last_csv_date(p)
        if d:
            latest = d if latest is None else max(latest, d)
            if _csv_has_date(p, today):  # 소급(--date) 검증에도 안전 (7/16 fix)
                ok += 1
    if ok >= len(codes) - 1:
        status = "PASS"
    elif ok > 0:
        status = "PARTIAL"
    else:
        status = "STALE"
    return {"status": status, "latest": latest, "ok": ok, "checked": len(codes)}


def build_summary(result: dict, today: str) -> str:
    """검증 결과 → 사장님 친화 핵심 채널 요약."""
    d = result["details"]
    y, m, dd = today.split("-")
    wd = _WD[date.fromisoformat(today).weekday()]
    head = f"📊 {int(m)}/{int(dd)}({wd}) 데이터 수집 요약"

    daily_st = d["daily_ohlcv"]["status"]
    inv_st = d["investor_flow"]["status"]
    # 종가: daily_ohlcv가 비율임계(dead 2개 허용)로 상폐 랜덤샘플 오탐을 자체 흡수(7/14 검수 High fix).
    #        따라서 여기서 요약 레벨 흡수 없이 상태 그대로 표기 — PARTIAL/FAIL은 진짜 부분수집 실패(M-2 fix).
    if daily_st == "PASS":
        daily_line = "✅ 종가(OHLCV)"
    else:
        daily_line = f"{_MARK.get(daily_st, '❓')} 종가(OHLCV)  {d['daily_ohlcv'].get('ok', '')}/{d['daily_ohlcv'].get('checked', '')}"

    fx = _foreign_exh_status(today)

    def _latest(key):
        v = d[key].get("latest", "")
        return v or ""

    # ticks(장중 체결 스냅샷) — 판정 원천. 7/21 F-15: 그간 요약 사각이라 7/20 오염이
    # 초록불로 위장됐다. SKIP(win32·VPS전용 부재)은 표시 생략, PASS/FAIL만 노출.
    tk = d.get("ticks", {})
    tk_st = tk.get("status", "SKIP")
    tk_line = None
    if tk_st != "SKIP":
        tk_line = f"{_MARK.get(tk_st, '❓')} 장중틱(판정원천) {tk.get('usable_pct', '')}%"

    lines = [
        head,
        "━━━━━━━━━━━━━━━━",
        daily_line,
        f"{_MARK.get(inv_st, '❓')} 투자자수급   {d['investor_flow'].get('ok', '')}/{d['investor_flow'].get('checked', '')}",
        f"{_MARK.get(d['flow_market']['status'], '❓')} 시장11주체   {_latest('flow_market')}",
        f"{_MARK.get(d['short_kis']['status'], '❓')} 공매도      {_latest('short_kis')}",
        f"{_MARK.get(fx['status'], '❓')} 외국인소진율  {fx['latest'] or '-'} ({fx['ok']}/{fx['checked']})",
        f"{_MARK.get(d['credit_kis']['status'], '❓')} 신용잔고    {_latest('credit_kis')}"
        + ("  T+지연 정상" if d['credit_kis']['status'] == 'PASS' else ""),
    ]
    if tk_line:
        lines.append(tk_line)
    lines.append("━━━━━━━━━━━━━━━━")

    core_ok = (
        daily_st == "PASS"
        and inv_st == "PASS"
        and d["flow_market"]["status"] == "PASS"
        and d["short_kis"]["status"] == "PASS"
        and fx["status"] in ("PASS", "PARTIAL")
        and tk_st in ("PASS", "SKIP")   # FAIL이면 판정 원천 죽음 → 초록불 금지(F-15)
    )
    if core_ok:
        # ★8/14 [F-164] 처방 ② — 판정(core_ok)은 그대로 두고 **문구만** 실상에 맞춘다.
        # PARTIAL을 정상으로 계수하는 것 자체는 의도된 설계다(네이버 소진율 부분 실패는
        # 상시이고, 그걸 실패로 치면 매일 빨간불 = [F-153] 마모). 문제는 같은 화면에
        # `⚠️`와 `✅ 모두 정상`이 동시에 남아, 마지막 줄만 읽으면 실상과 어긋난다는 것.
        # → "모두"는 진짜 전부 깨끗할 때만 쓰고, 아니면 무엇이 부분인지 병기한다(값 불변).
        caveats = []
        if fx["status"] == "PARTIAL":
            caveats.append(f"외국인소진율 부분수집 {fx['ok']}/{fx['checked']}")
        # 수급·공매도는 실측(7/30~8/14 12거래일) 활성 대비 **정확히 100.0%**라
        # 결손 자체가 이상 신호 → 소수라도 노출한다.
        for _key, _label in (("investor_flow", "수급"), ("short_kis", "공매도")):
            _m = d.get(_key, {}).get("missing") or 0
            if _m:
                caveats.append(f"{_label} 결손 {_m}종")
        # ★신용은 다르다 — 구조적으로 매일 80종 안팎이 비어 있는 것이 **정상 대역**이다
        # (실측 96.8~97.0%·8/14 기준 82종). 이걸 매일 "결손"으로 띄우면 사장님이 매일
        # 같은 경고를 보게 되고, 그게 바로 [F-153]이 말한 '사람이 경보를 끄게 만드는' 마모다.
        # (8/14 VPS 실측에서 실제로 그 문구가 나와 잡았다 — 픽스처에선 안 보였다.)
        # → 정상 대역을 벗어나 PARTIAL/FAIL 로 떨어질 때만 노출한다.
        _cs = d.get("credit_kis", {})
        if _cs.get("status") in ("PARTIAL", "FAIL"):
            caveats.append(f"신용 커버리지 {_cs.get('ok')}/{_cs.get('checked')}")
        if caveats:
            lines.append("✅ 핵심 채널 정상 — 단 " + " · ".join(caveats))
        else:
            lines.append("✅ 핵심 채널 모두 정상 — 실주문 0·페이퍼")
    else:
        bad = []
        if daily_st != "PASS":
            bad.append("종가")
        if inv_st != "PASS":
            bad.append("투자자수급")
        if d["flow_market"]["status"] != "PASS":
            bad.append("시장11주체")
        if d["short_kis"]["status"] != "PASS":
            bad.append("공매도")
        if fx["status"] == "STALE":
            bad.append("외국인소진율")
        if tk_st == "FAIL":
            bad.append("장중틱(판정원천)")
        lines.append(f"⚠️ 확인 필요: {', '.join(bad) or '세부 채널'}")

    lines.append("🤖 자동발송 · 터미널 불필요")
    return "\n".join(lines)


def _morning_ops_heartbeat(today: str):
    """아침 점검(08:30·F-29)이 오늘 실제로 돌았는지 1행 — '감시자를 감시하는' 계층.

    ★ 7/31 Tier1 H-2b: 아침 점검을 무인화해도 그 cron이 지워지거나 프로세스가 죽으면
    다시 아무도 모른다(F-29가 한 계층 위에서 재현). 20:10은 이미 검증된 발송 통로이므로
    여기서 아침분 스탬프를 대조해 병기한다 — A3(아침이 저녁을 감시)의 대칭.

    로그 파일 자체가 없으면 아직 배선 전이므로 무표기(None). 파일은 있는데 오늘 스탬프가
    없으면 = cron 제거/프로세스 사망 → 경보."""
    path = BASE_DIR.parent / "logs" / "daily_ops_check.log"
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-400:]
    except Exception:
        return None                      # 미배선 — 오알림 금지
    stamp = f"[ops] === {today}"
    ran = [ln for ln in lines if stamp in ln]
    if not ran:
        return "🚨 아침점검(08:30) 미실행 — cron/프로세스 확인"
    # ★8/6 Tier1 HIGH — 8/5에 아침점검 성공 마커가 `[ops][SEND_OK]`로 리네임됐는데
    # 이 줄만 옛 문구("발송 완료")를 보고 있어 성공한 날에도 매일 ⚠️가 나갈 뻔했다.
    # 성공·실패 어느 쪽도 옛 문구를 더는 안 쓰므로 이 감시 계층 자체가 죽은 상태였다.
    # 마커는 daily_ops_check 상수가 단일진실 — 파일 경계 밖에서도 위임받는다.
    try:
        from daily_ops_check import MARK_SENT, _LEGACY_SENT
    except Exception:  # noqa: BLE001 — 임포트 실패가 20:10 발송을 죽여선 안 된다
        MARK_SENT, _LEGACY_SENT = "[ops][SEND_OK]", "텔레그램 발송 완료"
    if any(MARK_SENT in ln or _LEGACY_SENT in ln for ln in ran):
        return "✅ 아침점검(08:30) 실행·발송"
    return "⚠️ 아침점검(08:30) 실행됐으나 발송 미확인"


def _load_config():
    import yaml
    if not CONFIG_PATH.exists():
        return None
    try:
        return yaml.safe_load(CONFIG_PATH.read_text("utf-8"))
    except Exception as e:  # pragma: no cover
        print(f"[notify] config.yaml 로드 실패: {e}", file=sys.stderr)
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="발송 없이 stdout만")
    ap.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘)")
    ap.add_argument("--force", action="store_true", help="휴장일 가드 무시(검수용)")
    args = ap.parse_args()

    today = args.date or date.today().isoformat()

    # ★ 7/31 F-4 — 실행 스탬프. 이 로그엔 시각이 없어 아침 A3 확인이 tail 순서에
    # 의존했다(기계 대조 불가). 아침 점검 무인화(daily_ops_check A3)가 '기준 거래일의
    # 실행분'을 정확히 집어 판정하려면 실행마다 날짜 스탬프가 있어야 한다.
    print(f"[notify] === {datetime.now():%Y-%m-%d %H:%M:%S} 실행 (기준 {today}) ===")

    # 휴장일 가드 — 정규 실행에서만 (검수 --date/--force는 예외)
    if not args.date and not args.force:
        if not is_trading_day(date.fromisoformat(today)):
            print(f"[notify] {today} 휴장일 — 미발송")
            return 0

    # ★ 7/30 M-1 — 소급(--date)·dry-run은 상태 파일을 쓰지 않는다(전일자 되돌려쓰기 차단).
    # 정규 20:10 실행(당일·발송)만 data_verify_result.json을 갱신.
    _save = not (args.dry_run or args.date)
    result = DataVerifier(today, save_result=_save).verify_all()
    msg = build_summary(result, today)

    # 아침 점검 하트비트는 요약 본문(build_summary) 밖에서 붙인다 — build_summary는
    # 아침 점검 A1도 소급 호출하는 공용 함수라 오염시키지 않는다.
    if not args.date:                    # 소급 검수 실행엔 무의미(그날의 아침이 아님)
        hb = _morning_ops_heartbeat(today)
        if hb:
            msg += f"\n{hb}"

    print(msg)

    if args.dry_run:
        print("\n[dry-run] 발송 생략")
        return 0

    config = _load_config()
    tg = TelegramAlert(config)
    if not tg.enabled:
        print("[notify] 텔레그램 미설정(토큰 없음) — 발송 스킵", file=sys.stderr)
        return 1
    if tg.send_report(msg):
        print("\n[notify] 텔레그램 발송 완료")
        return 0
    print("[notify] 텔레그램 발송 실패 — 사장님 미수신 가능(토큰/네트워크 확인)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
