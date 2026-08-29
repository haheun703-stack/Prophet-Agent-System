# -*- coding: utf-8 -*-
"""전략 데드라인 체커 — 검증→판정→폐기/교체 사이클 강제 (7/17 사장님 승인).

배경: 사장님 7/17 "매일 뭔가 하지만 스스로 진화가 없다" → "전략에 데드라인 = 진짜
진화 → 진행하자". 관측 전략이 판정 없이 무기한 도는 것을 코드로 차단한다.

동작:
  1) data/strategy_deadlines.json(기계 단일진실) 로드
  2) 활성 전략별 D-day 계산 + metric 있으면 실데이터에서 기준 자동 평가
     (v2_paper_cum_net = ⑲-3 장부 순누적 / cluster_sum_ret = ⑯ summary)
  3) 스코어보드(페이퍼 누적 수익금·승률) 첫 줄 출력 — "보고 첫 줄 = 숫자" 약속
  4) --notify: D-3 이내·기한 초과 전략이 있을 때만 텔레그램 (스팸 방지)

★ 자동 폐기 없음 — 이 도구는 판정을 '조르는' 알림·평가기. 폐기/연장 결정=사장님.
★ 안전: read-only(장부·대장 읽기만)·실주문 0·매수/매도/SAJANG/picks 무접촉.
  전 예외 graceful exit 0 (nightly 차단 금지).

실행: python tools/strategy_deadline_check.py            # 아침 루틴 A9 (stdout)
      python tools/strategy_deadline_check.py --notify   # nightly ⑲-4 (알림 조건부)
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
if str(BASE / "tools") not in sys.path:
    sys.path.insert(0, str(BASE / "tools"))   # observe_v2_paper 단일진실 위임용(F-17)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

DS = BASE / "data_store"
REGISTRY = BASE / "data" / "strategy_deadlines.json"
ALERT_DDAY = 3   # D-3부터 알림


def _v2_paper_cum_net():
    """⑲-3 페이퍼 장부 ★유효일 × 일일 상한★ 순누적 %p + 승률 (스코어보드·S-1 겸용).

    7/21 F-17 fix: observe_v2_paper._cum 단일진실 위임 → valid_*(BROKEN·TRUNCATED·
    미정산일 제외)을 씀. 기존엔 raw all-days 합이라 페이퍼 리포트("유효 기준")와
    S-1 판정 숫자가 어긋났다. 이제 두 도구가 같은 valid 숫자로 S-1을 판정한다.

    ★7/23 사장님 확정: 판정 기준 = valid_cap_*(일일 상한 CAP_VIEW_N=5건). 전 신호
    합산(valid_sum)은 사장님 영구 자금 룰(30% 현금보유·split_cash top_k 3+2)상
    실행 불가능한 수치라 참고로 강등 — 7/22 실측에서 두 지표의 부호가 반대였다
    (무제한 -244.15%p vs 상한 +3.34%p). 지표 선택이 곧 판정이므로 실행 가능한 쪽."""
    from observe_v2_paper import _load_ledger, _cum
    c = _cum(_load_ledger())
    return c["valid_cap_sum"], c["valid_cap_n"], c["valid_cap_win_rate"]


def _cluster_sum_ret():
    c = json.loads((DS / "cluster_harvest_paper.json").read_text(encoding="utf-8"))
    return c.get("summary", {}).get("sum_ret")


def _eval_metric(metric: str):
    """metric 이름 → 현재값. 미지원/실패/무표본 시 None(수동 판정).

    ★7/27 검수 M2: 무표본(ticks 수집장애로 유효일 0)이면 _v2_paper_cum_net의
    valid_cap_sum=0.0인데, 그대로 돌리면 _pass(0.0, ">", 0)=False → 데드라인 행이
    S-1을 "현재 +0.00 ❌미달"로 출력해 '데이터 없음'을 '전략 실패'로 위장한다
    (H-1 = 증거 부재 → 조용한 0 → ❌미달). observe_v2_paper.build_report는
    valid_cap_n < MIN_JUDGE_N이면 '판정 유예'로 표기하는데(observe_v2_paper.py),
    데드라인 체커만 그 가드가 없어 두 표면이 어긋났다. 여기서 표본 부족을 None으로
    흘려 '(평가 불가 — 수동 판정)'으로 일치시킨다."""
    try:
        if metric == "v2_paper_cum_net":
            from observe_v2_paper import MIN_JUDGE_N
            cum, n, _wr = _v2_paper_cum_net()
            return None if n < MIN_JUDGE_N else cum
        if metric == "cluster_sum_ret":
            return _cluster_sum_ret()
    except Exception:  # noqa: BLE001
        return None
    return None


def _pass(value, op: str, threshold):
    """True/False = 판정. None = 미지원 op(평가 불가 — 오표기 방지·Tier1 L2)."""
    if op == ">":
        return value > threshold
    if op == ">=":
        return value >= threshold
    if op == "<":
        return value < threshold
    if op == "<=":
        return value <= threshold
    return None


def build_report() -> tuple:
    """(lines, alerts) 생성 — 스코어보드 + 데드라인 대장 행.

    ★7/31 F-29: 아침 점검 무인화(`daily_ops_check.py`)가 A9를 같은 숫자로 보고해야
    하는데 로직이 main() 안에 있어 재사용 불가였다. 클론을 만들면 [F-37]
    (판정 상수 로컬 복제) 재발이므로 함수로 분리해 단일진실을 공유한다.
    main()의 동작·출력은 무변경."""
    today = date.today()

    # ── 스코어보드 (보고 첫 줄 = 숫자 — 7/17 약속) ──
    try:
        # 단일진실(장부와 동일 명목·상한·Tier1 L1) — 하드코딩 이중정의 금지
        from observe_v2_paper import NOTIONAL_KRW, CAP_VIEW_N
    except Exception:  # noqa: BLE001
        NOTIONAL_KRW, CAP_VIEW_N = 300_000, 5
    try:
        cum, n, wr = _v2_paper_cum_net()   # ★유효일 × 일일 상한(ticks OK만·F-17·7/23 확정)
        # ★ 7/31 전체검수 M-2: 표본 부족 가드가 _eval_metric에만 있고 스코어보드엔 없었다.
        # _load_ledger는 파싱 실패를 삼키고 {"days":{}}를 주므로(예외 아님) 장부가 손상되면
        # 이 줄이 "0건 승률 0% 순누적 +0.00%p"가 되어 **증거 부재가 +0으로 위장**된다.
        # 이 줄은 ⑲-4 텔레그램과 아침 점검 A9의 첫 줄이다 — 첫 줄이 거짓이면 전부 거짓이다.
        try:
            from observe_v2_paper import MIN_JUDGE_N as _MINN
        except Exception:  # noqa: BLE001
            _MINN = 10
        if n < _MINN:
            score = (f"📊 페이퍼 스코어보드: 표본 {n}건 < {_MINN}건 — "
                     f"판정 유예(장부 손상/수집 장애 확인 필요)")
        else:
            krw = int(cum / 100 * NOTIONAL_KRW)
            score = (f"📊 페이퍼 스코어보드(유효일·상한{CAP_VIEW_N}건/일): {n}건 승률 {wr or 0}% "
                     f"순누적 {cum:+.2f}%p ({krw:+,}원/30만·건)")
    except Exception:  # noqa: BLE001
        score = "📊 페이퍼 스코어보드: 장부 없음"

    # ★8/29 — 이 스코어보드는 **폐기된 [S-1]**의 장부다.
    #   8/21에 [S-1]을 폐기하면서 신호를 선정 피처로 보존하려고 러너·⑲-3 장부를 유지했는데,
    #   그 부작용으로 **폐기된 전략의 손실이 계속 누적돼 매일 첫 줄로 발송**되고 있었다
    #   (8/21 121건 -67.93%p → 8/29 148건 -85.91%p). 사장님이 매일 보시는 숫자가
    #   '판정 대상이 아닌 것'이면 그건 오보다. 라벨을 붙여 사실대로 말한다.
    #   ★값·집계는 무변경 — 문구만([F-164] 계열의 '보이게 하되 동작 불변').
    if score.startswith("📊 페이퍼 스코어보드(유효일"):
        score = score.replace("📊 페이퍼 스코어보드", "📉 [S-1 폐기 8/21·판정 대상 아님] 페이퍼")

    # ★8/29 — 현재 판정 대상([S-8])의 관측치를 **첫 줄에** 올린다.
    #   8/21 등재 때 "사후 재구성 가능하니 관측 인프라 추가 0"으로 판단했는데, 그 결과
    #   8일간 아무 기록도 남지 않았다. "나중에 계산할 수 있다"와 "지금 보고 있다"는 다르다.
    #   새 cron·새 스텝 없이 이 체커(이미 nightly ⑲-4 + 08:30 등록)가 계산한다(실측 3.5초).
    #   graceful — 실패해도 None을 받아 문구로 표면화하고 기존 동작은 불변.
    try:
        from s8_observe import observe as _s8_observe, summary_line as _s8_line
        s8_line = _s8_line(_s8_observe())
    except Exception:  # noqa: BLE001
        s8_line = "🔭 [S-8] 관측: 모듈 로드 실패 — 수동 확인 필요"

    lines = [s8_line, score, "⏰ 전략 데드라인 대장"]
    alerts = []
    if "실패" in s8_line:
        alerts.append(s8_line)

    # ★8/5 전체검수 [F-108] — **판정 증거 손상이 alerts로 승격되지 않았다.**
    #   위에서 "판정 유예(장부 손상/수집 장애 확인 필요)" 문구를 이미 만들어 놓고
    #   `lines`에만 넣었다. 그런데 ⑲-4 발송은 `if args.notify and alerts:`이고
    #   아침 점검 A9(`daily_ops_check.check_a9_deadlines`)는 `alerts` 유무만 본다
    #   → **텔레그램 0건 + 아침 초록불**. 바로 아래 '대장 로드 실패'는 같은 계열인데
    #   `alerts.append`를 하고 있어 **처리가 한쪽만 돼 있던 비대칭**이다.
    #   ★위험 구간: 8/7에 S-6가 정리되면 D-3 이내 전략이 없어 alerts=0이 되고,
    #     8/8~8/25 약 18일간 장부가 깨져도 아무도 모른다.
    if score.startswith("📊") and ("판정 유예" in score or "장부 없음" in score):
        alerts.append(f"🚨 {score} — 판정 근거 소실. 스코어보드가 0/+0으로 위장될 수 있다")

    # 대장 로드 실패 = '조르기' 정지 상태 — silent이면 존재 이유 무력화 → 알림으로 표면화(Tier1 Med)
    try:
        reg = json.loads(REGISTRY.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        row = f"⚠ 대장 로드 실패({e}) — 데드라인 조르기 정지 상태·즉시 복구 필요"
        lines.append(row)
        alerts.append(row)
        reg = {}

    active_n = 0
    for s in reg.get("strategies", []):
        # 엔트리별 격리 — 나쁜 1행이 전체 평가/알림을 무산시키지 않게(Tier1 Med)
        try:
            if s.get("status") != "active":
                continue
            active_n += 1
            dleft = (date.fromisoformat(s["deadline"]) - today).days
            dtag = f"D-{dleft}" if dleft > 0 else ("D-DAY" if dleft == 0 else f"⚠기한초과 {-dleft}일")
            row = f"[{s['id']}] {s['name']} — {dtag} ({s['deadline']})"
            if s.get("metric"):
                v = _eval_metric(s["metric"])
                ok = _pass(v, s.get("op", ">"), s.get("threshold", 0)) if v is not None else None
                if v is not None and ok is not None:
                    row += f" · 기준 {s.get('op', '>')}{s.get('threshold', 0)} 현재 {v:+.2f} {'✅충족' if ok else '❌미달'}"
                else:
                    row += " · (평가 불가 — 수동 판정)"
            else:
                row += " · 수동 판정"
            lines.append(row)
            if dleft <= ALERT_DDAY:
                alerts.append(row)

            # ★8/21 [F-179] 벤치마크 강제 — 절대수익 기준은 "시장을 이겼는가"를 못 잰다.
            #   [S-1]은 기준이 `>0`이었는데 같은 기간 시장 동일가중이 T+3 +0.37%p·T+5 +0.98%p였다.
            #   즉 **아무거나 사도 플러스인 구간에서 `>0`을 통과선으로 삼았고**, 명분 축은
            #   시장 대비 -6.05%p(t -6.33)로 가치를 파괴하는데 절대수익만 보면 '그냥 마이너스'로
            #   보였다. 규약(사장님 8/21 승인) = 신규 등재는 `benchmark` 필드 의무.
            #   ★자동 폐기·자동 실패 처리는 하지 않는다(이 도구의 불변식) — 표면화만 한다.
            if not s.get("benchmark"):
                brow = (f"[{s['id']}] ⚠ benchmark 미지정 — 절대수익 기준은 시장을 이겼는지 "
                        f"못 잰다([F-179] 8/21 사장님 승인 규약)")
                lines.append(brow)
                alerts.append(brow)
        except Exception as e:  # noqa: BLE001
            row = f"[{s.get('id', '?')}] ⚠ 엔트리 평가 실패({e}) — 대장 수동 확인"
            lines.append(row)
            alerts.append(row)

    # ★8/21 — 활성 전략 0건은 '이상 없음'이 아니라 **조르기가 멈춘 상태**다.
    #   이 파일 위쪽 주석이 이미 경고하던 위험구간("8/7에 S-6가 정리되면 alerts=0이 되고
    #   장부가 깨져도 아무도 모른다")이 8/21 [S-1] 폐기로 **실현**됐다: 체커 출력이
    #   스코어보드 + "⏰ 전략 데드라인 대장" 헤더뿐이고 alerts=0 → 텔레그램 미발송.
    #   7/17 사장님이 대장을 만든 이유가 '판정 없이 무기한 도는 것'의 차단인데,
    #   **전략이 하나도 없는 상태**는 그 반대편의 같은 실패(진화 정지)다.
    #   ★경보 마모 방지([F-153] 교훈) — 매일 같은 문구를 짖지 않도록 **경과일을 함께** 낸다.
    #     상태 파일은 쓰지 않는다('상태 파일 무접촉' 불변식·8/5) — 대장의 reported 최댓값에서 역산.
    if reg and active_n == 0:
        try:
            reported = [s["reported"] for s in reg.get("strategies", []) if s.get("reported")]
            since = (today - date.fromisoformat(max(reported))).days if reported else None
        except Exception:  # noqa: BLE001
            since = None
        gap = f"{since}일째" if since is not None else "기간 불명"
        row = (f"⚠ 활성 전략 0건 ({gap}) — 데드라인 조르기 정지. "
               f"탐색 중이면 [T-1] 진행 상황을, 아니면 대장 복구를 확인할 것")
        lines.append(row)
        alerts.append(row)

    if alerts:
        lines.append(f"→ 판정 보고 의무 {len(alerts)}건 — 폐기/연장 결정은 사장님 (자동 폐기 없음)")
    return lines, alerts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--notify", action="store_true", help="D-3 이내/초과 시 텔레그램")
    args = ap.parse_args()

    lines, alerts = build_report()
    out = "\n".join(lines)
    print(out)

    if args.notify and alerts:
        if sys.platform == "win32":   # 노트북=분석 전용 — 오알림 금지
            print("[deadline] win32 — 알림 생략")
            return 0
        try:
            from verifiers._common import send_telegram
            if not send_telegram(out):
                print("[deadline] 텔레그램 발송 실패 — 사장님 미수신 가능(토큰/네트워크 확인)")
        except Exception as e:  # noqa: BLE001
            print(f"[deadline] 텔레그램 실패(체크는 완료): {e}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 — record-only 도구: nightly 차단 금지
        print(f"[deadline] 치명 예외(관측 도구 — 무시): {e}")
        sys.exit(0)
