# -*- coding: utf-8 -*-
"""재진입 룰 shadow 시뮬레이션 (6/18 설계 구현, shadow-first·read-only).

설계: docs/02-design/reentry_rule_5_31.md
근거(6/18 중간점검): early shadow would_stop 93~100% → "손절 후 재진입하면 회복되나?"가
  6/30 flip 결정의 빈칸. 이 모듈이 우리 데이터로 "손절+재진입 vs 손절만 vs 홀딩" 3자 비교.

★★★ 안전 불변식 (기존 shadow와 동일) ★★★
- read-only: 일봉 csv만(네트워크 0). 주문함수·picks·recommendation.json·SAJANG 매도헬퍼·order_intent 0 접촉.
- record-only: 시뮬레이션 결과 json만 기록. 매수/매도 행동 0. ON/OFF 플래그 없음(생성기는 항상 돌되 관측만).
- 재진입 파라미터는 SAJANG 단일진실(should_reenter/effective_hold_days/TRAILING_PCT)만 참조 — 리터럴 0.97 등 금지.
- 후보 = early_variant_shadow.json 레코드 재사용(선정 단일진실). 이 모듈은 "선정"이 아니라 "결과 시뮬"만.
- 관측 없이 flip/라이브 금지 (5/31·5/26 사고 교훈). 검증 후 사장님 승인 시에만 라이브 매수경로 연결(별도, paper).

★ 한계(정직):
  - thesis 명분유효(invalidation_kind)는 4단 Stage3 신호 미배선 → shadow에선 thesis_ok=True 가정.
    실제 라이브는 명분붕괴 시 재진입 차단 추가 필요(설계 §2.3).
  - 손절 체결가 = 손절선 가격(일봉 근사). 갭다운 시 실제는 더 나쁨 → §5.1 분봉정밀 숙제.
  - 절대수익은 생존편향(상폐 미포함) → strict vs early 상대비교만 신뢰(5/31 교훈).

계산 단일진실(재사용):
- 일봉행: data.sector_reversal_shadow._daily_rows ([(date, close, high, low)], early shadow와 동일)
- 손절/재진입 룰: data.sajang_rules.SAJANG (should_reenter, effective_hold_days, TRAILING_PCT)
- 후보 소스: data_store/early_variant_shadow.json (build_early_shadow 산출)

실행(러너): tools/run_reentry_shadow_daily.py
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from data.sajang_rules import SAJANG
from data.limit_up_scanner import DAILY_DIR
from data.sector_reversal_shadow import _daily_rows

logger = logging.getLogger("BH.ReentryShadow")

EARLY_SHADOW = DAILY_DIR.parent / "early_variant_shadow.json"
SHADOW_OUT = DAILY_DIR.parent / "reentry_shadow.json"

HOLD_BENCH_OFFSETS = (3, 5)   # 홀딩 벤치마크(buy&hold) 비교용


# ───────────────────────── 시뮬레이션 (순수함수, read-only) ─────────────────────────
def _leg_stop_line(cur_entry: float, peak: float) -> int:
    """현재 leg의 손절선 (SAJANG 단일진실, 리터럴 0.97 금지).

    +3% 도달(고점 기준) 시 고점 트레일링(get_trailing_sl=peak-3%), 그 전엔 진입가 -3% 안전망(get_normal_sl).
    """
    gain_at_peak = (peak / cur_entry - 1) * 100.0 if cur_entry > 0 else 0.0
    if SAJANG.is_trailing_activated(gain_at_peak):
        return SAJANG.get_trailing_sl(peak)       # 고점 -3% (이익 락인)
    return SAJANG.get_normal_sl(cur_entry)         # 진입가 -3% (안전망)


def _simulate(rows, di: int, kki_grade: str, thesis_ok: bool = True):
    """손절+재진입 시퀀스 시뮬레이션 (SAJANG 단일진실 = 실제 매도룰 충실 모델).

    rows: [(date, close, high, low), ...] (거래일순). di: 진입(T0) 인덱스.
    반환: dict(stop_reenter_ret, stop_only_ret, n_reentries, n_legs, leg_reasons) | None(bar 부족=미해결).

    손절 모델(SAJANG): 고점 watermark 추적 → +3% 활성 시 고점-3% 트레일(이익 락인) / 활성 전 진입가-3%.
      손절 체결가 = 그 손절선(일봉 근사·갭다운 시 낙관적, §5.1 분봉정밀 숙제).
    재진입(설계 §2.1): reclaim(종가 ≥ 직전 손절선) AND should_reenter → 그 종가에 풀비중 재진입.
      최대 REENTRY_MAX회 / 윈도우=최초진입 후 effective_hold_days / 미손절 시 deadline 종가 청산.
    각 leg 순차(풀 비중) → 수익률 복리 합성.
    """
    n = len(rows)
    if di < 0 or di >= n:
        return None
    entry0 = rows[di][1]
    if entry0 <= 0:
        return None

    legs = []           # (entry, exit, ret_pct, reason)
    re_count = 0
    cur_idx = di
    cur_entry = entry0

    while True:
        deadline = SAJANG.effective_hold_days(kki_grade)   # cur_idx 기준 offset
        peak = rows[cur_idx][2]                             # 고점 watermark (진입일 고가부터)

        # 손절 도달 or deadline 까지 전진 (peak 트레일링)
        stop_idx = None
        stop_price = None
        j = cur_idx + 1
        end = cur_idx + deadline
        while j <= end:
            if j >= n:
                return None                  # bar 부족 → 미해결(나중 충전)
            sl = _leg_stop_line(cur_entry, peak)   # 직전 bar 까지의 peak 기준 (causal)
            if rows[j][3] <= sl:                    # 저가 ≤ 손절선
                stop_idx = j
                # 체결가: 손절선. 단 갭다운으로 당일 고가가 손절선보다 낮으면 그 위로 못 팜 → 고가로 클램프(낙관편향 차단)
                stop_price = min(sl, rows[j][2])
                break
            peak = max(peak, rows[j][2])            # 당일 고가로 watermark 갱신
            j += 1

        if stop_idx is None:
            # 미손절 → deadline 종가 청산
            if end >= n:
                return None                  # deadline bar 아직 없음 → 미해결
            exit_price = rows[end][1]
            legs.append((cur_entry, exit_price, (exit_price / cur_entry - 1) * 100.0, "HOLD_EXIT"))
            break

        # 손절 체결 (트레일선/안전망 가격 — 진입가 대비 ±)
        legs.append((cur_entry, stop_price, (stop_price / cur_entry - 1) * 100.0, "STOP"))
        if re_count >= SAJANG.REENTRY_MAX:
            break                            # 재진입 한도 소진 → 종료(해결)

        # reclaim 재진입 탐색 (stop_idx 종가부터, 윈도우=최초진입 후 effective_hold_days)
        reentered = False
        unresolved = False
        r = stop_idx
        while True:
            if r >= n:
                unresolved = True            # 윈도우 내 bar 소진 → 미래 reclaim 가능 → 미해결
                break
            day_offset = r - di
            if day_offset > SAJANG.effective_hold_days(kki_grade):
                break                        # 윈도우 종료 → 재진입 없음(해결)
            if SAJANG.should_reenter(rows[r][1], stop_price, kki_grade, re_count, day_offset, thesis_ok):
                re_count += 1
                cur_idx = r
                cur_entry = rows[r][1]
                reentered = True
                break
            r += 1

        if unresolved:
            return None
        if not reentered:
            break                            # 윈도우 내 reclaim 없음 → 종료(해결)
        # reentered → 새 leg 루프 계속

    comp = 1.0
    for (_, _, ret, _) in legs:
        comp *= (1 + ret / 100.0)
    return {
        "stop_reenter_ret": round((comp - 1) * 100.0, 2),
        "stop_only_ret": round(legs[0][2], 2),     # 첫 leg = 재진입 안 했을 때
        "n_reentries": re_count,
        "n_legs": len(legs),
        "leg_reasons": [lg[3] for lg in legs],
    }


def _hold_returns(rows, di: int):
    """buy&hold 벤치마크 (D+3/D+5 종가 수익률). bar 없으면 None."""
    out = {}
    bc = rows[di][1] if 0 <= di < len(rows) else 0
    for off in HOLD_BENCH_OFFSETS:
        if bc > 0 and di + off < len(rows):
            out[off] = round((rows[di + off][1] / bc - 1) * 100.0, 2)
        else:
            out[off] = None
    return out


# ───────────────────────── 적재 (멱등, record-only) ─────────────────────────
class ShadowLoadError(Exception):
    """장부 파싱 실패 — 덮어쓰기 금지 신호 (7/30 검수 H-1)."""


def _load() -> dict:
    # ★ 7/30 H-1 — 파싱 실패 삼키기 제거(124KB 관측 이력 전량 소실 통로) + 원자쓰기
    if SHADOW_OUT.exists():
        try:
            return json.loads(SHADOW_OUT.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            raise ShadowLoadError(
                f"{SHADOW_OUT.name} 파싱 실패 — 덮어쓰기 중단(관측 이력 보호): {e}"
            ) from e
    return {"records": [], "note": "재진입 룰 shadow 시뮬레이션(관측 전용, 매수/매도 무접촉)"}


def _save(data: dict) -> None:
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tmp = SHADOW_OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SHADOW_OUT)


def _load_early_records() -> list:
    if not EARLY_SHADOW.exists():
        return []
    try:
        return json.loads(EARLY_SHADOW.read_text(encoding="utf-8")).get("records", [])
    except Exception:
        return []


def build_reentry_shadow(save: bool = True) -> dict:
    """early_variant_shadow 후보별 손절+재진입 시뮬 → reentry_shadow.json 멱등 적재.

    멱등: (date, code, variant) 키. 이미 resolved(stop_reenter_ret 채워짐)면 skip.
    미해결(bar 부족)은 None 저장 안 함 → 다음 실행이 bar 생기면 충전.
    반환: 요약 dict.
    """
    early = _load_early_records()
    if not early:
        logger.info("[reentry/shadow] early_variant_shadow.json 후보 없음 — skip")
        return {"built": 0, "resolved": 0, "pending": 0}

    data = _load()
    idx = {(r["date"], r["code"], r["variant"]): k for k, r in enumerate(data["records"])}

    resolved = pending = built = 0
    for rec in early:
        key = (rec.get("date"), rec.get("code"), rec.get("variant"))
        if None in key:
            continue
        # 이미 해결된 레코드 skip (멱등)
        if key in idx and data["records"][idx[key]].get("stop_reenter_ret") is not None:
            continue

        code = rec["code"]
        grade = rec.get("kki_grade", "SLUGGISH")
        rows = _daily_rows(code)
        if not rows:
            continue
        di = next((k for k, r in enumerate(rows) if r[0] == rec["date"]), None)
        if di is None:
            continue

        sim = _simulate(rows, di, grade, thesis_ok=True)
        hold = _hold_returns(rows, di)

        if sim is None:
            pending += 1
            continue   # 미해결: None 저장 안 함(다음 실행 충전)

        out = {
            "date": rec["date"], "code": code, "name": rec.get("name", code),
            "variant": rec["variant"], "kki_grade": grade,
            "pos20": rec.get("pos20"), "volume_ratio": rec.get("volume_ratio"),
            "would_stop_day": rec.get("would_stop_day"),
            **sim,
            "hold_d3": hold.get(3), "hold_d5": hold.get(5),
            "reentry_lift": (round(sim["stop_reenter_ret"] - sim["stop_only_ret"], 2)),
            "vs_hold_d3": (round(sim["stop_reenter_ret"] - hold[3], 2) if hold.get(3) is not None else None),
        }
        if key in idx:
            data["records"][idx[key]] = out
        else:
            data["records"].append(out)
            idx[key] = len(data["records"]) - 1
        resolved += 1
        built += 1

    if save and built:
        _save(data)
    logger.info(f"[reentry/shadow] resolved {resolved} / pending(bar부족) {pending} -> {SHADOW_OUT.name}")
    return {"built": built, "resolved": resolved, "pending": pending}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    res = build_reentry_shadow()
    print(f"\n[reentry shadow] resolved {res['resolved']} / pending {res['pending']}")
    print("★ shadow: 손절+재진입 vs 손절만 vs 홀딩 3자 관측용. 자동매수/매도 X. 관측 없이 라이브 금지.")
