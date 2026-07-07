# -*- coding: utf-8 -*-
"""명분(catalyst) 라벨 → early/reentry shadow 결합 + MIN_KKI 병렬 코호트 요약 (7/7 사장님 지시).

배경: 7/7 재진입 forward 숙제#1(비용) 통과 후 사장님 지시 —
  "조금의 위험 감수해도 오를 근거 있는 종목 순, 잡주 X."
  ① catalyst_archive(nightly ⑪ 산출) 명분등급을 shadow 레코드에 additive 부착(forward 축적)
  ② REENTRY_MIN_KKI_GRADE 상향(현행→HUNTABLE) 병렬 코호트 요약(룰 변경 0·관측만).
  근거: docs/03-analysis/reentry_forward_cost_quality_7_7.md (명분 겹침 3건=판정불가 → 배선 필요).

★★★ 안전 불변식 ★★★
- record-only·additive: 기존 필드/레코드 무수정 — meongbun_grade 필드와 summary 블록만 추가.
- read: catalyst_archive/*.json. write: early_variant_shadow.json / reentry_shadow.json (라벨·요약만).
- 매수/매도/picks/recommendation/SAJANG 값/order_intent 0 접촉. SAJANG은 등급 서열·현행 최소등급 참조만.
- 멱등: 이미 라벨된 레코드 skip. 아카이브 없는 날짜는 미라벨 유지(다음 실행 충전).
- 관측 없이 flip 금지 — MIN_KKI 상향은 이 요약 2주+ 관측 후 사장님 결정.

nightly ⑪-2 (⑪ catalyst 이후 실행 필수 — 당일 아카이브 생성 뒤 라벨).
실행: python tools/run_meongbun_label_daily.py
"""
import json
import sys
from pathlib import Path
from statistics import mean, median

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

# Windows cp949 콘솔/리다이렉트에서 한글 출력 크래시 방지
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.sajang_rules import SAJANG  # noqa: E402 — 등급 서열·현행 최소등급 참조만(값 무변경)

STORE = BASE / "data_store"
ARCHIVE_DIR = STORE / "catalyst_archive"
EARLY = STORE / "early_variant_shadow.json"
REENTRY = STORE / "reentry_shadow.json"

# 분석 고정 상수(7/7 문서 판정점) — 거래룰 아님(SAJANG 대상 외), 왕복비용 %/leg
COST_PER_LEG = 0.5
# 등급 서열: SAJANG 단일진실 참조(폴백은 동일값 — drift 시 SAJANG 우선)
_GRADE_RANK = getattr(SAJANG, "_KKI_GRADE_RANK",
                      {"SLUGGISH": 0, "MODERATE": 1, "HUNTABLE": 2, "EXPLOSIVE": 3})
CANDIDATE_MIN = "HUNTABLE"   # 7/7 상향 후보(관측용) — 현행은 SAJANG.REENTRY_MIN_KKI_GRADE


def _atomic_write(path: Path, data: dict) -> None:
    """tmp+replace 원자 쓰기 (catalyst_scanner 패턴 — 쓰기 도중 종료로 인한 장부 손상 방지)."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _load_archive() -> dict:
    """{date: {code: 명분등급}} — items=강/중/약/무명분, next_candidates=선행후보."""
    arch = {}
    if not ARCHIVE_DIR.exists():
        return arch
    for f in sorted(ARCHIVE_DIR.glob("catalyst_*.json")):
        day = f.stem.replace("catalyst_", "")
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        codes = {}
        for it in d.get("items", []) or []:
            c = it.get("code")
            if c:
                codes[c] = it.get("meongbun_grade") or "무명분"
        for it in d.get("next_candidates", []) or []:
            c = it.get("code") if isinstance(it, dict) else None
            if c and c not in codes:
                codes[c] = "선행후보"
        arch[day] = codes
    return arch


def _label_records(recs: list, arch: dict) -> int:
    """meongbun_grade additive 부착(멱등). 아카이브 있는 날 미수록='비수록'(명분끼 후보 아니었음)."""
    labeled = 0
    for r in recs:
        if r.get("meongbun_grade") is not None:
            continue
        day, code = r.get("date"), r.get("code")
        if not day or not code or day not in arch:
            continue   # 아카이브 없는 날짜 — 미라벨 유지(라벨 불가=unknown)
        r["meongbun_grade"] = arch[day].get(code, "비수록")
        labeled += 1
    return labeled


def _stats(rows: list) -> dict:
    """비용 차감 net/lift 요약 (7/7 분석과 동일 산식)."""
    if not rows:
        return {"n": 0}
    net = [r["stop_reenter_ret"] - COST_PER_LEG * r.get("n_legs", 1) for r in rows]
    lift = [r["reentry_lift"] - COST_PER_LEG * (r.get("n_legs", 1) - 1) for r in rows]
    return {
        "n": len(rows),
        "reentered": sum(1 for r in rows if r.get("n_reentries", 0) > 0),
        "net_avg": round(mean(net), 2),
        "net_med": round(median(net), 2),
        "net_win_pct": round(100 * sum(1 for x in net if x > 0) / len(net), 1),
        "lift_avg": round(mean(lift), 2),
    }


def _summaries(data: dict) -> None:
    """reentry json에 MIN_KKI 병렬 코호트 + 명분등급별 요약 블록(additive) 재계산."""
    rows = [r for r in data.get("records", []) if r.get("stop_reenter_ret") is not None]

    def cohort(min_grade: str) -> list:
        mr = _GRADE_RANK.get(min_grade, 1)
        return [r for r in rows
                if _GRADE_RANK.get(r.get("kki_grade", "SLUGGISH"), 0) >= mr]

    cur = SAJANG.REENTRY_MIN_KKI_GRADE   # 현행 룰(단일진실 참조·무변경)
    data["summary_min_kki"] = {
        "cost_per_leg": COST_PER_LEG,
        "current": {"min_grade": cur, **_stats(cohort(cur))},
        "candidate": {"min_grade": CANDIDATE_MIN, **_stats(cohort(CANDIDATE_MIN))},
        "note": "병렬 관측용(7/7) — 현행 룰 무변경(SAJANG). 상향 flip=2주+ 관측 후 사장님 결정.",
    }

    by = {}
    for r in rows:
        g = r.get("meongbun_grade")
        if g is not None:
            by.setdefault(g, []).append(r)
    data["summary_meongbun"] = {
        "cost_per_leg": COST_PER_LEG,
        "by_grade": {g: _stats(v) for g, v in sorted(by.items())},
        "unlabeled": sum(1 for r in rows if r.get("meongbun_grade") is None),
        "note": "명분등급별 재진입 성과(7/7 배선) — catalyst_archive(6/29~) additive 라벨·표본 축적 중.",
    }


def run() -> int:
    arch = _load_archive()
    days = sorted(arch)
    print(f"[meongbun_label] 아카이브 {len(days)}일 로드"
          + (f" ({days[0]}~{days[-1]})" if days else ""))

    # ① early shadow — 라벨만(변경 있을 때만 저장)
    if EARLY.exists():
        try:
            e_data = json.loads(EARLY.read_text(encoding="utf-8"))
            e_n = _label_records(e_data.get("records", []), arch)
            if e_n:
                _atomic_write(EARLY, e_data)
            print(f"[meongbun_label] early  : 라벨 +{e_n} / 총 {len(e_data.get('records', []))}")
        except Exception as ex:
            print(f"[meongbun_label] early 라벨 실패(무시): {ex}")
    else:
        print("[meongbun_label] early_variant_shadow.json 없음 — skip")

    # ② reentry shadow — 라벨 + 코호트/명분 요약(항상 재계산·저장)
    if REENTRY.exists():
        try:
            r_data = json.loads(REENTRY.read_text(encoding="utf-8"))
            r_n = _label_records(r_data.get("records", []), arch)
            _summaries(r_data)
            _atomic_write(REENTRY, r_data)
            mk = r_data["summary_min_kki"]
            print(f"[meongbun_label] reentry: 라벨 +{r_n} / 총 {len(r_data.get('records', []))} | "
                  f"{mk['current']['min_grade']}+ n={mk['current'].get('n')} net{mk['current'].get('net_avg')} "
                  f"vs {mk['candidate']['min_grade']}+ n={mk['candidate'].get('n')} net{mk['candidate'].get('net_avg')} "
                  f"(cost {COST_PER_LEG}/leg)")
        except Exception as ex:
            print(f"[meongbun_label] reentry 라벨/요약 실패(무시): {ex}")
    else:
        print("[meongbun_label] reentry_shadow.json 없음 — skip")

    print("★ record-only·additive 라벨/요약만 — 매수/매도/picks/SAJANG 무접촉·관측 없이 flip 금지")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as e:  # 스케줄러 보호 — 어떤 예외도 exit 0
        print(f"[meongbun_label] 치명 예외(무시): {e}")
        sys.exit(0)
