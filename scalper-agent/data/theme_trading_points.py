# -*- coding: utf-8 -*-
"""테마 relay-aware 매매포인트 → 한국스윙 대시보드 섹션 (6/22 사장님 지시).

사장님: "theme_relay 매매포인트를 매일 한국스윙 페이지(매매포인트)에 기록. 퀀트봇도 그렇게 한다."
data/theme_relay_shadow.json(record-only 관측) 최신일 → 웹봇이 렌더할 dashboard_swing.trading_points JSONB.

★ read-only: shadow json만 읽어 표시용 섹션 생성. 매수/매도/주문 무접촉. 실매매 신호 아님(관측 매매포인트). ★
웹봇은 이 섹션을 한국스윙 '매매포인트' 탭에 렌더(강한 테마 주도그룹·초입후보·바통).
"""
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.ThemeTradingPoints")

SA = Path(__file__).resolve().parent.parent
SHADOW = SA / "data_store" / "theme_relay_shadow.json"

_GROUP_LABEL = {"LEADERS": "대장주", "SUPPLY_CHAIN": "소부장"}


def _latest_date(records: list) -> str:
    return max((r.get("date", "") for r in records), default="")


def build_trading_points(shadow_path: Path = None) -> dict:
    """theme_relay_shadow 최신일 → 매매포인트 섹션 (웹봇 렌더용). 실패/없음이면 빈 구조."""
    shadow_path = shadow_path or SHADOW
    try:
        recs = json.loads(shadow_path.read_text(encoding="utf-8")).get("records", [])
    except Exception as e:
        logger.warning(f"[trading_points] shadow 로드 실패(무시): {e}")
        return {"date": "", "themes": [], "note": "theme_relay_shadow 없음"}
    if not recs:
        return {"date": "", "themes": [], "note": "관측 데이터 없음"}

    asof = _latest_date(recs)
    today = [r for r in recs if r.get("date") == asof]

    themes = []
    for r in today:
        leader = r.get("relay_leader_group")
        label = _GROUP_LABEL.get(leader, "-")
        weak = bool(r.get("weak_theme"))
        rev = bool(r.get("leader_reversal"))
        early = [
            {"code": c.get("code"), "name": c.get("name"),
             "pos20": c.get("pos20"), "ret5": c.get("ret5")}
            for c in (r.get("early_candidates") or [])[:5]
        ]
        # 한줄 코멘트
        if weak:
            comment = "약세 테마 — 회피 (주도그룹도 마이너스)"
        else:
            parts = [f"{label} 주도"]
            if rev:
                parts.append("바통 전환(주도그룹 역전)")
            if early:
                parts.append("초입 후보 " + ", ".join(c["name"] for c in early[:3]))
            comment = " · ".join(parts)
        themes.append({
            "theme": r.get("theme"),
            "strength": r.get("theme_strength"),
            "leader_group": leader,
            "leader_label": label,
            "leaders_strength": r.get("leaders_strength"),
            "supply_strength": r.get("supply_strength"),
            "reversal": rev,
            "weak": weak,
            "early_candidates": early,
            "comment": comment,
        })

    # 정렬: 강한 테마 먼저(약세 후순위), 같으면 주도그룹 강도 desc
    def _sort_key(t):
        lead_str = t.get("leaders_strength") if t.get("leader_group") == "LEADERS" else t.get("supply_strength")
        return (1 if t.get("weak") else 0, -(lead_str if lead_str is not None else -999))
    themes.sort(key=_sort_key)

    strong = [t for t in themes if not t["weak"]]
    return {
        "date": asof,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "strong_count": len(strong),
        "themes": themes,
        "note": ("테마 relay-aware 매매포인트(관측) — 강한 테마의 주도그룹(대장주/소부장)·초입후보·바통. "
                 "paper shadow 관측이며 실매수 신호 아님. 관측 검증 후 사장님 결정으로 실매매 반영."),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    tp = build_trading_points()
    print(f"[trading_points] {tp.get('date')} 강한테마 {tp.get('strong_count')}개 / 전체 {len(tp.get('themes', []))}")
    for t in tp.get("themes", []):
        print(f"  {t['theme']:10s} | str {t['strength']} | {t['comment']}")
