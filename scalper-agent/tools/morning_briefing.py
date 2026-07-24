# -*- coding: utf-8 -*-
"""아침 시장 브리핑 빌더 (7/4 신설) — "장 시작 전 뭘 봐야 하나" 한 장 통합.

사장님 7/4 지시: "전체시장 시작 전에 우리가 뭘 봐야 하고, 그걸 약간의 토대로
장중 경우의 수(추매단타/상한가엔진/소문→뉴스/뉴스→개미) 플레이를 바둑처럼 유연하게."
→ 1단계 = 정찰 통합. 흩어진 저녁 산출물을 read-only로 한 장에 묶는다 (6/7 진단
  "신호는 되는데 통로가 따로 논다"의 아침 버전 해소).

입력 (전부 nightly 산출물 read-only·재계산 0):
  ⑰ market_regime.json           — 다음 거래일 레짐(NO_GO/CAUTION/GO)+지수 정배열
  ③-4 theme_relay_shadow.json    — 테마별 릴레이 단계(대장주/소부장·바통터치)
  ⑪ catalyst_scan.json           — 명분 있는 끼(★관측후보/★★소문매수)+선행 후보
  (⑯ cluster_harvest_paper.json — 7/24 S-2 판정으로 입력에서 제외·엣지 없음 확정)
  daytrading_picks.json           — 최신 발행 픽(참고)
  scalper_open_gate.json          — (있으면) 간밤 미국장 오버레이 — 아침 08:45 갱신됨 주의

출력: data_store/morning_briefing.json + 텍스트 요약(stdout).
★ 안전: record-only·읽기전용·매수/매도/picks/SAJANG/주문 0접촉. 플레이북 힌트는
  "오늘 어떤 정석을 우선 볼지" 관측 라벨일 뿐 — 매매 연결은 관측+사장님 승인 후.
사용: python tools/morning_briefing.py
"""
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

DS = BASE / "data_store"
OUT = DS / "morning_briefing.json"


def _load(name):
    p = DS / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _regime_block():
    r = _load("market_regime.json")
    if not r:
        return None
    lt = r.get("latest") or {}
    k = ((lt.get("index_proxy") or {}).get("kospi_069500") or {})
    q = ((lt.get("index_proxy") or {}).get("kosdaq_229200") or {})
    return {
        "regime": lt.get("regime"),
        "based_on": lt.get("based_on"),
        "applies_to": lt.get("applies_to"),
        "breadth_pct": (lt.get("breadth") or {}).get("breadth_pct"),
        "limit_up_yesterday": (lt.get("breadth") or {}).get("limit_up"),
        "kospi": {"above_ma20": k.get("above_ma20"), "stacked_bull": k.get("stacked_bull")},
        "kosdaq": {"above_ma20": q.get("above_ma20"), "stacked_bull": q.get("stacked_bull")},
    }


def _relay_block():
    r = _load("theme_relay_shadow.json")
    if not r:
        return None
    recs = r.get("records") or []
    if not recs:
        return None
    latest_date = max(x.get("date", "") for x in recs)
    out = []
    for x in recs:
        if x.get("date") != latest_date:
            continue
        out.append({
            "theme": x.get("theme"),
            "leader_group": x.get("relay_leader_group"),
            "baton_touch": bool(x.get("leader_reversal")),
            "weak_theme": bool(x.get("weak_theme")),
            "early_candidates": [c.get("name") for c in (x.get("early_candidates") or [])][:3],
        })
    return {"date": latest_date, "themes": out}


def _catalyst_block():
    r = _load("catalyst_scan.json")
    if not r:
        return None
    items = r.get("items") or []
    watch = [{"code": i.get("code"), "name": i.get("name"),
              "grade": i.get("meongbun_grade"), "kw": i.get("meongbun_kw"),
              "news_stage": i.get("news_stage"), "maek_jeom": i.get("maek_jeom"),
              "verdict": i.get("verdict")}
             for i in items
             if isinstance(i.get("verdict"), str) and i.get("verdict", "").startswith("★")]
    nxt = [{"code": c.get("code"), "theme": c.get("theme")}
           for c in (r.get("next_candidates") or [])][:10]
    return {"timestamp": r.get("timestamp"), "watch": watch[:10], "preemptive": nxt}


# _cluster_block() 제거 — 7/24 S-2 판정(엣지 없음)으로 ⑯이 nightly에서 빠졌다.
# 장부는 판정 근거로 동결되므로 그대로 읽으면 브리핑이 매일 아침 '고정된 과거'를
# 오늘의 진입 예정처럼 표시하게 된다(7/17 휴장일 스테일 스냅샷과 같은 계열의 함정).


def _picks_block():
    r = _load("daytrading_picks.json")
    if not r:
        return None
    return {
        "updated": r.get("updated"), "mode": r.get("mode"),
        "top": [{"code": p.get("code"), "name": p.get("name"), "track": p.get("track")}
                for p in (r.get("picks") or [])][:5],
    }


def _open_gate_block():
    r = _load("scalper_open_gate.json")
    if not r:
        return None
    return {"gate": r.get("gate"), "score": r.get("overnight_korea_score"),
            "asof": r.get("generated_at") or r.get("asof"),
            "note": "간밤 미국장 오버레이 — 매 거래일 08:45 잡이 갱신(이 값은 마지막 빌드 기준)"}


def _playbook_hints(regime_block, catalyst_block):
    """오늘 우선 볼 정석(플레이북) 힌트 — 관측 라벨(매매 연결 아님·flip 금지).

    7/4 레짐검증 근거: NO_GO(전일 breadth≤0.45)만 검증된 회피 신호. 나머지 힌트는
    설계 단계(playbook shadow 축적 전) — 관측이 쌓여야 신뢰도가 생긴다."""
    hints = []
    reg = (regime_block or {}).get("regime")
    if reg == "NO_GO":
        hints.append("회피일(검증된 신호): 신규진입 축소 우선 — 손절터치 26~28% 많은 날. "
                     "플레이는 관망/짧은 상한가엔진만 관측.")
    elif reg == "CAUTION":
        hints.append("주의일: 추매단타는 짧게(당일청산 전제 관측). 지수 20일선 아래면 "
                     "오후장 되밀림 경계.")
    elif reg == "GO":
        hints.append("회피 아님(상승예측 아님): 추매단타·릴레이 후발주 관측 우선.")
    if catalyst_block:
        rumor = [w for w in catalyst_block.get("watch", [])
                 if "소문" in str(w.get("verdict", ""))]
        if rumor:
            hints.append(f"소문단계 후보 {len(rumor)}종목 — '소문에 사서 뉴스에 판다' "
                         f"플레이북 관측 대상: {[w['name'] for w in rumor[:3]]}")
        news = [w for w in catalyst_block.get("watch", [])
                if str(w.get("news_stage")) == "뉴스"]
        if news:
            hints.append(f"뉴스단계 {len(news)}종목 — '뉴스에 사서 개미 유입에 판다' "
                         f"관측 대상(과열 추격위험 병행 체크).")
    if (regime_block or {}).get("limit_up_yesterday", 0) and \
            (regime_block or {}).get("limit_up_yesterday") >= 10:
        hints.append(f"전일 상한가 {regime_block['limit_up_yesterday']}개 — 상한가엔진/"
                     f"클러스터 릴레이 활성 환경. 되밀림도 큰 날이니 진입가 규율 필수.")
    return hints


def build_briefing(save=True) -> dict:
    regime = _regime_block()
    relay = _relay_block()
    catalyst = _catalyst_block()
    picks = _picks_block()
    open_gate = _open_gate_block()
    out = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "for_day": (regime or {}).get("applies_to"),
        "regime": regime,
        "open_gate_overlay": open_gate,
        "theme_relay": relay,
        "catalyst": catalyst,
        "daytrading_picks": picks,
        "playbook_hints": _playbook_hints(regime, catalyst),
        "note": "아침 정찰 한 장(record-only·read-only 통합). 플레이북 힌트=관측 라벨 — "
                "매매 연결은 playbook shadow 축적+사장님 승인 후. 관측 없이 flip 금지.",
    }
    if save:
        try:
            # 7/7 검수 L-2 fix: 원자쓰기(20:30 노트북 scp와 겹칠 때 torn-read 방지)
            tmp = OUT.with_suffix(".tmp")
            tmp.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
            tmp.replace(OUT)
        except Exception as e:  # noqa: BLE001
            print(f"[morning_briefing] 저장 실패(무시): {e}")
    return out


def format_text(b: dict) -> str:
    L = [f"=== 아침 브리핑 ({b.get('for_day') or '다음 거래일'}) ==="]
    r = b.get("regime") or {}
    if r:
        k = r.get("kospi") or {}
        L.append(f"레짐 {r.get('regime')} | 전일 breadth {r.get('breadth_pct')} | "
                 f"전일 상한가 {r.get('limit_up_yesterday')}개 | "
                 f"코스피 20일선{'위' if k.get('above_ma20') else '아래'}"
                 f"{'·정배열' if k.get('stacked_bull') else ''}")
    og = b.get("open_gate_overlay")
    if og:
        L.append(f"오픈게이트(간밤): {og.get('gate')} score={og.get('score')} (08:45 갱신 전 값)")
    tr = b.get("theme_relay") or {}
    for t in (tr.get("themes") or []):
        if t.get("weak_theme"):
            continue
        L.append(f"릴레이 {t['theme']}: {t.get('leader_group')}"
                 f"{' ★바통터치' if t.get('baton_touch') else ''} 초입후보 {t.get('early_candidates')}")
    ca = b.get("catalyst") or {}
    for w in (ca.get("watch") or [])[:5]:
        L.append(f"명분끼 {w.get('name')}({w.get('code')}): {w.get('verdict')} / "
                 f"{w.get('grade')} / {w.get('news_stage')} / {w.get('maek_jeom')}")
    for h in b.get("playbook_hints") or []:
        L.append(f"▶ {h}")
    L.append("(record-only 정찰 — 매매 연결 아님)")
    return "\n".join(L)


if __name__ == "__main__":
    try:
        b = build_briefing()
        print(format_text(b))
        print(f"[morning_briefing] -> {OUT.name} (record-only·read-only 통합)")
    except Exception as e:  # noqa: BLE001
        print(f"[morning_briefing] 치명 예외(무시): {e}")
    sys.exit(0)
