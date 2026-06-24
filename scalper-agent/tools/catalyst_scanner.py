# -*- coding: utf-8 -*-
"""명분 있는 끼 종목 발굴 스캐너 (6/24 사장님 지시) — record-only 관측.

사장님 6/24: "상한가/10%+ 오른 종목이 이유(명분) 있어서 오르면 매매 대상.
단 추격 말고 맥점(초입/눌림)에서 사서 5~10% 먹고 −3%에 자른다."
근거: feedback_money_path_catalyst_meongbun / paper_learning(명분 버팀 92% vs 터짐 14%).

파이프라인:
  1. 끼 종목 자동추출 — ranking_scan(상한가·근접·급등) + 일봉 3일 10%+
  2. 재료(명분) 자동분석 — news_collector 네이버 종목뉴스 제목
  3. 명분 등급 — 강(반도체/AI/로봇/수주/신약/정책) / 중(부동산/엔터/실적) / 약(대표변경/증자) / 무명분
  4. 맥점 판별 — 초입(pos20 바닥·아직 안 감) / 눌림(조정) / 추격위험(이미 폭등=거름)
  5. 종합 verdict — 명분(강·중) + 맥점(초입·눌림) = ★관측후보 / 명분O 추격 = 관망 / 명분약 = 제외

★★★ 안전 불변식 ★★★
- record-only: data_store/catalyst_scan.json 만 기록. 매수/매도/picks/recommendation/SAJANG/
  order_intent 0 접촉. 봇 OFF·실주문 0.
- 관측 검증 후 사장님 승인 → 매매 연결(관측 없이 flip 금지).
- read-only 입력: ranking_scan.json(KIS 실시간) + 일봉 csv + 네이버 종목뉴스(news_collector).

실행: python tools/catalyst_scanner.py [--top N]
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
DAILY_DIR = BASE / "data_store" / "daily"
RANKING_SCAN = BASE / "data_store" / "ranking_scan.json"
OUT = BASE / "data_store" / "catalyst_scan.json"

# 명분 등급 키워드 (재료 강도) — 강한 명분일수록 −3% 안 터지고 버틴다
STRONG_KW = ["반도체", "HBM", "AI", "데이터센터", "전력", "원전", "로봇", "휴머노이드",
             "2차전지", "배터리", "방산", "수주", "공급계약", "납품", "신약", "임상",
             "승인", "정책", "수혜", "흑자전환", "최대 실적", "공장"]
MID_KW = ["부동산", "재건축", "분양", "실적", "엔터", "흥행", "M&A", "인수", "합병",
          "신제품", "출시", "협약", "MOU"]
WEAK_KW = ["대표이사", "경영진", "사임", "유상증자", "전환사채", "감자",
           "투자경고", "투자주의", "관리종목"]

POS20_WINDOW = 20
SURGE_3D_MIN = 10.0       # 3일 누적 10%+ = 끼
CHASE_3D = 40.0           # 3일 +40%+ = 추격위험(blow-off)
PULLBACK_LO, PULLBACK_HI = 0.40, 0.70
# ★ 초입점화(진짜 맥점) — 휴면 후 거래량 급증 + 첫 강한 양봉 (6/24 보해양조 6/18형).
#   pos20 바닥만으론 죽은 횡보를 '초입'으로 오인(5/21~6/17) → 점화 조건으로 교체.
IGNITE_PRE = 5            # 직전 휴면 측정일
IGNITE_DORMANT_MAX = 7.0  # 직전 PRE일 최대 일간상승 < 7% = 휴면(조용히 바닥)
IGNITE_VOL_X = 2.0        # 당일 거래량 >= 20일평균 2배 (거래량 점화)
IGNITE_SURGE_MIN = 5.0    # 당일 +5%+ 첫 양봉


def _rows(code: str):
    """일봉 csv → [(date, close, high, low, volume), ...] (read-only)."""
    f = DAILY_DIR / f"{code}.csv"
    if not f.exists():
        return []
    try:
        lines = f.read_text(encoding="utf-8").strip().split("\n")[1:]
        out = []
        for ln in lines:
            p = ln.split(",")
            if len(p) >= 6:
                out.append((p[0], float(p[4]), float(p[2]), float(p[3]), float(p[5])))
            elif len(p) >= 5:
                out.append((p[0], float(p[4]), float(p[2]), float(p[3]), 0.0))
        return out
    except Exception:
        return []


def _chg_nd(rows, n: int):
    if len(rows) < n + 1:
        return None
    c_now = rows[-1][1]
    c_then = rows[-1 - n][1]
    return round((c_now / c_then - 1) * 100, 1) if c_then > 0 else None


def _pos20(rows):
    """20일 범위 내 위치 0~1 (0=바닥/초입, 1=천정/끝물)."""
    if len(rows) < 2:
        return None
    win = rows[-POS20_WINDOW:]
    lo = min(r[3] for r in win)
    hi = max(r[2] for r in win)
    c = rows[-1][1]
    return round((c - lo) / (hi - lo), 2) if hi > lo else None


SIHWANG_KW = ["코스피", "코스닥", "마감시황", "이 시각 시황", "서울데이터랩",
              "애프터마켓", "브리핑", "서킷브레이커", "증시", "검은 화요일",
              "맞은 증시", "급락 마감", "급등 출발"]


def _is_sihwang(title):
    """시황뉴스(종목 고유 재료 아님) 판별."""
    return any(k in title for k in SIHWANG_KW)


def _meongbun_grade(titles):
    """뉴스 제목 → 명분 등급 + 매칭 키워드. 강 > 중 > 약 > 무명분.

    ★ 6/24 오탐 fix: 시황뉴스(코스피/코스닥 마감·급락 등)는 종목 고유 재료가
    아니므로 제외. 부국철강이 '코스피 급락'에서 '반도체'만 잡혀 강으로 오인되던 것 차단."""
    own = [t for t in titles if t and not _is_sihwang(t)]
    text = " ".join(own)
    hits_s = [k for k in STRONG_KW if k in text]
    hits_m = [k for k in MID_KW if k in text]
    hits_w = [k for k in WEAK_KW if k in text]
    if hits_s:
        return "강", hits_s[:3]
    if hits_m:
        return "중", hits_m[:3]
    if hits_w:
        return "약", hits_w[:2]
    if not own:
        return "무명분", []
    return "보통", []


def _ignition(rows):
    """점화(진짜 초입) 판별 — 휴면 후 거래량 급증 + 첫 강한 양봉.

    ★ 6/24 보해양조 역추적 교훈: pos20 바닥만으론 죽은 횡보를 '초입'으로 오인
    (5/21~6/17 가짜 초입). 진짜 초입 = 조용히 바닥(휴면) → 거래량 터지며 첫 양봉(6/18).
    Returns: (is_ignition, vol_ratio, prior_max_chg)."""
    if len(rows) < IGNITE_PRE + 2:
        return False, None, None
    closes = [r[1] for r in rows]
    vols = [r[4] for r in rows]
    win_vols = vols[-21:-1]  # 직전 20일(오늘 제외)
    avg_vol = sum(win_vols) / len(win_vols) if win_vols else 0
    vol_ratio = round(vols[-1] / avg_vol, 1) if avg_vol > 0 else None
    # 직전 PRE일 최대 일간상승(오늘 제외) = 휴면도
    prior = []
    for j in range(max(1, len(closes) - 1 - IGNITE_PRE), len(closes) - 1):
        if closes[j - 1] > 0:
            prior.append((closes[j] / closes[j - 1] - 1) * 100)
    prior_max = round(max(prior), 1) if prior else 0.0
    today_chg = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] > 0 else 0
    is_ig = (prior_max < IGNITE_DORMANT_MAX) \
        and (vol_ratio is not None and vol_ratio >= IGNITE_VOL_X) \
        and (today_chg >= IGNITE_SURGE_MIN)
    return is_ig, vol_ratio, prior_max


def _maek_jeom(pos20, chg_3d, today_chg, is_upper, ig_info):
    """진입 맥점 판별 — 초입점화(진짜 맥점) / 눌림 / 추격위험 / 보통.

    ★ 핵심 = '초입점화': 휴면 후 거래량 급증 + 첫 강한 양봉(6/18형). pos20 바닥
    단독(죽은 횡보)은 더 이상 초입으로 보지 않음(5/21형 가짜 배제)."""
    is_ig, vol_ratio, prior_max = ig_info
    # 초입점화: 휴면 후 거래량 급증 + 첫 양봉 = 진짜 맥점. 천정 직전 아니면 추격보다 우선.
    if is_ig and (pos20 is None or pos20 < 0.90):
        return "초입점화"
    # 추격위험: 이미 폭등 — 상한가 추격 or 3일 +40%+ or pos20 천정
    if is_upper or (chg_3d is not None and chg_3d >= CHASE_3D) \
            or (pos20 is not None and pos20 >= 0.85):
        return "추격위험"
    # 눌림: 중간 위치 + 오늘 조정(음수) = 올랐다 쉬는 자리
    if pos20 is not None and PULLBACK_LO <= pos20 <= PULLBACK_HI \
            and today_chg is not None and today_chg < 0:
        return "눌림"
    return "보통"


def _verdict(grade, maek):
    """명분 + 맥점 → 종합 관측 판정 (record-only, 매수 아님)."""
    if grade in ("강", "중") and maek in ("초입점화", "눌림"):
        return "★관측후보(명분+맥점)"
    if grade in ("강", "중") and maek == "추격위험":
        return "관망(명분O·추격구간)"
    if grade in ("약", "무명분"):
        return "제외(명분약)"
    return "관찰"


def _gather_candidates():
    """ranking_scan(실시간) + 일봉 3일10%+ → 끼 종목 dict{code: {...}}."""
    cand = {}
    if RANKING_SCAN.exists():
        try:
            rs = json.loads(RANKING_SCAN.read_text(encoding="utf-8"))
        except Exception:
            rs = {}
        for u in rs.get("upper_limit", []):
            cand[u["code"]] = {"name": u["name"], "today_chg": u.get("change_rate"),
                               "is_upper": True, "srcs": ["상한가"]}
        for u in rs.get("near_limit", []):
            c = cand.setdefault(u["code"], {"name": u["name"], "today_chg": u.get("change_rate"),
                                            "is_upper": False, "srcs": []})
            if "상한근접" not in c["srcs"]:
                c["srcs"].append("상한근접")
        for u in rs.get("surge_top", []):
            if (u.get("change_rate") or 0) >= 5:
                c = cand.setdefault(u["code"], {"name": u["name"], "today_chg": u.get("change_rate"),
                                                "is_upper": False, "srcs": []})
                if "급등" not in c["srcs"]:
                    c["srcs"].append("급등")
    # 일봉 3일 10%+ (전체 스캔)
    if DAILY_DIR.exists():
        for fn in os.listdir(DAILY_DIR):
            if not fn.endswith(".csv"):
                continue
            code = fn[:-4]
            chg3 = _chg_nd(_rows(code), 3)
            if chg3 is not None and chg3 >= SURGE_3D_MIN:
                c = cand.setdefault(code, {"name": code, "today_chg": None,
                                           "is_upper": False, "srcs": []})
                c["chg_3d"] = chg3
                if "3일강세" not in c["srcs"]:
                    c["srcs"].append("3일강세")
    return cand


def _find_next_candidates(items, max_per_theme: int = 8, limit: int = 25):
    """★ 선행 발굴(6/24 사장님): 오늘 점화/강세 종목의 명분 키워드 →
    theme_map 같은 테마 종목 중 아직 휴면(안 간) 것 = '곧 오를 소재' 후보.

    '왜 올랐나'(후행)를 넘어 '곧 오를 소재 있는 종목'을 미리. 보해양조 '반도체'
    명분 → theme_map 반도체 테마 종목 중 휴면(pos20 바닥·3일 안 오름) 발굴."""
    try:
        from data.theme_collector import load_theme_map
    except Exception:
        return []
    tmap = load_theme_map()
    themes = tmap.get("themes", {})
    if not themes:
        return []
    # 오늘 강/중 명분 키워드 수집 (시황 오탐 제거된 등급)
    # 테마 확산은 섹터 테마 키워드만 — '수혜/수주/공장' 등 일반어는 엉뚱 매칭(식품주 오탐)
    THEME_KW = {"반도체", "HBM", "AI", "데이터센터", "전력", "원전", "로봇",
                "휴머노이드", "2차전지", "배터리", "방산"}
    hot_kw = set()
    for it in items:
        if it.get("meongbun_grade") in ("강", "중"):
            hot_kw.update(k for k in (it.get("meongbun_kw") or []) if k in THEME_KW)
    if not hot_kw:
        return []
    today_codes = {it["code"] for it in items}
    seen = {}
    for info in themes.values():
        tname = info.get("name", "")
        matched = [kw for kw in hot_kw if kw in tname]
        if not matched:
            continue
        cnt = 0
        for code in info.get("codes", []):
            if code in today_codes or code in seen:
                continue
            rows = _rows(code)
            pos = _pos20(rows)
            chg3 = _chg_nd(rows, 3)
            # 휴면 = pos20 바닥 + 3일 안 오름 = 점화 전(다음 후보)
            if pos is not None and pos < 0.45 and (chg3 is None or chg3 < 8):
                seen[code] = {"code": code, "theme": tname, "matched_kw": matched,
                              "pos20": pos, "chg_3d": chg3}
                cnt += 1
                if cnt >= max_per_theme:
                    break
    out = sorted(seen.values(), key=lambda x: x["pos20"])  # 가장 바닥부터
    return out[:limit]


def scan_catalyst(top_n: int = 30, save: bool = True) -> dict:
    """끼 종목 → 재료 → 명분등급 → 맥점 → verdict + 선행 후보. record-only."""
    from data.news_collector import NewsCollector
    nc = NewsCollector()

    cand = _gather_candidates()
    # 종목별 메트릭
    items = []
    for code, c in cand.items():
        rows = _rows(code)
        c["code"] = code
        c["chg_3d"] = c.get("chg_3d") if c.get("chg_3d") is not None else _chg_nd(rows, 3)
        c["pos20"] = _pos20(rows)
        items.append(c)
    # 우선순위: 상한가 > 3일강세 높은 > 오늘등락 높은
    items.sort(key=lambda x: (x["is_upper"], x.get("chg_3d") or 0, x.get("today_chg") or 0),
               reverse=True)
    items = items[:top_n]

    # 재료(뉴스) 분석
    import time
    for it in items:
        time.sleep(0.15)  # 네이버 종목뉴스 크롤링 과부하/차단 방지
        try:
            news = nc.fetch_naver_news(it["code"], count=5)
            titles = [n.get("title", "") for n in news if isinstance(n, dict)]
        except Exception:
            titles = []
        it["titles"] = titles[:3]
        it["meongbun_grade"], it["meongbun_kw"] = _meongbun_grade(titles)
        ig = _ignition(_rows(it["code"]))
        it["vol_ratio"], it["prior_max"] = ig[1], ig[2]
        it["maek_jeom"] = _maek_jeom(it.get("pos20"), it.get("chg_3d"),
                                     it.get("today_chg"), it["is_upper"], ig)
        it["verdict"] = _verdict(it["meongbun_grade"], it["maek_jeom"])

    next_cand = _find_next_candidates(items)
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(items),
        "items": items,
        "next_candidates": next_cand,  # ★ 선행 발굴: 같은 테마 휴면 종목(곧 오를 소재)
        "note": "record-only / 매수·매도·picks·SAJANG·order 무접촉 / 관측 없이 flip 금지",
    }
    if save:
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUT)
    return result


def format_report(result: dict) -> str:
    lines = [f"=== 명분 있는 끼 종목 발굴 ({result.get('timestamp','')}) — record-only ==="]
    items = result.get("items", [])
    buckets = {"★관측후보(명분+맥점)": [], "관망(명분O·추격구간)": [], "관찰": [], "제외(명분약)": []}
    for it in items:
        buckets.setdefault(it["verdict"], []).append(it)
    for v in ["★관측후보(명분+맥점)", "관망(명분O·추격구간)", "관찰", "제외(명분약)"]:
        bl = buckets.get(v, [])
        if not bl:
            continue
        lines.append(f"\n[{v}] {len(bl)}종목")
        for it in bl:
            tchg = f"{it['today_chg']:+.0f}%" if it.get("today_chg") is not None else "—"
            c3 = f"3일{it['chg_3d']:+.0f}%" if it.get("chg_3d") is not None else ""
            p20 = f"pos{it['pos20']}" if it.get("pos20") is not None else ""
            kw = "/".join(it.get("meongbun_kw") or [])
            title = (it.get("titles") or [""])[0][:38]
            lines.append(f"  {it['name']}({it['code']}) {tchg} {c3} {p20} "
                         f"명분[{it['meongbun_grade']}:{kw}] {it['maek_jeom']}")
            lines.append(f"      → {title}")
    # ★ 선행 발굴 — 같은 테마 휴면 종목(곧 오를 소재 후보)
    nxt = result.get("next_candidates", [])
    if nxt:
        lines.append(f"\n[★ 곧 오를 소재 후보 — 같은 테마 휴면(아직 안 감)] {len(nxt)}종목")
        for it in nxt[:15]:
            kw = "/".join(it.get("matched_kw") or [])
            c3 = f"3일{it['chg_3d']:+.0f}%" if it.get("chg_3d") is not None else "3일—"
            lines.append(f"  {it['code']} [{it['theme']}] kw:{kw} pos{it['pos20']} {c3}")
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()
    res = scan_catalyst(top_n=args.top)
    print(format_report(res))
    print(f"\n저장: {OUT.name} ({res['count']}종목) · record-only · 매수 무접촉")
