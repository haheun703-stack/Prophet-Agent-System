# -*- coding: utf-8 -*-
"""멀티 테마 대장주↔소부장 relay 분석 (6/22 사장님 지시, read-only).

사장님: "로봇·자동차·ESS 등 섹터들은 안 보는지? 그 다음에 설계 들어가자."
목적: 반도체에서 본 relay(소부장 먼저→대장주 나중)가 다른 테마에도 공통인지 검증 → 설계 일반화 근거.
종목코드는 universe.json 이름 매칭으로 해석(오타 방지). read-only(daily csv). 매수/매도/주문 무접촉.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / "data_store" / "daily"
UNIV = json.loads((ROOT / "data_store" / "universe.json").read_text(encoding="utf-8"))
NAME2CODE = {}
for code, info in UNIV.items():
    NAME2CODE.setdefault(info.get("name", ""), code)

# 테마별 대장주 / 소부장 (이름 기준 — universe에서 코드 해석)
THEMES = {
    "반도체": {
        "대장주": ["삼성전자", "SK하이닉스", "삼성전기", "SK스퀘어"],
        "소부장": ["한미반도체", "솔브레인", "원익IPS", "ISC", "리노공업", "동진쎄미켐",
                "주성엔지니어링", "DB하이텍", "티씨케이", "하나마이크론"],
    },
    "로봇": {
        "대장주": ["두산로보틱스", "레인보우로보틱스"],
        "소부장": ["에스피지", "로보스타", "에스비비테크", "뉴로메카", "하이젠알앤엠", "티로보틱스"],
    },
    "자동차": {
        "대장주": ["현대차", "기아", "현대모비스"],
        "소부장": ["HL만도", "한온시스템", "에스엘", "화신", "성우하이텍", "평화정공"],
    },
    "2차전지/ESS": {
        "대장주": ["LG에너지솔루션", "삼성SDI", "SK이노베이션"],
        "소부장": ["에코프로비엠", "엘앤에프", "포스코퓨처엠", "천보", "더블유씨피", "롯데에너지머티리얼즈"],
    },
    "방산": {
        "대장주": ["한화에어로스페이스", "LIG넥스원", "현대로템"],
        "소부장": ["한화시스템", "퍼스텍", "휴니드", "코츠테크놀로지", "STX엔진"],
    },
}

DATES = ["2026-06-02", "2026-06-04", "2026-06-05", "2026-06-08", "2026-06-09",
         "2026-06-10", "2026-06-11", "2026-06-12", "2026-06-16", "2026-06-17",
         "2026-06-18", "2026-06-19"]
T_BASE, T_MID, T_END = "2026-06-02", "2026-06-12", "2026-06-19"   # 초입~꼭지~따라잡기


def load_closes(code):
    f = DAILY / f"{code}.csv"
    if not f.exists():
        return {}
    out = {}
    for ln in f.read_text(encoding="utf-8").splitlines():
        p = ln.split(",")
        if len(p) < 6 or not p[0][:4].isdigit():
            continue
        try:
            out[p[0]] = float(p[4])
        except Exception:
            pass
    return out


def group_ret(names, a, b):
    """그룹 평균 수익률 a→b (이름→코드 해석, 데이터 있는 것만)."""
    rs = []
    miss = []
    for nm in names:
        code = NAME2CODE.get(nm)
        if not code:
            miss.append(nm + "(코드?)")
            continue
        cl = load_closes(code)
        if a in cl and b in cl and cl[a] > 0:
            rs.append((cl[b] / cl[a] - 1) * 100)
        else:
            miss.append(nm + "(데이터?)")
    avg = sum(rs) / len(rs) if rs else None
    return avg, len(rs), miss


def main():
    print("=" * 84)
    print("멀티 테마 relay 검증 — 소부장 먼저(초입 6/4→6/12) vs 대장주 나중(6/12→6/19)?")
    print("=" * 84)
    print(f"\n{'테마':12s} | {'구분':5s} | {'초입 6/4→6/12':>13s} | {'후반 6/12→6/19':>14s} | {'전구간 6/2→6/19':>14s}")
    print("-" * 84)
    for theme, grp in THEMES.items():
        rows = {}
        for label in ("대장주", "소부장"):
            e, ne, _ = group_ret(grp[label], "2026-06-04", "2026-06-12")   # 초입 다리
            l, nl, _ = group_ret(grp[label], "2026-06-12", "2026-06-19")   # 후반 다리
            f, nf, miss = group_ret(grp[label], "2026-06-02", "2026-06-19")
            rows[label] = (e, l, f, nf, miss)
        def fmt(x): return f"{x:+8.2f}%" if x is not None else "    n/a "
        for label in ("대장주", "소부장"):
            e, l, f, n, miss = rows[label]
            print(f"{theme:12s} | {label:5s} | {fmt(e):>13s} | {fmt(l):>14s} | {fmt(f):>14s} (n{n})")
        # relay 판정 (정직: 둘 다 손실이면 '테마 약세'로 분리 — 상대비교만으론 돈 안 됨)
        se, sl = rows["소부장"][0], rows["소부장"][1]
        de, dl = rows["대장주"][0], rows["대장주"][1]
        sf, df = rows["소부장"][2], rows["대장주"][2]   # 전구간
        verdict = "?"
        if None not in (se, sl, de, dl, sf, df):
            if sf < 0 and df < 0:
                verdict = "✘ 테마 약세 — 둘 다 손실, 살 자리 아님"
            elif se > de and dl > sl and se > 0:
                verdict = "✔ relay(소부장 먼저↑→대장주 나중↑)"
            elif sf > 0 and sf > df and se > 0:
                verdict = "→ 소부장 지속 우위(바통 안 넘김)"
            elif df > 0 and df > sf:
                verdict = "← 대장주 지속 우위(소부장 안 붙음)"
            else:
                verdict = "△ 혼조"
        print(f"{'':12s} | → {verdict}")
        miss_all = rows["대장주"][4] + rows["소부장"][4]
        if miss_all:
            print(f"{'':12s} |   누락: {', '.join(miss_all[:6])}")
        print("-" * 84)
    print("\n★ read-only. 매수/매도/주문 무접촉. daily csv(6/19까지) 실측. 누락=universe에 코드없음/데이터없음.")


if __name__ == "__main__":
    main()
