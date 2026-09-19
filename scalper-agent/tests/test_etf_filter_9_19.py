# -*- coding: utf-8 -*-
"""[F-227]/[F-244] ETF 배제 필터 — 무력화와 오탐 양쪽 (9/19 신설).

[F-227] 자산풀 매수 경로의 ETF 필터가 **구조적으로 무력**했다.
  후보 `ranked` 는 `get_top_candidates()` 산출인데 그 함수는 code/score/sources 만
  돌려준다(name **없음**). triggers 에 없는 코드는 name='' 이 되고
  `''.startswith(prefix)` 는 항상 False → 그대로 통과.
  실측: 후보 2,453종 중 **2,135종(87%)이 name 공란**.
  ★현재 실제 누출은 0건 — universe 2,531종에 ETF 가 없다. 구조만 뚫려 있었다.

[F-244] ★그런데 장부의 처방이 위험했다.
  장부는 *"auto_trader 만 10개고 다른 4벌은 18개"* 라며 18개로 맞추라는 취지였는데,
  실측해 보니 **18개 쪽이 정상 주식을 ETF 로 오분류**하고 있었다:
    institution_accum(18) → KB금융·BNK금융지주·KB발해인프라·KBI메탈·KBI동양철관·
                            KB오토시스 등 **7종 배제**
    intraday_scanner(13)  → BNK금융지주 **1종 배제**
  전부 PER·PBR·섹터가 있는 실제 기업이다. KB자산운용 ETF 브랜드는 2024년
  KBSTAR → **RISE** 로 바뀌었고 RISE 는 이미 목록에 있다 → `KB`·`BNK` 제거.
  **장부의 처방도 검증 대상이다**(8/29 교훈).
"""
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
UNI = BASE / "data_store" / "universe.json"


def _names():
    with open(UNI, encoding="utf-8") as f:
        uni = json.load(f)
    return {c: str(v.get("name", "")) for c, v in uni.items() if isinstance(v, dict)}


def _blocked(prefixes):
    return [(c, n) for c, n in _names().items()
            if any(n.startswith(p) for p in prefixes)]


# ------------------------------------------------------------------
#  [F-244] 오탐 — 정상 주식을 ETF 로 잡으면 안 된다
# ------------------------------------------------------------------

def test_institution_prefixes_have_no_false_positives():
    from data.institution_accum_scan import ETF_PREFIXES
    bad = _blocked(ETF_PREFIXES)
    assert bad == [], f"정상 주식을 ETF로 배제한다: {bad[:5]}"


def test_intraday_prefixes_have_no_false_positives():
    from data.intraday_scanner import _ETF_PREFIXES
    bad = _blocked(_ETF_PREFIXES)
    assert bad == [], f"정상 주식을 ETF로 배제한다: {bad[:5]}"


def test_short_ambiguous_prefixes_are_gone():
    """★`KB`·`BNK` 는 기업명을 먹는다 — 되살아나면 KB금융이 매일 배제된다."""
    from data.institution_accum_scan import ETF_PREFIXES as P1
    from data.intraday_scanner import _ETF_PREFIXES as P2
    for label, P in (("institution_accum", P1), ("intraday_scanner", P2)):
        assert "KB" not in P, f"{label}: 'KB' 가 KB금융·KBI메탈을 ETF로 만든다"
        assert "BNK" not in P, f"{label}: 'BNK' 가 BNK금융지주를 ETF로 만든다"


def test_known_stocks_are_not_treated_as_etf():
    """음성대조 — 옛 목록이었다면 이 종목들이 배제됐다."""
    names = _names()
    old_list = ["KODEX", "TIGER", "RISE", "PLUS", "KIWOOM", "KoAct", "TIME",
                "SOL", "HANARO", "KOSEF", "ACE", "ARIRANG", "BNK", "TIMEFOLIO",
                "FOCUS", "WOORI", "KB", "TREX"]
    victims = [c for c, n in names.items() if any(n.startswith(p) for p in old_list)]
    assert len(victims) >= 5, "실측 전제가 바뀌었다 — 옛 목록은 7종을 먹었다"

    from data.institution_accum_scan import ETF_PREFIXES as now
    still = [c for c in victims if any(names[c].startswith(p) for p in now)]
    assert still == [], f"여전히 배제되는 정상 주식: {[(c, names[c]) for c in still]}"


def test_real_etf_brands_still_blocked():
    """오탐을 없애면서 **진짜 ETF 브랜드는 그대로** 잡아야 한다."""
    from data.institution_accum_scan import ETF_PREFIXES as P
    for brand in ("KODEX 200", "TIGER 미국S&P500", "RISE 200", "ACE 국고채10년",
                  "SOL 반도체", "HANARO Fn K-POP", "PLUS 고배당주"):
        assert any(brand.startswith(p) for p in P), f"{brand} 를 못 잡는다"


# ------------------------------------------------------------------
#  [F-227] 무력화 — name 이 비면 필터가 통과시킨다
# ------------------------------------------------------------------

def test_empty_name_passes_any_prefix_filter():
    """구조 자체를 고정 — 빈 이름은 어떤 접두에도 안 걸린다(그래서 보강이 필요했다)."""
    P = ("KODEX", "TIGER")
    assert not any("".startswith(p) for p in P)


def test_top_candidates_have_no_name_field():
    """★필터가 무력했던 근본 — 후보 산출에 name 이 없다."""
    import inspect
    from utils import asset_pool_loader
    src = inspect.getsource(asset_pool_loader.get_top_candidates)
    body = src.split("return", 1)[1]
    assert '"name"' not in body, \
        "get_top_candidates 가 name 을 돌려주게 바뀌었다면 auto_trader 보강을 재검토할 것"


def test_auto_trader_enriches_names_from_universe():
    """보강 코드가 실제로 있는가 — 없으면 87%가 필터를 통과한다."""
    src = (BASE / "bot" / "auto_trader.py").read_text(encoding="utf-8")
    assert "[F-227]" in src
    assert "universe.json" in src
    assert "name_map[_c] = _v[\"name\"]" in src


def test_universe_currently_has_no_etf():
    """현재 누출이 0건인 이유를 사실로 박아 둔다 — 바뀌면 이 테스트가 알려준다."""
    from data.institution_accum_scan import ETF_PREFIXES as P
    assert _blocked(P) == [], "universe 에 ETF 가 들어왔다 — 필터 경로를 다시 볼 것"
