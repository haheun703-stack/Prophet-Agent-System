"""[F-237] 외국인소진율 원천 교체 — 신 네이버 trend JSON 파서 + 오염행 필터.

9/11~9/18 6영업일 결손 사고의 fix. 구 `finance.naver.com/item/frgn.naver`가 폐지돼
302로 넘어가면서 2,531종 전건 실패했다.

★이 테스트가 반드시 고정해야 하는 것 = **0.00이 살아남는가**.
  0.00은 오염이 아니라 '외국인 보유가 없다'는 값이고, 옛 필터(`> 0`)는 그것을 버렸다.
  전수 실측 84종(3.37%)이 여기 걸린다.
"""
import pandas as pd

from data import flow_collector as fc


def _row(bizdate, ratio, close="10,000"):
    return {
        "itemCode": "005930",
        "bizdate": bizdate,
        "foreignerHoldRatio": ratio,
        "closePrice": close,
        "foreignerPureBuyQuant": "-1,673,323",
    }


# ------------------------------------------------------------------
#  _parse_ratio_or_none — '미제공'과 '0'을 절대 섞지 않는다
# ------------------------------------------------------------------

def test_parse_ratio_distinguishes_zero_from_missing():
    # ★parametrize를 쓰지 않는다 — 저장소 러너(tools/run_all_tests.py)는 pytest가 아니고
    #   VPS에 pytest가 없다. parametrize로 쓰면 로컬 pytest에서만 PASS하고 **실제
    #   실행 경로에서는 fixture 미구현 ERROR**로 안 돈다(9/19 실제로 그렇게 났다).
    cases = [
        ("46.45%", 46.45),
        ("0.00%", 0.0),          # ★실제 값 0 — None 이면 안 된다
        ("0.01%", 0.01),
        ("-", None),             # ★미제공 — 0.0 이면 안 된다 (실측 7종)
        ("--", None),
        ("", None),
        (None, None),
        ("nan", None),
        ("1,234.5", 1234.5),
    ]
    for raw, expected in cases:
        assert fc._parse_ratio_or_none(raw) == expected, f"입력 {raw!r}"


def test_safe_float_would_have_confused_them():
    """음성대조: 기존 `_safe_float`를 썼다면 '-'가 0.0이 되어 '보유 0'으로 둔갑한다."""
    assert fc._safe_float("-") == 0.0
    assert fc._parse_ratio_or_none("-") is None


# ------------------------------------------------------------------
#  _parse_naver_trend_json — 스키마는 구 파서와 동일해야 한다
# ------------------------------------------------------------------

def test_trend_json_schema_matches_old_parser():
    rows = fc._parse_naver_trend_json("005930", [_row("20260918", "46.45%", "260,000")])
    assert len(rows) == 1
    assert set(rows[0]) == {"date", "소진율", "보유수량", "종가"}
    assert rows[0]["date"] == "2026-09-18"      # compact → ISO
    assert rows[0]["소진율"] == 46.45
    assert rows[0]["종가"] == 260000


def test_holding_is_none_not_zero():
    """★신 원천은 보유수량을 주지 않는다. 0으로 위장하면 '실제 보유 0'과 구분 불가."""
    rows = fc._parse_naver_trend_json("005930", [_row("20260918", "46.45%")])
    assert rows[0]["보유수량"] is None


def test_zero_ratio_row_is_kept():
    """★핵심 — 소진율 0.00 행은 만들어져야 한다(전수 84종)."""
    rows = fc._parse_naver_trend_json("115480", [_row("20260918", "0.00%")])
    assert len(rows) == 1
    assert rows[0]["소진율"] == 0.0


def test_dash_ratio_row_is_dropped_and_counted():
    stats = {}
    rows = fc._parse_naver_trend_json("012510", [_row("20260918", "-")], stats=stats)
    assert rows == []
    assert stats["no_ratio"] == 1


def test_rows_sorted_desc_and_deduped():
    rows = fc._parse_naver_trend_json("005930", [
        _row("20260916", "46.52%"),
        _row("20260918", "46.45%"),
        _row("20260917", "46.48%"),
        _row("20260918", "46.45%"),   # 중복
    ])
    assert [r["date"] for r in rows] == ["2026-09-18", "2026-09-17", "2026-09-16"]


def test_malformed_payload_yields_no_rows_without_raising():
    bads = [
        [{"bizdate": "2026-09-18", "foreignerHoldRatio": "46.45%"}],   # ISO가 오면 거부
        [{"bizdate": "", "foreignerHoldRatio": "46.45%"}],
        [{"foreignerHoldRatio": "46.45%"}],
        [{"bizdate": "20260918"}],                                     # ratio 키 자체 부재
        "not-a-list",
        None,
        [],
        [None, 123, "x"],
    ]
    for bad in bads:
        assert fc._parse_naver_trend_json("005930", bad) == [], f"입력 {bad!r}"


def test_old_html_parser_cannot_read_new_payload():
    """음성대조: 구 HTML 파서는 신 원천 응답을 못 읽는다 — 교체가 필요했던 이유.

    ★9/19 Tier-1 지적 반영 — 초안은 임의 문자열에도 통과하는 약한 단언이었다.
      '구 파서가 고장 난 것이 아니라 **먹이가 바뀐 것**'이 요점이므로, 구 파서가
      진짜 HTML 표는 여전히 읽는다는 대조를 같이 고정한다.
    """
    import json
    payload = json.dumps([_row("20260918", "46.45%", "260,000")])
    assert fc._parse_naver_frgn_html("005930", payload) == []
    # 대조 — 같은 파서가 구 HTML 표는 정상 파싱한다(파서 자체는 멀쩡)
    from tests.test_flow_collector_naver_frgn import NAVER_FRGN_HTML
    assert len(fc._parse_naver_frgn_html("005930", NAVER_FRGN_HTML)) > 0


# ------------------------------------------------------------------
#  오염행 필터 — `> 0` → `결측 아님`
# ------------------------------------------------------------------

# ★9/19 Tier-1 적발 — 초안은 여기에 필터 식을 **복제**해 두고 "구현이 바뀌면 같이
#   깨진다"고 적었으나 정반대였다(프로덕션을 옛 규칙으로 되돌려도 3건 전부 PASS).
#   이제 프로덕션 함수를 그대로 부른다 = 단일 진실.
_apply_filter = fc._drop_empty_exh_rows


def test_filter_keeps_zero_ratio_row_that_old_rule_dropped():
    """★음성대조 — 옛 규칙은 지웠고 새 규칙은 지키는 행."""
    df = pd.DataFrame(
        {"소진율": [0.0], "보유수량": [None], "종가": [2100]},
        index=pd.DatetimeIndex(["2026-09-18"], name="date"),
    )
    old_kept = df[(df["소진율"].fillna(0) > 0) | (df["보유수량"].fillna(0) > 0)]
    assert len(old_kept) == 0        # 옛 규칙이면 사라졌다
    assert len(_apply_filter(df)) == 1


def test_filter_never_deletes_existing_rows():
    """기존 캐시 행(두 컬럼 다 숫자)은 어떤 경우에도 보존 — [F-170] 계열 삭제 차단."""
    df = pd.DataFrame(
        {"소진율": [46.71, 0.0, 12.3], "보유수량": [2730687013, 0, 5000], "종가": [269000, 2100, 8000]},
        index=pd.DatetimeIndex(["2026-09-10", "2026-09-09", "2026-09-08"], name="date"),
    )
    assert len(_apply_filter(df)) == len(df)


def test_filter_drops_only_fully_empty_rows():
    df = pd.DataFrame(
        {"소진율": [None, 1.0], "보유수량": [None, None], "종가": [0, 100]},
        index=pd.DatetimeIndex(["2026-09-17", "2026-09-18"], name="date"),
    )
    out = _apply_filter(df)
    assert len(out) == 1
    assert out.index[0] == pd.Timestamp("2026-09-18")


# ------------------------------------------------------------------
#  fetch 경로 — 카운터가 실패와 정상 부재를 구분하는가
# ------------------------------------------------------------------

class _Resp:
    def __init__(self, payload, status=200):
        self._p = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._p


class _Sess:
    def __init__(self, resp):
        self._resp = resp
        self.last_url = None

    def get(self, url, **kw):
        self.last_url = url
        if isinstance(self._resp, Exception):
            raise self._resp
        return self._resp


def test_fetch_uses_new_endpoint_not_the_dead_one():
    sess = _Sess(_Resp([_row("20260918", "46.45%")]))
    fc._fetch_foreign_rates_naver("005930", http_session=sess)
    assert sess.last_url == "https://m.stock.naver.com/api/stock/005930/trend"
    assert "finance.naver.com" not in sess.last_url


def test_empty_payload_counts_as_no_rows_not_failure():
    stats = {}
    rows = fc._fetch_foreign_rates_naver("232530", http_session=_Sess(_Resp([])), stats=stats)
    assert rows == []
    assert stats.get("no_rows") == 1
    assert stats.get("http_error", 0) == 0     # ★실패로 세면 안 된다 (실측 40종)


def test_http_error_counts_as_http_error():
    stats = {}
    rows = fc._fetch_foreign_rates_naver("005930", http_session=_Sess(_Resp([], 500)), stats=stats)
    assert rows == []
    assert stats.get("http_error") == 1
    assert stats.get("no_rows", 0) == 0


def test_network_exception_is_swallowed_and_counted():
    stats = {}
    rows = fc._fetch_foreign_rates_naver("005930", http_session=_Sess(OSError("boom")), stats=stats)
    assert rows == []
    assert stats.get("http_error") == 1


def test_fetch_without_stats_still_works():
    """stats 미전달 경로(하위호환)에서 터지지 않는다."""
    rows = fc._fetch_foreign_rates_naver("005930", http_session=_Sess(_Resp([_row("20260918", "-")])))
    assert rows == []


# ------------------------------------------------------------------
#  9/19 Tier-1 검수 반영분 (D-1 ~ D-5)
# ------------------------------------------------------------------

def test_request_asks_for_wide_recovery_window():
    """★[D-1] 자가복구 창 — 파라미터 없이 부르면 10거래일뿐이다.

    이번 사고가 6영업일이었으니 여유가 단 4일이었다. 일주일만 늦게 잡았으면 정규
    수집 경로로는 영영 못 채웠고, 그 사실을 알려줄 알림도 없다.
    """
    class _CapSess:
        def __init__(self):
            self.params = None

        def get(self, url, **kw):
            self.params = kw.get("params")
            return _Resp([_row("20260918", "46.45%")])

    sess = _CapSess()
    fc._fetch_foreign_rates_naver("005930", http_session=sess)
    assert sess.params and sess.params.get("pageSize") == fc.NAVER_TREND_PAGE_SIZE
    assert fc.NAVER_TREND_PAGE_SIZE >= 20,         "구 HTML 경로가 받던 20행보다 좁으면 사고 대응력이 후퇴한다"


def test_schema_change_is_counted_not_swallowed():
    """★[D-2] 스키마가 바뀌면 **그렇다고 말해야** 한다.

    네이버가 `[...]` → `{"trend":[...]}` 로 감싸면 전건 실패인데, 초안은 어떤 카운터도
    올리지 않아 로그가 `실패2531 [HTTP오류0/상폐·정지0/...]` — 9/11 사고 때와 같은
    정보량이었다. 이 fix의 목적 자체가 미달이던 지점.
    """
    for payload in ({"trend": [_row("20260918", "46.45%")]}, "<html>302</html>", 123):
        stats = {}
        rows = fc._fetch_foreign_rates_naver("005930",
                                             http_session=_Sess(_Resp(payload)), stats=stats)
        assert rows == []
        assert stats.get("schema_error") == 1, f"스키마 이상이 안 세어졌다: {payload!r}"
        assert stats.get("no_rows", 0) == 0, "스키마 이상을 '상폐·정지'로 오분류했다"


def test_no_ratio_is_counted_per_stock_not_per_row():
    """★[D-4] 단위 혼재 — 한 종목 30행이 '30'으로 찍히면 옆 카운터와 자릿수가 다르다."""
    stats = {}
    rows = fc._fetch_foreign_rates_naver(
        "012510",
        http_session=_Sess(_Resp([_row(f"202609{d:02d}", "-") for d in (14, 15, 16, 17, 18)])),
        stats=stats,
    )
    assert rows == []
    assert stats.get("no_ratio") == 1, f"행 단위로 셌다: {stats}"


def test_close_price_missing_is_none_not_zero():
    """★[D-5] 종가도 0으로 위장하지 않는다.

    병합이 셀 단위라 0은 '값'으로 취급돼 **기존 캐시의 실제 종가를 0으로 덮는다**.
    소진율에만 이 원칙을 세우고 바로 옆 줄에서 깨뜨리고 있었다.
    """
    rows = fc._parse_naver_trend_json("005930", [
        {"bizdate": "20260918", "foreignerHoldRatio": "46.45%"},              # closePrice 키 부재
        {"bizdate": "20260917", "foreignerHoldRatio": "46.48%", "closePrice": None},
        {"bizdate": "20260916", "foreignerHoldRatio": "46.52%", "closePrice": "-"},
        {"bizdate": "20260915", "foreignerHoldRatio": "46.56%", "closePrice": "253,500"},
    ])
    by_date = {r["date"]: r for r in rows}
    assert by_date["2026-09-18"]["종가"] is None
    assert by_date["2026-09-17"]["종가"] is None
    assert by_date["2026-09-16"]["종가"] is None
    assert by_date["2026-09-15"]["종가"] == 253500
