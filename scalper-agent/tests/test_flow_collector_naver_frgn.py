import pandas as pd

from data import flow_collector as fc


NAVER_FRGN_HTML = """
<html>
  <body>
    <table>
      <thead>
        <tr>
          <th>날짜</th>
          <th>종가</th>
          <th>전일비</th>
          <th>등락률</th>
          <th>거래량</th>
          <th>기관</th>
          <th>외국인</th>
          <th>보유주식수</th>
          <th>보유율</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>2026.06.04</td>
          <td>351,500</td>
          <td>-9,000</td>
          <td>-2.50%</td>
          <td>1,234,567</td>
          <td>100</td>
          <td>200</td>
          <td>2,795,254,819</td>
          <td>47.81%</td>
        </tr>
        <tr>
          <td>2026.06.02</td>
          <td>360,500</td>
          <td>11,500</td>
          <td>3.30%</td>
          <td>2,345,678</td>
          <td>110</td>
          <td>210</td>
          <td>2,810,201,369</td>
          <td>48.07%</td>
        </tr>
        <tr>
          <td>2026.06.01</td>
          <td>349,000</td>
          <td>4,500</td>
          <td>1.31%</td>
          <td>3,456,789</td>
          <td>120</td>
          <td>220</td>
          <td>2,823,815,351</td>
          <td>48.30%</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


# ★9/19 [F-237] — 원천이 구 HTML에서 신 trend JSON으로 바뀌었다. 아래 Fake를 갈아끼운다.
#   ★이 파일의 존재 이유는 원천이 아니라 **[F-170]이 지키는 성질**이다:
#     ①스테일 응답이 최근 행을 지우지 않는다 ②중간 거래일이 자가복구된다
#     ③미래 일자는 잘린다 ④빈 행은 안 쌓인다.
#   원천을 바꾸면서 이 6건이 통째로 죽어 있었다(러너가 적발). 성질은 원천과 무관하므로
#   Fake만 교체하고 단언은 유지한다 — **재발 방지막을 걷어내지 않는다**.
NAVER_TREND_JSON = [
    {"bizdate": "20260604", "foreignerHoldRatio": "47.81%", "closePrice": "351,500"},
    {"bizdate": "20260602", "foreignerHoldRatio": "48.07%", "closePrice": "360,500"},
    {"bizdate": "20260601", "foreignerHoldRatio": "48.30%", "closePrice": "349,000"},
]


class FakeResponse:
    text = NAVER_FRGN_HTML          # 구 파서 단위 테스트용으로 남겨 둔다
    encoding = None

    def __init__(self, payload=None):
        self._payload = NAVER_TREND_JSON if payload is None else payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload=None):
        self._payload = payload

    def get(self, *args, **kwargs):
        return FakeResponse(self._payload)


def test_parse_naver_frgn_html_uses_trading_date():
    rows = fc._parse_naver_frgn_html("005930", NAVER_FRGN_HTML)

    assert rows[0]["date"] == "2026-06-04"
    assert rows[0]["소진율"] == 47.81
    assert rows[0]["보유수량"] == 2_795_254_819
    assert rows[0]["종가"] == 351_500
    assert "2026-06-03" not in {row["date"] for row in rows}


def test_fetch_foreign_rates_naver_returns_every_trading_day(monkeypatch):
    """★[F-170] 페이지가 준 거래일을 전부 돌려준다 — 되채움의 원천."""
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())

    rows = fc._fetch_foreign_rates_naver("005930")

    assert [r["date"] for r in rows] == ["2026-06-04", "2026-06-02", "2026-06-01"]


def test_fetch_foreign_rate_naver_singular_still_returns_latest_only(monkeypatch):
    """하위호환 — `_fetch_foreign_rate_api`가 여전히 최신 1행만 받는다."""
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())

    row = fc._fetch_foreign_rate_naver("005930")

    assert row["date"] == "2026-06-04"
    assert row["소진율"] == 47.81


def test_collect_foreign_exhaustion_backfills_missing_middle_date(tmp_path, monkeypatch):
    """★[F-170] 회귀 — ghost 컷이 지운 중간 거래일이 다음 수집에서 자가복구된다.

    구코드(`rows[0]` 1행만 병합)에서는 06-02가 영영 비어 있었다. 8/13 실측으로
    9거래일 226건의 구멍이 이 경로로 생겼음을 확인했다(음성대조 대상 테스트).
    """
    flow_dir = tmp_path / "flow"
    flow_dir.mkdir(parents=True)

    cache_file = flow_dir / "005930_foreign_exh.csv"
    # 06-02가 빠진 캐시 = 실제 VPS에서 관측된 '구멍' 모양 그대로
    pd.DataFrame(
        [
            {"소진율": 48.30, "보유수량": 2_823_815_351, "종가": 349_000},
            {"소진율": 47.81, "보유수량": 2_795_254_819, "종가": 351_500},
        ],
        index=pd.DatetimeIndex(["2026-06-01", "2026-06-04"], name="date"),
    ).to_csv(cache_file)

    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", tmp_path / "short")
    monkeypatch.setattr(fc, "NAT_DIR", tmp_path / "nationality")
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    dates = [idx.strftime("%Y-%m-%d") for idx in saved.index]
    assert dates == ["2026-06-01", "2026-06-02", "2026-06-04"]
    restored = saved.loc[pd.Timestamp("2026-06-02")]
    assert restored["소진율"] == 48.07
    assert restored["종가"] == 360_500
    # ★[F-237] 신 원천은 보유수량을 주지 않는다 → 공란. 0으로 채우면 '실제 보유 0'과
    #   구분이 사라지므로 **비어 있는 것이 올바른 상태**다. 백필은 소진율로 성립한다.
    assert pd.isna(restored["보유수량"])


def test_stale_naver_response_must_not_delete_recent_rows(tmp_path, monkeypatch):
    """★[F-170] 핵심 회귀 — 네이버가 스테일 응답을 줘도 최근 정상 행을 지우면 안 된다.

    구코드는 `df.index <= row_date`(네이버 최신일)로 잘라서, 네이버가 06-04 까지만 주면
    캐시의 06-05~06-09 를 **삭제**했다. 소형주는 네이버가 과거 보유주수를 0으로 덮으므로
    이렇게 지워진 행은 **영구 복구 불가**다(8/11 −6 실측이 이 경로).
    """
    flow_dir = tmp_path / "flow"
    flow_dir.mkdir(parents=True)

    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [
            {"소진율": 47.81, "보유수량": 2_795_254_819, "종가": 351_500},
            {"소진율": 47.90, "보유수량": 2_800_000_000, "종가": 352_000},
            {"소진율": 48.00, "보유수량": 2_805_000_000, "종가": 353_000},
        ],
        index=pd.DatetimeIndex(["2026-06-04", "2026-06-05", "2026-06-09"], name="date"),
    ).to_csv(cache_file)

    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", tmp_path / "short")
    monkeypatch.setattr(fc, "NAT_DIR", tmp_path / "nationality")
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)
    # 오늘은 06-10 인데 네이버(FakeSession)는 06-04 까지만 준다 = 스테일 응답
    monkeypatch.setattr(fc, "_today_kst_ts", lambda: pd.Timestamp("2026-06-10"))

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    dates = {idx.strftime("%Y-%m-%d") for idx in saved.index}
    assert "2026-06-05" in dates, "스테일 응답이 정상 행을 지웠다 — [F-170] 재발"
    assert "2026-06-09" in dates, "스테일 응답이 정상 행을 지웠다 — [F-170] 재발"


def test_future_dated_row_is_still_cut(tmp_path, monkeypatch):
    """미래 일자 행(정상 경로로는 생길 수 없음)은 여전히 잘린다 — 방어 목적 보존."""
    flow_dir = tmp_path / "flow"
    flow_dir.mkdir(parents=True)

    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 99.0, "보유수량": 1, "종가": 1}],
        index=pd.DatetimeIndex(["2026-06-20"], name="date"),
    ).to_csv(cache_file)

    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", tmp_path / "short")
    monkeypatch.setattr(fc, "NAT_DIR", tmp_path / "nationality")
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)
    monkeypatch.setattr(fc, "_today_kst_ts", lambda: pd.Timestamp("2026-06-10"))

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert "2026-06-20" not in {idx.strftime("%Y-%m-%d") for idx in saved.index}


def _setup_flow(tmp_path, monkeypatch, payload=None):
    flow_dir = tmp_path / "flow"
    flow_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", tmp_path / "short")
    monkeypatch.setattr(fc, "NAT_DIR", tmp_path / "nationality")
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession(payload))
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)
    return flow_dir


def test_zero_ratio_row_is_now_kept_not_dropped(tmp_path, monkeypatch):
    """★9/19 [F-237] **의도된 동작 변경** — 소진율 0.00 행을 더 이상 버리지 않는다.

    옛 규칙은 `(소진율 > 0) | (보유수량 > 0)` 이었다. 신 원천이 보유수량을 주지 않으므로
    그 조건은 한쪽 날개가 꺾인 채 돌아가고, **소진율 0.00인 84종(전수 실측 3.37%)**이
    매일 행을 못 쌓아 조용히 빈다. 그리고 0.00은 오염이 아니라 '외국인 보유가 없다'는
    값이다. 값을 값으로 취급하지 않은 옛 규칙이 틀렸다.
    """
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 0.0, "보유수량": 0, "종가": 350_000}],
        index=pd.DatetimeIndex(["2026-06-03"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert "2026-06-03" in {idx.strftime("%Y-%m-%d") for idx in saved.index},         "소진율 0.00 행이 사라졌다 — 84종이 매일 비게 된다 [F-237]"


def test_truly_empty_row_is_still_dropped(tmp_path, monkeypatch):
    """빈 행 방어는 살아 있다 — 소진율·보유수량이 **둘 다 결측**인 행만 제거."""
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": None, "보유수량": None, "종가": 350_000}],
        index=pd.DatetimeIndex(["2026-06-03"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert "2026-06-03" not in {idx.strftime("%Y-%m-%d") for idx in saved.index}


def test_merge_must_not_wipe_existing_holding_qty(tmp_path, monkeypatch):
    """★★9/19 [F-237] 최우선 회귀 — 병합이 기존 보유수량을 지우면 안 된다.

    신 원천은 보유수량을 주지 않는다. 행 단위 교체(`concat` + `duplicated(keep="last")`)
    를 쓰면 같은 날짜의 **기존 실값이 결측으로 덮인다**. 신 원천이 매번 10거래일을 주므로
    수집 때마다 3,510파일 × 최근 10행이 조용히 지워지고, 되채울 원천이 없다.
    8/13 [F-170]과 같은 얼굴이며, 9/19 검수에서 실제로 재현돼 잡혔다.
    """
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    # 캐시에는 FakeSession이 줄 날짜와 **같은 날짜**의 보유수량 실값이 있다
    pd.DataFrame(
        [{"소진율": 48.30, "보유수량": 2_823_815_351, "종가": 349_000},
         {"소진율": 47.81, "보유수량": 2_795_254_819, "종가": 351_500}],
        index=pd.DatetimeIndex(["2026-06-01", "2026-06-04"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert saved.loc[pd.Timestamp("2026-06-01"), "보유수량"] == 2_823_815_351,         "기존 보유수량이 지워졌다 — [F-170] 계열 조용한 삭제 재발"
    assert saved.loc[pd.Timestamp("2026-06-04"), "보유수량"] == 2_795_254_819,         "기존 보유수량이 지워졌다 — [F-170] 계열 조용한 삭제 재발"


def test_merge_lets_new_ratio_win_over_stale_cache(tmp_path, monkeypatch):
    """보유수량을 지키는 대신 **신규 값이 이기는 성질**까지 잃으면 안 된다.

    보존만 하고 갱신을 못 하면 소진율이 영원히 옛 값으로 굳는다 — 반대 방향의 사고.
    """
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    # 06-04 소진율을 일부러 틀린 값으로 둔다. 원천은 47.81 을 준다.
    pd.DataFrame(
        [{"소진율": 11.11, "보유수량": 2_795_254_819, "종가": 111_111}],
        index=pd.DatetimeIndex(["2026-06-04"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    row = saved.loc[pd.Timestamp("2026-06-04")]
    assert row["소진율"] == 47.81, "신규 소진율이 반영되지 않았다"
    assert row["종가"] == 351_500, "신규 종가가 반영되지 않았다"
    assert row["보유수량"] == 2_795_254_819, "보유수량은 기존 값을 지켜야 한다"


def test_csv_header_order_is_stable_after_merge(tmp_path, monkeypatch):
    """combine_first 는 컬럼을 알파벳순으로 재배열한다 — CSV 헤더가 흔들리면 안 된다."""
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 48.30, "보유수량": 2_823_815_351, "종가": 349_000}],
        index=pd.DatetimeIndex(["2026-06-01"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    header = cache_file.read_text(encoding="utf-8").splitlines()[0]
    assert header == "date,소진율,보유수량,종가", f"CSV 헤더가 바뀌었다: {header}"


def test_holding_qty_keeps_integer_csv_notation(tmp_path, monkeypatch):
    """★[F-237] 보유수량 표기가 `...0.0` 으로 바뀌면 안 된다.

    결측이 섞이면 pandas 가 컬럼을 float 로 올려 전 파일의 표기가 바뀐다. 값은 같지만
    **재수집 전후 바이트 재현** 대조가 죽는다 — 8/13·8/21 삭제 사고를 실제로 잡아낸 기법이다.
    """
    flow_dir = _setup_flow(tmp_path, monkeypatch)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 48.30, "보유수량": 2_823_815_351, "종가": 349_000}],
        index=pd.DatetimeIndex(["2026-06-01"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    text = cache_file.read_text(encoding="utf-8")
    assert "2823815351" in text, "보유수량 값이 사라졌다"
    assert "2823815351.0" not in text, "보유수량이 float 표기로 바뀌었다 — 바이트 대조가 죽는다"
    # 신 원천이 주지 않는 날짜(06-04)의 보유수량 칸은 비어 있어야 한다
    line_0604 = [l for l in text.splitlines() if l.startswith("2026-06-04")]
    assert line_0604 and line_0604[0].split(",")[2] == "", f"빈 칸이 아니다: {line_0604}"


def test_missing_close_must_not_overwrite_cached_close(tmp_path, monkeypatch):
    """★[F-237·D-5] 종가 결측이 기존 실값을 0으로 덮으면 안 된다 (배선 층).

    `_safe_int` 기본값 0 + 셀 단위 병합의 조합에서 나오는 조용한 파괴. 파서 단위
    테스트만으로는 안 잡히고 `collect_foreign_exhaustion` 을 타야 드러난다(5/25 교훈).
    """
    payload = [{"bizdate": "20260604", "foreignerHoldRatio": "47.90%"}]   # closePrice 없음
    flow_dir = _setup_flow(tmp_path, monkeypatch, payload=payload)
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 47.81, "보유수량": 2_795_254_819, "종가": 351_500}],
        index=pd.DatetimeIndex(["2026-06-04"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    row = saved.loc[pd.Timestamp("2026-06-04")]
    assert row["종가"] == 351_500, "결측 종가가 기존 실값을 덮었다 [F-237·D-5]"
    assert row["소진율"] == 47.90, "신규 소진율은 반영돼야 한다"


def test_delisted_stock_is_not_counted_as_failure(tmp_path, monkeypatch):
    """★[F-237·D-3] 상폐·정지(빈 응답)는 '실패'가 아니다 — 주석이 선언한 대로.

    전수 실측 40종이 매일 `실패40` 으로 찍히면 반복되는 숫자가 배경이 되고,
    진짜 실패가 그 안에 섞여도 안 보인다.
    """
    import io as _io
    import contextlib

    flow_dir = _setup_flow(tmp_path, monkeypatch, payload=[])   # 상폐·정지 모양
    buf = _io.StringIO()
    with contextlib.redirect_stdout(buf):
        fc.collect_foreign_exhaustion(["232530"], force=False)
    out = buf.getvalue()
    assert "실패0" in out, f"상폐·정지가 실패로 계수됐다: {out.strip().splitlines()[-1]}"
    assert "상폐·정지1" in out, f"정상 부재가 내역에 안 보인다: {out.strip().splitlines()[-1]}"


def test_schema_change_shows_up_in_the_summary_line(tmp_path, monkeypatch):
    """★[F-237·D-2] 스키마 변경이 요약 한 줄에서 구분돼야 한다(9/11 사고의 교훈)."""
    import io as _io
    import contextlib

    _setup_flow(tmp_path, monkeypatch, payload={"trend": []})
    buf = _io.StringIO()
    with contextlib.redirect_stdout(buf):
        fc.collect_foreign_exhaustion(["005930"], force=False)
    out = buf.getvalue()
    assert "스키마이상1" in out, f"스키마 이상이 안 보인다: {out.strip().splitlines()[-1]}"


def test_new_source_outage_must_not_wipe_the_cache(tmp_path, monkeypatch):
    """★9/19 사고 자체의 회귀 — 원천이 또 죽어도 기존 캐시를 지우면 안 된다.

    9/11~9/18에 2,531종 전건 실패했을 때 **기존 행은 살아남았다**(그래서 소급 대조가
    가능했다). 빈 응답에 캐시를 덮어쓰는 구현으로 바뀌면 다음 사고는 복구 불가가 된다.
    """
    flow_dir = _setup_flow(tmp_path, monkeypatch, payload=[])   # 원천 사망 = 빈 응답
    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 46.71, "보유수량": 2_730_687_013, "종가": 269_000}],
        index=pd.DatetimeIndex(["2026-06-03"], name="date"),
    ).to_csv(cache_file)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert "2026-06-03" in {idx.strftime("%Y-%m-%d") for idx in saved.index}
    assert saved.iloc[-1]["소진율"] == 46.71


def test_collect_foreign_exhaustion_writes_latest_trading_date_and_removes_ghost(tmp_path, monkeypatch):
    flow_dir = tmp_path / "flow"
    short_dir = tmp_path / "short"
    nat_dir = tmp_path / "nationality"
    flow_dir.mkdir(parents=True)

    ghost_file = flow_dir / "005930_foreign_exh.csv"
    ghost_df = pd.DataFrame(
        [{"소진율": 54.0, "보유수량": 3_100_000_000, "종가": 351_500}],
        index=pd.DatetimeIndex(["2026-06-05"], name="date"),
    )
    ghost_df.to_csv(ghost_file)

    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", short_dir)
    monkeypatch.setattr(fc, "NAT_DIR", nat_dir)
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)
    # ★[F-170] 기준일 고정 — 06-05는 '오늘(06-04)' 기준 **미래 일자**라 ghost로 잘린다.
    #   (예전엔 '네이버 최신일 이후'라 잘렸다. 지금은 미래인 것만 자른다.)
    monkeypatch.setattr(fc, "_today_kst_ts", lambda: pd.Timestamp("2026-06-04"))

    result = fc.collect_foreign_exhaustion(["005930"], force=False)

    df = result["005930"]
    saved = pd.read_csv(ghost_file, index_col=0, parse_dates=True)
    assert df.index[-1].strftime("%Y-%m-%d") == "2026-06-04"
    assert saved.index[-1].strftime("%Y-%m-%d") == "2026-06-04"
    assert "2026-06-05" not in {idx.strftime("%Y-%m-%d") for idx in saved.index}
    assert saved.iloc[-1]["소진율"] == 47.81
    assert saved.iloc[-1]["종가"] == 351_500
    assert pd.isna(saved.iloc[-1]["보유수량"])     # [F-237] 신 원천 미제공
