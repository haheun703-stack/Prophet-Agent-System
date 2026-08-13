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


class FakeResponse:
    text = NAVER_FRGN_HTML
    encoding = None

    def raise_for_status(self):
        return None


class FakeSession:
    def get(self, *args, **kwargs):
        return FakeResponse()


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
    assert restored["보유수량"] == 2_810_201_369
    assert restored["종가"] == 360_500


def test_collect_foreign_exhaustion_still_drops_zero_rows(tmp_path, monkeypatch):
    """전량 병합으로 바꿔도 빈/오염 행(소진율·보유수량 동시 0) 방어는 살아 있다."""
    flow_dir = tmp_path / "flow"
    flow_dir.mkdir(parents=True)

    cache_file = flow_dir / "005930_foreign_exh.csv"
    pd.DataFrame(
        [{"소진율": 0.0, "보유수량": 0, "종가": 350_000}],
        index=pd.DatetimeIndex(["2026-06-03"], name="date"),
    ).to_csv(cache_file)

    monkeypatch.setattr(fc, "FLOW_DIR", flow_dir)
    monkeypatch.setattr(fc, "SHORT_DIR", tmp_path / "short")
    monkeypatch.setattr(fc, "NAT_DIR", tmp_path / "nationality")
    monkeypatch.setattr(fc._requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(fc.time, "sleep", lambda *_: None)

    fc.collect_foreign_exhaustion(["005930"], force=False)

    saved = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    assert "2026-06-03" not in {idx.strftime("%Y-%m-%d") for idx in saved.index}


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

    result = fc.collect_foreign_exhaustion(["005930"], force=False)

    df = result["005930"]
    saved = pd.read_csv(ghost_file, index_col=0, parse_dates=True)
    assert df.index[-1].strftime("%Y-%m-%d") == "2026-06-04"
    assert saved.index[-1].strftime("%Y-%m-%d") == "2026-06-04"
    assert "2026-06-05" not in {idx.strftime("%Y-%m-%d") for idx in saved.index}
    assert saved.iloc[-1]["소진율"] == 47.81
    assert saved.iloc[-1]["보유수량"] == 2_795_254_819
    assert saved.iloc[-1]["종가"] == 351_500
