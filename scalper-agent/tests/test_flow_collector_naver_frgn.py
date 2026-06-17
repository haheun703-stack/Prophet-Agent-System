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
