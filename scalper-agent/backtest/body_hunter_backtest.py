"""
Body Hunter v2 백테스터 - 실제 CSV 데이터 기반
일봉 필터 + 상대강도 스캔 + 몸통포착 v2 통합 검증

v2 변경:
  - 일봉 필터 사전 적용 (B등급 이상만 후보)
  - LONG ONLY 모드
  - body_hunter_v2 사용 (수익잠금, 개선 트레일링)
  - 외국인/기관 수급 데이터 활용
  - 최대 3종목 동시 감시
"""

import logging
import sys
from pathlib import Path
from dataclasses import asdict
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

# 프로젝트 루트 경로 설정
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.csv_loader import CSVLoader
from strategies.etf_filter import ETFDirectionFilter, MarketDirection
from strategies.scanner import RelativeStrengthScanner, StockCandidate
from strategies.daily_filter import DailyFilter, DailyScore
from strategies.body_hunter_v2 import BodyHunterV2, BodyState
from engine.body_hunter_master import BodyHunterMaster, DailyTradeResult

logger = logging.getLogger(__name__)


class BodyHunterCSVBacktester:
    """
    실제 CSV 데이터 기반 Body Hunter v2 백테스트

    흐름:
      1. 전 종목 일봉 로드 + 일봉 필터 적용
      2. KODEX200 일봉 -> ETF 방향 판단
      3. 일봉 필터 통과 종목 중 상대강도 상위 3종목 추출
      4. 5분봉 시뮬레이션 → 몸통 포착/탑승/청산
      5. 일일 리포트 + 전체 성과
    """

    def __init__(
        self,
        csv_dir:        str   = "../stock_data_daily",
        top_n:          int   = 3,
        risk_per_trade: float = 50_000,
        etf_code:       str   = "069500",
        long_only:      bool  = True,
        min_grade:      str   = "B",
    ):
        self.csv_dir        = csv_dir
        self.top_n          = top_n
        self.risk_per_trade = risk_per_trade
        self.etf_code       = etf_code
        self.long_only      = long_only
        self.min_grade      = min_grade

        self.loader = CSVLoader(csv_dir)
        self.code_to_name = self.loader.get_code_name_map()
        self.daily_filter = DailyFilter()

        self.all_results:  List[DailyTradeResult] = []
        self.equity_curve: List[float] = [0.0]
        self.daily_stats:  List[dict]  = []

    def run(
        self,
        start_date: str = "2025-06-01",
        end_date:   str = "2025-12-31",
        n_stocks:   int = 200,
    ) -> pd.DataFrame:
        """실제 데이터 기반 백테스트"""
        print(f"\n{'='*60}")
        print(f"  Body Hunter v2 백테스트")
        print(f"  기간: {start_date} ~ {end_date}")
        print(f"  상위: {top_n}종목 | LONG ONLY: {self.long_only}")
        print(f"  일봉 최소등급: {self.min_grade} | 리스크: {self.risk_per_trade:,}원")
        print(f"{'='*60}")

        # ETF 데이터 로드
        etf_df = self._load_etf_data(start_date, end_date)
        if etf_df is None or etf_df.empty:
            print("  ETF 데이터 없음 - 삼성전자로 대용")
            etf_df = self._make_proxy_etf(start_date, end_date)

        # 전체 종목 데이터 로드
        all_stocks, all_stocks_full = self._load_all_stocks(start_date, end_date, n_stocks)
        print(f"  로드 종목: {len(all_stocks)}개")

        # 거래일 목록
        dates = sorted(etf_df["Date"].unique())
        print(f"  거래일: {len(dates)}일\n")

        for i, date in enumerate(dates):
            day_results = self._simulate_day(date, etf_df, all_stocks, all_stocks_full)
            self.all_results.extend(day_results)

            day_pnl = sum(self.risk_per_trade * r.rr_realized for r in day_results)
            self.equity_curve.append(self.equity_curve[-1] + day_pnl)

            self.daily_stats.append({
                "date": date,
                "trades": len(day_results),
                "pnl": day_pnl,
                "cumulative": self.equity_curve[-1],
            })

            if (i + 1) % 20 == 0:
                print(f"  {i+1}/{len(dates)}일 처리... 누적PnL: {self.equity_curve[-1]:+,.0f}원")

        return self._compile_report()

    def _load_stock(self, code, start, end) -> Optional[pd.DataFrame]:
        """종목 일봉 로드 → Title case 컬럼"""
        df = self.loader.load(code, start_date=start, end_date=end)
        if df is None or df.empty:
            return None
        col_map = {"date": "Date", "open": "Open", "high": "High",
                   "low": "Low", "close": "Close", "volume": "Volume"}
        df = df.rename(columns=col_map)
        if "Date" in df.columns:
            df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        return df

    def _load_stock_full(self, code, start, end) -> Optional[pd.DataFrame]:
        """종목 전체 컬럼 로드 (일봉 필터용, 소문자 컬럼 유지)"""
        return self.loader.load(code, start_date=start, end_date=end)

    def _load_etf_data(self, start, end) -> Optional[pd.DataFrame]:
        return self._load_stock(self.etf_code, start, end)

    def _make_proxy_etf(self, start, end) -> pd.DataFrame:
        df = self._load_stock("005930", start, end)
        return df if df is not None else pd.DataFrame()

    def _load_all_stocks(self, start, end, n_stocks) -> tuple:
        """전체 종목 데이터 로드 (Title case + 원본 소문자 두 버전)"""
        stocks = {}        # Title case (scanner/backtest용)
        stocks_full = {}   # 소문자 (일봉 필터용)

        top_list = self.loader.get_top_volume(n=n_stocks)

        for code, name, vol in top_list:
            try:
                df = self._load_stock(code, start, end)
                df_full = self._load_stock_full(code, start, end)
                if df is not None and len(df) > 20:
                    stocks[code] = df
                if df_full is not None and len(df_full) > 60:
                    stocks_full[code] = df_full
            except Exception:
                continue

        return stocks, stocks_full

    def _build_daily_whitelist(
        self, date: str, all_stocks_full: Dict[str, pd.DataFrame]
    ) -> Dict[str, tuple]:
        """특정 날짜 기준 일봉 필터 → 화이트리스트 생성"""
        whitelist = {}

        for code, df in all_stocks_full.items():
            # 해당 날짜까지의 데이터만 사용 (미래 데이터 참조 방지)
            if "date" in df.columns:
                target_date = pd.to_datetime(date)
                df_until = df[df["date"] <= target_date]
            else:
                df_until = df

            if len(df_until) < 60:
                continue

            name = self.code_to_name.get(code, code)
            score = self.daily_filter.score_stock(df_until, code, name)

            if score and score.total_score >= self.daily_filter.min_score:
                grade_order = {"S": 4, "A": 3, "B": 2, "C": 1}
                min_val = grade_order.get(self.min_grade, 2)
                if grade_order.get(score.grade, 0) >= min_val:
                    whitelist[code] = (score.total_score, score.grade)

        return whitelist

    def _simulate_day(
        self, date: str, etf_df: pd.DataFrame,
        all_stocks: Dict[str, pd.DataFrame],
        all_stocks_full: Dict[str, pd.DataFrame],
    ) -> List[DailyTradeResult]:
        """하루 시뮬레이션 (v2: 일봉 필터 적용)"""

        # 1. ETF 방향 판단
        etf_row = etf_df[etf_df["Date"] == date]
        if etf_row.empty:
            return []
        etf_row = etf_row.iloc[0]

        etf_change = (etf_row["Close"] - etf_row["Open"]) / etf_row["Open"] * 100

        # LONG ONLY: ETF 하락이면 스킵
        if self.long_only and etf_change < 0:
            return []

        etf_idx = etf_df[etf_df["Date"] == date].index[0]
        etf_hist = etf_df.loc[:etf_idx]
        avg_etf_vol = etf_hist["Volume"].tail(20).mean()

        etf_filter = ETFDirectionFilter(min_change=0.15)
        etf_candle = pd.Series({
            "open": etf_row["Open"], "close": etf_row["Close"], "volume": etf_row["Volume"]
        })
        signal = etf_filter.judge(etf_candle, avg_volume=avg_etf_vol)

        if signal.direction == MarketDirection.NEUTRAL:
            return []
        if self.long_only and signal.direction == MarketDirection.SHORT:
            return []

        direction = "LONG"

        # 2. 일봉 필터 → 화이트리스트
        whitelist = self._build_daily_whitelist(date, all_stocks_full)
        if not whitelist:
            return []

        # 3. 상대강도 스캔 (화이트리스트 내에서만)
        stock_candles = {}
        avg_volumes = {}
        stock_names = {}

        for code, df in all_stocks.items():
            row = df[df["Date"] == date]
            if row.empty:
                continue
            row = row.iloc[0]

            stock_candles[code] = pd.Series({
                "open": row["Open"], "high": row["High"],
                "low": row["Low"], "close": row["Close"],
                "volume": row["Volume"],
            })

            idx = df[df["Date"] == date].index[0]
            avg_volumes[code] = df.loc[:idx, "Volume"].tail(20).mean()
            stock_names[code] = self.code_to_name.get(code, code)

        scanner = RelativeStrengthScanner(
            top_n=self.top_n, min_relative_str=1.5, min_volume_ratio=1.2,
        )
        candidates = scanner.scan(
            etf_change=etf_change, market_dir=signal.direction,
            stock_data=stock_candles, avg_volumes=avg_volumes,
            stock_names=stock_names,
            daily_whitelist=whitelist,
            long_only=self.long_only,
        )

        if not candidates:
            return []

        # 4. 각 종목 몸통 시뮬레이션 (v2 엔진)
        day_results = []
        for cand in candidates[:self.top_n]:
            result = self._simulate_stock_body(cand, direction, date, all_stocks)
            if result:
                day_results.append(result)

        return day_results

    def _simulate_stock_body(
        self, cand: StockCandidate, direction: str,
        date: str, all_stocks: Dict[str, pd.DataFrame]
    ) -> Optional[DailyTradeResult]:
        """일봉 기반 개별 종목 몸통 시뮬레이션 (v2 엔진)"""
        df = all_stocks.get(cand.ticker)
        if df is None:
            return None

        row = df[df["Date"] == date]
        if row.empty:
            return None
        row = row.iloc[0]

        candles = self._daily_to_5min(row, direction, cand)

        hunter = BodyHunterV2(
            ticker=cand.ticker, name=cand.name, direction=direction,
            retest_required=True, volume_surge_min=1.5,
            trailing_atr_mult=1.2, breakeven_rr=0.3, trailing_rr=1.0,
            exhaustion_bars=2,
        )

        hunter.set_levels(candles[0], avg_volume=candles[0]["volume"])

        for i, candle in enumerate(candles[1:], 1):
            ts = pd.Timestamp(f"{date} {9+i//12:02d}:{(i%12)*5:02d}", tz="Asia/Seoul")
            candle.name = ts

            result = hunter.update(candle)

            if result["action"] == "EXIT":
                pos = result.get("position")
                if not pos:
                    break

                entry = pos.entry_price
                exit_p = result.get("exit_price", candle["close"])

                if direction == "LONG":
                    pnl_pct = (exit_p - entry) / entry * 100
                else:
                    pnl_pct = (entry - exit_p) / entry * 100

                ex_signals = result["exhaustion"].signals if result.get("exhaustion") else []

                return DailyTradeResult(
                    date=date, ticker=cand.ticker, name=cand.name,
                    direction=direction, entry_price=entry, exit_price=exit_p,
                    exit_reason=result.get("reason", ""),
                    rr_realized=result.get("rr_realized", 0),
                    hold_bars=result.get("hold_bars", 0),
                    pnl_pct=pnl_pct, exhaustion_signals=ex_signals,
                )

        return None

    @staticmethod
    def _daily_to_5min(
        row: pd.Series, direction: str, cand: StockCandidate, n_bars: int = 72
    ) -> List[pd.Series]:
        """일봉 OHLCV -> 72개 5분봉 근사 생성"""
        rng = np.random.default_rng(hash(f"{row['Date']}_{cand.ticker}") % 2**32)

        o_day = row["Open"]
        h_day = row["High"]
        l_day = row["Low"]
        c_day = row["Close"]
        v_day = row["Volume"]

        candles = []
        price = o_day
        v_per_bar = v_day / n_bars

        body_start = 12
        body_end = rng.integers(40, 55)

        for i in range(n_bars):
            if i == 0:
                bar_h = o_day + (h_day - o_day) * 0.3
                bar_l = o_day - (o_day - l_day) * 0.3
                bar_o = o_day
                bar_c = (bar_h + bar_l) / 2
                bar_v = v_per_bar * 1.2
            elif i < body_start:
                progress = i / body_start
                if direction == "LONG":
                    target = o_day + (h_day - o_day) * progress * 0.4
                else:
                    target = o_day - (o_day - l_day) * progress * 0.4
                noise = rng.normal(0, (h_day - l_day) * 0.02)
                bar_c = target + noise
                bar_o = price
                bar_h = max(bar_o, bar_c) * (1 + abs(rng.normal(0, 0.002)))
                bar_l = min(bar_o, bar_c) * (1 - abs(rng.normal(0, 0.002)))
                bar_v = v_per_bar * rng.uniform(1.0, 2.5)
            elif i < body_end:
                progress = (i - body_start) / (body_end - body_start)
                if direction == "LONG":
                    target = o_day + (h_day - o_day) * (0.3 + progress * 0.7)
                else:
                    target = o_day - (o_day - l_day) * (0.3 + progress * 0.7)
                strength = 1.0 - progress * 0.5
                noise = rng.normal(0, (h_day - l_day) * 0.015 * strength)
                bar_c = target + noise
                bar_o = price
                bar_h = max(bar_o, bar_c) * (1 + abs(rng.normal(0, 0.001)))
                bar_l = min(bar_o, bar_c) * (1 - abs(rng.normal(0, 0.001)))
                bar_v = v_per_bar * rng.uniform(1.5, 3.0) * strength
            else:
                progress = (i - body_end) / max(1, n_bars - body_end)
                if direction == "LONG":
                    target = h_day - (h_day - c_day) * progress
                else:
                    target = l_day + (c_day - l_day) * progress
                noise = rng.normal(0, (h_day - l_day) * 0.025)
                bar_c = target + noise
                bar_o = price
                bar_h = max(bar_o, bar_c) * (1 + abs(rng.normal(0, 0.003)))
                bar_l = min(bar_o, bar_c) * (1 - abs(rng.normal(0, 0.003)))
                bar_v = v_per_bar * rng.uniform(0.3, 0.8)

            candles.append(pd.Series({
                "open": bar_o, "high": bar_h, "low": bar_l,
                "close": bar_c, "volume": bar_v,
            }))
            price = bar_c

        return candles

    def _compile_report(self) -> pd.DataFrame:
        if not self.all_results:
            print("\n거래 결과 없음")
            return pd.DataFrame()

        df = pd.DataFrame([asdict(r) for r in self.all_results])
        equity = pd.Series(self.equity_curve)
        peak = equity.cummax()
        dd = equity - peak
        max_dd = dd.min()

        wins = (df["rr_realized"] > 0).sum()
        losses = (df["rr_realized"] <= 0).sum()
        total = len(df)
        wr = wins / total * 100
        avg_rr = df["rr_realized"].mean()
        total_pnl = self.equity_curve[-1]

        exit_dist = df["exit_reason"].value_counts()
        good_exits = df[df["exit_reason"].isin(["소진감지", "트레일링", "수익잠금"])].shape[0]

        print(f"\n{'='*60}")
        print(f"  Body Hunter v2 백테스트 결과")
        print(f"  [LONG ONLY: {self.long_only}] [일봉필터: {self.min_grade}등급↑]")
        print(f"{'='*60}")
        print(f"  총 거래    : {total}회 ({wins}승/{losses}패)")
        print(f"  승률       : {wr:.1f}%")
        print(f"  평균 RR    : {avg_rr:+.2f}")
        print(f"  총 PnL     : {total_pnl:+,.0f}원")
        print(f"  최대 낙폭  : {max_dd:,.0f}원")
        print(f"  좋은청산   : {good_exits}/{total} ({good_exits/total*100:.1f}%)")
        print(f"{'-'*60}")
        print(f"  청산사유 분포:")
        for reason, cnt in exit_dist.items():
            pct = cnt / total * 100
            bar = "#" * int(pct / 3)
            print(f"    {reason:<12} {cnt:3d}회 ({pct:.1f}%) {bar}")
        print(f"{'-'*60}")

        # LONG만 있어야 함 (LONG ONLY 모드)
        for d in ["LONG", "SHORT"]:
            sub = df[df["direction"] == d]
            if sub.empty:
                continue
            sub_wr = (sub["rr_realized"] > 0).sum() / len(sub) * 100
            print(f"  {d:<6} 승률:{sub_wr:.1f}% 평균RR:{sub['rr_realized'].mean():+.2f} ({len(sub)}회)")

        print(f"{'-'*60}")
        print(f"  종목별 성과 (상위 5):")
        by_ticker = df.groupby("ticker").agg(
            trades=("rr_realized", "count"),
            avg_rr=("rr_realized", "mean"),
            total_pnl=("pnl_pct", "sum"),
        ).sort_values("avg_rr", ascending=False)
        for ticker, row in by_ticker.head(5).iterrows():
            name = self.code_to_name.get(ticker, ticker)
            print(f"    {ticker}({name}) {row['trades']}회 avgRR:{row['avg_rr']:+.2f} PnL:{row['total_pnl']:+.1f}%")

        print(f"{'='*60}")

        return df


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

    top_n = 3

    bt = BodyHunterCSVBacktester(
        csv_dir="../stock_data_daily",
        top_n=top_n,
        risk_per_trade=50_000,
        long_only=True,
        min_grade="B",
    )
    df = bt.run(start_date="2025-06-01", end_date="2025-12-31", n_stocks=200)

    if not df.empty:
        output_path = Path(__file__).parent.parent / "results" / "body_hunter_v2_results.csv"
        output_path.parent.mkdir(exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"\n결과 저장: {output_path}")
