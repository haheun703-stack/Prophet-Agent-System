"""
Body Hunter 마스터 오케스트레이터 v2
ETF방향 -> 일봉필터 -> 상대강도 스캔 -> 몸통 포착 -> 자동매매 -> 일일리포트

v2 변경:
  - 일봉 필터 사전 적용 (B등급 이상만 스캔)
  - LONG ONLY 기본 모드
  - 최대 3종목 동시 감시 (5 → 3)
  - body_hunter_v2 사용 (수익잠금)
  - 좋은청산에 수익잠금 추가

전체 흐름:
  전일   일봉 필터 → 몸통 후보 유니버스 구성
  09:20  ETF 방향 판단 + 상위 3종목 추출
  09:20~ 각 종목 몸통 감시
  진입   이탈 + 리테스트 확인
  청산   소진감지 / 트레일링 / 수익잠금 / SL
  장마감 일일 리포트
"""

import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from strategies.etf_filter import ETFDirectionFilter, MarketDirection, ETFSignal
from strategies.scanner import RelativeStrengthScanner, StockCandidate
from strategies.body_hunter_v2 import BodyHunterV2, BodyState, ExitReason

logger = logging.getLogger(__name__)


@dataclass
class DailyTradeResult:
    date:         str
    ticker:       str
    name:         str
    direction:    str
    entry_price:  float
    exit_price:   float
    exit_reason:  str
    rr_realized:  float
    hold_bars:    int
    pnl_pct:      float
    exhaustion_signals: List[str]


class BodyHunterMaster:
    """
    ETF방향 + 일봉필터 + 상대강도 + 몸통포착 v2 통합 시스템

    특징:
      - 일봉 필터로 사전 스크리닝 (B등급 이상)
      - LONG ONLY 기본 모드
      - 최대 3종목 동시 감시
      - 수익잠금으로 본전청산 방지
      - 소진감지로 몸통 끝 자동 포착
    """

    def __init__(
        self,
        etf_ticker:     str   = "069500",
        top_n:          int   = 3,
        risk_per_trade: float = 50_000,
        max_trades_day: int   = 1,
        long_only:      bool  = True,
    ):
        self.etf_ticker     = etf_ticker
        self.top_n          = top_n
        self.risk_per_trade = risk_per_trade
        self.max_trades_day = max_trades_day
        self.long_only      = long_only

        self.etf_filter  = ETFDirectionFilter()
        self.scanner     = RelativeStrengthScanner(top_n=top_n)

        self.market_signal:   Optional[ETFSignal]       = None
        self.candidates:      List[StockCandidate]      = []
        self.daily_whitelist: Optional[Dict[str, tuple]] = None
        self.hunters:         Dict[str, BodyHunterV2]   = {}
        self.positions:       Dict[str, dict]           = {}
        self.today_results:   List[DailyTradeResult]    = []
        self.today_pnl:       float                     = 0.0

    def process_etf_candle(
        self, etf_candle: pd.Series,
        avg_etf_volume: float = None,
        confirm_candle: Optional[pd.Series] = None,
    ) -> ETFSignal:
        signal = self.etf_filter.judge(etf_candle, confirm_candle, avg_etf_volume)
        self.market_signal = signal
        return signal

    def set_daily_whitelist(self, whitelist: Dict[str, tuple]):
        """일봉 필터 결과를 화이트리스트로 설정 {code: (score, grade)}"""
        self.daily_whitelist = whitelist
        logger.info(f"일봉 화이트리스트: {len(whitelist)}종목")

    def scan_candidates(
        self,
        stock_data:  Dict[str, pd.Series],
        avg_volumes: Optional[Dict[str, float]] = None,
        stock_names: Optional[Dict[str, str]]   = None,
    ) -> List[StockCandidate]:
        if not self.market_signal:
            return []
        if self.market_signal.direction == MarketDirection.NEUTRAL:
            return []

        candidates = self.scanner.scan(
            etf_change      = self.market_signal.etf_change,
            market_dir      = self.market_signal.direction,
            stock_data      = stock_data,
            avg_volumes     = avg_volumes,
            stock_names     = stock_names,
            daily_whitelist = self.daily_whitelist,
            long_only       = self.long_only,
        )
        self.candidates = candidates

        for c in candidates:
            if c.ticker not in self.hunters:
                self.hunters[c.ticker] = BodyHunterV2(
                    ticker=c.ticker, name=c.name, direction=c.direction,
                    retest_required=True, volume_surge_min=1.5,
                    trailing_atr_mult=1.2, breakeven_rr=0.3, trailing_rr=1.0,
                    exhaustion_bars=2,
                )
                first_candle = stock_data[c.ticker]
                avg_vol = (avg_volumes or {}).get(c.ticker)
                self.hunters[c.ticker].set_levels(first_candle, avg_vol)

        return candidates

    def update_all(self, candles: Dict[str, pd.Series]) -> Dict[str, dict]:
        results = {}
        for ticker, hunter in self.hunters.items():
            if hunter.state == BodyState.DONE:
                continue
            candle = candles.get(ticker)
            if candle is None:
                continue

            signal = hunter.update(candle)
            results[ticker] = signal

            if signal["action"] == "ENTER":
                self.positions[ticker] = {"hunter": hunter, "setup": signal["position"]}
            elif signal["action"] == "EXIT":
                self._record_result(ticker, hunter, signal, candle)

        return results

    def _record_result(self, ticker, hunter, signal, candle):
        pos = signal.get("position")
        if not pos:
            return

        entry  = pos.entry_price
        exit_p = signal.get("exit_price", candle["close"])

        if hunter.direction == "LONG":
            pnl_pct = (exit_p - entry) / entry * 100
        else:
            pnl_pct = (entry - exit_p) / entry * 100

        rr = signal.get("rr_realized", 0)
        pnl_money = self.risk_per_trade * rr
        self.today_pnl += pnl_money

        ex_signals = signal["exhaustion"].signals if signal.get("exhaustion") else []

        self.today_results.append(DailyTradeResult(
            date=datetime.now().strftime("%Y-%m-%d"),
            ticker=ticker, name=hunter.name, direction=hunter.direction,
            entry_price=entry, exit_price=exit_p,
            exit_reason=signal.get("reason", ""),
            rr_realized=rr, hold_bars=signal.get("hold_bars", 0),
            pnl_pct=pnl_pct, exhaustion_signals=ex_signals,
        ))

        if ticker in self.positions:
            del self.positions[ticker]

    def daily_report(self) -> str:
        """일일 리포트 텍스트 생성"""
        results = self.today_results
        total = len(results)

        lines = []
        lines.append("")
        lines.append("=" * 55)
        lines.append("  Body Hunter 일일 리포트")
        lines.append("=" * 55)

        if self.market_signal:
            lines.append(f"  ETF방향: {self.market_signal.direction.value} "
                        f"({self.market_signal.etf_change:+.2f}%)")

        lines.append(f"  선정종목: {len(self.candidates)}개")

        if total == 0:
            lines.append("  오늘 거래 없음")
            lines.append("=" * 55)
            report = "\n".join(lines)
            print(report)
            return report

        wins = sum(1 for r in results if r.rr_realized > 0)
        losses = total - wins
        avg_rr = sum(r.rr_realized for r in results) / total
        avg_hold = sum(r.hold_bars for r in results) / total
        good_exits = sum(1 for r in results if r.exit_reason in ["소진감지", "트레일링", "수익잠금"])

        lines.append(f"  실거래:   {total}회 ({wins}승/{losses}패)")
        lines.append(f"  승률:     {wins/total*100:.1f}%")
        lines.append(f"  평균 RR:  {avg_rr:+.2f}")
        lines.append(f"  총 PnL:   {self.today_pnl:+,.0f}원")
        lines.append(f"  평균보유: {avg_hold:.1f}봉 ({avg_hold*5:.0f}분)")
        lines.append(f"  좋은청산: {good_exits}/{total} (소진감지/트레일링)")
        lines.append("-" * 55)

        for r in results:
            icon = "+" if r.rr_realized > 0 else "-"
            ex = f" [{','.join(r.exhaustion_signals[:2])}]" if r.exhaustion_signals else ""
            lines.append(
                f"  {icon} {r.ticker}({r.name}) {r.direction} "
                f"RR:{r.rr_realized:+.2f} {r.hold_bars}봉 "
                f"[{r.exit_reason}]{ex}"
            )
        lines.append("=" * 55)

        report = "\n".join(lines)
        print(report)
        return report

    def reset_daily(self):
        self.market_signal = None
        self.candidates = []
        self.daily_whitelist = None
        self.hunters = {}
        self.positions = {}
        self.today_results = []
        self.today_pnl = 0.0
