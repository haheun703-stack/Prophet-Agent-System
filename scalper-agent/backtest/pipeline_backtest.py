"""
Body Hunter v4 — Full Pipeline Backtest
========================================
추천→진입→보유→청산 전체 파이프라인 시뮬레이션.
CORTEX 6단계 체제 + 소프트 스코어링 v2 + ATR TP/SL + 트레일링 스탑.

사용법:
  cd scalper-agent
  python -m backtest.pipeline_backtest
  python -m backtest.pipeline_backtest --start 2025-11-01 --end 2026-02-28
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd
import numpy as np

# ── path setup ──────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_store"
PROCESSED_DIR = DATA_DIR / "processed"
DAILY_DIR = DATA_DIR / "daily"
UNIVERSE_PATH = DATA_DIR / "universe.json"

logger = logging.getLogger(__name__)

# ── constants ───────────────────────────────────────────────
BUY_COMMISSION = 0.00015       # 매수 수수료 0.015%
SELL_COMMISSION = 0.00015      # 매도 수수료 0.015%
SELL_TAX = 0.0023              # 매도 세금 0.23%
SELL_COST = SELL_COMMISSION + SELL_TAX  # 0.245%

MAX_POSITIONS = 2
CASH_RESERVE = 0.10            # 10% 현금 보유
MIN_SCORE = 10.0               # 최소 추천 점수
TIMEOUT_DAYS = 20              # 최대 보유일
TRAILING_ACTIVATE_PCT = 0.03   # +3% → 트레일링 활성화
TRAILING_DROP_PCT = 0.03       # 고점 대비 -3% → 청산
MIN_CAP_억 = 10000             # 시총 1조+

# ── regime config ───────────────────────────────────────────
REGIME_RULES = {
    "NORMAL":             {"new_buy": True,  "capital_use": 1.0},
    "CAUTION":            {"new_buy": True,  "capital_use": 0.7},
    "SHOCK":              {"new_buy": False, "capital_use": 0.5},
    "PANIC":              {"new_buy": False, "capital_use": 0.3},
    "RECOVERY_EARLY":     {"new_buy": True,  "capital_use": 0.5},
    "RECOVERY_CONFIRMED": {"new_buy": True,  "capital_use": 0.8},
}

REGIME_SCORE_MULT = {
    "NORMAL": 1.0, "CAUTION": 0.85, "SHOCK": 0.6,
    "PANIC": 0.3, "RECOVERY_EARLY": 1.2, "RECOVERY_CONFIRMED": 1.1,
}


# ════════════════════════════════════════════════════════════
#  Data Classes
# ════════════════════════════════════════════════════════════

@dataclass
class Position:
    code: str
    name: str
    entry_price: float
    shares: int
    entry_date: pd.Timestamp
    cost: float             # 총 매수 비용 (수수료 포함)
    sl: float
    tp: float
    high_watermark: float = 0.0
    trailing_active: bool = False
    regime_at_entry: str = "NORMAL"


@dataclass
class Trade:
    code: str
    name: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    shares: int
    pnl: float
    pnl_pct: float
    exit_reason: str
    regime: str
    hold_days: int


# ════════════════════════════════════════════════════════════
#  Pipeline Backtester
# ════════════════════════════════════════════════════════════

class PipelineBacktester:

    def __init__(self, start: str = "2025-11-01", end: str = "2026-02-28",
                 initial_cash: int = 1_150_000):
        self.start = pd.Timestamp(start)
        self.end = pd.Timestamp(end)
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[Tuple[str, float]] = []

        # regime state
        self.current_regime = "NORMAL"
        self.panic_low: Optional[float] = None
        self.in_panic_since: Optional[pd.Timestamp] = None

        # preloaded data
        self.stock_data: Dict[str, pd.DataFrame] = {}
        self.kodex: Optional[pd.DataFrame] = None
        self.universe: Dict[str, dict] = {}
        self.trading_days: List[pd.Timestamp] = []

    # ── data loading ────────────────────────────────────────

    def _load_universe(self):
        """시총 1조+ 보통주만 로드"""
        with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.universe = {
            code: info for code, info in raw.items()
            if info.get("cap_억", 0) >= MIN_CAP_억
            and "우" not in info.get("name", "")
            and code not in ("069500",)  # KODEX200 제외
        }
        print(f"  Universe: {len(self.universe)} stocks (시총 1조+, 우선주 제외)")

    def _load_kodex200(self):
        """KODEX200 CSV 로드 (시장 벤치마크)"""
        csv_path = DAILY_DIR / "069500.csv"
        if not csv_path.exists():
            print("  [WARN] KODEX200 CSV not found — regime detection disabled")
            return
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        df.index.name = "date"
        # 5MA 계산
        df["ma5"] = df["close"].rolling(5).mean()
        df["ret_1d"] = df["close"].pct_change()
        self.kodex = df
        print(f"  KODEX200: {df.index.min().date()} ~ {df.index.max().date()}")

    def _load_stock_data(self):
        """전 종목 processed parquet 일괄 로드 + ATR 사전 계산"""
        loaded = 0
        for code in self.universe:
            path = PROCESSED_DIR / f"{code}.parquet"
            if not path.exists():
                continue
            try:
                df = pd.read_parquet(path)
            except Exception:
                continue

            # 백테스트 기간 데이터 존재 확인 (최소 30일 이전 데이터 필요)
            lookback_start = self.start - pd.Timedelta(days=60)
            mask = df.index >= lookback_start
            if mask.sum() < 30:
                continue

            # ATR14 사전 계산
            high = df["고가"]
            low = df["저가"]
            close = df["종가"]
            prev_close = close.shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ], axis=1).max(axis=1)
            df["ATR14"] = tr.rolling(14).mean()

            self.stock_data[code] = df
            loaded += 1

        print(f"  Loaded: {loaded} stocks with parquet data")

    def _build_trading_days(self):
        """KODEX200 기준 거래일 목록"""
        if self.kodex is None:
            return
        mask = (self.kodex.index >= self.start) & (self.kodex.index <= self.end)
        self.trading_days = sorted(self.kodex.index[mask].tolist())
        print(f"  Trading days: {len(self.trading_days)}")

    def load_all(self):
        """전체 데이터 로드"""
        print("=" * 55)
        print("  Data Loading...")
        print("-" * 55)
        self._load_universe()
        self._load_kodex200()
        self._load_stock_data()
        self._build_trading_days()
        print("=" * 55)
        print()

    # ── regime detection ────────────────────────────────────

    def _detect_regime(self, T: pd.Timestamp) -> str:
        """KODEX200 기반 6단계 체제 판정"""
        if self.kodex is None:
            return "NORMAL"

        if T not in self.kodex.index:
            return self.current_regime  # 데이터 없으면 유지

        row = self.kodex.loc[T]
        close = row["close"]
        ret_1d = row.get("ret_1d", 0)
        if pd.isna(ret_1d):
            ret_1d = 0

        # 5일 수익률
        idx = self.kodex.index.get_loc(T)
        if idx >= 5:
            close_5d_ago = self.kodex.iloc[idx - 5]["close"]
            ret_5d = (close / close_5d_ago) - 1
        else:
            ret_5d = 0

        prev_regime = self.current_regime

        # ── 하향 전환 (스킵 가능) ──
        if ret_5d < -0.07 or (ret_1d < -0.05):
            new_regime = "PANIC"
            self.panic_low = close
            self.in_panic_since = T
        elif ret_1d < -0.03:
            new_regime = "SHOCK"
        elif ret_1d < -0.02:
            new_regime = "CAUTION"
        else:
            new_regime = "NORMAL"

        # ── 상향 전환 (단계적) ──
        if prev_regime == "PANIC" and new_regime == "NORMAL":
            # PANIC → RECOVERY_EARLY 조건 확인
            is_bullish = close > row.get("open", close)
            above_panic_low = (self.panic_low is not None and
                               close > self.panic_low)
            if is_bullish and above_panic_low:
                new_regime = "RECOVERY_EARLY"
            else:
                new_regime = "PANIC"  # 아직 PANIC 유지

        elif prev_regime == "RECOVERY_EARLY" and new_regime == "NORMAL":
            ma5 = row.get("ma5", None)
            if ma5 is not None and not pd.isna(ma5) and close > ma5:
                new_regime = "RECOVERY_CONFIRMED"
            else:
                new_regime = "RECOVERY_EARLY"

        elif prev_regime == "RECOVERY_CONFIRMED" and new_regime == "NORMAL":
            new_regime = "NORMAL"  # 완전 회복

        elif prev_regime == "SHOCK" and new_regime == "NORMAL":
            new_regime = "CAUTION"  # SHOCK → CAUTION 단계적

        self.current_regime = new_regime
        return new_regime

    # ── scoring ─────────────────────────────────────────────

    def _score_stock(self, code: str, T: pd.Timestamp,
                     kodex_chg: float) -> Optional[float]:
        """단일 종목 스코어링 (소프트 스코어링 v2)"""
        df = self.stock_data.get(code)
        if df is None:
            return None

        if T not in df.index:
            return None

        row = df.loc[T]
        idx = df.index.get_loc(T)

        # 이전 데이터 필요
        if idx < 5:
            return None

        score = 0.0

        # ── Tech Score (0~25) ──
        tech = 0.0
        ema20 = row.get("EMA20", np.nan)
        ema60 = row.get("EMA60", np.nan)
        close = row["종가"]
        rsi = row.get("RSI14", 50)
        macd_hist = row.get("MACD_hist", 0)

        if not pd.isna(ema20) and not pd.isna(ema60):
            if ema20 > ema60:
                tech += 1.0
            if close > ema20:
                tech += 1.0

        if not pd.isna(rsi):
            if rsi < 30:
                tech += 2.0
            elif 30 <= rsi <= 55:
                tech += 1.0
            elif rsi > 70:
                tech -= 0.5

        if not pd.isna(macd_hist) and macd_hist > 0:
            tech += 1.0

        # OBV 5일 추세
        obv_data = df.iloc[max(0, idx - 4):idx + 1]["OBV"]
        if len(obv_data) >= 2 and not obv_data.isna().all():
            obv_clean = obv_data.dropna()
            if len(obv_clean) >= 2:
                if obv_clean.iloc[-1] > obv_clean.iloc[0]:
                    tech += 1.0
                else:
                    tech -= 0.5

        score += max(tech, 0) * 5  # 0~25

        # ── Supply Score (0~30) ──
        fgn5 = row.get("외국인_누적5", 0)
        inst5 = row.get("기관_누적5", 0)
        if pd.isna(fgn5):
            fgn5 = 0
        if pd.isna(inst5):
            inst5 = 0

        supply = 0.0
        if fgn5 > 0:
            supply += min(fgn5 / 1e8, 15)
        if inst5 > 0:
            supply += min(inst5 / 1e8, 15)
        score += supply

        # ── MACD Zero Cross (+15) ──
        if idx >= 1:
            prev_hist = df.iloc[idx - 1].get("MACD_hist", 0)
            if pd.isna(prev_hist):
                prev_hist = 0
            curr_hist = macd_hist if not pd.isna(macd_hist) else 0
            if prev_hist <= 0 < curr_hist:
                score += 15

        # ── Penalties ──
        chg = row.get("등락률", 0)
        if pd.isna(chg):
            chg = 0
        rel = chg - kodex_chg
        if rel < -10:
            score -= 25
        elif rel < -5:
            score -= 15
        elif rel < -2:
            score -= 5

        # OBV 하락 추가 페널티
        if len(obv_data) >= 2:
            obv_clean = obv_data.dropna()
            if len(obv_clean) >= 2 and obv_clean.iloc[-1] < obv_clean.iloc[0]:
                score -= 10

        # ── Regime Multiplier ──
        mult = REGIME_SCORE_MULT.get(self.current_regime, 1.0)
        score *= mult

        return score

    def _rank_candidates(self, T: pd.Timestamp) -> List[Tuple[str, float]]:
        """전 종목 스코어링 → 상위 후보 반환"""
        kodex_chg = 0.0
        if self.kodex is not None and T in self.kodex.index:
            krow = self.kodex.loc[T]
            kodex_chg = krow.get("ret_1d", 0) * 100  # % 단위
            if pd.isna(kodex_chg):
                kodex_chg = 0.0

        scored = []
        for code in self.stock_data:
            # 이미 보유 중이면 스킵
            if code in self.positions:
                continue
            s = self._score_stock(code, T, kodex_chg)
            if s is not None and s > MIN_SCORE:
                scored.append((code, s))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    # ── entry logic ─────────────────────────────────────────

    def _calc_atr(self, code: str, T: pd.Timestamp) -> float:
        """T 시점 ATR14"""
        df = self.stock_data.get(code)
        if df is None or T not in df.index:
            return 0
        atr = df.loc[T].get("ATR14", np.nan)
        return atr if not pd.isna(atr) else 0

    def _check_entry_conditions(self, code: str, T: pd.Timestamp) -> bool:
        """간소화 진입 조건 (3개 중 2개+)"""
        df = self.stock_data.get(code)
        if df is None or T not in df.index:
            return False

        row = df.loc[T]
        macd_hist = row.get("MACD_hist", 0)
        rsi = row.get("RSI14", 50)
        close = row["종가"]
        ema20 = row.get("EMA20", np.nan)

        cond1 = (not pd.isna(macd_hist)) and macd_hist > 0
        cond2 = (not pd.isna(rsi)) and rsi < 75
        cond3 = (not pd.isna(ema20)) and close > ema20

        return sum([cond1, cond2, cond3]) >= 2

    def _try_entries(self, T: pd.Timestamp,
                     candidates: List[Tuple[str, float]]):
        """T일 시가에 진입 시도"""
        available_slots = MAX_POSITIONS - len(self.positions)
        if available_slots <= 0:
            return

        regime_rules = REGIME_RULES.get(self.current_regime,
                                         REGIME_RULES["NORMAL"])
        if not regime_rules["new_buy"]:
            return

        capital_use = regime_rules["capital_use"]
        entries_today = 0

        for code, score in candidates:
            if entries_today >= available_slots:
                break

            df = self.stock_data.get(code)
            if df is None or T not in df.index:
                continue

            entry_price = df.loc[T]["시가"]
            if pd.isna(entry_price) or entry_price <= 0:
                continue

            # 전날 기준 진입 조건 확인
            idx = df.index.get_loc(T)
            if idx < 1:
                continue
            prev_day = df.index[idx - 1]
            if not self._check_entry_conditions(code, prev_day):
                continue

            # ATR 기반 SL/TP (전날 기준)
            atr = self._calc_atr(code, prev_day)
            if atr <= 0:
                atr = entry_price * 0.03  # 폴백: 3%

            sl = entry_price - atr * 0.5
            sl = max(sl, entry_price * 0.95)  # 최대 -5%
            tp = entry_price + atr * 1.6

            # 매수 금액
            usable = self.cash * (1 - CASH_RESERVE) * capital_use
            num_targets = min(len(candidates), available_slots)
            if num_targets <= 0:
                num_targets = 1
            buy_budget = usable / num_targets

            if buy_budget < 50000:  # 최소 5만원
                continue

            shares = int(buy_budget // entry_price)
            if shares <= 0:
                continue

            cost = entry_price * shares
            commission = cost * BUY_COMMISSION
            total_cost = cost + commission

            if total_cost > self.cash:
                # 현금 부족 → 수량 줄임
                shares = int((self.cash * 0.95) // entry_price)
                if shares <= 0:
                    continue
                cost = entry_price * shares
                commission = cost * BUY_COMMISSION
                total_cost = cost + commission

            self.cash -= total_cost

            name = self.universe.get(code, {}).get("name", code)
            pos = Position(
                code=code, name=name,
                entry_price=entry_price, shares=shares,
                entry_date=T, cost=total_cost,
                sl=sl, tp=tp,
                high_watermark=entry_price,
                trailing_active=False,
                regime_at_entry=self.current_regime,
            )
            self.positions[code] = pos
            entries_today += 1

    # ── exit logic ──────────────────────────────────────────

    def _is_supply_exit(self, code: str, T: pd.Timestamp) -> bool:
        """3일 연속 외국인+기관 순매도 여부"""
        df = self.stock_data.get(code)
        if df is None:
            return False

        idx_loc = df.index.get_loc(T) if T in df.index else None
        if idx_loc is None or idx_loc < 2:
            return False

        for i in range(3):
            r = df.iloc[idx_loc - i]
            fgn = r.get("외국인합계", 0)
            inst = r.get("기관합계", 0)
            if pd.isna(fgn):
                fgn = 0
            if pd.isna(inst):
                inst = 0
            if (fgn + inst) >= 0:
                return False
        return True

    def _close_position(self, code: str, exit_price: float,
                        exit_date: pd.Timestamp, reason: str):
        """포지션 청산 + Trade 기록"""
        pos = self.positions.pop(code)
        gross = exit_price * pos.shares
        sell_cost = gross * SELL_COST
        net = gross - sell_cost

        pnl = net - pos.cost
        pnl_pct = (exit_price / pos.entry_price - 1) * 100

        # 실제 P&L은 수수료/세금 감안
        actual_pnl_pct = (net / pos.cost - 1) * 100

        hold_days = len(pd.bdate_range(pos.entry_date, exit_date)) - 1
        if hold_days < 1:
            hold_days = 1

        trade = Trade(
            code=code, name=pos.name,
            entry_date=str(pos.entry_date.date()),
            exit_date=str(exit_date.date()),
            entry_price=pos.entry_price,
            exit_price=round(exit_price, 0),
            shares=pos.shares,
            pnl=round(pnl, 0),
            pnl_pct=round(actual_pnl_pct, 2),
            exit_reason=reason,
            regime=pos.regime_at_entry,
            hold_days=hold_days,
        )
        self.trades.append(trade)
        self.cash += net

    def _check_exits(self, T: pd.Timestamp):
        """보유 포지션 청산 체크"""
        codes_to_exit = []

        for code, pos in self.positions.items():
            df = self.stock_data.get(code)
            if df is None or T not in df.index:
                continue

            # 진입일에는 청산 안 함
            if T <= pos.entry_date:
                continue

            row = df.loc[T]
            low = row["저가"]
            high = row["고가"]
            close = row["종가"]

            if pd.isna(low) or pd.isna(high):
                continue

            # 1) Stop Loss
            if low <= pos.sl:
                codes_to_exit.append((code, pos.sl, "SL_HIT"))
                continue

            # 2) Take Profit
            if high >= pos.tp:
                codes_to_exit.append((code, pos.tp, "TP_HIT"))
                continue

            # 3) Trailing Stop
            if high >= pos.entry_price * (1 + TRAILING_ACTIVATE_PCT):
                pos.trailing_active = True

            if pos.trailing_active:
                pos.high_watermark = max(pos.high_watermark, high)
                trailing_sl = pos.high_watermark * (1 - TRAILING_DROP_PCT)
                if low <= trailing_sl:
                    exit_px = max(trailing_sl, low)
                    codes_to_exit.append((code, exit_px, "TRAILING"))
                    continue

            # 4) Supply Exit
            if self._is_supply_exit(code, T):
                codes_to_exit.append((code, close, "SUPPLY_EXIT"))
                continue

            # 5) Timeout
            bdays = len(pd.bdate_range(pos.entry_date, T)) - 1
            if bdays >= TIMEOUT_DAYS:
                codes_to_exit.append((code, close, "TIMEOUT"))
                continue

        for code, px, reason in codes_to_exit:
            self._close_position(code, px, T, reason)

    # ── equity tracking ─────────────────────────────────────

    def _calc_equity(self, T: pd.Timestamp) -> float:
        """현재 총 자산 = 현금 + 포지션 평가"""
        equity = self.cash
        for code, pos in self.positions.items():
            df = self.stock_data.get(code)
            if df is not None and T in df.index:
                equity += df.loc[T]["종가"] * pos.shares
            else:
                equity += pos.entry_price * pos.shares
        return equity

    # ── main loop ───────────────────────────────────────────

    def run(self):
        """백테스트 메인 루프"""
        if not self.trading_days:
            print("[ERROR] No trading days — check data")
            return

        print(f"Backtesting: {self.start.date()} ~ {self.end.date()}")
        print(f"Initial cash: {self.initial_cash:,}")
        print(f"Max positions: {MAX_POSITIONS}")
        print()

        pending_entries: List[Tuple[str, float]] = []

        for i, T in enumerate(self.trading_days):
            # 1) Regime detection
            regime = self._detect_regime(T)

            # 2) Execute pending entries (from yesterday's scoring)
            if pending_entries:
                self._try_entries(T, pending_entries)
                pending_entries = []

            # 3) Check exits for existing positions
            self._check_exits(T)

            # 4) Score universe for tomorrow's entries
            if len(self.positions) < MAX_POSITIONS:
                rules = REGIME_RULES.get(regime, REGIME_RULES["NORMAL"])
                if rules["new_buy"]:
                    candidates = self._rank_candidates(T)
                    slots = MAX_POSITIONS - len(self.positions)
                    pending_entries = candidates[:slots]

            # 5) Track equity
            equity = self._calc_equity(T)
            self.equity_curve.append((str(T.date()), equity))

            # progress
            if (i + 1) % 20 == 0 or i == len(self.trading_days) - 1:
                pct = (equity / self.initial_cash - 1) * 100
                print(f"  [{T.date()}] Regime={regime:20s} | "
                      f"Equity={equity:>12,.0f} ({pct:+.1f}%) | "
                      f"Pos={len(self.positions)} | "
                      f"Trades={len(self.trades)}")

        # 강제 청산: 백테스트 종료 시 남은 포지션
        last_day = self.trading_days[-1]
        remaining = list(self.positions.keys())
        for code in remaining:
            df = self.stock_data.get(code)
            if df is not None and last_day in df.index:
                close = df.loc[last_day]["종가"]
            else:
                close = self.positions[code].entry_price
            self._close_position(code, close, last_day, "PERIOD_END")

        print()

    # ── reporting ───────────────────────────────────────────

    def report(self):
        """결과 리포트 출력"""
        trades = self.trades
        n = len(trades)

        if n == 0:
            print("[결과] 거래 없음")
            return

        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl <= 0]
        win_rate = len(wins) / n * 100

        total_pnl = sum(t.pnl for t in trades)
        final_equity = self.initial_cash + total_pnl
        total_ret = (final_equity / self.initial_cash - 1) * 100

        avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0

        gross_profit = sum(t.pnl for t in wins) if wins else 0
        gross_loss = abs(sum(t.pnl for t in losses)) if losses else 1
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999

        # Max Drawdown
        peak = self.initial_cash
        max_dd = 0
        for _, eq in self.equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd

        # Sharpe (일별 수익률 → 연환산)
        if len(self.equity_curve) >= 2:
            eqs = [e for _, e in self.equity_curve]
            daily_ret = pd.Series(eqs).pct_change().dropna()
            sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)
                      if daily_ret.std() > 0 else 0)
        else:
            sharpe = 0

        avg_hold = np.mean([t.hold_days for t in trades])

        # KODEX200 buy-and-hold
        kodex_ret = 0
        if self.kodex is not None:
            kstart = self.kodex.loc[self.kodex.index >= self.start]
            kend = self.kodex.loc[self.kodex.index <= self.end]
            if len(kstart) > 0 and len(kend) > 0:
                k0 = kstart.iloc[0]["close"]
                k1 = kend.iloc[-1]["close"]
                kodex_ret = (k1 / k0 - 1) * 100

        # ── 출력 ──
        sep = "=" * 55
        line = "-" * 55

        print(sep)
        print("  Body Hunter v4 - Pipeline Backtest Results")
        print(f"  기간: {self.start.date()} ~ {self.end.date()} "
              f"({len(self.trading_days)} 거래일)")
        print(f"  초기자금: {self.initial_cash:>12,}원")
        print(sep)
        print()
        print("  성과 요약")
        print(line)
        print(f"  총 수익률:      {total_ret:+.1f}% ({total_pnl:+,.0f}원)")
        print(f"  최종 자산:      {final_equity:>12,.0f}원")
        print(f"  총 거래 수:     {n}건")
        print(f"  승률:           {win_rate:.1f}% "
              f"({len(wins)}승 {len(losses)}패)")
        print(f"  평균 수익:      {avg_win:+.2f}% (승) / "
              f"{avg_loss:+.2f}% (패)")
        print(f"  Profit Factor:  {profit_factor:.2f}")
        print(f"  Max Drawdown:   {max_dd:.1f}%")
        print(f"  Sharpe (연환산): {sharpe:.2f}")
        print(f"  평균 보유일:    {avg_hold:.1f}일")
        print()
        print(f"  KODEX200 (B&H): {kodex_ret:+.1f}%")
        print(f"  초과수익(알파): {total_ret - kodex_ret:+.1f}%")
        print()

        # 월별 성과
        print("  월별 성과")
        print(line)
        monthly = defaultdict(lambda: {"pnl": 0, "trades": 0,
                                        "wins": 0, "losses": 0})
        for t in trades:
            m = t.exit_date[:7]  # YYYY-MM
            monthly[m]["pnl"] += t.pnl
            monthly[m]["trades"] += 1
            if t.pnl > 0:
                monthly[m]["wins"] += 1
            else:
                monthly[m]["losses"] += 1

        running = self.initial_cash
        for m in sorted(monthly):
            d = monthly[m]
            mpct = d["pnl"] / running * 100
            running += d["pnl"]
            print(f"  {m}: {mpct:+.1f}% ({d['trades']}거래, "
                  f"{d['wins']}승{d['losses']}패)")
        print()

        # 청산 사유 분포
        print("  청산 사유 분포")
        print(line)
        reasons = defaultdict(int)
        for t in trades:
            reasons[t.exit_reason] += 1
        for reason in ["TP_HIT", "TRAILING", "SL_HIT",
                       "SUPPLY_EXIT", "TIMEOUT", "PERIOD_END"]:
            cnt = reasons.get(reason, 0)
            if cnt > 0:
                pct = cnt / n * 100
                print(f"  {reason:15s}: {cnt:3d}건 ({pct:.1f}%)")
        print()

        # 체제 분포
        print("  체제 분포 (진입 시점)")
        print(line)
        regimes = defaultdict(int)
        for t in trades:
            regimes[t.regime] += 1
        for reg in sorted(regimes, key=lambda x: regimes[x], reverse=True):
            cnt = regimes[reg]
            pct = cnt / n * 100
            print(f"  {reg:20s}: {cnt:3d}건 ({pct:.1f}%)")
        print()

        # Top 10 trades
        print("  Top 수익 거래")
        print(line)
        sorted_trades = sorted(trades, key=lambda x: x.pnl, reverse=True)
        for t in sorted_trades[:5]:
            print(f"  {t.name:12s} {t.entry_date}→{t.exit_date} "
                  f"{t.pnl_pct:+.1f}% ({t.pnl:+,.0f}원) [{t.exit_reason}]")
        print()
        print("  Worst 손실 거래")
        print(line)
        for t in sorted_trades[-5:]:
            print(f"  {t.name:12s} {t.entry_date}→{t.exit_date} "
                  f"{t.pnl_pct:+.1f}% ({t.pnl:+,.0f}원) [{t.exit_reason}]")
        print(sep)

    def save_csv(self):
        """트레이드 로그 CSV 저장"""
        if not self.trades:
            return
        rows = []
        for t in self.trades:
            rows.append({
                "code": t.code, "name": t.name,
                "entry_date": t.entry_date, "exit_date": t.exit_date,
                "entry_price": t.entry_price, "exit_price": t.exit_price,
                "shares": t.shares, "pnl": t.pnl, "pnl_pct": t.pnl_pct,
                "exit_reason": t.exit_reason, "regime": t.regime,
                "hold_days": t.hold_days,
            })
        df = pd.DataFrame(rows)
        out_path = ROOT / "backtest" / (
            f"pipeline_result_{self.start.date()}_{self.end.date()}.csv"
        )
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  CSV saved: {out_path.name}")

        # equity curve
        eq_path = ROOT / "backtest" / (
            f"pipeline_equity_{self.start.date()}_{self.end.date()}.csv"
        )
        eq_df = pd.DataFrame(self.equity_curve, columns=["date", "equity"])
        eq_df.to_csv(eq_path, index=False, encoding="utf-8-sig")
        print(f"  Equity CSV: {eq_path.name}")


# ════════════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Body Hunter v4 — Full Pipeline Backtest"
    )
    parser.add_argument("--start", default="2025-11-01",
                        help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-02-28",
                        help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--cash", type=int, default=1_150_000,
                        help="초기자금 (원)")
    args = parser.parse_args()

    bt = PipelineBacktester(
        start=args.start, end=args.end,
        initial_cash=args.cash,
    )
    bt.load_all()
    bt.run()
    bt.report()
    bt.save_csv()
