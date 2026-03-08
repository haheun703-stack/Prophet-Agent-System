# -*- coding: utf-8 -*-
"""
모멘텀 헌터 백테스트 - 소형주 급등 포착 전략 검증
===================================================
3개월 일봉 데이터로 "거래량 폭발 + 기술 확인" 전략 검증.

핵심 질문: 모멘텀 스코어 상위 종목이 진짜 수익으로 이어지는가?

사용법:
  cd scalper-agent
  python -m backtest.momentum_backtest
  python -m backtest.momentum_backtest --period 2026-01-01:2026-03-06
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd
import numpy as np

# ── path setup ──
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data_store"
DAILY_DIR = DATA_DIR / "daily"
SMALLCAP_PATH = DATA_DIR / "universe_smallcap.json"
UNIVERSE_PATH = DATA_DIR / "universe.json"

logger = logging.getLogger(__name__)

# ── constants ──
BUY_COMMISSION = 0.00015
SELL_COMMISSION = 0.00015
SELL_TAX = 0.0023
SELL_COST = SELL_COMMISSION + SELL_TAX

# ETF/ETN 필터 키워드
ETF_KEYWORDS = {"ETF", "ETN", "인버스", "레버리지", "KODEX", "TIGER", "KBSTAR",
                "KOSEF", "ARIRANG", "HANARO", "SOL", "ACE", "PLUS", "RISE",
                "KINDEX", "BNK", "파워", "하나", "KoAct", "KOACT", "액티브",
                "FOCUS", "마이티", "TIMEFOLIO", "히어로즈"}


@dataclass
class MomentumTrade:
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
    hold_days: int
    score: float
    vol_spike: float
    price_5d: float


# ════════════════════════════════════════════════════
#  Momentum Backtester
# ════════════════════════════════════════════════════

class MomentumBacktester:
    """소형주 모멘텀 백테스트 - 거래량 폭발 + 기술 확인"""

    def __init__(self, start: str = "2026-01-05", end: str = "2026-03-06",
                 initial_cash: int = 230_000,  # 총자금 115만 × 20%
                 max_positions: int = 1,
                 max_hold_days: int = 5,
                 scan_interval: int = 1,
                 largecap: bool = False):  # 대형주 parquet 모드
        self.start = pd.Timestamp(start)
        self.end = pd.Timestamp(end)
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.max_positions = max_positions
        self.max_hold_days = max_hold_days
        self.scan_interval = scan_interval
        self.largecap = largecap

        self.positions: Dict[str, dict] = {}
        self.trades: List[MomentumTrade] = []
        self.equity_curve: List[Tuple[str, float]] = []
        self.daily_scans: Dict[str, list] = {}  # date → top candidates

        # data
        self.universe: Dict[str, dict] = {}
        self.stock_data: Dict[str, pd.DataFrame] = {}
        self.trading_days: List[pd.Timestamp] = []

    def _load_universe(self):
        """유니버스 로드 (ETF/ETN 제외)"""
        if self.largecap:
            # 대형주 유니버스 (시총 1조+)
            if not UNIVERSE_PATH.exists():
                print("  [ERROR] universe.json 없음")
                return
            with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            for code, info in raw.items():
                name = info.get("name", "")
                if any(kw in name for kw in ETF_KEYWORDS):
                    continue
                if "우" in name or "우B" in name:
                    continue
                self.universe[code] = info
            print(f"  대형주 유니버스: {len(self.universe)}종목")
        else:
            # 소형주 유니버스
            if not SMALLCAP_PATH.exists():
                print("  [ERROR] universe_smallcap.json 없음")
                return
            with open(SMALLCAP_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            for code, info in raw.items():
                name = info.get("name", "")
                if any(kw in name for kw in ETF_KEYWORDS):
                    continue
                if code[-1] in ("5", "7", "8", "9") and code[-2:] != "00":
                    continue
                self.universe[code] = info
            print(f"  소형주 유니버스: {len(self.universe)}종목 (ETF/ETN 제외)")

    def _load_daily_data(self):
        """일봉 데이터 로드 (largecap=parquet, smallcap=CSV)"""
        loaded = 0

        if self.largecap:
            # processed parquet (2년치, 지표 포함)
            processed_dir = DATA_DIR / "processed"
            for code in self.universe:
                path = processed_dir / f"{code}.parquet"
                if not path.exists():
                    continue
                try:
                    df = pd.read_parquet(path)
                    if "종가" not in df.columns or len(df) < 60:
                        continue
                    self.stock_data[code] = df
                    loaded += 1
                except Exception:
                    continue
        else:
            # daily CSV (3개월)
            for code in self.universe:
                csv_path = DAILY_DIR / f"{code}.csv"
                if not csv_path.exists():
                    continue
                try:
                    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                    df.index.name = "date"
                    if "close" in df.columns:
                        df = df.rename(columns={
                            "open": "시가", "high": "고가", "low": "저가",
                            "close": "종가", "volume": "거래량"})
                    if "종가" not in df.columns or len(df) < 30:
                        continue
                    self.stock_data[code] = df
                    loaded += 1
                except Exception:
                    continue

        print(f"  일봉 로드: {loaded}종목 "
              f"({'parquet 2Y' if self.largecap else 'CSV 3M'})")

    def _build_trading_days(self):
        """거래일 목록 (KODEX200 또는 최다 데이터 종목 기준)"""
        # KODEX200으로 거래일 산출
        kodex_path = DAILY_DIR / "069500.csv"
        if kodex_path.exists():
            df = pd.read_csv(kodex_path, index_col=0, parse_dates=True)
            mask = (df.index >= self.start) & (df.index <= self.end)
            self.trading_days = sorted(df.index[mask].tolist())
        else:
            # 대안: 가장 데이터 많은 종목 기준
            best_code = max(self.stock_data.keys(),
                            key=lambda c: len(self.stock_data[c]))
            df = self.stock_data[best_code]
            mask = (df.index >= self.start) & (df.index <= self.end)
            self.trading_days = sorted(df.index[mask].tolist())

        print(f"  거래일: {len(self.trading_days)}일 "
              f"({self.start.date()} ~ {self.end.date()})")

    def load_all(self):
        print("=" * 60)
        print("  모멘텀 백테스트 - 데이터 로딩")
        print("-" * 60)
        self._load_universe()
        self._load_daily_data()
        self._build_trading_days()
        print("=" * 60)
        print()

    # ── scoring ──

    def _calc_indicators(self, df: pd.DataFrame, idx: int) -> dict:
        """idx 시점 기술 지표 계산"""
        if idx < 20:
            return {}

        closes = df["종가"].values[:idx + 1].astype(float)
        vols = df["거래량"].values[:idx + 1].astype(float)
        highs = df["고가"].values[:idx + 1].astype(float)
        lows = df["저가"].values[:idx + 1].astype(float)

        # Volume spike (20일 평균 대비)
        vol_20avg = float(np.mean(vols[-21:-1]))
        vol_spike = vols[-1] / vol_20avg if vol_20avg > 0 else 0

        # RSI 14
        deltas = np.diff(closes[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains) if len(gains) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        rsi = 100 - (100 / (1 + rs))

        # MACD (12, 26, 9)
        if len(closes) >= 35:
            ema12 = pd.Series(closes).ewm(span=12).mean().iloc[-1]
            ema26 = pd.Series(closes).ewm(span=26).mean().iloc[-1]
            macd_line = ema12 - ema26
            macd_signal = pd.Series(closes).ewm(span=12).mean()  # simplified
            # MACD hist sign
            macd_hist_series = pd.Series(closes).ewm(span=12).mean() - pd.Series(closes).ewm(span=26).mean()
            macd_signal_series = macd_hist_series.ewm(span=9).mean()
            macd_hist = float(macd_hist_series.iloc[-1] - macd_signal_series.iloc[-1])
        else:
            macd_hist = 0

        # ATR 14
        tr_list = []
        for i in range(max(1, len(highs) - 14), len(highs)):
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
            tr_list.append(tr)
        atr = float(np.mean(tr_list)) if tr_list else closes[-1] * 0.03

        # 5일 등락률
        price_5d = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0

        # OBV 20일 추세
        obv_sign = 1 if len(closes) >= 21 else 0
        if len(closes) >= 21:
            obv = np.cumsum(np.sign(np.diff(closes[-21:])) * vols[-20:])
            obv_sign = 1 if (len(obv) >= 2 and obv[-1] > obv[0]) else -1

        return {
            "vol_spike": vol_spike,
            "rsi": rsi,
            "macd_hist": macd_hist,
            "atr": atr,
            "atr_pct": atr / closes[-1] * 100 if closes[-1] > 0 else 0,
            "price_5d": price_5d,
            "close": closes[-1],
            "obv_up": obv_sign > 0,
        }

    def _score_momentum(self, indicators: dict) -> float:
        """모멘텀 스코어 계산 (0~100)

        거래량 폭발 + RSI 적정 + MACD 양봉 + 5D 모멘텀 + OBV
        """
        score = 0

        # 거래량 폭발 (0~30)
        vs = indicators["vol_spike"]
        if vs >= 5.0:
            score += 30
        elif vs >= 3.0:
            score += 25
        elif vs >= 2.0:
            score += 20
        elif vs >= 1.5:
            score += 10

        # RSI 구간 (0~20)
        rsi = indicators["rsi"]
        if 40 <= rsi <= 60:
            score += 20  # 최적: 과열 아닌 상승초기
        elif 60 < rsi <= 70:
            score += 15
        elif 30 <= rsi < 40:
            score += 10
        elif rsi > 70:
            score += 5   # 과열 경고
        # rsi < 30: 0점 (하락 중)

        # MACD 양봉 (0~15)
        if indicators["macd_hist"] > 0:
            score += 15

        # 5D 모멘텀 (0~20)
        p5d = indicators["price_5d"]
        if 5 <= p5d <= 30:
            score += 20  # 적정 상승
        elif 0 < p5d < 5:
            score += 10  # 약한 상승
        elif p5d > 30:
            score += 5   # 과열
        # 음수: 0점

        # OBV 상승 (0~15)
        if indicators["obv_up"]:
            score += 15

        return score

    def _scan_day(self, T: pd.Timestamp) -> List[dict]:
        """T일 기준 모멘텀 후보 스캔

        v2 풀백 전략: 과거 N일 내 거래량 폭발이 있었고,
        이후 조정(풀백)을 거친 종목만 진입.
        """
        candidates = []

        for code, df in self.stock_data.items():
            if T not in df.index:
                continue

            idx = df.index.get_loc(T)
            if idx < 25:
                continue

            # ── v2: 풀백 진입 ──
            # 최근 5일 이내에 거래량 폭발(2x+)이 있었는지 확인
            explosion_day = None
            explosion_spike = 0
            closes = df["종가"].values[:idx + 1].astype(float)
            vols = df["거래량"].values[:idx + 1].astype(float)

            for lookback in range(1, 6):  # 1~5일 전
                past_idx = idx - lookback
                if past_idx < 21:
                    continue
                vol_20avg = float(np.mean(vols[past_idx - 20:past_idx]))
                if vol_20avg <= 0:
                    continue
                spike = vols[past_idx] / vol_20avg
                if spike >= 2.0:
                    if spike > explosion_spike:
                        explosion_spike = spike
                        explosion_day = lookback
                    break  # 가장 최근 폭발만

            if explosion_day is None:
                # 대안: 오늘 프리무브 (OBV 돌파 + 가격 미동 + 조용한 매집)
                indicators = self._calc_indicators(df, idx)
                if not indicators:
                    continue
                vs = indicators["vol_spike"]
                # 조용한 매집: 거래량 2x+ + 등가 ±2% + OBV 상승
                if vs >= 2.0:
                    pct_chg = abs(closes[-1] / closes[-2] - 1) * 100 if len(closes) >= 2 else 99
                    if pct_chg < 2.0 and indicators["obv_up"]:
                        score = self._score_momentum(indicators)
                        if score < 40:
                            continue
                        close = indicators["close"]
                        atr = indicators["atr"]
                        candidates.append({
                            "code": code,
                            "name": self.universe.get(code, {}).get("name", code),
                            "score": score,
                            "close": close,
                            "vol_spike": vs,
                            "rsi": indicators["rsi"],
                            "price_5d": indicators["price_5d"],
                            "atr_pct": indicators["atr_pct"],
                            "sl": close - atr * 1.0,
                            "tp": close + atr * 2.0,
                            "signal": "premove",
                        })
                continue

            # ── 풀백 조건 확인 ──
            explosion_close = closes[idx - explosion_day]
            explosion_high = df["고가"].values[idx - explosion_day]
            today_close = closes[-1]
            today_low = df["저가"].values[idx]

            # 풀백 조건: 폭발일 고가 대비 조정, 하지만 폭발일 종가 밑으로 안 빠짐
            pullback_from_high = (explosion_high - today_close) / explosion_high * 100
            above_explosion_close = today_close >= explosion_close * 0.97  # -3% 이내

            if not (1.0 <= pullback_from_high <= 15.0 and above_explosion_close):
                continue  # 풀백이 너무 작거나/크거나

            # 오늘 거래량이 폭발일 대비 감소 (매도 소진 확인)
            today_vol = vols[-1]
            explosion_vol = vols[idx - explosion_day]
            vol_ratio = today_vol / explosion_vol if explosion_vol > 0 else 1
            if vol_ratio > 0.8:
                continue  # 아직 거래량 높음 → 매도 진행 중

            # 기술 점수
            indicators = self._calc_indicators(df, idx)
            if not indicators:
                continue
            score = self._score_momentum(indicators)
            if score < 30:  # 풀백이라 점수 기준 완화
                continue

            # 풀백 보너스
            score += 10  # 풀백 진입 보너스

            close = indicators["close"]
            atr = indicators["atr"]
            sl = close - atr * 1.0
            tp = explosion_high * 1.02  # 폭발일 고점 재도전 + 2%

            candidates.append({
                "code": code,
                "name": self.universe.get(code, {}).get("name", code),
                "score": score,
                "close": close,
                "vol_spike": explosion_spike,
                "rsi": indicators["rsi"],
                "price_5d": indicators["price_5d"],
                "atr_pct": indicators["atr_pct"],
                "sl": sl,
                "tp": tp,
                "signal": "pullback",
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:10]

    # ── position management ──

    def _try_entry(self, T: pd.Timestamp, candidates: list):
        """T일 시가에 진입"""
        available = self.max_positions - len(self.positions)
        if available <= 0:
            return

        entries = 0
        for cand in candidates:
            if entries >= available:
                break

            code = cand["code"]
            if code in self.positions:
                continue

            df = self.stock_data.get(code)
            if df is None or T not in df.index:
                continue

            row = df.loc[T]
            entry_price = row["시가"]
            if pd.isna(entry_price) or entry_price <= 0:
                continue

            # ATR 재계산 (전날 기준)
            idx = df.index.get_loc(T)
            if idx < 1:
                continue
            prev_day = df.index[idx - 1]
            prev_indicators = self._calc_indicators(df, df.index.get_loc(prev_day))
            if not prev_indicators:
                continue

            atr = prev_indicators["atr"]
            sl = entry_price - atr * 1.0
            tp = entry_price + atr * 2.0

            # 매수 금액
            budget = self.cash * 0.90 / max(available, 1)
            if budget < 30000:
                continue

            shares = int(budget // entry_price)
            if shares <= 0:
                continue

            cost = entry_price * shares
            commission = cost * BUY_COMMISSION
            total_cost = cost + commission

            if total_cost > self.cash:
                shares = int((self.cash * 0.95) // entry_price)
                if shares <= 0:
                    continue
                cost = entry_price * shares
                commission = cost * BUY_COMMISSION
                total_cost = cost + commission

            self.cash -= total_cost

            self.positions[code] = {
                "entry_price": entry_price,
                "shares": shares,
                "cost": total_cost,
                "entry_date": T,
                "sl": sl,
                "tp": tp,
                "high_watermark": entry_price,
                "score": cand["score"],
                "vol_spike": cand["vol_spike"],
                "price_5d": cand["price_5d"],
            }
            entries += 1

    def _check_exits(self, T: pd.Timestamp):
        """청산 체크"""
        to_close = []

        for code, pos in self.positions.items():
            df = self.stock_data.get(code)
            if df is None or T not in df.index:
                continue
            if T <= pos["entry_date"]:
                continue

            row = df.loc[T]
            low = row["저가"]
            high = row["고가"]
            close = row["종가"]

            if pd.isna(low) or pd.isna(high):
                continue

            # 고점 갱신
            pos["high_watermark"] = max(pos["high_watermark"], high)

            # 1) Stop Loss
            if low <= pos["sl"]:
                to_close.append((code, pos["sl"], "SL_HIT"))
                continue

            # 2) Take Profit
            if high >= pos["tp"]:
                to_close.append((code, pos["tp"], "TP_HIT"))
                continue

            # 3) 트레일링 (고점 -5%)
            if pos["high_watermark"] > pos["entry_price"] * 1.03:
                trail_sl = pos["high_watermark"] * 0.95
                if low <= trail_sl:
                    to_close.append((code, max(trail_sl, low), "TRAILING"))
                    continue

            # 4) 최대 보유일
            bdays = len(pd.bdate_range(pos["entry_date"], T)) - 1
            if bdays >= self.max_hold_days:
                to_close.append((code, close, "TIMEOUT"))
                continue

        for code, exit_px, reason in to_close:
            self._close_position(code, exit_px, T, reason)

    def _close_position(self, code: str, exit_price: float,
                        exit_date: pd.Timestamp, reason: str):
        pos = self.positions.pop(code)
        gross = exit_price * pos["shares"]
        sell_cost = gross * SELL_COST
        net = gross - sell_cost

        pnl = net - pos["cost"]
        pnl_pct = (net / pos["cost"] - 1) * 100

        hold_days = len(pd.bdate_range(pos["entry_date"], exit_date)) - 1
        if hold_days < 1:
            hold_days = 1

        trade = MomentumTrade(
            code=code,
            name=self.universe.get(code, {}).get("name", code),
            entry_date=str(pos["entry_date"].date()),
            exit_date=str(exit_date.date()),
            entry_price=pos["entry_price"],
            exit_price=round(exit_price, 0),
            shares=pos["shares"],
            pnl=round(pnl, 0),
            pnl_pct=round(pnl_pct, 2),
            exit_reason=reason,
            hold_days=hold_days,
            score=pos["score"],
            vol_spike=pos["vol_spike"],
            price_5d=pos["price_5d"],
        )
        self.trades.append(trade)
        self.cash += net

    def _calc_equity(self, T: pd.Timestamp) -> float:
        equity = self.cash
        for code, pos in self.positions.items():
            df = self.stock_data.get(code)
            if df is not None and T in df.index:
                equity += df.loc[T]["종가"] * pos["shares"]
            else:
                equity += pos["entry_price"] * pos["shares"]
        return equity

    # ── main loop ──

    def run(self):
        if not self.trading_days:
            print("[ERROR] 거래일 없음")
            return

        print(f"모멘텀 백테스트: {self.start.date()} ~ {self.end.date()}")
        print(f"초기자금: {self.initial_cash:,}원 | "
              f"최대포지션: {self.max_positions} | "
              f"최대보유: {self.max_hold_days}일")
        print()

        pending_entries: list = []

        for i, T in enumerate(self.trading_days):
            # 1) 대기 진입 실행
            if pending_entries:
                self._try_entry(T, pending_entries)
                pending_entries = []

            # 2) 청산 체크
            self._check_exits(T)

            # 3) 스캔 (스캔 간격마다)
            if len(self.positions) < self.max_positions:
                if i % self.scan_interval == 0:
                    candidates = self._scan_day(T)
                    if candidates:
                        self.daily_scans[str(T.date())] = [
                            {"code": c["code"], "name": c["name"],
                             "score": c["score"], "vol": c["vol_spike"]}
                            for c in candidates[:5]
                        ]
                        pending_entries = candidates[:self.max_positions]

            # 4) 자산 추적
            equity = self._calc_equity(T)
            self.equity_curve.append((str(T.date()), equity))

            if (i + 1) % 10 == 0 or i == len(self.trading_days) - 1:
                pct = (equity / self.initial_cash - 1) * 100
                print(f"  [{T.date()}] "
                      f"Equity={equity:>10,.0f} ({pct:+.1f}%) | "
                      f"Pos={len(self.positions)} | "
                      f"Trades={len(self.trades)}")

        # 종료 시 잔여 포지션 강제 청산
        if self.trading_days:
            last_day = self.trading_days[-1]
            remaining = list(self.positions.keys())
            for code in remaining:
                df = self.stock_data.get(code)
                close = df.loc[last_day]["종가"] if (df is not None and last_day in df.index) else self.positions[code]["entry_price"]
                self._close_position(code, close, last_day, "PERIOD_END")

        print()

    # ── reporting ──

    def report(self):
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
        pf = gross_profit / gross_loss if gross_loss > 0 else 999

        # Max DD
        peak = self.initial_cash
        max_dd = 0
        for _, eq in self.equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd

        avg_hold = np.mean([t.hold_days for t in trades])

        # Sharpe
        if len(self.equity_curve) >= 2:
            eqs = [e for _, e in self.equity_curve]
            daily_ret = pd.Series(eqs).pct_change().dropna()
            sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)
                      if daily_ret.std() > 0 else 0)
        else:
            sharpe = 0

        sep = "=" * 60
        line = "-" * 60

        print(sep)
        print("  모멘텀 헌터 백테스트 결과")
        print(f"  기간: {self.start.date()} ~ {self.end.date()} "
              f"({len(self.trading_days)}거래일)")
        print(f"  초기자금: {self.initial_cash:>10,}원")
        print(f"  설정: max_pos={self.max_positions} | "
              f"max_hold={self.max_hold_days}일 | "
              f"scan_interval={self.scan_interval}일")
        print(sep)
        print()
        print("  핵심 성과")
        print(line)
        print(f"  총 수익률:      {total_ret:+.1f}% ({total_pnl:+,.0f}원)")
        print(f"  최종 자산:      {final_equity:>10,.0f}원")
        print(f"  총 거래:        {n}건")
        print(f"  승률:           {win_rate:.1f}% "
              f"({len(wins)}승 {len(losses)}패)")
        print(f"  평균 수익:      {avg_win:+.2f}% (승) / "
              f"{avg_loss:+.2f}% (패)")
        print(f"  Profit Factor:  {pf:.2f}")
        print(f"  Max Drawdown:   {max_dd:.1f}%")
        print(f"  Sharpe (연환산): {sharpe:.2f}")
        print(f"  평균 보유일:    {avg_hold:.1f}일")
        print()

        # 점수대별 성과
        print("  모멘텀 점수대별 성과")
        print(line)
        score_bins = [(80, 100, "80~100 (A급)"),
                      (60, 80, "60~80  (B급)"),
                      (40, 60, "40~60  (C급)")]
        for lo, hi, label in score_bins:
            bin_trades = [t for t in trades if lo <= t.score < hi]
            if not bin_trades:
                print(f"  {label}: 거래 없음")
                continue
            bin_wins = sum(1 for t in bin_trades if t.pnl > 0)
            bin_avg = np.mean([t.pnl_pct for t in bin_trades])
            bin_wr = bin_wins / len(bin_trades) * 100
            print(f"  {label}: {len(bin_trades)}건 | "
                  f"승률 {bin_wr:.0f}% | 평균 {bin_avg:+.1f}%")
        print()

        # 거래량 배수별 성과
        print("  거래량 배수별 성과")
        print(line)
        vol_bins = [(5.0, 999, "5x+ (폭발)"),
                    (3.0, 5.0, "3~5x (강함)"),
                    (2.0, 3.0, "2~3x (보통)"),
                    (1.5, 2.0, "1.5~2x (약함)")]
        for lo, hi, label in vol_bins:
            bin_trades = [t for t in trades if lo <= t.vol_spike < hi]
            if not bin_trades:
                continue
            bin_wins = sum(1 for t in bin_trades if t.pnl > 0)
            bin_avg = np.mean([t.pnl_pct for t in bin_trades])
            bin_wr = bin_wins / len(bin_trades) * 100
            print(f"  {label}: {len(bin_trades)}건 | "
                  f"승률 {bin_wr:.0f}% | 평균 {bin_avg:+.1f}%")
        print()

        # 청산 사유 분포
        print("  청산 사유 분포")
        print(line)
        reasons = defaultdict(int)
        reason_pnl = defaultdict(list)
        for t in trades:
            reasons[t.exit_reason] += 1
            reason_pnl[t.exit_reason].append(t.pnl_pct)
        for reason in ["TP_HIT", "TRAILING", "SL_HIT", "TIMEOUT", "PERIOD_END"]:
            cnt = reasons.get(reason, 0)
            if cnt > 0:
                avg_pnl = np.mean(reason_pnl[reason])
                print(f"  {reason:12s}: {cnt:3d}건 ({cnt/n*100:.0f}%) "
                      f"평균 {avg_pnl:+.1f}%")
        print()

        # Top / Worst 거래
        print("  Top 5 수익")
        print(line)
        sorted_trades = sorted(trades, key=lambda x: x.pnl_pct, reverse=True)
        for t in sorted_trades[:5]:
            print(f"  {t.name:12s} {t.entry_date}→{t.exit_date} "
                  f"스코어{t.score:.0f} Vol{t.vol_spike:.1f}x "
                  f"{t.pnl_pct:+.1f}% [{t.exit_reason}]")
        print()
        print("  Worst 5 손실")
        print(line)
        for t in sorted_trades[-5:]:
            print(f"  {t.name:12s} {t.entry_date}→{t.exit_date} "
                  f"스코어{t.score:.0f} Vol{t.vol_spike:.1f}x "
                  f"{t.pnl_pct:+.1f}% [{t.exit_reason}]")
        print(sep)

        # 주간 수익률
        print()
        print("  주간 수익률")
        print(line)
        weekly = defaultdict(lambda: {"pnl": 0, "trades": 0})
        for t in trades:
            # 주차 계산
            exit_ts = pd.Timestamp(t.exit_date)
            week = f"{exit_ts.isocalendar()[0]}-W{exit_ts.isocalendar()[1]:02d}"
            weekly[week]["pnl"] += t.pnl
            weekly[week]["trades"] += 1
        running = self.initial_cash
        for w in sorted(weekly):
            d = weekly[w]
            wpct = d["pnl"] / running * 100
            running += d["pnl"]
            print(f"  {w}: {wpct:+.1f}% ({d['trades']}거래)")
        print()

    def save_csv(self):
        if not self.trades:
            return
        rows = [{
            "code": t.code, "name": t.name,
            "entry_date": t.entry_date, "exit_date": t.exit_date,
            "entry_price": t.entry_price, "exit_price": t.exit_price,
            "shares": t.shares, "pnl": t.pnl, "pnl_pct": t.pnl_pct,
            "exit_reason": t.exit_reason, "hold_days": t.hold_days,
            "score": t.score, "vol_spike": t.vol_spike,
            "price_5d": t.price_5d,
        } for t in self.trades]
        df = pd.DataFrame(rows)
        out_path = ROOT / "backtest" / (
            f"momentum_result_{self.start.date()}_{self.end.date()}.csv"
        )
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  CSV: {out_path.name}")


# ════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="모멘텀 헌터 백테스트"
    )
    parser.add_argument("--start", default="2026-01-05",
                        help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-03-06",
                        help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--cash", type=int, default=230_000,
                        help="초기자금 (원, 총자금의 20%%)")
    parser.add_argument("--max-pos", type=int, default=1,
                        help="최대 포지션 수")
    parser.add_argument("--max-hold", type=int, default=5,
                        help="최대 보유일")
    parser.add_argument("--largecap", action="store_true",
                        help="대형주 processed parquet 데이터 사용 (2년치)")
    args = parser.parse_args()

    bt = MomentumBacktester(
        start=args.start, end=args.end,
        initial_cash=args.cash,
        max_positions=args.max_pos,
        max_hold_days=args.max_hold,
        largecap=args.largecap,
    )
    bt.load_all()
    bt.run()
    bt.report()
    bt.save_csv()
