# -*- coding: utf-8 -*-
"""
Body Hunter v2.3 Paper Trader
==============================
현대차·NAVER 중심 + 수급 A+/A 필터 + 실시간 KIS 5분봉

사용법:
  python paper_trader.py --kis          # KIS API 실시간
  python paper_trader.py --replay DATE  # 과거 데이터 리플레이 (YYYY-MM-DD)

흐름:
  1. 수급 스캔 (3D+4D+5D) → A+/A + STRONG_BUY/BUY/ENTER 만 통과
  2. 첫 5분봉 = FCR 설정
  3. 5분마다 봉 수신 → Body Hunter v2.3 업데이트
  4. 가상 매매 기록 (진입/청산/손익)
  5. 장 마감 후 일일 리포트
"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from strategies.body_hunter_v2 import BodyHunterV2, BodyState
from data.supply_analyzer import SupplyAnalyzer

# ─── 설정 ───────────────────────────────────────────────

# WR >= 40% 검증 완료 종목 (백테스트 기준)
QUALIFIED_STOCKS = {
    "005380": "현대차",
    "035420": "NAVER",
}

# Body Hunter v2.3 파라미터 (백테스트 검증 완료)
V23_PARAMS = dict(
    direction="LONG",
    retest_required=True,
    volume_surge_min=1.3,
    sl_ratio=0.6,
    fixed_tp_rr=2.0,
    close_only_breakout=True,
    choppy_max_attempts=3,
    trailing_atr_mult=1.2,
    breakeven_rr=0.3,
    cutoff_time="15:00",
)

# 수급 필터
SUPPLY_ACTIONS = {"STRONG_BUY", "BUY", "ENTER"}
SUPPLY_GRADES = {"A+", "A"}

# 리스크
RISK_PER_TRADE = 50000  # 원 (고정 리스크)

logger = logging.getLogger("paper_trader")


# ─── 가상 포지션 ────────────────────────────────────────

@dataclass
class PaperPosition:
    code: str
    name: str
    direction: str
    entry_price: float
    stop_loss: float
    entry_time: str
    quantity: int = 0  # RISK / (entry - SL)
    exit_price: float = 0.0
    exit_time: str = ""
    exit_reason: str = ""
    rr_realized: float = 0.0
    pnl_krw: float = 0.0
    closed: bool = False


# ─── KIS API 래퍼 ───────────────────────────────────────

class KISDataFeed:
    """KIS API에서 당일 1분봉 → 5분봉 변환"""

    def __init__(self):
        import mojito
        self.broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"),
            api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"),
            mock=False,
        )
        logger.info("KIS API 연결 완료")

    def fetch_today_5min(self, code: str) -> Optional[pd.DataFrame]:
        """당일 1분봉 → 5분봉 리샘플링"""
        try:
            resp = self.broker.fetch_today_1m_ohlcv(code)
        except Exception as e:
            logger.error(f"[{code}] 1분봉 조회 실패: {e}")
            return None

        rows = resp.get("output2", [])
        if not rows:
            return None

        data = []
        for r in rows:
            hour = str(r.get("stck_cntg_hour", "")).zfill(6)
            date_str = r.get("stck_bsop_date", "")
            if not date_str or not hour:
                continue
            ts = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {hour[:2]}:{hour[2:4]}:{hour[4:6]}"
            data.append({
                "datetime": pd.Timestamp(ts),
                "open": float(r.get("stck_oprc", 0)),
                "high": float(r.get("stck_hgpr", 0)),
                "low": float(r.get("stck_lwpr", 0)),
                "close": float(r.get("stck_prpr", 0)),
                "volume": float(r.get("cntg_vol", 0)),
            })

        if not data:
            return None

        df = pd.DataFrame(data).set_index("datetime").sort_index()

        # 5분봉 리샘플링
        df5 = df.resample("5min").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna()

        # 장중만 (09:00~15:25)
        df5 = df5.between_time("09:00", "15:25")
        return df5

    def fetch_current_price(self, code: str) -> Optional[float]:
        """현재가 조회"""
        try:
            resp = self.broker.fetch_price(code)
            return float(resp.get("output", {}).get("stck_prpr", 0))
        except Exception as e:
            logger.error(f"[{code}] 현재가 조회 실패: {e}")
            return None


class ReplayDataFeed:
    """과거 yfinance 5분봉 리플레이"""

    def __init__(self, target_date: str):
        self.target_date = pd.Timestamp(target_date).normalize()
        self._data: Dict[str, pd.DataFrame] = {}

        minute5_dir = ROOT / "data_store" / "minute5"
        for code in QUALIFIED_STOCKS:
            path = minute5_dir / f"{code}.csv"
            if path.exists():
                df = pd.read_csv(path, index_col=0, parse_dates=True)
                day = df[df.index.normalize() == self.target_date]
                if len(day) >= 20:
                    self._data[code] = day
                    logger.info(f"[{code}] 리플레이 데이터: {len(day)}봉")

    def get_day_candles(self, code: str) -> Optional[pd.DataFrame]:
        return self._data.get(code)


# ─── 수급 스캔 ──────────────────────────────────────────

def morning_scan(analyzer: SupplyAnalyzer) -> Dict[str, dict]:
    """아침 수급 스캔 → 오늘 거래할 종목 선별"""
    targets = {}

    print(f"\n{'='*60}")
    print(f"  수급 스캔 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print(f"{'='*60}")
    print(f"  {'종목':>10} {'등급':>4} {'점수':>4} {'액션':>12} {'5D':>10} {'판정':>6}")
    print(f"  {'-'*52}")

    for code, name in QUALIFIED_STOCKS.items():
        try:
            full = analyzer.analyze_full(code)
        except Exception as e:
            logger.warning(f"[{code}] 수급 분석 실패: {e}")
            print(f"  {name:>10} {'ERR':>4} {'':>4} {'':>12} {'':>10} {'SKIP':>6}")
            continue

        if full is None:
            print(f"  {name:>10} {'N/A':>4} {'':>4} {'':>12} {'':>10} {'SKIP':>6}")
            continue

        grade = full.score.grade if full.score else "?"
        total = full.score.total_score if full.score else 0
        action = full.action
        stab = full.stability.stability_grade if full.stability else "?"

        grade_ok = grade in SUPPLY_GRADES
        action_ok = action in SUPPLY_ACTIONS
        passed = grade_ok and action_ok

        mark = "TRADE" if passed else "SKIP"
        print(f"  {name:>10} {grade:>4} {total:>4.0f} {action:>12} {stab:>10} {mark:>6}")

        if passed:
            targets[code] = {
                "name": name,
                "grade": grade,
                "action": action,
                "stability": stab,
                "full": full,
            }

    print(f"  {'-'*52}")
    if targets:
        print(f"  오늘 대상: {', '.join(QUALIFIED_STOCKS[c] for c in targets)}")
    else:
        print(f"  오늘 대상 종목 없음 (수급 조건 미달)")
    print(f"{'='*60}\n")

    return targets


# ─── Paper Trading 엔진 ─────────────────────────────────

class PaperTrader:
    """가상 매매 엔진"""

    def __init__(self, risk_per_trade: float = RISK_PER_TRADE):
        self.risk = risk_per_trade
        self.positions: List[PaperPosition] = []
        self.hunters: Dict[str, BodyHunterV2] = {}
        self._bar_counts: Dict[str, int] = {}

    def register(self, code: str, name: str, avg_volume: float = 0):
        """종목 등록"""
        self.hunters[code] = BodyHunterV2(
            ticker=code, name=name, **V23_PARAMS
        )
        self._bar_counts[code] = 0
        self._vol_buffer: Dict[str, list] = getattr(self, '_vol_buffer', {})
        self._vol_buffer[code] = []
        self._avg_vol_override: Dict[str, float] = getattr(self, '_avg_vol_override', {})
        self._avg_vol_override[code] = avg_volume
        logger.info(f"[{code}] {name} 등록 완료")

    def feed_bar(self, code: str, candle: pd.Series) -> Optional[dict]:
        """5분봉 1개 입력 → 액션 반환"""
        hunter = self.hunters.get(code)
        if hunter is None:
            return None

        name = QUALIFIED_STOCKS.get(code, code)
        self._bar_counts[code] += 1

        # 볼륨 버퍼에 누적 (avg_vol 계산용)
        vol = candle["volume"]
        if vol > 0:
            self._vol_buffer.setdefault(code, []).append(vol)

        # 첫 봉: FCR 설정
        if self._bar_counts[code] == 1:
            first = pd.Series({
                "high": candle["high"], "low": candle["low"],
                "open": candle["open"], "close": candle["close"],
                "volume": candle["volume"],
            })
            # avg_vol: 사전 설정 > 볼륨 버퍼 > 첫봉 (0이면 1로 대체)
            avg_vol = self._avg_vol_override.get(code, 0)
            if avg_vol <= 0:
                avg_vol = max(vol, 1)
            hunter.set_levels(first, avg_volume=avg_vol)

            fcr_range = candle["high"] - candle["low"]
            print(f"  [{code}] FCR 설정: {candle['low']:,.0f} ~ {candle['high']:,.0f} "
                  f"(range: {fcr_range:,.0f}) avgVol: {avg_vol:,.0f}")
            return {"action": "FCR_SET", "code": code}

        # 5봉 후 avg_vol 업데이트
        if self._bar_counts[code] == 5:
            vols = self._vol_buffer.get(code, [])
            if vols:
                new_avg = sum(vols) / len(vols)
                hunter._avg_volume = new_avg
                logger.info(f"[{code}] avg_volume 업데이트: {new_avg:,.0f}")

        # 이후 봉: Body Hunter 업데이트
        bar = pd.Series({
            "open": candle["open"], "high": candle["high"],
            "low": candle["low"], "close": candle["close"],
            "volume": candle["volume"],
        })
        bar.name = candle.name if hasattr(candle, "name") else pd.Timestamp.now()

        result = hunter.update(bar)
        action = result.get("action", "WAIT")

        if action == "ENTER":
            pos_data = result.get("position")
            if pos_data:
                entry = pos_data.entry_price
                sl = pos_data.stop_loss
                risk_per_share = abs(entry - sl)
                qty = max(1, int(self.risk / risk_per_share)) if risk_per_share > 0 else 1

                pp = PaperPosition(
                    code=code, name=name,
                    direction="LONG",
                    entry_price=entry,
                    stop_loss=sl,
                    entry_time=str(bar.name),
                    quantity=qty,
                )
                self.positions.append(pp)

                tp = entry + risk_per_share * V23_PARAMS["fixed_tp_rr"]
                print(f"\n  >>> [{code}] PAPER BUY @ {entry:,.0f}")
                print(f"      SL: {sl:,.0f} | TP: {tp:,.0f} ({V23_PARAMS['fixed_tp_rr']:.0f}R)")
                print(f"      수량: {qty}주 | 리스크: {risk_per_share * qty:,.0f}원\n")

                return {"action": "ENTER", "code": code, "position": pp}

        elif action == "EXIT":
            exit_price = result.get("exit_price", 0)
            reason = result.get("reason", "")
            rr = result.get("rr_realized", 0)

            # 가장 최근 미청산 포지션 찾기
            for pp in reversed(self.positions):
                if pp.code == code and not pp.closed:
                    pp.exit_price = exit_price
                    pp.exit_time = str(bar.name)
                    pp.exit_reason = reason
                    pp.rr_realized = rr
                    pp.pnl_krw = (exit_price - pp.entry_price) * pp.quantity
                    pp.closed = True

                    icon = "$" if rr > 0 else "X"
                    print(f"\n  {icon} [{code}] PAPER SELL @ {exit_price:,.0f} ({reason})")
                    print(f"      진입: {pp.entry_price:,.0f} → 청산: {exit_price:,.0f}")
                    print(f"      RR: {rr:+.2f} | PnL: {pp.pnl_krw:+,.0f}원\n")

                    return {"action": "EXIT", "code": code, "position": pp}
                    break

        elif action == "HOLD":
            pos_data = result.get("position")
            rr = pos_data.rr_current if pos_data else 0
            bars = pos_data.hold_bars if pos_data else 0
            if bars % 6 == 0:  # 30분마다 상태 출력
                print(f"  [{code}] HOLD {bars}봉 RR:{rr:+.2f} "
                      f"@ {candle['close']:,.0f}")

        return result

    def summary(self):
        """일일 요약"""
        closed = [p for p in self.positions if p.closed]
        open_pos = [p for p in self.positions if not p.closed]

        print(f"\n{'='*60}")
        print(f"  Paper Trading 일일 요약")
        print(f"{'='*60}")

        if not closed and not open_pos:
            print(f"  오늘 거래 없음")
            print(f"{'='*60}\n")
            return

        # 청산 거래
        if closed:
            wins = sum(1 for p in closed if p.rr_realized > 0)
            total_pnl = sum(p.pnl_krw for p in closed)
            total_rr = sum(p.rr_realized for p in closed)

            print(f"\n  청산 거래: {len(closed)}건")
            print(f"  {'-'*56}")
            for p in closed:
                icon = "$" if p.rr_realized > 0 else "X"
                print(f"    {icon} {p.name}({p.code}) "
                      f"{p.entry_price:>10,.0f} → {p.exit_price:>10,.0f} "
                      f"RR:{p.rr_realized:>+6.2f} PnL:{p.pnl_krw:>+10,.0f} "
                      f"({p.exit_reason})")

            print(f"  {'-'*56}")
            wr = wins / len(closed) * 100 if closed else 0
            print(f"  승률: {wr:.0f}% ({wins}W/{len(closed)-wins}L)")
            print(f"  누적 RR: {total_rr:+.2f}")
            print(f"  누적 PnL: {total_pnl:+,.0f}원")

        # 미청산
        if open_pos:
            print(f"\n  미청산: {len(open_pos)}건")
            for p in open_pos:
                print(f"    {p.name}({p.code}) @ {p.entry_price:,.0f} "
                      f"SL:{p.stop_loss:,.0f} (진입: {p.entry_time})")

        print(f"{'='*60}\n")


# ─── 실행 모드 ──────────────────────────────────────────

def run_kis_live():
    """KIS API 실시간 페이퍼 트레이딩"""
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT.parent / ".env")  # 상위 폴더 .env도 로드

    feed = KISDataFeed()
    analyzer = SupplyAnalyzer()
    trader = PaperTrader()

    # 1. 아침 수급 스캔
    targets = morning_scan(analyzer)
    if not targets:
        print("오늘 거래 대상 없음. 종료.")
        return

    for code in targets:
        trader.register(code, QUALIFIED_STOCKS[code])

    print(f"\n  실시간 모니터링 시작 (5분 간격)")
    print(f"  Ctrl+C로 종료\n")

    last_bar_counts = {code: 0 for code in targets}

    try:
        while True:
            now = datetime.now()

            # 장 시간 체크 (09:00 ~ 15:30)
            if now.hour < 9 or (now.hour >= 15 and now.minute >= 30):
                if now.hour >= 15 and now.minute >= 30:
                    print(f"\n  장 마감 ({now.strftime('%H:%M')})")
                    break
                wait = max(1, (9 * 60 - now.hour * 60 - now.minute) * 60)
                print(f"  장 시작 대기중... ({now.strftime('%H:%M')})")
                time.sleep(min(wait, 60))
                continue

            # 각 종목 5분봉 조회 + 엔진 피딩
            for code in targets:
                df5 = feed.fetch_today_5min(code)
                if df5 is None or df5.empty:
                    continue

                # 새 봉만 피딩
                new_count = len(df5)
                if new_count > last_bar_counts[code]:
                    for i in range(last_bar_counts[code], new_count):
                        candle = df5.iloc[i]
                        candle.name = df5.index[i]
                        trader.feed_bar(code, candle)
                    last_bar_counts[code] = new_count

            # 5분 대기 (다음 봉 완성까지)
            now2 = datetime.now()
            next_5min = now2 + timedelta(minutes=5 - now2.minute % 5,
                                         seconds=-now2.second)
            wait_sec = max(10, (next_5min - now2).total_seconds() + 5)
            time.sleep(wait_sec)

    except KeyboardInterrupt:
        print(f"\n  수동 종료")

    trader.summary()


def run_replay(date_str: str):
    """과거 데이터 리플레이 모드"""
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT.parent / ".env")  # 상위 폴더 .env도 로드

    analyzer = SupplyAnalyzer()
    trader = PaperTrader()
    feed = ReplayDataFeed(date_str)

    # 수급 스캔
    targets = morning_scan(analyzer)

    # 수급 무관하게 WR 통과 종목은 무조건 포함 (리플레이용)
    for code, name in QUALIFIED_STOCKS.items():
        if code not in targets:
            targets[code] = {"name": name, "grade": "?", "action": "REPLAY"}

    for code in list(targets.keys()):
        day = feed.get_day_candles(code)
        if day is None:
            print(f"  [{code}] {date_str} 데이터 없음 → 제외")
            del targets[code]
            continue
        trader.register(code, QUALIFIED_STOCKS[code])

    if not targets:
        print("리플레이 데이터 없음. 종료.")
        return

    print(f"\n  리플레이 시작: {date_str}")
    print(f"  종목: {', '.join(QUALIFIED_STOCKS[c] for c in targets)}\n")

    # 봉별 순차 피딩
    for code in targets:
        day = feed.get_day_candles(code)
        if day is None:
            continue

        # avg_vol 미리 계산 (첫 5봉 평균, vol>0인 것만)
        vols = [v for v in day["volume"].iloc[:5] if v > 0]
        avg_vol = sum(vols) / len(vols) if vols else day["volume"].mean()
        trader.hunters[code]._avg_volume = avg_vol

        print(f"\n  --- {QUALIFIED_STOCKS[code]}({code}) avgVol:{avg_vol:,.0f} ---")
        for i in range(len(day)):
            candle = day.iloc[i]
            candle_s = pd.Series({
                "open": candle["open"], "high": candle["high"],
                "low": candle["low"], "close": candle["close"],
                "volume": candle["volume"],
            })
            candle_s.name = day.index[i]
            trader.feed_bar(code, candle_s)

    trader.summary()


# ─── CLI ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Body Hunter v2.3 Paper Trader")
    parser.add_argument("--kis", action="store_true",
                        help="KIS API 실시간 모드")
    parser.add_argument("--replay", type=str, default=None,
                        help="리플레이 날짜 (YYYY-MM-DD)")
    parser.add_argument("--skip-supply", action="store_true",
                        help="수급 필터 건너뛰기 (리플레이용)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="상세 로그")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    print(f"\n{'='*60}")
    print(f"  Body Hunter v2.3 Paper Trader")
    print(f"  종목: {', '.join(f'{n}({c})' for c, n in QUALIFIED_STOCKS.items())}")
    print(f"  전략: FCR → 돌파 → 리테스트 → SL=range*0.6, TP=2R")
    print(f"  필터: 수급 A+/A + STRONG_BUY/BUY/ENTER")
    print(f"  리스크: {RISK_PER_TRADE:,}원/거래")
    print(f"{'='*60}")

    if args.replay:
        run_replay(args.replay)
    elif args.kis:
        run_kis_live()
    else:
        parser.print_help()
        print(f"\n  예시:")
        print(f"    python paper_trader.py --kis              # 실시간")
        print(f"    python paper_trader.py --replay 2026-01-07  # 리플레이")


if __name__ == "__main__":
    main()
