"""
============================================================================
 단타봇 (DayTradeBot) — 모멘텀 연속 베팅 엔진  [ "말 타기 허준" ]
============================================================================

철학:  "오르는 말이 '진짜' 달리는 걸 확인하고 올라타서, 짧게 먹고 내린다."
       강할 때 산다. 단, FOMO 추격이 아니라 '확인된 모멘텀'에만 올라탄다.
       그 '확인'이 생명선 — 풀리면 그 순간 초보로 썩는다.

퀀트봇과의 관계:
  - 둘 다 허준(확인 후 진입). 다른 건 '무엇을 확인하느냐'.
      퀀트봇 = 약할 때(지지 반등 확인) / 단타봇 = 강할 때(모멘텀 확인)
  - 공유 부품 재활용: Bar, Indicators, DataProvider, UniverseBuilder,
                      TrendGate, MacroGate, TelegramNotifier
  - 다른 부품: 신호 엔진(MomentumEngine) + 리스크(시간청산 포함)

엔진 흐름:
  [0] 매크로 게이트 (공통 차단기 — 갭 리스크 큰 봇이라 더 중요)
        ↓
  [1] 유니버스 (정적 필터, 퀀트봇 공유)
        ↓
  [2] 추세 게이트 (ADX>25 + 주봉 + 정배열, 퀀트봇 공유)
        ↓
  [3] 모멘텀 확인 (돌파 + 거래량 서지 + 마감강도 + RS + 수급동행)
        ↓
  [4] 작전주/과열 필터 (이격도 + 단발 급등 컷 + 윗꼬리)  ← 제주반도체 방지
        ↓
  [5] 리스크 (타이트 SL + 빠른 TP + ★2거래일 시간청산★)
        ↓
  [6] 텔레그램 알림 (종가/마감동시호가 매수, 사람 체결)
============================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from abc import abstractmethod

# 퀀트봇에서 공유 부품 재활용
from quant_bot import (
    Bar, FlowData, SymbolMeta, Indicators,
    DataProvider, UniverseBuilder, TrendGate, MacroGate,
    TelegramNotifier, QuantConfig,
)


# ===========================================================================
# 0. 단타봇 설정
# ===========================================================================
@dataclass
class DayTradeConfig:
    # 유니버스/추세 게이트는 퀀트봇 설정 재사용 (소부장·중형 사냥터 동일)
    base: QuantConfig = None

    # --- 모멘텀 확인 ---
    BREAKOUT_LOOKBACK: int = 20          # 최근 N일 박스 고점 돌파 확인
    VOLUME_SURGE_RATIO: float = 2.0      # 당일 거래량 ≥ 20일평균 ×2 = 서지
    VOL_MA_PERIOD: int = 20
    RS_LOOKBACK: int = 20                # 상대강도 산출 기간
    RS_MIN: float = 0.0                  # 시장 대비 초과수익 > 0 이어야 (강한 놈만)

    # --- 마감 동시호가 강도 ---
    CLOSE_POS_MIN: float = 0.6           # 종가가 당일 레인지 상위 40% (윗꼬리 짧음)
    # close_pos = (close-low)/(high-low) ≥ 0.6 이어야 '강하게 마무리'

    # --- 작전주/과열 필터 (제주반도체 방지) ---
    MAX_EXTENSION: float = 0.15          # 20일선 대비 이격 +15% 초과 = 과열, 추격 금지
    MAX_SINGLE_DAY_GAIN: float = 0.15    # 당일 +15% 초과 단발 급등 = 작전성, 추격 금지
    MAX_UPPER_WICK: float = 0.35         # 윗꼬리 비율 > 35% = 장중 떠넘김(분산매도) 신호

    # --- 수급 동행 ---
    REQUIRE_SUPPLY: bool = True          # 외인+기관 당일 순매수 필수

    # --- 상한가 전조 (옵션 가산점) ---
    ORDERBOOK_BUY_RATIO_BONUS: float = 2.0  # 매수잔량/매도잔량 ≥ 2 이면 가산

    # --- 리스크 (단타: 타이트 + 빠름) ---
    ACCOUNT_EQUITY: float = 100_000_000
    RISK_PER_TRADE_PCT: float = 0.007    # 1회 위험 0.7% (단타는 빈도↑라 회당 위험↓)
    MAX_POSITION_PCT: float = 0.12
    MAX_POSITIONS: int = 4
    SL_BUFFER: float = 0.005             # 손절 = 당일 저점 -0.5%
    MAX_SL_PCT: float = 0.04             # 손절폭 상한 4% (저점이 너무 멀면 4%로 캡)
    TP1_R: float = 1.5                   # 빠른 1차 익절 (절반)
    TP2_R: float = 3.0
    MAX_HOLD_DAYS: int = 2               # ★ 2거래일 경과 → 무조건 청산 (단타봇 정체성) ★

    def __post_init__(self):
        if self.base is None:
            self.base = QuantConfig()


# ===========================================================================
# 1. 단타봇 데이터 제공자 (퀀트봇 인터페이스 + 지수/호가 추가)
# ===========================================================================
class DayTradeDataProvider(DataProvider):
    @abstractmethod
    def get_index_bars(self, count: int = 60) -> list[Bar]:
        """RS 계산용 시장지수 일봉 (코스피/코스닥). 종목과 동일 길이 권장."""
        ...

    def get_orderbook_imbalance(self, code: str) -> Optional[float]:
        """매수잔량/매도잔량 비율 (상한가 전조 옵션). 미지원 시 None."""
        return None


# ===========================================================================
# 2. 모멘텀 신호 엔진 (단타봇 핵심)
# ===========================================================================
@dataclass
class MomentumSignal:
    fired: bool
    score: float
    today_low: float
    diag: dict


class MomentumEngine:
    def __init__(self, cfg: DayTradeConfig):
        self.cfg = cfg

    @staticmethod
    def _close_strength(bar: Bar) -> float:
        rng = bar.high - bar.low
        if rng <= 0:
            return 1.0
        return (bar.close - bar.low) / rng

    @staticmethod
    def _upper_wick_ratio(bar: Bar) -> float:
        rng = bar.high - bar.low
        if rng <= 0:
            return 0.0
        return (bar.high - bar.close) / rng

    def _relative_strength(self, bars: list[Bar], index_bars: list[Bar]) -> Optional[float]:
        n = self.cfg.RS_LOOKBACK
        if len(bars) <= n or len(index_bars) <= n:
            return None
        stock_ret = bars[-1].close / bars[-1 - n].close - 1
        index_ret = index_bars[-1].close / index_bars[-1 - n].close - 1
        return stock_ret - index_ret

    def evaluate(self, bars: list[Bar], index_bars: list[Bar],
                 flow: FlowData, orderbook_ratio: Optional[float]) -> MomentumSignal:
        c = self.cfg
        diag: dict = {}
        today = bars[-1]
        yday = bars[-2]
        closes = [b.close for b in bars]
        vols = [b.volume for b in bars]
        ma20 = Indicators.sma(closes, 20)
        vol_ma = Indicators.sma(vols, c.VOL_MA_PERIOD)

        # ---- 하드 조건 (모두 충족해야 발화) ----
        # ① 박스 돌파: 당일 종가가 최근 N일(당일 제외) 고점 돌파
        box_high = max(b.high for b in bars[-1 - c.BREAKOUT_LOOKBACK:-1])
        breakout = today.close > box_high
        diag["breakout"] = breakout

        # ② 거래량 서지
        vol_surge = vol_ma and today.volume >= vol_ma * c.VOLUME_SURGE_RATIO
        diag["vol_surge"] = bool(vol_surge)
        diag["vol_mult"] = (today.volume / vol_ma) if vol_ma else None

        # ③ 마감 강도 (종가가 레인지 상위 + 양봉)
        cpos = self._close_strength(today)
        close_strong = cpos >= c.CLOSE_POS_MIN and today.close > today.open
        diag["close_pos"] = round(cpos, 2)

        # ④ 상대강도 (시장 대비 강함)
        rs = self._relative_strength(bars, index_bars)
        rs_ok = rs is not None and rs > c.RS_MIN
        diag["rs"] = round(rs, 4) if rs is not None else None

        # ⑤ 수급 동행 (외인+기관 당일 순매수)
        supply_ok = True
        if c.REQUIRE_SUPPLY:
            fi = (flow.foreign[-1] if flow.foreign else 0) + \
                 (flow.institution[-1] if flow.institution else 0)
            supply_ok = fi > 0
            diag["supply_today"] = fi

        hard = breakout and vol_surge and close_strong and rs_ok and supply_ok

        # ---- 작전주/과열 필터 (하나라도 걸리면 거부) ----
        ext = (today.close / ma20 - 1) if ma20 else 0
        day_gain = today.close / yday.close - 1
        wick = self._upper_wick_ratio(today)
        diag.update(extension=round(ext, 3), day_gain=round(day_gain, 3),
                    upper_wick=round(wick, 2))

        rejected = None
        if ext > c.MAX_EXTENSION:
            rejected = f"이격 과열({ext:+.0%}>{c.MAX_EXTENSION:.0%})"
        elif day_gain > c.MAX_SINGLE_DAY_GAIN:
            rejected = f"단발 급등({day_gain:+.0%}) 추격 금지(작전성)"
        elif wick > c.MAX_UPPER_WICK:
            rejected = f"윗꼬리 과다({wick:.0%}) 장중 떠넘김 의심"
        diag["rejected"] = rejected

        fired = bool(hard and rejected is None)

        # ---- 소프트 점수 (랭킹용) ----
        score = 0.0
        if fired:
            score += min((today.volume / vol_ma), 5.0)       # 거래량 배수 (최대 5)
            score += cpos * 2                                  # 마감 강도
            score += min(rs * 10, 3.0) if rs else 0           # RS 강도
            if orderbook_ratio and orderbook_ratio >= c.ORDERBOOK_BUY_RATIO_BONUS:
                score += 1.5                                   # 상한가 전조 가산
                diag["orderbook_bonus"] = True

        return MomentumSignal(fired=fired, score=round(score, 2),
                              today_low=today.low, diag=diag)


# ===========================================================================
# 3. 리스크 (단타: 타이트 + 빠름 + 시간청산)
# ===========================================================================
@dataclass
class DayTradePlan:
    code: str
    name: str
    entry: float
    stop_loss: float
    tp1: float
    tp2: float
    shares: int
    risk_amount: float
    rr: float
    position_value: float
    momentum_score: float
    max_hold_days: int
    note: str = ""


class DayTradeRiskManager:
    def __init__(self, cfg: DayTradeConfig):
        self.cfg = cfg

    def build_plan(self, meta: SymbolMeta, entry: float, today_low: float,
                   score: float, macro_scale: float, open_positions: int) -> Optional[DayTradePlan]:
        c = self.cfg
        if open_positions >= c.MAX_POSITIONS:
            return None

        # 손절 = 당일 저점 -버퍼, 단 손절폭 상한(4%)으로 캡
        stop = today_low * (1 - c.SL_BUFFER)
        if (entry - stop) / entry > c.MAX_SL_PCT:
            stop = entry * (1 - c.MAX_SL_PCT)
        risk_per_share = entry - stop
        if risk_per_share <= 0:
            return None

        tp1 = entry + risk_per_share * c.TP1_R
        tp2 = entry + risk_per_share * c.TP2_R
        rr = c.TP1_R

        risk_budget = c.ACCOUNT_EQUITY * c.RISK_PER_TRADE_PCT * macro_scale
        shares = int(risk_budget / risk_per_share)
        if shares <= 0:
            return None

        position_value = shares * entry
        max_value = c.ACCOUNT_EQUITY * c.MAX_POSITION_PCT
        if position_value > max_value:
            shares = int(max_value / entry)
            position_value = shares * entry
        if shares <= 0:
            return None

        return DayTradePlan(
            code=meta.code, name=meta.name, entry=entry, stop_loss=stop,
            tp1=tp1, tp2=tp2, shares=shares, risk_amount=shares * risk_per_share,
            rr=rr, position_value=position_value, momentum_score=score,
            max_hold_days=c.MAX_HOLD_DAYS,
        )


# ===========================================================================
# 4. 알림 포맷 (단타용)
# ===========================================================================
def format_daytrade_signal(plan: DayTradePlan, macro_note: str, diag: dict) -> str:
    return (
        "🟠 [단타봇] 매수 신호 (모멘텀 확인 = 말 타기)\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"종목 : {plan.name} ({plan.code})\n"
        f"진입 : {plan.entry:,.0f}  (종가/마감동시호가 매수)\n"
        f"손절 : {plan.stop_loss:,.0f}  ({(plan.stop_loss/plan.entry-1)*100:+.1f}%)\n"
        f"목표 : TP1 {plan.tp1:,.0f}(절반) / TP2 {plan.tp2:,.0f}\n"
        f"★청산: D+{plan.max_hold_days} 경과 시 무조건 정리 (모멘텀 안 나오면 자른다)\n"
        f"수량 : {plan.shares:,}주  (≈{plan.position_value:,.0f}원)\n"
        f"위험 : {plan.risk_amount:,.0f}원  /  RR {plan.rr:.1f}\n"
        f"확인 : 거래량 ×{diag.get('vol_mult', 0):.1f} / 마감강도 {diag.get('close_pos')} / RS {diag.get('rs')}\n"
        f"모멘텀점수 {plan.momentum_score} / 매크로 {macro_note}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ 오버나잇 갭 리스크 — 분산 필수. 체결은 사람이."
    )


# ===========================================================================
# 5. 오케스트레이터
# ===========================================================================
class DayTradeBot:
    def __init__(self, provider: DayTradeDataProvider, cfg: DayTradeConfig = None,
                 notifier: Optional[TelegramNotifier] = None):
        self.cfg = cfg or DayTradeConfig()
        self.p = provider
        self.universe_builder = UniverseBuilder(self.cfg.base)
        self.trend_gate = TrendGate(self.cfg.base)      # 퀀트봇과 동일 게이트 (둘 다 허준)
        self.engine = MomentumEngine(self.cfg)
        self.risk = DayTradeRiskManager(self.cfg)
        self.macro = MacroGate(self.cfg.base)
        self.notifier = notifier or TelegramNotifier()
        self.open_positions = 0

    def run_eod_scan(self, top_n: int = 4) -> list[DayTradePlan]:
        """장 마감 후 1회. 모멘텀 확인 종목 → 점수 랭킹 → 상위 N개 알림."""
        # 0) 매크로 게이트 (갭 리스크 큰 봇이라 더 엄격하게 작동)
        macro_score = self.p.get_macro_score()
        allow, scale, macro_note = self.macro.evaluate(macro_score)
        if not allow:
            self.notifier.send(f"🔴 [단타봇] {macro_note}\n오버나잇 진입 없음.")
            return []

        index_bars = self.p.get_index_bars()
        universe = self.universe_builder.build(self.p.get_universe())
        candidates: list[tuple[float, SymbolMeta, MomentumSignal]] = []

        for meta in universe:
            bars = self.p.get_daily_bars(meta.code)
            if len(bars) < self.cfg.base.MA_LONG + 5:
                continue
            gate_ok, _ = self.trend_gate.passes(bars)
            if not gate_ok:
                continue
            flow = self.p.get_flow(meta.code)
            ob = self.p.get_orderbook_imbalance(meta.code)
            sig = self.engine.evaluate(bars, index_bars, flow, ob)
            if sig.fired:
                candidates.append((sig.score, meta, sig))

        # 점수 랭킹 → 상위 N
        candidates.sort(key=lambda x: x[0], reverse=True)
        plans: list[DayTradePlan] = []
        for score, meta, sig in candidates[:top_n]:
            bars = self.p.get_daily_bars(meta.code)
            entry = bars[-1].close
            plan = self.risk.build_plan(meta, entry, sig.today_low, score, scale,
                                        self.open_positions + len(plans))
            if plan:
                plans.append(plan)
                self.notifier.send(format_daytrade_signal(plan, macro_note, sig.diag))

        if not plans:
            self.notifier.send("⚪ [단타봇] 오늘 올라탈 말 없음. (확인 안 되면 안 탄다)")
        return plans


# ===========================================================================
# 6. 데모 — 건강한 돌파(신호 발화) vs 작전주(필터 거부) 동시 검증
# ===========================================================================
class MockDayTradeProvider(DayTradeDataProvider):
    def __init__(self):
        self._healthy = self._make_healthy_breakout()
        self._pump = self._make_pump_and_dump()
        self._index = self._make_index()

    def _make_index(self) -> list[Bar]:
        bars, p = [], 2500.0
        for i in range(160):
            p *= 1.0005
            bars.append(Bar(f"I{i}", p, p * 1.003, p * 0.997, p, 1))
        return bars

    def _make_healthy_breakout(self) -> list[Bar]:
        """정배열 + 박스 + 당일 완만한 돌파 + 거래량 서지 + 강한 마감 (작전 아님)"""
        bars, p = [], 50_000
        for i in range(150):                      # 장기 추세 (정배열)
            p *= 1.003
            bars.append(Bar(f"D{i}", p * 0.997, p * 1.006, p * 0.994, p, 1_000_000))
        for i in range(8):                        # 박스 횡보
            bars.append(Bar(f"B{i}", p * 0.998, p * 1.005, p * 0.995, p * 1.0, 800_000))
        # 돌파일: +6% 완만 돌파, 거래량 2.5배, 종가 = 고가 부근(강한 마감)
        np = p * 1.06
        bars.append(Bar("BRK", p * 1.005, np * 1.002, p * 1.003, np, 2_200_000))
        return bars

    def _make_pump_and_dump(self) -> list[Bar]:
        """정배열·돌파·거래량은 OK인데 +24% 단발 급등 + 긴 윗꼬리 → 작전주 필터에 걸려야 정상"""
        bars, p = [], 50_000
        for i in range(150):
            p *= 1.003
            bars.append(Bar(f"D{i}", p * 0.997, p * 1.006, p * 0.994, p, 1_000_000))
        for i in range(8):
            bars.append(Bar(f"B{i}", p * 0.998, p * 1.005, p * 0.995, p * 1.0, 800_000))
        # 작전일: 장중 +30% 급등 후 윗꼬리 길게 달고 +24% 마감 (떠넘김)
        high = p * 1.30
        close = p * 1.24
        bars.append(Bar("PUMP", p * 1.01, high, p * 1.0, close, 5_000_000))
        return bars

    def get_universe(self) -> list[SymbolMeta]:
        return [
            SymbolMeta("111111", "건강돌파주", 60_000, 50_000_000_000, 2_000_000_000_000, False, True),
            SymbolMeta("999999", "작전급등주", 60_000, 50_000_000_000, 2_000_000_000_000, False, True),
        ]

    def get_daily_bars(self, code: str, count: int = 250) -> list[Bar]:
        return self._healthy if code == "111111" else self._pump

    def get_index_bars(self, count: int = 60) -> list[Bar]:
        return self._index

    def get_flow(self, code: str, days: int = 10) -> FlowData:
        return FlowData(foreign=[2e8], institution=[1e8], pension=[0])

    def get_orderbook_imbalance(self, code: str) -> Optional[float]:
        return 2.5 if code == "111111" else 1.0

    def get_macro_score(self) -> float:
        return 2.0


if __name__ == "__main__":
    bot = DayTradeBot(MockDayTradeProvider())
    print("=" * 60)
    print(" 단타봇 데모 — 건강돌파(발화) vs 작전급등(거부) 검증")
    print("=" * 60)
    plans = bot.run_eod_scan()
    print("\n[검증] 건강돌파주는 🟠 신호, 작전급등주는 필터 거부돼야 정상.")
    # 작전주가 왜 걸렸는지 진단 출력
    prov = MockDayTradeProvider()
    sig = MomentumEngine(DayTradeConfig()).evaluate(
        prov.get_daily_bars("999999"), prov.get_index_bars(),
        prov.get_flow("999999"), prov.get_orderbook_imbalance("999999"))
    print(f"작전급등주 판정: fired={sig.fired}, 거부사유={sig.diag.get('rejected')}")
