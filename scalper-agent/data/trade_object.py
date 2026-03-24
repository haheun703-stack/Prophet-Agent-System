# -*- coding: utf-8 -*-
"""
Trade Object Layer — 스크리너에서 트레이드 팩토리로
=====================================================
기존 스코어링 위에 Trade Builder 한 층 올려서,
모든 추천을 완전한 트레이드 계획으로 변환.

핵심: R:R < 1.5이면 점수 100점이어도 REJECT

Usage:
    python data/trade_object.py --test   # 현재 recommendation.json 기반 테스트
"""

import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

logger = logging.getLogger("BH.TradeObject")

_STORE = Path(__file__).resolve().parent.parent / "data_store"
TRADE_OBJECTS_PATH = _STORE / "trade_objects.json"

# ═══════════════════════════════════════════════════════
#  상수
# ═══════════════════════════════════════════════════════

CASH_RESERVE = 0.10           # 현금 10% 유보 — auto_trader min_cash_ratio와 동일
MAX_RISK_PER_TRADE = 0.02     # 트레이드당 최대 리스크 = 총자산 2%
MAX_POSITIONS = 2             # 최대 동시 포지션 수
MAX_SINGLE_PCT = 0.15         # 단일 종목 최대 비중 15%
DEFAULT_EQUITY = 50_000_000   # 기본 총자산 (테스트용)

# R:R 게이트 임계값
RR_MIN = 1.5                  # 최소 R:R
RR_PREFERRED = 2.0            # 선호 R:R
RR_MOMENTUM_EXCEPTION = 1.2   # MOMENTUM+HIGH 예외 허용 최소값

# 손절 캡
STOP_MAX_PCT = 5.0            # 최대 손절 -5%
STOP_MOMENTUM_PCT = 3.5       # MOMENTUM 손절 -3.5%

# 컨센서스 할인율
CONSENSUS_DISCOUNT = 0.35     # 단타: 컨센서스 업사이드의 35%


# ═══════════════════════════════════════════════════════
#  TradeObject 데이터 클래스
# ═══════════════════════════════════════════════════════

@dataclass
class TradeObject:
    # 식별
    trade_id: str
    code: str
    name: str

    # 스크리너 결과
    total_score: float
    sources: list
    regime: str = "NORMAL"
    confidence: str = "MED"

    # 트레이드 설계
    entry_price: int = 0
    stop_loss: int = 0
    stop_source: str = ""
    target_price: int = 0
    target_source: str = ""

    # R:R
    risk_pct: float = 0.0
    reward_pct: float = 0.0
    rr_ratio: float = 0.0
    rr_verdict: str = "REJECT"

    # 포지션 사이징
    position_krw: int = 0
    shares: int = 0
    risk_amount: int = 0
    conviction: float = 1.0

    # 보유기간
    expected_hold_days: int = 3
    hold_source: str = ""
    time_stop_days: int = 5

    # 기대수익률 상세
    expected_return_detail: dict = field(default_factory=dict)

    # 실행 추적
    status: str = "PLANNED"
    actual_entry: int = 0
    actual_exit: int = 0
    actual_pnl: float = 0.0
    actual_hold_days: int = 0
    close_reason: str = ""

    # 타임스탬프
    created_at: str = ""

    def invalidation_narrative(self) -> str:
        """시나리오 무효화 조건 1줄"""
        parts = []
        if self.stop_source == "SUPPORT":
            parts.append(f"지지선 {self.stop_loss:,}원 이탈")
        elif self.stop_source == "INST_COST":
            parts.append(f"매집원가 {self.stop_loss:,}원 하회")
        else:
            parts.append(f"{self.stop_loss:,}원 이탈(-{abs(self.risk_pct):.1f}%)")

        if self.regime == "MOMENTUM":
            parts.append("수급 순매도 전환")

        return " or ".join(parts)


# ═══════════════════════════════════════════════════════
#  StopPriceEngine — supply_analyzer.calc_baseline() 재사용
# ═══════════════════════════════════════════════════════

class StopPriceEngine:
    """손절가 산출 — 구조적 SL (고정 % 아님)"""

    def __init__(self):
        self._analyzer = None

    def _get_analyzer(self):
        if self._analyzer is None:
            try:
                from data.supply_analyzer import SupplyAnalyzer
                self._analyzer = SupplyAnalyzer()
            except Exception as e:
                logger.warning(f"SupplyAnalyzer 로드 실패: {e}")
        return self._analyzer

    def calc(self, code: str, entry: int, regime: str,
             existing_sl: int = 0) -> Tuple[int, str]:
        """
        Returns: (stop_price, source)
        우선순위: BaselineLevels.invalidation > existing_sl > fallback
        """
        analyzer = self._get_analyzer()

        # Method 1: supply_analyzer BaselineLevels
        bl_sl, bl_source = 0, ""
        if analyzer:
            try:
                bl = analyzer.calc_baseline(code)
                if bl and bl.invalidation > 0:
                    bl_sl = int(bl.invalidation)
                    bl_source = bl.invalidation_source or "ATR"
            except Exception as e:
                logger.debug(f"calc_baseline 실패 {code}: {e}")

        # Method 2: 기존 추천의 SL
        ex_sl = existing_sl if existing_sl > 0 else 0

        # 최종 선택: BaselineLevels 우선
        if bl_sl > 0:
            sl, source = bl_sl, bl_source
        elif ex_sl > 0:
            sl, source = ex_sl, "EXISTING"
        else:
            # Fallback: 고정 %
            pct = STOP_MOMENTUM_PCT if regime == "MOMENTUM" else STOP_MAX_PCT
            sl = int(entry * (1 - pct / 100))
            source = "DEFAULT"

        # 캡: 최대 -5% (R:R 유지)
        max_pct = STOP_MOMENTUM_PCT if regime == "MOMENTUM" else STOP_MAX_PCT
        floor = int(entry * (1 - max_pct / 100))
        if sl < floor:
            sl = floor

        # 손절이 진입가보다 높으면 안 됨
        if sl >= entry:
            sl = int(entry * 0.97)
            source = "CAP_3%"

        return sl, source


# ═══════════════════════════════════════════════════════
#  ExpectedReturnEngine — 3방법 가중 평균
# ═══════════════════════════════════════════════════════

class ExpectedReturnEngine:
    """기대수익률 산출 — 과거 통계 + 컨센서스 + 기존 TP"""

    def calc(self, code: str, entry: int, total_score: float,
             existing_tp: int = 0, hold_days: int = 3) -> Tuple[float, int, str, dict]:
        """
        Returns: (expected_return_pct, target_price, source, detail)
        """
        detail = {}

        # Method A: Score Bucket 과거 수익률
        ret_a = self._score_bucket_return(total_score, hold_days)
        detail["score_bucket"] = {"return_pct": round(ret_a, 2), "hold_days": hold_days}

        # Method B: 컨센서스 할인
        ret_b = self._consensus_return(code, entry)
        if ret_b is not None:
            detail["consensus"] = {"return_pct": round(ret_b, 2), "discount": CONSENSUS_DISCOUNT}

        # Method C: 기존 TP 기반
        if existing_tp > entry:
            ret_c = (existing_tp / entry - 1) * 100
        else:
            ret_c = ret_a  # fallback
        detail["existing_tp"] = {"return_pct": round(ret_c, 2), "tp": existing_tp}

        # 가중 평균
        if ret_b is not None:
            expected = ret_a * 0.35 + ret_b * 0.40 + ret_c * 0.25
            source = "BLEND"
        else:
            expected = ret_a * 0.55 + ret_c * 0.45
            source = "SCORE+TP"

        # 최소 기대수익 0.5%
        expected = max(expected, 0.5)
        target = int(entry * (1 + expected / 100))

        detail["blended"] = round(expected, 2)
        return expected, target, source, detail

    def _score_bucket_return(self, score: float, hold_days: int = 3) -> float:
        try:
            from data.trade_learner import get_expected_return
            return get_expected_return(score, hold_days)
        except Exception:
            # 절대 fallback
            if score >= 100: return 3.0
            if score >= 80: return 2.0
            if score >= 60: return 1.0
            return 0.5

    def _consensus_return(self, code: str, entry: int) -> Optional[float]:
        try:
            from data.consensus_scraper import fetch_consensus
            result = fetch_consensus(code)
            if result and result.get("target_price", 0) > 0:
                tp = result["target_price"]
                full_upside = (tp / entry - 1) * 100
                if full_upside > 0:
                    return full_upside * CONSENSUS_DISCOUNT
        except Exception as e:
            logger.debug(f"컨센서스 조회 실패 {code}: {e}")
        return None


# ═══════════════════════════════════════════════════════
#  HoldPeriodEngine — 보유기간 추정
# ═══════════════════════════════════════════════════════

class HoldPeriodEngine:
    """보유기간 추정 — 섹터 사이클 + 수급 지속성 기반"""

    def calc(self, regime: str, sector_hot_days: int = 0,
             sources: list = None) -> Tuple[int, str, int]:
        """
        Returns: (expected_days, source, time_stop_days)
        """
        if regime == "MOMENTUM":
            if sector_hot_days > 0:
                base = max(1, min(5, 6 - sector_hot_days))
                source = "SECTOR_CYCLE"
            else:
                base = 3
                source = "MOMENTUM_DEFAULT"
            time_stop = min(base + 2, 5)  # MOMENTUM: 최대 5일
        else:
            if sector_hot_days > 0:
                base = max(1, min(5, 7 - sector_hot_days))
                source = "SECTOR_CYCLE"
            else:
                base = 3
                source = "DEFAULT"
            time_stop = min(base + 3, 7)  # NORMAL: 최대 7일

        # 수급 소스가 많으면 +1일 (강한 확신)
        supply_sources = [s for s in (sources or [])
                          if any(k in s for k in ("tv:", "7SECRET(BUY", "bargain(SSS"))]
        if len(supply_sources) >= 2:
            base = min(base + 1, time_stop)

        return max(1, base), source, time_stop


# ═══════════════════════════════════════════════════════
#  PositionSizer — 현금 25% 철칙 하드코딩
# ═══════════════════════════════════════════════════════

class PositionSizer:
    """포지션 사이즈 — Kelly 변형 + 리스크 기반"""

    def calc_conviction(self, confidence: str, regime: str,
                        total_score: float, sources: list) -> float:
        """확신도 승수: 0.5 ~ 1.3"""
        mult = 1.0

        # 확신도
        if confidence == "HIGH":
            mult = 1.15
        elif confidence == "LOW":
            mult = 0.7

        # MOMENTUM + HIGH = 최고 확신
        if regime == "MOMENTUM" and confidence == "HIGH":
            mult = 1.3

        # 점수 보너스
        if total_score >= 120:
            mult += 0.1
        elif total_score >= 100:
            mult += 0.05

        # 교차 소스 보너스
        cross = len([s for s in sources if not s.startswith("brain")])
        if cross >= 4:
            mult += 0.1
        elif cross >= 3:
            mult += 0.05

        # TV QUIET_ACCUMULATION 보너스
        if any("QUIET" in s for s in sources):
            mult += 0.05

        return min(1.3, max(0.5, round(mult, 2)))

    def calc(self, equity: int, entry: int, stop: int,
             conviction: float, current_positions: int) -> Tuple[int, int, int]:
        """
        Returns: (shares, position_krw, risk_amount)
        """
        if entry <= 0 or stop >= entry:
            return 0, 0, 0

        usable = equity * (1 - CASH_RESERVE)
        risk_per_share = entry - stop

        # 리스크 기반 사이징
        max_risk_won = int(equity * MAX_RISK_PER_TRADE * conviction)
        shares = max_risk_won // risk_per_share if risk_per_share > 0 else 0
        position_krw = shares * entry

        # 슬롯 캡
        slots = max(1, MAX_POSITIONS - current_positions)
        slot_cap = int(usable / slots)
        if position_krw > slot_cap:
            shares = slot_cap // entry
            position_krw = shares * entry

        # 단일 종목 비중 캡
        max_position = int(equity * MAX_SINGLE_PCT)
        if position_krw > max_position:
            shares = max_position // entry
            position_krw = shares * entry

        risk_amount = shares * risk_per_share
        return shares, position_krw, risk_amount


# ═══════════════════════════════════════════════════════
#  RRGate — R:R 필터
# ═══════════════════════════════════════════════════════

class RRGate:
    """R:R < 1.5 → REJECT (점수 무관)
    패턴별 R:R override 지원 (market_journal 피드백 루프)
    """
    _overrides: dict = None

    @classmethod
    def _load_overrides(cls):
        path = _STORE / "learning" / "journal" / "rr_overrides.json"
        if path.exists():
            try:
                data = json.loads(path.read_text("utf-8"))
                cls._overrides = data.get("overrides", {})
                logger.info(f"[RRGate] rr_overrides 로드: {len(cls._overrides)}개 패턴")
            except Exception:
                cls._overrides = {}
        else:
            cls._overrides = {}

    @classmethod
    def _normalize_source(cls, src: str) -> str:
        """소스 문자열 → 패턴 키 (market_journal과 동일)"""
        s = src.lower().strip()
        if s.startswith("brain"):
            return ""
        if s.startswith("tv_cluster"):
            return "tv_cluster"
        if s.startswith("tv:"):
            part = src[3:].split("(")[0].strip().upper()
            return f"tv:{part}" if part else "tv"
        if s.startswith("7secret"):
            return "7SECRET"
        if ":" in s:
            return s.split(":")[0]
        if "(" in s:
            return s.split("(")[0]
        return s

    @classmethod
    def evaluate(cls, rr: float, regime: str, confidence: str,
                 sources: list = None) -> str:
        if cls._overrides is None:
            cls._load_overrides()

        # 패턴별 R:R override — 가장 낮은 값 사용
        effective_min = RR_MIN  # 기본 1.5
        override_used = None
        if sources and cls._overrides:
            for src in sources:
                key = cls._normalize_source(src)
                if key and key in cls._overrides:
                    ov_rr = cls._overrides[key].get("rr_min", RR_MIN)
                    if ov_rr < effective_min:
                        effective_min = ov_rr
                        override_used = key

        if override_used:
            logger.debug(f"[RRGate] R:R override 적용: {override_used} → min={effective_min}")

        if rr < 1.0:
            return "REJECT"
        if rr < effective_min:
            # MOMENTUM + HIGH만 예외 (rr >= 1.2)
            if (regime == "MOMENTUM" and confidence == "HIGH"
                    and rr >= RR_MOMENTUM_EXCEPTION):
                return "MARGINAL_PASS"
            return "REJECT"
        if rr < RR_PREFERRED:
            return "GOOD"
        return "STRONG_BUY"


# ═══════════════════════════════════════════════════════
#  TradeBuilder — 전체 파이프라인 조립
# ═══════════════════════════════════════════════════════

class TradeBuilder:
    """스크리너 결과 → TradeObject 변환"""

    def __init__(self, equity: int = DEFAULT_EQUITY, current_positions: int = 0):
        self.equity = equity
        self.current_positions = current_positions
        self.stop_engine = StopPriceEngine()
        self.return_engine = ExpectedReturnEngine()
        self.hold_engine = HoldPeriodEngine()
        self.sizer = PositionSizer()

    def build(self, stock: dict) -> TradeObject:
        """
        stock: RecommendedStock를 dict로 변환한 것
          필수: code, name, close/entry, total_score
          선택: sl, tp, regime, confidence, sources, sector_hot_days
        """
        code = stock["code"]
        name = stock.get("name", "")
        entry = stock.get("entry") or stock.get("close") or 0
        total_score = stock.get("total_score", 0)
        regime = stock.get("regime", "NORMAL")
        confidence = stock.get("confidence", "MED")
        sources = stock.get("sources", [])
        existing_sl = stock.get("sl", 0)
        existing_tp = stock.get("tp", 0)
        sector_hot_days = stock.get("sector_hot_days", 0)

        if entry <= 0:
            return self._reject_trade(code, name, total_score, sources, regime,
                                      confidence, "진입가 없음")

        trade_id = f"T_{datetime.now().strftime('%Y%m%d')}_{code}"

        # Step 1: 손절가
        stop, stop_source = self.stop_engine.calc(code, entry, regime, existing_sl)

        # Step 2: 보유기간
        hold_days, hold_source, time_stop = self.hold_engine.calc(
            regime, sector_hot_days, sources
        )

        # Step 3: 기대수익률 + 목표가
        expected_ret, target, target_source, ret_detail = self.return_engine.calc(
            code, entry, total_score, existing_tp, hold_days
        )

        # Step 4: R:R 계산
        risk_pct = (entry - stop) / entry * 100 if entry > 0 else 5.0
        reward_pct = (target - entry) / entry * 100 if entry > 0 else 0
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0

        # Step 5: R:R 게이트
        rr_verdict = RRGate.evaluate(rr_ratio, regime, confidence, sources=sources)

        # Step 6: 포지션 사이징
        conviction = self.sizer.calc_conviction(confidence, regime, total_score, sources)
        shares, position_krw, risk_amount = self.sizer.calc(
            self.equity, entry, stop, conviction, self.current_positions
        )

        return TradeObject(
            trade_id=trade_id,
            code=code,
            name=name,
            total_score=total_score,
            sources=sources,
            regime=regime,
            confidence=confidence,
            entry_price=entry,
            stop_loss=stop,
            stop_source=stop_source,
            target_price=target,
            target_source=target_source,
            risk_pct=round(risk_pct, 2),
            reward_pct=round(reward_pct, 2),
            rr_ratio=round(rr_ratio, 2),
            rr_verdict=rr_verdict,
            position_krw=position_krw,
            shares=shares,
            risk_amount=risk_amount,
            conviction=conviction,
            expected_hold_days=hold_days,
            hold_source=hold_source,
            time_stop_days=time_stop,
            expected_return_detail=ret_detail,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        )

    def _reject_trade(self, code, name, score, sources, regime, confidence, reason):
        return TradeObject(
            trade_id=f"T_{datetime.now().strftime('%Y%m%d')}_{code}",
            code=code,
            name=name,
            total_score=score,
            sources=sources,
            regime=regime,
            confidence=confidence,
            rr_verdict="REJECT",
            expected_return_detail={"reject_reason": reason},
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        )


# ═══════════════════════════════════════════════════════
#  파이프라인 함수
# ═══════════════════════════════════════════════════════

def build_trade_objects_from_report(report=None, equity: int = DEFAULT_EQUITY,
                                    current_positions: int = 0) -> List[TradeObject]:
    """
    RecommendationReport → List[TradeObject]
    report가 None이면 recommendation.json에서 로드
    """
    if report is None:
        report = _load_report_from_json()
        if report is None:
            return []

    # report가 객체든 dict든 처리
    stocks = []
    if hasattr(report, "stocks"):
        for s in report.stocks:
            d = _stock_to_dict(s)
            # rotation_detail에서 sector_hot_days 추출
            d["sector_hot_days"] = _get_sector_hot_days(s, report)
            stocks.append(d)
    elif isinstance(report, dict):
        for s in report.get("stocks", []):
            s["sector_hot_days"] = _get_sector_hot_days_from_dict(s, report)
            stocks.append(s)

    builder = TradeBuilder(equity=equity, current_positions=current_positions)
    trades = []
    for s in stocks:
        try:
            to = builder.build(s)
            trades.append(to)
        except Exception as e:
            logger.warning(f"TradeObject 빌드 실패 {s.get('code', '?')}: {e}")

    return trades


def _stock_to_dict(s) -> dict:
    """RecommendedStock → dict"""
    if hasattr(s, "__dict__"):
        return {k: v for k, v in s.__dict__.items()}
    return dict(s)


def _get_sector_hot_days(stock, report) -> int:
    """rotation_detail에서 해당 종목 섹터의 HOT 일수 추출"""
    if not hasattr(report, "rotation_detail"):
        return 0
    sector = getattr(stock, "sector", "") or ""
    for rd in (report.rotation_detail or []):
        if hasattr(rd, "get"):
            rd_sector = rd.get("sector", "")
            hd = rd.get("hot_days", 0)
        else:
            rd_sector = getattr(rd, "sector", "")
            hd = getattr(rd, "hot_days", 0)
        if rd_sector and (rd_sector in sector or sector in rd_sector):
            return hd
    return 0


def _get_sector_hot_days_from_dict(stock: dict, report: dict) -> int:
    for rd in report.get("rotation_detail", []):
        rd_sector = rd.get("sector", "")
        # 소스에서 섹터 추출 시도
        for src in stock.get("sources", []):
            if "sector_hot:" in src:
                sector_name = src.replace("sector_hot:", "")
                if rd_sector and (rd_sector in sector_name or sector_name in rd_sector):
                    return rd.get("hot_days", 0)
    return 0


def _load_report_from_json() -> Optional[dict]:
    """recommendation.json 로드"""
    path = _STORE / "recommendation.json"
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════
#  저장/로드
# ═══════════════════════════════════════════════════════

def save_trade_objects(trades: List[TradeObject]):
    _STORE.mkdir(parents=True, exist_ok=True)
    data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "equity": DEFAULT_EQUITY,
        "count": len(trades),
        "accepted": len([t for t in trades if t.rr_verdict != "REJECT"]),
        "rejected": len([t for t in trades if t.rr_verdict == "REJECT"]),
        "trades": [asdict(t) for t in trades],
    }
    tmp = TRADE_OBJECTS_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(TRADE_OBJECTS_PATH)


def load_trade_objects() -> List[TradeObject]:
    if not TRADE_OBJECTS_PATH.exists():
        return []
    try:
        with open(TRADE_OBJECTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        trades = []
        for d in data.get("trades", []):
            # dataclass 필드만 추출
            valid_fields = {f.name for f in TradeObject.__dataclass_fields__.values()}
            filtered = {k: v for k, v in d.items() if k in valid_fields}
            trades.append(TradeObject(**filtered))
        return trades
    except Exception as e:
        logger.warning(f"trade_objects.json 로드 실패: {e}")
        return []


# ═══════════════════════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════════════════════

def format_trade_objects_telegram(trades: List[TradeObject], paper: bool = True) -> str:
    """Trade Objects 텔레그램 출력"""
    if not trades:
        return ""

    tag = " [PAPER]" if paper else ""
    lines = [f"━━ Trade Objects{tag} ━━━━━━━━"]

    accepted = [t for t in trades if t.rr_verdict != "REJECT"]
    rejected = [t for t in trades if t.rr_verdict == "REJECT"]

    for t in sorted(accepted, key=lambda x: -x.rr_ratio):
        icon = "STRONG" if t.rr_verdict == "STRONG_BUY" else "GOOD"
        if t.rr_verdict == "MARGINAL_PASS":
            icon = "PASS*"

        lines.append(f"[{icon}] {t.name} R:R {t.rr_ratio:.1f}")
        lines.append(
            f"  진입 {t.entry_price:,} → 목표 {t.target_price:,}"
            f"(+{t.reward_pct:.1f}%) | 손절 {t.stop_loss:,}(-{abs(t.risk_pct):.1f}%)"
        )

        if t.position_krw > 0:
            lines.append(
                f"  비중 {t.position_krw/DEFAULT_EQUITY*100:.1f}% "
                f"| 최대손실 -{t.risk_amount:,}원 "
                f"| 보유 {t.expected_hold_days}~{t.time_stop_days}일"
            )

        lines.append(f"  무효화: {t.invalidation_narrative()}")
        lines.append("")

    if rejected:
        lines.append("─── REJECT ───")
        for t in rejected:
            if t.entry_price > 0 and t.risk_pct > 0:
                lines.append(
                    f"  {t.name} R:R {t.rr_ratio:.1f} "
                    f"(기대 +{t.reward_pct:.1f}% vs 손절 -{abs(t.risk_pct):.1f}%)"
                )
            else:
                reason = t.expected_return_detail.get("reject_reason", "R:R 부족")
                lines.append(f"  {t.name} — {reason}")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════════

def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" not in sys.argv:
        print("Usage: python data/trade_object.py --test")
        return

    print("=" * 60)
    print("Trade Object Layer — 테스트")
    print("=" * 60)

    # recommendation.json 로드
    report = _load_report_from_json()
    if not report:
        print("recommendation.json 없음!")
        return

    stocks = report.get("stocks", [])
    print(f"\n추천 종목: {len(stocks)}개")
    print(f"가정 총자산: {DEFAULT_EQUITY:,}원")
    print(f"현금 유보: {CASH_RESERVE*100:.0f}% ({int(DEFAULT_EQUITY*CASH_RESERVE):,}원)")
    print(f"투자 가용: {int(DEFAULT_EQUITY*(1-CASH_RESERVE)):,}원")
    print()

    # Trade Objects 생성
    trades = build_trade_objects_from_report(report)

    accepted = [t for t in trades if t.rr_verdict != "REJECT"]
    rejected = [t for t in trades if t.rr_verdict == "REJECT"]

    print(f"결과: {len(accepted)}건 ACCEPT / {len(rejected)}건 REJECT")
    print()

    for t in sorted(trades, key=lambda x: -x.rr_ratio):
        verdict_icon = {"STRONG_BUY": "★", "GOOD": "●", "MARGINAL_PASS": "◐",
                        "REJECT": "✗"}.get(t.rr_verdict, "?")

        print(f"  {verdict_icon} {t.name} ({t.code})")
        print(f"    점수: {t.total_score:.1f} | 확신: {t.confidence} | 레짐: {t.regime}")
        print(f"    진입: {t.entry_price:,} → 목표: {t.target_price:,}(+{t.reward_pct:.1f}%) "
              f"| 손절: {t.stop_loss:,}(-{abs(t.risk_pct):.1f}%)")
        print(f"    R:R: {t.rr_ratio:.2f} → {t.rr_verdict}")
        print(f"    SL근거: {t.stop_source} | TP근거: {t.target_source}")

        if t.rr_verdict != "REJECT":
            print(f"    포지션: {t.position_krw:,}원 ({t.shares}주) "
                  f"| 리스크: {t.risk_amount:,}원")
            print(f"    보유: {t.expected_hold_days}~{t.time_stop_days}일 ({t.hold_source})")
            print(f"    무효화: {t.invalidation_narrative()}")

        detail = t.expected_return_detail
        if detail:
            bucket = detail.get("score_bucket", {})
            cons = detail.get("consensus", {})
            tp_info = detail.get("existing_tp", {})
            blend = detail.get("blended", 0)
            parts = [f"점수대:{bucket.get('return_pct', 0):+.1f}%"]
            if cons:
                parts.append(f"컨센서스:{cons.get('return_pct', 0):+.1f}%")
            parts.append(f"기존TP:{tp_info.get('return_pct', 0):+.1f}%")
            parts.append(f"→ 혼합:{blend:+.1f}%")
            print(f"    기대수익: {' | '.join(parts)}")

        print()

    # 텔레그램 포맷 미리보기
    print("─── 텔레그램 미리보기 ───")
    print(format_trade_objects_telegram(trades))

    # 저장
    save_trade_objects(trades)
    print(f"\n저장: {TRADE_OBJECTS_PATH}")

    # 현금 25% 검증
    total_invested = sum(t.position_krw for t in accepted)
    total_risk = sum(t.risk_amount for t in accepted)
    print(f"\n투자 총액: {total_invested:,}원 ({total_invested/DEFAULT_EQUITY*100:.1f}%)")
    print(f"리스크 총액: {total_risk:,}원 ({total_risk/DEFAULT_EQUITY*100:.1f}%)")
    print(f"현금 잔여: {DEFAULT_EQUITY - total_invested:,}원 "
          f"({(DEFAULT_EQUITY - total_invested)/DEFAULT_EQUITY*100:.1f}%)")

    cash_pct = (DEFAULT_EQUITY - total_invested) / DEFAULT_EQUITY
    if cash_pct >= CASH_RESERVE:
        print(f"  현금 25% 철칙: OK (잔여 {cash_pct*100:.1f}%)")
    else:
        print(f"  현금 25% 철칙: FAIL! (잔여 {cash_pct*100:.1f}% < 25%)")


if __name__ == "__main__":
    main()
