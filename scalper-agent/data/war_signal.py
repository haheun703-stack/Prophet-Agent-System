# -*- coding: utf-8 -*-
"""
War Signal — 전쟁/휴전 시그널 감지 모듈
==========================================
이란 전쟁(2026.2.28~) 해소 시그널을 4축으로 감지하여
빅테크 저점매수 타이밍을 판단.

4축 감지:
  1. 유가 급락 — 브렌트 $95 이하 (-15%+ from peak)
  2. VIX 급락 — 25 이하
  3. 방산주 하락 — LMT/RTX/NOC 3일 연속 하락
  4. 해운 정상화 — BDI(Baltic Dry Index) 20% 이상 하락

시그널 레벨:
  CEASEFIRE_WATCH     — 1축 충족 (관심)
  CEASEFIRE_LIKELY    — 2축 충족 (준비)
  CEASEFIRE_CONFIRMED — 3축+ 충족 (진입 시작)
  WAR_ONGOING         — 0축 충족 (전쟁 지속)

스케줄: G7 C3 시점 1일 1회 (macro_baseline과 동시)
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("BH.WarSignal")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
CACHE_PATH = DATA_DIR / "war_signal.json"

# ═══════════════════════════════════════════
#  기준값
# ═══════════════════════════════════════════

# 이란 전쟁 시작 이후 피크 기준
WAR_START_DATE = "2026-02-28"
OIL_PEAK = 112.0          # 브렌트 피크 (근사치)
OIL_CEASEFIRE = 95.0       # 이 이하면 봉쇄 해소 시그널
VIX_CEASEFIRE = 25.0       # 이 이하면 공포 해소
DEFENSE_DOWN_DAYS = 3      # 방산주 연속 하락일
SHIPPING_DROP_PCT = -20.0  # BDI 피크 대비 % 하락

# 추적 티커
TICKERS = {
    "brent":   "BZ=F",       # 브렌트유 선물
    "vix":     "^VIX",       # VIX
    "lmt":     "LMT",        # Lockheed Martin
    "rtx":     "RTX",        # RTX (Raytheon)
    "noc":     "NOC",        # Northrop Grumman
    "bdry":    "BDRY",       # Breakwave Dry Bulk Shipping ETF (BDI 프록시)
    "bwet":    "BWET",       # Breakwave Tanker Shipping ETF (탱커 프록시)
}

# 탱커 vs 벌크 다이버전스 기준
TANKER_DROP_PCT = -15.0     # BWET 피크 대비 15% 하락 → 호르무즈 해소
BULK_RISE_PCT = 10.0         # BDRY 저점 대비 10% 상승 → 무역 정상화

# 빅테크 ETF 후보 (CEASEFIRE 시 NXT 추가)
BIGTECH_CANDIDATES = [
    {"code": "FNGU", "name": "MicroSectors FANG+ 3X", "type": "leveraged",
     "note": "FANG+ 3배 레버리지, 반등 시 최대 수익, 현금 38%"},
    {"code": "SOXX", "name": "iShares Semiconductor ETF", "type": "sector",
     "note": "반도체 ETF, 52주 고점 -23%, 바닥 근처"},
    {"code": "QQQ", "name": "Invesco QQQ Trust", "type": "index",
     "note": "나스닥100, 가장 안전한 빅테크 노출"},
]

# ═══════════════════════════════════════════
#  한국 해운 관련주 워치리스트
# ═══════════════════════════════════════════
KOREA_SHIPPING_WATCHLIST = {
    "tanker": [
        {"code": "005880", "name": "대한해운", "note": "벌크+탱커 혼합, 유조선 보유"},
        {"code": "028670", "name": "팬오션", "note": "벌크 1위 + 탱커 일부, BDI 민감"},
        {"code": "044450", "name": "KSS해운", "note": "LPG/케미컬탱커 전문"},
        {"code": "004020", "name": "현대미포조선", "note": "중형 탱커 건조, 수주잔고 호조"},
    ],
    "bulk": [
        {"code": "028670", "name": "팬오션", "note": "벌크 1위, BDI 상관계수 0.8+"},
        {"code": "005880", "name": "대한해운", "note": "벌크 운송 비중 高"},
        {"code": "011200", "name": "HMM", "note": "컨테이너+벌크, 해운 대장주"},
    ],
    "shipbuilding": [
        {"code": "009540", "name": "HD한국조선해양", "note": "세계 1위, LNG+탱커 수주"},
        {"code": "042660", "name": "한화오션", "note": "해군 함정+상선, 방산 연동"},
        {"code": "010140", "name": "삼성중공업", "note": "LNG선 + 해양플랜트"},
    ],
    "lng_carrier": [
        {"code": "017960", "name": "한국카본", "note": "LNG 보냉재 독점, LNG선 수주 연동"},
    ],
}


# ═══════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════

@dataclass
class WarSignalResult:
    """전쟁 시그널 분석 결과"""
    date: str = ""
    timestamp: str = ""

    # 축별 상태
    oil_signal: bool = False       # 유가 $95 이하?
    oil_current: float = 0.0
    oil_detail: str = ""

    vix_signal: bool = False       # VIX 25 이하?
    vix_current: float = 0.0
    vix_detail: str = ""

    defense_signal: bool = False   # 방산주 3일 연속 하락?
    defense_detail: str = ""
    defense_data: Dict = field(default_factory=dict)

    shipping_signal: bool = False  # 탱커↓ + 벌크↑ 다이버전스?
    shipping_current: float = 0.0
    shipping_detail: str = ""

    # 탱커 vs 벌크 상세 추적
    tanker_current: float = 0.0    # BWET 현재가
    tanker_peak: float = 0.0       # BWET 60일 피크
    tanker_chg_pct: float = 0.0    # BWET 피크 대비 변동률
    bulk_current: float = 0.0      # BDRY 현재가
    bulk_trough: float = 0.0       # BDRY 60일 저점
    bulk_chg_pct: float = 0.0      # BDRY 저점 대비 변동률
    tanker_bulk_divergence: str = ""  # 다이버전스 해석

    # 종합 판정
    signals_met: int = 0
    level: str = "WAR_ONGOING"     # WAR_ONGOING / CEASEFIRE_WATCH / LIKELY / CONFIRMED
    level_label: str = "전쟁 지속"
    narrative: str = ""

    # 빅테크 진입 가이드
    bigtech_action: str = "관망"   # 관망 / 준비 / 1차진입 / 본격매수
    bigtech_candidates: List[Dict] = field(default_factory=list)


# ═══════════════════════════════════════════
#  데이터 수집
# ═══════════════════════════════════════════

def _fetch_ticker_history(ticker: str, days: int = 30) -> Optional[list]:
    """yfinance로 종가 히스토리 수집. 실패 시 None."""
    try:
        import yfinance as yf
        end = datetime.now()
        start = end - timedelta(days=days + 10)
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"),
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None

        # yfinance MultiIndex 처리
        if hasattr(df.columns, 'levels') and len(df.columns.levels) > 1:
            close_col = [c for c in df.columns if 'Close' in str(c)]
            if close_col:
                closes = df[close_col[0]].dropna().tolist()
            else:
                return None
        else:
            if "Close" in df.columns:
                closes = df["Close"].dropna().tolist()
            else:
                return None

        return closes[-days:] if len(closes) > days else closes
    except Exception as e:
        logger.warning(f"[WAR] {ticker} 수집 실패: {e}")
        return None


def _check_oil(result: WarSignalResult) -> None:
    """축1: 브렌트유 급락 감지."""
    closes = _fetch_ticker_history(TICKERS["brent"], 30)
    if not closes:
        result.oil_detail = "브렌트 데이터 수집 실패"
        return

    current = closes[-1]
    peak_30d = max(closes)
    result.oil_current = round(current, 2)

    if current <= OIL_CEASEFIRE:
        result.oil_signal = True
        result.oil_detail = f"브렌트 ${current:.1f} ≤ ${OIL_CEASEFIRE} (호르무즈 해소 시그널)"
    else:
        drop_from_peak = ((current - peak_30d) / peak_30d) * 100
        result.oil_detail = f"브렌트 ${current:.1f} (30일 피크 ${peak_30d:.1f}, {drop_from_peak:+.1f}%)"


def _check_vix(result: WarSignalResult) -> None:
    """축2: VIX 급락 감지."""
    closes = _fetch_ticker_history(TICKERS["vix"], 10)
    if not closes:
        result.vix_detail = "VIX 데이터 수집 실패"
        return

    current = closes[-1]
    result.vix_current = round(current, 2)

    if current <= VIX_CEASEFIRE:
        result.vix_signal = True
        result.vix_detail = f"VIX {current:.1f} ≤ {VIX_CEASEFIRE} (공포 해소)"
    else:
        result.vix_detail = f"VIX {current:.1f} (해소 기준: {VIX_CEASEFIRE})"


def _check_defense(result: WarSignalResult) -> None:
    """축3: 방산주 3일 연속 하락 감지."""
    defense_tickers = ["lmt", "rtx", "noc"]
    all_down = True
    details = {}

    for key in defense_tickers:
        ticker = TICKERS[key]
        closes = _fetch_ticker_history(ticker, 5)
        if not closes or len(closes) < DEFENSE_DOWN_DAYS + 1:
            all_down = False
            details[key] = "데이터 부족"
            continue

        # 최근 N일 연속 하락 체크
        consecutive_down = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] < closes[i - 1]:
                consecutive_down += 1
            else:
                break

        daily_changes = []
        for i in range(max(0, len(closes) - DEFENSE_DOWN_DAYS), len(closes)):
            if i > 0:
                chg = ((closes[i] - closes[i-1]) / closes[i-1]) * 100
                daily_changes.append(round(chg, 2))

        details[key] = {
            "current": round(closes[-1], 2),
            "consecutive_down": consecutive_down,
            "recent_changes": daily_changes,
        }

        if consecutive_down < DEFENSE_DOWN_DAYS:
            all_down = False

    result.defense_data = details

    if all_down:
        result.defense_signal = True
        result.defense_detail = f"방산 3종 모두 {DEFENSE_DOWN_DAYS}일+ 연속 하락 (전쟁 해소 시그널)"
    else:
        down_count = sum(1 for v in details.values()
                         if isinstance(v, dict) and v.get("consecutive_down", 0) >= DEFENSE_DOWN_DAYS)
        result.defense_detail = f"방산 3종 중 {down_count}종목 {DEFENSE_DOWN_DAYS}일 연속 하락"


def _check_shipping(result: WarSignalResult) -> None:
    """축4: 해운 다이버전스 감지 (탱커 BWET ↓ + 벌크 BDRY ↑ = 호르무즈 해소).

    전쟁 중: 탱커(BWET) ↑↑↑ (호르무즈 프리미엄) + 벌크(BDRY) ↓ (무역 위축)
    휴전 시: 탱커(BWET) ↓↓↓ (프리미엄 소멸) + 벌크(BDRY) ↑ (무역 정상화)
    → 이 다이버전스 반전이 가장 강력한 휴전 시그널
    """
    # ── 탱커 (BWET) 수집 ──
    tanker_closes = _fetch_ticker_history(TICKERS["bwet"], 60)
    tanker_ok = False
    if tanker_closes and len(tanker_closes) >= 10:
        tanker_ok = True
        result.tanker_current = round(tanker_closes[-1], 2)
        result.tanker_peak = round(max(tanker_closes), 2)
        result.tanker_chg_pct = round(
            ((tanker_closes[-1] - result.tanker_peak) / result.tanker_peak) * 100, 1
        )

    # ── 벌크 (BDRY) 수집 ──
    bulk_closes = _fetch_ticker_history(TICKERS["bdry"], 60)
    bulk_ok = False
    if bulk_closes and len(bulk_closes) >= 10:
        bulk_ok = True
        result.bulk_current = round(bulk_closes[-1], 2)
        result.bulk_trough = round(min(bulk_closes), 2)
        result.bulk_chg_pct = round(
            ((bulk_closes[-1] - result.bulk_trough) / result.bulk_trough) * 100, 1
        ) if result.bulk_trough > 0 else 0.0

    # ── 다이버전스 판정 ──
    if not tanker_ok and not bulk_ok:
        result.shipping_detail = "BWET/BDRY 모두 수집 실패"
        return

    # 기존 호환: shipping_current = BDRY 현재가
    result.shipping_current = result.bulk_current

    # 탱커 하락 시그널 (피크 대비 -15%+)
    tanker_dropping = tanker_ok and result.tanker_chg_pct <= TANKER_DROP_PCT
    # 벌크 상승 시그널 (저점 대비 +10%+)
    bulk_rising = bulk_ok and result.bulk_chg_pct >= BULK_RISE_PCT

    # 다이버전스 해석
    if tanker_dropping and bulk_rising:
        result.shipping_signal = True
        result.tanker_bulk_divergence = "CEASEFIRE_REVERSAL"
        result.shipping_detail = (
            f"해운 다이버전스 반전! "
            f"BWET ${result.tanker_current} ({result.tanker_chg_pct:+.1f}% 피크), "
            f"BDRY ${result.bulk_current} ({result.bulk_chg_pct:+.1f}% 저점) "
            f"→ 호르무즈 해소 시그널"
        )
    elif tanker_dropping:
        result.shipping_signal = True  # 탱커 급락만으로도 시그널
        result.tanker_bulk_divergence = "TANKER_UNWINDING"
        result.shipping_detail = (
            f"탱커 프리미엄 해소 중! "
            f"BWET ${result.tanker_current} ({result.tanker_chg_pct:+.1f}% 피크), "
            f"BDRY ${result.bulk_current} ({result.bulk_chg_pct:+.1f}% 저점)"
        )
    elif bulk_rising:
        result.tanker_bulk_divergence = "BULK_RECOVERING"
        result.shipping_detail = (
            f"벌크 회복 중 (탱커는 유지): "
            f"BWET ${result.tanker_current} ({result.tanker_chg_pct:+.1f}% 피크), "
            f"BDRY ${result.bulk_current} ({result.bulk_chg_pct:+.1f}% 저점)"
        )
    else:
        result.tanker_bulk_divergence = "WAR_PREMIUM_ACTIVE"
        result.shipping_detail = (
            f"전쟁 프리미엄 유지: "
            f"BWET ${result.tanker_current} ({result.tanker_chg_pct:+.1f}% 피크), "
            f"BDRY ${result.bulk_current} ({result.bulk_chg_pct:+.1f}% 저점)"
        )


# ═══════════════════════════════════════════
#  종합 판정
# ═══════════════════════════════════════════

def _evaluate(result: WarSignalResult) -> None:
    """4축 결과를 종합하여 시그널 레벨 판정."""
    axes = [result.oil_signal, result.vix_signal,
            result.defense_signal, result.shipping_signal]
    result.signals_met = sum(axes)

    if result.signals_met >= 3:
        result.level = "CEASEFIRE_CONFIRMED"
        result.level_label = "휴전 확인"
        result.bigtech_action = "1차진입"
        result.bigtech_candidates = BIGTECH_CANDIDATES
        result.narrative = (
            f"4축 중 {result.signals_met}축 충족 → 전쟁 해소 확인. "
            f"빅테크 1차 진입 가능 (가용자금 20%)"
        )
    elif result.signals_met >= 2:
        result.level = "CEASEFIRE_LIKELY"
        result.level_label = "휴전 가능성"
        result.bigtech_action = "준비"
        result.bigtech_candidates = [c for c in BIGTECH_CANDIDATES if c["type"] != "leveraged"]
        result.narrative = (
            f"4축 중 {result.signals_met}축 충족 → 해소 가능성. "
            f"빅테크 관찰 리스트 준비 (레버리지 제외)"
        )
    elif result.signals_met >= 1:
        result.level = "CEASEFIRE_WATCH"
        result.level_label = "해소 조짐"
        result.bigtech_action = "관심"
        result.bigtech_candidates = []
        result.narrative = (
            f"4축 중 {result.signals_met}축 충족 → 해소 조짐. "
            f"아직 진입 불가, 추가 시그널 대기"
        )
    else:
        result.level = "WAR_ONGOING"
        result.level_label = "전쟁 지속"
        result.bigtech_action = "관망"
        result.bigtech_candidates = []
        result.narrative = (
            f"4축 모두 미충족 → 전쟁 지속. "
            f"금+인버스 유지, 빅테크 진입 불가"
        )


# ═══════════════════════════════════════════
#  메인 함수
# ═══════════════════════════════════════════

def analyze_war_signal(force: bool = False) -> WarSignalResult:
    """전쟁/휴전 시그널 4축 분석. 1일 1회 캐시."""
    # 캐시 체크 (6시간 이내)
    if not force:
        cached = load_cached_war_signal()
        if cached and cached.date == str(date.today()):
            logger.info(f"[WAR] 캐시 사용: {cached.level} ({cached.signals_met}/4)")
            return cached

    logger.info("[WAR] 전쟁/휴전 시그널 4축 분석 시작...")
    result = WarSignalResult(
        date=str(date.today()),
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    # 4축 수집
    _check_oil(result)
    _check_vix(result)
    _check_defense(result)
    _check_shipping(result)

    # 종합 판정
    _evaluate(result)

    # 캐시 저장
    _save_cache(result)

    logger.info(
        f"[WAR] 판정: {result.level_label} ({result.signals_met}/4) | "
        f"유가=${result.oil_current} VIX={result.vix_current} | "
        f"빅테크: {result.bigtech_action}"
    )

    return result


# ═══════════════════════════════════════════
#  캐시 관리
# ═══════════════════════════════════════════

def _save_cache(result: WarSignalResult) -> None:
    """결과 캐시 저장 (atomic write)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(result), f, ensure_ascii=False, indent=2)
        tmp.replace(CACHE_PATH)
    except Exception as e:
        logger.warning(f"[WAR] 캐시 저장 실패: {e}")


def load_cached_war_signal() -> Optional[WarSignalResult]:
    """캐시에서 로드."""
    if not CACHE_PATH.exists():
        return None
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = WarSignalResult(**{
            k: v for k, v in data.items()
            if k in WarSignalResult.__dataclass_fields__
        })
        return result
    except Exception:
        return None


# ═══════════════════════════════════════════
#  BRAIN 연동용 요약
# ═══════════════════════════════════════════

def get_war_signal_for_brain() -> Dict:
    """BRAIN에 전달할 전쟁 시그널 요약."""
    result = load_cached_war_signal()
    if not result:
        return {"level": "UNKNOWN", "signals_met": 0, "narrative": "데이터 없음"}

    return {
        "level": result.level,
        "level_label": result.level_label,
        "signals_met": result.signals_met,
        "oil": result.oil_current,
        "vix": result.vix_current,
        "bigtech_action": result.bigtech_action,
        "narrative": result.narrative,
        "axes": {
            "oil": result.oil_signal,
            "vix": result.vix_signal,
            "defense": result.defense_signal,
            "shipping": result.shipping_signal,
        },
        "shipping_detail": {
            "tanker_bwet": result.tanker_current,
            "tanker_peak_chg": result.tanker_chg_pct,
            "bulk_bdry": result.bulk_current,
            "bulk_trough_chg": result.bulk_chg_pct,
            "divergence": result.tanker_bulk_divergence,
        },
    }


def get_korea_shipping_watchlist() -> Dict:
    """한국 해운 관련주 워치리스트 반환.

    전쟁 중 → tanker/shipbuilding 유리
    휴전 시 → bulk 반등 + tanker 하락
    """
    result = load_cached_war_signal()
    action_map = {
        "WAR_ONGOING":         {"favor": "tanker", "avoid": "bulk", "note": "전쟁 지속 → 탱커주 유리, 벌크 약세"},
        "CEASEFIRE_WATCH":     {"favor": "tanker", "avoid": None, "note": "해소 조짐 → 탱커 유지, 벌크 관심"},
        "CEASEFIRE_LIKELY":    {"favor": "bulk", "avoid": "tanker", "note": "휴전 가능 → 벌크 매수, 탱커 익절 준비"},
        "CEASEFIRE_CONFIRMED": {"favor": "bulk", "avoid": "tanker", "note": "휴전 확인 → 벌크 적극 매수, 탱커 매도"},
    }
    level = result.level if result else "WAR_ONGOING"
    action = action_map.get(level, action_map["WAR_ONGOING"])

    return {
        "level": level,
        "action": action,
        "watchlist": KOREA_SHIPPING_WATCHLIST,
    }


def get_war_brain_score_adj() -> float:
    """BRAIN 스코어 보정값. CEASEFIRE_CONFIRMED → +20."""
    result = load_cached_war_signal()
    if not result:
        return 0.0

    adjustments = {
        "CEASEFIRE_CONFIRMED": 20.0,
        "CEASEFIRE_LIKELY": 10.0,
        "CEASEFIRE_WATCH": 5.0,
        "WAR_ONGOING": 0.0,
    }
    return adjustments.get(result.level, 0.0)


# ═══════════════════════════════════════════
#  텔레그램 알림 포맷
# ═══════════════════════════════════════════

def format_war_signal_alert(result: WarSignalResult) -> Optional[str]:
    """CEASEFIRE_LIKELY 이상일 때 텔레그램 알림 메시지 생성."""
    if result.level in ("WAR_ONGOING", "CEASEFIRE_WATCH"):
        return None  # 알림 불필요

    emoji = {
        "CEASEFIRE_LIKELY": "🟡",
        "CEASEFIRE_CONFIRMED": "🟢",
    }.get(result.level, "⚪")

    lines = [
        f"{emoji} 전쟁 시그널: {result.level_label} ({result.signals_met}/4축)",
        "",
        "━━━━━━━━━━━━━━━━━━━",
        f"{'✅' if result.oil_signal else '❌'} 유가: {result.oil_detail}",
        f"{'✅' if result.vix_signal else '❌'} VIX: {result.vix_detail}",
        f"{'✅' if result.defense_signal else '❌'} 방산: {result.defense_detail}",
        f"{'✅' if result.shipping_signal else '❌'} 해운: {result.shipping_detail}",
        "━━━━━━━━━━━━━━━━━━━",
        "",
        f"빅테크 액션: {result.bigtech_action}",
    ]

    # 탱커/벌크 다이버전스 상세
    if result.tanker_current or result.bulk_current:
        lines.append("")
        lines.append("📦 탱커 vs 벌크:")
        if result.tanker_current:
            lines.append(f"  BWET(탱커) ${result.tanker_current} ({result.tanker_chg_pct:+.1f}% 피크)")
        if result.bulk_current:
            lines.append(f"  BDRY(벌크) ${result.bulk_current} ({result.bulk_chg_pct:+.1f}% 저점)")
        if result.tanker_bulk_divergence:
            div_labels = {
                "WAR_PREMIUM_ACTIVE": "전쟁 프리미엄 유지",
                "BULK_RECOVERING": "벌크 회복 중",
                "TANKER_UNWINDING": "탱커 프리미엄 해소",
                "CEASEFIRE_REVERSAL": "다이버전스 반전! (강한 해소 시그널)",
            }
            lines.append(f"  → {div_labels.get(result.tanker_bulk_divergence, result.tanker_bulk_divergence)}")

    if result.bigtech_candidates:
        lines.append("")
        lines.append("빅테크 후보:")
        for c in result.bigtech_candidates:
            lines.append(f"  • {c['code']} — {c['note']}")

    lines.append("")
    lines.append(result.narrative)

    return "\n".join(lines)
