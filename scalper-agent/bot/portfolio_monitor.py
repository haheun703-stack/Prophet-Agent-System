# -*- coding: utf-8 -*-
"""
보유종목 포트폴리오 모니터 (Portfolio Monitor)
==============================================
2-Layer 시스템:
  Layer 1 - 대시보드: 1줄 1종목 요약, 정기 발송 (장중 3회)
  Layer 2 - 알림: ICT/수급 이벤트 발생 시에만 자동 푸시
"""

import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("BH.PortfolioMonitor")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
PORTFOLIO_PATH = DATA_DIR / "portfolio.json"

# ── 신호등 색상 ──
_CLR = {
    "green": "\U0001f7e2",   # 🟢
    "yellow": "\U0001f7e1",  # 🟡
    "orange": "\U0001f7e0",  # 🟠
    "red": "\U0001f534",     # 🔴
}


# ══════════════════════════════════════════
#  데이터 로딩
# ══════════════════════════════════════════

def load_portfolio() -> list[dict]:
    """portfolio.json 로드"""
    if not PORTFOLIO_PATH.exists():
        logger.warning("portfolio.json not found")
        return []
    with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("positions", [])


def load_recommended() -> dict[str, float]:
    """recommendation.json → {code: total_score}"""
    rec_path = DATA_DIR / "recommendation.json"
    if not rec_path.exists():
        return {}
    with open(rec_path, "r", encoding="utf-8") as f:
        rec = json.load(f)
    result = {}
    for s in rec.get("stocks", []):
        result[s["code"]] = s["total_score"]
    return result


def _get_avg_volume(code: str, n_days: int = 20) -> float:
    """최근 N일 평균 거래량 (daily OHLCV)"""
    fpath = DAILY_DIR / f"{code}.csv"
    if not fpath.exists():
        return 0
    try:
        df = pd.read_csv(fpath)
        col_map = {"날짜": "date", "거래량": "volume"}
        if "날짜" in df.columns:
            df = df.rename(columns=col_map)
        if "volume" not in df.columns:
            return 0
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        avg = df.tail(n_days)["volume"].mean()
        return avg if avg > 0 else 0
    except Exception:
        return 0


def _get_vwap(code: str, n_days: int = 20) -> float:
    """최근 N일 VWAP (Volume Weighted Average Price)
    VWAP = Σ(TP × Vol) / Σ(Vol), TP = (고가+저가+종가)/3
    """
    fpath = DAILY_DIR / f"{code}.csv"
    if not fpath.exists():
        return 0
    try:
        df = pd.read_csv(fpath)
        col_map = {"고가": "high", "저가": "low", "종가": "close", "거래량": "volume"}
        if "고가" in df.columns:
            df = df.rename(columns=col_map)
        for c in ("high", "low", "close", "volume"):
            if c not in df.columns:
                return 0
            df[c] = pd.to_numeric(df[c], errors="coerce")
        tail = df.tail(n_days).dropna(subset=["high", "low", "close", "volume"])
        if tail.empty:
            return 0
        tp = (tail["high"] + tail["low"] + tail["close"]) / 3
        total_vol = tail["volume"].sum()
        if total_vol <= 0:
            return 0
        return float((tp * tail["volume"]).sum() / total_vol)
    except Exception:
        return 0


def _market_time_fraction() -> float:
    """현재 시각 기준 장 진행률 (0.0~1.0)
    장외 시간에는 1.0 반환 (누적거래량 = 전일 전체이므로 보정 불필요)
    """
    now = datetime.now()
    now_min = now.hour * 60 + now.minute
    open_min = 9 * 60       # 09:00
    close_min = 15 * 60 + 30  # 15:30

    if now_min <= open_min or now_min >= close_min:
        return 1.0  # 장외 → 보정 없음 (전일 전체 거래량)

    return max((now_min - open_min) / (close_min - open_min), 0.05)


def _pnl_color(pnl: float) -> str:
    if pnl >= 0:
        return _CLR["green"]
    elif pnl >= -5:
        return _CLR["yellow"]
    elif pnl >= -10:
        return _CLR["orange"]
    return _CLR["red"]


# ══════════════════════════════════════════
#  Layer 1 - 대시보드
# ══════════════════════════════════════════

def generate_dashboard(kis_trader, portfolio: list[dict] = None) -> str:
    """1줄 1종목 요약 대시보드 생성

    Args:
        kis_trader: KISTrader 인스턴스 (현재가 조회)
        portfolio: 포트폴리오 데이터 (None이면 자동 로드)

    Returns:
        텔레그램용 대시보드 문자열
    """
    if portfolio is None:
        portfolio = load_portfolio()
    if not portfolio:
        return "\U0001f4cb 포트폴리오 비어있음"

    recommended = load_recommended()
    time_frac = _market_time_fraction()
    now_str = datetime.now().strftime("%H:%M")

    lines = [
        f"\U0001f4cb 보유종목 상태판 ({now_str})",
        "\u2501" * 24,
    ]

    total_eval = 0
    total_cost = 0
    win_count = 0

    for pos in portfolio:
        code = pos["code"]
        name = pos["name"]
        qty = pos["qty"]
        avg_price = pos["avg_price"]

        # ── 현재가 조회 ──
        current = avg_price
        current_vol = 0
        try:
            pd_ = kis_trader.fetch_price(code)
            if pd_.get("success"):
                current = pd_["current_price"]
                current_vol = pd_.get("volume", 0)
        except Exception as e:
            logger.warning(f"가격 조회 실패 {code}: {e}")

        # ── P&L ──
        pnl = (current - avg_price) / avg_price * 100 if avg_price > 0 else 0
        if pnl >= 0:
            win_count += 1
        total_eval += qty * current
        total_cost += qty * avg_price

        # ── Vol ratio (시간 보정) ──
        avg_vol = _get_avg_volume(code)
        vol_str = ""
        if avg_vol > 0 and current_vol > 0:
            projected = current_vol / time_frac
            vr = projected / avg_vol
            arrow = "\u25b2" if vr >= 1.5 else ("\u2500" if vr >= 0.8 else "\u25bc")
            vol_str = f"Vol{arrow}{vr:.1f}x"

        # ── VWAP (20일) ──
        vwap_val = _get_vwap(code)
        vwap_str = ""
        if vwap_val > 0 and current > 0:
            vwap_diff = (current - vwap_val) / vwap_val * 100
            if vwap_diff >= 0:
                vwap_str = f"V\u25b2{vwap_diff:.1f}%"
            else:
                vwap_str = f"V\u25bc{vwap_diff:.1f}%"

        # ── 색상 + 태그 ──
        color = _pnl_color(pnl)
        tags = []
        if code in recommended:
            tags.append(f"\u2b50{recommended[code]:.0f}")
        if pnl >= 5:
            tags.append("익절?")
        elif pnl <= -10:
            tags.append("손절?")

        tag_str = "  " + " ".join(tags) if tags else ""

        # ── 1줄 포맷 ──
        # 한글 이름 정렬 (한글=2칸, 영문=1칸)
        name_w = sum(2 if ord(c) > 0x7F else 1 for c in name)
        pad = " " * max(0, 10 - name_w)
        # indicators 결합 (빈 문자열 제외)
        indicators = " ".join(x for x in [vol_str, vwap_str] if x)
        line = f"{color} {name}{pad}{pnl:>+5.1f}%  {indicators}{tag_str}"
        lines.append(line)

    # ── 총 평가 ──
    total_pnl = (total_eval - total_cost) / total_cost * 100 if total_cost > 0 else 0
    lose_count = len(portfolio) - win_count
    lines.append("\u2501" * 24)
    lines.append(
        f"\U0001f4b0 {total_eval / 10000:,.0f}만 ({total_pnl:+.1f}%)"
        f" | \u2705{win_count} \u274c{lose_count}"
    )

    return "\n".join(lines)


# ══════════════════════════════════════════
#  Layer 2 - 이상 감지 알림
# ══════════════════════════════════════════

def check_alerts(
    kis_trader,
    portfolio: list[dict] = None,
    prev_states: dict = None,
) -> tuple[list[str], dict]:
    """ICT/수급 이벤트 감지 → 알림 목록 반환

    쿨다운 10분: 같은 종목+같은 유형 알림은 10분간 억제
    오프닝 5분(09:00~09:05): 급변 알림 억제 (변동성 과다)

    Args:
        kis_trader: KISTrader 인스턴스
        portfolio: 포트폴리오 데이터
        prev_states: 이전 상태 (delta 감지용)

    Returns:
        (alerts, new_states) - alerts는 알림 문자열 리스트
    """
    if portfolio is None:
        portfolio = load_portfolio()
    if prev_states is None:
        prev_states = {}

    from strategies.equal_level_detector import get_eq_score_adjustment
    from strategies.gap_support import get_gap_score_adjustment

    alerts = []
    new_states = {}
    time_frac = _market_time_fraction()
    today = date.today()
    now = datetime.now()
    now_min = now.hour * 60 + now.minute
    is_opening = 540 <= now_min <= 545  # 09:00~09:05 오프닝 변동성 구간

    # 쿨다운 맵: prev_states에 "_cooldowns" 키로 저장
    cooldowns = prev_states.get("_cooldowns", {})
    COOLDOWN_SEC = 600  # 10분

    def _can_alert(code: str, alert_type: str) -> bool:
        """쿨다운 체크 — 같은 종목+유형은 10분간 억제"""
        key = f"{code}:{alert_type}"
        last = cooldowns.get(key, 0)
        elapsed = (now - datetime.fromtimestamp(last)).total_seconds() if last > 0 else 9999
        if elapsed < COOLDOWN_SEC:
            return False
        cooldowns[key] = now.timestamp()
        return True

    for pos in portfolio:
        code = pos["code"]
        name = pos["name"]
        avg_price = pos["avg_price"]

        # 현재가
        try:
            pd_ = kis_trader.fetch_price(code)
            if not pd_.get("success"):
                continue
            current = pd_["current_price"]
            current_vol = pd_.get("volume", 0)
        except Exception:
            continue

        pnl = (current - avg_price) / avg_price * 100 if avg_price > 0 else 0
        prev = prev_states.get(code, {})

        state = {"price": current, "pnl": pnl, "vol": current_vol}

        # ── 1. Vol 폭발 (쿨다운 적용) ──
        avg_vol = _get_avg_volume(code)
        if avg_vol > 0 and current_vol > 0:
            projected = current_vol / time_frac
            vr = projected / avg_vol
            state["vol_ratio"] = vr

            prev_vr = prev.get("vol_ratio", 0)
            if vr >= 3.0 and prev_vr < 3.0 and _can_alert(code, "vol_burst"):
                alerts.append(f"\U0001f534 {name} Vol {vr:.1f}x \ud3ed\ubc1c!")
            elif vr >= 2.0 and prev_vr < 2.0 and _can_alert(code, "vol_strong"):
                alerts.append(f"\U0001f7e1 {name} Vol {vr:.1f}x \uac15\ud568")

        # ── 2. ICT EQ 레벨 (부호 변화만 감지 + 쿨다운) ──
        try:
            eq_adj, eq_reason = get_eq_score_adjustment(
                code, current, target_date=today
            )
            prev_eq_sign = 1 if prev.get("eq_adj", 0) > 0 else (-1 if prev.get("eq_adj", 0) < 0 else 0)
            curr_eq_sign = 1 if eq_adj > 0 else (-1 if eq_adj < 0 else 0)
            state["eq_adj"] = eq_adj

            if curr_eq_sign != prev_eq_sign and curr_eq_sign != 0:
                if curr_eq_sign < 0 and _can_alert(code, "eq_high"):
                    alerts.append(f"\u26a0\ufe0f {name} EQ High \uadfc\uc811! ({eq_reason})")
                elif curr_eq_sign > 0 and _can_alert(code, "eq_low"):
                    alerts.append(f"\U0001f4aa {name} EQ Low \uc9c0\uc9c0 ({eq_reason})")
        except Exception:
            pass

        # ── 3. ICT Gap 진입 (부호 변화만 감지 + 쿨다운) ──
        try:
            gap_adj, gap_reason = get_gap_score_adjustment(
                code, current, target_date=today
            )
            prev_gap_sign = 1 if prev.get("gap_adj", 0) > 0 else (-1 if prev.get("gap_adj", 0) < 0 else 0)
            curr_gap_sign = 1 if gap_adj > 0 else (-1 if gap_adj < 0 else 0)
            state["gap_adj"] = gap_adj

            if curr_gap_sign != prev_gap_sign and curr_gap_sign != 0:
                if curr_gap_sign > 0 and _can_alert(code, "gap"):
                    alerts.append(f"\U0001f4ca {name} \uac2d \uc9c0\uc9c0 \uc9c4\uc785 ({gap_reason})")
                elif curr_gap_sign < 0 and _can_alert(code, "gap"):
                    alerts.append(f"\U0001f4ca {name} \uac2d \uc800\ud56d \uc9c4\uc785 ({gap_reason})")
        except Exception:
            pass

        # ── 4. VWAP 크로스오버 (쿨다운 적용) ──
        vwap_val = _get_vwap(code)
        if vwap_val > 0:
            above_vwap = current >= vwap_val
            state["above_vwap"] = above_vwap
            prev_above = prev.get("above_vwap")

            if prev_above is not None and above_vwap != prev_above and _can_alert(code, "vwap"):
                diff_pct = (current - vwap_val) / vwap_val * 100
                if above_vwap:
                    alerts.append(
                        f"\U0001f4c8 {name} VWAP 상향돌파! "
                        f"({vwap_val:,.0f} \u2192 {current:,} V\u25b2{diff_pct:+.1f}%)"
                    )
                else:
                    alerts.append(
                        f"\U0001f4c9 {name} VWAP 하향이탈! "
                        f"({vwap_val:,.0f} \u2192 {current:,} V\u25bc{diff_pct:+.1f}%)"
                    )

        # ── 5. 급변 (5%p+ 스윙, 오프닝 억제 + 쿨다운) ──
        prev_pnl = prev.get("pnl", pnl)
        delta = pnl - prev_pnl
        if not is_opening and abs(delta) >= 5.0 and _can_alert(code, "swing"):
            emoji = "\U0001f4c8" if delta > 0 else "\U0001f4c9"
            alerts.append(
                f"{emoji} {name} {prev_pnl:+.1f}% \u2192 {pnl:+.1f}% ({delta:+.1f}%p)"
            )

        new_states[code] = state

    # 쿨다운 맵 저장
    new_states["_cooldowns"] = cooldowns

    return alerts, new_states


def format_alerts(alerts: list[str]) -> str:
    """알림 리스트 → 텔레그램 메시지"""
    if not alerts:
        return ""
    now_str = datetime.now().strftime("%H:%M")
    lines = [
        f"\U0001f6a8 보유종목 이상감지 ({now_str})",
        "\u2501" * 20,
    ]
    lines.extend(alerts)
    return "\n".join(lines)
