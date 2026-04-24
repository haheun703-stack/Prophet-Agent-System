# -*- coding: utf-8 -*-
"""
장중 알고리즘 감지기 (Intraday Algo Detector)
================================================
09:00~15:20 장중에 추천 종목 + 보유 종목의 이상 신호를 실시간 감지.

감지 유형:
  A) 거래량 폭발 + 체결강도 급등 (알고리즘 매집/투매 감지)
  B) 호가창 불균형 (빅오더 감지)
  C) 테마/섹터 동시 급등 (테마주 트래킹)

사용처: telegram_bot.py B14 (60초 반복)
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.AlgoDetector")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"

# ── 감지 임계값 ──
STRENGTH_BULL = 130      # 체결강도 130+ → 강매수 (외인/기관 알고 의심)
STRENGTH_BEAR = 60       # 체결강도 60 이하 → 강매도
VOLUME_RATIO_ALERT = 3.0  # 전일 대비 거래량 3배 이상 → 폭발
PRICE_SURGE = 5.0        # +5% 이상 급등
PRICE_PLUNGE = -3.0      # -3% 이상 급락
BID_ASK_IMBALANCE = 200  # 매수/매도 잔량 비율 200%+ → 빅오더 매수
BID_ASK_DUMP = 50        # 매수/매도 잔량 비율 50% 이하 → 빅오더 매도
THEME_MIN_STOCKS = 2     # 같은 섹터 2종목 이상 동시 급등 시 테마 알림

# 알림 쿨다운 (같은 종목+신호 5분 이내 중복 방지)
ALERT_COOLDOWN = 300  # 5분


class AlgoDetector:
    """장중 알고리즘 감지기."""

    def __init__(self, kis):
        """
        Args:
            kis: KISTrader 인스턴스
        """
        self.kis = kis
        self._alert_history: dict[str, float] = {}  # "code_signal" → last_alert_ts
        self._prev_snapshots: dict[str, dict] = {}   # code → 이전 스냅샷 (가속도 감지)
        self._sector_map: dict[str, str] = {}         # code → sector

    def load_targets(self) -> list[dict]:
        """감시 대상 종목 로드 (추천 + 보유)."""
        targets = {}

        # 1) recommendation.json
        rec_path = DATA_DIR / "recommendation.json"
        if rec_path.exists():
            try:
                data = json.loads(rec_path.read_text(encoding="utf-8"))
                stocks = data.get("stocks", data if isinstance(data, list) else [])
                for s in stocks:
                    code = s.get("code", "")
                    if code:
                        targets[code] = {
                            "code": code,
                            "name": s.get("name", code),
                            "prev_close": s.get("close", 0),
                            "prev_volume": s.get("volume", 0),
                            "sector": s.get("sector", ""),
                            "source": "recommendation",
                        }
                        if s.get("sector"):
                            self._sector_map[code] = s["sector"]
            except Exception as e:
                logger.warning(f"recommendation.json 로드 실패: {e}")

        # 2) positions.json (보유 종목)
        pos_path = DATA_DIR / "positions.json"
        if pos_path.exists():
            try:
                positions = json.loads(pos_path.read_text(encoding="utf-8"))
                if isinstance(positions, dict):
                    for code, p in positions.items():
                        if code not in targets:
                            targets[code] = {
                                "code": code,
                                "name": p.get("name", code),
                                "prev_close": p.get("entry_price", 0),
                                "prev_volume": 0,
                                "sector": p.get("sector", ""),
                                "source": "position",
                            }
            except Exception as e:
                logger.warning(f"positions.json 로드 실패: {e}")

        return list(targets.values())

    def scan_once(self, targets: list[dict]) -> list[dict]:
        """1회 스캔 — 모든 대상 종목의 실시간 데이터 수집 + 이상 감지.

        Returns: [{code, name, signals: [signal_dict, ...], snapshot: {...}}, ...]
        """
        results = []
        now = time.time()

        for t in targets:
            code = t["code"]
            name = t["name"]

            # 현재가 조회
            price_resp = self.kis.fetch_price(code)
            if not price_resp.get("success"):
                continue

            cp = price_resp["current_price"]
            chg_rate = price_resp.get("change_rate", 0)
            volume = price_resp.get("volume", 0)
            strength = price_resp.get("strength", 0)
            prev_close = t.get("prev_close", 0) or cp

            # 호가 잔량 조회 (빅오더 감지용)
            spread_resp = self.kis.fetch_expected_price(code)
            bar = 0
            if spread_resp.get("success"):
                bar = spread_resp.get("bid_ask_ratio", 0)

            # 거래량 비율 (전일 대비)
            prev_vol = t.get("prev_volume", 0)
            vol_ratio = volume / prev_vol if prev_vol > 0 else 0

            snapshot = {
                "price": cp, "change_rate": chg_rate,
                "volume": volume, "vol_ratio": round(vol_ratio, 1),
                "strength": strength, "bid_ask_ratio": bar,
            }

            # ── 이상 신호 감지 ──
            signals = []

            # A-1) 체결강도 급등 (알고리즘 매집)
            if strength >= STRENGTH_BULL:
                signals.append({
                    "type": "ALGO_BUY",
                    "desc": f"체결강도 {strength:.0f} (매수세 폭발)",
                    "severity": "HIGH" if strength >= 150 else "MEDIUM",
                })

            # A-2) 체결강도 급락 (알고리즘 투매)
            if 0 < strength <= STRENGTH_BEAR:
                signals.append({
                    "type": "ALGO_SELL",
                    "desc": f"체결강도 {strength:.0f} (매도세 압도)",
                    "severity": "HIGH" if strength <= 40 else "MEDIUM",
                })

            # A-3) 거래량 폭발
            if vol_ratio >= VOLUME_RATIO_ALERT:
                signals.append({
                    "type": "VOL_EXPLOSION",
                    "desc": f"거래량 {vol_ratio:.1f}배 (전일比)",
                    "severity": "HIGH" if vol_ratio >= 5.0 else "MEDIUM",
                })

            # B-1) 급등
            if chg_rate >= PRICE_SURGE:
                signals.append({
                    "type": "PRICE_SURGE",
                    "desc": f"급등 {chg_rate:+.1f}%",
                    "severity": "HIGH" if chg_rate >= 10 else "MEDIUM",
                })

            # B-2) 급락
            if chg_rate <= PRICE_PLUNGE:
                signals.append({
                    "type": "PRICE_PLUNGE",
                    "desc": f"급락 {chg_rate:+.1f}%",
                    "severity": "HIGH" if chg_rate <= -5 else "MEDIUM",
                })

            # B-3) 호가창 빅오더 매수
            if bar >= BID_ASK_IMBALANCE:
                signals.append({
                    "type": "BIG_BID",
                    "desc": f"매수벽 B/A {bar:.0f}%",
                    "severity": "MEDIUM",
                })

            # B-4) 호가창 빅오더 매도
            if 0 < bar <= BID_ASK_DUMP:
                signals.append({
                    "type": "BIG_ASK",
                    "desc": f"매도벽 B/A {bar:.0f}%",
                    "severity": "MEDIUM",
                })

            # A-4) 체결강도 + 거래량 동시 (복합 신호 = 강한 알고리즘)
            if strength >= STRENGTH_BULL and vol_ratio >= VOLUME_RATIO_ALERT:
                # 기존 개별 신호 대체
                signals = [s for s in signals
                           if s["type"] not in ("ALGO_BUY", "VOL_EXPLOSION")]
                signals.insert(0, {
                    "type": "ALGO_ACCUMULATE",
                    "desc": f"알고 매집 (강도{strength:.0f} + 거래{vol_ratio:.1f}배)",
                    "severity": "HIGH",
                })

            # 쿨다운 필터
            filtered = []
            for sig in signals:
                key = f"{code}_{sig['type']}"
                last = self._alert_history.get(key, 0)
                if now - last >= ALERT_COOLDOWN:
                    filtered.append(sig)
                    self._alert_history[key] = now

            if filtered:
                results.append({
                    "code": code,
                    "name": name,
                    "signals": filtered,
                    "snapshot": snapshot,
                    "sector": t.get("sector", ""),
                })

            snapshot["name"] = name  # 테마 감지 시 name 역조회용
            self._prev_snapshots[code] = snapshot
            time.sleep(0.12)  # API 제한

        return results

    def detect_theme_moves(self, results: list[dict]) -> list[dict]:
        """C) 테마/섹터 동시 급등 감지.

        같은 섹터에서 2종목 이상 동시에 급등(+3%↑) 또는 신호 발생 시 테마 알림.
        """
        sector_stocks = defaultdict(list)

        for r in results:
            sector = r.get("sector", "")
            if not sector:
                continue
            chg = r["snapshot"]["change_rate"]
            if chg >= 3.0 or any(s["severity"] == "HIGH" for s in r["signals"]):
                sector_stocks[sector].append(r)

        # 모든 감시 종목 중 +3% 이상인 것도 추가 (신호 없어도)
        for code, snap in self._prev_snapshots.items():
            if snap.get("change_rate", 0) >= 3.0:
                sector = self._sector_map.get(code, "")
                if sector and not any(
                    s["code"] == code for s in sector_stocks[sector]
                ):
                    sector_stocks[sector].append({
                        "code": code,
                        "name": snap.get("name", code),
                        "snapshot": snap,
                        "signals": [],
                        "sector": sector,
                    })

        themes = []
        now = time.time()
        for sector, stocks in sector_stocks.items():
            if len(stocks) >= THEME_MIN_STOCKS:
                key = f"THEME_{sector}"
                last = self._alert_history.get(key, 0)
                if now - last >= ALERT_COOLDOWN:
                    self._alert_history[key] = now
                    names = [s["name"] for s in stocks]
                    avg_chg = sum(s["snapshot"]["change_rate"] for s in stocks) / len(stocks)
                    themes.append({
                        "sector": sector,
                        "count": len(stocks),
                        "stocks": names,
                        "avg_change": round(avg_chg, 1),
                    })
        return themes


def format_algo_alert(results: list[dict], themes: list[dict]) -> str:
    """텔레그램 알림 포맷."""
    if not results and not themes:
        return ""

    now_str = datetime.now().strftime("%H:%M")
    lines = [f"🤖 알고리즘 감지 ({now_str})", "━━━━━━━━━━━━━━━━━━━"]

    severity_emoji = {"HIGH": "🔴", "MEDIUM": "🟡"}

    # 종목별 신호
    # HIGH 먼저, MEDIUM 나중
    sorted_results = sorted(
        results,
        key=lambda r: min(
            (0 if s["severity"] == "HIGH" else 1) for s in r["signals"]
        ) if r["signals"] else 2,
    )

    for r in sorted_results:
        name = r["name"]
        snap = r["snapshot"]
        chg = snap["change_rate"]
        sign = "+" if chg >= 0 else ""

        sig_parts = []
        for s in r["signals"]:
            emoji = severity_emoji.get(s["severity"], "")
            sig_parts.append(f"{emoji}{s['desc']}")

        sig_str = " | ".join(sig_parts)
        lines.append(f"{name} ({sign}{chg:.1f}%) — {sig_str}")

    # 테마 동시 급등
    if themes:
        lines.append("")
        lines.append("🔥 테마 동시 급등:")
        for t in themes:
            names = ", ".join(t["stocks"][:4])
            if len(t["stocks"]) > 4:
                names += f" 외 {len(t['stocks']) - 4}종목"
            lines.append(
                f"  [{t['sector']}] {t['count']}종목 평균 {t['avg_change']:+.1f}% — {names}"
            )

    return "\n".join(lines)


def save_algo_result(results: list[dict], themes: list[dict]):
    """감지 결과 JSON 저장 (최근 결과 누적)."""
    path = DATA_DIR / "algo_detect.json"

    # 기존 결과 로드 (최대 100건 유지)
    history = []
    if path.exists():
        try:
            history = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(history, list):
                history = []
        except Exception:
            history = []

    entry = {
        "timestamp": datetime.now().isoformat(),
        "alerts": len(results),
        "themes": len(themes),
        "results": results,
        "theme_moves": themes,
    }
    history.append(entry)
    # 최근 100건만 유지
    history = history[-100:]

    path.write_text(json.dumps(history, ensure_ascii=False, indent=2),
                    encoding="utf-8")
