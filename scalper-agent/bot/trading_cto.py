# -*- coding: utf-8 -*-
"""
TradingCTO - 기술 총괄 (Chief Technology Officer)
=================================================
파라미터/피처플래그/시그널 정확도를 중앙에서 관리.
~95개 하드코딩 상수를 JSON 기반으로 전환.

원칙:
  - JSON 파일 없으면 하드코딩 DEFAULT 폴백 (무중단)
  - 미등록 피처는 True 반환 (안전 폴백)
  - CTO 오류가 매매를 차단하는 일은 절대 없음
"""

import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.CTO")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
PARAMS_PATH = DATA_DIR / "cto_params.json"
FEATURES_PATH = DATA_DIR / "cto_features.json"
SIGNALS_PATH = DATA_DIR / "cto_signals.json"


class TradingCTO:
    """기술 총괄 파사드"""

    # ═══════════════════════════════════════════
    #  기본 파라미터 (JSON 없을 때 폴백)
    # ═══════════════════════════════════════════
    DEFAULT_PARAMS = {
        "trix": {
            "period": 12,
            "signal_period": 9,
            "adx_period": 14,
            "min_adx": 20,
        },
        "macd": {
            "fast": 12,
            "slow": 26,
            "signal": 9,
        },
        "rsi": {
            "period": 14,
        },
        "momentum": {
            "threshold": 0.40,
            "strong_threshold": 0.60,
        },
        "tv_scanner": {
            "quiet_acc_threshold": 80,
            "explosion_threshold": 80,
            "direct_quiet_bonus": 35,
            "direct_explosion_bonus": 30,
            "tv_ratio_aaa": 2.0,
            "tv_ratio_buy_signal": 1.5,
            "tv_ratio_caution": 1.3,
        },
        "nxt": {
            "min_score": 3,
            "budget_pct": 30,
            "max_positions": 1,
            "predawn_min_score": 90,   # 4/26: softcap 2단계 반영 (100→90, raw120+=final92)
            "predawn_budget_pct": 40,
            "min_score_tier1": 20,
            "min_score_tier2": 30,
        },
        "entry": {
            "conditions_needed": 3,
            "total_conditions": 6,
            "split_count": 3,
            "split_interval_sec": 300,
        },
        "brain": {
            "min_pct": 30,
            "max_pct": 120,
        },
        "eye": {
            "min_score": 50,
            "poll_interval": 10,
            "sr_touch_pct": 0.003,
            "bouncing_sl": 0.995,
            "alive_trailing_sl": 0.985,
            "weakening_trailing_sl": 0.975,
            "min_strength_weak": 90,
        },
        "risk": {
            "sl_pct": 3.5,
            "trailing_pct": 2.0,
            "max_hold_days_momentum": 5,
            "daily_loss_limit": 500000,
            "reserve_ratio": 0.10,
            "sl_pct_day": 2.0,
            "tp_pct_day": 5.0,
        },
        "bot": {
            "auto_buy_amount": 500000,
            "add_on_ratio": 0.30,
            "min_add_amount": 100000,
        },
    }

    # ═══════════════════════════════════════════
    #  기본 피처 플래그 (JSON 없을 때 폴백)
    # ═══════════════════════════════════════════
    DEFAULT_FEATURES = {
        "trix_divergence": True,
        "tv_direct": True,
        "tv_cluster": True,
        "tv_persistence": True,
        "nxt_nightwatch": True,
        "predawn_buy": True,
        "macd_scan": True,
        "news_sentiment": True,
        "short_analyzer": True,
        "options_signal": True,
        "etf_recommender": True,
        "sector_momentum": True,
        "commodity_relay": True,
        "position_guardian": True,
        "drawdown_shield": True,
        "brain_allocation": True,
        "flow_zscore": True,
    }

    def __init__(self, config: dict):
        self.config = config
        self._params = self._load_params()
        self._features = self._load_features()
        self._signals = self._load_signals()

    # ═══════════════════════════════════════════
    #  1. 파라미터 조회
    # ═══════════════════════════════════════════

    def get_params(self, indicator_name: str) -> dict:
        """지표 파라미터 전체 조회

        Args:
            indicator_name: "trix", "macd", "rsi" 등

        Returns:
            파라미터 dict (없으면 빈 dict)
        """
        return self._params.get(indicator_name, {})

    def get_param(self, indicator_name: str, key: str, default=None):
        """지표 파라미터 단일 조회

        Args:
            indicator_name: "trix", "macd" 등
            key: 파라미터 키
            default: 기본값

        Returns:
            값 (없으면 default)
        """
        return self._params.get(indicator_name, {}).get(key, default)

    # ═══════════════════════════════════════════
    #  2. 피처 플래그
    # ═══════════════════════════════════════════

    def is_enabled(self, feature_name: str) -> bool:
        """피처 활성화 여부

        미등록 피처는 True 반환 (안전 폴백 — 기존 동작 유지)

        Args:
            feature_name: 피처 이름

        Returns:
            True/False
        """
        return self._features.get(feature_name, True)

    def set_feature(self, feature_name: str, enabled: bool):
        """런타임 피처 플래그 변경

        Args:
            feature_name: 피처 이름
            enabled: True/False
        """
        self._features[feature_name] = enabled
        self._save_features()
        logger.info(f"[CTO] 피처 변경: {feature_name} = {enabled}")

    # ═══════════════════════════════════════════
    #  3. 시그널 추적
    # ═══════════════════════════════════════════

    def track_signal(
        self,
        code: str,
        signal_source: str,
        predicted_direction: str,
        score: float,
        target_price: int = 0,
    ):
        """시그널 예측 기록

        Args:
            code: 종목코드
            signal_source: "trix_divergence", "tv_direct", "macd_scan" 등
            predicted_direction: "BUY" / "SELL"
            score: 시그널 점수
            target_price: 예측 목표가 (0=미설정)
        """
        today = date.today().isoformat()
        if today not in self._signals:
            self._signals[today] = []

        self._signals[today].append({
            "code": code,
            "source": signal_source,
            "direction": predicted_direction,
            "score": score,
            "target_price": target_price,
            "time": datetime.now().strftime("%H:%M:%S"),
        })

        self._save_signals()

    # ═══════════════════════════════════════════
    #  4. 시그널 정확도
    # ═══════════════════════════════════════════

    def calc_signal_accuracy(self, source: str = "", lookback_days: int = 30) -> dict:
        """시그널 소스별 정확도 계산

        recommendation_history.json의 T+N 수익률과 대조하여
        각 소스의 예측 적중률을 계산.

        Args:
            source: 필터할 소스 (빈 문자열=전체)
            lookback_days: 분석 기간 (일)

        Returns:
            {source: {"total", "correct", "accuracy_pct", "avg_return"}, ...}
        """
        result = {}

        try:
            # recommendation_history.json에서 실제 결과 로드
            history_path = DATA_DIR / "recommendation_history.json"
            if not history_path.exists():
                return result

            with open(history_path, "r", encoding="utf-8") as f:
                history = json.load(f)

            cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

            for dt, snapshot in history.items():
                if dt < cutoff:
                    continue

                for stock in snapshot.get("stocks", []):
                    # T+1 수익률이 있는 종목만 분석
                    tn_returns = stock.get("tn_returns", {})
                    if not tn_returns:
                        continue

                    sources = stock.get("sources", [])
                    if isinstance(sources, str):
                        sources = [sources]

                    t1_ret = tn_returns.get("T+1", 0)

                    for src in sources:
                        if source and src != source:
                            continue
                        if src not in result:
                            result[src] = {
                                "total": 0,
                                "correct": 0,
                                "total_return": 0.0,
                            }
                        result[src]["total"] += 1
                        result[src]["total_return"] += t1_ret
                        if t1_ret > 0:
                            result[src]["correct"] += 1

            # 정확도 계산
            for src, data in result.items():
                total = data["total"]
                data["accuracy_pct"] = (
                    round(data["correct"] / total * 100, 1) if total > 0 else 0.0
                )
                data["avg_return"] = (
                    round(data["total_return"] / total, 2) if total > 0 else 0.0
                )

        except Exception as e:
            logger.warning(f"CTO: 시그널 정확도 계산 실패: {e}")

        return result

    # ═══════════════════════════════════════════
    #  5. 대시보드
    # ═══════════════════════════════════════════

    def get_status(self) -> str:
        """CTO 상태 대시보드"""
        lines = [
            "=== CTO 기술 대시보드 ===",
        ]

        # 피처 플래그 현황
        enabled = [k for k, v in self._features.items() if v]
        disabled = [k for k, v in self._features.items() if not v]
        lines.append(f"\n피처 플래그: {len(enabled)} ON / {len(disabled)} OFF")
        if disabled:
            lines.append(f"  OFF: {', '.join(disabled)}")

        # 오늘 시그널 현황
        today = date.today().isoformat()
        today_signals = self._signals.get(today, [])
        if today_signals:
            source_counts = {}
            for sig in today_signals:
                src = sig.get("source", "unknown")
                source_counts[src] = source_counts.get(src, 0) + 1
            lines.append(f"\n오늘 시그널: {len(today_signals)}건")
            for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
                lines.append(f"  {src}: {cnt}건")
        else:
            lines.append(f"\n오늘 시그널: 0건")

        # 정확도 (최근 30일)
        try:
            accuracy = self.calc_signal_accuracy(lookback_days=30)
            if accuracy:
                lines.append(f"\n시그널 정확도 (30일):")
                for src, data in sorted(
                    accuracy.items(), key=lambda x: -x[1]["accuracy_pct"]
                ):
                    lines.append(
                        f"  {src}: {data['accuracy_pct']:.1f}% "
                        f"({data['correct']}/{data['total']}) "
                        f"avg={data['avg_return']:+.2f}%"
                    )
        except Exception:
            pass

        # 주요 파라미터
        lines.append(f"\n주요 파라미터:")
        for ind in ["trix", "macd", "entry", "nxt", "risk", "eye", "bot"]:
            p = self._params.get(ind, {})
            if p:
                vals = ", ".join(f"{k}={v}" for k, v in p.items())
                lines.append(f"  {ind}: {vals}")

        return "\n".join(lines)

    # ═══════════════════════════════════════════
    #  내부: JSON 로드/저장
    # ═══════════════════════════════════════════

    def _load_params(self) -> dict:
        """파라미터 JSON 로드 (없으면 DEFAULT 폴백)"""
        try:
            if PARAMS_PATH.exists():
                with open(PARAMS_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                # DEFAULT와 merge (새 키 추가 시 자동 반영)
                merged = {}
                for key, defaults in self.DEFAULT_PARAMS.items():
                    merged[key] = {**defaults, **loaded.get(key, {})}
                return merged
        except Exception as e:
            logger.warning(f"CTO params 로드 실패 (DEFAULT 사용): {e}")
        return {k: dict(v) for k, v in self.DEFAULT_PARAMS.items()}

    def _load_features(self) -> dict:
        """피처플래그 JSON 로드 (없으면 DEFAULT 폴백)"""
        try:
            if FEATURES_PATH.exists():
                with open(FEATURES_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                # DEFAULT와 merge
                merged = {**self.DEFAULT_FEATURES, **loaded}
                return merged
        except Exception as e:
            logger.warning(f"CTO features 로드 실패 (DEFAULT 사용): {e}")
        return dict(self.DEFAULT_FEATURES)

    def _save_features(self):
        """피처플래그 저장"""
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = FEATURES_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._features, f, ensure_ascii=False, indent=2)
            tmp.replace(FEATURES_PATH)
        except Exception as e:
            logger.error(f"CTO features 저장 실패: {e}")

    def _load_signals(self) -> dict:
        """시그널 이력 로드"""
        try:
            if SIGNALS_PATH.exists():
                with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"CTO signals 로드 실패: {e}")
        return {}

    def _save_signals(self):
        """시그널 이력 저장 (최근 30일만)"""
        try:
            # 30일 초과 데이터 정리
            cutoff = (date.today() - timedelta(days=30)).isoformat()
            cleaned = {k: v for k, v in self._signals.items() if k >= cutoff}
            self._signals = cleaned

            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = SIGNALS_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._signals, f, ensure_ascii=False, indent=2)
            tmp.replace(SIGNALS_PATH)
        except Exception as e:
            logger.error(f"CTO signals 저장 실패: {e}")
