# -*- coding: utf-8 -*-
"""
줍줍 스캐너 (Bargain Scanner) - 추천 파이프라인 자동 통합용
=============================================================
전쟁 전 고점(2025-12 ~ 2026-02-27) 대비 낙폭이 큰 종목 중
기관/외국인 진성매집이 확인된 종목을 자동 발굴.

파이프라인 Step 2.7로 동작:
  1. processed parquet에서 전쟁전고점 vs 최신종가 낙폭 계산
  2. Flow(investor) CSV에서 기관/외인 N일 순매수 합산
  3. 기술적 과매도 (RSI/BB) - parquet에 이미 계산됨
  4. 종합 점수 → 상위 종목 반환

릴레이/사전감지와 함께 step5 교차검증에 투입됨.
"""

import logging
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger("BH.Bargain")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
PROCESSED_DIR = DATA_DIR / "processed"
FLOW_DIR = DATA_DIR / "flow"

# 전쟁 전 기간 (고점 탐색 구간)
PRE_WAR_START = "2025-12-01"
PRE_WAR_END = "2026-02-27"

# 필터 기준
MIN_DROP_PCT = -25.0       # 최소 낙폭 25%
MIN_SUPPLY_AMOUNT = 100    # 최소 순매수 금액 합산 (100백만원 = 1억원, flow 데이터 단위: 백만원)


@dataclass
class BargainCandidate:
    code: str
    name: str
    close: int
    pre_war_high: int
    drop_pct: float          # 고점 대비 낙폭 (%)
    foreign_5d: float        # 외인 5일 순매수 (억원)
    foreign_10d: float       # 외인 10일 순매수 (억원)
    inst_5d: float           # 기관 5일 순매수 (억원)
    inst_10d: float          # 기관 10일 순매수 (억원)
    combined_10d: float      # 기관+외인 10일 합산 (억원)
    supply_grade: str        # SSS / SS / S / A
    rsi: float
    bb_pctb: float
    bargain_score: float     # 종합 점수


def _load_flow(code: str, days: int = 10) -> tuple:
    """투자자별 수급 데이터 로드 → (외인dict, 기관dict) 순매수 금액"""
    flow_path = FLOW_DIR / f"{code}_investor.csv"
    if not flow_path.exists():
        return {}, {}

    try:
        df = pd.read_csv(flow_path, index_col=0, encoding="utf-8")
        if df.empty:
            return {}, {}

        foreign_col = None
        inst_col = None
        for c in df.columns:
            if "외국인" in c and "금액" in c:
                foreign_col = c
            if "기관" in c and "금액" in c:
                inst_col = c

        result_foreign = {}
        result_inst = {}

        if foreign_col:
            vals = df[foreign_col].dropna().tail(days)
            for d in [1, 3, 5, 10]:
                n = min(d, len(vals))
                result_foreign[f"{d}d"] = float(vals.tail(n).sum()) if n > 0 else 0
            result_foreign["buy_days_5"] = int((vals.tail(5) > 0).sum()) if len(vals) >= 5 else 0

        if inst_col:
            vals = df[inst_col].dropna().tail(days)
            for d in [1, 3, 5, 10]:
                n = min(d, len(vals))
                result_inst[f"{d}d"] = float(vals.tail(n).sum()) if n > 0 else 0
            result_inst["buy_days_5"] = int((vals.tail(5) > 0).sum()) if len(vals) >= 5 else 0

        return result_foreign, result_inst
    except Exception as e:
        logger.debug(f"Flow 로드 실패 {code}: {e}")
        return {}, {}


def _grade_supply(foreign: dict, inst: dict) -> str:
    """수급 등급 판정
    SSS: 기관+외인 동시 3일+ 순매수
    SS:  외인 단독 3일+ 순매수 (10D 합산 50억+)
    S:   기관 단독 3일+ 순매수 (10D 합산 30억+)
    A:   어느 한쪽 1일+ 순매수
    """
    f_days = foreign.get("buy_days_5", 0)
    i_days = inst.get("buy_days_5", 0)
    f_10d = foreign.get("10d", 0)
    i_10d = inst.get("10d", 0)

    if f_days >= 3 and i_days >= 3:
        return "SSS"
    if f_days >= 3 and f_10d >= 5e9:  # 50억+
        return "SS"
    if i_days >= 3 and i_10d >= 3e9:  # 30억+
        return "S"
    if f_days >= 1 or i_days >= 1:
        return "A"
    return ""


def scan_bargain(universe: dict = None, top_n: int = 10) -> list[BargainCandidate]:
    """줍줍 스캐너 메인 - 전쟁전 고점 대비 낙폭 + 수급매집 종목 발굴

    Args:
        universe: {code: (name, suffix, market)} dict
        top_n: 상위 N종목 반환

    Returns:
        BargainCandidate 리스트 (bargain_score 내림차순)
    """
    if not PROCESSED_DIR.exists():
        logger.warning(f"processed 디렉토리 없음: {PROCESSED_DIR}")
        return []

    # 이름 매핑
    name_map = {}
    if universe:
        for code, info in universe.items():
            if isinstance(info, (list, tuple)) and len(info) > 0:
                name_map[code] = info[0]
            elif isinstance(info, dict):
                name_map[code] = info.get("name", code)

    pq_files = list(PROCESSED_DIR.glob("*.parquet"))
    if not pq_files:
        logger.warning("Parquet 파일 없음")
        return []

    candidates = []
    errors = 0

    for pq_path in pq_files:
        code = pq_path.stem
        if code.startswith("_"):
            continue

        try:
            df = pd.read_parquet(pq_path)
            if len(df) < 30:
                continue

            # processed parquet: 인덱스='날짜', 컬럼='시가/고가/저가/종가/거래량/...'
            # 인덱스를 DatetimeIndex로 변환
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            close_col = "종가" if "종가" in df.columns else "close"
            high_col = "고가" if "고가" in df.columns else "high"

            close_arr = df[close_col].astype(float).values
            high_arr = df[high_col].astype(float).values if high_col in df.columns else close_arr

            current_close = int(close_arr[-1])
            if current_close <= 0:
                continue

            # 전쟁전 고점 (2025-12 ~ 2026-02-27)
            pre_mask = (df.index >= PRE_WAR_START) & (df.index <= PRE_WAR_END)
            pre_highs = high_arr[pre_mask]
            if len(pre_highs) == 0:
                continue

            pre_war_high = int(np.max(pre_highs))
            if pre_war_high <= 0:
                continue

            drop_pct = ((current_close - pre_war_high) / pre_war_high) * 100

            # 최소 낙폭 필터
            if drop_pct > MIN_DROP_PCT:
                continue

            # 수급 데이터 로드
            foreign, inst = _load_flow(code)
            if not foreign and not inst:
                continue

            # 수급 등급
            grade = _grade_supply(foreign, inst)
            if not grade:  # 수급 시그널 없으면 패스
                continue

            f_5d = foreign.get("5d", 0)
            f_10d = foreign.get("10d", 0)
            i_5d = inst.get("5d", 0)
            i_10d = inst.get("10d", 0)
            combined_10d = f_10d + i_10d

            # 최소 순매수 금액 필터
            if combined_10d < MIN_SUPPLY_AMOUNT:
                continue

            # 기술적 지표 (parquet에 이미 계산되어 있음)
            rsi_col = "RSI14" if "RSI14" in df.columns else None
            bb_upper_col = "BB_upper" if "BB_upper" in df.columns else None
            bb_lower_col = "BB_lower" if "BB_lower" in df.columns else None

            if rsi_col:
                rsi = float(df[rsi_col].iloc[-1])
            else:
                # fallback 계산
                deltas = np.diff(close_arr)
                gains = np.where(deltas > 0, deltas, 0)
                losses = np.where(deltas < 0, -deltas, 0)
                ag = np.mean(gains[-14:]) if len(gains) >= 14 else 1
                al = np.mean(losses[-14:]) if len(losses) >= 14 else 1
                rsi = 100 - (100 / (1 + ag / al)) if al > 0 else 50

            if bb_upper_col and bb_lower_col:
                bb_u = float(df[bb_upper_col].iloc[-1])
                bb_l = float(df[bb_lower_col].iloc[-1])
                bb_pctb = (current_close - bb_l) / (bb_u - bb_l) if (bb_u - bb_l) > 0 else 0.5
            else:
                sma = np.mean(close_arr[-20:]) if len(close_arr) >= 20 else current_close
                std = np.std(close_arr[-20:]) if len(close_arr) >= 20 else 1
                bb_u = sma + 2 * std
                bb_l = sma - 2 * std
                bb_pctb = (current_close - bb_l) / (bb_u - bb_l) if (bb_u - bb_l) > 0 else 0.5

            # === 종합 점수 계산 ===
            score = 0.0

            # 1. 낙폭 점수 (최대 30)
            drop_score = min(abs(drop_pct) * 0.6, 30)
            score += drop_score

            # 2. 수급 등급 점수 (최대 25)
            grade_scores = {"SSS": 25, "SS": 20, "S": 15, "A": 8}
            score += grade_scores.get(grade, 0)

            # 3. RSI 과매도 점수 (최대 15)
            if rsi < 30:
                score += 15
            elif rsi < 40:
                score += 10
            elif rsi < 50:
                score += 5

            # 4. BB %B 과매도 (최대 15)
            if bb_pctb < 0.2:
                score += 15
            elif bb_pctb < 0.3:
                score += 10
            elif bb_pctb < 0.5:
                score += 5

            # 5. 순매수 규모 보너스 (최대 15) - 데이터 단위: 백만원
            combined_bil = combined_10d / 100  # 억원 단위 (백만원 → 억원)
            if combined_bil >= 100:
                score += 15
            elif combined_bil >= 50:
                score += 10
            elif combined_bil >= 10:
                score += 5

            name = name_map.get(code, code)

            candidates.append(BargainCandidate(
                code=code,
                name=name,
                close=current_close,
                pre_war_high=pre_war_high,
                drop_pct=round(drop_pct, 1),
                foreign_5d=round(f_5d / 100, 1),     # 백만원 → 억원
                foreign_10d=round(f_10d / 100, 1),
                inst_5d=round(i_5d / 100, 1),
                inst_10d=round(i_10d / 100, 1),
                combined_10d=round(combined_10d / 100, 1),
                supply_grade=grade,
                rsi=round(rsi, 1),
                bb_pctb=round(bb_pctb, 2),
                bargain_score=round(score, 1),
            ))

        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.debug(f"스캔 실패 {code}: {e}")

    # 점수 내림차순 정렬
    candidates.sort(key=lambda c: c.bargain_score, reverse=True)

    logger.info(
        f"줍줍 스캔 완료: {len(candidates)}종목 통과 "
        f"(전체 {len(pq_files)}, 에러 {errors}건)"
    )

    return candidates[:top_n]


def format_bargain_report(candidates: list[BargainCandidate]) -> str:
    """텔레그램용 줍줍 리포트"""
    if not candidates:
        return "줍줍 후보 없음"

    lines = [
        "🛒 줍줍 스캐너 (전쟁전 고점 대비 낙폭+수급매집)",
        "=" * 36,
        "",
    ]

    grade_emoji = {"SSS": "🔴", "SS": "🟡", "S": "🟢", "A": "⚪"}

    for i, c in enumerate(candidates, 1):
        ge = grade_emoji.get(c.supply_grade, "⚪")
        lines.append(
            f"{ge} {i}. {c.name}({c.code}) [{c.supply_grade}] "
            f"점수:{c.bargain_score:.0f}"
        )
        lines.append(
            f"   현재: {c.close:,}원 | 고점: {c.pre_war_high:,}원 | "
            f"낙폭: {c.drop_pct:.1f}%"
        )
        lines.append(
            f"   외인10D: {c.foreign_10d:+,.1f}억 | "
            f"기관10D: {c.inst_10d:+,.1f}억"
        )
        lines.append(
            f"   RSI: {c.rsi:.0f} | BB%B: {c.bb_pctb:.2f}"
        )
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO)
    try:
        from data.kis_collector import UNIVERSE
    except Exception:
        UNIVERSE = None
    results = scan_bargain(universe=UNIVERSE)
    print(format_bargain_report(results))
