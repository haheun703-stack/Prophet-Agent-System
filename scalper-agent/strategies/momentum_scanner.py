# -*- coding: utf-8 -*-
"""
소형주 급등 포착 스캐너 (Momentum Hunter)
==========================================
시총 300억~5000억 소형주 전용.
테마(AI/방산/원전 등) 연결 + 거래량 폭발 + 기술 확인 → 급등 직전 종목 포착.

성호전자 사례: 엔비디아 공급망 → 3/4 25,650 → 3/6 37,600 (+46%, 2일)

사용법:
  python -m strategies.momentum_scanner           # 스캔 실행
  python -m strategies.momentum_scanner --top 10  # 상위 10개
"""

import sys
import logging
import warnings
warnings.filterwarnings("ignore")
from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path

import numpy as np
import pandas as pd

# CLI 직접 실행 시 경로 보정
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from dotenv import load_dotenv
    load_dotenv(str(Path(__file__).parent.parent.parent / ".env"))

from data.extend_parquet_data import load_daily
from data.universe_builder import load_smallcap_universe
from strategies.dynamic_target import calc_rsi, calc_atr

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"


# ═══════════════════════════════════════════════════
#  테마-공급망 매핑 (소형주 급등의 핵심)
# ═══════════════════════════════════════════════════

THEME_SUPPLY_CHAIN = {
    "AI_GPU": {
        "keywords": ["엔비디아", "GPU", "AI서버", "HBM", "데이터센터",
                     "AI반도체", "AI칩", "그래픽카드", "CUDA", "AI가속기"],
        "대장": ["000660", "005930"],  # SK하이닉스, 삼성전자
    },
    "AI_소프트웨어": {
        "keywords": ["AI", "인공지능", "챗GPT", "LLM", "생성형AI",
                     "AI에이전트", "클라우드AI", "머신러닝"],
        "대장": ["035420", "035720"],  # 네이버, 카카오
    },
    "방산": {
        "keywords": ["K-방산", "전쟁", "이란", "우크라이나", "방산수출",
                     "방위산업", "무기수출", "폴란드", "NATO", "중동"],
        "대장": ["012450", "079550"],  # 한화에어로, 현대로템
    },
    "원전": {
        "keywords": ["SMR", "원전수출", "소형원자로", "원전 재가동",
                     "체코 원전", "원자력", "핵융합"],
        "대장": ["034020"],  # 두산에너빌리티
    },
    "2차전지": {
        "keywords": ["배터리", "리튬", "양극재", "음극재", "전해질",
                     "전고체", "LFP", "NCM", "CATL"],
        "대장": ["006400", "373220"],  # 삼성SDI, LG에너지
    },
    "로봇": {
        "keywords": ["로봇", "협동로봇", "자율주행", "로봇팔",
                     "산업로봇", "서비스로봇", "AI로봇"],
        "대장": ["012450", "005380"],  # 한화, 현대차
    },
    "반도체장비": {
        "keywords": ["반도체장비", "반도체소부장", "EUV", "웨이퍼",
                     "패키징", "CMP", "식각", "증착"],
        "대장": ["000660", "042700"],  # SK하이닉스, 한미반도체
    },
    "바이오": {
        "keywords": ["FDA", "임상3상", "신약", "바이오시밀러",
                     "ADC", "세포치료", "유전자치료"],
        "대장": ["207940", "068270"],  # 삼성바이오, 셀트리온
    },
}


# ═══════════════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════════════

@dataclass
class MomentumCandidate:
    """소형주 급등 후보"""
    code: str
    name: str
    momentum_score: float = 0       # 0~100 종합 점수
    theme: str = ""                 # 매칭된 테마
    theme_detail: str = ""          # 상세 (어떤 키워드 매칭)
    volume_spike: float = 0         # 거래량 배수 (20일 대비)
    price_change_5d: float = 0      # 5일 등락률 (%)
    leader_return_5d: float = 0     # 대장주 5일 수익률 (%)
    leader_gap: float = 0           # 대장-소부장 갭 (%)
    rsi: float = 0
    atr_pct: float = 0             # ATR/종가 (변동성 %)
    signals: list = field(default_factory=list)
    entry: int = 0
    sl: int = 0
    tp: int = 0
    cap_억: int = 0


# ═══════════════════════════════════════════════════
#  Phase 1: 거래량 폭발 필터
# ═══════════════════════════════════════════════════

def _check_volume_explosion(df: pd.DataFrame) -> tuple:
    """거래량 폭발 체크

    Returns: (volume_spike: float, signals: list)
    """
    close_col = "종가" if "종가" in df.columns else "close"
    vol_col = "거래량" if "거래량" in df.columns else "volume"
    high_col = "고가" if "고가" in df.columns else "high"
    low_col = "저가" if "저가" in df.columns else "low"

    if vol_col not in df.columns or len(df) < 21:
        return 0, []

    vols = df[vol_col].values.astype(float)
    closes = df[close_col].values.astype(float)

    vol_20avg = float(np.mean(vols[-21:-1]))  # 오늘 제외 20일 평균
    if vol_20avg <= 0:
        return 0, []

    vol_spike = vols[-1] / vol_20avg
    signals = []

    # 거래량 2배 이상
    if vol_spike >= 2.0:
        signals.append(f"거래량 {vol_spike:.1f}x")

    # OBV 돌파: OBV 신고가인데 가격은 아직
    if len(closes) >= 21:
        obv = np.cumsum(np.sign(np.diff(closes[-21:])) * vols[-20:])
        if len(obv) > 0:
            obv_max_20 = float(np.max(obv[:-1])) if len(obv) > 1 else 0
            price_max_20 = float(np.max(closes[-21:-1]))
            if obv[-1] > obv_max_20 and closes[-1] < price_max_20:
                signals.append("OBV돌파(가격지연)")

    # 조용한 매집: 거래량 3x+ 인데 가격변동 3% 미만
    if vol_spike >= 3.0 and len(closes) >= 2:
        pct_chg = abs(closes[-1] / closes[-2] - 1) * 100
        if pct_chg < 3.0:
            signals.append("조용한매집")

    # 연속 거래량 증가 (3일)
    if len(vols) >= 4:
        if vols[-1] > vols[-2] > vols[-3]:
            signals.append("3일연속거래량증가")

    return vol_spike, signals


# ═══════════════════════════════════════════════════
#  Phase 2: 테마 매칭
# ═══════════════════════════════════════════════════

def _match_theme(code: str, name: str) -> tuple:
    """종목명/뉴스에서 테마 매칭

    Returns: (theme_name, theme_detail, leader_return_5d, theme_score)
    """
    best_theme = ""
    best_detail = ""
    best_leader_ret = 0.0
    best_score = 0

    # 1) 뉴스 키워드 매칭 (Google News RSS)
    try:
        from data.news_ai_scanner import fetch_naver_news_search
        news_items = fetch_naver_news_search(name, max_items=5)
        news_text = " ".join(n.title for n in news_items) if news_items else ""
    except Exception:
        news_text = ""

    for theme_name, theme_info in THEME_SUPPLY_CHAIN.items():
        matched_kw = []
        for kw in theme_info["keywords"]:
            if kw in name or kw in news_text:
                matched_kw.append(kw)

        if not matched_kw:
            continue

        # 대장주 5일 수익률 체크
        leader_ret = _get_leader_return(theme_info["대장"])
        theme_score = len(matched_kw) * 15 + max(0, leader_ret * 5)

        if theme_score > best_score:
            best_score = theme_score
            best_theme = theme_name
            best_detail = f"{theme_name}({','.join(matched_kw[:3])})"
            best_leader_ret = leader_ret

    return best_theme, best_detail, best_leader_ret, min(best_score, 100)


def _get_leader_return(leader_codes: list) -> float:
    """대장주 5일 수익률 (평균)"""
    returns = []
    for code in leader_codes:
        df = load_daily(code)
        if df is None or len(df) < 6:
            continue
        close_col = "종가" if "종가" in df.columns else "close"
        closes = df[close_col].values.astype(float)
        ret = (closes[-1] / closes[-6] - 1) * 100
        returns.append(ret)
    return float(np.mean(returns)) if returns else 0.0


# ═══════════════════════════════════════════════════
#  Phase 3: 기술 확인 + 과열 방지
# ═══════════════════════════════════════════════════

def _check_technicals(df: pd.DataFrame) -> tuple:
    """기술적 조건 확인

    Returns: (tech_score, rsi, atr_pct, signals)
    """
    close_col = "종가" if "종가" in df.columns else "close"
    high_col = "고가" if "고가" in df.columns else "high"
    low_col = "저가" if "저가" in df.columns else "low"

    closes = df[close_col].values.astype(float)
    highs = df[high_col].values.astype(float)
    lows = df[low_col].values.astype(float)

    signals = []
    score = 0

    # RSI
    rsi = calc_rsi(closes, 14)
    if 30 <= rsi <= 70:
        score += 30
        signals.append(f"RSI적정({rsi:.0f})")
    elif rsi > 70:
        score += 10  # 과열이지만 모멘텀 있음
        signals.append(f"RSI과열({rsi:.0f})")
    else:
        score += 15  # 과매도 반등 가능
        signals.append(f"RSI과매도({rsi:.0f})")

    # ATR% (변동성)
    atr = calc_atr(highs, lows, closes, 14)
    atr_pct = (atr / closes[-1] * 100) if closes[-1] > 0 else 0
    if atr_pct >= 2.0:
        score += 20
        signals.append(f"ATR{atr_pct:.1f}%")

    # MACD
    if len(closes) >= 26:
        ema12 = pd.Series(closes).ewm(span=12).mean().values
        ema26 = pd.Series(closes).ewm(span=26).mean().values
        macd = ema12 - ema26
        signal_line = pd.Series(macd).ewm(span=9).mean().values
        hist = macd - signal_line

        if hist[-1] > 0:
            score += 20
            if len(hist) >= 2 and hist[-2] <= 0:
                score += 10
                signals.append("MACD양전환")
            else:
                signals.append("MACD양봉")

    # 5일 등락률 (과열 방지)
    if len(closes) >= 6:
        chg_5d = (closes[-1] / closes[-6] - 1) * 100
        if chg_5d >= 30:
            score -= 20  # 이미 폭등 → 감점
            signals.append(f"5D+{chg_5d:.0f}%(과열)")
        elif chg_5d >= 10:
            score += 10  # 모멘텀 시작
            signals.append(f"5D+{chg_5d:.0f}%")
    else:
        chg_5d = 0

    return max(0, min(score, 100)), rsi, atr_pct, signals, chg_5d


# ═══════════════════════════════════════════════════
#  메인 스캔 함수
# ═══════════════════════════════════════════════════

def scan_momentum(top_n: int = 5) -> List[MomentumCandidate]:
    """소형주 급등 포착 스캔 — 3-Phase 파이프라인

    Phase 1: 거래량 폭발 (volume_scanner 패턴 재사용)
    Phase 2: 테마 매칭 (THEME_SUPPLY_CHAIN + 뉴스)
    Phase 3: 기술 확인 (RSI/MACD/ATR)

    Returns: 최대 top_n개 MomentumCandidate
    """
    print("=" * 60)
    print("  소형주 급등 포착 (Momentum Hunter)")
    print("=" * 60)

    universe = load_smallcap_universe()
    if not universe:
        print("  소형주 유니버스 없음 — build_smallcap_universe() 먼저 실행")
        return []

    codes = list(universe.keys())
    print(f"  유니버스: {len(codes)}개 (시총 300억~5000억)")

    phase1_pass = []  # 거래량 통과
    phase2_pass = []  # 테마 매칭
    candidates = []   # 최종 후보

    # ── Phase 1: 거래량 폭발 ──
    print(f"\n  [Phase 1] 거래량 폭발 필터...")
    for i, code in enumerate(codes):
        if (i + 1) % 200 == 0:
            print(f"    스캔 중... {i+1}/{len(codes)}")

        try:
            df = load_daily(code)
            if df is None or len(df) < 21:
                continue

            vol_spike, vol_signals = _check_volume_explosion(df)

            # 거래량 2배+ 또는 신호 1개+ 통과
            if vol_spike >= 2.0 or len(vol_signals) >= 1:
                phase1_pass.append({
                    "code": code,
                    "name": universe[code]["name"],
                    "cap_억": universe[code].get("cap_억", 0),
                    "df": df,
                    "vol_spike": vol_spike,
                    "vol_signals": vol_signals,
                })
        except Exception as e:
            logger.debug(f"Phase1 실패 {code}: {e}")

    print(f"    → {len(phase1_pass)}종목 통과")

    if not phase1_pass:
        print("  거래량 폭발 종목 없음")
        return []

    # ── Phase 2: 테마 매칭 ──
    print(f"\n  [Phase 2] 테마 매칭...")
    for item in phase1_pass:
        try:
            theme, theme_detail, leader_ret, theme_score = _match_theme(
                item["code"], item["name"]
            )

            # 테마 매칭 없어도 거래량 3x+ 이면 통과 (순수 모멘텀)
            if not theme and item["vol_spike"] < 3.0:
                continue

            item["theme"] = theme
            item["theme_detail"] = theme_detail
            item["leader_ret"] = leader_ret
            item["theme_score"] = theme_score
            phase2_pass.append(item)

        except Exception as e:
            logger.debug(f"Phase2 실패 {item['code']}: {e}")

    print(f"    → {len(phase2_pass)}종목 통과")

    if not phase2_pass:
        print("  테마 매칭 종목 없음")
        return []

    # ── Phase 3: 기술 확인 + 스코어링 ──
    print(f"\n  [Phase 3] 기술 확인 + 스코어링...")
    for item in phase2_pass:
        try:
            df = item["df"]
            tech_score, rsi, atr_pct, tech_signals, chg_5d = _check_technicals(df)

            # 과열 방지: 5일 +30% 이상이면 제외
            if chg_5d >= 30:
                continue

            # 종합 점수
            vol_score = min(item["vol_spike"] * 15, 30)  # 최대 30
            theme_score = min(item["theme_score"] * 0.3, 30)  # 최대 30
            leader_gap = max(0, item["leader_ret"] - chg_5d)  # 대장-소부장 갭
            gap_score = min(leader_gap * 4, 20)  # 최대 20
            t_score = min(tech_score * 0.2, 20)  # 최대 20

            momentum_score = vol_score + theme_score + gap_score + t_score

            # SL/TP 계산 (소형주 전용)
            close_col = "종가" if "종가" in df.columns else "close"
            high_col = "고가" if "고가" in df.columns else "high"
            low_col = "저가" if "저가" in df.columns else "low"
            closes = df[close_col].values.astype(float)
            highs = df[high_col].values.astype(float)
            lows = df[low_col].values.astype(float)

            entry = int(closes[-1])
            atr = calc_atr(highs, lows, closes, 14)
            sl = max(int(entry - atr * 1.0), int(entry * 0.90))  # ATR×1.0, 최대 -10%
            tp = int(entry + atr * 3.0)  # ATR×3.0 (급등 기대)

            all_signals = item["vol_signals"] + tech_signals

            candidates.append(MomentumCandidate(
                code=item["code"],
                name=item["name"],
                momentum_score=round(momentum_score, 1),
                theme=item["theme"],
                theme_detail=item["theme_detail"],
                volume_spike=round(item["vol_spike"], 1),
                price_change_5d=round(chg_5d, 1),
                leader_return_5d=round(item["leader_ret"], 1),
                leader_gap=round(leader_gap, 1),
                rsi=round(rsi, 0),
                atr_pct=round(atr_pct, 1),
                signals=all_signals,
                entry=entry,
                sl=sl,
                tp=tp,
                cap_억=item["cap_억"],
            ))

        except Exception as e:
            logger.debug(f"Phase3 실패 {item['code']}: {e}")

    # 점수 순 정렬
    candidates.sort(key=lambda c: c.momentum_score, reverse=True)
    result = candidates[:top_n]

    # 결과 출력
    print(f"\n  === 급등주 후보 TOP {len(result)} ===")
    for i, c in enumerate(result):
        print(
            f"  {i+1}. {c.name}({c.code}) 시총{c.cap_억}억\n"
            f"     점수: {c.momentum_score:.0f} | 테마: {c.theme_detail or '순수모멘텀'}\n"
            f"     거래량: {c.volume_spike:.1f}x | 5D: {c.price_change_5d:+.1f}%\n"
            f"     대장5D: {c.leader_return_5d:+.1f}% | 갭: {c.leader_gap:.1f}%\n"
            f"     RSI: {c.rsi:.0f} | ATR%: {c.atr_pct:.1f}%\n"
            f"     Entry: {c.entry:,} | SL: {c.sl:,} | TP: {c.tp:,}\n"
            f"     신호: {' + '.join(c.signals)}"
        )

    return result


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="소형주 급등 포착 스캐너")
    parser.add_argument("--top", type=int, default=5, help="상위 N개 (기본 5)")
    args = parser.parse_args()

    results = scan_momentum(args.top)
    if not results:
        print("\n  급등 후보 없음")
