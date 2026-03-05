# -*- coding: utf-8 -*-
"""
NIGHTWATCH — 3단 예측 체인 야간매매 신호 엔진
═══════════════════════════════════════════════
미국장 개장 7시간 전에 방향을 읽는다.

[1단] 아시안 리스크 스코어 (09:00~15:30 KST)
  - AUD/JPY: 위험자산 온도계
  - CNH: 차이나머니 방향
  - KOSPI 외국인 순매매

[2단] 유럽 오픈 스코어 (16:00~16:30 KST) ★핵심★
  - DAX 첫 30분 등락률
  - EUR/USD: 달러 수요
  - HYG: 크레딧 스프레드

[3단] 괴리 감지 (Divergence Detection)
  - HYG vs ES 방향 불일치
  - VIX 레벨 + 급변
  - 채권(TNX) vs 금(GC) 방향 불일치

최종: -10 ~ +10 점수 → 5단계 신호
  🟢🟢 강매수 / 🟢 매수 / 🟡 관망 / 🔴 금지 / 💀 패닉

사용:
  python -m data.nightwatch           # 전체 실행
  python -m data.nightwatch --test    # 테스트 (캐시된 데이터)
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
REPORT_PATH = DATA_DIR / "nightwatch_report.json"

# ═══════════════════════════════════════════════════
#  yfinance 지연 import (설치 안 되어도 에러 안 남)
# ═══════════════════════════════════════════════════
_yf = None

def _get_yf():
    global _yf
    if _yf is None:
        try:
            import yfinance as yf
            _yf = yf
        except ImportError:
            logger.error("yfinance 미설치. pip install yfinance")
            raise
    return _yf


# ═══════════════════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════════════════
@dataclass
class IndicatorData:
    """단일 지표 데이터"""
    name: str
    value: Optional[float] = None
    change_pct: Optional[float] = None
    change_abs: Optional[float] = None
    signal: str = "⬜"  # 🟢 🟡 🔴 ⬜
    error: str = ""


@dataclass
class NightwatchReport:
    """NIGHTWATCH 최종 리포트"""
    timestamp: str = ""
    # 1단: 아시안 리스크
    asian_score: float = 0.0
    asian_detail: Dict = field(default_factory=dict)
    # 2단: 유럽 오픈
    europe_score: float = 0.0
    europe_detail: Dict = field(default_factory=dict)
    # 3단: 괴리 감지
    divergence_score: float = 0.0
    divergences: List[str] = field(default_factory=list)
    # 최종
    total_score: float = 0.0
    signal: str = "🟡"
    signal_text: str = "관망"
    recommended_sectors: List[str] = field(default_factory=list)
    # 원본 데이터
    raw_indicators: Dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════
#  yfinance 유틸리티
# ═══════════════════════════════════════════════════
def _fetch_ticker(symbol: str, period: str = "5d") -> Optional[IndicatorData]:
    """yfinance에서 티커 데이터 가져오기 (최근 2일 비교)"""
    yf = _get_yf()
    name = symbol.replace("=X", "").replace("=F", "").replace("^", "")
    try:
        data = yf.Ticker(symbol).history(period=period)
        if data is None or len(data) < 2:
            return IndicatorData(name=name, error="데이터 부족")

        prev = float(data["Close"].iloc[-2])
        curr = float(data["Close"].iloc[-1])

        if prev == 0:
            return IndicatorData(name=name, value=curr, error="전일가 0")

        pct = ((curr - prev) / prev) * 100
        abs_chg = curr - prev

        return IndicatorData(
            name=name,
            value=round(curr, 4),
            change_pct=round(pct, 3),
            change_abs=round(abs_chg, 4),
        )
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] {symbol} 수집 실패: {e}")
        return IndicatorData(name=name, error=str(e))


def _fetch_intraday_change(symbol: str, minutes: int = 30) -> Optional[IndicatorData]:
    """장중 첫 N분 등락률 (유럽 오픈 30분용)"""
    yf = _get_yf()
    name = symbol.replace("=X", "").replace("=F", "").replace("^", "")
    try:
        data = yf.Ticker(symbol).history(period="1d", interval="5m")
        if data is None or len(data) < 2:
            return IndicatorData(name=name, error="인트라데이 데이터 부족")

        # 첫 N분 = 첫 (N/5)개 봉
        n_bars = max(1, minutes // 5)
        if len(data) < n_bars + 1:
            n_bars = len(data) - 1

        open_price = float(data["Open"].iloc[0])
        close_price = float(data["Close"].iloc[min(n_bars, len(data) - 1)])

        if open_price == 0:
            return IndicatorData(name=name, error="시가 0")

        pct = ((close_price - open_price) / open_price) * 100
        return IndicatorData(
            name=name,
            value=round(close_price, 2),
            change_pct=round(pct, 3),
        )
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] {symbol} 인트라데이 실패: {e}")
        return IndicatorData(name=name, error=str(e))


# ═══════════════════════════════════════════════════
#  [1단] 아시안 리스크 스코어
# ═══════════════════════════════════════════════════
def collect_asian_risk() -> Tuple[float, Dict]:
    """
    아시안 세션 위험자산 온도 측정
    AUD/JPY + CNH + KOSPI 외인 수급
    Returns: (score, detail_dict)
    """
    score = 0.0
    detail = {}

    # --- AUD/JPY: 핵심 리스크 바로미터 ---
    audjpy = _fetch_ticker("AUDJPY=X")
    if audjpy and audjpy.change_pct is not None:
        pct = audjpy.change_pct
        if pct > 0.3:
            audjpy.signal = "🟢"
            score += 2.0
        elif pct > 0.1:
            audjpy.signal = "🟢"
            score += 1.0
        elif pct < -0.3:
            audjpy.signal = "🔴"
            score -= 2.0
        elif pct < -0.1:
            audjpy.signal = "🔴"
            score -= 1.0
        else:
            audjpy.signal = "🟡"
    detail["AUD/JPY"] = asdict(audjpy) if audjpy else {}

    # --- CNH (위안화): 차이나머니 ---
    # CNY=X = 1USD = X위안 → 하락 = 위안화 강세 = 긍정
    cnh = _fetch_ticker("CNY=X")
    if cnh and cnh.change_pct is not None:
        pct = cnh.change_pct
        if pct < -0.2:  # 위안화 강세 (CNY=X 하락)
            cnh.signal = "🟢"
            score += 1.5
        elif pct > 0.2:  # 위안화 약세 (CNY=X 상승)
            cnh.signal = "🔴"
            score -= 1.5
        else:
            cnh.signal = "🟡"
    detail["CNH"] = asdict(cnh) if cnh else {}

    # --- KOSPI 외국인 순매매 (기존 모듈 재활용) ---
    try:
        from data.nationality_signal import score_nationality
        # KODEX200 (069500) 대표로 시장 전체 외인 동향 파악
        raw_score = score_nationality("069500")
        # score_nationality가 tuple 반환할 수 있음 → float 추출
        nat_score = float(raw_score[0]) if isinstance(raw_score, (tuple, list)) else float(raw_score)
        foreign_signal = "🟢" if nat_score > 0 else ("🔴" if nat_score < 0 else "🟡")
        score += nat_score * 0.5  # 가중치 0.5
        detail["KOSPI_외인"] = {"score": nat_score, "signal": foreign_signal}
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] 외인 수급 실패: {e}")
        detail["KOSPI_외인"] = {"score": 0, "signal": "⬜", "error": str(e)}

    return round(score, 1), detail


# ═══════════════════════════════════════════════════
#  [2단] 유럽 오픈 스코어 ★핵심★
# ═══════════════════════════════════════════════════
def collect_europe_open() -> Tuple[float, Dict]:
    """
    유럽장 오픈 첫 30분 방향 측정
    DAX + EUR/USD + HYG
    Returns: (score, detail_dict)
    """
    score = 0.0
    detail = {}

    # --- DAX 첫 30분: 유럽 기관의 첫 포지션 ★ ---
    dax = _fetch_intraday_change("^GDAXI", minutes=30)
    if dax and dax.change_pct is not None:
        pct = dax.change_pct
        if pct > 0.5:
            dax.signal = "🟢"
            score += 3.0  # DAX 가중치 최대
        elif pct > 0.2:
            dax.signal = "🟢"
            score += 1.5
        elif pct < -0.5:
            dax.signal = "🔴"
            score -= 3.0
        elif pct < -0.2:
            dax.signal = "🔴"
            score -= 1.5
        else:
            dax.signal = "🟡"
    detail["DAX_30min"] = asdict(dax) if dax else {}

    # --- EUR/USD: 달러 약세 = 위험자산 우호 ---
    eurusd = _fetch_ticker("EURUSD=X")
    if eurusd and eurusd.change_pct is not None:
        pct = eurusd.change_pct
        if pct > 0.3:  # 유로 강세 = 달러 약세
            eurusd.signal = "🟢"
            score += 1.0
        elif pct < -0.3:  # 유로 약세 = 달러 강세
            eurusd.signal = "🔴"
            score -= 1.0
        else:
            eurusd.signal = "🟡"
    detail["EUR/USD"] = asdict(eurusd) if eurusd else {}

    # --- HYG: 크레딧 스프레드 (가장 빠른 스트레스 감지) ---
    hyg = _fetch_ticker("HYG")
    if hyg and hyg.change_pct is not None:
        pct = hyg.change_pct
        if pct > 0.3:
            hyg.signal = "🟢"
            score += 1.5
        elif pct < -0.3:
            hyg.signal = "🔴"
            score -= 2.0  # HYG 하락은 더 강한 경고
        else:
            hyg.signal = "🟡"
    detail["HYG"] = asdict(hyg) if hyg else {}

    return round(score, 1), detail


# ═══════════════════════════════════════════════════
#  [3단] 괴리 감지 (Divergence Detection)
# ═══════════════════════════════════════════════════
def detect_divergence() -> Tuple[float, List[str], Dict]:
    """
    시장 간 괴리 감지 — 진짜 엣지
    Returns: (penalty_score, divergence_list, raw_data)
    """
    penalty = 0.0
    divergences = []
    raw = {}

    # --- VIX 레벨 ---
    vix = _fetch_ticker("^VIX")
    if vix and vix.value is not None:
        raw["VIX"] = asdict(vix)
        if vix.value >= 30:
            vix.signal = "💀"
            penalty -= 3.0
            divergences.append(f"💀 VIX {vix.value:.1f} — 패닉 구간")
        elif vix.value >= 25:
            vix.signal = "🔴"
            penalty -= 2.0
            divergences.append(f"🔴 VIX {vix.value:.1f} — 공포 구간")
        elif vix.value >= 20:
            vix.signal = "🟡"
            penalty -= 0.5
        else:
            vix.signal = "🟢"
            penalty += 0.5

    # --- ES 선물 (S&P500) ---
    es = _fetch_ticker("ES=F")
    raw["ES"] = asdict(es) if es else {}

    # --- 괴리 1: HYG 하락 + ES 안정 = 숨은 스트레스 ---
    hyg = _fetch_ticker("HYG")
    raw["HYG_div"] = asdict(hyg) if hyg else {}
    if (hyg and hyg.change_pct is not None and
            es and es.change_pct is not None):
        if hyg.change_pct < -0.3 and es.change_pct > -0.2:
            penalty -= 2.0
            divergences.append(
                f"⚠️ HYG {hyg.change_pct:+.2f}% vs ES {es.change_pct:+.2f}%"
                f" — 숨은 크레딧 스트레스"
            )

    # --- 괴리 2: 채권 상승(금리 하락) + 금 하락 = 위험선호 전환 ---
    tnx = _fetch_ticker("^TNX")
    gold = _fetch_ticker("GC=F")
    raw["TNX"] = asdict(tnx) if tnx else {}
    raw["GOLD"] = asdict(gold) if gold else {}
    if (tnx and tnx.change_abs is not None and
            gold and gold.change_pct is not None):
        if tnx.change_abs < -0.03 and gold.change_pct < -0.3:
            penalty += 1.5
            divergences.append(
                f"💡 TNX {tnx.change_abs:+.3f} + Gold {gold.change_pct:+.2f}%"
                f" — 스마트머니 위험선호 전환"
            )
        elif tnx.change_abs > 0.05 and gold.change_pct > 0.3:
            penalty -= 1.5
            divergences.append(
                f"🔴 TNX {tnx.change_abs:+.3f} + Gold {gold.change_pct:+.2f}%"
                f" — 안전자산 쏠림"
            )

    # --- 괴리 3: 원/달러 급등 ---
    usdkrw = _fetch_ticker("KRW=X")
    raw["USDKRW"] = asdict(usdkrw) if usdkrw else {}
    if usdkrw and usdkrw.change_pct is not None:
        if usdkrw.change_pct > 0.5:  # 원화 약세 급등
            penalty -= 1.0
            divergences.append(
                f"🔴 원/달러 {usdkrw.change_pct:+.2f}% — 원화 급락"
            )
        elif usdkrw.change_pct < -0.3:  # 원화 강세
            penalty += 0.5

    return round(penalty, 1), divergences, raw


# ═══════════════════════════════════════════════════
#  최종 점수 계산
# ═══════════════════════════════════════════════════
def calculate_nightwatch_score(
    asian_score: float,
    asian_detail: Dict,
    europe_score: float,
    europe_detail: Dict,
    div_score: float,
    divergences: List[str],
    raw_indicators: Dict,
) -> NightwatchReport:
    """3단 체인 종합 → 최종 리포트"""
    total = asian_score + europe_score + div_score
    total = max(-10.0, min(10.0, total))

    # 신호 변환
    if total >= 5:
        signal, text = "🟢🟢", "강한 매수"
    elif total >= 2:
        signal, text = "🟢", "매수 고려"
    elif total >= -1:
        signal, text = "🟡", "관망"
    elif total >= -4:
        signal, text = "🔴", "진입 금지"
    else:
        signal, text = "💀", "전체 포지션 점검"

    # 추천 섹터
    sectors = []
    if total >= 5:
        sectors.extend(["반도체 (SK하이닉스, 삼성전자)", "방산 (한화에어로, 현대로템)"])
    if total >= 2:
        sectors.append("조선 (HD현대중공업)")
    # CNH 강세 시 2차전지
    cnh = asian_detail.get("CNH", {})
    if cnh.get("change_pct") and cnh["change_pct"] > 0.2:
        sectors.append("2차전지 (POSCO홀딩스, 에코프로비엠)")
    if total < -2:
        sectors.append("인버스 (KODEX 200선물인버스2X)")

    return NightwatchReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        asian_score=asian_score,
        asian_detail=asian_detail,
        europe_score=europe_score,
        europe_detail=europe_detail,
        divergence_score=div_score,
        divergences=divergences,
        total_score=round(total, 1),
        signal=signal,
        signal_text=text,
        recommended_sectors=sectors,
        raw_indicators=raw_indicators,
    )


# ═══════════════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════════════
def run_nightwatch() -> NightwatchReport:
    """NIGHTWATCH 전체 실행"""
    logger.info("[NIGHTWATCH] 실행 시작")

    # 1단: 아시안 리스크
    logger.info("[NIGHTWATCH] [1단] 아시안 리스크 수집...")
    asian_score, asian_detail = collect_asian_risk()
    logger.info(f"[NIGHTWATCH] 아시안 스코어: {asian_score:+.1f}")

    # 2단: 유럽 오픈
    logger.info("[NIGHTWATCH] [2단] 유럽 오픈 수집...")
    europe_score, europe_detail = collect_europe_open()
    logger.info(f"[NIGHTWATCH] 유럽 스코어: {europe_score:+.1f}")

    # 3단: 괴리 감지
    logger.info("[NIGHTWATCH] [3단] 괴리 감지...")
    div_score, divergences, raw = detect_divergence()
    logger.info(f"[NIGHTWATCH] 괴리 스코어: {div_score:+.1f}, 감지: {len(divergences)}건")

    # 종합
    report = calculate_nightwatch_score(
        asian_score, asian_detail,
        europe_score, europe_detail,
        div_score, divergences, raw,
    )

    # 저장
    save_nightwatch_report(report)
    logger.info(f"[NIGHTWATCH] 완료 | {report.signal} {report.signal_text} | "
                f"점수: {report.total_score:+.1f}")

    return report


# ═══════════════════════════════════════════════════
#  저장 / 로드
# ═══════════════════════════════════════════════════
def save_nightwatch_report(report: NightwatchReport):
    """리포트 JSON 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2)
    logger.info(f"[NIGHTWATCH] 저장: {REPORT_PATH}")


def load_nightwatch_report() -> Optional[NightwatchReport]:
    """저장된 리포트 로드"""
    if not REPORT_PATH.exists():
        return None
    try:
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return NightwatchReport(**data)
    except Exception as e:
        logger.warning(f"[NIGHTWATCH] 로드 실패: {e}")
        return None


# ═══════════════════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════════════════
def format_nightwatch_report(report: NightwatchReport) -> str:
    """텔레그램 발송용 메시지 포맷"""

    def _sig(d: dict) -> str:
        return d.get("signal", "⬜")

    def _pct(d: dict) -> str:
        v = d.get("change_pct")
        return f"{v:+.2f}%" if v is not None else "N/A"

    def _val(d: dict) -> str:
        v = d.get("value")
        return f"{v}" if v is not None else "N/A"

    ad = report.asian_detail
    ed = report.europe_detail
    ri = report.raw_indicators

    lines = [
        f"NIGHTWATCH | {report.timestamp}",
        "=" * 34,
        "",
        "[1단] 아시안 리스크  ({:+.1f})".format(report.asian_score),
        f"  {_sig(ad.get('AUD/JPY', {}))} AUD/JPY: {_pct(ad.get('AUD/JPY', {}))}",
        f"  {_sig(ad.get('CNH', {}))} CNH: {_val(ad.get('CNH', {}))} ({_pct(ad.get('CNH', {}))})",
        f"  {ad.get('KOSPI_외인', {}).get('signal', '⬜')} 외인: {ad.get('KOSPI_외인', {}).get('score', 0):+.1f}",
        "",
        "[2단] 유럽 오픈  ({:+.1f})  ★".format(report.europe_score),
        f"  {_sig(ed.get('DAX_30min', {}))} DAX 30분: {_pct(ed.get('DAX_30min', {}))}",
        f"  {_sig(ed.get('EUR/USD', {}))} EUR/USD: {_pct(ed.get('EUR/USD', {}))}",
        f"  {_sig(ed.get('HYG', {}))} HYG: {_pct(ed.get('HYG', {}))}",
        "",
        "[3단] 괴리 감지  ({:+.1f})".format(report.divergence_score),
    ]

    # VIX
    vix_d = ri.get("VIX", {})
    lines.append(f"  {vix_d.get('signal', '⬜')} VIX: {_val(vix_d)}")

    # 괴리 항목
    if report.divergences:
        for dv in report.divergences:
            lines.append(f"  {dv}")
    else:
        lines.append("  괴리 없음")

    lines.extend([
        "",
        "=" * 34,
        f"종합: {report.total_score:+.1f} / 10",
        f"{report.signal} {report.signal_text}",
        "",
    ])

    if report.recommended_sectors:
        lines.append("주목 섹터:")
        for s in report.recommended_sectors:
            lines.append(f"  - {s}")
    else:
        lines.append("주목 섹터: 없음 (관망/금지)")

    lines.append("=" * 34)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    report = run_nightwatch()
    msg = format_nightwatch_report(report)
    print()
    print(msg)
