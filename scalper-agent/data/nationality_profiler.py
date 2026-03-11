"""
외국인 국적별 행동 프로파일러 + 내일 수급 예측
─────────────────────────────────────────────
T-1 데이터로 T+1을 유추하는 방법:

전략 핵심:
  1. 연속성 모멘텀: 3일+ 연속 증가 국가 → 내일도 매수 확률 높음
  2. 기관국(영국,노르웨이) 방향: 꾸준하면 → 바닥 다짐 (안전)
  3. 헤지펀드(케이맨) 급증 후: 1~2일 후 빠질 수 있음 → 단기 경고
  4. 중국/홍콩 신규 진입: 관망→진입 전환 시 → 상승 초기 신호
  5. 복합 시그널: 기관+아시아 동시 증가 = 가장 강한 매수 신호

국가별 아키타입:
  🏛 패시브 기관 (영국,노르웨이,스위스): 천천히 움직이지만 방향 잡으면 오래감
  🦈 헤지펀드 (케이맨,버진): 빠르게 들어왔다 빠르게 나감 (1~5일)
  🐉 아시아 큰손 (중국,홍콩,싱가포르): 진입 느리지만 진입하면 추세 강함
  🦅 미국: 글로벌 방향성, ETF 리밸런싱 위주

사용:
  from data.nationality_profiler import analyze_nationality_behavior, predict_tomorrow_flow

  # 종목별 국가 행동 분석
  profiles = analyze_nationality_behavior(["000660", "005930"])

  # 내일 수급 예측 시그널
  signals = predict_tomorrow_flow(["000660", "005930"])

  # 텔레그램 보고
  report = format_prediction_report(signals, {"000660": "SK하이닉스"})

  # CLI
  python data/nationality_profiler.py 000660 005930 068270
"""

import json
import logging
import statistics
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── 국가 아키타입 분류 ──
ARCHETYPE = {
    # 패시브 기관 (방향 잡으면 오래감, 느림)
    "영국": ("기관", "🏛"),
    "노르웨이": ("기관", "🏛"),
    "스위스": ("기관", "🏛"),
    "스웨덴": ("기관", "🏛"),
    "아일랜드": ("기관", "🏛"),
    "룩셈부르크": ("기관", "🏛"),
    "덴마크": ("기관", "🏛"),
    # 헤지펀드 (빠른 진입/이탈, 1~5일)
    "케이맨 제도": ("헤지펀드", "🦈"),
    "영국령 버진아일랜드": ("헤지펀드", "🦈"),
    "버뮤다": ("헤지펀드", "🦈"),
    # 아시아 큰손 (진입 느리지만 추세 강함)
    "중국": ("아시아", "🐉"),
    "홍콩": ("아시아", "🐉"),
    "싱가포르": ("아시아", "🐉"),
    "대만": ("아시아", "🐉"),
    "일본": ("아시아", "🐉"),
    # 북미
    "미국": ("북미", "🦅"),
    "캐나다": ("북미", "🦅"),
}

# 분석 대상 주요국 (순서)
FOCUS_COUNTRIES = [
    "영국", "미국", "케이맨 제도", "싱가포르", "중국", "홍콩",
    "노르웨이", "일본", "스위스", "오스트레일리아", "프랑스",
]

# ── 행동 패턴 정의 ──
PATTERN_ACCUMULATING = "매집중"     # 3일+ 연속 증가
PATTERN_DUMPING = "이탈중"          # 3일+ 연속 감소
PATTERN_STEADY = "꾸준유지"         # 변동성 낮고 매일 매수 (기관형)
PATTERN_DAYTRADING = "단타형"       # 거래량 변동 극심
PATTERN_WATCHING = "관망"           # 간헐적 거래
PATTERN_ENTERING = "신규진입"       # 없다가 최근 등장
PATTERN_NORMAL = "일반매매"         # 위 어느 것에도 해당 안됨


# ═══════════════════════════════════════════
# 일별 데이터 수집
# ═══════════════════════════════════════════

def _get_trading_days(n_days: int = 5) -> List[str]:
    """최근 N 영업일 리스트 (주말 제외)"""
    days = []
    dt = datetime.now()
    # KRX 데이터는 T-1 → 오늘 18시 전이면 어제부터
    if dt.hour < 18:
        dt -= timedelta(days=1)
    while len(days) < n_days:
        if dt.weekday() < 5:  # 평일
            days.append(dt.strftime("%Y%m%d"))
        dt -= timedelta(days=1)
    return list(reversed(days))


def collect_daily_series(
    codes: List[str],
    n_days: int = 5,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """종목별 일별 국적별 거래량 시계열 수집

    Returns: {code: {YYYYMMDD: {국가명: 거래량}}}
    """
    from data.krx_nationality_crawler import (
        _get_valid_session, _fetch_nationality_http, REQUEST_INTERVAL,
    )

    days = _get_trading_days(n_days)
    logger.info(f"수집 기간: {days[0]}~{days[-1]} ({len(days)}일)")

    session = _get_valid_session()
    if not session:
        logger.error("KRX 세션 없음")
        return {}

    results = {}
    for code in codes:
        results[code] = {}
        for day in days:
            # 캐시 확인
            cache_path = DATA_DIR / f"{code}_{day}.csv"
            if cache_path.exists():
                try:
                    df = pd.read_csv(cache_path, encoding="utf-8-sig")
                    results[code][day] = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
                    continue
                except Exception:
                    pass

            # KRX API 수집
            for mkt in ["STK", "KSQ"]:
                df = _fetch_nationality_http(session, code, day, day, mkt)
                if df is None:
                    session = _get_valid_session()
                    if session:
                        df = _fetch_nationality_http(session, code, day, day, mkt)
                if df is not None and not df.empty:
                    data = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
                    results[code][day] = data
                    # 캐시 저장
                    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
                    break
                time.sleep(REQUEST_INTERVAL)
            time.sleep(REQUEST_INTERVAL)

        logger.info(f"{code}: {len(results[code])}/{len(days)}일 수집")

    return results


# ═══════════════════════════════════════════
# 국가별 행동 프로파일링
# ═══════════════════════════════════════════

def _classify_pattern(
    daily_vols: List[int],
    active_days: int,
    total_days: int,
) -> str:
    """일별 거래량 시계열 → 행동 패턴 분류"""
    if active_days == 0:
        return PATTERN_WATCHING

    # 최근 등장 (이전엔 없다가 마지막 1~2일에 진입)
    if active_days <= 2 and total_days >= 4:
        if daily_vols[-1] > 0 and sum(daily_vols[:-2]) == 0:
            return PATTERN_ENTERING

    if active_days <= 2:
        return PATTERN_WATCHING

    # 연속 증가/감소 (0 제외한 nonzero 기준)
    nonzero = [(i, v) for i, v in enumerate(daily_vols) if v > 0]
    if len(nonzero) >= 3:
        nz_vals = [v for _, v in nonzero]
        diffs = [nz_vals[i] - nz_vals[i - 1] for i in range(1, len(nz_vals))]
        inc_count = sum(1 for d in diffs if d > 0)
        dec_count = sum(1 for d in diffs if d < 0)

        # 3일+ 연속 증가
        if inc_count >= len(diffs) * 0.7 and len(diffs) >= 2:
            return PATTERN_ACCUMULATING
        # 3일+ 연속 감소
        if dec_count >= len(diffs) * 0.7 and len(diffs) >= 2:
            return PATTERN_DUMPING

        # 변동성 판단 (CV = 표준편차/평균)
        if len(nz_vals) >= 3:
            avg = statistics.mean(nz_vals)
            if avg > 0:
                cv = statistics.stdev(nz_vals) / avg
                if cv < 0.35:
                    return PATTERN_STEADY
                if cv > 0.8:
                    return PATTERN_DAYTRADING

    return PATTERN_NORMAL


def analyze_nationality_behavior(
    codes: List[str],
    n_days: int = 5,
    code_names: Dict[str, str] = None,
) -> Dict[str, List[dict]]:
    """종목별 국가 행동 프로파일 분석

    Returns: {code: [{"국가", "아키타입", "패턴", "일별거래량", "평균", "추세", "예측점수"}]}
    """
    daily_data = collect_daily_series(codes, n_days)

    profiles = {}
    for code in codes:
        daily = daily_data.get(code, {})
        if len(daily) < 2:
            profiles[code] = []
            continue

        sorted_days = sorted(daily.keys())
        all_countries = set()
        for d in daily.values():
            all_countries.update(d.keys())

        stock_profiles = []
        for country in FOCUS_COUNTRIES:
            if country not in all_countries:
                continue

            vols = [daily.get(day, {}).get(country, 0) for day in sorted_days]
            total = sum(vols)
            if total == 0:
                continue

            active_days = sum(1 for v in vols if v > 0)
            pattern = _classify_pattern(vols, active_days, len(sorted_days))

            arch_type, emoji = ARCHETYPE.get(country, ("기타", "🌐"))

            # 추세 점수 (-10 ~ +10): 양수=증가추세, 음수=감소추세
            nonzero = [v for v in vols if v > 0]
            if len(nonzero) >= 2:
                avg = statistics.mean(nonzero)
                # 최근 절반 vs 이전 절반 비교
                mid = len(nonzero) // 2
                if mid > 0:
                    early_avg = statistics.mean(nonzero[:mid])
                    late_avg = statistics.mean(nonzero[mid:])
                    if early_avg > 0:
                        trend = (late_avg - early_avg) / early_avg * 10
                        trend = max(-10, min(10, trend))
                    else:
                        trend = 5.0 if late_avg > 0 else 0
                else:
                    trend = 0
            else:
                avg = total
                trend = 0

            # 내일 예측 점수 (-20 ~ +20)
            pred_score = _calc_prediction_score(pattern, arch_type, trend, vols)

            stock_profiles.append({
                "국가": country,
                "아키타입": arch_type,
                "이모지": emoji,
                "패턴": pattern,
                "일별": vols,
                "날짜": sorted_days,
                "활성일": active_days,
                "총일수": len(sorted_days),
                "평균": int(avg),
                "추세": round(trend, 1),
                "예측점수": round(pred_score, 1),
            })

        # 예측점수 기준 정렬
        stock_profiles.sort(key=lambda x: x["예측점수"], reverse=True)
        profiles[code] = stock_profiles

    return profiles


def _calc_prediction_score(
    pattern: str,
    archetype: str,
    trend: float,
    daily_vols: List[int],
) -> float:
    """내일 매수 지속 확률 점수 (-20 ~ +20)

    핵심 로직:
      - 기관국이 꾸준하면 = 안전 바닥 (+5~10)
      - 아시아 큰손이 매집중이면 = 추세 초기 (+8~15)
      - 헤지펀드 급증이면 = 단기 +5, 하지만 3일 후 반전 위험
      - 이탈중이면 = 마이너스
    """
    score = 0.0

    # 패턴 기본 점수
    pattern_base = {
        PATTERN_ACCUMULATING: 8,
        PATTERN_STEADY: 6,
        PATTERN_ENTERING: 4,
        PATTERN_NORMAL: 1,
        PATTERN_DAYTRADING: -2,
        PATTERN_WATCHING: 0,
        PATTERN_DUMPING: -8,
    }
    score += pattern_base.get(pattern, 0)

    # 아키타입 보정
    if archetype == "기관":
        if pattern in (PATTERN_ACCUMULATING, PATTERN_STEADY):
            score += 4  # 기관이 매집 = 매우 긍정적
        elif pattern == PATTERN_DUMPING:
            score -= 3  # 기관 이탈 = 매우 부정적
    elif archetype == "아시아":
        if pattern in (PATTERN_ACCUMULATING, PATTERN_ENTERING):
            score += 5  # 중국/홍콩 진입 = 강한 추세 시작
        elif pattern == PATTERN_STEADY:
            score += 3  # 꾸준히 사고 있음
    elif archetype == "헤지펀드":
        if pattern == PATTERN_ACCUMULATING:
            score += 2  # 단기적 호재, 하지만 지속성 의문
            # 3일 이상이면 빠질 위험 경고
            active = sum(1 for v in daily_vols if v > 0)
            if active >= 4:
                score -= 3  # 헤지펀드 4일+ = 차익실현 임박
        elif pattern == PATTERN_DAYTRADING:
            score -= 3  # 변동성만 키움

    # 추세 반영 (±3)
    score += trend * 0.3

    return max(-20, min(20, score))


# ═══════════════════════════════════════════
# 내일 수급 예측 시그널
# ═══════════════════════════════════════════

def predict_tomorrow_flow(
    codes: List[str],
    n_days: int = 5,
    code_names: Dict[str, str] = None,
) -> List[dict]:
    """종목별 내일 외국인 수급 예측

    Returns: [{
        "code", "name",
        "signal": "STRONG_BUY" | "BUY" | "NEUTRAL" | "CAUTION" | "SELL",
        "score": -100~+100,
        "reason": "기관매집+아시아진입",
        "key_countries": [{국가, 패턴, 예측점수}],
        "risk": "헤지펀드 차익실현 임박" | None,
    }]
    """
    profiles = analyze_nationality_behavior(codes, n_days, code_names)

    predictions = []
    for code in codes:
        name = (code_names or {}).get(code, code)
        stock_profiles = profiles.get(code, [])
        if not stock_profiles:
            predictions.append({
                "code": code, "name": name,
                "signal": "NEUTRAL", "score": 0,
                "reason": "데이터부족", "key_countries": [], "risk": None,
            })
            continue

        # 총 예측점수 합산
        total_score = sum(p["예측점수"] for p in stock_profiles)

        # 아키타입별 합산
        arch_scores = {}
        for p in stock_profiles:
            arch = p["아키타입"]
            arch_scores[arch] = arch_scores.get(arch, 0) + p["예측점수"]

        # 시그널 결정
        reasons = []
        risks = []

        # 기관 방향
        inst_score = arch_scores.get("기관", 0)
        if inst_score >= 10:
            reasons.append("🏛기관매집")
        elif inst_score <= -5:
            risks.append("🏛기관이탈")

        # 아시아 큰손
        asia_score = arch_scores.get("아시아", 0)
        if asia_score >= 8:
            reasons.append("🐉아시아진입")
        elif asia_score <= -5:
            risks.append("🐉아시아이탈")

        # 헤지펀드
        hf_score = arch_scores.get("헤지펀드", 0)
        if hf_score >= 5:
            reasons.append("🦈헤지관심")
        elif hf_score <= -3:
            risks.append("🦈헤지이탈")
        # 헤지펀드 차익실현 경고
        for p in stock_profiles:
            if p["아키타입"] == "헤지펀드" and p["활성일"] >= 4:
                if p["패턴"] in (PATTERN_ACCUMULATING, PATTERN_STEADY):
                    risks.append("🦈차익실현임박")
                    break

        # 북미
        na_score = arch_scores.get("북미", 0)
        if na_score >= 5:
            reasons.append("🦅미국매수")

        # 복합 시그널 보너스
        if inst_score >= 5 and asia_score >= 5:
            total_score += 10  # 기관+아시아 = 최강 시그널
            reasons.append("⭐복합매수")
        if inst_score <= -5 and asia_score <= -5:
            total_score -= 10
            risks.append("⚠️복합이탈")

        # 시그널 등급 결정
        if total_score >= 25:
            signal = "STRONG_BUY"
        elif total_score >= 12:
            signal = "BUY"
        elif total_score >= -5:
            signal = "NEUTRAL"
        elif total_score >= -15:
            signal = "CAUTION"
        else:
            signal = "SELL"

        # 주요 국가 (예측점수 상위 5)
        key_countries = [
            {"국가": p["국가"], "패턴": p["패턴"],
             "이모지": p["이모지"], "예측점수": p["예측점수"]}
            for p in stock_profiles[:5]
        ]

        predictions.append({
            "code": code,
            "name": name,
            "signal": signal,
            "score": round(total_score, 1),
            "reason": " + ".join(reasons) if reasons else "특이사항 없음",
            "risk": " / ".join(risks) if risks else None,
            "key_countries": key_countries,
            "arch_scores": arch_scores,
        })

    # 점수 기준 정렬
    predictions.sort(key=lambda x: x["score"], reverse=True)
    return predictions


# ═══════════════════════════════════════════
# 텔레그램 보고 포맷
# ═══════════════════════════════════════════

SIGNAL_EMOJI = {
    "STRONG_BUY": "🔴",
    "BUY": "🟠",
    "NEUTRAL": "⚪",
    "CAUTION": "🟡",
    "SELL": "🔵",
}

SIGNAL_KR = {
    "STRONG_BUY": "강력매수",
    "BUY": "매수",
    "NEUTRAL": "중립",
    "CAUTION": "주의",
    "SELL": "매도",
}


def format_prediction_report(
    predictions: List[dict],
    code_names: Dict[str, str] = None,
) -> str:
    """텔레그램 보고용 포맷"""
    lines = []
    lines.append("🌍 외국인 국적별 수급 예측")
    lines.append(f"📅 기준: {datetime.now().strftime('%m/%d')} 장마감 → 내일 전망")
    lines.append("─" * 28)

    for pred in predictions:
        name = (code_names or {}).get(pred["code"], pred.get("name", pred["code"]))
        sig = pred["signal"]
        sig_emoji = SIGNAL_EMOJI.get(sig, "⚪")
        sig_kr = SIGNAL_KR.get(sig, sig)
        score = pred["score"]

        lines.append(f"\n{sig_emoji} {name} [{sig_kr}] (점수:{score:+.0f})")

        # 매수 근거
        if pred["reason"] != "특이사항 없음":
            lines.append(f"  근거: {pred['reason']}")

        # 리스크
        if pred.get("risk"):
            lines.append(f"  ⚠️ {pred['risk']}")

        # 주요국 패턴 (상위 3)
        for kc in pred.get("key_countries", [])[:3]:
            lines.append(
                f"  {kc['이모지']}{kc['국가']}: {kc['패턴']} "
                f"({kc['예측점수']:+.0f}점)"
            )

    lines.append("\n" + "─" * 28)
    lines.append("🔴강력매수 🟠매수 ⚪중립 🟡주의 🔵매도")
    lines.append("※ 국적별 수급은 T-1 기준, 예측 참고용")

    return "\n".join(lines)


# ═══════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # 기본 종목: WAR TOP + 주요
    default_targets = {
        "068270": "셀트리온",
        "000660": "SK하이닉스",
        "196170": "알테오젠",
        "373220": "LG에너지솔루션",
        "005930": "삼성전자",
        "065450": "RFHIC",
        "055550": "신한지주",
        "105560": "KB금융",
    }

    if len(sys.argv) > 1:
        codes = sys.argv[1:]
        code_names = {c: c for c in codes}
    else:
        codes = list(default_targets.keys())
        code_names = default_targets

    print(f"\n{'=' * 50}")
    print(f"  외국인 국적별 수급 예측 ({len(codes)}종목)")
    print(f"{'=' * 50}\n")

    predictions = predict_tomorrow_flow(codes, n_days=5, code_names=code_names)
    report = format_prediction_report(predictions, code_names)
    print(report)

    # JSON 저장
    out_path = Path(__file__).parent.parent / "data_store" / "nationality_prediction.json"
    safe_preds = []
    for p in predictions:
        sp = {k: v for k, v in p.items()}
        sp["arch_scores"] = {k: float(v) for k, v in sp.get("arch_scores", {}).items()}
        safe_preds.append(sp)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(safe_preds, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n저장: {out_path}")
