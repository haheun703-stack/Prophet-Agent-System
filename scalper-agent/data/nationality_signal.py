"""
외국인 국적별 수급 시그널 & 텔레그램 보고
─────────────────────────────────────────────
KRX HARD053 국적별 거래량 → 일별 스냅샷 저장 → 전일 대비 변화 분석

기능:
  1. 일별 스냅샷 저장/로드  (nationality/{code}_{YYYYMMDD}.csv)
  2. 전일 대비 변화 계산     (T-1 vs T-2)
  3. 텔레그램 보고 포맷      (국가별 증감 + 해석)
  4. 추천 파이프라인 점수     (기관국 매집 = +점, 이탈 = -점)

국가 분류:
  기관국 (패시브): 영국, 노르웨이, 스위스, 스웨덴, 아일랜드, 룩셈부르크
  헤지펀드국:      케이맨 제도, 영국령 버진아일랜드, 버뮤다
  아시아머니:      중국, 홍콩, 싱가포르, 일본
  북미:           미국, 캐나다
  기타:           프랑스, 독일, 호주, 말레이시아 등

사용:
  from data.nationality_signal import generate_nationality_report, score_nationality
  report = generate_nationality_report(["065450", "103140"])
  score = score_nationality("065450")
"""

import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from data.trading_calendar import last_trading_day
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── 국가 분류 ──
INSTITUTIONAL = {"영국", "노르웨이", "스위스", "스웨덴", "아일랜드", "룩셈부르크", "덴마크"}
HEDGE_FUND = {"케이맨 제도", "영국령 버진아일랜드", "버뮤다"}
ASIA_MONEY = {"중국", "홍콩", "싱가포르", "일본", "대만"}
NORTH_AMERICA = {"미국", "캐나다"}

# 주요국 (보고 대상) - 순서대로 표시
MAJOR_COUNTRIES = [
    "영국", "미국", "케이맨 제도", "싱가포르", "중국", "홍콩",
    "노르웨이", "스위스", "오스트레일리아", "프랑스", "일본",
    "스웨덴", "캐나다", "영국령 버진아일랜드", "말레이시아",
]


# ═══════════════════════════════════════════
# 일별 스냅샷 저장/로드
# ═══════════════════════════════════════════

def save_daily_snapshot(code: str, date: str, data: dict):
    """일별 국적별 거래량 스냅샷 저장

    Args:
        code: 종목코드
        date: YYYYMMDD
        data: {국가명: 거래량}
    """
    if not data:
        return
    df = pd.DataFrame(
        [{"국가명": k, "거래량": v} for k, v in data.items()]
    ).sort_values("거래량", ascending=False).reset_index(drop=True)
    path = DATA_DIR / f"{code}_{date}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")


def load_daily_snapshot(code: str, date: str) -> Optional[dict]:
    """일별 스냅샷 로드 → {국가명: 거래량}"""
    path = DATA_DIR / f"{code}_{date}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        return dict(zip(df["국가명"], df["거래량"]))
    except Exception:
        return None


def _find_prev_trading_day(date_str: str, max_lookback: int = 7) -> Optional[str]:
    """이전 영업일 찾기 (스냅샷 존재하는 가장 최근 날짜)"""
    dt = datetime.strptime(date_str, "%Y%m%d")
    for i in range(1, max_lookback + 1):
        prev = (dt - timedelta(days=i)).strftime("%Y%m%d")
        # 스냅샷 파일 패턴으로 확인 (아무 종목이라도 있으면 영업일)
        snapshots = list(DATA_DIR.glob(f"*_{prev}.csv"))
        if snapshots:
            return prev
    return None


# ═══════════════════════════════════════════
# KRX 데이터 수집 (단일일 + 스냅샷 저장)
# ═══════════════════════════════════════════

def collect_daily_snapshots(
    codes: List[str],
    date: str = None,
) -> Dict[str, dict]:
    """여러 종목의 단일일 국적별 거래량 수집 + 스냅샷 저장

    Args:
        codes: 종목코드 리스트
        date: 수집일 YYYYMMDD (기본: 전일 - T-1 지연 감안)

    Returns: {code: {국가명: 거래량}}
    """
    from data.krx_nationality_crawler import (
        _get_valid_session, _fetch_nationality_http, REQUEST_INTERVAL,
    )

    if not date:
        # KRX 국적 데이터는 1일 지연 → 전일 기준
        date = _get_latest_data_date()

    session = _get_valid_session()
    if not session:
        logger.error("KRX 세션 없음")
        return {}

    results = {}
    for i, code in enumerate(codes):
        # 캐시 확인
        cached = load_daily_snapshot(code, date)
        if cached:
            results[code] = cached
            continue

        # 수집
        for mkt in ["STK", "KSQ"]:
            df = _fetch_nationality_http(session, code, date, date, mkt)
            if df is None:
                session = _get_valid_session()
                if session:
                    df = _fetch_nationality_http(session, code, date, date, mkt)
            if df is not None and not df.empty:
                data = dict(zip(df["국가명"], df["거래량"]))
                save_daily_snapshot(code, date, data)
                results[code] = data
                break
            time.sleep(REQUEST_INTERVAL)

        if i < len(codes) - 1:
            time.sleep(REQUEST_INTERVAL)

    return results


def _get_latest_data_date() -> str:
    """KRX 국적 데이터 최신 가용일 추정 (T-1)

    - 장 마감(15:30) 후 당일 데이터 반영 시점 불확실
    - 안전하게 전 영업일 기준
    """
    now = datetime.now()
    if now.hour < 18:
        # 장중/장마감 직후 → 직전 거래일
        return last_trading_day(now.date()).strftime("%Y%m%d")
    else:
        # 18시 이후 → 당일 가능 (시도)
        return now.strftime("%Y%m%d")


# ═══════════════════════════════════════════
# 전일 대비 변화 계산
# ═══════════════════════════════════════════

def compare_nationality(
    code: str,
    date_new: str = None,
    date_old: str = None,
) -> Optional[List[dict]]:
    """전일 대비 국적별 거래량 변화 계산

    Returns: [{"국가": str, "전일": int, "금일": int, "변화": int, "변화율": float, "분류": str}]
    """
    if not date_new:
        date_new = _get_latest_data_date()

    data_new = load_daily_snapshot(code, date_new)
    if not data_new:
        return None

    # 이전 영업일 스냅샷 찾기
    if not date_old:
        date_old = _find_prev_trading_day(date_new)
    if not date_old:
        return None

    data_old = load_daily_snapshot(code, date_old)
    if not data_old:
        return None

    # 변화 계산
    all_countries = set(list(data_new.keys()) + list(data_old.keys()))
    changes = []
    for country in all_countries:
        v_old = data_old.get(country, 0)
        v_new = data_new.get(country, 0)
        delta = v_new - v_old
        pct = (delta / v_old * 100) if v_old > 0 else (100.0 if v_new > 0 else 0)

        # 국가 분류
        if country in INSTITUTIONAL:
            cat = "기관"
        elif country in HEDGE_FUND:
            cat = "헤지펀드"
        elif country in ASIA_MONEY:
            cat = "아시아"
        elif country in NORTH_AMERICA:
            cat = "북미"
        else:
            cat = "기타"

        changes.append({
            "국가": country,
            "전일": v_old,
            "금일": v_new,
            "변화": delta,
            "변화율": pct,
            "분류": cat,
        })

    # 변화량 절대값 기준 정렬
    changes.sort(key=lambda x: abs(x["변화"]), reverse=True)
    return changes


# ═══════════════════════════════════════════
# 텔레그램 보고 포맷
# ═══════════════════════════════════════════

def _format_volume(vol: int) -> str:
    """거래량 → 읽기 편한 포맷 (만주 단위)"""
    if abs(vol) >= 10000:
        return f"{vol / 10000:.1f}만"
    elif abs(vol) >= 1000:
        return f"{vol / 1000:.1f}천"
    return str(vol)


def _country_emoji(category: str) -> str:
    """국가 분류별 이모지"""
    return {
        "기관": "🏛",
        "헤지펀드": "🦈",
        "아시아": "🐉",
        "북미": "🦅",
        "기타": "🌐",
    }.get(category, "🌐")


def format_nationality_report(
    code: str,
    name: str = "",
    changes: List[dict] = None,
    date_new: str = None,
    date_old: str = None,
) -> str:
    """단일 종목 국적별 수급 보고 텔레그램 포맷

    Returns:
        🌍 빅텍(065450) 국적별 수급 (3/3 vs 2/27)
        ─────────────────
        🏛 영국     +135.1만 (+125%) ⬆ 기관 매집
        🦈 케이맨    +128.1만 (+328%) ⬆ 헤지펀드 급증
        🐉 중국      +7.3만 (+658%) ⬆ 관심 급증
        🦅 미국      +0.2만 (NEW)    ⬆ 신규 진입
        ─────────────────
        📊 기관국 +150만 | 헤지펀드 +130만 | 아시아 +9만
    """
    if changes is None:
        changes = compare_nationality(code, date_new, date_old)
    if not changes:
        return f"🌍 {name or code} - 비교 데이터 없음"

    if not date_new:
        date_new = _get_latest_data_date()
    if not date_old:
        date_old = _find_prev_trading_day(date_new) or "?"

    # 날짜 포맷 (M/D)
    d_new = f"{int(date_new[4:6])}/{int(date_new[6:8])}" if len(date_new) >= 8 else date_new
    d_old = f"{int(date_old[4:6])}/{int(date_old[6:8])}" if len(date_old) >= 8 else date_old

    header = f"🌍 {name}({code}) 국적별 수급 ({d_new} vs {d_old})"
    lines = [header, "─" * 20]

    # 주요 변화 (상위 8국가, 변화량 있는 것만)
    shown = 0
    for ch in changes:
        if shown >= 8:
            break
        if ch["변화"] == 0 and ch["금일"] == 0:
            continue

        country = ch["국가"]
        delta = ch["변화"]
        pct = ch["변화율"]
        cat = ch["분류"]
        emoji = _country_emoji(cat)

        # 방향 화살표
        if delta > 0:
            arrow = "⬆"
        elif delta < 0:
            arrow = "⬇"
        else:
            arrow = "➡"

        # 변화 해석
        vol_str = _format_volume(delta)
        if ch["전일"] == 0 and ch["금일"] > 0:
            pct_str = "NEW"
            comment = "신규 진입"
        elif abs(pct) >= 200:
            pct_str = f"{pct:+.0f}%"
            comment = "급증" if delta > 0 else "급감"
        elif abs(pct) >= 50:
            pct_str = f"{pct:+.0f}%"
            comment = "매집" if delta > 0 else "이탈"
        else:
            pct_str = f"{pct:+.0f}%"
            comment = ""

        sign = "+" if delta >= 0 else ""
        line = f"{emoji} {country:8s} {sign}{vol_str:>8s} ({pct_str})"
        if comment:
            line += f" {arrow} {comment}"
        else:
            line += f" {arrow}"
        lines.append(line)
        shown += 1

    # 분류별 합산
    lines.append("─" * 20)
    cat_sums = {}
    for ch in changes:
        cat = ch["분류"]
        cat_sums[cat] = cat_sums.get(cat, 0) + ch["변화"]

    summary_parts = []
    for cat_name, cat_emoji in [("기관", "🏛"), ("헤지펀드", "🦈"), ("아시아", "🐉"), ("북미", "🦅")]:
        if cat_name in cat_sums and cat_sums[cat_name] != 0:
            vol = _format_volume(cat_sums[cat_name])
            sign = "+" if cat_sums[cat_name] > 0 else ""
            summary_parts.append(f"{cat_emoji}{cat_name} {sign}{vol}")

    if summary_parts:
        lines.append("📊 " + " | ".join(summary_parts))

    return "\n".join(lines)


def generate_nationality_report(
    codes: List[str],
    code_names: Dict[str, str] = None,
    date_new: str = None,
) -> str:
    """여러 종목 국적별 수급 종합 보고

    Args:
        codes: 종목코드 리스트
        code_names: {code: name} 매핑 (없으면 코드만 표시)
        date_new: 기준일 (기본: 최신 가용일)

    Returns: 텔레그램 메시지 문자열
    """
    if not date_new:
        date_new = _get_latest_data_date()

    date_old = _find_prev_trading_day(date_new)

    reports = []
    for code in codes:
        name = (code_names or {}).get(code, code)
        changes = compare_nationality(code, date_new, date_old)
        if changes:
            report = format_nationality_report(
                code, name, changes, date_new, date_old
            )
            reports.append(report)
        else:
            reports.append(f"🌍 {name}({code}) - 데이터 없음")

    return "\n\n".join(reports)


# ═══════════════════════════════════════════
# 추천 파이프라인 점수 (nationality score)
# ═══════════════════════════════════════════

def score_nationality(
    code: str,
    date_new: str = None,
) -> Tuple[float, str]:
    """국적별 수급 시그널 점수 (추천 파이프라인용)

    점수 기준:
      - 기관국(영국 등) 거래량 +50% 이상 → +15점
      - 기관국 거래량 -50% 이상 → -10점
      - 헤지펀드(케이맨 등) 급증(+100%) → +10점
      - 아시아(중국/홍콩) 급증 → +5점
      - 전체 국가 수 증가 → +5점 (관심 확대)
      - 전체 국가 수 감소 → -5점 (관심 축소)

    Returns: (score, reason_text)
      score: -30 ~ +50
      reason: "🏛기관+15 🦈헤지+10" 형태
    """
    changes = compare_nationality(code, date_new)
    if not changes:
        return 0.0, "데이터없음"

    score = 0.0
    reasons = []

    # 분류별 거래량 변화율 계산
    cat_old_total = {}
    cat_new_total = {}
    for ch in changes:
        cat = ch["분류"]
        cat_old_total[cat] = cat_old_total.get(cat, 0) + ch["전일"]
        cat_new_total[cat] = cat_new_total.get(cat, 0) + ch["금일"]

    # 기관국 변화
    inst_old = cat_old_total.get("기관", 0)
    inst_new = cat_new_total.get("기관", 0)
    if inst_old > 0:
        inst_pct = (inst_new - inst_old) / inst_old * 100
        if inst_pct >= 50:
            pts = min(15, inst_pct / 10)
            score += pts
            reasons.append(f"🏛기관+{pts:.0f}")
        elif inst_pct <= -50:
            pts = max(-10, inst_pct / 10)
            score += pts
            reasons.append(f"🏛기관{pts:.0f}")

    # 헤지펀드국 변화
    hf_old = cat_old_total.get("헤지펀드", 0)
    hf_new = cat_new_total.get("헤지펀드", 0)
    if hf_old > 0:
        hf_pct = (hf_new - hf_old) / hf_old * 100
        if hf_pct >= 100:
            pts = min(10, hf_pct / 20)
            score += pts
            reasons.append(f"🦈헤지+{pts:.0f}")
        elif hf_pct <= -50:
            score -= 5
            reasons.append("🦈헤지-5")
    elif hf_new > 0:
        score += 5
        reasons.append("🦈헤지NEW")

    # 아시아머니 변화
    asia_old = cat_old_total.get("아시아", 0)
    asia_new = cat_new_total.get("아시아", 0)
    if asia_old > 0:
        asia_pct = (asia_new - asia_old) / asia_old * 100
        if asia_pct >= 100:
            score += 5
            reasons.append("🐉아시아+5")
        elif asia_pct <= -50:
            score -= 3
            reasons.append("🐉아시아-3")

    # 국가 수 변화 (관심도 확대/축소)
    new_countries = sum(1 for ch in changes if ch["전일"] == 0 and ch["금일"] > 0)
    lost_countries = sum(1 for ch in changes if ch["전일"] > 0 and ch["금일"] == 0)
    if new_countries >= 3:
        score += 5
        reasons.append(f"🌐신규{new_countries}국")
    elif lost_countries >= 3:
        score -= 5
        reasons.append(f"🌐이탈{lost_countries}국")

    reason_text = " ".join(reasons) if reasons else "변화미미"
    return round(score, 1), reason_text


def score_nationality_batch(
    codes: List[str],
    date_new: str = None,
) -> Dict[str, Tuple[float, str]]:
    """여러 종목 국적별 점수 배치 계산

    Returns: {code: (score, reason)}
    """
    results = {}
    for code in codes:
        results[code] = score_nationality(code, date_new)
    return results
