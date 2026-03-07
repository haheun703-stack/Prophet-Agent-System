# -*- coding: utf-8 -*-
"""
COT 스마트머니 포지션 추적 (4D Slow Eye)
═══════════════════════════════════════════
CFTC Commitments of Traders 주간 보고서 → "스마트머니" 포지셔닝 감지.

핵심 원리:
  Commercial(상업적 헤저) = 산업 인사이더, 평균적으로 맞는 쪽
  Non-Commercial(대형 투기자) = 극단일 때 역지표 (crowded → 되돌림)

추적 대상 (4대 글로벌 선물):
  1. E-mini S&P 500  — 미국 주식 리스크 심리  (KOSPI β=+1.0)
  2. Gold (GC)       — 안전자산 수요           (KOSPI β=-0.5)
  3. Crude Oil (CL)  — 에너지/경기 전망        (KOSPI β=-0.3)
  4. Japanese Yen (JY)— 캐리 트레이드/리스크    (KOSPI β=-0.8)

시그널 로직:
  Commercial z-score > +1.5 → 스마트머니 매수 (해당 자산 BULLISH)
  Commercial z-score < -1.5 → 스마트머니 매도 (해당 자산 BEARISH)
  Speculator z-score > +1.5 → 투기 과열 (contrarian BEARISH)
  Speculator z-score < -1.5 → 투기 과매도 (contrarian BULLISH)

  → kospi_beta 곱해서 KOSPI 영향으로 변환 → 합산 = smart_money_score

주기: 주 1회 (CFTC 금요일 발표, 화요일 기준 데이터)
데이터 소스: CFTC 공개 보고서 (https://www.cftc.gov/dea/newcot/deafut.txt)
저장: jarvis/data/cot_history.json (52주 롤링)
"""

import csv
import io
import json
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_PATH = DATA_DIR / "cot_raw_cache.json"
HISTORY_PATH = DATA_DIR / "cot_history.json"

# ═══════════════════════════════════════
#  추적 대상 계약
# ═══════════════════════════════════════

TRACKED_CONTRACTS = {
    "sp500": {
        "name": "E-Mini S&P 500",
        "search_patterns": ["E-MINI S&P 500", "EMINI S&P 500"],
        "kospi_beta": 1.0,       # S&P ↑ → KOSPI ↑
        "comm_weight": 2.0,      # Commercial 신호 가중치
        "spec_weight": 1.5,      # Speculator 신호 가중치
        "korea_impact": "미국 위험자산 심리 → KOSPI 동조",
    },
    "gold": {
        "name": "Gold",
        "search_patterns": ["GOLD - COMMODITY EXCHANGE"],
        "kospi_beta": -0.5,      # Gold ↑ → 안전자산 선호 → KOSPI ↓ (약한)
        "comm_weight": 0.5,      # Commercial = 광산업체 헤지 → 방향성 약함
        "spec_weight": 2.5,      # Managed Money 극단이 핵심 역지표
        "korea_impact": "안전자산 수요 → 위험자산 이탈",
    },
    "crude_oil": {
        "name": "Crude Oil",
        "search_patterns": ["CRUDE OIL, LIGHT SWEET", "CRUDE OIL LIGHT SWEET"],
        "kospi_beta": -0.3,      # Oil ↑ → 한국 수입비용 ↑ → KOSPI 약간 ↓
        "comm_weight": 1.5,
        "spec_weight": 1.5,
        "korea_impact": "에너지 수입국 → 유가 상승 = 비용 부담",
    },
    "yen": {
        "name": "Japanese Yen",
        "search_patterns": ["JAPANESE YEN"],
        "kospi_beta": -0.8,      # Yen ↑(강세) → 캐리 언와인드 → 아시아 유출
        "comm_weight": 2.0,
        "spec_weight": 1.0,
        "korea_impact": "엔 캐리 트레이드 → 엔 강세=아시아 자금 유출",
    },
}

# Z-score 극단 임계값
Z_EXTREME = 1.5
Z_MODERATE = 1.0

# CFTC 데이터 URL
CFTC_LATEST_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"
# Legacy Combined Report (연간 아카이브, annual.txt 포함)
CFTC_YEARLY_URL = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"

# 캐시 유효 기간 (일)
CACHE_MAX_AGE_DAYS = 5  # 화~금 사이에 갱신되도록


# ═══════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════

@dataclass
class ContractPosition:
    """단일 계약의 포지션 분석"""
    contract_id: str
    contract_name: str
    report_date: str = ""
    # 원시 데이터
    open_interest: int = 0
    noncomm_long: int = 0
    noncomm_short: int = 0
    comm_long: int = 0
    comm_short: int = 0
    # 계산값
    noncomm_net: int = 0       # 투기적 순매수 (long - short)
    comm_net: int = 0          # 상업적 순매수
    noncomm_net_change: int = 0  # 전주 대비 변화
    comm_net_change: int = 0
    # Z-score (52주)
    noncomm_z: float = 0.0
    comm_z: float = 0.0
    # 신호
    signal: str = ""
    korea_impact: str = ""
    kospi_contribution: float = 0.0  # KOSPI 기여 점수


@dataclass
class COTReport:
    """COT 종합 보고"""
    date: str = ""
    data_date: str = ""        # CFTC 보고 기준일 (화요일)
    positions: list = field(default_factory=list)   # [ContractPosition]
    # 종합 판단
    smart_money_score: float = 0.0   # -10 ~ +10 (KOSPI 관점)
    smart_money_signal: str = ""     # RISK_ON / RISK_OFF / NEUTRAL
    confidence: float = 0.0          # 0~1
    extreme_count: int = 0           # 극단 신호 수 (0~8)
    signal: str = ""                 # 텔레그램 1줄 요약
    # BRAIN 연동
    allocation_adjust: dict = field(default_factory=dict)
    # 히스토리
    score_trend: str = ""            # UP / FLAT / DOWN


# ═══════════════════════════════════════
#  CFTC 데이터 다운로드 + 파싱
# ═══════════════════════════════════════

def _download_text(url: str) -> Optional[str]:
    """URL에서 텍스트 다운로드 (CFTC 서버용 User-Agent)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/plain, text/csv, */*",
    }
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except URLError as e:
        logger.warning(f"[COT] 다운로드 실패 {url}: {e}")
        return None
    except Exception as e:
        logger.warning(f"[COT] 다운로드 오류 {url}: {e}")
        return None


def _download_zip(url: str) -> Optional[str]:
    """ZIP URL에서 텍스트 추출"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=60) as resp:
            zip_data = resp.read()
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            # ZIP 내 첫 번째 파일
            names = zf.namelist()
            if not names:
                return None
            with zf.open(names[0]) as f:
                return f.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"[COT] ZIP 다운로드 실패 {url}: {e}")
        return None


def _parse_cot_text(text: str) -> List[Dict]:
    """CFTC COT 텍스트 파싱 → 관심 계약만 추출

    CFTC deafut.txt는 헤더 없는 포지셔널 CSV:
      col[0]: Market_and_Exchange_Names (quoted)
      col[1]: As_of_Date_In_Form_YYMMDD
      col[2]: Report_Date_as_YYYY-MM-DD
      col[7]: Open_Interest_All
      col[8]: NonComm_Positions_Long_All
      col[9]: NonComm_Positions_Short_All
      col[10]: NonComm_Positions_Spreading_All
      col[11]: Comm_Positions_Long_All
      col[12]: Comm_Positions_Short_All
    """
    rows = []
    try:
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if len(row) < 13:
                continue

            market_name = row[0].strip()
            if not market_name:
                continue

            # 관심 계약인지 확인 (첫 매칭만, MICRO/XRATE 등 제외)
            matched_id = None
            for cid, cdef in TRACKED_CONTRACTS.items():
                for pattern in cdef["search_patterns"]:
                    if pattern.upper() in market_name.upper():
                        # MICRO, XRATE, MINI 변형 제외 (정확한 계약만)
                        if cid == "sp500" and "MICRO" in market_name.upper():
                            continue
                        if cid == "gold" and "MICRO" in market_name.upper():
                            continue
                        if cid == "yen" and "XRATE" in market_name.upper():
                            continue
                        matched_id = cid
                        break
                if matched_id:
                    break

            if not matched_id:
                continue

            # 날짜
            report_date = row[2].strip() if len(row) > 2 else ""
            if not report_date or len(report_date) < 8:
                raw = row[1].strip() if len(row) > 1 else ""
                if raw and len(raw) == 6:
                    try:
                        report_date = datetime.strptime(raw, "%y%m%d").strftime("%Y-%m-%d")
                    except ValueError:
                        continue

            if not report_date:
                continue

            def safe_int(idx):
                if idx >= len(row):
                    return 0
                val = row[idx].strip().replace(",", "")
                try:
                    return int(val)
                except ValueError:
                    return 0

            rows.append({
                "contract_id": matched_id,
                "report_date": report_date,
                "market_name": market_name,
                "open_interest": safe_int(7),
                "noncomm_long": safe_int(8),
                "noncomm_short": safe_int(9),
                "comm_long": safe_int(11),
                "comm_short": safe_int(12),
            })

    except Exception as e:
        logger.error(f"[COT] 파싱 오류: {e}")

    return rows


def _fetch_cot_data() -> List[Dict]:
    """CFTC COT 데이터 수집 (캐시 + 누적 방식)

    전략:
      1. 로컬 캐시(누적 히스토리)가 5일 이내면 그대로 사용
      2. 5일 초과 → CFTC 최신 주간 보고서 다운로드 → 캐시에 추가 저장
      3. 52주치 데이터 누적 → z-score 계산 가능
         (첫 10주까지는 z=0 반환, 이후부터 유효)

    Returns:
        관심 계약의 주간 데이터 리스트 (시간순)
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 기존 캐시(누적 데이터) 로드
    cached = _load_cache()
    existing_data = cached.get("data", []) if cached else []

    # 2. 캐시 유효 여부 확인
    if cached:
        cache_date = cached.get("fetched_at", "")
        if cache_date:
            try:
                age = (datetime.now() - datetime.fromisoformat(cache_date)).days
                if age < CACHE_MAX_AGE_DAYS:
                    logger.info(f"[COT] 캐시 사용 (age={age}일, {len(existing_data)}건)")
                    return existing_data
            except ValueError:
                pass

    # 3. 히스토리가 부족하면 연간 아카이브 백필 (z-score 즉시 활성화)
    contract_counts = {}
    for r in existing_data:
        contract_counts[r["contract_id"]] = contract_counts.get(r["contract_id"], 0) + 1
    min_weeks = min(contract_counts.values()) if contract_counts else 0

    if min_weeks < 10:
        logger.info(f"[COT] 히스토리 부족 ({min_weeks}주) → 연간 아카이브 백필 시작")
        existing_data = _backfill_yearly(existing_data)

    # 4. CFTC 최신 주간 데이터 다운로드
    logger.info("[COT] CFTC 최신 주간 보고서 다운로드...")
    latest_text = _download_text(CFTC_LATEST_URL)
    latest_rows = _parse_cot_text(latest_text) if latest_text else []

    if latest_rows:
        logger.info(f"[COT] 최신 주간: {len(latest_rows)}건 파싱 (날짜: {latest_rows[0].get('report_date', '?')})")

        # 5. 기존 데이터에 신규 추가 (중복 제거)
        existing_keys = {(r["contract_id"], r["report_date"]) for r in existing_data}
        new_count = 0
        for row in latest_rows:
            key = (row["contract_id"], row["report_date"])
            if key not in existing_keys:
                existing_data.append(row)
                existing_keys.add(key)
                new_count += 1

        if new_count > 0:
            logger.info(f"[COT] 신규 {new_count}건 추가 (누적 {len(existing_data)}건)")

    # 날짜순 정렬 + 트림
    existing_data.sort(key=lambda x: x["report_date"])
    existing_data = _trim_data(existing_data, max_weeks_per_contract=60)

    if existing_data:
        _save_cache(existing_data)
    else:
        # 다운로드 실패 → 기존 캐시 사용
        if cached and cached.get("data"):
            logger.warning("[COT] 다운로드 실패 → 오래된 캐시 사용")
            return cached["data"]
        logger.warning("[COT] 데이터 수집 완전 실패")

    return existing_data


def _backfill_yearly(existing_data: List[Dict]) -> List[Dict]:
    """CFTC 연간 아카이브에서 과거 데이터 백필

    deacot{year}.zip → annual.txt (Legacy Combined, 헤더행 포함)
    최근 1년(전년+올해) 다운로드 → 52주 z-score 즉시 활성화
    """
    now = datetime.now()
    existing_keys = {(r["contract_id"], r["report_date"]) for r in existing_data}
    total_added = 0

    for year in [now.year - 1, now.year]:
        url = CFTC_YEARLY_URL.format(year=year)
        logger.info(f"[COT] {year}년 아카이브 다운로드: {url}")
        yearly_text = _download_zip(url)
        if not yearly_text:
            logger.warning(f"[COT] {year}년 아카이브 실패")
            continue

        yearly_rows = _parse_cot_text(yearly_text)
        added = 0
        for row in yearly_rows:
            key = (row["contract_id"], row["report_date"])
            if key not in existing_keys:
                existing_data.append(row)
                existing_keys.add(key)
                added += 1

        total_added += added
        logger.info(f"[COT] {year}년: {len(yearly_rows)}건 파싱, {added}건 신규")

    if total_added > 0:
        existing_data.sort(key=lambda x: x["report_date"])
        logger.info(f"[COT] 백필 완료: 총 {total_added}건 추가 (누적 {len(existing_data)}건)")

    return existing_data


def _trim_data(data: List[Dict], max_weeks_per_contract: int = 60) -> List[Dict]:
    """계약별로 최근 N주치만 유지"""
    from collections import defaultdict
    by_contract = defaultdict(list)
    for row in data:
        by_contract[row["contract_id"]].append(row)

    trimmed = []
    for cid, rows in by_contract.items():
        rows.sort(key=lambda x: x["report_date"])
        trimmed.extend(rows[-max_weeks_per_contract:])

    trimmed.sort(key=lambda x: x["report_date"])
    return trimmed


def _load_cache() -> Optional[Dict]:
    """로컬 캐시 로드"""
    if not CACHE_PATH.exists():
        return None
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(data: List[Dict]):
    """로컬 캐시 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cache = {
        "fetched_at": datetime.now().isoformat(),
        "count": len(data),
        "data": data,
    }
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ═══════════════════════════════════════
#  Z-score 계산
# ═══════════════════════════════════════

def _calc_z_score(values: List[float], current: float) -> float:
    """52주(최근 N개) 기준 z-score"""
    if len(values) < 10:
        return 0.0
    import numpy as np
    arr = np.array(values[-52:])  # 최대 52주
    mean = arr.mean()
    std = arr.std()
    if std < 1e-8:
        return 0.0
    return round(float((current - mean) / std), 2)


def _get_contract_history(
    all_data: List[Dict], contract_id: str
) -> List[Dict]:
    """특정 계약의 시간순 히스토리"""
    rows = [r for r in all_data if r["contract_id"] == contract_id]
    # 날짜별 중복 제거 (최신 우선)
    seen = set()
    unique = []
    for r in reversed(rows):
        if r["report_date"] not in seen:
            seen.add(r["report_date"])
            unique.append(r)
    unique.reverse()
    return unique


# ═══════════════════════════════════════
#  메인 분석
# ═══════════════════════════════════════

def analyze_cot() -> COTReport:
    """COT 스마트머니 분석

    Returns:
        COTReport (smart_money_score: -10 ~ +10)
    """
    import numpy as np

    report = COTReport(date=datetime.now().strftime("%Y-%m-%d"))

    # 데이터 수집
    all_data = _fetch_cot_data()
    if not all_data:
        report.signal = "CFTC 데이터 수집 실패"
        report.smart_money_signal = "NEUTRAL"
        report.allocation_adjust = _no_adjust("데이터 없음")
        return report

    total_score = 0.0
    extreme_count = 0

    for cid, cdef in TRACKED_CONTRACTS.items():
        history = _get_contract_history(all_data, cid)

        pos = ContractPosition(
            contract_id=cid,
            contract_name=cdef["name"],
            korea_impact=cdef["korea_impact"],
        )

        if not history:
            pos.signal = "데이터 없음"
            report.positions.append(pos)
            continue

        latest = history[-1]
        pos.report_date = latest["report_date"]
        pos.open_interest = latest["open_interest"]
        pos.noncomm_long = latest["noncomm_long"]
        pos.noncomm_short = latest["noncomm_short"]
        pos.comm_long = latest["comm_long"]
        pos.comm_short = latest["comm_short"]
        pos.noncomm_net = pos.noncomm_long - pos.noncomm_short
        pos.comm_net = pos.comm_long - pos.comm_short

        # 전주 대비 변화
        if len(history) >= 2:
            prev = history[-2]
            prev_noncomm_net = prev["noncomm_long"] - prev["noncomm_short"]
            prev_comm_net = prev["comm_long"] - prev["comm_short"]
            pos.noncomm_net_change = pos.noncomm_net - prev_noncomm_net
            pos.comm_net_change = pos.comm_net - prev_comm_net

        # Z-score 계산 (52주 기준)
        noncomm_nets = [r["noncomm_long"] - r["noncomm_short"] for r in history]
        comm_nets = [r["comm_long"] - r["comm_short"] for r in history]

        pos.noncomm_z = _calc_z_score(noncomm_nets, pos.noncomm_net)
        pos.comm_z = _calc_z_score(comm_nets, pos.comm_net)

        if not report.data_date:
            report.data_date = pos.report_date

        # ── 계약별 KOSPI 기여 점수 계산 ──
        kospi_beta = cdef["kospi_beta"]
        comm_weight = cdef["comm_weight"]
        spec_weight = cdef["spec_weight"]
        # Gold: comm_weight 0.5 (광산업체 헤지=방향성 약함)
        #        spec_weight 2.5 (Managed Money 극단=핵심 역지표)
        # 나머지: comm_weight 1.5~2.0, spec_weight 1.0~1.5

        contract_bullish = 0.0  # 해당 자산이 BULLISH인 정도

        # Commercial 신호 (스마트머니와 같은 방향)
        if pos.comm_z >= Z_EXTREME:
            contract_bullish += comm_weight
            extreme_count += 1
        elif pos.comm_z >= Z_MODERATE:
            contract_bullish += comm_weight * 0.5
        elif pos.comm_z <= -Z_EXTREME:
            contract_bullish -= comm_weight
            extreme_count += 1
        elif pos.comm_z <= -Z_MODERATE:
            contract_bullish -= comm_weight * 0.5

        # Speculator 신호 (역지표 — 극단 롱 → 되돌림 = bearish)
        if pos.noncomm_z >= Z_EXTREME:
            contract_bullish -= spec_weight
            extreme_count += 1
        elif pos.noncomm_z >= Z_MODERATE:
            contract_bullish -= spec_weight * 0.5
        elif pos.noncomm_z <= -Z_EXTREME:
            contract_bullish += spec_weight
            extreme_count += 1
        elif pos.noncomm_z <= -Z_MODERATE:
            contract_bullish += spec_weight * 0.5

        # KOSPI 기여 = 자산 방향 × KOSPI 베타
        pos.kospi_contribution = round(contract_bullish * kospi_beta, 2)
        total_score += pos.kospi_contribution

        # 신호 텍스트
        signals = []
        if abs(pos.comm_z) >= Z_EXTREME:
            direction = "매수" if pos.comm_z > 0 else "매도"
            signals.append(f"Commercial 극단 {direction}(z={pos.comm_z:+.1f})")
        if abs(pos.noncomm_z) >= Z_EXTREME:
            direction = "과열" if pos.noncomm_z > 0 else "과매도"
            signals.append(f"Speculator {direction}(z={pos.noncomm_z:+.1f})")

        if signals:
            pos.signal = " | ".join(signals)
        elif abs(pos.comm_z) >= Z_MODERATE or abs(pos.noncomm_z) >= Z_MODERATE:
            pos.signal = f"주시 (Comm z={pos.comm_z:+.1f}, Spec z={pos.noncomm_z:+.1f})"
        else:
            pos.signal = f"중립 (Comm z={pos.comm_z:+.1f}, Spec z={pos.noncomm_z:+.1f})"

        report.positions.append(pos)

    # ── 종합 판단 ──
    # total_score 범위: 이론적 최대 ±(2+1.5+1.5+2)*(2+1.5) = ±24.5
    # 실제 -10~+10 으로 클램프
    report.smart_money_score = round(max(-10.0, min(10.0, total_score)), 1)
    report.extreme_count = extreme_count

    # 신호 판정
    if report.smart_money_score >= 3.0:
        report.smart_money_signal = "RISK_ON"
        report.confidence = min(0.9, abs(report.smart_money_score) / 10)
        report.signal = f"RISK_ON ({report.smart_money_score:+.1f}) — 스마트머니 위험자산 선호"
    elif report.smart_money_score <= -3.0:
        report.smart_money_signal = "RISK_OFF"
        report.confidence = min(0.9, abs(report.smart_money_score) / 10)
        report.signal = f"RISK_OFF ({report.smart_money_score:+.1f}) — 스마트머니 방어적"
    elif extreme_count >= 3:
        # 극단 신호가 많지만 방향이 상쇄 → 변동성 경고
        report.smart_money_signal = "VOLATILE"
        report.confidence = 0.5
        report.signal = f"혼조 ({report.smart_money_score:+.1f}) — {extreme_count}개 극단 신호 (방향 상쇄)"
    else:
        report.smart_money_signal = "NEUTRAL"
        report.confidence = 0.3
        report.signal = f"중립 ({report.smart_money_score:+.1f}) — 뚜렷한 포지셔닝 없음"

    # BRAIN 배분 조정
    report.allocation_adjust = _calc_allocation_adjust(report)

    # 히스토리 저장 + 추세 계산
    _save_history(report)
    report.score_trend = _calc_score_trend()

    return report


def _calc_allocation_adjust(report: COTReport) -> dict:
    """COT 신호 기반 BRAIN 배분 조정"""
    score = report.smart_money_score

    if report.smart_money_signal == "RISK_OFF" and score <= -5.0:
        return {
            "cash_shift_pct": 8,
            "inverse_shift_pct": 3,
            "swing_reduce_pct": 10,
            "reason": f"스마트머니 강한 RISK_OFF ({score:+.1f})",
        }
    elif report.smart_money_signal == "RISK_OFF":
        return {
            "cash_shift_pct": 5,
            "inverse_shift_pct": 0,
            "swing_reduce_pct": 5,
            "reason": f"스마트머니 RISK_OFF ({score:+.1f})",
        }
    elif report.smart_money_signal == "RISK_ON" and score >= 5.0:
        return {
            "cash_shift_pct": -5,
            "inverse_shift_pct": -3,
            "swing_reduce_pct": -5,
            "reason": f"스마트머니 강한 RISK_ON ({score:+.1f})",
        }
    elif report.smart_money_signal == "RISK_ON":
        return {
            "cash_shift_pct": -3,
            "inverse_shift_pct": 0,
            "swing_reduce_pct": -3,
            "reason": f"스마트머니 RISK_ON ({score:+.1f})",
        }
    elif report.smart_money_signal == "VOLATILE":
        return {
            "cash_shift_pct": 3,
            "inverse_shift_pct": 0,
            "swing_reduce_pct": 3,
            "reason": f"스마트머니 혼조 — 변동성 경계",
        }
    return _no_adjust("중립 — 조정 불필요")


def _no_adjust(reason: str) -> dict:
    return {
        "cash_shift_pct": 0,
        "inverse_shift_pct": 0,
        "swing_reduce_pct": 0,
        "reason": reason,
    }


# ═══════════════════════════════════════
#  히스토리
# ═══════════════════════════════════════

def _save_history(report: COTReport):
    """COT 히스토리 저장 (52주 롤링)"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    history = {}
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            history = {}

    history[report.date] = {
        "data_date": report.data_date,
        "smart_money_score": report.smart_money_score,
        "smart_money_signal": report.smart_money_signal,
        "extreme_count": report.extreme_count,
        "confidence": report.confidence,
        "positions": {
            p.contract_id: {
                "noncomm_net": p.noncomm_net,
                "comm_net": p.comm_net,
                "noncomm_z": p.noncomm_z,
                "comm_z": p.comm_z,
                "kospi_contribution": p.kospi_contribution,
            }
            for p in report.positions
        },
    }

    # 52주(약 365일) 초과 데이터 삭제
    dates = sorted(history.keys())
    while len(dates) > 52:
        del history[dates.pop(0)]

    HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _calc_score_trend() -> str:
    """최근 3주 스마트머니 스코어 추세"""
    if not HISTORY_PATH.exists():
        return "FLAT"
    try:
        history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return "FLAT"

    dates = sorted(history.keys())[-3:]
    if len(dates) < 2:
        return "FLAT"

    values = [history[d]["smart_money_score"] for d in dates]
    if values[-1] > values[0] + 1.0:
        return "UP"
    elif values[-1] < values[0] - 1.0:
        return "DOWN"
    return "FLAT"


def load_cot_history() -> dict:
    """히스토리 로드 (외부 참조용)"""
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_cot_report(report: COTReport) -> str:
    """텔레그램용 COT 스마트머니 리포트"""

    signal_emoji = {
        "RISK_ON": "🟢",
        "RISK_OFF": "🔴",
        "VOLATILE": "🟡",
        "NEUTRAL": "⬜",
    }
    emoji = signal_emoji.get(report.smart_money_signal, "⬜")

    lines = [
        "🏦 COT 스마트머니 포지션 (4D)",
        f"📅 분석: {report.date} | 데이터: {report.data_date or 'N/A'}",
        "",
        f"{emoji} 스마트머니 스코어: {report.smart_money_score:+.1f}/10"
        f" [{report.smart_money_signal}]",
        f"   극단 신호: {report.extreme_count}개"
        f" | 확신도: {report.confidence:.0%}"
        f" | 추세: {report.score_trend}",
        "",
        "── 계약별 포지션 ──",
    ]

    for p in report.positions:
        if not p.report_date:
            lines.append(f"  {p.contract_name}: 데이터 없음")
            continue

        # 방향 화살표
        comm_arrow = "+" if p.comm_net > 0 else "-"
        spec_arrow = "+" if p.noncomm_net > 0 else "-"

        # 극단 여부
        comm_tag = ""
        if abs(p.comm_z) >= Z_EXTREME:
            comm_tag = " **극단**" if p.comm_z > 0 else " **극단**"
        spec_tag = ""
        if abs(p.noncomm_z) >= Z_EXTREME:
            spec_tag = " **극단**"

        lines.append(
            f"  {p.contract_name}"
            f" (KOSPI 기여: {p.kospi_contribution:+.1f})"
        )
        lines.append(
            f"    Commercial: {p.comm_net:+,}"
            f" (z={p.comm_z:+.1f}{comm_tag})"
            f" Δ{p.comm_net_change:+,}"
        )
        lines.append(
            f"    Speculator: {p.noncomm_net:+,}"
            f" (z={p.noncomm_z:+.1f}{spec_tag})"
            f" Δ{p.noncomm_net_change:+,}"
        )
        if p.signal and "중립" not in p.signal:
            lines.append(f"    → {p.signal}")

    # 배분 조정
    adj = report.allocation_adjust
    if adj.get("reason"):
        lines.append("")
        lines.append(f"💡 {adj['reason']}")
        if adj.get("swing_reduce_pct", 0) != 0:
            lines.append(
                f"   스윙 {adj['swing_reduce_pct']:+d}%p"
                f" / 현금 {adj['cash_shift_pct']:+d}%p"
            )

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    print("COT 스마트머니 분석 시작...")
    report = analyze_cot()
    print(format_cot_report(report))
    print(f"\n배분 조정: {report.allocation_adjust}")
