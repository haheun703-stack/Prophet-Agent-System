# -*- coding: utf-8 -*-
"""
NIGHTWATCH 블라인드 백테스트
═══════════════════════════════════════════════
2025-03 ~ 2026-02 (12개월) 일별 시뮬레이션

검증 항목:
  1. 방향 적중률 (시그널 vs 다음날 갭)
  2. 섹터 선택 정확도 (추천 섹터 vs KOSPI 대비 초과수익)
  3. 누적 수익률 (NXT 갭 트레이딩 시뮬)

사용법:
  cd scalper-agent
  python -m backtest.nightwatch_backtest
  python -m backtest.nightwatch_backtest --start 2025-06 --end 2025-12
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# ── yfinance ──
try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance pandas numpy 필요")
    sys.exit(1)

# ── nightwatch 로직 재사용 ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data.nightwatch import (
    JARVIS_SECTORS,
    select_sectors_and_targets,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════
#  설정
# ═══════════════════════════════════════════════════
GLOBAL_TICKERS = {
    "AUDJPY":  "AUDJPY=X",
    "CNH":     "CNY=X",
    "DAX":     "^GDAXI",
    "EURUSD":  "EURUSD=X",
    "HYG":     "HYG",
    "VIX":     "^VIX",
    "ES":      "ES=F",
    "TNX":     "^TNX",
    "GOLD":    "GC=F",
    "USDKRW":  "KRW=X",
    "NQ":      "NQ=F",
    "CL":      "CL=F",
    "HG":      "HG=F",
}

# 한국 대표 종목 (KOSPI + 섹터별 Tier1)
KR_TICKERS = {
    "KOSPI":       "^KS11",
    "KODEX200":    "069500.KS",
    # 반도체
    "SK하이닉스":   "000660.KS",
    "삼성전자":     "005930.KS",
    # SW/AI
    "NAVER":       "035420.KS",
    "카카오":       "035720.KS",
    # 바이오
    "삼성바이오":   "207940.KS",
    "셀트리온":     "068270.KS",
    # 전력
    "HD현대일렉":   "267260.KS",
    "LS일렉트릭":   "010120.KS",
    # 원유
    "S-Oil":       "010950.KS",
    # 방산
    "한화에어로":   "012450.KS",
    "한국항공우주":  "047810.KS",
    # 엔터
    "하이브":       "352820.KS",
    # 증권
    "미래에셋":     "006800.KS",
    "키움증권":     "039490.KS",
    # 리츠
    "SK리츠":       "395400.KS",
    "맥쿼리인프라":  "088980.KS",
    # 조선
    "HD한국조선":   "009540.KS",
    "한화오션":     "042660.KS",
    # 2차전지
    "LG에너지":    "373220.KS",
    "삼성SDI":     "006400.KS",
    # 인버스
    "KODEX인버스2X": "252670.KS",
}

# 섹터키 → 대표 한국주식 매핑 (백테스트 수익 측정용)
SECTOR_REPR = {
    "semiconductor":  ["SK하이닉스", "삼성전자"],
    "software_ai":    ["NAVER", "카카오"],
    "bio":            ["삼성바이오", "셀트리온"],
    "power_infra":    ["HD현대일렉", "LS일렉트릭"],
    "oil_resource":   ["S-Oil"],
    "space_defense":  ["한화에어로", "한국항공우주"],
    "entertainment":  ["하이브"],
    "securities":     ["미래에셋", "키움증권"],
    "reits":          ["SK리츠", "맥쿼리인프라"],
    "shipbuilding":   ["HD한국조선", "한화오션"],
    "battery_ev":     ["LG에너지", "삼성SDI"],
    "inverse":        ["KODEX인버스2X"],
}


# ═══════════════════════════════════════════════════
#  데이터 다운로드
# ═══════════════════════════════════════════════════
def download_all_data(
    start: str = "2025-02-01",
    end: str = "2026-03-01",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    글로벌 + 한국 티커 전부 다운로드
    Returns: (global_df, kr_df) - 멀티인덱스 컬럼 (ticker, OHLCV)
    """
    print(f"\n{'='*50}")
    print(f"  데이터 다운로드: {start} ~ {end}")
    print(f"{'='*50}")

    # 글로벌
    global_symbols = list(GLOBAL_TICKERS.values())
    print(f"\n글로벌 지표 ({len(global_symbols)}개): {', '.join(GLOBAL_TICKERS.keys())}")
    global_df = yf.download(
        global_symbols,
        start=start,
        end=end,
        auto_adjust=True,
        progress=True,
        threads=True,
    )

    # 한국
    kr_symbols = list(KR_TICKERS.values())
    print(f"\n한국 종목 ({len(kr_symbols)}개): {', '.join(KR_TICKERS.keys())}")
    kr_df = yf.download(
        kr_symbols,
        start=start,
        end=end,
        auto_adjust=True,
        progress=True,
        threads=True,
    )

    print(f"\n글로벌 데이터: {len(global_df)}일")
    print(f"한국 데이터:   {len(kr_df)}일")

    return global_df, kr_df


def build_daily_changes(
    global_df: pd.DataFrame,
    kr_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    일별 변화율 테이블 생성
    각 티커의 전일 대비 변화율(%) + 절대변화
    """
    records = []

    # 글로벌 Close 추출
    g_close = {}
    for name, symbol in GLOBAL_TICKERS.items():
        try:
            if isinstance(global_df.columns, pd.MultiIndex):
                col = ("Close", symbol)
                if col in global_df.columns:
                    g_close[name] = global_df[col].dropna()
                else:
                    print(f"  WARN: {name}({symbol}) Close 없음")
            else:
                g_close[name] = global_df["Close"].dropna()
        except Exception as e:
            print(f"  WARN: {name} 추출 실패: {e}")

    # 한국 Close + Open 추출
    kr_close = {}
    kr_open = {}
    for name, symbol in KR_TICKERS.items():
        try:
            if isinstance(kr_df.columns, pd.MultiIndex):
                col_c = ("Close", symbol)
                col_o = ("Open", symbol)
                if col_c in kr_df.columns:
                    kr_close[name] = kr_df[col_c].dropna()
                if col_o in kr_df.columns:
                    kr_open[name] = kr_df[col_o].dropna()
        except Exception as e:
            print(f"  WARN: {name} 추출 실패: {e}")

    # 공통 날짜 (한국 거래일 기준)
    if "KOSPI" in kr_close:
        base_dates = kr_close["KOSPI"].index
    elif "KODEX200" in kr_close:
        base_dates = kr_close["KODEX200"].index
    else:
        print("ERROR: KOSPI/KODEX200 데이터 없음")
        return pd.DataFrame()

    print(f"\n한국 거래일 수: {len(base_dates)}")

    for i in range(1, len(base_dates)):
        date = base_dates[i]
        prev_date = base_dates[i - 1]

        row = {"date": date}

        # ── 글로벌 지표 변화율 ──
        for name, series in g_close.items():
            # 가장 가까운 이전 데이터 찾기
            prev_vals = series[series.index <= prev_date]
            curr_vals = series[series.index <= date]

            if len(prev_vals) == 0 or len(curr_vals) == 0:
                row[f"{name}_pct"] = None
                row[f"{name}_abs"] = None
                row[f"{name}_val"] = None
                continue

            prev_val = float(prev_vals.iloc[-1])
            curr_val = float(curr_vals.iloc[-1])

            if prev_val == 0:
                row[f"{name}_pct"] = None
                row[f"{name}_abs"] = None
            else:
                row[f"{name}_pct"] = ((curr_val - prev_val) / prev_val) * 100
                row[f"{name}_abs"] = curr_val - prev_val
            row[f"{name}_val"] = curr_val

        # ── 한국 종목 갭 (당일 시가 vs 전일 종가) ──
        for name in KR_TICKERS:
            if name in kr_close and name in kr_open:
                prev_c = kr_close[name]
                curr_o = kr_open[name]

                prev_vals = prev_c[prev_c.index <= prev_date]
                curr_vals = curr_o[curr_o.index <= date]

                if len(prev_vals) > 0 and len(curr_vals) > 0:
                    pc = float(prev_vals.iloc[-1])
                    co = float(curr_vals.iloc[-1])
                    if pc > 0:
                        row[f"{name}_gap"] = ((co - pc) / pc) * 100
                    else:
                        row[f"{name}_gap"] = None
                else:
                    row[f"{name}_gap"] = None
            else:
                row[f"{name}_gap"] = None

        records.append(row)

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    return df


# ═══════════════════════════════════════════════════
#  NIGHTWATCH 스코어 재현 (히스토리컬)
# ═══════════════════════════════════════════════════
def calc_asian_score(row: pd.Series) -> Tuple[float, Dict]:
    """[1단] 아시안 리스크 (AUD/JPY + CNH)"""
    score = 0.0
    detail = {}

    # AUD/JPY
    audjpy_pct = row.get("AUDJPY_pct")
    if pd.notna(audjpy_pct):
        if audjpy_pct > 0.3:
            score += 2.0
        elif audjpy_pct > 0.1:
            score += 1.0
        elif audjpy_pct < -0.3:
            score -= 2.0
        elif audjpy_pct < -0.1:
            score -= 1.0
        detail["AUD/JPY"] = {"change_pct": audjpy_pct}

    # CNH (CNY=X: 하락 = 위안화 강세 = 긍정)
    cnh_pct = row.get("CNH_pct")
    if pd.notna(cnh_pct):
        if cnh_pct < -0.2:
            score += 1.5
        elif cnh_pct > 0.2:
            score -= 1.5
        detail["CNH"] = {"change_pct": cnh_pct}

    # KOSPI 외인 (백테스트에서는 근사: KOSPI 자체 움직임으로 대체)
    # 실제로는 nationality_signal 사용하지만 과거 데이터 없음
    # → 0으로 설정 (보수적)
    detail["KOSPI_외인"] = {"score": 0, "signal": "⬜"}

    return round(score, 1), detail


def calc_europe_score(row: pd.Series) -> Tuple[float, Dict]:
    """[2단] 유럽 오픈 (DAX + EUR/USD + HYG)"""
    score = 0.0
    detail = {}

    # DAX (일간 변화율을 30분 프록시로 사용)
    dax_pct = row.get("DAX_pct")
    if pd.notna(dax_pct):
        if dax_pct > 0.5:
            score += 3.0
        elif dax_pct > 0.2:
            score += 1.5
        elif dax_pct < -0.5:
            score -= 3.0
        elif dax_pct < -0.2:
            score -= 1.5
        detail["DAX_30min"] = {"change_pct": dax_pct}

    # EUR/USD
    eurusd_pct = row.get("EURUSD_pct")
    if pd.notna(eurusd_pct):
        if eurusd_pct > 0.3:
            score += 1.0
        elif eurusd_pct < -0.3:
            score -= 1.0
        detail["EUR/USD"] = {"change_pct": eurusd_pct}

    # HYG
    hyg_pct = row.get("HYG_pct")
    if pd.notna(hyg_pct):
        if hyg_pct > 0.3:
            score += 1.5
        elif hyg_pct < -0.3:
            score -= 2.0
        detail["HYG"] = {"change_pct": hyg_pct}

    return round(score, 1), detail


def calc_divergence(row: pd.Series) -> Tuple[float, List[str]]:
    """[3단] 괴리 감지"""
    penalty = 0.0
    divergences = []

    # VIX 레벨
    vix_val = row.get("VIX_val")
    if pd.notna(vix_val):
        if vix_val >= 30:
            penalty -= 3.0
            divergences.append(f"VIX {vix_val:.1f} 패닉")
        elif vix_val >= 25:
            penalty -= 2.0
            divergences.append(f"VIX {vix_val:.1f} 공포")
        elif vix_val >= 20:
            penalty -= 0.5
        else:
            penalty += 0.5

    es_pct = row.get("ES_pct")

    # 괴리1: HYG↓ + ES 안정
    hyg_pct = row.get("HYG_pct")
    if pd.notna(hyg_pct) and pd.notna(es_pct):
        if hyg_pct < -0.3 and es_pct > -0.2:
            penalty -= 2.0
            divergences.append(f"HYG {hyg_pct:+.2f}% vs ES {es_pct:+.2f}%")

    # 괴리2: 채권↑(금리↓) + 금↓ = 위험선호 전환
    tnx_abs = row.get("TNX_abs")
    gold_pct = row.get("GOLD_pct")
    if pd.notna(tnx_abs) and pd.notna(gold_pct):
        if tnx_abs < -0.03 and gold_pct < -0.3:
            penalty += 1.5
            divergences.append("스마트머니 위험선호 전환")
        elif tnx_abs > 0.05 and gold_pct > 0.3:
            penalty -= 1.5
            divergences.append("안전자산 쏠림")

    # 괴리3: 원/달러 급등
    usdkrw_pct = row.get("USDKRW_pct")
    if pd.notna(usdkrw_pct):
        if usdkrw_pct > 0.5:
            penalty -= 1.0
            divergences.append(f"원/달러 {usdkrw_pct:+.2f}%")
        elif usdkrw_pct < -0.3:
            penalty += 0.5

    return round(penalty, 1), divergences


def calc_macro_conditions(row: pd.Series, asian_detail: Dict) -> Dict:
    """[4단] 매크로 조건 (JARVIS 섹터 매핑용)"""
    conditions = {}

    # NQ (나스닥)
    nq_pct = row.get("NQ_pct")
    if pd.notna(nq_pct):
        conditions["nasdaq_up"] = nq_pct > 0.5
        conditions["nasdaq_down"] = nq_pct < -0.5
        conditions["nasdaq_pct"] = nq_pct

    # CL (원유)
    cl_pct = row.get("CL_pct")
    if pd.notna(cl_pct):
        conditions["oil_up"] = cl_pct > 1.5
        conditions["oil_down"] = cl_pct < -1.5
        conditions["oil_pct"] = cl_pct

    # TNX (금리)
    tnx_abs = row.get("TNX_abs")
    if pd.notna(tnx_abs):
        conditions["rate_down"] = tnx_abs < -0.04
        conditions["rate_up"] = tnx_abs > 0.04

    # CNH
    cnh = asian_detail.get("CNH", {})
    cnh_pct = cnh.get("change_pct")
    if cnh_pct is not None:
        conditions["cnh_strong"] = cnh_pct < -0.15

    # 지정학
    audjpy = asian_detail.get("AUD/JPY", {})
    audjpy_pct = audjpy.get("change_pct")
    hg_pct = row.get("HG_pct")
    if audjpy_pct is not None and pd.notna(hg_pct):
        conditions["geopolitical"] = (audjpy_pct < -0.2 and hg_pct > 0.3)
    elif audjpy_pct is not None and audjpy_pct < -0.4:
        conditions["geopolitical"] = True

    return conditions


def simulate_day(row: pd.Series) -> Dict:
    """하루 시뮬레이션 → 신호 + 섹터 + 스코어"""
    # 1단
    asian_score, asian_detail = calc_asian_score(row)

    # 2단
    europe_score, europe_detail = calc_europe_score(row)

    # 3단
    div_score, divergences = calc_divergence(row)

    # 총점
    total = asian_score + europe_score + div_score
    total = max(-10.0, min(10.0, total))

    # 신호 (v2: 관망 확대, 금지/패닉 축소)
    if total >= 5:
        signal = "강매수"
    elif total >= 2:
        signal = "매수"
    elif total >= -2:
        signal = "관망"  # was -1 → -2
    elif total >= -5:
        signal = "금지"  # was -4 → -5
    else:
        signal = "패닉"

    # 4단 매크로
    macro = calc_macro_conditions(row, asian_detail)

    # 섹터 선택
    sector_names, nxt_targets, reason = select_sectors_and_targets(
        total_score=total,
        macro=macro,
        max_tier=1,  # 백테스트는 Tier1만
    )
    sector_keys = list(dict.fromkeys(t["sector_key"] for t in nxt_targets)) if nxt_targets else []

    return {
        "total_score": round(total, 1),
        "signal": signal,
        "asian": asian_score,
        "europe": europe_score,
        "divergence": div_score,
        "sector_keys": sector_keys,
        "reason": reason,
        "macro": macro,
        "n_targets": len(nxt_targets),
    }


# ═══════════════════════════════════════════════════
#  수익률 계산
# ═══════════════════════════════════════════════════
def calc_sector_gap(row: pd.Series, sector_keys: List[str]) -> Optional[float]:
    """추천 섹터의 평균 다음날 갭(%)"""
    gaps = []
    for key in sector_keys:
        repr_stocks = SECTOR_REPR.get(key, [])
        for name in repr_stocks:
            gap = row.get(f"{name}_gap")
            if pd.notna(gap):
                gaps.append(gap)
    return np.mean(gaps) if gaps else None


def calc_kospi_gap(row: pd.Series) -> Optional[float]:
    """KOSPI 다음날 갭(%)"""
    gap = row.get("KOSPI_gap")
    return gap if pd.notna(gap) else None


# ═══════════════════════════════════════════════════
#  메인 백테스트
# ═══════════════════════════════════════════════════
def run_backtest(
    start: str = "2025-03-01",
    end: str = "2026-03-01",
) -> pd.DataFrame:
    """전체 백테스트 실행"""

    # 1달 앞 데이터부터 받아야 변화율 계산 가능
    dl_start = (pd.Timestamp(start) - timedelta(days=40)).strftime("%Y-%m-%d")
    dl_end = (pd.Timestamp(end) + timedelta(days=5)).strftime("%Y-%m-%d")

    global_df, kr_df = download_all_data(dl_start, dl_end)

    print("\n일별 변화율 계산 중...")
    daily = build_daily_changes(global_df, kr_df)

    if daily.empty:
        print("ERROR: 데이터 구성 실패")
        return pd.DataFrame()

    # 기간 필터
    mask = (daily.index >= start) & (daily.index < end)
    daily = daily[mask]
    print(f"\n백테스트 기간: {daily.index[0].date()} ~ {daily.index[-1].date()}")
    print(f"거래일 수: {len(daily)}")

    # 일별 시뮬레이션
    print("\n시뮬레이션 실행 중...")
    results = []

    for i in range(len(daily) - 1):  # 마지막 날은 다음날 갭이 없으므로 제외
        row = daily.iloc[i]
        next_row = daily.iloc[i + 1]
        date = daily.index[i]

        # NIGHTWATCH 스코어 계산
        sim = simulate_day(row)

        # 다음날 갭 측정
        kospi_gap = next_row.get("KOSPI_gap")
        sector_gap = calc_sector_gap(next_row, sim["sector_keys"])

        # 인버스 시그널인 경우: 수익 = -KOSPI갭 (인버스니까)
        if "inverse" in sim["sector_keys"] and pd.notna(kospi_gap):
            sector_gap = -kospi_gap

        results.append({
            "date": date,
            "score": sim["total_score"],
            "signal": sim["signal"],
            "asian": sim["asian"],
            "europe": sim["europe"],
            "div": sim["divergence"],
            "reason": sim["reason"],
            "sectors": ",".join(sim["sector_keys"]),
            "n_targets": sim["n_targets"],
            "kospi_gap": kospi_gap if pd.notna(kospi_gap) else None,
            "sector_gap": sector_gap,
            # 방향 적중 여부
            "direction_hit": None,
            # 초과수익
            "excess": None,
        })

    df = pd.DataFrame(results)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    # 방향 적중 계산
    for idx in df.index:
        row = df.loc[idx]
        sig = row["signal"]
        kg = row["kospi_gap"]

        if pd.isna(kg) or sig == "관망":
            continue

        if sig in ("강매수", "매수"):
            df.at[idx, "direction_hit"] = 1.0 if kg > 0 else 0.0
        elif sig in ("금지", "패닉"):
            df.at[idx, "direction_hit"] = 1.0 if kg < 0 else 0.0

    # 초과수익 계산
    for idx in df.index:
        row = df.loc[idx]
        sg = row["sector_gap"]
        kg = row["kospi_gap"]
        if pd.notna(sg) and pd.notna(kg):
            df.at[idx, "excess"] = sg - kg

    return df


# ═══════════════════════════════════════════════════
#  리포트 출력
# ═══════════════════════════════════════════════════
def print_report(df: pd.DataFrame):
    """백테스트 결과 리포트"""

    print(f"\n{'='*60}")
    print(f"  NIGHTWATCH 블라인드 백테스트 결과")
    print(f"  {df.index[0].date()} ~ {df.index[-1].date()}")
    print(f"{'='*60}")

    total_days = len(df)
    print(f"\n총 거래일: {total_days}")

    # ── 1. 시그널 분포 ──
    print(f"\n[1] 시그널 분포")
    print("-" * 40)
    for sig in ["강매수", "매수", "관망", "금지", "패닉"]:
        cnt = (df["signal"] == sig).sum()
        pct = cnt / total_days * 100
        bar = "#" * int(pct)
        print(f"  {sig:4s}: {cnt:3d}일 ({pct:5.1f}%) {bar}")

    # ── 2. 방향 적중률 ──
    print(f"\n[2] 방향 적중률 (시그널 vs 다음날 갭)")
    print("-" * 40)

    hits = df["direction_hit"].dropna()
    if len(hits) > 0:
        overall_hit = hits.mean() * 100
        print(f"  전체: {overall_hit:.1f}% ({int(hits.sum())}/{len(hits)})")

        for sig in ["강매수", "매수", "금지", "패닉"]:
            mask = df["signal"] == sig
            sig_hits = df.loc[mask, "direction_hit"].dropna()
            if len(sig_hits) > 0:
                rate = sig_hits.mean() * 100
                print(f"  {sig:4s}: {rate:.1f}% ({int(sig_hits.sum())}/{len(sig_hits)})")
    else:
        print("  (적중 데이터 없음)")

    # ── 3. 수익률 통계 ──
    print(f"\n[3] 갭 수익률 통계 (%)")
    print("-" * 40)

    # 매수 시그널일 때만
    buy_mask = df["signal"].isin(["강매수", "매수"])
    buy_gaps = df.loc[buy_mask, "sector_gap"].dropna()

    if len(buy_gaps) > 0:
        print(f"  매수 시그널 날:")
        print(f"    거래 횟수: {len(buy_gaps)}")
        print(f"    평균 갭:   {buy_gaps.mean():+.3f}%")
        print(f"    중앙값:    {buy_gaps.median():+.3f}%")
        print(f"    최대:      {buy_gaps.max():+.3f}%")
        print(f"    최소:      {buy_gaps.min():+.3f}%")
        print(f"    승률:      {(buy_gaps > 0).mean() * 100:.1f}%")
        # 누적 수익
        cumul = (1 + buy_gaps / 100).prod() - 1
        print(f"    누적 수익:  {cumul * 100:+.2f}%")

    # 금지/패닉 시그널일 때 (인버스 또는 미진입)
    bear_mask = df["signal"].isin(["금지", "패닉"])
    bear_kospi = df.loc[bear_mask, "kospi_gap"].dropna()
    if len(bear_kospi) > 0:
        avoided = bear_kospi[bear_kospi < 0]
        print(f"\n  금지/패닉 시그널 날:")
        print(f"    발생 횟수:   {len(bear_kospi)}")
        print(f"    KOSPI 갭:    {bear_kospi.mean():+.3f}% (평균)")
        print(f"    실제 하락일: {len(avoided)}일 (적중 {len(avoided)/len(bear_kospi)*100:.0f}%)")

    # 관망일 분석
    neutral_mask = df["signal"] == "관망"
    neutral_kospi = df.loc[neutral_mask, "kospi_gap"].dropna()
    if len(neutral_kospi) > 0:
        print(f"\n  관망 시그널 날:")
        print(f"    발생 횟수: {len(neutral_kospi)}")
        print(f"    KOSPI 갭:  {neutral_kospi.mean():+.3f}% (평균)")
        print(f"    놓친 상승:  {(neutral_kospi > 0.5).sum()}일")

    # ── 4. 초과수익 ──
    print(f"\n[4] 섹터 선택 초과수익 (vs KOSPI)")
    print("-" * 40)
    excess = df.loc[buy_mask, "excess"].dropna()
    if len(excess) > 0:
        print(f"  평균 초과수익:  {excess.mean():+.3f}%")
        print(f"  초과 달성률:    {(excess > 0).mean() * 100:.1f}%")

    # ── 5. 월별 요약 ──
    print(f"\n[5] 월별 요약")
    print("-" * 60)
    print(f"  {'월':>7s} | {'거래':>4s} | {'적중률':>6s} | {'평균갭':>7s} | {'누적':>7s} | 주요 섹터")

    df["month"] = df.index.to_period("M")
    for month, grp in df.groupby("month"):
        buy_g = grp[grp["signal"].isin(["강매수", "매수"])]
        gaps = buy_g["sector_gap"].dropna()
        hits_m = buy_g["direction_hit"].dropna()

        n_trades = len(gaps)
        hit_rate = f"{hits_m.mean()*100:.0f}%" if len(hits_m) > 0 else "N/A"
        avg_gap = f"{gaps.mean():+.2f}%" if n_trades > 0 else "N/A"
        cumul_m = f"{((1 + gaps / 100).prod() - 1) * 100:+.2f}%" if n_trades > 0 else "N/A"

        # 가장 많이 추천된 섹터
        all_sectors = ",".join(buy_g["sectors"].dropna()).split(",")
        all_sectors = [s for s in all_sectors if s]
        if all_sectors:
            from collections import Counter
            top_sec = Counter(all_sectors).most_common(2)
            sec_str = "/".join(s[0] for s in top_sec)
        else:
            sec_str = "-"

        print(f"  {str(month):>7s} | {n_trades:>4d} | {hit_rate:>6s} | {avg_gap:>7s} | {cumul_m:>7s} | {sec_str}")

    # ── 6. Buy & Hold 비교 ──
    print(f"\n[6] 전략 vs Buy & Hold 비교")
    print("-" * 40)

    all_kospi_gaps = df["kospi_gap"].dropna()
    if len(all_kospi_gaps) > 0 and len(buy_gaps) > 0:
        # Buy & Hold: 매일 갭 트레이딩 (선별 없이)
        bnh_cumul = (1 + all_kospi_gaps / 100).prod() - 1
        strat_cumul = (1 + buy_gaps / 100).prod() - 1

        print(f"  Buy & Hold (매일 KOSPI 갭):  {bnh_cumul * 100:+.2f}%")
        print(f"  NIGHTWATCH 전략:             {strat_cumul * 100:+.2f}%")
        print(f"  초과 성과:                   {(strat_cumul - bnh_cumul) * 100:+.2f}%")
        print(f"  거래 효율 (거래일 비율):      {len(buy_gaps)}/{len(all_kospi_gaps)} "
              f"({len(buy_gaps)/len(all_kospi_gaps)*100:.0f}%)")

    # ── 7. 스코어 분포 ──
    print(f"\n[7] 스코어 분포")
    print("-" * 40)
    for lo, hi, label in [(-10, -4, "[-10,-4] 패닉"),
                           (-4, -1, "[-4,-1] 금지"),
                           (-1, 2, "[-1,2] 관망"),
                           (2, 5, "[2,5] 매수"),
                           (5, 11, "[5,10] 강매수")]:
        cnt = ((df["score"] >= lo) & (df["score"] < hi)).sum()
        pct = cnt / total_days * 100
        bar = "#" * int(pct / 2)
        print(f"  {label:15s}: {cnt:3d}일 ({pct:5.1f}%) {bar}")

    print(f"\n{'='*60}")

    return df


def save_results(df: pd.DataFrame, path: str):
    """결과 CSV 저장"""
    out = df.copy()
    if "month" in out.columns:
        out = out.drop(columns=["month"])
    out.to_csv(path, encoding="utf-8-sig")
    print(f"\n결과 저장: {path}")


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="NIGHTWATCH 블라인드 백테스트")
    parser.add_argument("--start", default="2025-03-01", help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-03-01", help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--output", default=None, help="결과 CSV 경로")
    args = parser.parse_args()

    df = run_backtest(start=args.start, end=args.end)

    if not df.empty:
        print_report(df)

        out_path = args.output or str(
            Path(__file__).resolve().parent / "nightwatch_backtest_result.csv"
        )
        save_results(df, out_path)
