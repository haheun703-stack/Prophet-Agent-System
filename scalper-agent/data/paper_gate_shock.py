# -*- coding: utf-8 -*-
"""급락 선행 게이트 헬퍼 (6/27 신설) — 공매도/신용 진입 회피 판정.

배경:
  - 페이퍼 수익률 박살 주범 = 6/22 폭탄(공매도 과열 종목 -45%·-39%).
  - 진입 전 공매도 비중/급증·신용 과다를 보면 그런 폭탄을 사전 회피 가능(데이터 입증).

★ look-ahead 방지: 게이트는 진입일(virtual_entry_date) **이전** 거래일 데이터만 본다.
  (진입 종가 시점엔 당일 공매도·신용 집계 전이므로, 미래 정보 사용 금지)

★ 안전: read-only(csv 읽기만). 매매/주문/SAJANG/picks 0 접촉. paper_rule_shadow(연습장부)
  전용 보조. 데이터 없으면 None 반환 → 호출측에서 "유지"(게이트 안 걸림·보수적).
"""
from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import pandas as pd

logger = logging.getLogger("BH.PaperGateShock")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
_SUFFIX = {"short": "short_bal", "credit": "credit_bal"}


@lru_cache(maxsize=4096)
def _load(kind: str, ticker: str):
    """공매도/신용 일별 csv → date index DataFrame(정렬). 없으면 None."""
    f = DATA_DIR / kind / f"{ticker}_{_SUFFIX[kind]}.csv"
    if not f.exists():
        return None
    try:
        df = pd.read_csv(f, index_col=0, parse_dates=True)
        return df.sort_index()
    except Exception:
        return None


def _prior(kind: str, ticker: str, date: str, win: int = 0):
    """date 이전(< date) 데이터. win=0이면 직전 1행, win>0이면 직전 win행."""
    df = _load(kind, ticker)
    if df is None or df.empty:
        return None
    prior = df[df.index < pd.Timestamp(date)]
    if prior.empty:
        return None
    return prior if win else prior.iloc[-1]


def short_ratio(ticker: str, date: str):
    """진입 직전 공매도 거래비중(%). 없으면 None."""
    r = _prior("short", ticker, date)
    if r is None or "short_ratio" not in r:
        return None
    try:
        return float(r["short_ratio"])
    except Exception:
        return None


def short_surge(ticker: str, date: str, win: int = 5):
    """진입 직전 공매도 비중 / 직전 win일 평균 = 급증 배수. 없으면 None."""
    prior = _prior("short", ticker, date, win=win + 1)
    if prior is None or len(prior) < win + 1 or "short_ratio" not in prior:
        return None
    last = float(prior.iloc[-1]["short_ratio"])
    base = prior.iloc[-(win + 1):-1]["short_ratio"].astype(float).mean()
    return (last / base) if base and base > 0 else None


def credit_rate(ticker: str, date: str):
    """진입 직전 신용융자 잔고율(%). 없으면 None."""
    r = _prior("credit", ticker, date)
    if r is None or "credit_buy_rate" not in r:
        return None
    try:
        return float(r["credit_buy_rate"])
    except Exception:
        return None


# ── keep 판정 헬퍼 (True=유지, False=게이트로 제외) ──
# 데이터 없음(None) → 유지(보수적, 정보 없을 때 거르지 않음)

def keep_short_ratio_below(ticker: str, date: str, thr: float) -> bool:
    v = short_ratio(ticker, date)
    return True if v is None else v < thr


def keep_short_surge_below(ticker: str, date: str, thr: float, win: int = 5) -> bool:
    v = short_surge(ticker, date, win)
    return True if v is None else v < thr


def keep_credit_below(ticker: str, date: str, thr: float) -> bool:
    v = credit_rate(ticker, date)
    return True if v is None else v < thr
