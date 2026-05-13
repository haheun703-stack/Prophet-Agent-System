"""
Stock Utilities
===============
ETF 판별, 일봉 CSV 로드, KIS 1분봉 파서 등 공용 유틸리티
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── ETF/ETN 키워드 (전체 superset) ──────────────────────────────
ETF_KEYWORDS = (
    "KODEX", "TIGER", "ACE", "KIWOOM", "SOL ", "HANARO", "KOSEF", "ARIRANG",
    "BNK", "PLUS ", "FOCUS", "TIMEFOLIO", "RISE ", "TIME ", "ITF ", "1Q ",
    "KoAct", "WON ", "UNICORN", "Active", "액티브", "KBSTAR",
    "ETF", "ETN", "인버스", "레버리지",
)


def is_etf(name: str) -> bool:
    """ETF/ETN 종목 여부 판별.

    name이 빈 문자열이거나 '?'이면 ETF로 간주 (필터링 대상).
    """
    if not name or name == "?":
        return True
    return any(kw in name for kw in ETF_KEYWORDS)


def load_daily(code: str, daily_dir: Path, min_rows: int = 5) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드.

    Args:
        code: 종목코드
        daily_dir: 일봉 CSV 디렉토리 경로
        min_rows: 최소 필요 행 수 (기본 5)
    """
    path = daily_dir / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < min_rows:
            return None
        return df.sort_index()
    except Exception:
        return None


def parse_kis_1min(rows: List[dict]) -> Optional[pd.DataFrame]:
    """KIS API 1분봉 응답 → DataFrame.

    Args:
        rows: KIS API output 리스트 (stck_bsop_date, stck_cntg_hour, ...)
    """
    records = []
    for r in rows:
        dt_str = r.get("stck_bsop_date", "")
        tm_str = r.get("stck_cntg_hour", "")
        if not dt_str or not tm_str:
            continue
        try:
            ts = pd.Timestamp(
                f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]} "
                f"{tm_str[:2]}:{tm_str[2:4]}:{tm_str[4:6]}"
            )
            records.append({
                "datetime": ts,
                "open": int(r.get("stck_oprc", 0)),
                "high": int(r.get("stck_hgpr", 0)),
                "low": int(r.get("stck_lwpr", 0)),
                "close": int(r.get("stck_prpr", 0)),
                "volume": int(r.get("cntg_vol", 0)),
            })
        except (ValueError, TypeError):
            continue

    if not records:
        return None
    return pd.DataFrame(records).set_index("datetime").sort_index()
