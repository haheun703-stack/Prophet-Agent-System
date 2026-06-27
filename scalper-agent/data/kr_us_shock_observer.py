# -*- coding: utf-8 -*-
"""한미 충격 비교(kr_us_shock) 관측기 (6/27 신설) — 정보봇 verdict record-only 적재.

배경:
  - 정보봇(JGIS)이 매 평일 daily_intelligence.json 에 kr_us_shock_summary
    (한국 vs 미국 시장충격 비교 — verdict / kr_shock / us_shock / drivers)를 공급한다.
  - 단타봇은 daily_intelligence 를 읽는 통로는 있으나(premarket_risk_scanner 등)
    이 필드를 꺼내 쓰지 않았다(신호는 왔는데 통로 따로놂 — 사장님 6/7 진단).
  - 한미 충격 verdict(한국 취약)는 변동성 장 급락 선행지표로, 오늘 추가한
    공매도/신용과 결이 같다. record-only 로 관측 누적 → 추후 통합 리스크 게이트
    검토(관측 없이 flip 금지).

안전(불변식):
  - 봇 OFF·실주문0·매도무손상·picks불변·SAJANG무접촉·★매매경로 0접촉★. 읽기+csv기록만.
  - daily_intelligence 에 필드 없으면 graceful skip(에러 아님). 휴장일 skip(RULE-002).

사용법: python -m data.kr_us_shock_observer
"""
import sys
import json
import logging
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from data.trading_calendar import is_trading_day  # noqa: E402
from utils.jgis_path import jgis_intel_path  # noqa: E402

logger = logging.getLogger(__name__)

DATA_DIR = ROOT / "data_store"
OBS_CSV = DATA_DIR / "kr_us_shock_observed.csv"


def _parse_summary(ks: dict, intel_date: str, generated_at: str) -> dict:
    """kr_us_shock_summary(요약 dict) → 관측 행. 방어적 파싱(구조 유연 대응)."""
    drivers = ks.get("drivers", []) or []
    ch = ks.get("channels", {}) or {}

    def _ch(name, key):
        v = ch.get(name, {})
        return v.get(key) if isinstance(v, dict) else None

    return {
        "date": intel_date,
        "verdict": ks.get("verdict", ""),
        "kr_shock": ks.get("kr_shock"),
        "us_shock": ks.get("us_shock"),
        "diff": ks.get("diff"),
        # channels 가 요약에 포함되면 함께 관측(없으면 None — graceful)
        "fx_pressure": _ch("fx", "pressure"),
        "rate_pressure": _ch("rate", "pressure"),
        "vol_score": _ch("vol", "score"),
        "decouple_score": _ch("decouple", "score"),
        "driver1": drivers[0] if len(drivers) > 0 else "",
        "driver2": drivers[1] if len(drivers) > 1 else "",
        "driver3": drivers[2] if len(drivers) > 2 else "",
        "observed_at": generated_at,
    }


def collect_kr_us_shock_observation() -> dict:
    """daily_intelligence.json 의 kr_us_shock_summary 를 record-only 로 관측 누적.

    record-only — 매수/매도/주문/SAJANG/picks 어디도 접촉하지 않는다(읽기+csv기록만).
    """
    today_d = date.today()
    if not is_trading_day(today_d):
        logger.info("[KRUS] 휴장일 — skip")
        return {"status": "skip_holiday"}

    intel = jgis_intel_path()
    if not intel.exists():
        logger.warning(f"[KRUS] daily_intelligence 없음: {intel}")
        return {"status": "no_intel"}

    try:
        raw = json.loads(intel.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[KRUS] daily_intelligence 로드 실패: {e}")
        return {"status": "load_error"}

    ks = raw.get("kr_us_shock_summary")
    if not ks:
        logger.info("[KRUS] kr_us_shock_summary 미수신(정보봇 평일 08:00 공급 전) — skip")
        return {"status": "no_field"}

    intel_date = raw.get("date", today_d.isoformat())
    row = _parse_summary(ks, intel_date, raw.get("generated_at", ""))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame([row])
    if OBS_CSV.exists():
        try:
            old = pd.read_csv(OBS_CSV, encoding="utf-8-sig")  # BOM 통일(멱등성 보호)
            df = pd.concat([old, df_new], ignore_index=True)
        except Exception:  # noqa: BLE001
            df = df_new
    else:
        df = df_new
    df = df.drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df.to_csv(OBS_CSV, index=False, encoding="utf-8-sig")

    logger.info(
        f"[KRUS] 관측 적재: {intel_date} verdict={row['verdict']} "
        f"kr={row['kr_shock']} us={row['us_shock']} diff={row['diff']}"
    )
    return {"status": "ok", "row": row}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    print(collect_kr_us_shock_observation())
