# -*- coding: utf-8 -*-
"""오늘 5/19 D-Day 실시간 급등주 트래커 (관망 모드 학습용)
=========================================================
KIS API → 등락률/거래량/체결강도 TOP 동시 수집 →
시간대별 스냅샷 저장 → 장 마감 후 패턴 추출.

사장님 5/19 결정: 오늘은 매수 X, 관망 + 패턴 학습 + 장 후 피드백.
"""
from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from datetime import datetime

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.kis_trader import KISTrader  # type: ignore

DATA_DIR = ROOT / "data_store" / "surge_learning"
DATA_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("BH.SurgeTracker")


def snapshot_now(top_n: int = 30) -> dict:
    """현재 시점 등락률/거래량/체결강도 TOP 동시 스냅샷."""
    trader = KISTrader()
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")

    snap = {
        "timestamp": ts,
        "fluctuation_top": [],
        "volume_top": [],
        "strength_top": [],
        "errors": [],
    }

    # 1) 등락률 TOP (KOSPI+KOSDAQ 통합)
    try:
        flu = trader.fetch_ranking_fluctuation(market="J", top_n=top_n)
        snap["fluctuation_top"] = flu
    except Exception as e:
        snap["errors"].append(f"fluctuation: {e}")

    # 2) 거래량 TOP
    try:
        vol = trader.fetch_ranking_volume(market="J", top_n=top_n)
        snap["volume_top"] = vol
    except Exception as e:
        snap["errors"].append(f"volume: {e}")

    # 3) 체결강도 TOP
    try:
        stg = trader.fetch_ranking_strength(market="J", top_n=top_n)
        snap["strength_top"] = stg
    except Exception as e:
        snap["errors"].append(f"strength: {e}")

    # 저장: 분단위 파일 + 누적 히스토리
    hh_mm = now.strftime("%H%M")
    file_path = DATA_DIR / f"snap_{now.strftime('%Y%m%d')}_{hh_mm}.json"
    file_path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    return snap


def print_summary(snap: dict):
    """터미널에 요약 출력."""
    print()
    print("=" * 100)
    print(f"📊 5/19 D-Day 라이브 스냅샷 — {snap['timestamp']}")
    print("=" * 100)

    if snap.get("errors"):
        print(f"⚠️ 에러: {snap['errors']}")

    flu = snap.get("fluctuation_top", [])
    print(f"\n🔥 등락률 TOP {len(flu)}건 (KIS fluctuation)")
    print("-" * 100)
    print(f"{'순':<3}{'코드':<8}{'종목명':<18}{'현재가':>10}{'등락률':>9}{'거래량':>15}")
    for i, x in enumerate(flu[:20], 1):
        print(f"{i:<3}{x['code']:<8}{x['name'][:17]:<18}{x['price']:>10,}{x['change_rate']:>8.2f}%{x['volume']:>15,}")

    vol = snap.get("volume_top", [])
    print(f"\n💹 거래량 TOP {len(vol)}건")
    print("-" * 100)
    print(f"{'순':<3}{'코드':<8}{'종목명':<18}{'현재가':>10}{'등락률':>9}{'거래량':>15}")
    for i, x in enumerate(vol[:15], 1):
        ch = x.get("change_rate", x.get("change", 0))
        pr = x.get("price", x.get("close", 0))
        vv = x.get("volume", 0)
        print(f"{i:<3}{x['code']:<8}{x['name'][:17]:<18}{pr:>10,}{ch:>8.2f}%{vv:>15,}")

    stg = snap.get("strength_top", [])
    print(f"\n⚡ 체결강도 TOP {len(stg)}건")
    print("-" * 100)
    print(f"{'순':<3}{'코드':<8}{'종목명':<18}{'체결강도':>10}{'등락률':>9}{'거래량':>15}")
    for i, x in enumerate(stg[:15], 1):
        st = x.get("strength", 0)
        ch = x.get("change_rate", 0)
        vv = x.get("volume", 0)
        print(f"{i:<3}{x['code']:<8}{x['name'][:17]:<18}{st:>10.1f}{ch:>8.2f}%{vv:>15,}")

    # 3개 시그널 합류 종목
    flu_codes = {x["code"] for x in flu}
    vol_codes = {x["code"] for x in vol}
    stg_codes = {x["code"] for x in stg}

    triple = flu_codes & vol_codes & stg_codes
    print(f"\n🎯 3개 시그널 동시 충족 (등락+거래량+체결강도): {len(triple)}건")
    print("-" * 100)
    if triple:
        flu_map = {x["code"]: x for x in flu}
        stg_map = {x["code"]: x for x in stg}
        for code in triple:
            f = flu_map.get(code, {})
            s = stg_map.get(code, {})
            name = f.get("name") or s.get("name", "?")
            print(f"  ★ {name} ({code}) | 등락 {f.get('change_rate',0):.2f}% | "
                  f"체결강도 {s.get('strength',0):.1f} | 거래량 {f.get('volume',0):,}")

    # 2개 시그널 합류 (등락 + 거래량)
    duo = (flu_codes & vol_codes) - triple
    print(f"\n🟡 2개 시그널 (등락+거래량, 체결강도 미포함): {len(duo)}건")
    if duo:
        flu_map = {x["code"]: x for x in flu}
        for code in duo:
            f = flu_map.get(code, {})
            print(f"  - {f.get('name','?')} ({code}) | 등락 {f.get('change_rate',0):.2f}% | 거래량 {f.get('volume',0):,}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    snap = snapshot_now(top_n=30)
    print_summary(snap)
