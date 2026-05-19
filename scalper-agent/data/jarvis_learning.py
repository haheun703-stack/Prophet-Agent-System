# -*- coding: utf-8 -*-
"""자비스 학습 모듈 — 섹터 추격 타이밍 자기 진화

사장님 5/19 23:00 최종 명령:
> "섹터 추격 타이밍은 니가 터득해야 한다. 매일 매매 + 학습까지 해야 한다."

매일 장 마감 후(15:40 KST) 자비스 자기 회고:
  1. 오늘 자비스 매수 종목 → 섹터별 집계
  2. 오늘 시장 강세 섹터 (sector_flow.json 기관/외인 매수 우위)
  3. 자비스 진입 섹터 vs 진짜 강세 섹터 일치도
  4. 인사이트 추출 (어떤 패턴이 통했나)
  5. 누적 로그 → 5/21+ 자비스 진화의 토대

출력: data_store/jarvis_learning/journal_YYYYMMDD.json
누적: data_store/jarvis_learning/sector_timing_insights.json (전체 인사이트)

이 모듈은 학습 데이터 누적만. 의사결정에는 직접 영향 X.
5/21+ 자비스가 이 데이터를 보고 자율 진화 (별도 의사결정 모듈).
"""
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = _ROOT / "data_store"
_LEARN_DIR = _DATA_DIR / "jarvis_learning"
_LEARN_DIR.mkdir(parents=True, exist_ok=True)


def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def _today_compact() -> str:
    return date.today().strftime("%Y%m%d")


def _load_positions() -> Dict:
    p = _DATA_DIR / "positions.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_sector_flow() -> Optional[Dict]:
    p = _DATA_DIR / "sector_flow.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_universe() -> Dict:
    p = _DATA_DIR / "universe.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_trade_journal_today() -> List[Dict]:
    """오늘자 trade_journal에서 자비스 매수/매도 추출."""
    # trade_journal 위치는 별도 모듈이 관리하지만 fallback으로 직접 파일 시도
    candidates = [
        _DATA_DIR / "trade_journal.json",
        _DATA_DIR / f"trade_journal_{_today_compact()}.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            items = data if isinstance(data, list) else data.get("trades", [])
            today = _today_str()
            return [
                t for t in items
                if isinstance(t, dict) and str(t.get("date", t.get("timestamp", ""))).startswith(today)
            ]
        except Exception as e:
            logger.warning(f"[jarvis_learning] trade_journal 로드 실패 {path}: {e}")
    return []


def analyze_today() -> Dict:
    """오늘 자비스 매매 + 시장 상황 회고 분석.

    Returns:
        {
            "date": "2026-05-20",
            "positions_today": [...],          # 오늘 자비스가 보유한 종목
            "buy_sectors": {sector: count},     # 자비스가 진입한 섹터별 종목 수
            "strong_sectors_today": [...],      # 오늘 진짜 강세 섹터 (기관+외인 매수)
            "weak_sectors_today": [...],        # 약세 섹터
            "alignment_score": float,           # 0~100, 자비스 진입 섹터가 강세와 얼마나 일치
            "insights": [...],                  # 자비스 인사이트
        }
    """
    today = _today_str()
    result = {
        "date": today,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "positions_today": [],
        "buy_sectors": {},
        "strong_sectors_today": [],
        "weak_sectors_today": [],
        "alignment_score": 0.0,
        "insights": [],
    }

    # ── 1) 자비스가 들고 있는 종목 (asset_pool source) ──
    positions = _load_positions()
    universe = _load_universe()
    asset_pool_positions = []
    for code, pos in positions.items():
        if pos.get("source") == "asset_pool":
            sector = "기타"
            if code in universe and isinstance(universe[code], dict):
                sector = universe[code].get("sector", "기타")
            asset_pool_positions.append({
                "code": code,
                "name": pos.get("name", code),
                "sector": sector,
                "entry_price": pos.get("entry_price", 0),
                "high_watermark": pos.get("high_watermark", 0),
                "entry_date": pos.get("entry_date", ""),
            })
    result["positions_today"] = asset_pool_positions

    # 진입 섹터 집계
    sector_count = {}
    for p in asset_pool_positions:
        sec = p["sector"]
        sector_count[sec] = sector_count.get(sec, 0) + 1
    result["buy_sectors"] = sector_count

    # ── 2) 오늘 시장 강세/약세 섹터 (sector_flow 기준) ──
    sf = _load_sector_flow()
    if sf and isinstance(sf, dict):
        sectors = sf.get("sectors", [])
        # 기관 1d + 외인 1d 합산 (억 단위) → 양수=매수우위=강세
        def _score(s):
            return float(s.get("inst_1d", 0) or 0) + float(s.get("foreign_1d", 0) or 0)

        scored = [(s.get("sector") or s.get("alias", "?"), _score(s)) for s in sectors]
        scored.sort(key=lambda x: -x[1])

        # 상위 5 / 하위 5
        result["strong_sectors_today"] = [
            {"sector": name, "net_flow_억": round(sc, 1)}
            for name, sc in scored[:5] if sc > 0
        ]
        result["weak_sectors_today"] = [
            {"sector": name, "net_flow_억": round(sc, 1)}
            for name, sc in scored[-5:] if sc < 0
        ]

    # ── 3) 일치도 점수 ──
    strong_set = {s["sector"] for s in result["strong_sectors_today"]}
    if asset_pool_positions and strong_set:
        hit = sum(1 for p in asset_pool_positions if p["sector"] in strong_set)
        result["alignment_score"] = round(hit / len(asset_pool_positions) * 100, 1)
    elif not asset_pool_positions:
        result["alignment_score"] = -1  # 매수 0건 (학습 데이터 없음)

    # ── 4) 인사이트 추출 ──
    insights = []
    if not asset_pool_positions:
        insights.append("오늘 자비스 매수 0건 — 게이트 차단 또는 후보 부족. 학습 데이터 없음.")
    else:
        insights.append(f"자비스 진입 {len(asset_pool_positions)}종목 / 섹터 분포: {sector_count}")
        if result["alignment_score"] >= 60:
            insights.append(
                f"✅ 섹터 추격 일치도 {result['alignment_score']}% — 자비스 진입 섹터가 강세와 일치"
            )
        elif result["alignment_score"] >= 30:
            insights.append(
                f"⚠️ 섹터 추격 일치도 {result['alignment_score']}% — 절반은 맞춤, 절반은 빗나감"
            )
        else:
            insights.append(
                f"❌ 섹터 추격 일치도 {result['alignment_score']}% — 자비스 진입 섹터가 약세 / 학습 필요"
            )

        # 약세 섹터에 진입한 경우 경고
        weak_set = {s["sector"] for s in result["weak_sectors_today"]}
        weak_entries = [p for p in asset_pool_positions if p["sector"] in weak_set]
        if weak_entries:
            names = ", ".join(f"{p['name']}({p['sector']})" for p in weak_entries[:3])
            insights.append(f"🚨 약세 섹터 진입: {names}")

    result["insights"] = insights
    return result


def save_journal(analysis: Dict) -> Path:
    """일자별 회고 일지 저장."""
    path = _LEARN_DIR / f"journal_{_today_compact()}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)
    logger.info(f"[jarvis_learning] 일지 저장: {path.name}")
    return path


def update_insights_log(analysis: Dict) -> Path:
    """누적 인사이트 로그 갱신 — 5/21+ 자비스가 참조."""
    log_path = _LEARN_DIR / "sector_timing_insights.json"
    entries = []
    if log_path.exists():
        try:
            entries = json.loads(log_path.read_text(encoding="utf-8"))
            if not isinstance(entries, list):
                entries = []
        except Exception:
            entries = []

    entry = {
        "date": analysis["date"],
        "alignment_score": analysis["alignment_score"],
        "buy_sectors": analysis["buy_sectors"],
        "strong_sectors": [s["sector"] for s in analysis.get("strong_sectors_today", [])],
        "weak_sectors": [s["sector"] for s in analysis.get("weak_sectors_today", [])],
        "position_count": len(analysis["positions_today"]),
        "insights": analysis["insights"],
    }

    # 같은 날짜 중복 제거
    entries = [e for e in entries if e.get("date") != analysis["date"]]
    entries.append(entry)
    # 최근 90일만 유지
    entries = sorted(entries, key=lambda x: x.get("date", ""), reverse=True)[:90]

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    logger.info(f"[jarvis_learning] 누적 인사이트: {len(entries)}일치")
    return log_path


def run_daily() -> Dict:
    """trading_coo가 15:40에 호출. 회고 + 누적 + 텔레그램용 요약."""
    logger.info("[jarvis_learning] 일일 회고 시작...")
    analysis = analyze_today()
    save_journal(analysis)
    update_insights_log(analysis)
    logger.info(
        f"[jarvis_learning] 완료 — 일치도={analysis['alignment_score']}% / "
        f"진입={len(analysis['positions_today'])}종목"
    )
    return analysis


def build_telegram_summary(analysis: Dict) -> str:
    """텔레그램용 자비스 회고 메시지."""
    lines = [
        f"🦾 자비스 일일 회고 ({analysis['date']})",
        f"  · 진입 {len(analysis['positions_today'])}종목",
    ]
    if analysis["alignment_score"] >= 0:
        lines.append(f"  · 섹터 일치도: {analysis['alignment_score']}%")
    if analysis["buy_sectors"]:
        sec_str = ", ".join(f"{k}({v})" for k, v in analysis["buy_sectors"].items())
        lines.append(f"  · 진입 섹터: {sec_str}")
    if analysis["strong_sectors_today"]:
        top = analysis["strong_sectors_today"][0]
        lines.append(f"  · 오늘 최강 섹터: {top['sector']} (+{top['net_flow_억']}억)")
    for ins in analysis["insights"][:3]:
        lines.append(f"  {ins}")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = run_daily()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print()
    print(build_telegram_summary(result))
