"""주목 종목 박스 — 시나리오별 매수 후보 통합

7개 데이터 소스를 합쳐 시나리오별 주목 종목 최대 15개를 선정한다.
매일 G7(17:00) 이후 자동 갱신 → Supabase + 텔레그램.

데이터 소스:
  1. bottom_scan.json   — 52주 고점 대비 낙폭 우량주 (바닥매수)
  2. sector_flow.json   — 17섹터 수급 점수 (쌍매수/쌍매도)
  3. stealth_scan.json  — 선매집 포착 (stealth + moving + surged)
  4. fib_leaders.json   — 피보나치 지지선 근처 대형주
  5. recommendation.json — 모닝추천 상위 종목
  6. nationality CSV     — 외국인/기관 연속 매수일
  7. insights.json       — 학습 기반 소스 신뢰도

시나리오:
  - 휴전/재건: 건설, 인프라, 시멘트, 철강, 중장비
  - 전쟁격화: 방산, 정유, 원자재, 금
  - 금리인하: 2차전지, 반도체, 성장주, 리츠
  - 수급포착: 시나리오 무관, 기관/외국인 선매집 감지

Author: Body Hunter v4
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"

# ══════════════════════════════════════════════════════
#  시나리오 매핑 (섹터 → 시나리오 태그)
# ══════════════════════════════════════════════════════

SCENARIO_MAP = {
    "휴전/재건": {
        "sectors": ["건설", "건설/플랜트", "시멘트", "철강", "인프라", "중공업", "조선"],
        "keywords": ["건설", "시멘트", "철강", "E&A", "DL이앤씨", "GS건설", "현대건설",
                     "대우건설", "HDC현대산업", "쌍용C&E", "한일시멘트", "성신양회",
                     "포스코", "현대제철", "동국제강", "두산에너빌리티",
                     "HD한국조선", "삼성중공업", "한화오션"],
        "emoji": "\U0001f3d7\ufe0f",  # 🏗️
        "max_slots": 5,
    },
    "전쟁격화": {
        "sectors": ["방산", "에너지", "정유", "자원"],
        "keywords": ["한화에어로", "현대로템", "LIG넥스원", "한국항공우주", "풍산",
                     "한화", "S-Oil", "SK이노베이션", "GS칼텍스",
                     "한국석유", "금", "TIGER 금선물"],
        "emoji": "\u2694\ufe0f",  # ⚔️
        "max_slots": 3,
    },
    "금리인하": {
        "sectors": ["반도체/AI", "2차전지", "바이오", "성장주", "리츠"],
        "keywords": ["삼성전자", "SK하이닉스", "한미반도체", "ISC", "원익",
                     "LG에너지솔루션", "삼성SDI", "에코프로", "포스코퓨처M",
                     "엘앤에프", "셀트리온", "삼성바이오로직스", "알테오젠",
                     "리가켐바이오", "네이버", "카카오"],
        "emoji": "\U0001f4c9",  # 📉
        "max_slots": 5,
    },
    "수급포착": {
        "sectors": [],  # 시나리오 무관 — 수급 자체로 선별
        "keywords": [],
        "emoji": "\U0001f50d",  # 🔍
        "max_slots": 5,
    },
}


@dataclass
class WatchboxStock:
    """주목 종목 1건."""
    code: str
    name: str
    scenario: str  # 시나리오 태그
    price: int = 0  # 현재가
    drop_from_high: float = 0.0  # 52주 고점 대비 낙폭 %
    supply_status: str = ""  # 수급 상태 (기관3일연속, 쌍매수 등)
    entry_zone: str = ""  # 진입 구간 (피보나치 등)
    fib_position: str = ""  # 피보나치 위치
    score: float = 0.0  # 통합 점수 (내부 정렬용)
    sector: str = ""
    cap: int = 0  # 시총 (억)
    memo: str = ""  # 짧은 메모
    sources: List[str] = field(default_factory=list)  # 데이터 소스 태그


def _load_json(filename: str) -> Optional[dict | list]:
    """data_store에서 JSON 로드."""
    p = DATA_STORE / filename
    if not p.exists():
        logger.warning(f"[WatchBox] {filename} 없음")
        return None
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception as e:
        logger.error(f"[WatchBox] {filename} 로드 실패: {e}")
        return None


def _match_scenario(name: str, sector: str) -> str:
    """종목명/섹터로 시나리오 매칭. 매칭 안 되면 빈 문자열."""
    for scenario, cfg in SCENARIO_MAP.items():
        if scenario == "수급포착":
            continue  # 수급포착은 별도 로직
        # 섹터 매칭
        for s in cfg["sectors"]:
            if s in sector:
                return scenario
        # 키워드 매칭
        for kw in cfg["keywords"]:
            if kw in name:
                return scenario
    return ""


def _get_flow_status(code: str, sector_flow_data: list) -> str:
    """sector_flow에서 해당 종목 섹터의 수급 상태 반환."""
    # sector_flow는 섹터 단위라 종목 단위 매칭은 어려움
    # nationality CSV에서 직접 조회하는 게 정확
    return ""


def _read_investor_csv(code: str) -> Dict:
    """종목별 investor CSV에서 최근 수급 분석."""
    csv_path = DATA_STORE / "flow" / f"{code}_investor.csv"
    if not csv_path.exists():
        return {}
    try:
        lines = csv_path.read_text("utf-8").strip().split("\n")
        if len(lines) < 3:
            return {}

        header = lines[0].split(",")
        # 최근 5일 데이터
        recent = []
        for line in lines[-5:]:
            vals = line.split(",")
            if len(vals) >= len(header):
                row = dict(zip(header, vals))
                recent.append(row)

        if not recent:
            return {}

        # 기관/외국인 연속 매수일 계산
        inst_consec = 0
        frgn_consec = 0
        for row in reversed(recent):
            inst_val = float(row.get("기관_금액", row.get("inst_amount", 0)) or 0)
            frgn_val = float(row.get("외국인_금액", row.get("frgn_amount", 0)) or 0)
            if inst_val > 0:
                inst_consec += 1
            else:
                break
        for row in reversed(recent):
            frgn_val = float(row.get("외국인_금액", row.get("frgn_amount", 0)) or 0)
            if frgn_val > 0:
                frgn_consec += 1
            else:
                break

        status_parts = []
        if inst_consec >= 2:
            status_parts.append(f"기관{inst_consec}일연속")
        if frgn_consec >= 2:
            status_parts.append(f"외인{frgn_consec}일연속")
        if inst_consec >= 1 and frgn_consec >= 1:
            status_parts.append("쌍매수")

        return {
            "inst_consec": inst_consec,
            "frgn_consec": frgn_consec,
            "supply_status": " ".join(status_parts),
        }
    except Exception as e:
        logger.debug(f"[WatchBox] {code} CSV 읽기 실패: {e}")
        return {}


def build_watchbox() -> Dict:
    """7개 소스를 통합하여 주목 종목 박스를 생성한다.

    Returns:
        {
            "timestamp": "2026-04-08 17:30",
            "scenarios": {
                "휴전/재건": [...],
                "전쟁격화": [...],
                "금리인하": [...],
                "수급포착": [...],
            },
            "total_count": 15,
            "stocks": [...],  # 전체 flat list
        }
    """
    now = datetime.now()
    logger.info("[WatchBox] 주목 종목 박스 생성 시작...")

    # ── 1. 데이터 로드 ──
    bottom_scan = _load_json("bottom_scan.json") or []
    sector_flow = _load_json("sector_flow.json") or {}
    stealth_scan = _load_json("stealth_scan.json") or {}
    fib_leaders = _load_json("fib_leaders.json") or []
    recommendation = _load_json("recommendation.json") or {}
    insights = _load_json("learning/insights.json") or {}

    # 섹터 수급 데이터 (쌍매수 섹터 추출)
    sf_sectors = sector_flow.get("sectors", [])
    dual_buy_sectors = set()
    for s in sf_sectors:
        if isinstance(s, dict) and "쌍매수" in s.get("agreement", ""):
            dual_buy_sectors.add(s.get("sector", ""))
            for alias in [s.get("alias", "")]:
                if alias:
                    dual_buy_sectors.add(alias)

    # 선매집 종목 코드 세트
    stealth_codes = {}
    for key in ["stealth", "moving", "surged"]:
        for st in stealth_scan.get(key, []):
            if isinstance(st, dict) and st.get("code"):
                stealth_codes[st["code"]] = st

    # 추천 종목 코드 세트
    rec_stocks = {}
    for s in recommendation.get("stocks", []):
        if isinstance(s, dict) and s.get("code"):
            rec_stocks[s["code"]] = s

    # 피보나치 종목 코드 세트
    fib_map = {}
    for f in fib_leaders:
        if isinstance(f, dict) and f.get("code"):
            fib_map[f["code"]] = f

    # ── 2. 후보 생성 (bottom_scan 기반 — 가장 포괄적) ──
    candidates: Dict[str, WatchboxStock] = {}

    # 2a. bottom_scan: 낙폭 우량주
    for bs in bottom_scan:
        code = bs.get("code", "")
        name = bs.get("name", "")
        if not code or not name:
            continue

        cap = bs.get("cap", 0)
        if cap < 5000:  # 시총 5000억 미만 제외 (충분한 유동성)
            continue

        scenario = _match_scenario(name, bs.get("sector", ""))
        if not scenario:
            continue

        drop = bs.get("drop", 0)
        price = bs.get("price", 0)
        fib_zone = bs.get("fib_zone", "")
        fib_status = bs.get("fib_status", "")

        # 기본 점수: 낙폭이 클수록 + 시총이 클수록
        score = abs(drop) * 0.4 + min(cap / 1000, 30) * 0.3

        # 피보나치 지지선 근처면 가점
        if fib_zone == "DEEP":
            score += 10
        elif fib_zone == "MID":
            score += 5

        sources = ["바닥스캔"]

        # 수급 데이터 보강
        flow_info = _read_investor_csv(code)
        supply_status = flow_info.get("supply_status", "")
        if "쌍매수" in supply_status:
            score += 15
            sources.append("쌍매수")
        elif "기관" in supply_status and "연속" in supply_status:
            score += 8
            sources.append("기관매수")
        elif "외인" in supply_status and "연속" in supply_status:
            score += 8
            sources.append("외인매수")

        # 선매집 감지 보너스
        if code in stealth_codes:
            st_info = stealth_codes[code]
            score += 12
            supply_status += (" " if supply_status else "") + f"선매집({st_info.get('pattern', '')})"
            sources.append("선매집")

        # 피보나치 리더 보너스
        if code in fib_map:
            score += 5
            sources.append("피보나치")

        # 추천 종목 보너스 (점수 비례 — 100점 이상이면 강한 가점)
        if code in rec_stocks:
            rec_score = rec_stocks[code].get("total_score", 0)
            if rec_score >= 100:
                _rec_bonus = 15 + (rec_score - 100) * 0.1  # 100점=15, 130점=18
                score += _rec_bonus
                sources.append(f"추천({rec_score:.0f})")
            elif rec_score > 50:
                score += 8
                sources.append(f"추천({rec_score:.0f})")

        # 진입 구간 계산
        fib_382 = bs.get("fib_382", 0)
        fib_500 = bs.get("fib_500", 0)
        if price and fib_382:
            if price < fib_382:
                entry_zone = f"현재 {price:,} (38.2% {fib_382:,} 아래 = 바닥권)"
            elif price < fib_500:
                entry_zone = f"현재 {price:,} (38.2%~50% 구간)"
            else:
                entry_zone = f"현재 {price:,} (50% 위 = 반등 진행 중)"
        else:
            entry_zone = f"현재 {price:,}"

        candidates[code] = WatchboxStock(
            code=code,
            name=name,
            scenario=scenario,
            price=price,
            drop_from_high=round(drop, 1),
            supply_status=supply_status.strip(),
            entry_zone=entry_zone,
            fib_position=fib_status,
            score=round(score, 1),
            sector=bs.get("sector", ""),
            cap=cap,
            sources=sources,
        )

    # 2b. 선매집 종목 중 bottom_scan에 없는 것도 "수급포착" 시나리오로 추가
    for code, st_info in stealth_codes.items():
        if code in candidates:
            continue  # 이미 시나리오 배정됨

        name = st_info.get("name", "")
        cap = st_info.get("cap", 0)
        if cap < 3000:  # 선매집은 시총 3000억 이상
            continue

        st_score = st_info.get("score", 0)
        dual_buy = st_info.get("dual_buy", False)

        score = st_score * 0.3
        if dual_buy:
            score += 15

        # supply_status 조합 (중복 "쌍매수" 방지)
        _parts = []
        _pattern = st_info.get("pattern", "")
        if _pattern:
            _parts.append(_pattern)
        flow_info = _read_investor_csv(code)
        _flow_st = flow_info.get("supply_status", "")
        if _flow_st:
            # _flow_st에 이미 "쌍매수"가 있으면 dual_buy 중복 추가 불필요
            _parts.append(_flow_st)
        elif dual_buy:
            _parts.append("쌍매수")
        supply_status = " ".join(_parts)

        # 가격 보강: stealth에 price 없으면 investor CSV 마지막 종가
        st_price = st_info.get("price", 0)
        if not st_price:
            csv_p = DATA_STORE / "flow" / f"{code}_investor.csv"
            if csv_p.exists():
                try:
                    last_line = csv_p.read_text("utf-8").strip().split("\n")[-1]
                    header = csv_p.read_text("utf-8").strip().split("\n")[0].split(",")
                    vals = last_line.split(",")
                    row = dict(zip(header, vals))
                    st_price = int(float(row.get("종가", 0)))
                except Exception:
                    pass
            # fib_leaders에서도 시도
            if not st_price and code in fib_map:
                st_price = fib_map[code].get("price", 0)

        sources = ["선매집"]
        if dual_buy:
            sources.append("쌍매수")
        if code in fib_map:
            score += 5
            sources.append("피보나치")
        if code in rec_stocks:
            score += 8
            sources.append("추천")

        candidates[code] = WatchboxStock(
            code=code,
            name=name,
            scenario="수급포착",
            price=st_price,
            supply_status=supply_status.strip(),
            score=round(score, 1),
            sector=st_info.get("sector", ""),
            cap=cap,
            sources=sources,
        )

    # ── 2c. 모닝추천 TOP2 강제 포함 ──
    # 추천 1~2위는 어떤 시나리오든 반드시 박스에 포함 (점수 부스트로 슬롯 경쟁 보장)
    rec_top = sorted(rec_stocks.values(), key=lambda x: x.get("total_score", 0), reverse=True)[:2]
    for rt in rec_top:
        code = rt.get("code", "")
        if not code:
            continue
        rec_score_val = rt.get("total_score", 0)
        if code in candidates:
            # 이미 있으면 점수 대폭 부스트 + 추천TOP 태그 추가
            c = candidates[code]
            _boost = max(20, rec_score_val * 0.2)  # 최소 20점, 130점추천이면 26점
            c.score += _boost
            c.score = round(c.score, 1)
            if "추천TOP" not in c.sources:
                c.sources.insert(0, "추천TOP")
            c.memo = "모닝추천 상위"
            logger.info(f"[WatchBox] 추천TOP 부스트: {c.name}({code}) +{_boost:.0f} → {c.score}")
        else:
            name = rt.get("name", "")
            scenario = _match_scenario(name, rt.get("sector", ""))
            if not scenario:
                scenario = "수급포착"

            flow_info = _read_investor_csv(code)
            supply_status = flow_info.get("supply_status", "")

            candidates[code] = WatchboxStock(
                code=code,
                name=name,
                scenario=scenario,
                price=rt.get("close", 0),
                score=round(rec_score_val * 0.3, 1),
                sector=rt.get("sector", ""),
                supply_status=supply_status,
                sources=["추천TOP", f"추천({rec_score_val:.0f})"],
                memo="모닝추천 상위",
            )
            logger.info(f"[WatchBox] 추천TOP 신규: {name}({code}) score={rec_score_val:.0f}")

    # ── 3. 시나리오별 정렬 + 슬롯 할당 ──
    result_scenarios: Dict[str, List[Dict]] = {}
    all_stocks: List[Dict] = []

    for scenario, cfg in SCENARIO_MAP.items():
        max_slots = cfg["max_slots"]
        emoji = cfg["emoji"]

        # 해당 시나리오 종목만 필터
        scenario_stocks = [
            s for s in candidates.values()
            if s.scenario == scenario
        ]
        # 점수 내림차순 정렬
        scenario_stocks.sort(key=lambda x: x.score, reverse=True)
        # 슬롯 제한
        selected = scenario_stocks[:max_slots]

        result_list = []
        for s in selected:
            d = asdict(s)
            d["emoji"] = emoji
            result_list.append(d)
            all_stocks.append(d)

        result_scenarios[scenario] = result_list
        logger.info(f"[WatchBox] {emoji} {scenario}: {len(selected)}/{len(scenario_stocks)}종목 선정")

    # ── 4. 결과 저장 ──
    result = {
        "timestamp": now.strftime("%Y-%m-%d %H:%M"),
        "total_count": len(all_stocks),
        "scenarios": result_scenarios,
        "stocks": all_stocks,
    }

    out_path = DATA_STORE / "watchbox.json"
    try:
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"[WatchBox] watchbox.json 저장 완료 ({len(all_stocks)}종목)")
    except Exception as e:
        logger.error(f"[WatchBox] watchbox.json 저장 실패: {e}")

    return result


def format_watchbox_telegram(data: Optional[Dict] = None) -> str:
    """텔레그램 출력용 포맷."""
    if data is None:
        data = _load_json("watchbox.json")
    if not data:
        return "📦 주목 종목 박스가 아직 생성되지 않았습니다."

    lines = [
        f"📦 주목 종목 박스",
        f"📅 {data.get('timestamp', '?')} | {data.get('total_count', 0)}종목",
        "",
    ]

    for scenario, stocks in data.get("scenarios", {}).items():
        cfg = SCENARIO_MAP.get(scenario, {})
        emoji = cfg.get("emoji", "📌")

        if not stocks:
            continue

        lines.append(f"{emoji} [{scenario}]")
        lines.append("─" * 28)

        for i, s in enumerate(stocks, 1):
            name = s.get("name", "?")
            price = s.get("price", 0)
            drop = s.get("drop_from_high", 0)
            supply = s.get("supply_status", "")
            cap = s.get("cap", 0)

            # 컴팩트 표시
            drop_str = f"▼{abs(drop):.0f}%" if drop < 0 else f"▲{drop:.0f}%"
            cap_str = f"{cap/10000:.1f}조" if cap >= 10000 else f"{cap:,}억"

            line = f"  {i}. {name}  {price:,}원  {drop_str}"
            if supply:
                line += f"\n     💡 {supply}"
            line += f"\n     시총 {cap_str} | {' '.join(s.get('sources', []))}"

            lines.append(line)

        lines.append("")

    lines.append("⚠️ 주목 종목 ≠ 매수 신호. 진입은 퐝가님 판단으로!")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════
#  Supabase 업로드
# ══════════════════════════════════════════════════════

def upload_watchbox_supabase(data: Optional[Dict] = None) -> bool:
    """watchbox 데이터를 Supabase에 업로드."""
    if data is None:
        data = _load_json("watchbox.json")
    if not data or not data.get("stocks"):
        logger.warning("[WatchBox] 업로드할 데이터 없음")
        return False

    try:
        from data.flowx_content import _get_client
        sb = _get_client()
        if not sb:
            logger.error("[WatchBox] Supabase 클라이언트 없음")
            return False

        today = datetime.now().strftime("%Y-%m-%d")
        rows = []
        for s in data["stocks"]:
            rows.append({
                "date": today,
                "code": s["code"],
                "name": s["name"],
                "scenario": s["scenario"],
                "price": s.get("price", 0),
                "drop_from_high": s.get("drop_from_high", 0),
                "supply_status": s.get("supply_status", ""),
                "entry_zone": s.get("entry_zone", ""),
                "fib_position": s.get("fib_position", ""),
                "score": s.get("score", 0),
                "sector": s.get("sector", ""),
                "cap": s.get("cap", 0),
                "memo": s.get("memo", ""),
                "sources": s.get("sources", []),
            })

        # upsert (date + code 복합키)
        sb.table("watchbox").upsert(
            rows, on_conflict="date,code"
        ).execute()
        logger.info(f"[WatchBox] Supabase 업로드 완료 ({len(rows)}건)")
        return True

    except Exception as e:
        logger.error(f"[WatchBox] Supabase 업로드 실패: {e}")
        return False


# ══════════════════════════════════════════════════════
#  메인 (테스트용)
# ══════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = build_watchbox()
    print(format_watchbox_telegram(result))
