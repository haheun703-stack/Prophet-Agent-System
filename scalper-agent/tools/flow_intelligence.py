"""v5.0 수급 인텔리전스 — 장중 4회 수급 리포트 + 15:00 bomb 매수 알림.

스케줄:
  10:00  [1차 수급] 외인/기관 순매수 TOP + bomb 교차
  11:20  [2차 수급] 변동 업데이트
  13:20  [3차 수급] 오후장 전환
  14:30  [4차 수급] 최종 흐름
  15:00  ★[매수] bomb TOP + 4주체 상세 + 익절/손절가

데이터 소스:
  - KIS API FHPTJ04400000 (fetch_foreign_inst_total): 장중 가집계
  - bomb_watchlist.json: C35에서 생성된 bomb 후보
  - flow CSV: 전일 4주체 상세 수급
"""
import json
import csv
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("BH.FlowIntel")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
FLOW_DIR = DATA_STORE / "flow"

# 리포트 차수 캐시 (일중 변화 추적용)
_prev_report = {}  # {"frgn_buy": [...], "inst_buy": [...]}

# ── CSV 컬럼 인덱스 상수 (헤더 기반 자동 매핑) ──
# 수급 CSV 헤더:
#   date,종가,전일대비,외국인_수량,기관_수량,개인_수량,외국인_금액,기관_금액,개인_금액,기타법인_금액,기타법인_수량
_COL_MAP_CACHE = {}  # {frozenset(header_tuple): {col_name: idx}}


def _parse_flow_csv(filepath: Path, tail_n: int = 0) -> list[dict]:
    """수급 CSV를 헤더 기반으로 파싱. 컬럼 이름으로 접근 → 인덱스 실수 원천 차단.

    Args:
        filepath: CSV 파일 경로
        tail_n: 마지막 N행만 반환 (0=전체)
    Returns:
        list of dict (각 행이 {컬럼명: 값})
    """
    text = filepath.read_text(encoding="utf-8").strip()
    if not text:
        return []
    lines = text.split("\n")
    if len(lines) < 2:
        return []

    header = lines[0].split(",")
    header = [h.strip() for h in header]
    data_lines = lines[1:]

    if tail_n > 0 and len(data_lines) > tail_n:
        data_lines = data_lines[-tail_n:]

    rows = []
    for line in data_lines:
        vals = line.split(",")
        if len(vals) < len(header):
            continue
        row = {}
        for i, col in enumerate(header):
            row[col] = vals[i].strip()
        rows.append(row)
    return rows


def _csv_float(row: dict, key: str, default: float = 0.0) -> float:
    """dict에서 float 안전 추출."""
    v = row.get(key, "")
    try:
        return float(v) if v else default
    except (ValueError, TypeError):
        return default

# 장중 수급 누적 파일 (매일 리셋)
INTRADAY_CUMUL_PATH = DATA_STORE / "intraday_flow_cumulative.json"

# NXT 대상종목 캐시
_nxt_codes = None


def _get_market_type(code: str) -> str:
    """종목의 마켓 타입 반환 — NXT 또는 KRX."""
    global _nxt_codes
    if _nxt_codes is None:
        nxt_path = DATA_STORE / "nxt_eligible.json"
        if nxt_path.exists():
            try:
                data = json.loads(nxt_path.read_text(encoding="utf-8"))
                _nxt_codes = set(data.get("stocks", {}).keys())
            except Exception:
                _nxt_codes = set()
        else:
            _nxt_codes = set()
    return "NXT" if code in _nxt_codes else "KRX"


def _market_tag(code: str) -> str:
    """텔레그램 표시용 마켓 태그."""
    mt = _get_market_type(code)
    return "[NXT]" if mt == "NXT" else "[KRX전용]"


def _load_bomb_map():
    """bomb_watchlist.json에서 bomb 맵 로드."""
    bw_path = DATA_STORE / "bomb_watchlist.json"
    if not bw_path.exists():
        return {}
    try:
        data = json.loads(bw_path.read_text(encoding="utf-8"))
        return {b["code"]: b for b in data.get("watchlist", [])}
    except Exception as e:
        logger.warning(f"bomb_watchlist 로드 실패: {e}")
        return {}


def _get_yesterday_flow(code):
    """전일 4주체 수급 상세 (flow CSV 마지막 행)."""
    fp = FLOW_DIR / f"{code}_investor.csv"
    if not fp.exists():
        return None
    try:
        with open(fp, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if not rows:
            return None
        last = rows[-1]
        date_key = list(rows[0].keys())[0]

        # 최근 5일 연속 쌍매수 체크
        consec = 0
        for row in reversed(rows[-5:]):
            fv = float(row.get("외국인_금액", 0) or 0)
            iv = float(row.get("기관_금액", 0) or 0)
            if fv > 0 and iv > 0:
                consec += 1
            else:
                break

        return {
            "date": last.get(date_key, ""),
            "frgn": float(last.get("외국인_금액", 0) or 0),   # 백만원
            "inst": float(last.get("기관_금액", 0) or 0),
            "indi": float(last.get("개인_금액", 0) or 0),
            "etc": float(last.get("기타법인_금액", 0) or 0),
            "consec_doublebuy": consec,
        }
    except Exception as e:
        logger.debug(f"flow CSV 로드 실패 {code}: {e}")
        return None


def _get_close_price(code):
    """daily CSV에서 최근 종가."""
    dp = DATA_STORE / "daily" / f"{code}.csv"
    if not dp.exists():
        return 0
    try:
        import pandas as pd
        df = pd.read_csv(dp, encoding="utf-8")
        if len(df) > 0:
            return int(df["종가"].iloc[-1])
    except Exception:
        pass
    return 0


def _flow_arrow(amount_m):
    """수급 금액(백만원) → 방향 표시."""
    b = amount_m / 100  # 억 환산
    if b >= 100:
        return f"+{b:.0f}억"
    elif b >= 10:
        return f"+{b:.0f}억"
    elif b > 0:
        return f"+{b:.1f}억"
    elif b <= -100:
        return f"{b:.0f}억"
    elif b <= -10:
        return f"{b:.0f}억"
    elif b < 0:
        return f"{b:.1f}억"
    else:
        return "0"


def _judge_flow(flow):
    """수급 패턴 판단."""
    if not flow:
        return "데이터없음"
    frgn = flow["frgn"]
    inst = flow["inst"]
    indi = flow["indi"]

    if frgn > 0 and inst > 0 and indi < 0:
        return "건강한 수급 (외인+기관 매수, 개인 매도)"
    elif frgn > 0 and inst > 0:
        return "쌍매수 (외인+기관)"
    elif frgn > 0 and inst <= 0:
        return "외인 단독 매수"
    elif frgn <= 0 and inst > 0:
        if indi > 0:
            return "주의: 기관 매수 + 개인 추격"
        return "기관 단독 매수"
    elif frgn < 0 and inst < 0:
        return "위험: 외인+기관 동반 매도"
    elif frgn < 0 and inst > 0 and indi > 0:
        return "주의: 외인 던지기 (기관+개인 받는 중)"
    else:
        return "혼조"


# ═══════════════════════════════════════
#  장중 수급 리포트 (4회)
# ═══════════════════════════════════════
async def generate_intraday_flow_report(kis_trader, round_num: int) -> str:
    """장중 수급 리포트 생성.

    Args:
        kis_trader: KISTrader 인스턴스
        round_num: 1~4 (1차=10:00, 2차=11:20, 3차=13:20, 4차=14:30)

    Returns:
        텔레그램 전송용 텍스트
    """
    global _prev_report

    round_times = {1: "10:00", 2: "11:20", 3: "13:20", 4: "14:30"}
    time_str = round_times.get(round_num, "")
    today = datetime.now().strftime("%m/%d")

    bomb_map = _load_bomb_map()

    # KIS API 가집계 호출
    try:
        import asyncio
        frgn_buy = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "1", "0")  # 외인 순매수
        frgn_sell = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "1", "1")  # 외인 순매도
        inst_buy = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "2", "0")  # 기관 순매수
        inst_sell = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "2", "1")  # 기관 순매도
    except Exception as e:
        logger.error(f"KIS 가집계 API 실패: {e}")
        return f"[{round_num}차 수급] API 호출 실패: {e}"

    lines = []
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  [{round_num}차 수급] {today} {time_str}")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")

    # 외인 순매수 TOP5
    lines.append(f"\n[외인 순매수 TOP5]")
    frgn_buy_top = frgn_buy[:5] if frgn_buy else []
    for i, s in enumerate(frgn_buy_top):
        bomb_tag = " *BOMB*" if s.get("code") in bomb_map else ""
        mt = _market_tag(s.get("code", ""))
        lines.append(f"  {i+1}. {s['name']}({s['code']}) {s.get('change_rate', 0):+.1f}% {mt}{bomb_tag}")

    # 외인 순매도 TOP3
    lines.append(f"\n[외인 순매도 TOP3]")
    frgn_sell_top = frgn_sell[:3] if frgn_sell else []
    for i, s in enumerate(frgn_sell_top):
        lines.append(f"  {i+1}. {s['name']}({s['code']}) {s.get('change_rate', 0):+.1f}%")

    # 기관 순매수 TOP5
    lines.append(f"\n[기관 순매수 TOP5]")
    inst_buy_top = inst_buy[:5] if inst_buy else []
    for i, s in enumerate(inst_buy_top):
        bomb_tag = " *BOMB*" if s.get("code") in bomb_map else ""
        mt = _market_tag(s.get("code", ""))
        lines.append(f"  {i+1}. {s['name']}({s['code']}) {s.get('change_rate', 0):+.1f}% {mt}{bomb_tag}")

    # 기관 순매도 TOP3
    lines.append(f"\n[기관 순매도 TOP3]")
    inst_sell_top = inst_sell[:3] if inst_sell else []
    for i, s in enumerate(inst_sell_top):
        lines.append(f"  {i+1}. {s['name']}({s['code']}) {s.get('change_rate', 0):+.1f}%")

    # bomb 교차 확인
    frgn_buy_codes = {s.get("code") for s in frgn_buy_top}
    inst_buy_codes = {s.get("code") for s in inst_buy_top}
    bomb_in_frgn = frgn_buy_codes & set(bomb_map.keys())
    bomb_in_inst = inst_buy_codes & set(bomb_map.keys())
    bomb_both = bomb_in_frgn & bomb_in_inst

    if bomb_both or bomb_in_frgn or bomb_in_inst:
        lines.append(f"\n[bomb 교차]")
        if bomb_both:
            for c in bomb_both:
                lines.append(f"  ★ {bomb_map[c]['name']} — 외인+기관 쌍매수 진행중!")
        for c in bomb_in_frgn - bomb_both:
            lines.append(f"  → {bomb_map[c]['name']} — 외인 매수중")
        for c in bomb_in_inst - bomb_both:
            lines.append(f"  → {bomb_map[c]['name']} — 기관 매수중")

    # 이전 리포트 대비 변동 (2차부터)
    if round_num >= 2 and _prev_report:
        prev_frgn = set(_prev_report.get("frgn_buy_codes", []))
        new_frgn = frgn_buy_codes - prev_frgn
        out_frgn = prev_frgn - frgn_buy_codes
        if new_frgn or out_frgn:
            lines.append(f"\n[변동]")
            if new_frgn and frgn_buy:
                names = [s["name"] for s in frgn_buy if s.get("code") in new_frgn]
                lines.append(f"  외인 신규진입: {', '.join(names[:3])}")
            if out_frgn:
                lines.append(f"  외인 이탈: {len(out_frgn)}종목")

    # 매집 레이더 교차 확인 (전일 감지 종목이 오늘도 수급 TOP에 등장?)
    radar_path = DATA_STORE / "accumulation_radar.json"
    if radar_path.exists():
        try:
            radar = json.loads(radar_path.read_text(encoding="utf-8"))
            radar_stocks = {s["code"]: s for s in radar.get("stocks", [])}
            radar_in_frgn = frgn_buy_codes & set(radar_stocks.keys())
            radar_in_inst = inst_buy_codes & set(radar_stocks.keys())
            radar_matched = radar_in_frgn | radar_in_inst

            if radar_matched:
                lines.append(f"\n[매집 레이더 교차]")
                for c in radar_matched:
                    rs = radar_stocks[c]
                    dual_today = c in radar_in_frgn and c in radar_in_inst
                    status = "외인+기관 쌍매수 지속!" if dual_today else (
                        "외인 매집 지속" if c in radar_in_frgn else "기관 매수 합류")
                    lines.append(
                        f"  🔥 {rs['name']} — {status} "
                        f"({rs.get('frgn_days',0)}일→{rs.get('frgn_days',0)+1}일째)")
        except Exception as e:
            logger.debug(f"매집 레이더 교차 체크 실패 (무시): {e}")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━")

    # 캐시 저장
    _prev_report = {
        "frgn_buy_codes": list(frgn_buy_codes),
        "inst_buy_codes": list(inst_buy_codes),
        "round": round_num,
    }

    # ═══ 누적 저장 (15시 매수 추천용) ═══
    _save_cumulative_round(round_num, frgn_buy or [], inst_buy or [])

    return "\n".join(lines)


# ═══════════════════════════════════════
#  장중 수급 누적 저장/조회
# ═══════════════════════════════════════
def _save_cumulative_round(round_num: int, frgn_buy: list, inst_buy: list):
    """각 라운드 수급 데이터를 JSON에 누적 저장.

    구조: {
      "date": "2026-04-21",
      "rounds": {
        "1": {"frgn_buy": [...], "inst_buy": [...]},
        "2": {...}, ...
      }
    }
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # 기존 파일 로드 (오늘 날짜 아니면 리셋)
    cumul = {}
    if INTRADAY_CUMUL_PATH.exists():
        try:
            cumul = json.loads(INTRADAY_CUMUL_PATH.read_text(encoding="utf-8"))
        except Exception:
            cumul = {}

    if cumul.get("date") != today:
        cumul = {"date": today, "rounds": {}}

    # TOP 20까지 저장 (전체 종목 중 상위)
    frgn_data = []
    for s in (frgn_buy or [])[:20]:
        frgn_data.append({
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "amount": s.get("amount", 0),
            "change_rate": s.get("change_rate", 0),
        })

    inst_data = []
    for s in (inst_buy or [])[:20]:
        inst_data.append({
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "amount": s.get("amount", 0),
            "change_rate": s.get("change_rate", 0),
        })

    cumul["rounds"][str(round_num)] = {
        "frgn_buy": frgn_data,
        "inst_buy": inst_data,
        "time": datetime.now().strftime("%H:%M"),
    }

    try:
        INTRADAY_CUMUL_PATH.write_text(
            json.dumps(cumul, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"[누적저장] {round_num}차 외인{len(frgn_data)}종목 기관{len(inst_data)}종목 저장")
    except Exception as e:
        logger.error(f"[누적저장] 실패: {e}")


def _load_cumulative() -> dict:
    """오늘 누적 데이터 로드."""
    today = datetime.now().strftime("%Y-%m-%d")
    if not INTRADAY_CUMUL_PATH.exists():
        return {}
    try:
        cumul = json.loads(INTRADAY_CUMUL_PATH.read_text(encoding="utf-8"))
        if cumul.get("date") != today:
            return {}
        return cumul
    except Exception:
        return {}


# ═══════════════════════════════════════
#  15:00 장중 수급 누적 매수 추천
# ═══════════════════════════════════════
async def generate_15h_flow_buy_alert(kis_trader=None, min_rounds: int = 2, top_n: int = 8) -> str:
    """15시 장중 수급 누적 매수 추천.

    1~4차 수급 데이터를 분석하여 일관적으로 매수세가 유입된 종목을 추천.
    - 외인/기관 TOP20에 여러 라운드 등장 = 일관 매수
    - 3회 이상 등장 + 쌍매수 = 최강

    Args:
        kis_trader: KIS 인스턴스 (현재 미사용, 향후 15시 실시간 5차 조회 확장용)
        min_rounds: 최소 등장 라운드 수 (기본 2)
        top_n: 추천 종목 수 (기본 8)

    Returns:
        텔레그램 전송용 텍스트
    """
    cumul = _load_cumulative()
    if not cumul or not cumul.get("rounds"):
        return "[15시 수급] 장중 누적 데이터 없음 — 1~4차 수급 미수집"

    rounds = cumul["rounds"]
    total_rounds = len(rounds)

    if total_rounds < 2:
        return f"[15시 수급] 수집된 라운드 {total_rounds}회 — 최소 2회 이상 필요"

    # ── 종목별 누적 집계 ──
    stock_stats = {}  # code → {name, frgn_rounds, inst_rounds, total_amount, ...}

    for rnum, rdata in rounds.items():
        # 외인 매수 TOP
        for s in rdata.get("frgn_buy", []):
            code = s.get("code", "")
            if not code:
                continue
            if code not in stock_stats:
                stock_stats[code] = {
                    "code": code,
                    "name": s.get("name", code),
                    "frgn_rounds": 0,
                    "inst_rounds": 0,
                    "frgn_total_amt": 0,
                    "inst_total_amt": 0,
                    "last_chg": s.get("change_rate", 0),
                    "rounds_detail": [],
                }
            stock_stats[code]["frgn_rounds"] += 1
            stock_stats[code]["frgn_total_amt"] += s.get("amount", 0)
            stock_stats[code]["last_chg"] = s.get("change_rate", 0)

        # 기관 매수 TOP
        for s in rdata.get("inst_buy", []):
            code = s.get("code", "")
            if not code:
                continue
            if code not in stock_stats:
                stock_stats[code] = {
                    "code": code,
                    "name": s.get("name", code),
                    "frgn_rounds": 0,
                    "inst_rounds": 0,
                    "frgn_total_amt": 0,
                    "inst_total_amt": 0,
                    "last_chg": s.get("change_rate", 0),
                    "rounds_detail": [],
                }
            stock_stats[code]["inst_rounds"] += 1
            stock_stats[code]["inst_total_amt"] += s.get("amount", 0)
            stock_stats[code]["last_chg"] = s.get("change_rate", 0)

    # ── 스코어링 ──
    uni_path = DATA_STORE / "universe.json"
    universe = {}
    if uni_path.exists():
        try:
            universe = json.loads(uni_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    scored = []
    for code, st in stock_stats.items():
        name = st["name"]

        # ETF 필터
        if _is_etf(name):
            continue

        # 시총 필터 (최소 500억)
        cap = universe.get(code, {}).get("cap_억", 0)
        if cap and cap < 500:
            continue

        # 이미 급등한 종목 제외 (당일 +10% 이상)
        if st["last_chg"] >= 10.0:
            continue

        frgn_r = st["frgn_rounds"]
        inst_r = st["inst_rounds"]
        combined_rounds = frgn_r + inst_r  # 외인3 + 기관2 = 5
        max_side = max(frgn_r, inst_r)     # 한쪽 최대 등장 수

        # 최소 라운드 필터: 외인 또는 기관 중 하나라도 min_rounds 이상
        if max_side < min_rounds:
            continue

        # 점수 계산
        # 기본: 한쪽 라운드 등장 * 10 + 양쪽 라운드 합산 * 5
        score = max_side * 10 + combined_rounds * 5

        # 쌍매수 보너스: 같은 라운드에 외인+기관 동시 등장
        is_dual = frgn_r >= 2 and inst_r >= 2
        if is_dual:
            score += 20

        # 금액 보너스 (합산 1억 이상당 1점, 최대 30점)
        total_amt = st["frgn_total_amt"] + st["inst_total_amt"]
        amt_bonus = min(30, total_amt // 100000000) if total_amt > 0 else 0
        score += amt_bonus

        scored.append({
            "code": code,
            "name": name,
            "frgn_rounds": frgn_r,
            "inst_rounds": inst_r,
            "is_dual": is_dual,
            "score": score,
            "last_chg": st["last_chg"],
            "frgn_amt_억": round(st["frgn_total_amt"] / 100000000, 1) if st["frgn_total_amt"] else 0,
            "inst_amt_억": round(st["inst_total_amt"] / 100000000, 1) if st["inst_total_amt"] else 0,
            "cap_억": cap,
            "market_type": _get_market_type(code),
        })

    # 점수순 정렬
    scored.sort(key=lambda x: -x["score"])
    picks = scored[:top_n]

    if not picks:
        return f"[15시 수급] {total_rounds}회 수집 — 일관 매수 종목 없음 (기준: {min_rounds}회 이상)"

    # ── 텔레그램 메시지 포맷 ──
    today = datetime.now().strftime("%m/%d(%a)")
    lines = []
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  [15시 수급 매수 후보]")
    lines.append(f"  {today} — {total_rounds}회 누적 분석")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"")

    for i, p in enumerate(picks, 1):
        mt = "[NXT]" if p["market_type"] == "NXT" else ""
        dual_mark = " 쌍매수" if p["is_dual"] else ""

        lines.append(f"{i}. {p['name']}({p['code']}) {p['last_chg']:+.1f}% {mt}")

        # 수급 상세
        frgn_str = f"외인 {p['frgn_rounds']}/{total_rounds}회"
        inst_str = f"기관 {p['inst_rounds']}/{total_rounds}회"
        lines.append(f"   {frgn_str} | {inst_str}{dual_mark}")

        # 금액
        if p["frgn_amt_억"] or p["inst_amt_억"]:
            lines.append(f"   누적: 외인 {p['frgn_amt_억']:.0f}억 + 기관 {p['inst_amt_억']:.0f}억")

        lines.append(f"")

    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  기준: {min_rounds}회+ 일관 매수 | 당일+10% 초과 제외")
    lines.append(f"  ★ 수동 매수 후 익일 갭/수급 확인")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")

    names_log = ", ".join(f"{p['name']}(sc={p['score']})" for p in picks[:5])
    logger.info(f"[15시수급추천] {len(picks)}종목: {names_log}")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  15:00 bomb 매수 알림
# ═══════════════════════════════════════
async def generate_bomb_buy_alert(kis_trader, top_n: int = 5) -> str:
    """15:00 bomb 매수 알림 생성.

    Args:
        kis_trader: KISTrader 인스턴스
        top_n: 추천 종목 수 (기본 5)

    Returns:
        텔레그램 전송용 텍스트
    """
    bomb_map = _load_bomb_map()
    if not bomb_map:
        return "[매수] bomb 데이터 없음 — bomb_watchlist.json 확인 필요"

    today = datetime.now().strftime("%m/%d(%a)")

    # KIS 가집계로 현재 외인/기관 순매수 종목 확인
    frgn_buy_codes = set()
    inst_buy_codes = set()
    try:
        import asyncio
        frgn_buy = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "1", "0")
        inst_buy = await asyncio.to_thread(
            kis_trader.fetch_foreign_inst_total, "2", "0")
        frgn_buy_codes = {s.get("code") for s in (frgn_buy or [])}
        inst_buy_codes = {s.get("code") for s in (inst_buy or [])}
    except Exception as e:
        logger.warning(f"KIS 가집계 실패 (무시): {e}")

    # bomb 종목 스코어링 (v4.3 경량패스 + 당일 가집계)
    scored = []
    for code, bw in bomb_map.items():
        bomb_adj = bw.get("bomb_adj", 15)
        flow = _get_yesterday_flow(code)
        close = _get_close_price(code)

        # 수급 점수
        doublebuy_sc = 0
        if flow:
            c = flow["consec_doublebuy"]
            if c >= 4: doublebuy_sc = 15
            elif c >= 3: doublebuy_sc = 10
            elif c >= 2: doublebuy_sc = 5

        # 당일 가집계 보너스
        today_bonus = 0
        today_tags = []
        if code in frgn_buy_codes:
            today_bonus += 10
            today_tags.append("외인매수중")
        if code in inst_buy_codes:
            today_bonus += 10
            today_tags.append("기관매수중")

        # 총점
        v5_score = bomb_adj + doublebuy_sc + today_bonus

        scored.append({
            "code": code,
            "name": bw.get("name", code),
            "signal": bw.get("signal", ""),
            "gap_days": bw.get("gap_days", 0),
            "bomb_adj": bomb_adj,
            "doublebuy": doublebuy_sc,
            "today_bonus": today_bonus,
            "today_tags": today_tags,
            "v5_score": v5_score,
            "flow": flow,
            "close": close,
        })

    # 점수 내림차순 정렬
    scored.sort(key=lambda x: -x["v5_score"])
    picks = scored[:top_n]

    if not picks:
        return "[매수] bomb 후보 없음"

    lines = []
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  [매수] 수급폭탄 TOP{len(picks)}")
    lines.append(f"  {today} 15:00")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")

    for i, p in enumerate(picks):
        close = p["close"]
        tp = int(close * 1.03) if close else 0
        sl = int(close * 0.98) if close else 0
        tp_pct = "+3%"
        sl_pct = "-2%"

        mt = _market_tag(p['code'])
        lines.append(f"")
        lines.append(f"{i+1}. {p['name']}({p['code']}) {close:,}원 {mt}")

        # bomb 정보
        gap_str = f"{p['gap_days']}일 매집" if p['gap_days'] else ""
        lines.append(f"   폭탄: {p['signal']} {gap_str}")

        # 수급 상세 (전일 기준)
        flow = p["flow"]
        if flow:
            f_str = _flow_arrow(flow["frgn"])
            i_str = _flow_arrow(flow["inst"])
            d_str = _flow_arrow(flow["indi"])
            e_str = _flow_arrow(flow["etc"])
            consec = flow["consec_doublebuy"]

            lines.append(f"   외인 {f_str} 기관 {i_str}")
            lines.append(f"   개인 {d_str} 기타 {e_str}")
            if consec >= 2:
                lines.append(f"   쌍매수 {consec}일 연속")
            judge = _judge_flow(flow)
            lines.append(f"   → {judge}")
        else:
            lines.append(f"   수급: 데이터 없음")

        # 당일 가집계
        if p["today_tags"]:
            lines.append(f"   오늘: {', '.join(p['today_tags'])}")

        # 익절/손절
        if close:
            lines.append(f"   익절 {tp:,}원({tp_pct}) 손절 {sl:,}원({sl_pct})")

    lines.append(f"")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  전략: 익일 갭업 → 시가 매도")
    lines.append(f"       보합 → 10시 수급 보고 판단")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")

    # 로그
    names = ", ".join(f"{p['name']}(v5={p['v5_score']})" for p in picks)
    logger.info(f"[매수알림] TOP{len(picks)}: {names}")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  수급 강도 TOP (FLOWX 스윙시스템 패널)
# ═══════════════════════════════════════
# ETF/펀드 키워드 필터
_ETF_KEYWORDS = [
    "KODEX", "TIGER", "ACE", "KIWOOM", "SOL ", "HANARO", "KOSEF", "ARIRANG",
    "BNK", "PLUS ", "FOCUS", "TIMEFOLIO", "RISE ", "TIME ", "ITF ", "1Q ",
    "KoAct", "WON ", "UNICORN", "Active", "액티브",
]

# 과열 기준: 5일 등락률 이 이상이면 과열 경고
OVERHEAT_THRESHOLD_PCT = 20.0


def _is_etf(name: str) -> bool:
    """ETF/펀드 여부 판별."""
    if not name or name == "?":
        return True
    for kw in _ETF_KEYWORDS:
        if kw in name:
            return True
    return False


def generate_flow_intensity_data(top_n: int = 15, min_cap: int = 2000) -> dict:
    """수급 강도 TOP 데이터 생성 (시총 대비 유입 비율).

    Args:
        top_n: 상위 종목 수 (기본 15)
        min_cap: 최소 시총(억) 필터 (기본 2000억)

    Returns:
        {"date": str, "total_scanned": int, "top_stocks": [...],
         "dual_buy_count": int, "overheat_count": int}
    """
    uni_path = DATA_STORE / "universe.json"
    if not uni_path.exists():
        logger.error("universe.json 없음")
        return {}

    universe = json.loads(uni_path.read_text(encoding="utf-8"))

    results = []
    scanned = 0

    for f in FLOW_DIR.glob("*_investor.csv"):
        code = f.name.split("_")[0]
        info = universe.get(code, {})
        name = info.get("name", "?")

        if _is_etf(name):
            continue

        cap = info.get("cap_\uc5b5", 0)  # cap_억
        if not cap or cap < min_cap:
            continue

        mtype = _get_market_type(code)

        rows = _parse_flow_csv(f, tail_n=5)
        if len(rows) < 5:
            continue

        recent = []
        _last_csv_date = None
        for row in rows:
            fn = _csv_float(row, "외국인_금액")
            ins = _csv_float(row, "기관_금액")
            close = _csv_float(row, "종가")
            dt = row.get("date", "")
            if dt:
                _last_csv_date = dt
            recent.append({"f": fn, "i": ins, "close": close})

        if len(recent) < 5 or recent[-1]["close"] < 1000:
            continue

        scanned += 1

        # 3일 합산 (백만원 단위)
        f3 = sum(r["f"] for r in recent[-3:])
        i3 = sum(r["i"] for r in recent[-3:])

        # 5일 등락
        pct5 = (recent[-1]["close"] / recent[0]["close"] - 1) * 100 if recent[0]["close"] > 0 else 0

        # 수급 강도 = (3일 합산 / 100 → 억) / 시총(억) * 100
        intensity = (f3 + i3) / 100 / cap * 100 if cap > 0 else 0

        # 양수(유입)만 의미 있음
        if intensity <= 0:
            continue

        # 쌍매수 여부
        last = recent[-1]
        dual_buy = last["f"] > 0 and last["i"] > 0

        # 연속 매수일
        consec_f = 0
        for r in reversed(recent):
            if r["f"] > 0:
                consec_f += 1
            else:
                break

        consec_i = 0
        for r in reversed(recent):
            if r["i"] > 0:
                consec_i += 1
            else:
                break

        # 과열 여부
        is_overheated = pct5 >= OVERHEAT_THRESHOLD_PCT

        results.append({
            "code": code,
            "name": name,
            "market_type": mtype,
            "intensity_pct": round(intensity, 2),
            "foreign_3d_억": round(f3 / 100, 0),
            "inst_3d_억": round(i3 / 100, 0),
            "total_3d_억": round((f3 + i3) / 100, 0),
            "cap_억": cap,
            "close": int(recent[-1]["close"]),
            "pct_5d": round(pct5, 1),
            "dual_buy": dual_buy,
            "consec_foreign": consec_f,
            "consec_inst": consec_i,
            "is_overheated": is_overheated,
            "_csv_date": _last_csv_date,
        })

    # ── 품질 스코어링 (intensity만이 아닌 복합 스코어) ──
    # 순서: 가점 먼저 → 감점(배수) 마지막 (감점이 가점을 상쇄하도록)
    for r in results:
        score = r["intensity_pct"] * 10          # 기본: 수급 강도

        # 쌍매수 가점 (외인+기관 동시 유입)
        if r["dual_buy"]:
            score += 20

        # 연속 매수일 가점
        consec = max(r["consec_foreign"], r["consec_inst"])
        score += consec * 5

        # 쌍연속 (외인+기관 모두 3일+) 강한 신호
        if r["consec_foreign"] >= 3 and r["consec_inst"] >= 3:
            score += 15

        # ── 감점은 가점 합산 후 적용 (배수 감점이 전체에 영향) ──
        # 과열 감점 (5일 +20% 이상)
        if r["is_overheated"]:
            score *= 0.3

        # 급등 후 매수 감점 (5일 +10% 이상이면 추격 위험)
        if r["pct_5d"] >= 10:
            score *= 0.5

        r["quality_score"] = round(score, 1)

    # 품질 스코어순 정렬 (intensity가 아닌 복합 스코어)
    results.sort(key=lambda x: -x["quality_score"])

    # 과열 종목 제외 후 TOP-N 선별
    filtered = [r for r in results if not r["is_overheated"]]
    top = filtered[:top_n]

    # 랭크 부여
    for i, r in enumerate(top, 1):
        r["rank"] = i

    dual_cnt = sum(1 for r in top if r["dual_buy"])
    heat_cnt = sum(1 for r in results if r["is_overheated"])  # 전체 대상 기준

    # CSV 최신 날짜 사용 (last_trading_day()는 전일 반환이라 장 마감 후 1일 차이)
    csv_date = None
    for r in results:
        if r.get("_csv_date") and (csv_date is None or r["_csv_date"] > csv_date):
            csv_date = r["_csv_date"]
    if csv_date is None:
        from data.trading_calendar import last_trading_day
        csv_date = last_trading_day().isoformat()

    # 임시 키 제거
    for r in results:
        r.pop("_csv_date", None)

    out = {
        "date": csv_date,
        "total_scanned": scanned,
        "top_stocks": top,
        "dual_buy_count": dual_cnt,
        "overheat_count": heat_cnt,
    }

    logger.info(
        f"[수급강도] {scanned}종목 스캔 → TOP{len(top)} "
        f"(TOP내 쌍매수 {dual_cnt}, 전체 과열제외 {heat_cnt})"
    )
    return out


# ─────────────────────────────────────────────
# D-1 대량 쌍매수 감지 (전일 외인+기관 합산 100억+ DUAL)
# ─────────────────────────────────────────────
MASSIVE_DUAL_MIN_TOTAL_억 = 100   # 합산 최소 100억
MASSIVE_DUAL_MIN_CAP_억 = 2000    # 시총 최소 2000억

def detect_massive_dual_buy(min_total: float = MASSIVE_DUAL_MIN_TOTAL_억,
                             min_cap: int = MASSIVE_DUAL_MIN_CAP_억) -> dict:
    """전일 외인+기관 합산 대량 쌍매수 종목 감지.

    수급 CSV 마지막 행(직전 거래일)에서:
    - 외인 > 0 AND 기관 > 0 (쌍매수)
    - 합산 >= min_total(억)
    - 시총 >= min_cap(억)

    Returns:
        {"date": str, "scanned": int, "detected": int,
         "stocks": [{"code","name","foreign_억","inst_억","total_억","cap_억","close","pct_1d"}]}
    """
    uni_path = DATA_STORE / "universe.json"
    if not uni_path.exists():
        logger.error("universe.json 없음")
        return {}

    universe = json.loads(uni_path.read_text(encoding="utf-8"))
    results = []
    scanned = 0
    csv_date = None

    for f in FLOW_DIR.glob("*_investor.csv"):
        code = f.name.split("_")[0]
        info = universe.get(code, {})
        name = info.get("name", "?")

        if _is_etf(name):
            continue

        cap = info.get("cap_억", 0)
        if not cap or cap < min_cap:
            continue

        rows = _parse_flow_csv(f, tail_n=2)
        if len(rows) < 1:
            continue

        scanned += 1

        last = rows[-1]
        dt = last.get("date", "")
        fn = _csv_float(last, "외국인_금액")
        ins = _csv_float(last, "기관_금액")
        close = _csv_float(last, "종가")

        if csv_date is None or dt > csv_date:
            csv_date = dt

        # 쌍매수 필터: 외인 > 0 AND 기관 > 0
        if fn <= 0 or ins <= 0:
            continue

        total_억 = (fn + ins) / 100  # 백만원 → 억
        if total_억 < min_total:
            continue

        # 전일 종가 대비 등락 (있으면)
        pct_1d = 0.0
        if len(rows) >= 2:
            prev_close = _csv_float(rows[-2], "종가")
            if prev_close > 0:
                pct_1d = (close / prev_close - 1) * 100

        results.append({
            "code": code,
            "name": name,
            "foreign_억": round(fn / 100, 1),
            "inst_억": round(ins / 100, 1),
            "total_억": round(total_억, 1),
            "cap_억": cap,
            "close": int(close),
            "pct_1d": round(pct_1d, 1),
        })

    # 합산 금액 내림차순 정렬
    results.sort(key=lambda x: -x["total_억"])

    # 랭크
    for i, r in enumerate(results, 1):
        r["rank"] = i

    if csv_date is None:
        from data.trading_calendar import last_trading_day
        csv_date = last_trading_day().isoformat()

    out = {
        "date": csv_date,
        "scanned": scanned,
        "detected": len(results),
        "stocks": results,
    }

    logger.info(
        f"[대량쌍매수] {scanned}종목 스캔 → {len(results)}종목 감지 "
        f"(합산 {min_total}억+, 시총 {min_cap}억+)"
    )
    return out


def detect_consecutive_surge(min_pct: float = 20.0) -> dict:
    """전일 +20% 이상 급등 종목 감지 (연속상한가 추적용).

    learning/journal/daily/ 최신 파일에서 gainers_top10 추출.

    Returns:
        {"date": str, "stocks": [{"code","name","change_pct","volume_ratio"}]}
    """
    journal_dir = DATA_STORE / "learning" / "journal" / "daily"
    if not journal_dir.exists():
        logger.warning("journal/daily 디렉토리 없음")
        return {}

    # 최신 journal 파일
    files = sorted(journal_dir.glob("*.json"), reverse=True)
    if not files:
        return {}

    data = json.loads(files[0].read_text(encoding="utf-8"))
    date_str = data.get("date", files[0].stem)
    gainers = data.get("gainers_top10", [])

    surges = []
    for g in gainers:
        pct = g.get("change_rate", g.get("pct", 0))
        if pct >= min_pct:
            surges.append({
                "code": g.get("code", ""),
                "name": g.get("name", "?"),
                "change_pct": round(pct, 1),
                "volume_ratio": round(g.get("volume_ratio", 0), 1),
                "cap_억": g.get("cap", 0),
                "sector": g.get("sector", "?"),
            })

    out = {
        "date": date_str,
        "count": len(surges),
        "stocks": surges,
    }

    logger.info(f"[연속급등] {date_str} +{min_pct}% 이상: {len(surges)}종목")
    return out


def detect_foreign_dump(
    consec_days: int = 3,
    min_total_억: float = 100.0,
    min_cap: int = 500,
    overheat_pct: float = 50.0,
    overheat_days: int = 5,
) -> dict:
    """외인 투매/과열 종목 감지 → 매수금지·감점 태그.

    3가지 패턴:
    1. FOREIGN_DUMP: 외인 consec_days일 연속 매도 + 합산 -min_total_억 이상
    2. FOREIGN_FLIP: 전일 외인 매수 → 당일 매도 전환 (일중반전)
    3. THEME_OVERHEAT: overheat_days일 내 +overheat_pct%↑ + 외인 매도 전환

    Returns:
        {code: {"tag": "FOREIGN_DUMP"|"FOREIGN_FLIP"|"THEME_OVERHEAT",
                "penalty": -20|-15|-25,
                "detail": str,
                "foreign_3d_억": float, "pct_5d": float}}
    """
    uni_path = DATA_STORE / "universe.json"
    if not uni_path.exists():
        return {}

    universe = json.loads(uni_path.read_text(encoding="utf-8"))
    result = {}
    scanned = 0

    for f in FLOW_DIR.glob("*_investor.csv"):
        code = f.name.split("_")[0]
        info = universe.get(code, {})
        name = info.get("name", "?")

        if _is_etf(name):
            continue

        cap = info.get("cap_억", 0)
        if not cap or cap < min_cap:
            continue

        rows = _parse_flow_csv(f, tail_n=max(consec_days + 1, overheat_days + 1))
        if len(rows) < consec_days:
            continue

        scanned += 1

        # 최근 N일 외인 금액 (백만원 단위)
        recent = rows[-consec_days:]
        foreign_vals = [_csv_float(r, "외국인_금액") for r in recent]
        foreign_sum_억 = sum(foreign_vals) / 100  # 백만→억

        # 종가 추이
        close_now = _csv_float(rows[-1], "종가")
        close_prev = _csv_float(rows[-2], "종가") if len(rows) >= 2 else 0

        # ── 패턴 1: FOREIGN_DUMP (연속 매도 + 대량) ──
        all_sell = all(v < 0 for v in foreign_vals)
        if all_sell and abs(foreign_sum_억) >= min_total_억:
            result[code] = {
                "tag": "FOREIGN_DUMP",
                "penalty": -20,
                "name": name,
                "cap": cap,
                "foreign_sum_억": round(foreign_sum_억, 1),
                "consec_sell_days": consec_days,
                "detail": (
                    f"외인{consec_days}일연속매도 "
                    f"합산{foreign_sum_억:+.0f}억"
                ),
            }
            continue  # 가장 강한 태그 → 다른 패턴 스킵

        # ── 패턴 2: FOREIGN_FLIP (전일 매수→당일 매도 반전) ──
        if len(rows) >= 2:
            f_yesterday = _csv_float(rows[-2], "외국인_금액")
            f_today = _csv_float(rows[-1], "외국인_금액")
            # 전일 +30억 이상 매수 → 당일 -30억 이상 매도 (급반전)
            if f_yesterday > 3000 and f_today < -3000:  # 백만원 단위
                result[code] = {
                    "tag": "FOREIGN_FLIP",
                    "penalty": -15,
                    "name": name,
                    "cap": cap,
                    "foreign_sum_억": round((f_yesterday + f_today) / 100, 1),
                    "detail": (
                        f"외인반전 전일{f_yesterday/100:+.0f}억"
                        f"→당일{f_today/100:+.0f}억"
                    ),
                }
                continue

        # ── 패턴 3: THEME_OVERHEAT (급등 후 외인 매도 전환) ──
        if len(rows) >= overheat_days:
            close_5d_ago = _csv_float(rows[-overheat_days], "종가")
            if close_5d_ago > 0 and close_now > 0:
                pct_5d = (close_now / close_5d_ago - 1) * 100
                f_today = _csv_float(rows[-1], "외국인_금액")
                f_yesterday = _csv_float(rows[-2], "외국인_금액") if len(rows) >= 2 else 0
                # 5일 +50% 이상 + 최근 2일 외인 매도
                if pct_5d >= overheat_pct and f_today < 0 and f_yesterday < 0:
                    result[code] = {
                        "tag": "THEME_OVERHEAT",
                        "penalty": -25,
                        "name": name,
                        "cap": cap,
                        "pct_5d": round(pct_5d, 1),
                        "foreign_sum_억": round(
                            (f_today + f_yesterday) / 100, 1
                        ),
                        "detail": (
                            f"{overheat_days}d{pct_5d:+.0f}% "
                            f"외인매도전환"
                        ),
                    }

    logger.info(
        f"[외인던지기] {scanned}종목 스캔 → {len(result)}종목 감지 "
        f"(DUMP:{sum(1 for v in result.values() if v['tag']=='FOREIGN_DUMP')} "
        f"FLIP:{sum(1 for v in result.values() if v['tag']=='FOREIGN_FLIP')} "
        f"OVERHEAT:{sum(1 for v in result.values() if v['tag']=='THEME_OVERHEAT')})"
    )
    return result
