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
            if new_frgn:
                names = [s["name"] for s in frgn_buy if s.get("code") in new_frgn]
                lines.append(f"  외인 신규진입: {', '.join(names[:3])}")
            if out_frgn:
                lines.append(f"  외인 이탈: {len(out_frgn)}종목")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━")

    # 캐시 저장
    _prev_report = {
        "frgn_buy_codes": list(frgn_buy_codes),
        "inst_buy_codes": list(inst_buy_codes),
        "round": round_num,
    }

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
