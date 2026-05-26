# -*- coding: utf-8 -*-
"""검증 모드 (Verification Mode) — 1주 실전 수익률 검증.

사장님 제안 (2026-05-17): morning_recommendation 추천 전체 종목을 1주씩 실전 매수해서
1~2일 정산 → 수익률 확인 후 본격 진입 결정. Paper trading의 슬리피지/체결 갭 문제 해결.

흐름:
  09:00  morning_recommendation TOP 후보 (~13개) → 1주씩 시장가 매수
  09:00~15:20  HOLD (SL/TP/6조건 모두 무시 — 검증 목적)
  15:25  전체 verification 포지션 시장가 강제 청산
  15:35  일일 정산 리포트 텔레그램 자동 발송 (시그널별 적중률 분석)

설정 (`.env`):
  VERIFICATION_MODE=true      # ON/OFF 토글
  VERIFICATION_START=20260518  # 시작일 (YYYYMMDD, default 2026-05-18)
  VERIFICATION_END=20260519    # 종료일 (default 2026-05-19)

자동 안전망:
  - 날짜 범위 벗어나면 자동 OFF (실수 방지)
  - 봇 재시작 시 verification 보유 종목 자동 인계
  - 외인 던지기 OVERHEAT는 검증 모드에서도 즉시 청산 (사고 방지)

DB: scalper_verification_log (영구 기록 → 시그널 가중치 분석 데이터)
"""
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

logger = logging.getLogger(__name__)

# ── 경로 ───────────────────────────────────────────────
_DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "verification"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── 기본 설정 (사장님 5/20 D-Day 결정: B 진보 + 1주 모드, 종료일 연장) ──
# 5/19 검증 결과 + 사장님 자비스 정신 v2 원칙(자율 판단)에 따라
# 5/20 D-Day부터 본격 1주 모드 가동. 끝 날짜는 사장님이 .env로 조절.
_DEFAULT_START = date(2026, 5, 18)
_DEFAULT_END = date(2026, 5, 30)  # 2주간 1주 모드 (사장님 .env로 추가 연장 가능)


def _today() -> date:
    return datetime.now(KST).date() if KST else date.today()


def _parse_env_date(env_key: str, default: date) -> date:
    raw = os.getenv(env_key, "").strip()
    if not raw or len(raw) != 8 or not raw.isdigit():
        return default
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    except ValueError:
        return default


def is_active() -> bool:
    """검증 모드 활성 여부 — .env 토글 + 날짜 범위 둘 다 충족.

    사장님 5/20 결정: default를 "true"로 변경 — .env에 키 누락 시에도
    1주 모드 자동 활성화 보장. 명시적 OFF는 .env에 VERIFICATION_MODE=false 작성.

    [5/20 D-Day 안전망] .env의 VERIFICATION_END가 오늘보다 과거인 경우
    (예: 어제 끝난 5/19로 박혀있음) → 코드 default를 사용해 자동 연장.
    사장님이 VPS .env를 매번 수정하지 않아도 1주 모드 안 끊김.
    """
    if os.getenv("VERIFICATION_MODE", "true").strip().lower() != "true":
        return False
    today = _today()
    start = _parse_env_date("VERIFICATION_START", _DEFAULT_START)
    end = _parse_env_date("VERIFICATION_END", _DEFAULT_END)
    # ★ 5/26 사장님 분노 후 영구 제거 ★ 5/20 단타봇 자율 "default 5/30 자동 연장" 안전망
    # → 5/22~5/26 매일 47만 사장님 인지 X 매매 사고 → 영구 차단
    # 향후 검증모드 연장은 사장님 직접 .env 수정만 가능 (단타봇 자율 X)
    return start <= today <= end


def get_config_summary() -> Dict:
    return {
        "active": is_active(),
        "start_date": str(_parse_env_date("VERIFICATION_START", _DEFAULT_START)),
        "end_date": str(_parse_env_date("VERIFICATION_END", _DEFAULT_END)),
        "today": str(_today()),
        "env_toggle": os.getenv("VERIFICATION_MODE", "false"),
    }


def _state_path(target_date: Optional[date] = None) -> Path:
    d = target_date or _today()
    return _DATA_DIR / f"state_{d.strftime('%Y%m%d')}.json"


def _load_state(target_date: Optional[date] = None) -> Dict:
    path = _state_path(target_date)
    if not path.exists():
        return {"date": str(target_date or _today()), "positions": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"[verification] state 로드 실패 ({path}): {e}")
        return {"date": str(target_date or _today()), "positions": {}}


def _save_state(state: Dict, target_date: Optional[date] = None) -> None:
    path = _state_path(target_date)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def record_buy(code: str, name: str, buy_price: float, signal_tags: str = "",
               final_score: float = 0.0) -> None:
    """매수 기록 — state JSON + DB 적재."""
    state = _load_state()
    now_iso = datetime.now(KST).isoformat() if KST else datetime.now().isoformat()
    state["positions"][code] = {
        "name": name,
        "buy_price": float(buy_price),
        "buy_time": now_iso,
        "signal_tags": signal_tags,
        "final_score": float(final_score),
        "sold": False,
    }
    _save_state(state)
    logger.info(f"[verification] 매수 기록: [{code}] {name} @ {buy_price:,.0f}원")

    # DB 적재 (best-effort, 실패해도 무시)
    try:
        from utils.supabase_sql import execute
        execute(
            """
            INSERT INTO scalper_verification_log (
                trade_date, stock_ticker, stock_name, buy_price, buy_time,
                signal_tags, final_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, stock_ticker) DO UPDATE SET
                buy_price = EXCLUDED.buy_price,
                buy_time = EXCLUDED.buy_time,
                signal_tags = EXCLUDED.signal_tags,
                final_score = EXCLUDED.final_score
            """,
            (state["date"], code, name, float(buy_price), now_iso,
             signal_tags, float(final_score)),
        )
    except Exception as e:
        logger.warning(f"[verification] DB 매수 적재 실패 (무시): {e}")


def record_sell(code: str, sell_price: float, sell_reason: str = "1525_close") -> Optional[Dict]:
    """청산 기록 + PnL 계산. 보유 안 한 종목은 None."""
    state = _load_state()
    pos = state["positions"].get(code)
    if not pos or pos.get("sold"):
        return None

    buy_price = pos["buy_price"]
    pnl_pct = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0.0
    now_iso = datetime.now(KST).isoformat() if KST else datetime.now().isoformat()

    pos["sell_price"] = float(sell_price)
    pos["sell_time"] = now_iso
    pos["sell_reason"] = sell_reason
    pos["pnl_pct"] = round(pnl_pct, 3)
    pos["sold"] = True
    _save_state(state)
    logger.info(
        f"[verification] 청산: [{code}] {pos['name']} "
        f"{buy_price:,.0f} → {sell_price:,.0f} ({pnl_pct:+.2f}%)"
    )

    # DB 갱신
    try:
        from utils.supabase_sql import execute
        execute(
            """
            UPDATE scalper_verification_log
            SET sell_price = %s, sell_time = %s, sell_reason = %s, pnl_pct = %s
            WHERE trade_date = %s AND stock_ticker = %s
            """,
            (float(sell_price), now_iso, sell_reason, round(pnl_pct, 3),
             state["date"], code),
        )
    except Exception as e:
        logger.warning(f"[verification] DB 청산 적재 실패 (무시): {e}")

    return {"code": code, "name": pos["name"], "pnl_pct": pnl_pct, **pos}


def get_active_positions(target_date: Optional[date] = None) -> List[Dict]:
    """현재 verification 보유 종목 (sold=False)."""
    state = _load_state(target_date)
    return [
        {"code": code, **pos}
        for code, pos in state["positions"].items()
        if not pos.get("sold")
    ]


def daily_settlement(target_date: Optional[date] = None) -> Dict:
    """일일 정산 계산 — 시그널별 적중률 + 통계."""
    state = _load_state(target_date)
    positions = list(state["positions"].items())
    if not positions:
        return {"date": state.get("date"), "count": 0, "avg_pnl": 0.0, "items": []}

    sold = [p for code, p in positions if p.get("sold")]
    pnls = [p["pnl_pct"] for p in sold]
    wins = [p for p in sold if p["pnl_pct"] > 0]

    avg_pnl = sum(pnls) / len(pnls) if pnls else 0.0
    win_rate = len(wins) / len(sold) * 100 if sold else 0.0

    # 시그널별 적중률 — sources 텍스트에서 태그 추출
    signal_stats: Dict[str, Dict[str, int]] = {}
    for p in sold:
        tags = p.get("signal_tags", "") or ""
        # 태그 패턴: "etf_lead(TOP10/4ETF:+13)", "dual_buy(...)" 등 → 괄호 앞 식별자만
        for raw in tags.replace(",", " ").split():
            tag_id = raw.split("(")[0]
            if not tag_id:
                continue
            s = signal_stats.setdefault(tag_id, {"count": 0, "wins": 0, "pnl_sum": 0.0})
            s["count"] += 1
            if p["pnl_pct"] > 0:
                s["wins"] += 1
            s["pnl_sum"] += p["pnl_pct"]

    signal_summary = sorted(
        [
            {
                "tag": k,
                "count": v["count"],
                "wins": v["wins"],
                "win_rate": v["wins"] / v["count"] * 100 if v["count"] else 0.0,
                "avg_pnl": v["pnl_sum"] / v["count"] if v["count"] else 0.0,
            }
            for k, v in signal_stats.items()
        ],
        key=lambda x: x["avg_pnl"],
        reverse=True,
    )

    return {
        "date": state.get("date"),
        "count": len(positions),
        "sold_count": len(sold),
        "unsold_count": len(positions) - len(sold),
        "avg_pnl": round(avg_pnl, 3),
        "win_rate": round(win_rate, 1),
        "max_pnl": round(max(pnls), 3) if pnls else 0.0,
        "min_pnl": round(min(pnls), 3) if pnls else 0.0,
        "items": sorted(
            [{"code": code, **p} for code, p in positions if p.get("sold")],
            key=lambda x: x["pnl_pct"],
            reverse=True,
        ),
        "signal_stats": signal_summary,
    }


def format_settlement_message(s: Dict) -> str:
    """텔레그램용 정산 메시지 포맷."""
    items = s["items"]
    top3 = items[:3]
    bot3 = items[-3:][::-1] if len(items) > 3 else []

    lines = [
        f"📊 [검증모드 {s['date']} 정산]",
        f"",
        f"매수: {s['count']}종목 × 1주 = ?원 (실제 잔고 차감)",
        f"청산: {s['sold_count']}/{s['count']}종목",
        f"",
        f"💰 평균 PnL: {s['avg_pnl']:+.3f}%",
        f"🎯 승률: {s['win_rate']:.1f}%  ({sum(1 for p in items if p['pnl_pct']>0)}승 / "
        f"{sum(1 for p in items if p['pnl_pct']<=0)}패)",
        f"📈 최대: {s['max_pnl']:+.2f}%  📉 최소: {s['min_pnl']:+.2f}%",
    ]
    if top3:
        lines.append("")
        lines.append("🏆 TOP 3:")
        for p in top3:
            lines.append(f"  [{p['code']}] {p['name'][:14]:<14s}  {p['pnl_pct']:+6.2f}%")
    if bot3 and len(items) > 3:
        lines.append("")
        lines.append("⚠️ BOTTOM 3:")
        for p in bot3:
            lines.append(f"  [{p['code']}] {p['name'][:14]:<14s}  {p['pnl_pct']:+6.2f}%")
    if s["signal_stats"]:
        lines.append("")
        lines.append("📊 시그널별 평균 PnL (TOP 5):")
        for sig in s["signal_stats"][:5]:
            lines.append(
                f"  {sig['tag'][:18]:<18s}  {sig['wins']}/{sig['count']}  "
                f"승률 {sig['win_rate']:>5.1f}%  평균 {sig['avg_pnl']:+5.2f}%"
            )
    if s["unsold_count"] > 0:
        lines.append("")
        lines.append(f"⚠️ 미청산: {s['unsold_count']}종목 (수동 확인 필요)")
    return "\n".join(lines)


def create_table_via_raw_conn():
    """1회성 DDL 직접 실행 — scalper_verification_log 테이블 생성."""
    sql_path = (Path(__file__).resolve().parent.parent / "sql" /
                "scalper_verification_log_migration.sql")
    if not sql_path.exists():
        raise FileNotFoundError(f"DDL 파일 없음: {sql_path}")
    ddl = sql_path.read_text(encoding="utf-8")

    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.supabase_sql import get_conn
    logger.info("DDL 실행 중 (scalper_verification_log)...")
    with get_conn(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("DDL 실행 완료.")


if __name__ == "__main__":
    import sys, io, argparse
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="검증 모드 admin 도구")
    parser.add_argument("--create-table", action="store_true",
                        help="scalper_verification_log 테이블 1회성 자동 생성")
    parser.add_argument("--show-settlement", action="store_true",
                        help="오늘 정산 결과 미리보기 (장 마감 후)")
    args = parser.parse_args()

    print("\n=== 검증 모드 설정 확인 ===")
    print(json.dumps(get_config_summary(), ensure_ascii=False, indent=2))

    if args.create_table:
        create_table_via_raw_conn()
        print("\n✅ scalper_verification_log 테이블 생성/확인 완료")

    if args.show_settlement:
        s = daily_settlement()
        if s.get("count", 0) > 0:
            print("\n=== 오늘 정산 미리보기 ===")
            print(format_settlement_message(s))
        else:
            print("\n오늘 매수 기록 없음")

    if not is_active():
        print("\n⚠️ 검증 모드 OFF — .env에 VERIFICATION_MODE=true 추가 필요")
    else:
        print("\n✅ 검증 모드 ACTIVE")
        positions = get_active_positions()
        print(f"\n현재 보유: {len(positions)}종목")
        for p in positions:
            print(f"  [{p['code']}] {p['name']} @ {p.get('buy_price', 0):,.0f}")
