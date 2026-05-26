# -*- coding: utf-8 -*-
"""
NXT 야간매수 TOP 5 Supabase 업로드
===================================
intelligence_nxt_picks: NXT TOP 5 추천 저장 (C32)
intelligence_nxt_performance: NXT 성적표 저장 (C33)
quant_nxt_picks: NXT 후보 상세 스코어링 저장 (C32 연동)
"""

import logging
from pathlib import Path

logger = logging.getLogger("BH.NxtPerf")
DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"


def upload_nxt_picks(picks_data: dict) -> bool:
    """NXT TOP 5 추천을 Supabase에 저장.

    Args:
        picks_data: extract_nxt_top5() 반환값
    """
    if not picks_data:
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        picks = picks_data.get("picks", [])
        picks_json = []
        for p in picks:
            picks_json.append({
                "rank": p.get("rank"),
                "code": p.get("code"),
                "name": p.get("name"),
                "sector": p.get("sector"),
                "supply_score": p.get("supply_score"),
                "entry_price": p.get("entry_price"),
                "foreign_flow_warning": p.get("foreign_flow_warning", ""),
            })

        row = {
            "date": picks_data["date"],
            "mode": picks_data.get("mode", "confirmed"),
            "nxt_score": picks_data.get("nxt_score", 0),
            "signal": picks_data.get("nxt_signal", ""),
            "sectors": picks_data.get("recommended_sectors", []),
            "picks": picks_json,
        }

        try:
            client.table("intelligence_nxt_picks") \
                .upsert(row, on_conflict="date") \
                .execute()
        except Exception as upsert_err:
            # mode 컬럼 미존재 시 제거 후 재시도
            if "mode" in str(upsert_err):
                logger.warning("mode 컬럼 미존재 — mode 제외 후 재시도")
                row.pop("mode", None)
                client.table("intelligence_nxt_picks") \
                    .upsert(row, on_conflict="date") \
                    .execute()
            else:
                raise

        logger.info(
            f"NXT 픽 업로드 완료: {picks_data['date']} · "
            f"mode={picks_data.get('mode', 'confirmed')} · {len(picks)}종목"
        )
        return True

    except Exception as e:
        logger.error(f"NXT 픽 업로드 실패: {e}")
        return False


def upload_accumulation_radar(radar_data: dict) -> bool:
    """매집 레이더 데이터를 Supabase에 저장.

    NXT TOP5 바로 아래, 매매 타임라인 위에 표시.
    외인 3일+ 매집 중이나 아직 안 오른 "미발화" 종목.

    Args:
        radar_data: {
            "date": "2026-04-20",
            "stocks": [{"code","name","frgn_days","accel_b","chg5","tag","last_dual"}, ...]
        }
    """
    if not radar_data or not radar_data.get("stocks"):
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        stocks_json = []
        for s in radar_data["stocks"]:
            stocks_json.append({
                "code": s.get("code", ""),
                "name": s.get("name", ""),
                "frgn_days": s.get("frgn_days", 0),
                "accel_b": round(s.get("accel_b", 0), 1),
                "chg5": round(s.get("chg5", 0), 1),
                "tag": s.get("tag", ""),
                "last_dual": s.get("last_dual", False),
                "supply_score": s.get("supply_score", 0),
                "combined_supply": round(s.get("combined_supply", 0), 1),
            })

        row = {
            "date": radar_data["date"],
            "stocks": stocks_json,
        }

        client.table("intelligence_accumulation_radar") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(f"매집 레이더 업로드 완료: {radar_data['date']} · {len(stocks_json)}종목")
        return True

    except Exception as e:
        logger.error(f"매집 레이더 업로드 실패: {e}")
        return False


def upload_nxt_performance(report: dict) -> bool:
    """NXT 성적표를 Supabase에 저장.

    Args:
        report: build_nxt_performance_report() 반환값
    """
    if not report:
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        cum = report.get("cumulative", {})
        items = report.get("items", [])

        items_json = []
        for it in items:
            items_json.append({
                "rank": it.get("rank"),
                "code": it.get("code"),
                "name": it.get("name"),
                "sector": it.get("sector"),
                "supply_score": it.get("supply_score"),
                "entry_price": it.get("entry_price"),
                "close_price": it.get("close_price"),
                "return_pct": it.get("return_pct"),
            })

        row = {
            "pick_date": report["pick_date"],
            "result_date": report["result_date"],
            "avg_return": report.get("avg_return", 0),
            "best_pick": report.get("best_pick", ""),
            "worst_pick": report.get("worst_pick", ""),
            "weekly_return": cum.get("weekly_return", 0),
            "weekly_days": cum.get("weekly_days", 0),
            "weekly_wins": cum.get("weekly_wins", 0),
            "monthly_return": cum.get("monthly_return", 0),
            "monthly_days": cum.get("monthly_days", 0),
            "monthly_wins": cum.get("monthly_wins", 0),
            "items": items_json,
            # ★ 5/27 catch-up fix ★ NXT 관망일 마커 (웹봇 STALE 알람 사고 후 추가)
            "is_observation_day": bool(report.get("is_observation_day", False)),
            "observation_reason": report.get("observation_reason", None),
        }

        client.table("intelligence_nxt_performance") \
            .upsert(row, on_conflict="pick_date") \
            .execute()

        logger.info(
            f"NXT 성적표 업로드 완료: {report['pick_date']} → "
            f"{report['result_date']} · 평균 {report.get('avg_return', 0):+.2f}%"
        )
        return True

    except Exception as e:
        logger.error(f"NXT 성적표 업로드 실패: {e}")
        return False


def upload_observation_day(pick_date, reason: str) -> bool:
    """★ 5/27 catch-up fix ★ NXT 관망일 마커 row 적재 (웹봇 STALE 알람 차단).

    휴장일/관망일 (NXT 픽 없음)에 row 자동 적재하여 웹봇이 stale 알람 안 띄움.
    is_observation_day=True 마커로 UI에서 "관망일" 명시.

    Args:
        pick_date: 관망일 date (예: date(2026, 5, 23))
        reason: 관망 사유 (예: "KRX 휴장 (토요일)")

    Returns:
        True 성공 / False 실패
    """
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        row = {
            "pick_date": pick_date.isoformat() if hasattr(pick_date, 'isoformat') else str(pick_date),
            "result_date": pick_date.isoformat() if hasattr(pick_date, 'isoformat') else str(pick_date),
            "avg_return": 0.0,
            "items": [],
            "is_observation_day": True,
            "observation_reason": reason,
        }
        client.table("intelligence_nxt_performance") \
            .upsert(row, on_conflict="pick_date") \
            .execute()
        logger.info(f"NXT 관망일 마커 적재: {pick_date} ({reason})")
        return True
    except Exception as e:
        logger.error(f"NXT 관망일 마커 적재 실패: {e}")
        return False


def auto_catchup_observation_days(check_days: int = 7) -> int:
    """★ 5/27 cron 자율 ★ 최근 N일 누락 row 자동 catch-up (관망일 마커).

    매일 17:30 cron에서 호출 → 누락된 휴장일/관망일 row 자동 채움.
    웹봇 STALE 알람 영구 차단.

    Args:
        check_days: 검사 일수 (default 7일)

    Returns:
        적재된 row 수
    """
    from datetime import date, timedelta
    from data.trading_calendar import is_trading_day
    from utils.supabase_sql import query

    today = date.today()
    added = 0

    # 최근 N일 검사
    for offset in range(1, check_days + 1):
        check_date = today - timedelta(days=offset)
        # 이미 row 있는지 확인
        rows = query(
            "SELECT 1 FROM intelligence_nxt_performance WHERE pick_date = %s",
            (check_date,)
        )
        if rows:
            continue   # 이미 존재
        # 휴장일 확인 → 관망일 마커 적재
        if not is_trading_day(check_date):
            weekday = check_date.weekday()
            if weekday == 5:
                reason = "KRX 휴장 (토요일)"
            elif weekday == 6:
                reason = "KRX 휴장 (일요일)"
            else:
                reason = "KRX 공휴일 휴장"
            if upload_observation_day(check_date, reason):
                added += 1

    if added > 0:
        logger.info(f"[NXT catch-up] {added}일 관망일 마커 적재 완료")
    return added


def upload_oneshot_stealth(scan_data: dict) -> bool:
    """원샷 쌍매수 잠복 데이터를 Supabase에 저장.

    NXT TOP5 아래, 매집 레이더 위에 표시.
    최근 7일 내 외인+기관 200억+ 동시매수 후 아직 ±7% 잠복 종목.

    Args:
        scan_data: scan_oneshot_stealth() 반환값
    """
    stealth = scan_data.get("stealth", [])
    if not stealth:
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        stocks_json = []
        for s in stealth:
            stocks_json.append({
                "ticker": s.get("ticker", ""),
                "name": s.get("name", ""),
                "signal_date": s.get("signal_date", ""),
                "frgn_buy": s.get("frgn_buy", 0),
                "inst_buy": s.get("inst_buy", 0),
                "dual_total": s.get("dual_total", 0),
                "signal_close": s.get("signal_close", 0),
                "latest_close": s.get("latest_close", 0),
                "chg_pct": s.get("chg_pct", 0),
            })

        row = {
            "date": scan_data.get("latest_date", ""),
            "lookback_days": scan_data.get("lookback_days", 7),
            "min_dual_buy": scan_data.get("min_dual_buy", 200),
            "stealth_count": scan_data.get("summary", {}).get("stealth_count", 0),
            "gone_count": scan_data.get("summary", {}).get("gone_count", 0),
            "failed_count": scan_data.get("summary", {}).get("failed_count", 0),
            "stocks": stocks_json,
        }

        client.table("intelligence_oneshot_stealth") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(f"원샷 잠복 업로드 완료: {row['date']} · {len(stocks_json)}종목")
        return True

    except Exception as e:
        logger.error(f"원샷 잠복 업로드 실패: {e}")
        return False


def upload_quant_nxt_picks(target_date: str = None) -> bool:
    """NXT 후보 상세 스코어링을 quant_nxt_picks 테이블에 업로드.

    nightwatch nxt_targets + daily OHLCV + flow investor 데이터를 조합.

    Args:
        target_date: "2026-05-06" 형식. None이면 오늘.
    """
    import json
    from datetime import date as _date

    if not target_date:
        target_date = _date.today().isoformat()

    # ── nightwatch_report.json 로드 ──
    nw_path = DATA_STORE / "nightwatch_report.json"
    if not nw_path.exists():
        logger.warning("nightwatch_report.json 없음 — quant_nxt_picks 스킵")
        return False

    try:
        nw = json.loads(nw_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"nightwatch_report 파싱 실패: {e}")
        return False

    if nw.get("date", "") != target_date:
        logger.warning(
            f"nightwatch stale: report={nw.get('date')} != {target_date}"
        )
        return False

    targets = nw.get("nxt_targets", [])
    if not targets:
        logger.warning("nxt_targets 비어있음 — quant_nxt_picks 스킵")
        return False

    # ── 보조 데이터 로더 ──
    def _load_daily(code: str) -> dict:
        """daily CSV에서 최근 20일 OHLCV 로드."""
        fp = DATA_STORE / "daily" / f"{code}.csv"
        if not fp.exists():
            return {}
        try:
            import pandas as pd
            df = pd.read_csv(fp, encoding="utf-8")
            if df.empty:
                return {}
            last = df.iloc[-1]
            close = float(last.get("종가", 0))
            prev_close = float(df.iloc[-2].get("종가", 0)) if len(df) > 1 else close
            ret_d0 = round((close / prev_close - 1) * 100, 2) if prev_close else 0

            vol = float(last.get("거래량", 0))
            vol_ma20 = float(df["거래량"].tail(20).mean()) if len(df) >= 20 else vol
            vol_ratio = round(vol / vol_ma20, 2) if vol_ma20 > 0 else 0

            ma20 = float(df["종가"].tail(20).mean()) if len(df) >= 20 else close
            ma20_dev = round((close / ma20 - 1) * 100, 2) if ma20 else 0

            # RSI 14
            rsi = 50.0
            if len(df) >= 15:
                deltas = df["종가"].diff().tail(14)
                gains = deltas.clip(lower=0).mean()
                losses = (-deltas.clip(upper=0)).mean()
                if losses > 0:
                    rs = gains / losses
                    rsi = round(100 - 100 / (1 + rs), 1)
                elif gains > 0:
                    rsi = 100.0

            return {
                "close": int(close),
                "ret_d0": ret_d0,
                "vol_ratio": vol_ratio,
                "ma20_dev": ma20_dev,
                "rsi": rsi,
            }
        except Exception:
            return {}

    def _load_flow(code: str) -> dict:
        """flow investor CSV에서 수급 데이터 추출."""
        fp = DATA_STORE / "flow" / f"{code}_investor.csv"
        if not fp.exists():
            return {}
        try:
            import pandas as pd
            df = pd.read_csv(fp, encoding="utf-8")
            if df.empty or len(df) < 2:
                return {}

            last = df.iloc[-1]
            foreign_net = round(float(last.get("외국인_금액", 0)) / 10, 1)  # 백만→억
            inst_net = round(float(last.get("기관_금액", 0)) / 10, 1)

            # 연속 매수일 계산
            def _streak(col_name):
                vals = df[col_name].tolist()
                streak = 0
                for v in reversed(vals):
                    if float(v) > 0:
                        streak += 1
                    else:
                        break
                return streak

            foreign_streak = _streak("외국인_금액")
            inst_streak = _streak("기관_금액")
            dual_streak = min(foreign_streak, inst_streak)

            # 5일 누적
            tail5 = df.tail(5)
            foreign_cum = round(float(tail5["외국인_금액"].sum()) / 10, 1)
            inst_cum = round(float(tail5["기관_금액"].sum()) / 10, 1)

            return {
                "foreign_net": foreign_net,
                "inst_net": inst_net,
                "foreign_streak": foreign_streak,
                "inst_streak": inst_streak,
                "dual_streak": dual_streak,
                "foreign_cum": foreign_cum,
                "inst_cum": inst_cum,
            }
        except Exception:
            return {}

    # ── TV 데이터 로드 ──
    tv_data = {}
    tv_path = DATA_STORE / "tv_scanner.json"
    if tv_path.exists():
        try:
            tv_raw = json.loads(tv_path.read_text(encoding="utf-8"))
            if isinstance(tv_raw, dict):
                tv_data = tv_raw
        except Exception:
            pass

    # ── 행 구성 ──
    rows = []
    for t in targets:
        if t.get("is_etf"):
            continue
        code = t.get("code", "")
        if not code:
            continue

        daily = _load_daily(code)
        flow = _load_flow(code)
        tv_info = tv_data.get(code, {})

        supply_score = t.get("supply_score", 0)
        accum_score = flow.get("foreign_cum", 0) + flow.get("inst_cum", 0)

        row = {
            "date": target_date,
            "ticker": code,
            "name": t.get("name", ""),
            "close": daily.get("close", 0),
            "ret_d0": daily.get("ret_d0", 0),
            "vol_ratio": daily.get("vol_ratio", 0),
            "ma20_dev": daily.get("ma20_dev", 0),
            "rsi": daily.get("rsi", 50),
            "tv": round(float(tv_info.get("tv_ratio", 0) or 0), 1),
            "foreign_net": flow.get("foreign_net", 0),
            "inst_net": flow.get("inst_net", 0),
            "foreign_streak": flow.get("foreign_streak", 0),
            "inst_streak": flow.get("inst_streak", 0),
            "dual_streak": flow.get("dual_streak", 0),
            "foreign_cum": flow.get("foreign_cum", 0),
            "inst_cum": flow.get("inst_cum", 0),
            "accum_score": round(accum_score, 1),
            "final_score": supply_score,
            "pension_net": 0,
            "finance_net": 0,
            "pension_5d": 0,
            "finance_5d": 0,
        }
        rows.append(row)

    if not rows:
        logger.warning("quant_nxt_picks 업로드 대상 0건")
        return False

    # ── Supabase 업로드 ──
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        # 당일 기존 데이터 삭제 후 insert
        client.table("quant_nxt_picks") \
            .delete() \
            .eq("date", target_date) \
            .execute()

        client.table("quant_nxt_picks") \
            .insert(rows) \
            .execute()

        logger.info(f"quant_nxt_picks 업로드 완료: {target_date} · {len(rows)}종목")
        return True

    except Exception as e:
        logger.error(f"quant_nxt_picks 업로드 실패: {e}")
        return False
