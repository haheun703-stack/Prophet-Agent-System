"""
FLOWX sector_universe 테이블 — 수급/거래량 일간 UPDATE
매일 15:40 KST 실행 (정보봇 15:35 → 5분 뒤 충돌 방지)

KR 종목: foreign_net, institution_net, volume_ratio
US 종목: volume_ratio만
"""
import os
import sys
import time
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger("flowx_universe_updater")

# ── Supabase 클라이언트 (lazy) ──
_supabase = None


def _get_client():
    global _supabase
    if _supabase is not None:
        return _supabase

    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    return _supabase


def _get_kis_trader():
    """KIS Trader 인스턴스 (singleton)"""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from bot.kis_trader import KISTrader
        return KISTrader()
    except Exception as e:
        logger.error(f"KISTrader 초기화 실패: {e}")
        return None


def update_sector_universe() -> dict:
    """sector_universe 테이블의 수급/거래량 컬럼 일간 UPDATE

    Returns: {"updated_kr": int, "updated_us": int, "errors": int}
    """
    sb = _get_client()
    if not sb:
        return {"updated_kr": 0, "updated_us": 0, "errors": 1}

    # ── 1. sector_universe에서 전체 종목 조회 ──
    try:
        result = sb.table("sector_universe").select("id,ticker,market").execute()
        rows = result.data or []
    except Exception as e:
        logger.error(f"sector_universe 조회 실패: {e}")
        return {"updated_kr": 0, "updated_us": 0, "errors": 1}

    if not rows:
        logger.warning("sector_universe 비어있음 — 스킵")
        return {"updated_kr": 0, "updated_us": 0, "errors": 0}

    kr_rows = [r for r in rows if r.get("market", "KR") == "KR"]
    us_rows = [r for r in rows if r.get("market", "KR") == "US"]
    logger.info(f"sector_universe: KR {len(kr_rows)}종목, US {len(us_rows)}종목")

    updated_kr = 0
    updated_us = 0
    errors = 0

    # ── 2. KR 종목: foreign_net + institution_net + volume_ratio ──
    if kr_rows:
        trader = _get_kis_trader()
        if not trader:
            logger.error("KISTrader 없음 — KR 업데이트 스킵")
            errors += len(kr_rows)
        else:
            for row in kr_rows:
                ticker = row["ticker"]
                row_id = row["id"]
                try:
                    # 투자자별 매매동향 (당일 = 첫 번째 행)
                    inv = trader.fetch_investor_daily(ticker)
                    # 현재가 (거래량)
                    price_info = trader.fetch_price(ticker)

                    update_data = {}

                    if inv and inv.get("success") and inv.get("data") and len(inv["data"]) > 0:
                        today_data = inv["data"][0]  # 최신일 = 첫 행
                        update_data["foreign_net"] = today_data.get("foreign_net_qty", 0)
                        update_data["institution_net"] = today_data.get("inst_net_qty", 0)

                    if price_info and price_info.get("success"):
                        vol = price_info.get("volume", 0)
                        # volume_ratio = 당일거래량 / 20일평균 (일봉 데이터 없으면 1.0)
                        vol_ratio = _calc_volume_ratio_kr(trader, ticker, vol)
                        update_data["volume_ratio"] = round(vol_ratio, 2)

                    if update_data:
                        update_data["updated_at"] = date.today().isoformat()
                        sb.table("sector_universe").update(update_data).eq(
                            "id", row_id
                        ).execute()
                        updated_kr += 1

                    # KIS API rate limit 방어
                    time.sleep(0.15)

                except Exception as e:
                    logger.warning(f"KR {ticker} 업데이트 실패: {e}")
                    errors += 1

    # ── 3. US 종목: volume_ratio만 ──
    for row in us_rows:
        ticker = row["ticker"]
        row_id = row["id"]
        try:
            vol_ratio = _calc_volume_ratio_us(ticker)
            if vol_ratio > 0:
                sb.table("sector_universe").update({
                    "volume_ratio": round(vol_ratio, 2),
                    "updated_at": date.today().isoformat(),
                }).eq("id", row_id).execute()
                updated_us += 1
        except Exception as e:
            logger.warning(f"US {ticker} 업데이트 실패: {e}")
            errors += 1

    summary = {
        "updated_kr": updated_kr,
        "updated_us": updated_us,
        "errors": errors,
    }
    logger.info(f"sector_universe UPDATE 완료: {summary}")
    return summary


def _calc_volume_ratio_kr(trader, code: str, today_vol: int) -> float:
    """KR 종목 volume_ratio = 당일거래량 / 20일 평균거래량"""
    try:
        from data.csv_loader import load_stock_data
        df = load_stock_data(code)
        if df is not None and len(df) >= 20:
            avg_vol = df["Volume"].tail(20).mean()
            if avg_vol > 0:
                return today_vol / avg_vol
    except Exception:
        pass
    # 로컬 CSV 없으면 1.0 반환
    return 1.0


def _calc_volume_ratio_us(ticker: str) -> float:
    """US 종목 volume_ratio (yfinance)"""
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        hist = tk.history(period="1mo")
        if hist is not None and len(hist) >= 5:
            today_vol = hist["Volume"].iloc[-1]
            avg_vol = hist["Volume"].iloc[:-1].mean()
            if avg_vol > 0:
                return today_vol / avg_vol
    except Exception as e:
        logger.debug(f"US volume_ratio 실패 {ticker}: {e}")
    return 0.0


# ── CLI 테스트 ──
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = update_sector_universe()
    print(result)
