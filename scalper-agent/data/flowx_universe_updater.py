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

# ── Supabase 클라이언트 (shared 모듈) ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.supabase_client import get_client as _get_client


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
        # ★8/29 [F-166] — `load_stock_data` 는 저장소에 **없는 이름**이다(csv_loader는
        #   클래스만 제공). `except Exception: pass` + `return 1.0` 이라 KR 종목의
        #   volume_ratio 가 **언제나 정확히 1.0**("평소와 동일")으로 FLOWX에 올라가고 있었다
        #   = 거래량이 몇 배로 터진 종목도 정상으로 보고되는 고장난 체온계.
        # ★함정 하나 더 — 이름만 고치면 여전히 실패한다: `load_ohlcv()` 가 돌려주는 컬럼은
        #   **소문자 `volume`**(csv_loader:148)인데 여기선 `df["Volume"]`(대문자)를 읽고 있어
        #   KeyError → 같은 except로 빠져 또 1.0이 된다. 컬럼명까지 같이 고친다.
        # ★8/29 [F-166] 2차 정정 — 장부에 적힌 처방(`CSVLoader().load_ohlcv()`)도 **틀렸다.**
        #   실측: ①CSVLoader.data_dir = `bodyhunter/stock_data_daily`(4,017개)로 **우리 수집본이
        #   아닌 별도 디렉터리**를 본다 ②`load_ohlcv()`는 `df[['date',...]]`를 하는데 `load()`가
        #   date 컬럼을 안 줘서 **KeyError로 자체 파손**돼 있다. 그대로 썼으면 같은 except로
        #   빠져 **여전히 1.0**이었다(실측에서 400/400=100%가 1.0으로 확인됨).
        #   → 프로젝트 표준 로더 `utils.stock_utils.load_daily(code, DAILY_DIR)` 사용.
        #     우리가 매일 검증하는 `data_store/daily/*.csv`(컬럼 `거래량`)를 읽는다.
        from pathlib import Path as _Path
        from utils.stock_utils import load_daily as _load_daily
        _daily_dir = _Path(__file__).resolve().parent.parent / "data_store" / "daily"
        df = _load_daily(code, _daily_dir, min_rows=20)
        if df is not None and len(df) >= 20:
            avg_vol = float(df["거래량"].tail(20).mean())
            if avg_vol > 0:
                return today_vol / avg_vol
    except Exception as e:  # noqa: BLE001
        # ★조용한 실패가 이 버그를 8/11~8/29 살려뒀다 — 이제는 흔적을 남긴다([F-156] 계열).
        logger.debug("[flowx][F-166] volume_ratio 계산 실패 %s: %s — 1.0 폴백", code, e)
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
