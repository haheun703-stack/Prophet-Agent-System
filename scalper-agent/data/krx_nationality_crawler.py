"""
KRX 외국인 국적별 거래량 수집기 (HARD053)
─────────────────────────────────────────────
하이브리드: Playwright(네이버 OAuth 로그인) + HTTP(JSON API 데이터)

로그인: Playwright → 네이버 OAuth → KRX 세션 → 쿠키 저장
데이터: requests + JSON API (MDCHARD05302, 브라우저 불필요)

사용:
    # CLI
    python krx_nationality_crawler.py 000660                        # SK하이닉스 (최근 5일)
    python krx_nationality_crawler.py 005930 20260304               # 삼성전자 (종료일 지정)
    python krx_nationality_crawler.py 005930 20260304 20260228      # 기간 지정
    python krx_nationality_crawler.py --login                       # 수동 로그인

    # 코드
    from data.krx_nationality_crawler import fetch_nationality, fetch_nationality_batch
    df = fetch_nationality("000660")
    df = fetch_nationality("000660", "20260228", "20260304")
    results = fetch_nationality_batch(["000660", "005930"])

    # async (텔레그램 봇)
    from data.krx_nationality_crawler import afetch_nationality_batch
    results = await afetch_nationality_batch(["000660", "005930"], "20260228", "20260304")
"""

import asyncio
import json
import logging
import os
import sys
import time
import requests
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ── 환경변수 ──
ENV_PATH = Path(__file__).parent.parent.parent / ".env"
load_dotenv(ENV_PATH)
KRX_ID = os.getenv("KRX_ID", "")
KRX_PW = os.getenv("KRX_PW", "")

# ── 경로 ──
DATA_DIR = Path(__file__).parent.parent / "data_store" / "nationality"
DATA_DIR.mkdir(parents=True, exist_ok=True)
COOKIE_FILE = Path(__file__).parent.parent / "data_store" / "krx_cookies.json"

# ── KRX API URLs ──
GEN_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
DOWN_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
KRX_LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
HARD053_URL = "https://data.krx.co.kr/contents/MDC/HARD/hardController/MDCHARD053.cmd"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030106",
}

REQUEST_INTERVAL = 0.5  # KRX rate limit (초)


# ═══════════════════════════════════════════
# ISIN 계산
# ═══════════════════════════════════════════

def compute_isin(stock_code: str) -> str:
    """한국 주식 종목코드(6자리) → ISIN 변환 (KR7XXXXXX00C)"""
    base = f"KR7{stock_code}00"
    digits = ""
    for ch in base:
        if ch.isdigit():
            digits += ch
        else:
            digits += str(ord(ch) - 55)
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    check = (10 - (total % 10)) % 10
    return base + str(check)


# ═══════════════════════════════════════════
# 쿠키 관리
# ═══════════════════════════════════════════

def _load_cookies():
    """저장된 KRX 쿠키 로드 → requests용 dict"""
    if COOKIE_FILE.exists():
        try:
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)
            if cookies:
                return cookies
        except Exception:
            pass
    return None


def _save_cookies(cookies):
    """쿠키 저장 (Playwright format)"""
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)


def _cookies_to_dict(pw_cookies: list) -> dict:
    """Playwright 쿠키 리스트 → requests용 {name: value} dict"""
    return {c["name"]: c["value"] for c in pw_cookies if "krx.co.kr" in c.get("domain", "")}


def _build_session(cookies: list = None) -> requests.Session:
    """KRX 세션 생성 (쿠키 적용)"""
    session = requests.Session()
    session.headers.update(HEADERS)
    if cookies:
        for c in cookies:
            if "krx.co.kr" in c.get("domain", ""):
                session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
    return session


def _test_session(session: requests.Session) -> bool:
    """세션 유효성 테스트 (JSON API로 삼성전자 1일 조회)"""
    try:
        # JSON API로 직접 테스트 (HARD053 국적별 데이터)
        json_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        params = {
            "bld": "dbms/MDC/HARD/MDCHARD05302",
            "locale": "ko_KR",
            "mktId": "STK",
            "isuCd": "KR7005930003",
            "isuCd2": "005930",
            "strtDd": datetime.now().strftime("%Y%m%d"),
            "endDd": datetime.now().strftime("%Y%m%d"),
        }
        resp = session.post(json_url, data=params, timeout=10)
        if resp.status_code != 200:
            return False
        text = resp.text.strip()
        # LOGOUT이면 실패, JSON 형태면 성공 (데이터 없어도 {} 반환)
        if "LOGOUT" in text[:20]:
            return False
        data = resp.json()
        return isinstance(data, dict)
    except Exception:
        return False


# ═══════════════════════════════════════════
# Playwright 로그인 (네이버 OAuth)
# ═══════════════════════════════════════════

async def _playwright_login(headless: bool = True) -> list:
    """Playwright로 KRX 네이버 로그인 → 쿠키 반환

    흐름:
      1. KRX 로그인 페이지 접속
      2. "네이버로 로그인하기" 클릭
      3. 네이버 ID/PW 입력 → 로그인
      4. KRX로 리다이렉트 → 세션 확인
      5. 쿠키 저장 + 반환
    """
    from playwright.async_api import async_playwright

    if not KRX_ID or not KRX_PW:
        logger.error(".env에 KRX_ID / KRX_PW 없음")
        return []

    logger.info("KRX 네이버 로그인 시작 (Playwright)...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )
        page = await context.new_page()

        try:
            # ── 1. KRX 로그인 페이지 ──
            await page.goto(KRX_LOGIN_URL, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            # ── 2. "네이버로 로그인하기" 버튼 클릭 ──
            naver_btn = page.frame_locator("#COMS001_FRAME").locator(
                'text=네이버로 로그인하기'
            )
            try:
                if await naver_btn.is_visible(timeout=5000):
                    await naver_btn.click()
                    logger.info("네이버 로그인 버튼 클릭 (iframe)")
                    await page.wait_for_timeout(3000)
            except Exception:
                # iframe이 아닌 경우 직접 탐색
                naver_btn2 = page.locator('text=네이버로 로그인하기')
                try:
                    if await naver_btn2.is_visible(timeout=3000):
                        await naver_btn2.click()
                        logger.info("네이버 로그인 버튼 클릭 (직접)")
                        await page.wait_for_timeout(3000)
                except Exception:
                    # 이미 네이버로 리다이렉트됐을 수 있음
                    pass

            # 네이버 페이지 안 갔으면 HARD053 시도 (자동 리다이렉트)
            if "naver.com" not in page.url.lower():
                logger.info("네이버 미도착 → HARD053 시도 (자동 리다이렉트)")
                await page.goto(HARD053_URL, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(5000)

            # ── 3. 네이버 로그인 페이지 대기 + 입력 ──
            # 리다이렉트 안정화
            for _ in range(10):
                if "nid.naver" in page.url.lower():
                    break
                await page.wait_for_timeout(2000)

            if "naver.com" not in page.url.lower():
                logger.error(f"네이버 페이지 미도착: {page.url}")
                await page.screenshot(path=str(DATA_DIR / "login_debug.png"))
                await browser.close()
                return []

            logger.info("네이버 로그인 페이지 도착")
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)

            # ID 입력
            id_el = page.locator("#id")
            await id_el.click()
            await page.wait_for_timeout(500)
            # JS로 값 주입 (네이버 봇 탐지 대응)
            await page.evaluate(f"""() => {{
                var el = document.querySelector('#id');
                if (el) {{
                    el.focus();
                    el.value = '{KRX_ID}';
                    el.dispatchEvent(new Event('input', {{bubbles:true}}));
                }}
            }}""")
            await page.wait_for_timeout(500)

            # PW 입력
            await page.evaluate(f"""() => {{
                var el = document.querySelector('#pw');
                if (el) {{
                    el.focus();
                    el.value = '{KRX_PW}';
                    el.dispatchEvent(new Event('input', {{bubbles:true}}));
                }}
            }}""")
            await page.wait_for_timeout(500)

            # 로그인 버튼 클릭
            login_btn = page.locator("#log\\.login, .btn_login, button:has-text('로그인')").first
            await login_btn.click()
            logger.info("네이버 로그인 버튼 클릭")

            # ── 4. 로그인 결과 대기 (2FA 대응) ──
            await page.wait_for_timeout(5000)

            # 2단계 인증 페이지 감지
            page_text = await page.content()
            if "2단계 인증" in page_text or "nid.naver" in page.url.lower():
                logger.info("네이버 2단계 인증 → 핸드폰 알림 승인 대기 (최대 60초)...")

                # "이 브라우저는 2단계 인증 없이 로그인" 체크
                try:
                    skip_2fa = page.locator(
                        'text=이 브라우저는, '
                        'input[type="checkbox"]'
                    ).first
                    if await skip_2fa.is_visible(timeout=3000):
                        await skip_2fa.check()
                        logger.info("2FA 스킵 체크박스 체크")
                except Exception:
                    pass

                # 60초간 승인 대기 (3초 간격)
                for i in range(20):
                    await page.wait_for_timeout(3000)
                    if "data.krx.co.kr" in page.url.lower():
                        logger.info(f"2FA 승인 완료! ({(i+1)*3}초)")
                        break
                    if "nid.naver" not in page.url.lower():
                        break
                    if i % 5 == 4:
                        logger.info(f"  핸드폰에서 네이버 알림을 승인해주세요... ({(i+1)*3}초)")
                else:
                    logger.error("2FA 시간 초과 (60초)")
                    await page.screenshot(path=str(DATA_DIR / "naver_2fa_timeout.png"))
                    await browser.close()
                    return []

            # ── 5. KRX 리다이렉트 대기 ──
            for _ in range(15):
                await page.wait_for_timeout(2000)
                if "data.krx.co.kr" in page.url.lower():
                    break

            if "data.krx.co.kr" not in page.url.lower():
                logger.error(f"KRX 리다이렉트 실패: {page.url}")
                await page.screenshot(path=str(DATA_DIR / "login_redirect_fail.png"))
                await browser.close()
                return []

            logger.info("KRX 리다이렉트 완료!")

            # ── 6. 쿠키 저장 ──
            cookies = await context.cookies()
            _save_cookies(cookies)
            logger.info(f"쿠키 저장: {len(cookies)}개")

            await browser.close()
            return cookies

        except Exception as e:
            logger.error(f"Playwright 로그인 오류: {e}")
            try:
                await page.screenshot(path=str(DATA_DIR / "login_error.png"))
            except Exception:
                pass
            await browser.close()
            return []


async def _manual_login() -> list:
    """수동 로그인 (자동 실패 시 백업)"""
    from playwright.async_api import async_playwright

    print("\n" + "=" * 50)
    print("  KRX Data 수동 로그인")
    print("=" * 50)
    print("1. 브라우저에서 네이버로 KRX 로그인하세요")
    print("2. HARD053 페이지가 나오면 Enter")
    print("=" * 50 + "\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )
        page = await context.new_page()
        await page.goto(KRX_LOGIN_URL, wait_until="networkidle", timeout=30000)
        input("로그인 완료 후 Enter... ")
        cookies = await context.cookies()
        _save_cookies(cookies)
        print(f"쿠키 저장: {len(cookies)}개")
        await browser.close()
    return cookies


# ═══════════════════════════════════════════
# HTTP 데이터 다운로드 (OTP → CSV)
# ═══════════════════════════════════════════

def _fetch_nationality_http(
    session: requests.Session,
    stock_code: str,
    date_from: str,
    date_to: str,
    mkt_id: str = "STK",
) -> pd.DataFrame:
    """KRX HARD053 JSON API로 외국인 국적별 거래량 수집

    Args:
        session: 인증된 requests 세션
        stock_code: 종목코드 6자리
        date_from: 시작일 YYYYMMDD
        date_to: 종료일 YYYYMMDD
        mkt_id: 시장 (STK/KSQ)

    Returns:
        None = 세션 만료(LOGOUT), DataFrame() = 데이터 없음, DataFrame = 성공
    """
    isin = compute_isin(stock_code)
    json_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    params = {
        "bld": "dbms/MDC/HARD/MDCHARD05302",
        "locale": "ko_KR",
        "mktId": mkt_id,
        "isuCd": isin,
        "isuCd2": stock_code,
        "strtDd": date_from,
        "endDd": date_to,
    }

    try:
        resp = session.post(json_url, data=params, timeout=15)

        if resp.status_code == 400 or "LOGOUT" in resp.text[:20]:
            logger.warning("세션 만료 (LOGOUT)")
            return None

        data = resp.json()
        rows = data.get("block1", [])

        if not rows:
            return pd.DataFrame()

        # 3쌍 (A/B/C) → 단일 [국가명, 거래량] 변환
        records = []
        for row in rows:
            for suffix in ["A", "B", "C"]:
                country = row.get(f"CNTR_NM_{suffix}", "").strip()
                vol_str = row.get(f"ACC_TRDVOL_{suffix}", "0").replace(",", "")
                if country:
                    try:
                        vol = int(vol_str) if vol_str else 0
                    except (ValueError, TypeError):
                        vol = 0
                    records.append({"국가명": country, "거래량": vol})

        if not records:
            return pd.DataFrame()

        df = (
            pd.DataFrame(records)
            .sort_values("거래량", ascending=False)
            .reset_index(drop=True)
        )
        return df

    except Exception as e:
        logger.error(f"국적별 API 오류: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════
# KRX 직접 로그인 (smile.krx.co.kr)
# ═══════════════════════════════════════════

SMILE_LOGIN_URL = "https://smile.krx.co.kr/oauth/userLogin"


def _direct_login() -> requests.Session:
    """KRX 자체 계정으로 직접 로그인 (Playwright 불필요)

    smile.krx.co.kr → POST userId/userPw → 세션 쿠키 발급
    네이버 OAuth보다 안정적 (봇탐지/2FA 없음)
    """
    if not KRX_ID or not KRX_PW:
        logger.warning("KRX_ID / KRX_PW 미설정 → 직접 로그인 불가")
        return None

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        resp = session.post(
            SMILE_LOGIN_URL,
            data={"userId": KRX_ID, "userPw": KRX_PW},
            timeout=10,
            allow_redirects=True,
        )
        # 302 → 200 (redirect followed) = 성공
        if resp.status_code == 200 and session.cookies:
            if _test_session(session):
                logger.info(f"KRX 직접 로그인 성공 (쿠키 {len(session.cookies)}개)")
                # 쿠키를 Playwright 포맷으로 저장 (호환)
                pw_cookies = [
                    {"name": c.name, "value": c.value, "domain": c.domain or ".krx.co.kr"}
                    for c in session.cookies
                ]
                _save_cookies(pw_cookies)
                return session
            else:
                logger.warning("KRX 직접 로그인: 세션은 생성됐으나 데이터 접근 불가")
                return None

        logger.warning(f"KRX 직접 로그인 실패: status={resp.status_code}")
        return None

    except Exception as e:
        logger.error(f"KRX 직접 로그인 오류: {e}")
        return None


# ═══════════════════════════════════════════
# 세션 관리 + 자동 재로그인
# ═══════════════════════════════════════════

def _get_valid_session() -> requests.Session:
    """유효한 KRX 세션 반환

    우선순위:
      1. 기존 쿠키 재사용
      2. KRX 직접 로그인 (smile.krx.co.kr) ← 가장 안정적
      3. Playwright 네이버 OAuth (백업)
    """
    # 1) 기존 쿠키
    cookies = _load_cookies()
    if cookies:
        session = _build_session(cookies)
        if _test_session(session):
            logger.info("기존 쿠키 유효")
            return session
        logger.warning("쿠키 만료 → 재로그인")

    # 2) KRX 직접 로그인 (1순위)
    session = _direct_login()
    if session:
        return session

    # 3) Playwright 네이버 OAuth (백업)
    logger.info("직접 로그인 실패 → Playwright 네이버 OAuth 시도")
    try:
        new_cookies = asyncio.run(_playwright_login(headless=True))
        if new_cookies:
            session = _build_session(new_cookies)
            if _test_session(session):
                logger.info("Playwright 로그인 성공!")
                return session
    except Exception as e:
        logger.warning(f"Playwright 백업 실패: {e}")

    logger.error("모든 로그인 방법 실패")
    return None


async def _get_valid_session_async() -> requests.Session:
    """async 버전: 유효한 KRX 세션 반환

    우선순위: 쿠키 → 직접 로그인 → Playwright
    """
    # 1) 기존 쿠키
    cookies = _load_cookies()
    if cookies:
        session = _build_session(cookies)
        valid = await asyncio.to_thread(_test_session, session)
        if valid:
            logger.info("기존 쿠키 유효")
            return session
        logger.warning("쿠키 만료 → 재로그인")

    # 2) KRX 직접 로그인
    session = await asyncio.to_thread(_direct_login)
    if session:
        return session

    # 3) Playwright 백업
    logger.info("직접 로그인 실패 → Playwright 네이버 OAuth 시도")
    try:
        new_cookies = await _playwright_login(headless=True)
        if new_cookies:
            session = _build_session(new_cookies)
            valid = await asyncio.to_thread(_test_session, session)
            if valid:
                logger.info("Playwright 로그인 성공!")
                return session
    except Exception as e:
        logger.warning(f"Playwright 백업 실패: {e}")

    logger.error("모든 로그인 방법 실패")
    return None


# ═══════════════════════════════════════════
# 공개 API
# ═══════════════════════════════════════════

def fetch_nationality(
    stock_code: str,
    date_from: str = None,
    date_to: str = None,
    **kwargs,
) -> pd.DataFrame:
    """외국인 국적별 거래량 조회 (단일 종목)

    Args:
        stock_code: 종목코드 6자리
        date_from: 시작일 YYYYMMDD (기본: 5일 전)
        date_to: 종료일 YYYYMMDD (기본: 오늘)

    Returns: DataFrame [국가명, 거래량]
    """
    if not date_to:
        date_to = datetime.now().strftime("%Y%m%d")
    if not date_from:
        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    session = _get_valid_session()
    if not session:
        logger.error("유효한 세션 없음")
        return pd.DataFrame()

    logger.info(f"국적별 수급: {stock_code} ({date_from}~{date_to})")

    # STK 먼저, 실패시 KSQ
    for mkt in ["STK", "KSQ"]:
        df = _fetch_nationality_http(session, stock_code, date_from, date_to, mkt)
        if df is None:
            # LOGOUT → 재로그인 후 재시도
            session = _get_valid_session()
            if session:
                df = _fetch_nationality_http(session, stock_code, date_from, date_to, mkt)
        if df is not None and not df.empty:
            save_path = DATA_DIR / f"nationality_{stock_code}.csv"
            df.to_csv(save_path, index=False, encoding="utf-8-sig")
            logger.info(f"저장: {save_path} ({len(df)}행)")
            return df
        time.sleep(REQUEST_INTERVAL)

    return pd.DataFrame()


def fetch_nationality_batch(
    stock_codes: list,
    date_from: str = None,
    date_to: str = None,
    **kwargs,
) -> dict:
    """여러 종목 배치 조회

    Args:
        stock_codes: 종목코드 리스트
        date_from: 시작일 YYYYMMDD (기본: 5일 전)
        date_to: 종료일 YYYYMMDD (기본: 오늘)

    Returns: {code: DataFrame}
    """
    if not date_to:
        date_to = datetime.now().strftime("%Y%m%d")
    if not date_from:
        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    session = _get_valid_session()
    if not session:
        return {}

    results = {}
    for i, code in enumerate(stock_codes):
        logger.info(f"[{i+1}/{len(stock_codes)}] {code}")

        for mkt in ["STK", "KSQ"]:
            df = _fetch_nationality_http(session, code, date_from, date_to, mkt)
            if df is None:
                # LOGOUT → 재로그인
                session = _get_valid_session()
                if session:
                    df = _fetch_nationality_http(session, code, date_from, date_to, mkt)
            if df is not None and not df.empty:
                save_path = DATA_DIR / f"nationality_{code}.csv"
                df.to_csv(save_path, index=False, encoding="utf-8-sig")
                results[code] = df
                logger.info(f"  {code}: {len(df)}행")
                break
            time.sleep(REQUEST_INTERVAL)
        else:
            results[code] = pd.DataFrame()

        if i < len(stock_codes) - 1:
            time.sleep(REQUEST_INTERVAL)

    return results


async def afetch_nationality_batch(
    stock_codes: list,
    date_from: str = None,
    date_to: str = None,
    **kwargs,
) -> dict:
    """여러 종목 배치 조회 (async - 텔레그램 봇용)

    Args:
        stock_codes: 종목코드 리스트
        date_from: 시작일 YYYYMMDD (기본: 5일 전)
        date_to: 종료일 YYYYMMDD (기본: 오늘)
    """
    if not date_to:
        date_to = datetime.now().strftime("%Y%m%d")
    if not date_from:
        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    session = await _get_valid_session_async()
    if not session:
        return {}

    results = {}
    for i, code in enumerate(stock_codes):
        logger.info(f"[{i+1}/{len(stock_codes)}] {code}")

        for mkt in ["STK", "KSQ"]:
            df = await asyncio.to_thread(
                _fetch_nationality_http, session, code, date_from, date_to, mkt
            )
            if df is None:
                session = await _get_valid_session_async()
                if session:
                    df = await asyncio.to_thread(
                        _fetch_nationality_http, session, code, date_from, date_to, mkt
                    )
            if df is not None and not df.empty:
                save_path = DATA_DIR / f"nationality_{code}.csv"
                df.to_csv(save_path, index=False, encoding="utf-8-sig")
                results[code] = df
                logger.info(f"  {code}: {len(df)}행")
                break
            await asyncio.sleep(REQUEST_INTERVAL)
        else:
            results[code] = pd.DataFrame()

        if i < len(stock_codes) - 1:
            await asyncio.sleep(REQUEST_INTERVAL)

    return results


# ═══════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if "--login" in sys.argv:
        asyncio.run(_manual_login())
        sys.exit(0)

    code = sys.argv[1] if len(sys.argv) > 1 else "000660"
    date_to = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y%m%d")
    date_from = sys.argv[3] if len(sys.argv) > 3 else (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    print(f"\nKRX 외국인 국적별 거래량")
    print(f"종목: {code} | 기간: {date_from}~{date_to}")
    print(f"{'=' * 60}\n")

    df = fetch_nationality(code, date_from, date_to)

    if not df.empty:
        print(f"\n{'=' * 60}")
        print(f"[OK] {len(df)}행 데이터")
        print(f"{'=' * 60}")
        print(df.to_string())

        # 주요국 하이라이트
        name_col = None
        for col in df.columns:
            if "국가" in col or "country" in col.lower():
                name_col = col
                break
        if name_col:
            print(f"\n--- 주요국 ---")
            for country in ["중국", "홍콩", "영국", "미국", "일본", "싱가포르"]:
                row = df[df[name_col].str.contains(country, na=False)]
                if not row.empty:
                    print(f"  {country}: {row.iloc[0].to_dict()}")
    else:
        print(f"\n[FAIL] 데이터 없음")
        print(f"  → 비영업일이거나 로그인 실패")
        print(f"  → 수동 로그인: python {__file__} --login")
