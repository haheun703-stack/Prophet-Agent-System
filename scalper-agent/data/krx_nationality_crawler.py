"""
KRX SMILE 외국인 국적별 매매 데이터 크롤러
─────────────────────────────────────────────
data.krx.co.kr → HARD053 (종목별 외국인 국적 분류)
Playwright 브라우저 자동화 + 쿠키 기반 세션 관리

사전 조건:
  pip install playwright pandas python-dotenv
  playwright install chromium
  최초 1회: python krx_nationality_crawler.py --login  (수동 로그인 → 쿠키 저장)

사용:
    # CLI
    python krx_nationality_crawler.py 000660              # SK하이닉스 (최근 5일)
    python krx_nationality_crawler.py 005930 20260201 20260304  # 기간 지정
    python krx_nationality_crawler.py --login             # 수동 로그인 모드

    # 코드
    from data.krx_nationality_crawler import fetch_nationality
    df = fetch_nationality("000660")
"""

import asyncio
import json
import logging
import os
import sys
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ── 환경변수 ──
ENV_PATH = Path(__file__).parent.parent.parent / ".env"
load_dotenv(ENV_PATH)

# ── 경로 ──
DATA_DIR = Path(__file__).parent.parent / "data_store" / "nationality"
DATA_DIR.mkdir(parents=True, exist_ok=True)
COOKIE_FILE = Path(__file__).parent.parent / "data_store" / "krx_cookies.json"

# ── KRX HARD053 URL (종목별 외국인 국적 분류) ──
HARD053_URL = "https://data.krx.co.kr/contents/MDC/HARD/hardController/MDCHARD053.cmd"


# ═══════════════════════════════════════════
# ISIN 계산
# ═══════════════════════════════════════════

def compute_isin(stock_code: str) -> str:
    """한국 주식 종목코드(6자리) → ISIN 변환 (KR7XXXXXX00C)

    ISIN = KR(2) + NSIN(9) + check(1) = 12자리
    한국 NSIN = 7 + stock_code(6) + 00
    체크디짓 = Luhn 알고리즘
    """
    base = f"KR7{stock_code}00"

    # 문자 → 숫자 변환 (A=10, B=11, ..., Z=35)
    digits = ""
    for ch in base:
        if ch.isdigit():
            digits += ch
        else:
            digits += str(ord(ch) - 55)  # A=10, K=20, R=27

    # Luhn 알고리즘
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
    """저장된 쿠키 로드"""
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
    """쿠키 저장"""
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)


# ═══════════════════════════════════════════
# 수동 로그인
# ═══════════════════════════════════════════

async def manual_login():
    """수동 로그인: 브라우저 → 사용자 로그인 → 쿠키 저장

    1. 브라우저가 열리면 KRX Data 로그인
    2. '깊이 있는 통계' 메뉴까지 접근  (HARD 세션 활성화)
    3. 완료 후 터미널에서 Enter
    """
    from playwright.async_api import async_playwright

    print("\n" + "=" * 50)
    print("  KRX Data 수동 로그인")
    print("=" * 50)
    print("1. 브라우저에서 KRX Data 로그인하세요")
    print("2. 좌측 '깊이 있는 통계' 메뉴 클릭 (HARD 세션 활성화)")
    print("3. 완료 후 이 터미널에서 Enter")
    print("=" * 50 + "\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )
        page = await context.new_page()
        await page.goto("https://data.krx.co.kr", wait_until="networkidle", timeout=30000)

        input("로그인 + HARD 메뉴 접근 완료 후 Enter... ")

        cookies = await context.cookies()
        _save_cookies(cookies)
        print(f"쿠키 저장 완료: {len(cookies)}개 → {COOKIE_FILE}")
        await browser.close()

    return True


# ═══════════════════════════════════════════
# 크롤링 코어
# ═══════════════════════════════════════════

async def _crawl_nationality(
    stock_code: str,
    date_from: str,
    date_to: str,
    headless: bool = True,
) -> pd.DataFrame:
    """HARD053 페이지에서 외국인 국적별 매매 데이터 수집

    흐름:
      1. 쿠키 복원 → HARD053 직접 접속
      2. 종목 변경 (finder popup or JS)
      3. 날짜 설정 (JS — readonly 필드)
      4. 조회 (CI.MDI.search() JS 호출)
      5. 데이터 추출 (대형 table → pd.read_html)
    """
    from playwright.async_api import async_playwright

    cookies = _load_cookies()
    if not cookies:
        logger.error("저장된 쿠키 없음 → --login으로 먼저 로그인 필요")
        return pd.DataFrame()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )
        await context.add_cookies(cookies)
        logger.info(f"쿠키 복원: {len(cookies)}개")

        page = await context.new_page()

        try:
            # ── 1. HARD053 직접 접속 ──
            logger.info(f"HARD053 접속... 종목={stock_code} 기간={date_from}~{date_to}")
            await page.goto(HARD053_URL, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            # 세션 유효성 체크: 데이터 테이블 존재 여부
            tables = page.locator("table")
            table_count = await tables.count()

            if table_count < 2:
                logger.error(f"테이블 {table_count}개 → 세션 만료. --login 재실행 필요")
                await page.screenshot(path=str(DATA_DIR / "session_expired.png"), full_page=True)
                await browser.close()
                return pd.DataFrame()

            logger.info(f"페이지 로드 OK (테이블 {table_count}개)")

            # ── 2. 종목 변경 ──
            await _set_stock(page, stock_code)

            # ── 3. 날짜 설정 (readonly → JS) ──
            await page.evaluate(f"""() => {{
                var s = document.querySelector('[name=strtDd]');
                var e = document.querySelector('[name=endDd]');
                if (s) {{ s.removeAttribute('readonly'); s.value = '{date_from}'; }}
                if (e) {{ e.removeAttribute('readonly'); e.value = '{date_to}'; }}
            }}""")
            logger.info(f"날짜: {date_from} ~ {date_to}")

            # ── 4. 조회 실행 ──
            await _trigger_search(page)

            # ── 5. 결과 대기 ──
            await page.wait_for_timeout(5000)
            try:
                await page.wait_for_selector(
                    ".CI-LOADING, .loading, .spinner",
                    state="hidden", timeout=15000,
                )
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            # 스크린샷
            await page.screenshot(
                path=str(DATA_DIR / f"result_{stock_code}.png"), full_page=True,
            )

            # ── 6. 데이터 추출 ──
            df = await _extract_table(page)

            if df.empty:
                logger.warning(f"데이터 없음 → result_{stock_code}.png 확인")
                return pd.DataFrame()

            # ── 7. 3쌍 → 단일 테이블 정리 + 저장 ──
            df_clean = _reshape_nationality(df)
            save_path = DATA_DIR / f"nationality_{stock_code}.csv"
            df_clean.to_csv(save_path, index=False, encoding="utf-8-sig")
            logger.info(f"저장: {save_path} ({len(df_clean)}개국)")

            return df_clean

        except Exception as e:
            logger.error(f"크롤링 오류: {e}")
            try:
                await page.screenshot(
                    path=str(DATA_DIR / f"error_{datetime.now():%H%M%S}.png"),
                    full_page=True,
                )
            except Exception:
                pass
            raise
        finally:
            try:
                _save_cookies(await context.cookies())
            except Exception:
                pass
            await browser.close()


# ── 종목 변경 ──

async def _set_stock(page, stock_code: str):
    """종목코드 입력 + 자동완성/finder 선택

    시도 순서:
      1) 가시 입력창에 코드 타이핑 → 자동완성 드롭다운 선택
      2) 실패 시 → ISIN 계산 후 hidden 필드 JS 직접 설정
    """
    stock_input = page.locator('input[name="tboxisuCd_finder_stkisu0"]')
    if await stock_input.count() == 0:
        logger.warning("종목 입력창 없음")
        return

    # 현재 값 확인 — 이미 원하는 종목이면 스킵
    current_val = await stock_input.get_attribute("value") or ""
    if stock_code in current_val:
        logger.info(f"이미 {stock_code} 선택됨: {current_val}")
        return

    # 방법 1: 입력 → 자동완성
    logger.info(f"종목 변경: {current_val} → {stock_code}")
    await stock_input.click()
    await stock_input.fill("")
    await stock_input.type(stock_code, delay=80)
    await page.wait_for_timeout(2000)

    # 자동완성 드롭다운 탐색
    autocomplete_found = False
    for sel in [
        ".CI-AUTOCOMPLETE-LIST li",
        ".ui-autocomplete li",
        ".autocomplete-suggestions div",
        f'li:has-text("{stock_code}")',
    ]:
        try:
            item = page.locator(sel).first
            if await item.is_visible(timeout=2000):
                text = (await item.inner_text()).strip()
                await item.click()
                logger.info(f"자동완성 선택: {text}")
                autocomplete_found = True
                await page.wait_for_timeout(1000)
                break
        except Exception:
            continue

    if autocomplete_found:
        return

    # 방법 2: ISIN 직접 설정
    isin = compute_isin(stock_code)
    logger.info(f"자동완성 실패 → JS 직접: isuCd={isin}")
    await page.evaluate(f"""() => {{
        var cd = document.querySelector('[name=isuCd]');
        var cd2 = document.querySelector('[name=isuCd2]');
        if (cd) cd.value = '{isin}';
        if (cd2) cd2.value = '{isin}';
    }}""")


# ── 조회 트리거 ──

async def _trigger_search(page):
    """조회 실행 (여러 방법 시도)"""

    # 방법 1: KRX 표준 JS 함수
    try:
        await page.evaluate("CI.MDI.search()")
        logger.info("조회: CI.MDI.search()")
        return
    except Exception as e:
        logger.debug(f"CI.MDI.search() 실패: {e}")

    # 방법 2: 조회 버튼 클릭 ("닫기" 제외)
    for sel in [
        "button.CI-BTN-SEARCH",
        "a.CI-BTN-SEARCH",
        ".CI-MDI-BTN-SEARCH",
        'button:has-text("조회")',
        'a:has-text("조회")',
        'input[value="조회"]',
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                text = (await btn.inner_text()).strip() if sel.startswith(("button", "a")) else ""
                if "닫기" in text:
                    continue
                await btn.click()
                logger.info(f"조회 버튼: {sel}")
                return
        except Exception:
            continue

    # 방법 3: 아이콘 버튼 (돋보기)
    try:
        search_icons = page.locator('button:has(img[alt*="조회"]), button:has(i.search), a.btn-search')
        cnt = await search_icons.count()
        if cnt > 0:
            await search_icons.first.click()
            logger.info("조회: 아이콘 버튼")
            return
    except Exception:
        pass

    logger.warning("조회 트리거를 찾지 못함")


# ── 데이터 추출 ──

async def _extract_table(page) -> pd.DataFrame:
    """페이지에서 국적별 데이터 테이블 추출

    대형 테이블(10행 이상)을 뒤에서부터 탐색하여 데이터 테이블 식별
    """
    tables = page.locator("table")
    table_count = await tables.count()
    logger.info(f"테이블 수: {table_count}")

    # 뒤에서부터 탐색 (데이터 테이블 = 보통 마지막 대형 테이블)
    for idx in range(table_count - 1, -1, -1):
        try:
            tbl = tables.nth(idx)
            row_count = await tbl.locator("tr").count()
            if row_count < 5:
                continue

            html = await tbl.inner_html()
            dfs = pd.read_html(StringIO(f"<table>{html}</table>"))
            if dfs and len(dfs[0]) >= 5:
                df = dfs[0]
                logger.info(f"데이터: table[{idx}] → {df.shape[0]}행 × {df.shape[1]}열")
                return df
        except Exception:
            continue

    # 전체 HTML 폴백
    try:
        content = await page.content()
        dfs = pd.read_html(StringIO(content))
        if dfs:
            best = max(dfs, key=lambda x: len(x))
            if len(best) >= 5:
                logger.info(f"전체 HTML에서 추출: {len(best)}행")
                return best
    except Exception:
        pass

    return pd.DataFrame()


# ── 데이터 정리 ──

def _reshape_nationality(df: pd.DataFrame) -> pd.DataFrame:
    """3쌍 (국가명, 거래규모) 테이블 → 단일 [국가명, 거래규모] DataFrame

    KRX 원본:
      국가명 | 거래규모 | 국가명.1 | 거래규모.1 | 국가명.2 | 거래규모.2
      영국   | 133M     | 케이맨    | 12M       | 미국     | 10M
    → 변환:
      국가명  | 거래규모
      영국    | 133M
      미국    | 10M
      ...
    """
    cols = list(df.columns)
    records = []

    if len(cols) >= 6:
        for _, row in df.iterrows():
            for i in range(0, min(len(cols), 6), 2):
                country = row.iloc[i]
                volume = row.iloc[i + 1]
                if pd.notna(country) and str(country).strip():
                    try:
                        vol = int(float(volume)) if pd.notna(volume) else 0
                    except (ValueError, TypeError):
                        vol = 0
                    records.append({"국가명": str(country).strip(), "거래규모": vol})

    if records:
        result = (
            pd.DataFrame(records)
            .sort_values("거래규모", ascending=False)
            .reset_index(drop=True)
        )
        return result

    # 이미 정리된 형태이면 그대로 반환
    return df


# ═══════════════════════════════════════════
# 공개 API
# ═══════════════════════════════════════════

def fetch_nationality(
    stock_code: str,
    date_from: str = None,
    date_to: str = None,
    headless: bool = True,
) -> pd.DataFrame:
    """외국인 국적별 매매 데이터 조회

    Args:
        stock_code: 6자리 종목코드 (예: "000660")
        date_from: 시작일 YYYYMMDD (기본: 5일 전)
        date_to: 종료일 YYYYMMDD (기본: 오늘)
        headless: 헤드리스 모드

    Returns:
        DataFrame [국가명, 거래규모] — 거래규모 내림차순
    """
    if not date_to:
        date_to = datetime.now().strftime("%Y%m%d")
    if not date_from:
        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    return asyncio.run(_crawl_nationality(stock_code, date_from, date_to, headless))


def fetch_nationality_multi(
    stock_codes: list,
    date_from: str = None,
    date_to: str = None,
    headless: bool = True,
) -> dict:
    """여러 종목 순차 조회 → {코드: DataFrame}"""
    results = {}
    for code in stock_codes:
        try:
            results[code] = fetch_nationality(code, date_from, date_to, headless)
        except Exception as e:
            logger.error(f"{code} 실패: {e}")
            results[code] = pd.DataFrame()
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
        asyncio.run(manual_login())
        sys.exit(0)

    code = sys.argv[1] if len(sys.argv) > 1 else "000660"
    date_from = sys.argv[2] if len(sys.argv) > 2 else None
    date_to = sys.argv[3] if len(sys.argv) > 3 else None

    print(f"\nKRX SMILE 외국인 국적별 크롤러")
    print(f"종목: {code} | 기간: {date_from or '5일전'} ~ {date_to or '오늘'}")
    print(f"{'=' * 50}\n")

    df = fetch_nationality(code, date_from, date_to, headless=False)

    if not df.empty:
        print(f"\n{'=' * 50}")
        print(f"[OK] {len(df)}개국 데이터")
        print(f"{'=' * 50}")
        print(df.to_string())

        # 주요국 하이라이트
        print(f"\n--- 주요국 ---")
        for country in ["중국", "홍콩", "영국", "미국", "일본", "싱가포르"]:
            row = df[df["국가명"] == country]
            if not row.empty:
                vol = row.iloc[0]["거래규모"]
                print(f"  ▸ {country}: {vol:,}")
    else:
        print(f"\n[FAIL] 데이터 없음")
        print(f"  → {DATA_DIR} 스크린샷 확인")
        print(f"  → 쿠키 만료 시: python {__file__} --login")
