"""14:30 수급 체크 + 텔레그램 발송 스크립트
감시풀 중 continuation_score >= 40 종목의 실시간 수급/가격 조회 후 텔레그램 전송
"""
import sys, os, io, json, time, requests
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"))

# === KIS API 설정 ===
APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL = "https://openapi.koreainvestment.com:9443"
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")

def get_token():
    res = requests.post(f"{BASE_URL}/oauth2/tokenP",
                        json={"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET})
    return res.json().get("access_token")

def kis_headers(token, tr_id):
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY, "appsecret": APP_SECRET,
        "tr_id": tr_id, "custtype": "P",
    }

def get_price(token, code):
    """현재가 + 등락 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    h = kis_headers(token, "FHKST01010100")
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
    r = requests.get(url, headers=h, params=params)
    d = r.json()
    if d.get("rt_cd") == "0":
        o = d["output"]
        return {
            "price": int(o.get("stck_prpr", 0)),
            "chg": o.get("prdy_vrss", "0"),
            "rate": o.get("prdy_ctrt", "0"),
            "vol": int(o.get("acml_vol", 0)),
            "tr_amt": int(o.get("acml_tr_pbmn", 0)),
            "frgn_rate": o.get("hts_frgn_ehrt", "0"),
        }
    return None

def get_investor(token, code):
    """투자자별 매매동향 (일별 데이터 - 가장 최근 확정일 기준)"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor"
    h = kis_headers(token, "FHKST01010900")
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    r = requests.get(url, headers=h, params=params)
    d = r.json()
    result = {"date": "", "개인": 0, "외국인": 0, "기관": 0}
    if d.get("rt_cd") == "0":
        for item in d.get("output", []):
            frgn = item.get("frgn_ntby_qty", "")
            if frgn:  # 빈 문자열이 아닌 첫 번째 행 = 가장 최근 확정일
                result["date"] = item.get("stck_bsop_date", "")
                result["개인"] = int(item.get("prsn_ntby_qty", 0))
                result["외국인"] = int(frgn)
                result["기관"] = int(item.get("orgn_ntby_qty", 0))
                break
    return result

def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT:
        print("[TG] 미설정")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    # 4096자 제한 분할
    chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
    for chunk in chunks:
        requests.post(url, json={"chat_id": TG_CHAT, "text": chunk})
        time.sleep(0.3)

def main():
    # 14:30 대기
    now = datetime.now()
    target = now.replace(hour=14, minute=30, second=0, microsecond=0)
    if now < target:
        wait = (target - now).total_seconds()
        print(f"[{now.strftime('%H:%M:%S')}] 14:30까지 {wait:.0f}초 대기...")
        time.sleep(wait)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 수급 체크 시작")

    # watchlist 로드
    wl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "data_store", "limit_up", "watchlist.json")
    with open(wl_path, "r", encoding="utf-8") as f:
        wdata = json.load(f)
    items = wdata.get("items", [])

    # score >= 40 종목 + 구데이터(score=0) 중 triggered
    targets = [i for i in items if i.get("continuation_score", 0) >= 40]
    targets.sort(key=lambda x: -x.get("continuation_score", 0))

    if not targets:
        print("score >= 40 종목 없음")
        return

    token = get_token()
    if not token:
        print("토큰 발급 실패")
        return

    # 지수 먼저
    lines = []
    lines.append(f"[상한가엔진] 14:30 수급 리포트")
    lines.append(f"={('=' * 28)}")

    for idx_code, idx_name in [("0001", "KOSPI"), ("1001", "KOSDAQ")]:
        url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-index-price"
        h = kis_headers(token, "FHPUP02100000")
        params = {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": idx_code}
        r = requests.get(url, headers=h, params=params)
        d = r.json()
        if d.get("rt_cd") == "0":
            o = d.get("output", {})
            price = o.get("bstp_nmix_prpr", "?")
            chg = o.get("bstp_nmix_prdy_vrss", "0")
            rate = o.get("bstp_nmix_prdy_ctrt", "0")
            sign = "+" if not str(chg).startswith("-") and chg != "0" else ""
            lines.append(f"{idx_name}: {price} ({sign}{rate}%)")
        time.sleep(0.2)

    lines.append("")
    lines.append(f"[score>=40 감시종목 {len(targets)}건]")
    lines.append("-" * 30)

    for item in targets:
        code = item.get("code", "")
        name = item.get("name", "?")
        score = item.get("continuation_score", 0)
        fg = item.get("flow_grade", "?")

        pdata = get_price(token, code)
        time.sleep(0.2)
        inv = get_investor(token, code)
        time.sleep(0.2)

        if pdata:
            p = pdata["price"]
            chg = pdata["chg"]
            rate = pdata["rate"]
            sign = "+" if not str(chg).startswith("-") and str(chg) != "0" else ""
            tr_억 = pdata["tr_amt"] / 100_000_000

            # 시그널 종가 대비 등락
            sig_close = item.get("signal_close", 0)
            if sig_close > 0:
                vs_sig = (p - sig_close) / sig_close * 100
                vs_str = f" (시그널比{vs_sig:+.1f}%)"
            else:
                vs_str = ""

            frgn = inv["외국인"]
            inst = inv["기관"]
            indiv = inv["개인"]
            inv_date = inv["date"]

            frgn_sign = "+" if frgn > 0 else ""
            inst_sign = "+" if inst > 0 else ""

            # 날짜 라벨: "20260509" → "5/9"
            if inv_date:
                inv_label = f"전일({inv_date[4:6].lstrip('0')}/{inv_date[6:].lstrip('0')})"
            else:
                inv_label = "수급미확인"

            lines.append(f"")
            lines.append(f"{name}({code}) score:{score:.0f} 수급:{fg}")
            lines.append(f"  {p:,}원 {sign}{chg}({sign}{rate}%) 대금:{tr_억:,.0f}억{vs_str}")
            if inv_date:
                lines.append(f"  {inv_label} 외인:{frgn_sign}{frgn:,} 기관:{inst_sign}{inst:,} 개인:{'+' if indiv > 0 else ''}{indiv:,}")
            else:
                lines.append(f"  수급: 데이터 미확정")
        else:
            lines.append(f"{name}({code}) score:{score:.0f} - 조회실패")

    lines.append("")
    lines.append("=" * 30)
    lines.append(f"발송: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    msg = "\n".join(lines)
    print(msg)
    print("\n[텔레그램 발송 중...]")
    send_telegram(msg)
    print("[완료]")

if __name__ == "__main__":
    main()
