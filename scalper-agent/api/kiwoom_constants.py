"""
Kiwoom Open API 상수 정의
=========================
TR코드, FID코드, 주문유형, 에러코드 등 모든 매직넘버를 한 곳에서 관리
"""

# === TR Codes ===
TR_OPT10001 = "opt10001"  # 주식기본정보요청
TR_OPT10003 = "opt10003"  # 체결정보요청
TR_OPT10004 = "opt10004"  # 주식호가요청
TR_OPT10080 = "opt10080"  # 주식분봉차트조회요청
TR_OPT10081 = "opt10081"  # 주식일봉차트조회요청
TR_OPW00001 = "opw00001"  # 예수금상세현황요청
TR_OPW00018 = "opw00018"  # 계좌평가잔고내역요청

# === FID Codes (실시간) ===
FID_CURRENT_PRICE = 10    # 현재가
FID_DIFF = 11             # 전일대비
FID_DIFF_RATE = 12        # 등락율
FID_VOLUME = 15           # 거래량 (체결)
FID_CUMUL_VOLUME = 13     # 누적거래량
FID_TRADE_TIME = 20       # 체결시간 (HHMMSS)
FID_OPEN = 16             # 시가
FID_HIGH = 17             # 고가
FID_LOW = 18              # 저가
FID_TRADE_AMOUNT = 14     # 누적거래대금

# 호가 FID
FID_ASK_PRICE_1 = 41      # 매도호가1
FID_ASK_PRICE_2 = 42
FID_ASK_PRICE_3 = 43
FID_ASK_PRICE_4 = 44
FID_ASK_PRICE_5 = 45
FID_BID_PRICE_1 = 51      # 매수호가1
FID_BID_PRICE_2 = 52
FID_BID_PRICE_3 = 53
FID_BID_PRICE_4 = 54
FID_BID_PRICE_5 = 55
FID_ASK_VOL_1 = 61        # 매도호가잔량1
FID_BID_VOL_1 = 71        # 매수호가잔량1
FID_TOTAL_ASK_VOL = 121   # 매도호가총잔량
FID_TOTAL_BID_VOL = 125   # 매수호가총잔량

# 체잔 FID
FID_ORDER_NO = 9203       # 주문번호
FID_ORDER_STATUS = 913     # 주문상태
FID_ORDER_QTY = 900        # 주문수량
FID_ORDER_PRICE = 901      # 주문가격
FID_FILLED_QTY = 911       # 체결량
FID_FILLED_PRICE = 910     # 체결가
FID_STOCK_CODE = 9001      # 종목코드
FID_ORDER_TYPE = 905       # 주문구분 (+매수, -매도)
FID_REMAIN_QTY = 902       # 미체결수량

# === Order Types ===
ORDER_BUY_NEW = 1          # 신규매수
ORDER_SELL_NEW = 2         # 신규매도
ORDER_BUY_CANCEL = 3       # 매수취소
ORDER_SELL_CANCEL = 4      # 매도취소
ORDER_BUY_MODIFY = 5       # 매수정정
ORDER_SELL_MODIFY = 6      # 매도정정

# === Hoga Types ===
HOGA_LIMIT = "00"          # 지정가
HOGA_MARKET = "03"         # 시장가
HOGA_CONDITIONAL = "05"    # 조건부지정가
HOGA_BEST = "06"           # 최유리지정가

# === Error Codes ===
ERR_NONE = 0
ERR_FAIL = -1
ERR_COND = -100
ERR_LOGIN = -101
ERR_CONNECT = -102

# === Market Codes ===
MARKET_KOSPI = "0"
MARKET_KOSDAQ = "10"

# === 실시간 등록 타입 ===
REAL_TYPE_TICK = "주식체결"
REAL_TYPE_ORDERBOOK = "주식호가잔량"
REAL_TYPE_CHEJAN = "주문체결"

# === 실시간 FID 목록 (문자열) ===
TICK_FIDS = "10;11;12;13;14;15;16;17;18;20"
ORDERBOOK_FIDS = "41;42;43;44;45;51;52;53;54;55;61;71;121;125"
