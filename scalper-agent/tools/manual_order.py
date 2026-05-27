# -*- coding: utf-8 -*-
"""
수동 주문 CLI — Claude가 장중에 직접 매매 실행
================================================
사용법:
    # 현재가 조회
    python tools/manual_order.py --price 풍산

    # 매수 (금액 기준, 시장가)
    python tools/manual_order.py --buy 풍산 --amount 500000

    # 매수 (수량 기준)
    python tools/manual_order.py --buy 103140 --qty 10

    # 매수 확정 (--confirm 없으면 드라이런)
    python tools/manual_order.py --buy 풍산 --amount 500000 --confirm

    # 매도 (수량)
    python tools/manual_order.py --sell 풍산 --qty 5 --confirm

    # 매도 (전량)
    python tools/manual_order.py --sell 풍산 --all --confirm

    # 잔고 조회
    python tools/manual_order.py --balance
"""

import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

from bot.kis_trader import KISTrader, resolve_stock


def cmd_price(args):
    """현재가 조회"""
    code, name = resolve_stock(args.stock)
    if not code:
        print(f"[ERROR] 종목 '{args.stock}' 찾을 수 없음")
        return 1

    trader = KISTrader()
    p = trader.fetch_price(code)
    if not p.get("success"):
        print(f"[ERROR] 현재가 조회 실패: {p.get('message')}")
        return 1

    print(f"{'='*40}")
    print(f"  {name} ({code})")
    print(f"{'='*40}")
    print(f"  현재가:  {p['current_price']:>10,}원")
    print(f"  등락률:  {p['change_rate']:>+9.2f}%")
    print(f"  고가:    {p['high']:>10,}원")
    print(f"  저가:    {p['low']:>10,}원")
    print(f"  거래량:  {p['volume']:>10,}주")
    return 0


def cmd_buy(args):
    """매수 주문"""
    code, name = resolve_stock(args.stock)
    if not code:
        print(f"[ERROR] 종목 '{args.stock}' 찾을 수 없음")
        return 1

    trader = KISTrader()

    # 현재가 조회
    p = trader.fetch_price(code)
    if not p.get("success"):
        print(f"[ERROR] 현재가 조회 실패: {p.get('message')}")
        return 1

    price = p["current_price"]

    # 수량 계산
    if args.qty:
        qty = args.qty
    elif args.amount:
        if price <= 0:
            print(f"[ERROR] 현재가 0원 — 수량 계산 불가")
            return 1
        qty = args.amount // price
        if qty <= 0:
            print(f"[ERROR] 금액 {args.amount:,}원으로 1주도 못 삼 (현재가 {price:,}원)")
            return 1
    else:
        print("[ERROR] --qty 또는 --amount 필요")
        return 1

    total_cost = qty * price

    print(f"{'='*40}")
    print(f"  📈 매수 주문 {'[실행]' if args.confirm else '[드라이런]'}")
    print(f"{'='*40}")
    print(f"  종목:    {name} ({code})")
    print(f"  현재가:  {price:>10,}원 ({p['change_rate']:+.2f}%)")
    print(f"  수량:    {qty:>10,}주")
    print(f"  예상금액: {total_cost:>10,}원")
    if args.amount:
        print(f"  (요청금액 {args.amount:,}원 → {qty}주 × {price:,}원)")

    if not args.confirm:
        print(f"\n  ⚠️ 드라이런 — 실제 주문 안 됨")
        print(f"  실행하려면 --confirm 추가")
        return 0

    # 실제 주문 (분할 없이 원샷)
    print(f"\n  주문 전송 중...")
    result = trader.buy_market(code, qty, split=1, manual=True)
    if result.get("success"):
        print(f"  ✅ {result['message']}")
        return 0
    else:
        print(f"  ❌ {result['message']}")
        return 1


def cmd_sell(args):
    """매도 주문"""
    code, name = resolve_stock(args.stock)
    if not code:
        print(f"[ERROR] 종목 '{args.stock}' 찾을 수 없음")
        return 1

    trader = KISTrader()

    # 전량 매도: 잔고에서 수량 조회
    if args.all:
        bal = trader.fetch_balance()
        if not bal.get("success"):
            print(f"[ERROR] 잔고 조회 실패: {bal.get('message')}")
            return 1
        pos = next((p for p in bal["positions"] if p["code"] == code), None)
        if not pos:
            print(f"[ERROR] {name}({code}) 보유 없음")
            return 1
        qty = pos["qty"]
        print(f"  전량 매도: {qty}주 보유 확인")
    elif args.qty:
        qty = args.qty
    else:
        print("[ERROR] --qty 또는 --all 필요")
        return 1

    # 현재가 조회
    p = trader.fetch_price(code)
    if not p.get("success"):
        print(f"[ERROR] 현재가 조회 실패: {p.get('message')}")
        return 1

    price = p["current_price"]
    total_est = qty * price

    print(f"{'='*40}")
    print(f"  📉 매도 주문 {'[실행]' if args.confirm else '[드라이런]'}")
    print(f"{'='*40}")
    print(f"  종목:    {name} ({code})")
    print(f"  현재가:  {price:>10,}원 ({p['change_rate']:+.2f}%)")
    print(f"  수량:    {qty:>10,}주")
    print(f"  예상금액: {total_est:>10,}원")

    if not args.confirm:
        print(f"\n  ⚠️ 드라이런 — 실제 주문 안 됨")
        print(f"  실행하려면 --confirm 추가")
        return 0

    # 실제 주문
    print(f"\n  주문 전송 중...")
    result = trader.sell_market(code, qty, split=1, manual=True)
    if result.get("success"):
        print(f"  ✅ {result['message']}")
        return 0
    else:
        print(f"  ❌ {result['message']}")
        return 1


def cmd_balance(args):
    """잔고 조회"""
    trader = KISTrader()
    bal = trader.fetch_balance()
    if not bal.get("success"):
        print(f"[ERROR] 잔고 조회 실패: {bal.get('message')}")
        return 1

    print(f"{'='*40}")
    print(f"  💰 계좌 잔고")
    print(f"{'='*40}")
    print(f"  현금:      {bal['cash']:>12,}원")
    print(f"  총 평가액: {bal['total_eval']:>12,}원")
    print(f"  보유종목:  {len(bal['positions'])}종목")

    if bal["positions"]:
        print(f"\n  {'종목명':<12s} {'수량':>6s} {'평균가':>10s} {'현재가':>10s} {'손익':>10s} {'수익률':>7s}")
        print(f"  {'-'*60}")
        for p in bal["positions"]:
            print(f"  {p['name']:<12s} {p['qty']:>6d} {p['avg_price']:>10,} {p['current_price']:>10,} {p['pnl_amount']:>+10,} {p['pnl_rate']:>+6.1f}%")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Body Hunter 수동 주문 CLI")
    sub = parser.add_subparsers(dest="command")

    # -- price --
    p_price = sub.add_parser("price", help="현재가 조회")
    p_price.add_argument("stock", help="종목명 또는 코드")

    # -- buy --
    p_buy = sub.add_parser("buy", help="시장가 매수")
    p_buy.add_argument("stock", help="종목명 또는 코드")
    p_buy.add_argument("--amount", type=int, help="매수 금액 (원)")
    p_buy.add_argument("--qty", type=int, help="매수 수량 (주)")
    p_buy.add_argument("--confirm", action="store_true", help="실제 주문 실행")

    # -- sell --
    p_sell = sub.add_parser("sell", help="시장가 매도")
    p_sell.add_argument("stock", help="종목명 또는 코드")
    p_sell.add_argument("--qty", type=int, help="매도 수량 (주)")
    p_sell.add_argument("--all", action="store_true", help="전량 매도")
    p_sell.add_argument("--confirm", action="store_true", help="실제 주문 실행")

    # -- balance --
    sub.add_parser("balance", help="계좌 잔고 조회")

    # 단축 인터페이스: --price, --buy, --sell, --balance 플래그
    parser.add_argument("--price", metavar="STOCK", help="현재가 조회 (단축)")
    parser.add_argument("--buy", metavar="STOCK", help="매수 (단축)")
    parser.add_argument("--sell", metavar="STOCK", help="매도 (단축)")
    parser.add_argument("--amount", type=int, help="매수 금액")
    parser.add_argument("--qty", type=int, help="수량")
    parser.add_argument("--all", action="store_true", help="전량 매도")
    parser.add_argument("--confirm", action="store_true", help="실제 주문 실행")
    parser.add_argument("--balance", action="store_true", help="잔고 조회 (단축)")

    args = parser.parse_args()

    # 단축 플래그 처리
    if args.price:
        args.stock = args.price
        return cmd_price(args)
    elif args.buy:
        args.stock = args.buy
        return cmd_buy(args)
    elif args.sell:
        args.stock = args.sell
        return cmd_sell(args)
    elif args.balance:
        return cmd_balance(args)
    elif args.command == "price":
        return cmd_price(args)
    elif args.command == "buy":
        return cmd_buy(args)
    elif args.command == "sell":
        return cmd_sell(args)
    elif args.command == "balance":
        return cmd_balance(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
