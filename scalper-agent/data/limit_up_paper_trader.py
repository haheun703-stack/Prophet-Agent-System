# -*- coding: utf-8 -*-
"""하위호환 stub — engine/limit_up_paper_trader.py 로 이동됨."""
from engine.limit_up_paper_trader import (  # noqa: F401
    run_paper_trading,
    get_paper_status,
)

__all__ = ["run_paper_trading", "get_paper_status"]

if __name__ == "__main__":
    # python -m data.limit_up_paper_trader 하위호환
    import sys
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    if args.status:
        print(get_paper_status())
    else:
        run_paper_trading(send_telegram=not args.no_telegram)
