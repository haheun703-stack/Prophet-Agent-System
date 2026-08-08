# -*- coding: utf-8 -*-
"""Build the read-only FLOWX paper summary consumed by the 18:00 integration."""

import argparse
import json
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from data.flowx_paper_summary import (  # noqa: E402
    FLOWX_SUMMARY_PATH,
    build_flowx_paper_summary,
    write_flowx_paper_summary,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-dir", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=FLOWX_SUMMARY_PATH)
    parser.add_argument("--as-of", default=None, help="timezone-aware ISO-8601 cutoff")
    args = parser.parse_args()

    summary = build_flowx_paper_summary(event_dir=args.event_dir, as_of=args.as_of)
    output = write_flowx_paper_summary(
        summary, args.output, source_event_dir=args.event_dir
    )
    print(json.dumps({
        "paper_only": True,
        "output": str(output),
        "strategy_market_count": summary["totals"]["strategy_market_count"],
        "closed_trade_count": summary["totals"]["closed_trade_count"],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
