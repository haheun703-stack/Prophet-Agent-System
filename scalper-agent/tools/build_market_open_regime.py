# -*- coding: utf-8 -*-
"""Build MARKET_OPEN_REGIME packets for quant/scalper/info bots.

Outputs:
- data_store/market_open_raw.json
- data_store/quant_market_regime.json
- data_store/scalper_open_gate.json
- data_store/info_market_open_digest.json
"""

import argparse
import json
import sys
from pathlib import Path

SCALPER_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCALPER_DIR))

from data.market_open_regime import build_all, format_brief  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Build market open regime packets.")
    parser.add_argument("--data-dir", default=str(SCALPER_DIR / "data_store"))
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--json", action="store_true", help="Print full JSON instead of brief.")
    args = parser.parse_args()

    result = build_all(Path(args.data_dir), save=not args.no_save)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_brief(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

