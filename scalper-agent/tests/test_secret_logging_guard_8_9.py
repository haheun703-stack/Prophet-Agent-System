# -*- coding: utf-8 -*-
import logging
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from run_bot import _harden_third_party_logging  # noqa: E402


class SecretLoggingGuardTest(unittest.TestCase):
    def test_http_clients_cannot_log_credential_bearing_urls_at_info(self):
        logging.getLogger("httpx").setLevel(logging.INFO)
        logging.getLogger("httpcore").setLevel(logging.INFO)

        _harden_third_party_logging()

        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertEqual(logging.getLogger("httpcore").level, logging.WARNING)


if __name__ == "__main__":
    unittest.main(verbosity=2)
