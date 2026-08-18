# -*- coding: utf-8 -*-
import io
import logging
import os
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from run_bot import _harden_third_party_logging, _print_startup_safety_status  # noqa: E402


class SecretLoggingGuardTest(unittest.TestCase):
    def test_http_clients_cannot_log_credential_bearing_urls_at_info(self):
        logging.getLogger("httpx").setLevel(logging.INFO)
        logging.getLogger("httpcore").setLevel(logging.INFO)

        _harden_third_party_logging()

        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertEqual(logging.getLogger("httpcore").level, logging.WARNING)

    def test_startup_status_never_emits_account_or_chat_identifiers(self):
        account = "TEST-ACCOUNT-DO-NOT-LOG"
        chat = "TEST-CHAT-DO-NOT-LOG"
        output = io.StringIO()

        with (
            patch.dict(os.environ, {"KIS_ACC_NO": account, "TELEGRAM_CHAT_ID": chat}),
            patch("bot.trade_runtime_config.is_paper_only", return_value=True),
            redirect_stdout(output),
        ):
            _print_startup_safety_status()

        rendered = output.getvalue()
        self.assertNotIn(account, rendered)
        self.assertNotIn(chat, rendered)
        self.assertIn("KIS 계좌: 설정됨", rendered)
        self.assertIn("Telegram Chat: 설정됨", rendered)
        self.assertIn("자동주문: PAPER_ONLY (실주문 차단)", rendered)
        self.assertIn(
            "수동주문: 사용자 명령 시 실주문 가능 (자동가드와 별도)",
            rendered,
        )

    def test_import_does_not_load_dotenv(self):
        source = (ROOT / "run_bot.py").read_text(encoding="utf-8")
        import_pos = source.index("from dotenv import load_dotenv")
        loader_pos = source.index("def _load_runtime_environment")

        self.assertGreater(import_pos, loader_pos)
        self.assertNotIn("KIS API 실매매 준비 완료", source)
        self.assertNotIn("동적 목표가 + KIS 실매매", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
