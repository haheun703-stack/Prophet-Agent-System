"""
Kiwoom Open API Core - QAxWidget COM 래퍼
==========================================
키움증권 Open API의 COM 객체를 감싸고, 이벤트를 시그널로 변환.
모든 API 호출의 기반이 되는 핵심 모듈.
"""

import logging
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer, pyqtSignal
from PyQt5.QtWidgets import QApplication

from api.kiwoom_constants import ERR_NONE

logger = logging.getLogger('Scalper.Core')


class KiwoomCore(QAxWidget):
    """키움 Open API COM 객체 래퍼"""

    sig_connected = pyqtSignal(int)
    sig_tr_data = pyqtSignal(str, str, str, str, str, int, str, str, str)
    sig_real_data = pyqtSignal(str, str, str)
    sig_chejan = pyqtSignal(str, int, str)
    sig_msg = pyqtSignal(str, str, str, str)

    def __init__(self):
        super().__init__()
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self._screen_counter = 1000
        self._event_loop = QEventLoop()
        self._tr_event_loop = QEventLoop()
        self._login_result = -1
        self._tr_data = {}

        # COM 이벤트 → 내부 핸들러 연결
        self.OnEventConnect.connect(self._on_event_connect)
        self.OnReceiveTrData.connect(self._on_receive_tr_data)
        self.OnReceiveRealData.connect(self._on_receive_real_data)
        self.OnReceiveChejanData.connect(self._on_receive_chejan)
        self.OnReceiveMsg.connect(self._on_receive_msg)

        logger.info("KiwoomCore 초기화 완료")

    # === 로그인 ===

    def login(self) -> int:
        """키움 로그인 (동기 대기)"""
        logger.info("키움 로그인 시도...")
        self.dynamicCall("CommConnect()")
        self._event_loop.exec_()
        if self._login_result == ERR_NONE:
            logger.info("키움 로그인 성공")
        else:
            logger.error(f"키움 로그인 실패: {self._login_result}")
        return self._login_result

    def is_connected(self) -> bool:
        ret = self.dynamicCall("GetConnectState()")
        return ret == 1

    def get_login_info(self, tag: str) -> str:
        """로그인 정보 조회 (ACCNO, USER_ID, USER_NAME 등)"""
        return self.dynamicCall("GetLoginInfo(QString)", tag).strip()

    def get_account_list(self) -> list:
        """계좌 목록"""
        raw = self.get_login_info("ACCNO")
        return [acc for acc in raw.split(";") if acc]

    # === 스크린 번호 ===

    def get_next_screen(self) -> str:
        self._screen_counter += 1
        if self._screen_counter > 9999:
            self._screen_counter = 1000
        return str(self._screen_counter)

    # === TR 요청 (저수준) ===

    def set_input_value(self, fid: str, value: str):
        self.dynamicCall("SetInputValue(QString, QString)", fid, value)

    def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str) -> int:
        return self.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rq_name, tr_code, prev_next, screen_no
        )

    def get_comm_data(self, tr_code: str, record: str, index: int, field: str) -> str:
        return self.dynamicCall(
            "GetCommData(QString, QString, int, QString)",
            tr_code, record, index, field
        ).strip()

    def get_repeat_cnt(self, tr_code: str, record: str) -> int:
        return self.dynamicCall(
            "GetRepeatCnt(QString, QString)", tr_code, record
        )

    def request_tr(self, rq_name: str, tr_code: str, prev_next: int = 0,
                   screen_no: str = None, timeout_ms: int = 5000) -> dict:
        """TR 요청 후 응답 대기 (동기)"""
        if screen_no is None:
            screen_no = self.get_next_screen()

        self._tr_data = {}
        ret = self.comm_rq_data(rq_name, tr_code, prev_next, screen_no)

        if ret != ERR_NONE:
            logger.error(f"TR 요청 실패: {rq_name} ({tr_code}), 코드={ret}")
            return {}

        QTimer.singleShot(timeout_ms, self._tr_event_loop.quit)
        self._tr_event_loop.exec_()

        return self._tr_data

    # === 실시간 ===

    def set_real_reg(self, screen_no: str, codes: str, fids: str, opt_type: str) -> int:
        """
        실시간 데이터 등록
        opt_type: "0" = 기존 등록 해제 후 등록, "1" = 추가 등록
        """
        return self.dynamicCall(
            "SetRealReg(QString, QString, QString, QString)",
            screen_no, codes, fids, opt_type
        )

    def set_real_remove(self, screen_no: str, code: str):
        self.dynamicCall("SetRealRemove(QString, QString)", screen_no, code)

    def get_comm_real_data(self, code: str, fid: int) -> str:
        return self.dynamicCall(
            "GetCommRealData(QString, int)", code, fid
        ).strip()

    # === 주문 ===

    def send_order(self, rq_name: str, screen_no: str, account: str,
                   order_type: int, code: str, qty: int, price: int,
                   hoga_type: str, org_order_no: str = "") -> int:
        return self.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            rq_name, screen_no, account, order_type, code, qty, price,
            hoga_type, org_order_no
        )

    # === 유틸리티 ===

    def get_code_list(self, market: str = "0") -> list:
        """종목코드 목록 (0=코스피, 10=코스닥)"""
        raw = self.dynamicCall("GetCodeListByMarket(QString)", market)
        return [c for c in raw.split(";") if c]

    def get_code_name(self, code: str) -> str:
        return self.dynamicCall("GetMasterCodeName(QString)", code).strip()

    # === 이벤트 핸들러 ===

    def _on_event_connect(self, err_code: int):
        self._login_result = err_code
        self._event_loop.quit()
        self.sig_connected.emit(err_code)

    def _on_receive_tr_data(self, screen_no, rq_name, tr_code, record,
                            prev_next, data_len, err_code, msg, sp_msg):
        self._tr_data = {
            'screen_no': screen_no,
            'rq_name': rq_name,
            'tr_code': tr_code,
            'record': record,
            'prev_next': prev_next,
        }
        self._tr_event_loop.quit()
        self.sig_tr_data.emit(
            screen_no, rq_name, tr_code, record,
            prev_next, data_len, err_code, msg, sp_msg
        )

    def _on_receive_real_data(self, code: str, real_type: str, data: str):
        self.sig_real_data.emit(code, real_type, data)

    def _on_receive_chejan(self, gubun: str, item_cnt: int, fid_list: str):
        self.sig_chejan.emit(gubun, item_cnt, fid_list)

    def _on_receive_msg(self, screen_no, rq_name, tr_code, msg):
        logger.debug(f"[MSG] {rq_name}: {msg}")
        self.sig_msg.emit(screen_no, rq_name, tr_code, msg)
