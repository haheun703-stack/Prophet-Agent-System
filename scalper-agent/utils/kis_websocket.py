# -*- coding: utf-8 -*-
"""KIS WebSocket 실시간 시세 모듈 — Phase 3.

자동매매 진입 평가의 게임체인저:
- 기존 REST polling: 30초마다 KIS API 호출 → 지연 30s + 호출 부담
- WebSocket 구독: 체결 즉시 push (밀리초) + 호출 0 + 동시 다종목

Why: 자동매매 진입 6조건 평가 시 30초 지연은 갭상승/갭하락 놓침.
WebSocket으로 체결 이벤트 받으면 즉시 평가 → 진입 정확도 ↑↑.

TR_ID:
- H0STCNT0: 주식 체결가 (정규장)
- H0STASP0: 주식 호가 (정규장)
- H0STMOM0: 지수 체결가

응답 형식 (체결가 H0STCNT0):
- "0|H0STCNT0|001|코드^체결시각^현재가^전일대비부호^전일대비^등락률^..."
  필드[0]=종목코드, [1]=시각, [2]=현재가, [3]=대비부호, [4]=대비, [5]=등락률,
  [12]=누적거래량, [13]=누적거래대금

자동 재접속: 5초 대기 후 재연결, 구독 자동 복구.

Usage:
    from utils.kis_websocket import KISWebSocketClient

    async def on_tick(code, price, volume, ts, etc):
        print(f"{code} 체결 {price:,}원 거래량 {volume:,}")

    client = KISWebSocketClient()
    await client.subscribe(['005930', '000660'], on_tick)
    await client.run_forever()  # 영구 루프
"""
import asyncio
import json
import logging
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

try:
    import websockets
except ImportError:
    websockets = None

from utils.kis_nxt_kit import get_approval_key

logger = logging.getLogger("BH.KisWebSocket")

KIS_WS_URI = "ws://ops.koreainvestment.com:21000"

# 콜백 타입: (code, price, volume, time_hhmmss, raw_fields) -> Coroutine
TickCallback = Callable[[str, int, int, str, list], Awaitable[None]]


class KISWebSocketClient:
    """KIS 실시간 시세 WebSocket 클라이언트.

    Args:
        max_subscriptions: 동시 구독 한도 (KIS 기본 41건/세션)
        reconnect_delay: 끊김 후 재접속 대기 (초)
    """

    def __init__(self, max_subscriptions: int = 40, reconnect_delay: float = 5.0):
        if websockets is None:
            raise RuntimeError("websockets 패키지 필요: pip install websockets")
        self.max_subs = max_subscriptions
        self.reconnect_delay = reconnect_delay
        self._ws = None
        self._subs: Dict[Tuple[str, str], TickCallback] = {}  # (code, tr_id) -> callback
        self._running = False
        self._approval_key: Optional[str] = None
        # M12 fix: send 동시 호출 race condition 방지 (websockets 1 connection 동시 send hang)
        self._send_lock = asyncio.Lock()

    async def connect(self):
        """WebSocket 연결 + approval_key 발급 (M3 fix: 실패 시 force_refresh 재시도)."""
        self._approval_key = get_approval_key()
        if not self._approval_key:
            raise RuntimeError("approval_key 발급 실패")
        try:
            self._ws = await websockets.connect(
                KIS_WS_URI,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
            )
            logger.info(f"[WS] 연결 성공: {KIS_WS_URI}")
        except Exception as e:
            # 자정 넘긴 stale key 가능성 → 강제 재발급 후 재시도 1회
            logger.warning(f"[WS] 연결 실패 ({e}) — approval_key 강제 갱신 후 재시도")
            self._approval_key = get_approval_key(force_refresh=True)
            if not self._approval_key:
                raise
            self._ws = await websockets.connect(
                KIS_WS_URI,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
            )
            logger.info(f"[WS] 연결 성공 (재시도): {KIS_WS_URI}")

    async def _send_request(self, code: str, tr_id: str, action: str = "1"):
        """KIS 구독/해제 요청.

        action: '1' = 구독, '2' = 해제
        """
        if not self._ws:
            raise RuntimeError("WebSocket 미연결 — connect() 먼저 호출")
        req = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": "P",
                "tr_type": action,
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": code,
                }
            },
        }
        # M12 fix: 동시 send 직렬화
        async with self._send_lock:
            await self._ws.send(json.dumps(req))

    async def subscribe(self, codes: List[str], on_tick: TickCallback,
                        tr_id: str = "H0STCNT0"):
        """종목 리스트 구독.

        Args:
            codes: 6자리 종목코드 리스트
            on_tick: 체결 콜백 (code, price, volume, time, raw_fields)
            tr_id: H0STCNT0(체결가) / H0STASP0(호가) / H0STMOM0(지수)
        """
        if not self._ws:
            await self.connect()

        if len(self._subs) + len(codes) > self.max_subs:
            raise ValueError(
                f"구독 한도 초과: 현재 {len(self._subs)} + 신규 {len(codes)} "
                f"> 한도 {self.max_subs}"
            )

        for code in codes:
            key = (code, tr_id)
            if key in self._subs:
                logger.debug(f"[WS] 이미 구독 중: {code} {tr_id}")
                continue
            await self._send_request(code, tr_id, action="1")
            self._subs[key] = on_tick
            await asyncio.sleep(0.05)  # KIS rate limit (50ms)
        logger.info(f"[WS] 구독 완료: {len(codes)}종목 {tr_id} "
                    f"(전체 {len(self._subs)})")

    async def unsubscribe(self, codes: List[str], tr_id: str = "H0STCNT0"):
        """종목 구독 해제."""
        if not self._ws:
            return
        for code in codes:
            key = (code, tr_id)
            if key not in self._subs:
                continue
            await self._send_request(code, tr_id, action="2")
            self._subs.pop(key, None)
            await asyncio.sleep(0.05)
        logger.info(f"[WS] 구독 해제: {len(codes)}종목 {tr_id}")

    async def _handle_message(self, data: str):
        """수신 메시지 라우팅."""
        # H3 fix: 빈 메시지 / ping-pong 가드
        if not data:
            return
        # KIS 응답 형식:
        # - 시스템 메시지: JSON ({"header":...,"body":...})
        # - 체결 데이터: "0|TR_ID|건수|payload" 또는 "1|TR_ID|건수|payload(암호화)"
        if data.startswith("{"):
            # JSON 시스템 메시지 (구독 응답, ping 등)
            try:
                msg = json.loads(data)
                tr_id = msg.get("header", {}).get("tr_id", "?")
                rt_cd = msg.get("body", {}).get("rt_cd", "?")
                if rt_cd not in ("0", "?"):
                    logger.warning(f"[WS] 시스템 응답: {tr_id} rt_cd={rt_cd} "
                                   f"msg={msg.get('body', {}).get('msg1', '')}")
            except Exception:
                pass
            return

        if data[0] not in ("0", "1"):
            return

        # 체결 데이터 파싱 (KIS WebSocket 응답: "0|TR_ID|건수|payload")
        # CSV가 아닌 KIS WS 전용 포맷이므로 tuple unpacking으로 안전 파싱
        try:
            _flag, tr_id, _count, payload = data.split("|", 3)
        except ValueError:
            return

        # payload: code^time^price^...^volume^... (^로 구분, KIS 표준)
        fields = payload.split("^")
        if len(fields) < 3:
            return

        # 인덱스 의미는 KIS TR_ID별 명세 참조 — H0STCNT0 기준 헬퍼:
        def _f(idx: int, default: str = "") -> str:
            return fields[idx] if idx < len(fields) else default

        def _fi(idx: int) -> int:
            v = _f(idx)
            try:
                return int(v) if v else 0
            except ValueError:
                return 0

        code = _f(0)
        callback = self._subs.get((code, tr_id))
        if not callback:
            return

        try:
            price = _fi(2)
            time_hhmmss = _f(1)
            volume = _fi(12)
            await callback(code, price, volume, time_hhmmss, fields)
        except Exception as e:
            logger.warning(f"[WS] 콜백 에러 {code}: {e}")

    async def run_forever(self):
        """수신 루프 (자동 재접속)."""
        self._running = True
        while self._running:
            try:
                if not self._ws:
                    await self.connect()
                    # 재접속 시 기존 구독 복구
                    if self._subs:
                        subs_copy = dict(self._subs)
                        self._subs.clear()
                        for (code, tr_id), cb in subs_copy.items():
                            await self._send_request(code, tr_id, "1")
                            self._subs[(code, tr_id)] = cb
                            await asyncio.sleep(0.05)
                        logger.info(f"[WS] 재접속 후 {len(self._subs)} 구독 복구")

                data = await self._ws.recv()
                await self._handle_message(data)

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[WS] 연결 끊김: {e} — {self.reconnect_delay}초 후 재접속")
                self._ws = None
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                logger.error(f"[WS] 루프 에러: {type(e).__name__}: {e}")
                self._ws = None
                await asyncio.sleep(self.reconnect_delay)

    async def stop(self):
        """루프 종료 + 연결 닫기."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        logger.info("[WS] 종료")


# ─────────────────────────────────────────────
# CLI 검증
# ─────────────────────────────────────────────
async def _cli_test():
    """삼성전자 + SK하이닉스 15초간 구독 검증."""
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    received = {"count": 0}

    async def on_tick(code, price, volume, ts, fields):
        received["count"] += 1
        if received["count"] <= 5:
            print(f"  [{ts}] {code}: {price:,}원 누적량 {volume:,}")

    client = KISWebSocketClient()
    await client.subscribe(["005930", "000660"], on_tick)
    print("15초간 수신 대기...")

    task = asyncio.create_task(client.run_forever())
    await asyncio.sleep(15)
    await client.stop()
    try:
        await asyncio.wait_for(task, timeout=2)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    print(f"\n총 수신 이벤트: {received['count']}건")
    if received["count"] > 0:
        print("✅ WebSocket 정상 작동")
    else:
        print("⚠️ 수신 0건 — 장 마감 시간이면 정상 (체결 없음). 정규장에 재검증 필요")


if __name__ == "__main__":
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("=== KIS WebSocket 실시간 시세 검증 ===")
    asyncio.run(_cli_test())
