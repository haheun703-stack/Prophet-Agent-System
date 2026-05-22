"""position_safety.py — 포지션 안전장치 3종 + 매도 큐 (박사 자율 v1.1)
====================================================================

GPT 베이스 + 박사 X 4개 fix + 추가 개선 4개 통합 (2026-05-21 19:30).

배경:
- 5/19 일진전기 -626,400원 사고 (사장님 보호 보호 X)
- 5/20 -293만 사고 (대한전선/SKC/제룡전기/산일전기 4일 룰 청산)
- 5/21 로킷헬스케어 -15.4% 손실 진행 (positions.json 누락 = SL 작동 X)

3개 함수:
  1. sync_positions() — KIS ↔ 메모리 동기화 (5분 사이클)
  2. enforce_sl()     — SL=None 포지션 디폴트 SL 강제
  3. hard_kill_check() — 메모리 무관 최후 방어선 (1분 사이클)

박사 fix (GPT 베이스 대비):
  - self.kis → self.trader (auto_trader.py 멤버명)
  - fetch_balance() 리턴 dict (kis_balance["positions"])
  - 필드명 avg_price/code/name/qty (KIS raw 아님)
  - fetch_price() 리턴 dict {success, current_price}
  - trade_journal.log_sell 직접 호출 (_record_trade 없음)
  - APScheduler job_queue 사용 (schedule 라이브러리 X)

박사 추가 개선:
  - 사장님 보호 종목 (sl_disabled/source=verification/manual_sync) 강제 제외
  - buy_price=0 케이스 처리 (KIS API 마감 후 0 반환)
  - 정규장 시간 체크 (장외 시 매도 큐 등록)
  - smart_sell 손절 vs 시장가 분기

호출 방식 (mixin 패턴):
  - auto_trader.py 상단에서 from bot.position_safety import sync_positions, enforce_sl, hard_kill_check
  - AutoTrader 클래스에 직접 메서드로 바인딩 (또는 _mixin_safety 함수 호출)

의존:
  - self.trader: KISTrader (fetch_balance, fetch_price, smart_sell, sell_market)
  - self._positions: dict (메모리)
  - self._save_positions(): 저장
  - logger: logging
"""
import json
import logging
from datetime import datetime, time as dtime
from pathlib import Path

logger = logging.getLogger("BH.Safety")

# ============================================================
# 매도 큐 파일 (장외 hard_kill 도달 → 다음 정규장 시초 매도)
# ============================================================
_PENDING_SELLS_PATH = (
    Path(__file__).resolve().parent.parent / "data_store" / "pending_sells.json"
)


def _load_pending_sells() -> dict:
    """장외 큐 로드 — {code: {name, qty, queued_at, reason, ...}}."""
    try:
        if _PENDING_SELLS_PATH.exists():
            return json.loads(_PENDING_SELLS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[QUEUE] pending_sells 로드 실패: {e}")
    return {}


def _save_pending_sells(data: dict) -> None:
    """장외 큐 저장."""
    try:
        _PENDING_SELLS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PENDING_SELLS_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.error(f"[QUEUE] pending_sells 저장 실패: {e}")

# ============================================================
# 디폴트 설정 (config.yaml에서 오버라이드 가능)
# ============================================================
DEFAULT_SL_PCT = 0.03   # -3% 손절
DEFAULT_TP_PCT = 0.05   # +5% 익절 (트레일링 활성 전)
HARD_KILL_PCT = 0.05    # -5% 최후 방어선
HARD_KILL_WARN_PCT = 0.03  # -3% 경고 (-5%의 60% 도달)

KST_MARKET_OPEN = dtime(9, 0)
KST_MARKET_CLOSE = dtime(15, 20)


# ============================================================
# 보호 종목 판정 (박사 추가 개선 #1)
# ============================================================
def _is_protected(pos: dict) -> bool:
    """사장님 보호 종목 여부 — hard_kill 강제 매도에서 제외.

    5/19~5/20 사고 후 사장님 영구 명령:
    - sl_disabled=True → 모든 자동 매도 차단
    - source="verification" → 검증 모드 보호
    - source="manual_sync*" → 사장님 수동 매수
    - source="sync_auto*" → 메모리 누락 자동등록 (사장님 수동 매수일 수 있음, 1사이클 보호)

    ★ 5/21 19:40 bkit:code-analyzer 검증 발견 fix: sync_auto 보호 추가 ★
    사유: sync_positions가 자동등록한 종목 = 사장님 수동 매수 가능성 = 보호 의심 우선
    """
    if not pos:
        # 빈 dict = 메모리 누락 = 보호 가정 (사장님 종목 의심)
        return True
    if pos.get("sl_disabled"):
        return True
    source = str(pos.get("source", ""))
    if source == "verification" or source.startswith("manual_sync"):
        return True
    if source.startswith("sync_auto"):
        # 자동등록 종목 = 사장님 수동 매수 의심 = 보호 (수동 sl_disabled=false 변경 시까지)
        return True
    return False


# ============================================================
# 1. sync_positions() — KIS ↔ 메모리 동기화
# ============================================================
def sync_positions(self):
    """KIS 실제 잔고 ↔ positions.json 동기화.

    3가지 케이스:
      A) KIS에 있고 메모리에 없음 → 자동 등록 + 디폴트 SL/TP
      B) 메모리에 있고 KIS에 없음 → 매도 완료 처리 + 저널 기록
      C) 양쪽 있으나 수량 불일치 → KIS 기준 덮어쓰기

    호출 위치: trading_coo.py 스케줄 (5분 간격)
    """
    try:
        kis_balance = self.trader.fetch_balance()
        if not kis_balance or not kis_balance.get("success"):
            logger.warning(
                f"[SYNC] KIS 잔고 조회 실패 — 동기화 스킵 "
                f"({kis_balance.get('message', '?') if kis_balance else 'None'})"
            )
            return

        # KIS positions → dict 변환 (정확한 필드명 사용)
        kis_dict = {}
        for p in kis_balance.get("positions", []):
            code = str(p.get("code", "")).strip()
            if not code:
                continue
            kis_dict[code] = {
                "name": p.get("name", code),
                "qty": int(p.get("qty", 0)),
                "avg_price": int(p.get("avg_price", 0)),
                "current_price": int(p.get("current_price", 0)),
            }

        memory_codes = set(self._positions.keys())
        kis_codes = set(kis_dict.keys())

        synced = {"added": [], "removed": [], "qty_fixed": []}

        # ── 케이스 A: KIS에만 있음 → 메모리 자동 등록 ──
        for code in kis_codes - memory_codes:
            kp = kis_dict[code]
            buy_price = kp["avg_price"]

            # 박사 fix: buy_price=0 케이스 (KIS API 마감 후)
            if buy_price <= 0:
                # 현재가로 임시 대체
                buy_price = kp["current_price"]
                if buy_price <= 0:
                    logger.warning(
                        f"[SYNC] {kp['name']}({code}) avg_price/current_price 둘 다 0 — "
                        f"SL 미설정 등록"
                    )
                    self._positions[code] = {
                        "name": kp["name"], "qty": kp["qty"],
                        "buy_price": 0, "stop_loss": None, "take_profit": None,
                        "source": "sync_auto_no_price",
                        "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    synced["added"].append(f"{kp['name']}({code}) [price=0]")
                    continue

            default_sl = int(round(buy_price * (1 - DEFAULT_SL_PCT)))
            default_tp = int(round(buy_price * (1 + DEFAULT_TP_PCT)))

            self._positions[code] = {
                "name": kp["name"],
                "qty": kp["qty"],
                "buy_price": float(buy_price),
                "entry_price": float(buy_price),
                "high_watermark": float(buy_price),
                "trailing_activated": False,
                "trailing_sl": 0,
                "stop_loss": default_sl,
                "take_profit": default_tp,
                "regime": "NORMAL",
                "mode": "day",
                "source": "sync_auto",
                "entry_date": datetime.now().strftime("%Y-%m-%d"),
                "sync_note": "KIS 발견 → 메모리 누락 자동등록",
                "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            synced["added"].append(
                f"{kp['name']}({code}) qty={kp['qty']} SL={default_sl} TP={default_tp}"
            )
            logger.warning(
                f"[SYNC] 🆕 메모리 누락 자동등록: {kp['name']}({code}) "
                f"qty={kp['qty']} buy={buy_price} SL={default_sl} TP={default_tp}"
            )

        # ── 케이스 B: 메모리에만 있음 → 매도 완료 처리 ──
        for code in memory_codes - kis_codes:
            old = self._positions[code]
            logger.warning(
                f"[SYNC] 🗑️ KIS 없음 → 매도완료 처리: "
                f"{old.get('name')}({code}) qty={old.get('qty')}"
            )
            synced["removed"].append(f"{old.get('name')}({code})")

            # ★ 5/22 단타봇 fix (TODO ⑭) ★ guardian 가짜 매도 -100% 회계 결함 fix
            # 사장님 5/22 11:50 명령: 진짜 손익 왜곡 → 실제 매도가 best-effort 추적
            # sell_price=0 → KIS 현재가 (시장 시점) 또는 buy_price 폴백 (손익 0%)
            # 결함 영구 기록: [[project_5_22_evening_learning_fix]] TODO ⑭
            buy_price_old = float(old.get("buy_price", 0))
            sell_price_est = buy_price_old  # 폴백: 손익 0% (가짜 -100% 방지)
            try:
                price_info = self.trader.fetch_price(code)
                if price_info and price_info.get("success"):
                    current_px = int(price_info.get("current_price", 0))
                    if current_px > 0:
                        sell_price_est = float(current_px)
            except Exception as _px_e:
                logger.debug(f"[SYNC] {code} fetch_price 실패(폴백 buy_price): {_px_e}")

            # trade_journal 적재 (박사 fix 5/21 19:45: 화이트리스트 안전 매핑)
            try:
                from data import trade_journal as _tj
                _tj.log_sell(
                    code=code,
                    name=old.get("name", code),
                    qty=int(old.get("qty", 0)),
                    sell_price=sell_price_est,  # ★ 5/22 fix: 시장 현재가 또는 buy_price 폴백 ★
                    buy_price=buy_price_old,
                    event_type="sell_close",  # 화이트리스트 호환 (외부 청산 류)
                    source="guardian",        # 화이트리스트 호환 (안전장치 매도)
                    note=f"sync_remove: KIS 잔고 사라짐 / sell_price_est={sell_price_est:.0f} "
                         f"(5/22 단타봇 TODO ⑭ fix — 가짜 -100% 방지)",
                )
            except Exception as _tj_e:
                logger.debug(f"[SYNC] trade_journal 적재 실패(무시): {_tj_e}")

            del self._positions[code]

        # ── 케이스 C: 수량 불일치 → KIS 기준 ──
        for code in kis_codes & memory_codes:
            kis_qty = kis_dict[code]["qty"]
            mem_qty = int(self._positions[code].get("qty", 0))
            if kis_qty != mem_qty:
                logger.warning(
                    f"[SYNC] 🔄 수량 불일치: {code} 메모리={mem_qty} → KIS={kis_qty}"
                )
                if kis_qty == 0:
                    del self._positions[code]
                    synced["removed"].append(f"{code}(qty=0)")
                else:
                    self._positions[code]["qty"] = kis_qty
                    synced["qty_fixed"].append(f"{code}: {mem_qty}→{kis_qty}")

        if any(synced.values()):
            self._save_positions()
            logger.info(
                f"[SYNC] ✅ 완료: 추가={len(synced['added'])} "
                f"제거={len(synced['removed'])} 수량수정={len(synced['qty_fixed'])}"
            )
            # 텔레그램 알림 (변경사항 있을 때만)
            # 박사 fix v1.1: bkit:code-analyzer WARN #2 — asyncio.get_event_loop deprecated
            # Python 3.12+ 호환: 실행 중 루프 우선, 없으면 fire-and-forget
            if hasattr(self, "_send_alert") and self._send_alert:
                msg_parts = ["🔄 [메모리 동기화]"]
                if synced["added"]:
                    msg_parts.append(f"  🆕 자동등록 {len(synced['added'])}: " + ", ".join(synced["added"][:3]))
                if synced["removed"]:
                    msg_parts.append(f"  🗑️ 제거 {len(synced['removed'])}: " + ", ".join(synced["removed"][:3]))
                if synced["qty_fixed"]:
                    msg_parts.append(f"  🔄 수량 {len(synced['qty_fixed'])}: " + ", ".join(synced["qty_fixed"][:3]))
                try:
                    import asyncio
                    coro = self._send_alert("\n".join(msg_parts))
                    if asyncio.iscoroutine(coro):
                        # Python 3.12+ 안전: 실행 중 루프 발견 시 create_task
                        try:
                            loop = asyncio.get_running_loop()
                            loop.create_task(coro)
                        except RuntimeError:
                            # 실행 중 루프 없음 (동기 컨텍스트) → asyncio.run
                            try:
                                asyncio.run(coro)
                            except Exception:
                                # 새 루프 생성 실패도 silent (코어 로직 영향 X)
                                pass
                except Exception as _e:
                    logger.debug(f"[SYNC] alert 전송 실패: {_e}")
        else:
            logger.debug("[SYNC] 변경사항 없음 — 메모리=KIS 일치")

        return synced

    except Exception as e:
        logger.error(f"[SYNC] ❌ 동기화 오류: {e}", exc_info=True)
        return None


# ============================================================
# 2. enforce_sl() — SL=None 강제 세팅
# ============================================================
def enforce_sl(
    self,
    default_sl_pct: float = DEFAULT_SL_PCT,
    default_tp_pct: float = DEFAULT_TP_PCT,
):
    """모든 포지션 순회 → SL/TP None인 종목 디폴트 강제 세팅.

    호출 위치:
      - 봇 시작 시 1회 (auto_trader.__init__)
      - sync_positions 직후 (안전망 2중)
    """
    fixed = 0
    for code, pos in list(self._positions.items()):
        name = pos.get("name", code)
        buy_price = float(pos.get("buy_price", 0))

        if buy_price <= 0:
            logger.debug(f"[ENFORCE_SL] {name}({code}) buy_price=0 — 스킵")
            continue

        changed = False
        # SL 없음 + 보호 X → 디폴트 SL
        if pos.get("stop_loss") is None and not pos.get("sl_disabled"):
            pos["stop_loss"] = int(round(buy_price * (1 - default_sl_pct)))
            logger.warning(
                f"[ENFORCE_SL] 🛡️ {name}({code}) SL=None → "
                f"디폴트 {pos['stop_loss']} (-{default_sl_pct*100:.0f}%)"
            )
            changed = True

        # TP 없음 → 디폴트 TP
        if pos.get("take_profit") is None:
            pos["take_profit"] = int(round(buy_price * (1 + default_tp_pct)))
            logger.warning(
                f"[ENFORCE_SL] 🎯 {name}({code}) TP=None → "
                f"디폴트 {pos['take_profit']} (+{default_tp_pct*100:.0f}%)"
            )
            changed = True

        if changed:
            fixed += 1

    if fixed > 0:
        self._save_positions()
        logger.info(f"[ENFORCE_SL] ✅ {fixed}종목 SL/TP 강제 세팅")
    else:
        logger.debug("[ENFORCE_SL] 모든 포지션 SL/TP 정상")

    return fixed


# ============================================================
# 3. hard_kill_check() — 메모리 무관 최후 방어선
# ============================================================
def hard_kill_check(
    self,
    kill_pct: float = HARD_KILL_PCT,
    dry_run: bool = False,
):
    """KIS 잔고 직접 조회 → -kill_pct% 이상 하락 종목 강제 매도.

    박사 fix:
      - 사장님 보호 종목 (_is_protected) 강제 제외 ★ CRITICAL
      - 정규장 시간만 매도 (장외 = 다음날 시초 큐)
      - smart_sell vs sell_market 분기 (긴급 = 시장가)
      - trade_journal 표준 적재
    """
    try:
        # 정규장 시간 체크 (박사 개선 #3)
        now_time = datetime.now().time()
        in_market = KST_MARKET_OPEN <= now_time <= KST_MARKET_CLOSE

        kis_balance = self.trader.fetch_balance()
        if not kis_balance or not kis_balance.get("success"):
            logger.error(f"[HARD_KILL] KIS 잔고 조회 실패")
            return []

        killed = []
        warned = []

        for p in kis_balance.get("positions", []):
            code = str(p.get("code", "")).strip()
            name = p.get("name", code)
            qty = int(p.get("qty", 0))
            buy_price = int(p.get("avg_price", 0))

            if qty <= 0 or buy_price <= 0:
                continue

            # 박사 fix: 사장님 보호 종목 제외 ★ CRITICAL (5/21 19:40 강화)
            # bkit:code-analyzer 발견: 메모리 누락 시 _is_protected(빈 dict)=False 위험
            # → _is_protected()에 빈 dict 보호 가정 추가 + sync_auto 보호 + 명시 로그
            mem_pos = self._positions.get(code, {})
            if not mem_pos:
                # 메모리 누락 = 사장님 수동 매수 의심 = 절대 매도 X
                logger.warning(
                    f"[HARD_KILL] 🛡️ {name}({code}) 메모리 미등록 — "
                    f"사장님 보호 가정 (수동 매수 의심) → 스킵"
                )
                continue
            if _is_protected(mem_pos):
                logger.info(
                    f"[HARD_KILL] 🛡️ {name}({code}) 사장님 보호 — 스킵 "
                    f"(sl_disabled={mem_pos.get('sl_disabled')} "
                    f"source={mem_pos.get('source')})"
                )
                continue

            # 현재가 조회
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info or not price_info.get("success"):
                    continue
                current_price = int(price_info.get("current_price", 0))
                if current_price <= 0:
                    continue
            except Exception as _pe:
                logger.warning(f"[HARD_KILL] {name}({code}) 현재가 조회 실패: {_pe}")
                continue

            # 손실률 계산
            loss_pct = (current_price - buy_price) / buy_price

            # ── 강제 매도 단계 ──
            if loss_pct <= -kill_pct:
                logger.critical(
                    f"[HARD_KILL] 🚨 {name}({code}) "
                    f"매수가={buy_price:,} 현재={current_price:,} "
                    f"손실={loss_pct*100:+.2f}% > -{kill_pct*100:.0f}%"
                )

                if dry_run:
                    logger.info(f"[HARD_KILL] 🧪 DRY_RUN: {name}({code}) 매도 안 함")
                    killed.append({
                        "code": code, "name": name,
                        "loss_pct": loss_pct,
                        "action": "dry_run",
                    })
                    continue

                if not in_market:
                    # 장외 → 다음 정규장 시초 매도 큐 등록 (박사 fix v1.1)
                    # bkit:code-analyzer 발견 WARN: 큐 미구현 TODO → 실제 큐 구현
                    try:
                        pending = _load_pending_sells()
                        pending[code] = {
                            "name": name,
                            "qty": qty,
                            "buy_price": buy_price,
                            "queued_price": current_price,
                            "loss_pct": round(loss_pct * 100, 2),
                            "reason": f"hard_kill_offhours -{kill_pct*100:.0f}%",
                            "queued_at": datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "kill_pct": kill_pct,
                        }
                        _save_pending_sells(pending)
                        logger.error(
                            f"[HARD_KILL] 📋 {name}({code}) 장외 매도 큐 등록 — "
                            f"다음 정규장 09:00 시초 매도 예약 "
                            f"(현재 손실 {loss_pct*100:+.2f}%)"
                        )
                    except Exception as _qe:
                        logger.error(f"[HARD_KILL] 큐 등록 실패: {_qe}")
                    killed.append({
                        "code": code, "name": name,
                        "loss_pct": loss_pct,
                        "action": "queued_for_next_open",
                    })
                    continue

                # 정규장 시장가 즉시 매도 (긴급)
                try:
                    result = self.trader.sell_market(code, qty)
                    success = bool(result and result.get("success"))
                    logger.critical(
                        f"[HARD_KILL] 💀 시장가 매도: {name}({code}) "
                        f"qty={qty} success={success} result={result}"
                    )
                    killed.append({
                        "code": code, "name": name,
                        "loss_pct": loss_pct,
                        "action": "KILLED" if success else "FAILED",
                        "qty": qty,
                        "result": result,
                    })

                    if success:
                        # 메모리 정리
                        if code in self._positions:
                            del self._positions[code]
                            self._save_positions()

                        # trade_journal 표준 적재 (박사 fix 5/21 19:45: 화이트리스트 안전 매핑)
                        # bkit:code-analyzer 발견: "hard_kill"/"hard_kill_check" 화이트리스트 없음
                        # → "sell_sl" (손절 류) + "guardian" (안전장치 매도) 사용
                        try:
                            from data import trade_journal as _tj
                            _tj.log_sell(
                                code=code, name=name, qty=qty,
                                sell_price=current_price,
                                buy_price=buy_price,
                                event_type="sell_sl",     # 화이트리스트 호환 (손절 류)
                                source="guardian",         # 화이트리스트 호환 (안전장치)
                                order_no=(result or {}).get("order_no")
                                          or (result or {}).get("ODNO"),
                                note=f"hard_kill: -{kill_pct*100:.0f}% 강제매도 (손실 {loss_pct*100:+.2f}%)",
                            )
                        except Exception as _tj_e:
                            logger.warning(f"[HARD_KILL] trade_journal 실패: {_tj_e}")

                except Exception as _se:
                    logger.error(
                        f"[HARD_KILL] 매도 실패 {name}({code}): {_se}",
                        exc_info=True,
                    )

            # ── 경고 단계 (-3% 도달) ──
            elif loss_pct <= -HARD_KILL_WARN_PCT:
                warned.append({
                    "code": code, "name": name,
                    "loss_pct": loss_pct,
                })
                logger.warning(
                    f"[HARD_KILL] ⚠️ 경고: {name}({code}) "
                    f"손실 {loss_pct*100:+.2f}% — 킬라인 -{kill_pct*100:.0f}% 접근"
                )

        if killed:
            logger.critical(f"[HARD_KILL] 🔥 {len(killed)}종목 처리 (dry_run={dry_run})")
        if warned:
            logger.warning(f"[HARD_KILL] ⚠️ {len(warned)}종목 경고")

        return killed

    except Exception as e:
        logger.error(f"[HARD_KILL] ❌ 오류: {e}", exc_info=True)
        return []


# ============================================================
# 통합 사이클 — 봇 호출 단일 진입점
# ============================================================
def safety_check_cycle(self, dry_run: bool = False):
    """5분 사이클: 동기화 → SL 강제 → 킬체크.

    config.bot.safety.* 토글로 단계별 활성/비활성 가능.
    """
    cfg = (self.config.get("bot", {}) or {}).get("safety", {}) or {}

    if cfg.get("sync_enabled", True):
        sync_positions(self)
    if cfg.get("enforce_sl_enabled", True):
        enforce_sl(self)
    if cfg.get("hard_kill_enabled", True):
        kill_pct = float(cfg.get("hard_kill_pct", HARD_KILL_PCT))
        actual_dry_run = dry_run or bool(cfg.get("hard_kill_dry_run", False))
        hard_kill_check(self, kill_pct=kill_pct, dry_run=actual_dry_run)


# ============================================================
# 4. process_pending_sells() — 장외 큐 시초 매도 실행 (박사 fix v1.1)
# ============================================================
def process_pending_sells(self):
    """다음 정규장 시초가 직후 호출 — pending_sells.json 큐 시장가 매도.

    호출 위치: trading_coo.py 스케줄 (09:01 KST, 시초가 안정화 1분 후).
    각 종목은 시장가 매도 시도 후 큐에서 제거.

    박사 v1.1: bkit:code-analyzer WARN #1 fix (장외 매도 큐 미구현).
    """
    try:
        pending = _load_pending_sells()
        if not pending:
            logger.debug("[QUEUE] pending_sells 빈 큐 — 노옵")
            return

        # 정규장 시간 체크 (안전망)
        now_time = datetime.now().time()
        if not (KST_MARKET_OPEN <= now_time <= KST_MARKET_CLOSE):
            logger.warning(
                f"[QUEUE] 정규장 외 호출 — 큐 처리 스킵 (대기: {len(pending)}종)"
            )
            return

        logger.critical(
            f"[QUEUE] 🚨 시초 매도 큐 처리 시작 — {len(pending)}종"
        )

        processed = []
        for code, info in list(pending.items()):
            name = info.get("name", code)
            qty = int(info.get("qty", 0))
            buy_price = float(info.get("buy_price", 0))
            queued_reason = info.get("reason", "queued_kill")

            # 사장님 보호 종목 재확인 (큐 등록 후 명령 변경 가능)
            mem_pos = self._positions.get(code, {})
            if _is_protected(mem_pos):
                logger.warning(
                    f"[QUEUE] 🛡️ {name}({code}) 큐 등록 후 보호 종목 됨 — "
                    f"매도 취소 (큐에서 제거)"
                )
                del pending[code]
                processed.append({"code": code, "action": "PROTECTED_CANCEL"})
                continue

            # 시장가 매도 실행 (시초 = 변동성 큼 → 시장가 즉시)
            try:
                result = self.trader.sell_market(code, qty)
                success = bool(result and result.get("success"))

                if success:
                    # 현재가 조회 (실현 손익 추정)
                    sell_price = buy_price
                    try:
                        price_info = self.trader.fetch_price(code)
                        if price_info and price_info.get("success"):
                            sell_price = int(price_info.get("current_price", buy_price))
                    except Exception:
                        pass

                    actual_loss = (
                        (sell_price - buy_price) / buy_price * 100
                        if buy_price > 0 else 0
                    )
                    logger.critical(
                        f"[QUEUE] 💀 시초 매도 체결: {name}({code}) qty={qty} "
                        f"매도가 {sell_price:,} 손익 {actual_loss:+.2f}% "
                        f"(큐 등록 사유: {queued_reason})"
                    )

                    # 메모리 정리
                    if code in self._positions:
                        del self._positions[code]
                        self._save_positions()

                    # trade_journal 적재 (화이트리스트 안전 매핑)
                    try:
                        from data import trade_journal as _tj
                        _tj.log_sell(
                            code=code, name=name, qty=qty,
                            sell_price=sell_price,
                            buy_price=buy_price,
                            event_type="sell_sl",
                            source="guardian",
                            order_no=(result or {}).get("order_no")
                                      or (result or {}).get("ODNO"),
                            note=f"pending_sells 큐 시초 매도: {queued_reason}",
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[QUEUE] trade_journal 실패: {_tj_e}")

                    processed.append({
                        "code": code, "name": name,
                        "action": "SOLD",
                        "sell_price": sell_price,
                        "loss_pct": actual_loss,
                    })
                    del pending[code]
                else:
                    logger.error(
                        f"[QUEUE] ❌ 시초 매도 실패: {name}({code}) result={result}"
                    )
                    processed.append({"code": code, "action": "FAILED"})
                    # 큐에 유지 — 다음 사이클 재시도

            except Exception as _se:
                logger.error(
                    f"[QUEUE] 매도 예외 {name}({code}): {_se}",
                    exc_info=True,
                )
                processed.append({"code": code, "action": "ERROR"})

        # 큐 갱신 (성공한 종목만 제거됨)
        _save_pending_sells(pending)

        sold_count = sum(1 for p in processed if p.get("action") == "SOLD")
        logger.critical(
            f"[QUEUE] ✅ 큐 처리 완료: 매도 {sold_count} / "
            f"보호취소 {sum(1 for p in processed if p.get('action') == 'PROTECTED_CANCEL')} / "
            f"실패 {sum(1 for p in processed if p.get('action') in ('FAILED','ERROR'))} / "
            f"잔여 {len(pending)}"
        )

        return processed

    except Exception as e:
        logger.error(f"[QUEUE] ❌ 큐 처리 오류: {e}", exc_info=True)
        return []


# ============================================================
# AutoTrader 클래스 mixin 헬퍼
# ============================================================
def attach_to(cls):
    """AutoTrader 클래스에 안전장치 3종 메서드를 동적 추가.

    사용법 (auto_trader.py 상단):
        from bot.position_safety import attach_to
        attach_to(AutoTrader)  # 클래스 정의 직후 호출

    또는 인스턴스 메서드 직접 호출:
        from bot.position_safety import sync_positions, enforce_sl, hard_kill_check
        sync_positions(auto_trader_instance)
    """
    cls.sync_positions = sync_positions
    cls.enforce_sl = enforce_sl
    cls.hard_kill_check = hard_kill_check
    cls.safety_check_cycle = safety_check_cycle
    cls.process_pending_sells = process_pending_sells  # 박사 v1.1: 장외 매도 큐
    return cls
