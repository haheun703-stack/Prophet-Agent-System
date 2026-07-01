# -*- coding: utf-8 -*-
"""안전한 보유 종목 수동 동기화 도구 (5/19 일진전기 사고 후 신규)
==================================================================
사장님이 KIS 잔고를 봇 positions.json에 동기화할 때 사용.

기존 문제 (5/16 사고):
  - 동기화 시 default로 -25% SL 자동 설정
  - 사장님 "회복 신뢰" 의도 vs 시스템 "SL -25% 매도" 모순
  - 5/19 일진전기 -25.10% 자동 매도 (-626,400원 손실)

이 도구의 해결책:
  - 사장님께 종목별 의도 묻기 (단타/스윙/장기)
  - 의도에 맞게 SL/TP 자동 설정 + sl_disabled 플래그

사용:
  python -X utf8 tools/safe_sync_positions.py            # 인터랙티브 모드
  python -X utf8 tools/safe_sync_positions.py --auto-swing  # 자동 (전체 스윙으로)
"""
from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try: s.reconfigure(encoding="utf-8", errors="replace")
            except: pass

ROOT = Path(__file__).resolve().parent.parent.parent  # tools/manual/ → scalper-agent
sys.path.insert(0, str(ROOT))

POSITIONS_PATH = ROOT / "data_store" / "positions.json"

# 의도 → SL/TP 매핑
INTENT_CONFIG = {
    "day": {
        "label": "단타 (당일~며칠)",
        "sl_pct": -0.03,       # -3% (자비스 v2 5/18 학습 [feedback_lifelong_lessons])
        "tp_pct": 0.05,        # +5% (회전 익절)
        "sl_disabled": False,
        "explanation": "자동 SL/TP 작동. +5% 자동 익절, -3% 자동 손절.",
    },
    "swing": {
        "label": "스윙 (1~4주)",
        "sl_pct": -0.10,       # -10%
        "tp_pct": 0.15,        # +15%
        "sl_disabled": False,
        "explanation": "자동 SL/TP 작동. +15% 자동 익절, -10% 자동 손절. 단타보다 여유.",
    },
    "longterm": {
        "label": "장기 (실적/이벤트 대기)",
        "sl_pct": 0.0,         # SL 없음
        "tp_pct": 0.0,         # TP 없음
        "sl_disabled": True,
        "explanation": "★ 봇 자동 매도 절대 안 함. 사장님 수동 매도만 가능. (일진전기 사고 방지)",
    },
}


def load_kis_balance() -> list:
    """KIS 실잔고 로드."""
    from bot.kis_trader import KISTrader
    t = KISTrader()
    bal = t.fetch_balance()
    if not bal or not bal.get("success"):
        print("[ERROR] KIS 잔고 조회 실패")
        return []
    return bal.get("positions", [])


def load_current_positions() -> dict:
    """기존 positions.json 로드."""
    if not POSITIONS_PATH.exists():
        return {}
    return json.loads(POSITIONS_PATH.read_text("utf-8"))


def calc_sl_tp(entry_price: int, intent: str) -> tuple[int, int, bool, str]:
    """의도 기반 SL/TP 계산.

    Returns: (sl, tp, sl_disabled, reason)
    """
    cfg = INTENT_CONFIG[intent]
    sl = int(entry_price * (1 + cfg["sl_pct"])) if cfg["sl_pct"] != 0 else 0
    tp = int(entry_price * (1 + cfg["tp_pct"])) if cfg["tp_pct"] != 0 else 0
    reason = (
        f"사장님 {datetime.now().strftime('%Y-%m-%d %H:%M')} 동기화 — "
        f"의도={cfg['label']}. {cfg['explanation']}"
    )
    return sl, tp, cfg["sl_disabled"], reason


def interactive_sync():
    """대화형 동기화 — 종목별 의도 묻기."""
    kis_positions = load_kis_balance()
    if not kis_positions:
        return

    current = load_current_positions()

    print()
    print("=" * 80)
    print("🛡️ 안전 동기화 도구 (5/19 일진전기 사고 방지)")
    print("=" * 80)
    print(f"KIS 실잔고 {len(kis_positions)}종목 발견")
    print()
    print("의도 옵션:")
    for k, v in INTENT_CONFIG.items():
        print(f"  [{k:9}] {v['label']:<22} — {v['explanation']}")
    print()

    new_positions = {}
    today_tag = f"manual_sync_{datetime.now().strftime('%Y%m%d')}"

    for p in kis_positions:
        code = p.get("code", "")
        name = p.get("name", "?")
        avg = int(p.get("avg_price", p.get("pchs_avg_pric", 0)))
        cur = int(p.get("current_price", p.get("prpr", 0)))
        qty = int(p.get("qty", p.get("hldg_qty", 0)))
        if avg <= 0:
            continue

        pnl_pct = (cur/avg - 1)*100 if avg > 0 else 0
        existing = current.get(code, {})

        print(f"\n━━━ {name} ({code}) ━━━")
        print(f"  평단 {avg:,} / 현재 {cur:,} / 수량 {qty} / 평가손익 {pnl_pct:+.2f}%")
        if existing:
            print(f"  기존 설정: SL={existing.get('stop_loss', 0):,} "
                  f"TP={existing.get('take_profit', 0):,} "
                  f"sl_disabled={existing.get('sl_disabled', False)}")

        # 의도 선택
        while True:
            choice = input(f"  의도 [day/swing/longterm/skip]: ").strip().lower()
            if choice in INTENT_CONFIG or choice == "skip":
                break
            print(f"    ⚠️ 옵션: day / swing / longterm / skip")

        if choice == "skip":
            # 기존 유지
            if existing:
                new_positions[code] = existing
                print(f"  → 기존 설정 유지")
            continue

        sl, tp, disabled, reason = calc_sl_tp(avg, choice)
        new_positions[code] = {
            "entry_price": avg,
            "stop_loss": sl,
            "take_profit": tp,
            "high_watermark": avg,
            "trailing_activated": False,
            "trailing_sl": 0,
            "entry_date": existing.get("entry_date", datetime.now().strftime("%Y-%m-%d")),
            "name": name,
            "target_state": None,
            "regime": "NORMAL",
            "source": today_tag,
            "sl_disabled": disabled,
            "sl_disabled_reason": reason if disabled else None,
            "intent": choice,
            "qty": qty,
        }
        print(f"  ✓ {INTENT_CONFIG[choice]['label']} 적용: SL={sl:,} TP={tp:,} disabled={disabled}")

    # 저장
    print()
    print("=" * 80)
    print(f"적용할 동기화 결과: {len(new_positions)}종목")
    for c, p in new_positions.items():
        print(f"  {p['name']:<14} ({c}): {p.get('intent','keep'):<8} | "
              f"SL={p['stop_loss']:,} TP={p['take_profit']:,} disabled={p.get('sl_disabled', False)}")
    confirm = input("\n저장하시겠습니까? [y/N]: ").strip().lower()
    if confirm != "y":
        print("취소됨.")
        return

    # 백업
    if POSITIONS_PATH.exists():
        backup = POSITIONS_PATH.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        backup.write_text(POSITIONS_PATH.read_text("utf-8"), encoding="utf-8")
        print(f"백업: {backup.name}")

    POSITIONS_PATH.write_text(
        json.dumps(new_positions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"✅ 저장 완료: {POSITIONS_PATH}")
    print(f"⚠️ 봇 재시작 후 적용됨 (장 중이면 [규칙 -2] 따라 15:35 후 권장)")


def auto_sync_all_longterm():
    """비대화형 — KIS 실잔고 모두 longterm(SL 면제)으로 적재.

    사장님이 "7월 실적까지 다 보유" 같은 일괄 결정 시 사용.
    """
    kis_positions = load_kis_balance()
    if not kis_positions:
        return

    new_positions = {}
    today_tag = f"manual_sync_{datetime.now().strftime('%Y%m%d')}"

    for p in kis_positions:
        code = p.get("code", "")
        name = p.get("name", "?")
        avg = int(p.get("avg_price", p.get("pchs_avg_pric", 0)))
        qty = int(p.get("qty", p.get("hldg_qty", 0)))
        if avg <= 0:
            continue

        sl, tp, disabled, reason = calc_sl_tp(avg, "longterm")
        new_positions[code] = {
            "entry_price": avg,
            "stop_loss": sl,
            "take_profit": tp,
            "high_watermark": avg,
            "trailing_activated": False,
            "trailing_sl": 0,
            "entry_date": datetime.now().strftime("%Y-%m-%d"),
            "name": name,
            "target_state": None,
            "regime": "NORMAL",
            "source": today_tag,
            "sl_disabled": True,
            "sl_disabled_reason": reason,
            "intent": "longterm",
            "qty": qty,
        }

    if POSITIONS_PATH.exists():
        backup = POSITIONS_PATH.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        backup.write_text(POSITIONS_PATH.read_text("utf-8"), encoding="utf-8")

    POSITIONS_PATH.write_text(
        json.dumps(new_positions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"✅ {len(new_positions)}종목 longterm(SL 면제) 적재 완료")
    for c, p in new_positions.items():
        print(f"  {p['name']} ({c}): 평단 {p['entry_price']:,} SL=0 disabled=True")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-longterm", action="store_true",
                        help="모든 KIS 종목을 longterm(SL 면제)으로 자동 적재")
    args = parser.parse_args()

    if args.auto_longterm:
        auto_sync_all_longterm()
    else:
        interactive_sync()
