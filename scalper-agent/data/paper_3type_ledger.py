"""3-Type paper training 전용 ledger (6/3 설계 c1aa4d3 / 8e3f80e 승인 -> 6/5 구현).

A: STEADY_EVENT_RIDE  (S급 DART 명분 + STEADY 추세)
B: ROTATION_PULLBACK_BUY  (강한 섹터/그룹 눌림)
C: ROTATION_RIDE_BUY      (강한 섹터/그룹 올라타기)

원칙(불변):
- 기존 PaperPortfolio(data_store/paper_portfolio.json, daytrading_pick)는 무손상 — 완전 별도 ledger.
- 실주문 0, KIS 주문 함수 호출 0, scheduler 연결 0, SAJANG 변경 0. 기록 전용.
- A/B/C 후보 0건이어도 ledger 파일 생성.

저장: data_store/paper_3type/ledger_{YYYY-MM-DD}.json
"""
import json
from pathlib import Path
from datetime import datetime

_ROOT = Path(__file__).resolve().parent.parent
LEDGER_DIR = _ROOT / "data_store" / "paper_3type"

TYPE_SOURCE = {
    "A": "paper:3type:A_STEADY_EVENT_RIDE",
    "B": "paper:3type:B_ROTATION_PULLBACK",
    "C": "paper:3type:C_ROTATION_RIDE",
}

# 필수 기록 필드 (사장님 지시서 6/5)
FIELDS = [
    "date", "type", "ticker", "name", "sector", "group",
    "entry_reason", "signal_source",
    "virtual_entry_price", "virtual_exit_price",
    "unrealized_pnl", "realized_pnl", "MFE", "MAE", "holding_days",
    "supply", "market_regime",
    "sector_rotation_score", "group_rotation_score",
    "capital_allocated", "position_size_pct", "reject_reason",
]


class Paper3TypeLedger:
    """A/B/C 분리 3-Type paper 기록 장부. 기존 PaperPortfolio 무손상."""

    def __init__(self, date: str):
        self.date = date
        self.candidates = {"A": [], "B": [], "C": []}
        self.rejects = {"A": [], "B": [], "C": []}

    def _row(self, type_: str, fields: dict) -> dict:
        rec = {f: fields.get(f) for f in FIELDS}
        rec["date"] = self.date
        rec["type"] = type_
        rec["signal_source"] = fields.get("signal_source") or TYPE_SOURCE.get(type_)
        # 넓게 병행 기록 보존: FIELDS 외 키(forward 부품 등)는 extra로 (후방호환 — 없으면 미생성)
        extra = {k: v for k, v in fields.items() if k not in FIELDS and v is not None}
        if extra:
            rec["extra"] = extra
        return rec

    def record(self, type_: str, **fields):
        """A/B/C 후보 1건 기록 (가상 매수 후보, 실주문 아님)."""
        if type_ not in ("A", "B", "C"):
            raise ValueError(f"type must be A/B/C, got {type_}")
        self.candidates[type_].append(self._row(type_, fields))

    def record_reject(self, type_: str, reject_reason: str, **fields):
        """게이트 탈락 후보 기록 (reject_reason 필수)."""
        if type_ not in ("A", "B", "C"):
            raise ValueError(f"type must be A/B/C, got {type_}")
        fields["reject_reason"] = reject_reason
        self.rejects[type_].append(self._row(type_, fields))

    def summary(self) -> dict:
        return {
            "A": len(self.candidates["A"]),
            "B": len(self.candidates["B"]),
            "C": len(self.candidates["C"]),
            "rejects": {k: len(v) for k, v in self.rejects.items()},
        }

    def save(self) -> Path:
        """ledger_{date}.json 원자적 저장. 후보 0건이어도 파일 생성."""
        LEDGER_DIR.mkdir(parents=True, exist_ok=True)
        path = LEDGER_DIR / f"ledger_{self.date}.json"
        data = {
            "date": self.date,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type_source": TYPE_SOURCE,
            "summary": self.summary(),
            "candidates": self.candidates,
            "rejects": self.rejects,
            "note": "real_order=0 / scheduler=untouched / SAJANG=untouched / record-only",
        }
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
        return path


def new_ledger(date: str) -> Paper3TypeLedger:
    return Paper3TypeLedger(date)
