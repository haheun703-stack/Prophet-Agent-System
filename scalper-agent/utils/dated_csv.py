# -*- coding: utf-8 -*-
"""dated_csv.py — 날짜 있는 CSV 판독 **정본** ([F-82] 처방·8/7 신설)

★왜 만드는가 — 같은 사고가 세 번 났다
  8/1  단타봇이 새로 쓴 실측 스크립트가 `flow_market`(`YYYYMMDD`+BOM)을 못 읽어
       ④ 시장11주체·⑧ 순위를 "최신 없음"으로 오판 → 허위 경보 직전(검증기가 옳았다).
  8/5  같은 뿌리·다른 표면. ticks price를 컬럼 **인덱스 2**로 읽어 유효율을 64.84%로
       오산(정본 `ticks_health`는 100.0%).
  8/7  또 났다. 단타봇이 `_csv_has_date(p, "2026-08-07")`로 `flow_market`을 조회해
       8/6·8/7 **둘 다 결손**으로 셌다. 실제로는 둘 다 정상 적재였다.

  공통 원인은 표기가 여러 종류인 것이 **아니라**, 판독기를 매번 새로 파싱하는 것이다.

★무엇이 실제로 갈라져 있었나 (8/7 실측 — 단순 중복이 아니었다)
  `data_verifier._kis_last_date`      → `YYYY-MM-DD` 로 정규화
  `verify_channels.norm_date`         → `YYYYMMDD`   로 정규화 (하이픈 **제거**)
  `data_verifier._get_last_csv_date`  → 정규화 **안 함** (원본 [:10])
  세 벌이 **서로 반대 방향**이라 바꿔 끼우면 비교가 조용히 뒤집힌다. 중복보다 나쁘다.

★그래서 방향을 이름에 박는다
  `iso()` / `compact()` — 호출부가 어느 형태를 원하는지 코드에 드러난다.
  통일된 단일 형태를 강제하지 않는 이유: `ticks/20260807` 처럼 **경로 자체가
  compact**인 소비자가 실재한다(`verify_channels:241`). 하나로 밀면 경로가 깨진다.

저장 표기 현황 (8/7 실측)
  `YYYY-MM-DD` : daily · flow(investor·foreign_exh) · short · credit · ranking_snapshots
  `YYYYMMDD`   : flow_market(+BOM 선두) · ticks 디렉터리명

안전 불변식: read-only·매매 무접촉·SAJANG 무접촉·판정식 불변
  (이 모듈은 **판독만** 한다. 임계·verdict·유효일 판정은 각 정본이 그대로 소유한다.)
"""

from pathlib import Path
from typing import List, Optional, Sequence, Tuple

__all__ = ["iso", "compact", "read_dated_csv", "last_csv_date", "csv_has_date"]

BOM = "﻿"


def _bare(s) -> str:
    """BOM·따옴표·공백을 벗겨 날짜 토큰만 남긴다.

    `2026-03-19 15:30:00` 처럼 시각이 붙은 값은 **날짜 부분만** 취한다
    (`_get_last_csv_date`가 `[:10]`으로 하던 동작을 표기 무관하게 일반화)."""
    if s is None:
        return ""
    t = str(s).strip().lstrip(BOM).strip().strip('"').strip("'").strip()
    # 날짜 뒤 시각/요일 등은 첫 공백·'T'에서 끊는다
    for sep in (" ", "T", "\t"):
        if sep in t:
            t = t.split(sep)[0]
    return t.strip()


def _digits(s) -> Optional[str]:
    """날짜 토큰 → 8자리 숫자. 해석 불가면 None (예외 대신 None — 판정을 막지 않는다)."""
    t = _bare(s).replace("-", "").replace("/", "").replace(".", "")
    if len(t) == 8 and t.isdigit():
        return t
    return None


def iso(s) -> Optional[str]:
    """어떤 표기든 → `YYYY-MM-DD`. 해석 불가면 None."""
    d = _digits(s)
    return f"{d[:4]}-{d[4:6]}-{d[6:]}" if d else None


def compact(s) -> Optional[str]:
    """어떤 표기든 → `YYYYMMDD`. 해석 불가면 None.

    ticks 디렉터리명처럼 **경로가 compact**인 소비자용."""
    return _digits(s)


def read_dated_csv(path: Path, *, form: str = "iso"
                   ) -> Tuple[Optional[List[str]], List[Sequence[str]]]:
    """`(header, rows)` — BOM 흡수(utf-8-sig) + 0번 컬럼 날짜 정규화.

    행은 원본 필드를 그대로 두되 **0번만** 정규화 결과로 교체한다(해석 불가 시 원본 유지).
    파싱 실패·부재는 `(None, [])` — 호출부의 판정을 예외로 끊지 않는다."""
    norm = iso if form == "iso" else compact
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None, []
    if not lines:
        return None, []
    header = [c.strip().lstrip(BOM) for c in lines[0].split(",")]
    rows: List[Sequence[str]] = []
    for ln in lines[1:]:
        if not ln.strip():
            continue
        f = ln.split(",")
        d = norm(f[0])
        if d:
            f[0] = d
        rows.append(f)
    return header, rows


def last_csv_date(path: Path, *, form: str = "iso") -> Optional[str]:
    """마지막 **비어있지 않은** 행의 날짜. 데이터 행이 없으면 None.

    기존 `_get_last_csv_date` 계약 보존: 헤더뿐(행 2 미만)이면 None,
    꼬리 빈 줄은 건너뛴다. 달라진 점은 **표기를 정규화해 돌려준다**는 것뿐."""
    norm = iso if form == "iso" else compact
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    if len(lines) < 2:
        return None
    for ln in reversed(lines[1:]):
        if ln.strip():
            return norm(ln.split(",")[0])
    return None


def csv_has_date(path: Path, target, tail_rows: int = 5) -> bool:
    """꼬리 `tail_rows` 행 안에 `target` 날짜 행이 있는지 — **양쪽 다 정규화해서** 비교.

    ★8/7 사고 지점. 구 `_csv_has_date`는 저장 문자열과 인자를 **원문 그대로** 비교해
    `flow_market`(`20260807`)에 `"2026-08-07"`을 물으면 언제나 False였다. 결손이
    아닌데 결손으로 세어진다.

    꼬리 N행 비교를 유지하는 이유(7/16 실측): '마지막 행 == target'은 장중
    placeholder나 이후 날짜 행이 붙는 순간 과거일 소급 검증(--date)이 깨진다."""
    t = iso(target)
    if t is None:
        return False
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return False
    for ln in reversed(lines[-tail_rows:] if tail_rows else lines):
        s = ln.strip()
        if not s:
            continue
        if iso(s.split(",")[0]) == t:
            return True
    return False
