# -*- coding: utf-8 -*-
"""
수급 11주체 호환 스키마 정의
====================================
2026-04-16 작성 (T3 패턴 감지기용)

현재 상황:
- 수집 중: 4주체 (외국인/기관합계/개인/기타법인) via KIS API + CSV
- 확보 예정: 11주체 세부 via KRX OpenAPI (이용신청 승인 대기)

핵심 설계:
- 11주체 컬럼을 미리 정의 → 스키마 호환성 유지
- 없는 컬럼은 None/0.0 처리 → 감지기 코드 수정 불필요
- 데이터 확보 시 자동 플러그인 활성화
"""

# ── 11주체 컬럼 ID (영문, DataFrame 컬럼으로 사용) ──
ENTITY_COLUMNS = [
    # 현재 수집 중 (4주체)
    "foreign",          # 외국인합계
    "institution",      # 기관합계
    "individual",       # 개인
    "other_corp",       # 기타법인
    # KRX OpenAPI 확보 후 (7주체 세부)
    "finance_invest",   # 금융투자 (증권사 자기매매)
    "trust",            # 투신 (자산운용)
    "private_equity",   # 사모펀드
    "bank",             # 은행
    "insurance",        # 보험
    "other_finance",    # 기타금융
    "pension",          # 연기금등
]

# ── 영문→한글 매핑 (표시용) ──
ENTITY_KR = {
    "foreign": "외국인",
    "institution": "기관종합",
    "individual": "개인",
    "other_corp": "기타법인",
    "finance_invest": "금융투자",
    "trust": "투신",
    "private_equity": "사모펀드",
    "bank": "은행",
    "insurance": "보험",
    "other_finance": "기타금융",
    "pension": "연기금",
}

# ── 한글→영문 역매핑 (기존 CSV 변환용) ──
ENTITY_EN = {v: k for k, v in ENTITY_KR.items()}

# ── 현재 수집 가능 주체 (현실) ──
AVAILABLE_ENTITIES = ["foreign", "institution", "individual", "other_corp"]

# ── OpenAPI 확보 시 추가될 주체 ──
PENDING_ENTITIES = [
    "finance_invest", "trust", "private_equity",
    "bank", "insurance", "other_finance", "pension",
]

# ── KRX OpenAPI TRDVAL 매핑 (공식 순서) ──
# pykrx 소스 확인: TRDVAL1~11 + TRDVAL_TOT
# 실제 KRX [12009] 상세 뷰 순서 기준
KRX_TRDVAL_MAP = {
    "TRDVAL1": "finance_invest",   # 금융투자
    "TRDVAL2": "insurance",        # 보험
    "TRDVAL3": "trust",            # 투신
    "TRDVAL4": "other_finance",    # 기타금융
    "TRDVAL5": "bank",             # 은행
    "TRDVAL6": "pension",          # 연기금등
    "TRDVAL7": "private_equity",   # 사모
    "TRDVAL8": "institution",      # 기관합계
    "TRDVAL9": "other_corp",       # 기타법인
    "TRDVAL10": "individual",      # 개인
    "TRDVAL11": "foreign",         # 외국인
    "TRDVAL_TOT": "total",         # 합계
}

# ── 기존 CSV 컬럼 → 영문 ID 매핑 ──
# data_store/flow/{code}_investor.csv 의 컬럼 변환
LEGACY_CSV_MAP = {
    "외국인_금액": "foreign",
    "기관_금액": "institution",
    "개인_금액": "individual",
    "기타법인_금액": "other_corp",
}


def to_canonical(row: dict) -> dict:
    """
    입력 row(한글 또는 TRDVAL 형식) → 영문 ID 표준형 변환.
    없는 주체는 None 처리.

    Args:
        row: {"외국인_금액": 100, "기관_금액": 50, ...}
             또는 {"TRDVAL1": 10, "TRDVAL2": 20, ...}
             또는 이미 영문 ID 형식

    Returns:
        {"foreign": 100.0, "institution": 50.0, ..., "pension": None}
    """
    result = {k: None for k in ENTITY_COLUMNS}

    for k, v in row.items():
        # 이미 영문 ID
        if k in ENTITY_COLUMNS:
            result[k] = _safe_float(v)
            continue
        # 한글 기존 CSV 포맷
        if k in LEGACY_CSV_MAP:
            result[LEGACY_CSV_MAP[k]] = _safe_float(v)
            continue
        # KRX TRDVAL 포맷
        if k in KRX_TRDVAL_MAP and KRX_TRDVAL_MAP[k] in ENTITY_COLUMNS:
            result[KRX_TRDVAL_MAP[k]] = _safe_float(v)
            continue
        # 순수 한글 (외국인, 기관 등)
        if k in ENTITY_EN:
            result[ENTITY_EN[k]] = _safe_float(v)
            continue

    return result


def _safe_float(v):
    """숫자 변환 (None/빈값/NaN/inf → None)."""
    if v is None or v == "":
        return None
    try:
        # 한국식 천단위 쉼표 처리
        if isinstance(v, str):
            v = v.replace(",", "").replace(" ", "")
        f = float(v)
        # NaN / inf 방어 (Supabase/PostgreSQL JSONB 호환)
        if f != f:  # NaN
            return None
        if f == float("inf") or f == float("-inf"):
            return None
        return f
    except (ValueError, TypeError):
        return None


def format_entity_amounts(canonical: dict, unit: str = "억원") -> str:
    """
    표준형 수급 딕셔너리 → 표시용 문자열.
    단위: 억원 (기본)

    Example:
        >>> format_entity_amounts({"foreign": 65.7, "institution": 62.0, "individual": -127.1, ...})
        '외인 +65.7억 / 기관 +62.0억 / 개인 -127.1억 (+기타법인 +3.2억)'
    """
    parts = []
    for eid in ["foreign", "institution", "individual", "other_corp"]:
        v = canonical.get(eid)
        if v is None:
            continue
        parts.append(f"{ENTITY_KR[eid]} {v:+.1f}{unit}")

    # 세부 주체 중 유의미한 값만
    detail_parts = []
    for eid in PENDING_ENTITIES:
        v = canonical.get(eid)
        if v is None or abs(v) < 1.0:
            continue
        detail_parts.append(f"{ENTITY_KR[eid]} {v:+.1f}{unit}")

    output = " / ".join(parts)
    if detail_parts:
        output += f" | 세부: {', '.join(detail_parts)}"
    return output


if __name__ == "__main__":
    # 스모크 테스트
    print("=== 11주체 스키마 테스트 ===\n")

    # 기존 4주체 CSV 형식
    legacy = {
        "외국인_금액": 6570,  # 백만원 (예시)
        "기관_금액": 6200,
        "개인_금액": -12710,
        "기타법인_금액": -60,
    }
    c = to_canonical(legacy)
    print("[기존 4주체 입력]:", c)
    print("[표시]:", format_entity_amounts(
        {k: v/100 if v else None for k, v in c.items()}
    ))  # 백만원 → 억
    print()

    # KRX 11주체 TRDVAL 형식 (미래)
    krx_future = {
        "TRDVAL1": "3000",  # 금융투자
        "TRDVAL6": "5000",  # 연기금
        "TRDVAL7": "1000",  # 사모
        "TRDVAL8": "6200",  # 기관합계 (검증용)
        "TRDVAL11": "6570", # 외국인
    }
    c2 = to_canonical(krx_future)
    print("[KRX 11주체 입력]:", {k: v for k, v in c2.items() if v is not None})
