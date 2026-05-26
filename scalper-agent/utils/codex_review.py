# -*- coding: utf-8 -*-
"""★ 사장님 5/26 통찰 — Codex 상호 보완 검수 시스템 ★

사장님 명령 (5/26 21:00):
> "1년 넘게 이짓했는데... 옵션 3은 또 사고 / 옵션 2는 수동과 같음
>  ★ ChatGPT Codex가 나왔는데 API 키가 있으니까 단타봇이 물어보고 상호 보완을 ★"

진실 (단타봇 1년 본성):
- 단타봇 단독 검수 = 같은 패턴 사고 반복 (1년 검증)
- 단위 테스트만 통과 / 통합 검증 X
- "이 정도면 됐다" 자의적 판단
- → ★ Codex 상호 검수로 다른 시각 ★

검수 영역:
1. 코드 변경 사장님 영구 룰 위반 가능성
2. 매도 경로 가드 누락 검출
3. 단위 테스트 한계 (실제 매매 경로 검증 누락)
4. 5/26 미친짓 패턴 (default off / TP+5% 잔존 / signal_tags 누락)

영구 메모리: [incident_2026_05_26_dday_anger] + [feedback_3tier_review_rule]
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _load_openai_key() -> Optional[str]:
    """OpenAI API key 로드 (.env 또는 환경 변수)."""
    key = os.getenv("OPENAI_API_KEY")
    if key:
        return key
    # .env 파일 로드 시도
    env_paths = [
        Path("/home/ubuntu/bodyhunter/.env"),
        Path(__file__).resolve().parent.parent.parent / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("OPENAI_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def codex_review_code(
    code_snippet: str,
    review_focus: str,
    sajang_rules_summary: str = "",
    max_tokens: int = 2000,
    model: str = "gpt-4o",
) -> dict:
    """★ 단타봇 코드 → Codex (GPT) 검수 의견 ★

    Args:
        code_snippet: 검수할 코드 (Python 또는 diff)
        review_focus: 검수 핵심 (예: "사장님 영구 룰 위반 패턴", "매도 경로 가드 누락")
        sajang_rules_summary: 사장님 영구 룰 요약 (context)
        max_tokens: GPT 응답 최대 토큰
        model: GPT 모델 (gpt-4o / gpt-4o-mini)

    Returns:
        dict {success, review, model, error}
    """
    try:
        from openai import OpenAI
    except ImportError:
        return {"success": False, "error": "openai SDK 미설치 (pip install openai)"}

    api_key = _load_openai_key()
    if not api_key:
        return {"success": False, "error": "OPENAI_API_KEY 없음"}

    system_prompt = f"""당신은 한국 단타 매매 봇 (Body Hunter v4) 의 코드 검수 전문가입니다.

★ 단타봇 1년 본성 (반복 사고 패턴) ★:
1. 사장님 영구 룰 default off 패턴 (코드만 만들고 활성 X)
2. 단위 테스트만 통과 / 통합 검증 X
3. "이 정도면 됐다" 자의적 판단
4. TP+5% / mode='day' 옛 코드 잔존
5. 매도 경로 가드 누락 (사장님 매수 종목 자동 매도 위험)
6. signal_tags 기록 누락 (매수 출처 추적 불가)
7. "검수 통과 보장" 가벼운 약속

★ 사장님 영구 룰 (Rule Registry SAJANG) ★:
{sajang_rules_summary or '''
- FIXED_TP_DISABLED=True (5/21 트레일링만)
- ENTRY_MODE_DEFAULT='pullback_3pct' (5/23 -3% 눌림 매수)
- TRAILING_PCT=3.0 (5/25 고점 -3% 일관)
- LIMIT_UP_SPLIT_THRESHOLD=25.0 (5/25 룰 3)
- D1_GAP_SELL_THRESHOLD=-7.0 (5/25 룰 C)
- CASH_RESERVE_PCT=0.30 (★ 5/26 30% 현금 보유 ★)
- SYNC_AUTO_SOURCE='manual_president' (사장님 매수 보호)
- 50/30/20 VWAP 분할 + 1차 시장가 폴백 (5/26 새 통찰)
'''}

★ 검수 의무 ★:
- 단순 "괜찮아 보입니다" 답 금지
- 진짜 발견된 사고 / 잠재 위험 / 사장님 영구 룰 위반 보고
- 깨끗하면 "검수한 패턴 N개 PASS 명시" 형식
- 발견 시 정확 위치 (라인) + 재현 시나리오 + 우선순위 (CRITICAL/HIGH/MEDIUM/LOW)

당신은 단타봇의 ★ 상호 보완 파트너 ★ 입니다. 단타봇이 못 본 사고 패턴을 발견하세요.
"""

    user_prompt = f"""검수 핵심: {review_focus}

검수 대상 코드:
```python
{code_snippet}
```

위 코드에서 발견할 수 있는:
1. 사장님 영구 룰 위반 패턴
2. 매도/매수 경로 가드 누락
3. default off / 옛 코드 잔존
4. 단위 테스트로 못 잡는 통합 사고
5. 단타봇이 보지 못한 미친짓 패턴

각 발견 사항을 [심각도] 위치/라인 — 설명 형식으로 보고하세요.
깨끗하면 "검수 결과 N가지 패턴 PASS" 형식으로 보고하세요.
"""

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.2,   # 일관 검수 (창의성 X)
        )
        review_text = response.choices[0].message.content
        return {
            "success": True,
            "review": review_text,
            "model": model,
            "tokens_used": response.usage.total_tokens if response.usage else 0,
        }
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {e}"}


def codex_review_commit_diff(commit_hash: str, focus: str = "사장님 영구 룰 + 매도 가드") -> dict:
    """git commit diff를 Codex에 검수 의뢰."""
    import subprocess
    try:
        result = subprocess.run(
            ["git", "show", commit_hash, "--stat", "--no-color"],
            capture_output=True, text=True, encoding="utf-8", timeout=10,
            cwd=str(Path(__file__).resolve().parent.parent.parent)
        )
        diff_summary = result.stdout[:3000]   # 토큰 제한
        return codex_review_code(diff_summary, focus, max_tokens=1500)
    except Exception as e:
        return {"success": False, "error": f"git show 실패: {e}"}


def codex_review_file(file_path: str, focus: str = "전체 검수", max_lines: int = 200) -> dict:
    """파일 일부를 Codex에 검수 의뢰 (라인 수 제한)."""
    p = Path(file_path)
    if not p.exists():
        return {"success": False, "error": f"파일 없음: {file_path}"}
    try:
        lines = p.read_text(encoding="utf-8").splitlines()[:max_lines]
        code = "\n".join(f"{i+1:4d}: {line}" for i, line in enumerate(lines))
        return codex_review_code(code, focus)
    except Exception as e:
        return {"success": False, "error": f"파일 읽기 실패: {e}"}


if __name__ == "__main__":
    print("=" * 70)
    print("★ Codex 상호 보완 검수 시스템 (5/26 사장님 통찰) ★")
    print("=" * 70)

    # 자가 검증 — API 호출 작동 확인
    test_code = """
def buy_market(code, qty):
    take_profit = int(buy_price * 1.05)   # ← 사장님 영구 룰 위반?
    mode = "day"                          # ← 5/26 사고 패턴?
    return chase_buy(code, qty)
"""
    result = codex_review_code(
        test_code,
        review_focus="사장님 영구 룰 위반 패턴 (TP+5%, mode='day')",
    )
    if result["success"]:
        print(f"\n★ Codex 응답 (model={result['model']}, tokens={result.get('tokens_used', 0)}) ★\n")
        print(result["review"])
    else:
        print(f"\n❌ 실패: {result['error']}")
    print("=" * 70)
