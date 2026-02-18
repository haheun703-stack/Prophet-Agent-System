"""
수급 통합 백테스트
— 수급 점수가 Body Hunter 성과에 미치는 영향 검증

검증 방법:
  1. 기존 Body Hunter 진입 종목의 수급 점수 확인
  2. 수급 A등급에서 진입 vs 전체 진입 → 승률/수익 비교

사용법:
  python -m backtest.supply_backtest
"""

import sys
import logging
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data.supply_analyzer import SupplyAnalyzer, SupplyScore


def analyze_past_trades():
    """기존 백테스트 결과의 수급 점수를 역산"""
    analyzer = SupplyAnalyzer()

    # Body Hunter 결과 로드
    results_file = ROOT / "results" / "body_hunter_v2_results.csv"
    if not results_file.exists():
        print("body_hunter_v2_results.csv 없음")
        # 그룹 순환매 결과로 대체
        results_file = ROOT / "results" / "group_rotation_results.csv"
        if not results_file.exists():
            print("결과 파일 없음. 먼저 백테스트를 실행하세요.")
            return

    df = pd.read_csv(results_file)
    print(f"거래 기록: {len(df)}건")
    print(f"컬럼: {list(df.columns)}")

    # 종목코드 컬럼 찾기
    code_col = None
    for c in ["ticker", "code", "종목코드"]:
        if c in df.columns:
            code_col = c
            break

    date_col = None
    for c in ["date", "entry_date", "날짜"]:
        if c in df.columns:
            date_col = c
            break

    pnl_col = None
    for c in ["pnl_pct", "rr", "수익률"]:
        if c in df.columns:
            pnl_col = c
            break

    if not code_col or not pnl_col:
        print(f"필요 컬럼 없음: code={code_col}, pnl={pnl_col}")
        return

    print(f"\n사용 컬럼: code={code_col}, date={date_col}, pnl={pnl_col}")

    # 각 거래의 수급 점수 확인
    results_with_score = []
    for _, row in df.iterrows():
        code = str(row[code_col]).replace(".KS", "").zfill(6)
        date_str = str(row[date_col]) if date_col else None

        score = analyzer.analyze(code, as_of=date_str)
        if score is None:
            continue

        results_with_score.append({
            "code": code,
            "date": date_str,
            "pnl": float(row[pnl_col]),
            "supply_score": score.total_score,
            "grade": score.grade,
            "is_body": score.is_body,
            "inst_5d": score.inst_net_5d,
            "foreign_5d": score.foreign_net_5d,
        })

    if not results_with_score:
        print("수급 데이터 매칭 실패")
        return

    rdf = pd.DataFrame(results_with_score)

    print(f"\n{'='*60}")
    print(f"  수급 점수별 성과 분석 ({len(rdf)}건)")
    print(f"{'='*60}")

    # 등급별 분석
    for grade in ["A+", "A", "B", "C", "D"]:
        sub = rdf[rdf["grade"] == grade]
        if len(sub) == 0:
            continue
        wins = (sub["pnl"] > 0).sum()
        wr = wins / len(sub) * 100
        avg_pnl = sub["pnl"].mean()
        print(f"  [{grade}] {len(sub)}건 | WR: {wr:.0f}% | 평균수익: {avg_pnl:+.2f}")

    # 몸통 vs 꼬리
    print(f"\n  --- 몸통 vs 꼬리 ---")
    body = rdf[rdf["is_body"]]
    tail = rdf[~rdf["is_body"]]

    if len(body) > 0:
        bw = (body["pnl"] > 0).sum() / len(body) * 100
        print(f"  몸통(A+/A): {len(body)}건 WR:{bw:.0f}% 평균:{body['pnl'].mean():+.2f}")
    if len(tail) > 0:
        tw = (tail["pnl"] > 0).sum() / len(tail) * 100
        print(f"  기타(B~D):  {len(tail)}건 WR:{tw:.0f}% 평균:{tail['pnl'].mean():+.2f}")

    # 수급 점수 상관관계
    corr = rdf["supply_score"].corr(rdf["pnl"])
    print(f"\n  수급점수↔수익률 상관계수: {corr:.3f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    analyze_past_trades()
