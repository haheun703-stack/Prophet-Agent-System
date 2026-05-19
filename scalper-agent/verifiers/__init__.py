# -*- coding: utf-8 -*-
"""Verifier 팀 (사장님 5/19 09:40 결정 — "AI 에이전트 팀 만들어라").

5명 검수 에이전트:
- EnvChecker: .env / config.yaml 모든 토글 점검
- DataIntegrity: 정보봇 / 큰형 / JGIS / Brain 데이터 도착 감시
- FlowMonitor: 실매매 흐름 5분 간격 감시
- CodeAuditor: commit 직후 코드 검수 (별도 pre-commit hook)
- Reporter: 위 4명 결과 통합 일일 보고

사장님이 매번 직관으로 잡아주신 패턴 → 시스템화.
"""
