# -*- coding: utf-8 -*-
"""
릴레이 통합 허브 (Relay Hub)
- 섹터/그룹/ETF 3개 에이전트 통합
- 교차 검증: 3개 중 2개 이상에서 같은 종목 나오면 신뢰도 UP
- 최종 릴레이 추천 종목 생성
- pykrx 야간 블로킹 대비: socket timeout 설정
"""

from dataclasses import dataclass, field
from typing import Optional
import logging
import socket

from data.sector_relay import scan_all_sectors, SectorStatus
from data.group_relay import scan_all_groups, GroupStatus
from data.etf_relay import scan_all_etfs, ETFStatus

logger = logging.getLogger(__name__)

# pykrx 야간 블로킹 대비: 전역 소켓 타임아웃 (30초)
_ORIGINAL_TIMEOUT = socket.getdefaulttimeout()
_SCAN_TIMEOUT = 30  # 초


@dataclass
class RelaySignal:
    code: str
    name: str
    close: int = 0
    sources: list = field(default_factory=list)  # ["sector:방산", "group:한화", "etf:K방산"]
    signal_count: int = 0  # 몇 개 에이전트에서 감지됐나
    avg_gap: float = 0.0  # 평균 후행 갭
    change_5d: float = 0.0
    vol_ratio: float = 0.0
    confidence: str = ""  # HIGH(3개) / MED(2개) / LOW(1개)


@dataclass
class RelayReport:
    hot_sectors: list = field(default_factory=list)
    relay_sectors: list = field(default_factory=list)
    hot_groups: list = field(default_factory=list)
    relay_groups: list = field(default_factory=list)
    leading_etfs: list = field(default_factory=list)
    # 교차 검증 결과
    unified_signals: list = field(default_factory=list)


def _run_with_timeout(func, timeout_sec: int = 60):
    """함수를 별도 스레드에서 실행 + 하드 타임아웃 (pykrx hang 방지)"""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(func)
        return future.result(timeout=timeout_sec)


def scan_relay_all() -> RelayReport:
    """3개 에이전트 통합 스캔 (pykrx 야간 대비 하드 타임아웃)"""
    report = RelayReport()

    # pykrx는 내부적으로 urllib/requests를 사용 → 소켓 타임아웃으로 블로킹 방지
    socket.setdefaulttimeout(_SCAN_TIMEOUT)

    try:
        # ── 1. 섹터 스캔 (60초 하드 타임아웃) ──
        logger.info("[RelayHub] 섹터 스캔 시작...")
        try:
            sectors = _run_with_timeout(scan_all_sectors, 60)
            report.hot_sectors = [s for s in sectors if s.status in ("HOT", "WARMING")]
            report.relay_sectors = [s for s in sectors if s.status == "RELAY"]
        except Exception as e:
            logger.warning(f"[RelayHub] 섹터 스캔 실패/타임아웃: {e}")

        # ── 2. 그룹 스캔 (60초 하드 타임아웃) ──
        logger.info("[RelayHub] 그룹 스캔 시작...")
        try:
            groups = _run_with_timeout(scan_all_groups, 60)
            report.hot_groups = [g for g in groups if g.status == "HOT"]
            report.relay_groups = [g for g in groups if g.status == "RELAY"]
        except Exception as e:
            logger.warning(f"[RelayHub] 그룹 스캔 실패/타임아웃: {e}")

        # ── 3. ETF 스캔 (60초 하드 타임아웃) ──
        logger.info("[RelayHub] ETF 스캔 시작...")
        try:
            etfs = _run_with_timeout(scan_all_etfs, 60)
            report.leading_etfs = [e for e in etfs if e.status in ("LEADING", "DIVERGING")]
        except Exception as e:
            logger.warning(f"[RelayHub] ETF 스캔 실패/타임아웃: {e}")

    finally:
        # 소켓 타임아웃 원복
        socket.setdefaulttimeout(_ORIGINAL_TIMEOUT)

    # ── 4. 교차 검증 ──
    logger.info("[RelayHub] 교차 검증...")
    signal_map = {}  # code -> RelaySignal

    # 섹터 릴레이 후보
    for ss in report.relay_sectors:
        for rc in ss.relay_candidates:
            if rc.code not in signal_map:
                signal_map[rc.code] = RelaySignal(
                    code=rc.code, name=rc.name, close=rc.close,
                    change_5d=rc.change_5d, vol_ratio=rc.vol_ratio,
                )
            signal_map[rc.code].sources.append(f"sector:{ss.sector_name}")

    # 섹터 HOT 종목
    for ss in report.hot_sectors:
        for st in ss.stocks[:5]:
            if st.code not in signal_map:
                signal_map[st.code] = RelaySignal(
                    code=st.code, name=st.name, close=st.close,
                    change_5d=st.change_5d, vol_ratio=st.vol_ratio,
                )
            signal_map[st.code].sources.append(f"sector_hot:{ss.sector_name}")

    # 그룹 릴레이 후보
    for gs in report.relay_groups:
        for rc in gs.relay_candidates:
            if rc.code not in signal_map:
                signal_map[rc.code] = RelaySignal(
                    code=rc.code, name=rc.name, close=rc.close,
                    change_5d=rc.change_5d, vol_ratio=rc.vol_ratio,
                )
            signal_map[rc.code].sources.append(f"group:{gs.group_name}")

    # 그룹 HOT 종목
    for gs in report.hot_groups:
        for m in gs.members[:3]:
            if m.code not in signal_map:
                signal_map[m.code] = RelaySignal(
                    code=m.code, name=m.name, close=m.close,
                    change_5d=m.change_5d, vol_ratio=m.vol_ratio,
                )
            signal_map[m.code].sources.append(f"group_hot:{gs.group_name}")

    # ETF 후행 종목
    for es in report.leading_etfs:
        for lg in es.laggards:
            if lg.code not in signal_map:
                signal_map[lg.code] = RelaySignal(
                    code=lg.code, name=lg.name, close=lg.close,
                    change_5d=lg.change_5d, vol_ratio=lg.vol_ratio,
                )
            signal_map[lg.code].sources.append(f"etf:{es.theme_name}")
            signal_map[lg.code].avg_gap = lg.gap_vs_etf

    # 신호 개수 + 신뢰도
    for sig in signal_map.values():
        sig.signal_count = len(sig.sources)
        if sig.signal_count >= 3:
            sig.confidence = "HIGH"
        elif sig.signal_count >= 2:
            sig.confidence = "MED"
        else:
            sig.confidence = "LOW"

    # 정렬: 신호 개수 → 갭 크기
    report.unified_signals = sorted(
        signal_map.values(),
        key=lambda x: (x.signal_count, x.avg_gap),
        reverse=True,
    )

    return report


def format_relay_report(report: RelayReport, max_budget: int = 450000) -> str:
    """텔레그램용 통합 리포트"""
    lines = ["🔄 릴레이 통합 리포트", "=" * 30, ""]

    # ── 섹터 요약 ──
    if report.hot_sectors or report.relay_sectors:
        lines.append("📊 섹터")
        for s in report.hot_sectors:
            lines.append(f"  🔥 {s.sector_name} [{s.status}] 5D:{s.momentum_5d:+.1f}%")
        for s in report.relay_sectors:
            lines.append(f"  🔄 {s.sector_name} [RELAY] 갭:{s.relay_gap:+.1f}%")
        lines.append("")

    # ── 그룹 요약 ──
    if report.hot_groups or report.relay_groups:
        lines.append("🏢 그룹")
        for g in report.hot_groups:
            lines.append(f"  🔥 {g.group_name} 대장:{g.leader_5d:+.1f}%")
        for g in report.relay_groups:
            lines.append(f"  🔄 {g.group_name} 갭:{g.relay_gap:+.1f}%")
        lines.append("")

    # ── ETF 요약 ──
    if report.leading_etfs:
        lines.append("📈 ETF")
        for e in report.leading_etfs:
            lines.append(
                f"  🚀 {e.theme_name} ETF:{e.etf_5d:+.1f}% 종목:{e.constituents_avg_5d:+.1f}%"
                f" 괴리:{e.divergence:+.1f}%p"
            )
        lines.append("")

    # ── 교차 검증 TOP ──
    if report.unified_signals:
        lines.append("⭐ 교차 검증 릴레이 후보")
        conf_emoji = {"HIGH": "🔴", "MED": "🟡", "LOW": "⚪"}
        for sig in report.unified_signals[:10]:
            emoji = conf_emoji.get(sig.confidence, "⚪")
            shares = max_budget // sig.close if sig.close > 0 else 0
            buy_str = f"{shares}주={shares * sig.close:,}원" if shares > 0 else "불가"
            src_str = " + ".join(sig.sources)
            lines.append(
                f"  {emoji} {sig.name}({sig.code}) [{sig.confidence}]"
                f" {sig.close:,}원 5D:{sig.change_5d:+.1f}%"
            )
            lines.append(f"     매수:{buy_str} | {src_str}")

    if not (report.hot_sectors or report.relay_sectors or report.hot_groups
            or report.relay_groups or report.leading_etfs):
        lines.append("⬜ 현재 활성 릴레이 신호 없음")

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("릴레이 통합 스캔 시작...")
    report = scan_relay_all()
    print(format_relay_report(report))
