"""
그룹 릴레이 에이전트 (Group Relay Agent)
- 7대 재벌그룹 순환 감지
- 대장주 → 계열사 → 소부장 릴레이 패턴
- 대장주가 오르면 계열사/소부장이 따라오는 구조 감지
"""

from dataclasses import dataclass, field
from typing import Optional
from pykrx import stock
import logging

logger = logging.getLogger(__name__)

# ── 7대 그룹 정의 ──────────────────────────────────────
GROUPS = {
    "samsung": {
        "name": "삼성그룹",
        "leader": ("005930", "삼성전자"),
        "affiliates": [
            ("009150", "삼성전기"),
            ("006400", "삼성SDI"),
            ("028260", "삼성물산"),
            ("032830", "삼성생명"),
            ("000810", "삼성화재"),
            ("018260", "삼성에스디에스"),
            ("207940", "삼성바이오로직스"),
            ("010140", "삼성중공업"),
        ],
        "sobujan": [
            ("007660", "이수페타시스"),
        ],
    },
    "sk": {
        "name": "SK그룹",
        "leader": ("000660", "SK하이닉스"),
        "affiliates": [
            ("034730", "SK"),
            ("402340", "SK스퀘어"),
            ("096770", "SK이노베이션"),
            ("017670", "SK텔레콤"),
            ("326030", "SK바이오팜"),
        ],
        "sobujan": [],
    },
    "hanwha": {
        "name": "한화그룹",
        "leader": ("012450", "한화에어로스페이스"),
        "affiliates": [
            ("272210", "한화시스템"),
            ("042660", "한화오션"),
            ("009830", "한화솔루션"),
        ],
        "sobujan": [
            ("077970", "STX엔진"),
            ("003570", "SNT다이내믹스"),
        ],
    },
    "hyundai_motor": {
        "name": "현대차그룹",
        "leader": ("005380", "현대차"),
        "affiliates": [
            ("000270", "기아"),
            ("012330", "현대모비스"),
            ("064350", "현대로템"),
        ],
        "sobujan": [
            ("204320", "만도"),
            ("018880", "한온시스템"),
        ],
    },
    "hyundai_heavy": {
        "name": "HD현대그룹",
        "leader": ("267250", "HD현대"),
        "affiliates": [
            ("329180", "HD현대중공업"),
            ("009540", "HD한국조선해양"),
        ],
        "sobujan": [],
    },
    "lg": {
        "name": "LG그룹",
        "leader": ("066570", "LG전자"),
        "affiliates": [
            ("051910", "LG화학"),
            ("373220", "LG에너지솔루션"),
            ("051900", "LG생활건강"),
            ("011070", "LG이노텍"),
            ("032640", "LG유플러스"),
        ],
        "sobujan": [],
    },
    "posco": {
        "name": "포스코그룹",
        "leader": ("005490", "POSCO홀딩스"),
        "affiliates": [
            ("003670", "포스코퓨처엠"),
            ("004020", "현대제철"),
        ],
        "sobujan": [],
    },
}


@dataclass
class GroupMember:
    code: str
    name: str
    role: str  # leader / affiliate / sobujan
    close: int = 0
    change_1d: float = 0.0
    change_5d: float = 0.0
    vol_ratio: float = 0.0


@dataclass
class GroupStatus:
    group_id: str
    group_name: str
    status: str  # HOT / RELAY / COLD
    leader: Optional[GroupMember] = None
    leader_5d: float = 0.0
    affiliate_avg_5d: float = 0.0
    sobujan_avg_5d: float = 0.0
    relay_gap: float = 0.0  # leader - (affiliate+sobujan avg)
    members: list = field(default_factory=list)
    relay_candidates: list = field(default_factory=list)


def _fetch_member(code: str, name: str, role: str) -> Optional[GroupMember]:
    """그룹 멤버 모멘텀 조회"""
    try:
        from datetime import datetime, timedelta
        end = datetime.now()
        start = end - timedelta(days=45)
        df = stock.get_market_ohlcv(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), code)
        if df is None or len(df) < 5:
            return None

        close = int(df.iloc[-1]["종가"])
        if close == 0:
            return None

        c1d = (close / int(df.iloc[-2]["종가"]) - 1) * 100 if len(df) >= 2 else 0
        c5d = (close / int(df.iloc[-5]["종가"]) - 1) * 100 if len(df) >= 5 else 0
        vol = int(df.iloc[-1]["거래량"])
        avg_vol = int(df["거래량"].iloc[-6:-1].mean()) if len(df) >= 6 else vol
        vr = vol / avg_vol if avg_vol > 0 else 0

        return GroupMember(
            code=code, name=name, role=role,
            close=close, change_1d=c1d, change_5d=c5d, vol_ratio=vr,
        )
    except Exception as e:
        logger.warning(f"[GroupRelay] {name}({code}) error: {e}")
        return None


def scan_group(group_id: str) -> Optional[GroupStatus]:
    """단일 그룹 분석"""
    grp = GROUPS.get(group_id)
    if not grp:
        return None

    members = []

    # 리더
    lcode, lname = grp["leader"]
    leader_m = _fetch_member(lcode, lname, "leader")
    if leader_m:
        members.append(leader_m)

    # 계열사
    for code, name in grp["affiliates"]:
        m = _fetch_member(code, name, "affiliate")
        if m:
            members.append(m)

    # 소부장
    for code, name in grp["sobujan"]:
        m = _fetch_member(code, name, "sobujan")
        if m:
            members.append(m)

    if not members or not leader_m:
        return None

    affiliates = [m for m in members if m.role == "affiliate"]
    sobujan_list = [m for m in members if m.role == "sobujan"]

    aff_avg = sum(m.change_5d for m in affiliates) / len(affiliates) if affiliates else 0
    sub_avg = sum(m.change_5d for m in sobujan_list) / len(sobujan_list) if sobujan_list else 0
    follower_avg = (aff_avg + sub_avg) / 2 if sobujan_list else aff_avg
    relay_gap = leader_m.change_5d - follower_avg

    # 릴레이 후보: 리더 > 2% 인데 자기는 < 리더 * 0.3
    relay_candidates = []
    if leader_m.change_5d >= 2.0:
        for m in affiliates + sobujan_list:
            if m.change_5d < leader_m.change_5d * 0.3:
                relay_candidates.append(m)
        relay_candidates.sort(key=lambda x: x.change_5d)

    # 상태 판정
    if leader_m.change_5d >= 3.0 and relay_gap >= 2.0:
        status = "RELAY"
    elif leader_m.change_5d >= 2.0 and aff_avg >= 1.0:
        status = "HOT"
    else:
        status = "COLD"

    return GroupStatus(
        group_id=group_id,
        group_name=grp["name"],
        status=status,
        leader=leader_m,
        leader_5d=round(leader_m.change_5d, 2),
        affiliate_avg_5d=round(aff_avg, 2),
        sobujan_avg_5d=round(sub_avg, 2),
        relay_gap=round(relay_gap, 2),
        members=sorted(members, key=lambda x: x.change_5d, reverse=True),
        relay_candidates=relay_candidates,
    )


def scan_all_groups() -> list[GroupStatus]:
    """전 그룹 스캔"""
    results = []
    for gid in GROUPS:
        gs = scan_group(gid)
        if gs:
            results.append(gs)

    priority = {"HOT": 0, "RELAY": 1, "COLD": 2}
    results.sort(key=lambda x: (priority.get(x.status, 9), -x.leader_5d))
    return results


def format_group_report(results: list[GroupStatus]) -> str:
    """텔레그램용 그룹 리포트"""
    status_emoji = {"HOT": "🔥", "RELAY": "🔄", "COLD": "⬜"}

    lines = ["🏢 그룹 릴레이 스캔", ""]

    for gs in results:
        emoji = status_emoji.get(gs.status, "⬜")
        leader_str = f"{gs.leader.name} 5D:{gs.leader_5d:+.1f}%" if gs.leader else "N/A"
        lines.append(
            f"{emoji} {gs.group_name} [{gs.status}]"
            f"  대장:{leader_str}  계열:{gs.affiliate_avg_5d:+.1f}%  소부장:{gs.sobujan_avg_5d:+.1f}%"
        )

        if gs.status == "RELAY" and gs.relay_candidates:
            lines.append(f"   🔄 릴레이 후보 (갭 {gs.relay_gap:+.1f}%):")
            for rc in gs.relay_candidates[:5]:
                lines.append(
                    f"      · {rc.name}({rc.role}) {rc.close:,}원"
                    f" 5D:{rc.change_5d:+.1f}% 거래:{rc.vol_ratio:.1f}x"
                )

        if gs.status == "HOT":
            for m in gs.members[:3]:
                tag = {"leader": "★", "affiliate": "◆", "sobujan": "·"}[m.role]
                lines.append(f"   {tag} {m.name} {m.close:,}원 5D:{m.change_5d:+.1f}%")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        gid = sys.argv[1]
        result = scan_group(gid)
        if result:
            print(format_group_report([result]))
        else:
            print(f"그룹 '{gid}' 없음. 가능: {list(GROUPS.keys())}")
    else:
        print("전 그룹 스캔 시작...")
        results = scan_all_groups()
        print(format_group_report(results))
