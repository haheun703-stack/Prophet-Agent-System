# -*- coding: utf-8 -*-
"""VPS → 로컬 핵심 데이터 동기화 스크립트

VPS(AWS Lightsail)를 정본(source of truth)으로 사용.
로컬 data_store의 핵심 JSON + 최근 CSV를 VPS에서 가져옴.

사용법:
  python tools/sync_from_vps.py              # 핵심 JSON만 동기화
  python tools/sync_from_vps.py --full       # JSON + 최근 CSV도 동기화
  python tools/sync_from_vps.py --dry-run    # 실제 복사 없이 확인만

VPS 정보:
  IP: 13.209.153.221
  SSH Key: _tmp_zips/lightsail_60gb.pem
  원격 경로: /home/ubuntu/bodyhunter/scalper-agent/data_store/
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCALPER_DIR = PROJECT_ROOT / "scalper-agent"
LOCAL_DATA = SCALPER_DIR / "data_store"
SSH_KEY = PROJECT_ROOT / "_tmp_zips" / "lightsail_60gb.pem"
VPS_HOST = "ubuntu@13.209.153.221"
VPS_DATA = "/home/ubuntu/bodyhunter/scalper-agent/data_store"

# 핵심 JSON 파일 (항상 동기화)
CORE_FILES = [
    "_last_collect.json",
    "recommendation.json",
    "brain_report.json",
    "brain_allocation.json",
    "data_verify_result.json",
    "nxt_top5_picks.json",
    "nxt_early_data.json",
    "nxt_eligible.json",
    "bomb_watchlist.json",
    "coo_state.json",
    "coo_run_log.json",
    "circuit_breaker.json",
    "oneshot_stealth.json",
    "quant_investor_extra.json",
    "sector_history.json",
    "options_signal_latest.json",
    "flow_zscore.json",
    "market_health.json",
    "regime_state.json",
    "macro_baseline.json",
    "sector_momentum.json",
    "sector_flow.json",
    "cycle_scan.json",
    "fib_leaders.json",
    "etf_flow.json",
    "massive_dual_buy.json",
    "consecutive_surge.json",
    "accumulation_radar.json",
    "pension_scan.json",
    "pension_ownership.json",
    "daytrading_picks.json",
    "flowx_swing.json",
]

# 학습 데이터 (항상 동기화 — 일부 파일은 VPS에 없을 수 있음, 실패는 무시)
LEARNING_FILES = [
    "learning/insights.json",
    "learning/brain_performance.json",
    "learning/nxt_performance.json",
]

# 최근 일지 (--full 모드)
JOURNAL_DIRS = [
    "learning/journal/daily/",
    "learning/missed_gainers/",
    "learning/pattern_scan/",
]

# 데이터 디렉토리 (--full 모드, tar+ssh로 변경된 파일만 sync)
# 매일 가져와야 하는 핵심 데이터 디렉토리
DATA_DIRS = [
    "daily/",          # 종가 OHLCV (~2596 CSV)
    "etf_daily/",      # ETF 일봉 (~21 CSV)
    "flow/",           # 투자자/외인 수급 (~5972 CSV)
    "nationality/",    # 국적별 수급 (누적형 + 일별 스냅샷)
    "news_sentiment/", # 뉴스 감성분석
    "learning/",       # 학습 데이터 (insights, journal 등)
]


def run_scp(remote_path: str, local_path: str, dry_run: bool = False) -> bool:
    """SCP로 단일 파일 복사"""
    local_dir = Path(local_path).parent
    local_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "scp", "-i", str(SSH_KEY),
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{VPS_HOST}:{remote_path}",
        str(local_path),
    ]

    if dry_run:
        print(f"  [DRY] {remote_path} → {local_path}")
        return True

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return True
        else:
            return False
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] {remote_path}")
        return False
    except Exception as e:
        print(f"  [ERROR] {remote_path}: {e}")
        return False


def run_tar_sync(remote_dir: str, local_dir: str, since_days: int = 3,
                 dry_run: bool = False) -> dict:
    """tar+ssh로 디렉토리 sync (rsync 미설치 환경 대응).

    원격에서 since_days일 내 변경된 파일만 tar로 묶어 stdout으로 전송
    → 로컬에서 tar 압축 해제. SCP 기반이라 Windows Git Bash에서도 동작.

    Args:
        remote_dir: VPS 디렉토리 (예: "nationality/")
        local_dir: 로컬 디렉토리 (data_store 기준 상대경로)
        since_days: 최근 N일 변경 파일만 sync (기본 3일)
        dry_run: 실제 sync 없이 변경 파일 수만 카운트

    Returns: {"files": N, "size_kb": K, "ok": bool}
    """
    local_path = LOCAL_DATA / local_dir.rstrip("/")
    local_path.mkdir(parents=True, exist_ok=True)

    # 원격에서 since_days 기준 파일 찾고 tar 생성 → stdout으로 stream
    # find -newermt + tar -czf - 패턴, ssh로 실행하고 로컬에서 tar -xzf -
    remote_full = f"{VPS_DATA}/{remote_dir}"
    find_cmd = (
        f"cd {VPS_DATA} && "
        f"find {remote_dir.rstrip('/')} -type f -mtime -{since_days} "
        f"-not -name '*.tmp' -not -name '*.bak'"
    )

    if dry_run:
        ssh_cmd = [
            "ssh", "-i", str(SSH_KEY),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=15",
            VPS_HOST, f"{find_cmd} | wc -l",
        ]
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
            n = int(result.stdout.strip()) if result.stdout.strip().isdigit() else 0
            return {"files": n, "size_kb": 0, "ok": True}
        except Exception as e:
            return {"files": 0, "size_kb": 0, "ok": False, "error": str(e)}

    # 실제 sync: ssh로 tar stream → 로컬 압축 해제
    # bash -c 'find ... | tar czf - -T -' 패턴 (xargs/tar -T 활용)
    remote_pipe = (
        f"{find_cmd} > /tmp/_sync_list_$$.txt && "
        f"tar czf - -T /tmp/_sync_list_$$.txt 2>/dev/null && "
        f"rm -f /tmp/_sync_list_$$.txt"
    )

    ssh_cmd = [
        "ssh", "-i", str(SSH_KEY),
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=15",
        VPS_HOST, remote_pipe,
    ]

    try:
        # ssh stdout → tar stdin (pipe)
        ssh_proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tar_proc = subprocess.Popen(
            ["tar", "xzf", "-", "-C", str(LOCAL_DATA)],
            stdin=ssh_proc.stdout, stderr=subprocess.PIPE,
        )
        ssh_proc.stdout.close()  # SIGPIPE 방지
        tar_proc.wait(timeout=600)  # 최대 10분
        ssh_proc.wait(timeout=10)

        if tar_proc.returncode != 0 or ssh_proc.returncode != 0:
            err_t = tar_proc.stderr.read().decode("utf-8", errors="replace")[:200]
            err_s = ssh_proc.stderr.read().decode("utf-8", errors="replace")[:200]
            return {"files": 0, "size_kb": 0, "ok": False, "error": f"tar={err_t} ssh={err_s}"}

        # 변경된 파일 수 카운트 (로컬에서 mtime이 since_days 이내인 파일)
        import time as _time
        cutoff = _time.time() - since_days * 86400
        files = [f for f in local_path.rglob("*") if f.is_file() and f.stat().st_mtime >= cutoff]
        size_kb = sum(f.stat().st_size for f in files) // 1024
        return {"files": len(files), "size_kb": size_kb, "ok": True}
    except subprocess.TimeoutExpired:
        return {"files": 0, "size_kb": 0, "ok": False, "error": "timeout"}
    except Exception as e:
        return {"files": 0, "size_kb": 0, "ok": False, "error": str(e)}


def sync_data_dirs(dry_run: bool = False, since_days: int = 3) -> dict:
    """데이터 디렉토리 sync (tar+ssh 방식)"""
    stats = {"total_files": 0, "total_kb": 0, "dirs_ok": 0, "dirs_fail": 0}

    print(f"\n=== 데이터 디렉토리 sync (최근 {since_days}일) ===")
    for d in DATA_DIRS:
        marker = "[DRY]" if dry_run else "[SYNC]"
        print(f"  {marker} {d}", end=" ", flush=True)
        r = run_tar_sync(d, d, since_days=since_days, dry_run=dry_run)
        if r["ok"]:
            stats["dirs_ok"] += 1
            stats["total_files"] += r["files"]
            stats["total_kb"] += r["size_kb"]
            print(f"-> {r['files']}개 파일, {r['size_kb']}KB")
        else:
            stats["dirs_fail"] += 1
            print(f"-> FAIL: {r.get('error', 'unknown')}")

    return stats


def run_rsync(remote_dir: str, local_dir: str, dry_run: bool = False,
              recent_days: int = 7) -> int:
    """rsync로 디렉토리 동기화 (최근 N일 파일만)"""
    local_path = Path(local_dir)
    local_path.mkdir(parents=True, exist_ok=True)

    # ssh를 통한 rsync
    ssh_cmd = f"ssh -i {SSH_KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10"
    cmd = [
        "rsync", "-avz", "--progress",
        "-e", ssh_cmd,
        f"{VPS_HOST}:{remote_dir}",
        str(local_path) + "/",
    ]

    if dry_run:
        cmd.insert(1, "--dry-run")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            count = sum(1 for l in lines if not l.startswith("sending")
                       and not l.startswith("total") and ".json" in l)
            return count
        return 0
    except Exception as e:
        print(f"  [ERROR] rsync {remote_dir}: {e}")
        return 0


def sync_core(dry_run: bool = False) -> dict:
    """핵심 JSON 동기화"""
    stats = {"ok": 0, "fail": 0, "files": []}

    print("\n=== 핵심 JSON 동기화 ===")
    all_files = CORE_FILES + LEARNING_FILES
    for f in all_files:
        remote = f"{VPS_DATA}/{f}"
        local = str(LOCAL_DATA / f)
        ok = run_scp(remote, local, dry_run)
        if ok:
            stats["ok"] += 1
            stats["files"].append(f)
            marker = "[OK]" if not dry_run else "[DRY]"
            print(f"  {marker} {f}")
        else:
            stats["fail"] += 1
            print(f"  [FAIL] {f}")

    return stats


def sync_journals(dry_run: bool = False) -> dict:
    """최근 일지 동기화"""
    stats = {"ok": 0}

    print("\n=== 일지/학습 데이터 동기화 ===")
    for jdir in JOURNAL_DIRS:
        remote = f"{VPS_DATA}/{jdir}"
        local = str(LOCAL_DATA / jdir)
        count = run_rsync(remote, local, dry_run)
        stats["ok"] += count
        print(f"  {jdir}: {count}건 동기화")

    return stats


def verify_sync():
    """동기화 후 검증"""
    import json

    print("\n=== 동기화 검증 ===")
    checks = {
        "_last_collect.json": lambda d: d.get("date"),
        "recommendation.json": lambda d: d.get("timestamp"),
        "brain_report.json": lambda d: d.get("date"),
        "data_verify_result.json": lambda d: f"{d.get('date')} {d.get('status')} {d.get('passed')}/{d.get('total')}",
        "nxt_top5_picks.json": lambda d: f"{d.get('date')} {len(d.get('picks',[]))}종목",
    }

    for fname, extract in checks.items():
        fpath = LOCAL_DATA / fname
        if fpath.exists():
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                val = extract(data)
                print(f"  [OK] {fname}: {val}")
            except Exception as e:
                print(f"  [ERR] {fname}: {e}")
        else:
            print(f"  [MISS] {fname}")


def main():
    parser = argparse.ArgumentParser(description="VPS → 로컬 데이터 동기화")
    parser.add_argument("--full", action="store_true",
                        help="JSON + 데이터 디렉토리(daily/etf/flow/nationality) 포함")
    parser.add_argument("--core-only", action="store_true",
                        help="핵심 JSON만 sync (기본 동작 - 빠름)")
    parser.add_argument("--dry-run", action="store_true", help="실제 복사 없이 확인만")
    parser.add_argument("--since-days", type=int, default=3,
                        help="데이터 디렉토리: 최근 N일 변경 파일만 sync (기본 3)")
    parser.add_argument("--quiet", action="store_true",
                        help="간략 출력 (자동화/스케줄러 용)")
    args = parser.parse_args()

    t_start = datetime.now()
    if not args.quiet:
        print(f"{'='*60}")
        print(f"  VPS → 로컬 데이터 동기화")
        print(f"  시각: {t_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  모드: {'DRY RUN' if args.dry_run else 'FULL' if args.full else 'CORE'}")
        print(f"  VPS: {VPS_HOST}")
        print(f"{'='*60}")

    # SSH 키 확인
    if not SSH_KEY.exists():
        print(f"\n[ERROR] SSH 키 없음: {SSH_KEY}")
        sys.exit(1)

    # 1. 핵심 JSON 동기화
    core = sync_core(args.dry_run)

    # 2. 데이터 디렉토리 sync (--full 모드)
    data_stats = {"dirs_ok": 0, "dirs_fail": 0, "total_files": 0, "total_kb": 0}
    journal = {"ok": 0}
    if args.full:
        data_stats = sync_data_dirs(args.dry_run, since_days=args.since_days)
        # 일지는 데이터 디렉토리에 learning/ 포함되므로 별도 호출 생략

    # 3. 검증
    if not args.dry_run and not args.quiet:
        verify_sync()

    # 결과 요약 (자동화 친화적: quiet 모드는 1줄)
    elapsed = (datetime.now() - t_start).total_seconds()
    if args.quiet:
        ts = t_start.strftime('%Y-%m-%d %H:%M')
        print(f"[{ts}] sync OK: JSON={core['ok']}/{core['ok']+core['fail']}, "
              f"DIRS={data_stats['dirs_ok']}/{data_stats['dirs_ok']+data_stats['dirs_fail']}, "
              f"files={data_stats['total_files']}, {data_stats['total_kb']}KB, "
              f"{elapsed:.0f}s")
    else:
        print(f"\n{'='*60}")
        print(f"  동기화 완료 ({elapsed:.1f}s)")
        print(f"  핵심 JSON: {core['ok']}건 성공, {core['fail']}건 실패")
        if args.full:
            print(f"  데이터 디렉토리: {data_stats['dirs_ok']}/{data_stats['dirs_ok']+data_stats['dirs_fail']} 성공")
            print(f"  변경 파일: {data_stats['total_files']}개 ({data_stats['total_kb']}KB)")
        print(f"{'='*60}")

    # exit code: 핵심 JSON 실패가 있으면 1
    sys.exit(1 if core['fail'] > 0 else 0)


if __name__ == "__main__":
    main()
