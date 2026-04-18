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
]

# 학습 데이터 (항상 동기화)
LEARNING_FILES = [
    "learning/insights.json",
    "learning/brain_performance.json",
    "learning/nxt_performance.json",
    "learning/patterns.json",
]

# 최근 일지 (--full 모드)
JOURNAL_DIRS = [
    "learning/journal/daily/",
    "learning/missed_gainers/",
    "learning/pattern_scan/",
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
    parser.add_argument("--full", action="store_true", help="JSON + 일지/학습 데이터 포함")
    parser.add_argument("--dry-run", action="store_true", help="실제 복사 없이 확인만")
    args = parser.parse_args()

    print(f"{'='*60}")
    print(f"  VPS → 로컬 데이터 동기화")
    print(f"  시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  모드: {'DRY RUN' if args.dry_run else 'FULL' if args.full else 'CORE'}")
    print(f"  VPS: {VPS_HOST}")
    print(f"{'='*60}")

    # SSH 키 확인
    if not SSH_KEY.exists():
        print(f"\n[ERROR] SSH 키 없음: {SSH_KEY}")
        sys.exit(1)

    # 1. 핵심 JSON 동기화
    core = sync_core(args.dry_run)

    # 2. 일지/학습 (--full 모드)
    journal = {"ok": 0}
    if args.full:
        journal = sync_journals(args.dry_run)

    # 3. 검증
    if not args.dry_run:
        verify_sync()

    # 결과 요약
    print(f"\n{'='*60}")
    print(f"  동기화 완료")
    print(f"  핵심 JSON: {core['ok']}건 성공, {core['fail']}건 실패")
    if args.full:
        print(f"  일지/학습: {journal['ok']}건")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
