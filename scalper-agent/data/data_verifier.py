# -*- coding: utf-8 -*-
"""
Data Pipeline Verifier — 장마감 후 데이터 수집 검증
=====================================================
분석 돌리기 전에 "데이터 다 들어왔나" 자동 확인.

16개 데이터 소스 검증:
  CRITICAL: daily_ohlcv, investor_flow
  HIGH:     tv_scanner, brain_report, minute_candles, minute_5min,
            nationality, nightwatch, sector_history, sector_relay,
            guardian, insights
  NOTE: tv_scanner — G6 C5T에서 독립 갱신 (C7 검증 전 보장)
        brain_report는 G7 C13에서 생성 + safeguard 보장
  MEDIUM:   short_selling, news_sentiment
  LOW:      dart_disclosure, consensus

Usage:
    python data/data_verifier.py              # 전체 검증
    python data/data_verifier.py --detail     # 상세 출력
"""

import json
import logging
import os
import random
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("BH.DataVerifier")

BASE_DIR = Path(__file__).resolve().parent.parent
STORE_DIR = BASE_DIR / "data_store"
DAILY_DIR = STORE_DIR / "daily"
FLOW_DIR = STORE_DIR / "flow"
MIN1_DIR = STORE_DIR / "1min"
MIN5_DIR = STORE_DIR / "5min"
NATIONALITY_DIR = STORE_DIR / "nationality"
SHORT_DIR = STORE_DIR / "short"
NEWS_DIR = STORE_DIR / "news_sentiment"
SIGNALS_DIR = STORE_DIR / "signals"
LEARNING_DIR = STORE_DIR / "learning"


# ═══════════════════════════════════════════
#  검증 우선순위
# ═══════════════════════════════════════════

PRIORITY_CRITICAL = "CRITICAL"
PRIORITY_HIGH = "HIGH"
PRIORITY_MEDIUM = "MEDIUM"
PRIORITY_LOW = "LOW"


# ═══════════════════════════════════════════
#  유니버스 로더 (랜덤 샘플링용)
# ═══════════════════════════════════════════

def _all_universe_codes() -> List[str]:
    """universe.json 전체 코드(샘플링 전). 로드 실패 시 []."""
    uni_path = STORE_DIR / "universe.json"
    if not uni_path.exists():
        return []
    try:
        data = json.loads(uni_path.read_text("utf-8"))
        return list(data.keys()) if isinstance(data, dict) else []
    except Exception:
        return []


def _load_universe_codes(n: int = 10) -> List[str]:
    """유니버스에서 n개 랜덤 샘플 코드 반환"""
    codes = _all_universe_codes()
    if len(codes) <= n:
        return codes
    return random.sample(codes, n)


def _load_active_codes(n: int, today: str, max_scan: int = 300) -> List[str]:
    """오늘 거래된 활성 종목(daily 꼬리 5행에 today 행 존재)만 n개 랜덤 반환.

    상폐·거래정지 종목은 daily·investor·short 3채널이 동반 정지(KIS 미제공=정상)라
    universe.json(6/22 박제)에 남아 있어도 daily가 stale → 여기서 자동 제외된다.
    이로써 투자자 수급 검증이 죽은 종목을 밟아 뜨던 랜덤 PARTIAL 오탐을 원천 차단(7/13 사장님 지시).
    반대로 daily는 today인데 investor만 stale인 '진짜 수집 실패'는 여전히 표본에 포함돼 잡힌다.

    max_scan: today 매칭 종목이 0인 상황(장전 A0가 오늘 daily 수집 전 호출·전종목 미수집)에
    전 유니버스(~2600) daily 풀스캔을 방지하는 상한. 활성 비율이 정상(≈99.8%)이면 수 개 읽고
    끝나 무영향이고, 재앙 상황(활성<3%)에서만 300개 캡 후 부족분 반환 → 상위에서 FAIL 판정."""
    codes = _all_universe_codes()
    if not codes:
        return []
    random.shuffle(codes)
    active: List[str] = []
    for c in codes[:max_scan]:
        if _csv_has_date(DAILY_DIR / f"{c}.csv", today):
            active.append(c)
            if len(active) >= n:
                break
    return active


def _csv_has_date(csv_path: Path, target: str, tail_rows: int = 5) -> bool:
    """CSV 마지막 tail_rows행 안에 target 날짜 행이 존재하는지.

    '마지막 행 == target' 비교는 장중 placeholder(당일 0값 행)나 이후 날짜 행이
    붙는 순간 과거일 소급 검증(--date)이 깨진다(7/16 실측: 7/15 investor가
    아침 재검증에서 10/10→0/10 착시). 당일 저녁 정규 검증에선
    마지막 행 == target ⟹ 포함이므로 기존 판정과 동일하다."""
    if not csv_path.exists():
        return False
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in reversed(lines[-tail_rows:]):
            s = line.strip()
            if not s:
                continue
            if s.split(",")[0].strip()[:10] == target:
                return True
        return False
    except Exception:
        return False


def _get_last_csv_date(csv_path: Path) -> Optional[str]:
    """CSV 파일의 마지막 행 날짜 추출 (YYYY-MM-DD)"""
    if not csv_path.exists():
        return None
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return None
        last_line = lines[-1].strip()
        if not last_line:
            last_line = lines[-2].strip() if len(lines) > 2 else ""
        if not last_line:
            return None
        # 첫 번째 컬럼이 날짜
        date_str = last_line.split(",")[0].strip()
        # datetime 형식이면 날짜 부분만 (2026-03-19 15:30:00 → 2026-03-19)
        return date_str[:10]
    except Exception:
        return None


def _file_mtime_date(path: Path) -> Optional[str]:
    """파일 수정일을 YYYY-MM-DD로 반환"""
    if not path.exists():
        return None
    try:
        mt = os.path.getmtime(path)
        return datetime.fromtimestamp(mt).strftime("%Y-%m-%d")
    except Exception:
        return None


# ═══════════════════════════════════════════
#  개별 검증 함수
# ═══════════════════════════════════════════

def _verify_daily_ohlcv(today: str) -> dict:
    """일봉 OHLCV CSV — 랜덤 10종목 꼬리 5행에 today 행 존재 (비율 임계).

    죽은 종목(상폐·정지)이 universe.json(6/22 박제)에 ~3.4% 잔존 → 랜덤 표본에 섞이면
    '전원 today' 기준은 3회 중 1회꼴로 거짓 PARTIAL(7/14 검수 실측 29.5%). dead 2개까지
    허용하는 비율 임계로 오탐 차단하되, 진짜 부분수집 실패(다수 stale)는 PARTIAL/FAIL로 잡음.
    investor_flow(_load_active_codes)와 달리 daily는 활성 판별의 앵커라 활성필터 치환은
    순환논리 → 비율 임계 방식 채택(독립 판정 유지·7/14 검수 High fix)."""
    codes = _load_universe_codes(10)
    if not codes:
        return {"status": "FAIL", "reason": "유니버스 로드 실패", "checked": 0, "ok": 0}
    ok = 0
    for code in codes:
        if _csv_has_date(DAILY_DIR / f"{code}.csv", today):
            ok += 1
    n = len(codes)
    if ok >= n - 2:        # dead 노이즈(상폐·정지 최대 2개) 허용 — 오탐 29.5%→~0.3%
        status = "PASS"
    elif ok >= n // 2:
        status = "PARTIAL"
    else:
        status = "FAIL"
    return {"status": status, "checked": n, "ok": ok}


def _verify_investor_flow(today: str) -> dict:
    """투자자 수급 CSV — 활성 종목(daily 오늘자) 랜덤 10 꼬리 5행에 today 행 존재.

    상폐·정지 종목(daily도 stale=KIS 미제공 정상)은 _load_active_codes가 표본에서
    자동 제외 → 6/22 박제 universe.json의 죽은 종목을 밟던 랜덤 PARTIAL 오탐 차단(7/13).
    daily는 today인데 investor만 stale이면 여전히 ok 미가산 → 진짜 수집 실패는 잡힌다."""
    codes = _load_active_codes(10, today)
    if not codes:
        # 거래일에 활성 종목 0 = daily 전종목 미수집/유니버스 로드 실패 (기존 시맨틱 유지: FAIL).
        # 이 경우 daily_ohlcv(CRITICAL)도 함께 FAIL로 잡히는 안전망이 존재.
        return {"status": "FAIL", "reason": "활성 종목 0 (유니버스 로드 실패/전종목 미수집)",
                "checked": 0, "ok": 0}
    ok = 0
    for code in codes:
        if _csv_has_date(FLOW_DIR / f"{code}_investor.csv", today):
            ok += 1
    status = "PASS" if ok == len(codes) else ("PARTIAL" if ok > 0 else "FAIL")
    return {"status": status, "checked": len(codes), "ok": ok}


def _verify_nationality(today: str) -> dict:
    """KRX 국적별 — 6/22 KRX kill switch 이후 점검 제외 (7/7 검수 M-2 fix).

    절대룰: 기초데이터 점검은 KIS 채널만·nationality는 표에서 제외.
    매일 FAIL 노이즈가 박제·sync되던 것을 SKIP으로 정직화(수집 재개 제안 아님)."""
    return {"status": "SKIP", "reason": "KRX kill switch 정상 (점검 제외·절대룰)"}


def _verify_news_sentiment(today: str) -> dict:
    """뉴스 AI 감성분석 — DC-05: 경로+패턴 수정"""
    if not NEWS_DIR.exists():
        return {"status": "FAIL", "reason": "news_sentiment 디렉터리 없음"}
    today_short = today.replace("-", "")
    # 정확한 파일명: sentiment_{YYYYMMDD}.json
    today_file = NEWS_DIR / f"sentiment_{today_short}.json"
    if today_file.exists():
        return {"status": "PASS", "file": today_file.name}
    # sentiment_latest.json 신선도 체크
    latest = NEWS_DIR / "sentiment_latest.json"
    if latest.exists():
        try:
            import json as _json
            data = _json.loads(latest.read_text("utf-8"))
            if data.get("date") == today:
                return {"status": "PASS", "source": "latest"}
        except Exception:
            pass
    return {"status": "SKIP", "reason": "오늘 뉴스 분석 미실행 (온디맨드)"}


def _verify_options_signal(today: str) -> dict:
    """옵션 심리 (P/C Ratio) — DC-05: options_signal_latest.json 직접 체크"""
    sig_path = STORE_DIR / "options_signal_latest.json"
    if not sig_path.exists():
        return {"status": "FAIL", "reason": "options_signal_latest.json 없음"}
    try:
        import json
        sig = json.loads(sig_path.read_text("utf-8"))
        sig_date = sig.get("date", "")
        if sig_date == today:
            ratio = sig.get("pc_ratio", "?")
            return {"status": "PASS", "date": sig_date, "pc_ratio": ratio}
        return {"status": "PARTIAL", "reason": f"오늘 데이터 아님 ({sig_date})"}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_dart_disclosure() -> dict:
    """DART 공시 — 주기적 배치라 날짜 체크 불필요"""
    # DART는 야간 배치 / 주 1회 — 존재 여부만 확인
    return {"status": "SKIP", "reason": "야간 배치 (별도 검증 불요)"}


def _verify_consensus() -> dict:
    """컨센서스 목표가 — 모닝추천 종목별 lazy-cache (30일 이내 허용)

    consensus.json은 모닝추천이 추천 종목에 대해서만 증권사 목표가를
    저장하는 lazy 캐시. 매일 전체 갱신되는 파일이 아니므로 기준을 완화.
    """
    cons_path = STORE_DIR / "consensus.json"
    if not cons_path.exists():
        alt_paths = [
            STORE_DIR / "consensus_cache.json",
            STORE_DIR / "processed" / "consensus.json",
        ]
        for p in alt_paths:
            if p.exists():
                cons_path = p
                break
        else:
            return {"status": "SKIP", "reason": "컨센서스 파일 없음 (모닝추천 시 생성)"}

    mtime = _file_mtime_date(cons_path)
    if not mtime:
        return {"status": "FAIL", "reason": "파일 수정일 확인 실패"}

    today_d = date.today()
    mtime_d = date.fromisoformat(mtime)
    days_old = (today_d - mtime_d).days
    # 30일 이내면 PASS (lazy-cache 특성상 매일 갱신 X)
    if days_old <= 30:
        return {"status": "PASS", "updated": mtime, "days_ago": days_old}
    return {"status": "PARTIAL", "reason": f"{days_old}일 전 업데이트", "updated": mtime}


def _verify_etf_daily(today: str) -> dict:
    """ETF 일봉(pykrx) — KRX kill switch로 갱신 중단 = 정상 (7/7 검수 M-2 fix).

    매일 FAIL 노이즈 정직화. 레짐 프록시(069500/229200)는 KIS daily/ 사용이라 무관."""
    return {"status": "SKIP", "reason": "KRX kill switch 정상 (pykrx 중단·점검 제외)"}


def _kis_last_date(path: Path) -> Optional[str]:
    """KIS 채널 CSV 마지막 행 날짜 (YYYYMMDD → YYYY-MM-DD 정규화)."""
    d = _get_last_csv_date(path)
    if d and len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _evening() -> bool:
    """저녁 수집(nightly ⑫⑬) 완료 기대 시각 이후인지 — 장전 박제 노이즈 방지용."""
    return datetime.now().hour >= 19


def _verify_ticks(today: str) -> dict:
    """장중 체결 스냅샷(ticks) 건강도 — 7/21 F-15 fix(검증 사각 해소).

    ticks는 ⑲ replay·⑲-3 페이퍼·러너의 판정 원천인데 지금까지 어떤 검증기도 보지
    않아 7/20 조용한 오염(19% 결손)을 저녁 알림이 초록불로 위장했다. 건강도 판정은
    playbook_shadow.ticks_health 단일진실 위임(재구현 금지).
    노트북(win32)은 ticks dir 부재 = VPS 전용 자산이라 SKIP(오FAIL 방지)."""
    import sys as _sys
    try:
        import tools.playbook_shadow as _pb  # noqa: E402
    except Exception:
        try:
            _sys.path.insert(0, str(BASE_DIR / "tools"))
            import playbook_shadow as _pb  # type: ignore
        except Exception as e:  # noqa: BLE001
            return {"status": "SKIP", "reason": f"playbook_shadow 로드 실패: {e}"}
    day = today.replace("-", "")
    if not (_pb.TICKS / day).exists():
        # ★ win32(노트북)=VPS 전용 자산 부재라 SKIP. 그러나 VPS에서 거래일 dir이 아예
        #   없으면 = 수집기 자체 미가동 → SKIP으로 초록불 위장 금지, FAIL(Tier1 M-1).
        if _sys.platform == "win32":
            return {"status": "SKIP", "reason": "ticks 없음 (win32 분석기·VPS 전용)"}
        try:
            from data.trading_calendar import is_trading_day
            if is_trading_day(date.fromisoformat(today)):
                return {"status": "FAIL", "reason": "ticks dir 총부재 (거래일 수집기 미가동 의심)"}
        except Exception:  # noqa: BLE001
            pass
        return {"status": "SKIP", "reason": "ticks 없음 (휴장/수집 전)"}
    h = _pb.ticks_health(day)
    if not h:
        return {"status": "SKIP", "reason": "건강도 판정 불가"}
    v = h.get("verdict")
    if v == "OK":
        return {"status": "PASS", "usable_pct": h.get("usable_pct"), "last_med": h.get("last_med")}
    if v in ("BROKEN", "TRUNCATED"):
        # 판정 원천이 죽었다 = 저녁 요약이 절대 '전부 정상' 초록불이면 안 됨
        return {"status": "FAIL", "verdict": v, "usable_pct": h.get("usable_pct"),
                "last_med": h.get("last_med"),
                "reason": f"ticks {v} (사용가능 {h.get('usable_pct')}%·마감 {h.get('last_med')})"}
    return {"status": "UNKNOWN", "verdict": v}


def _verify_short_kis(today: str) -> dict:
    """KIS 공매도(⑬·6/27 신설) — 저녁 19시 전엔 전일분 정상(수집=nightly)."""
    if not SHORT_DIR.exists():
        return {"status": "SKIP", "reason": "short/ 디렉터리 없음"}
    dates = [d for d in (_kis_last_date(SHORT_DIR / f"{c}_short_bal.csv")
                         for c in _load_universe_codes(5)) if d]
    if not dates:
        return {"status": "FAIL", "reason": "샘플 파일 없음"}
    best = max(dates)
    if best >= today:
        return {"status": "PASS", "latest": best}
    if not _evening():
        return {"status": "PASS", "latest": best, "note": "T-1 (저녁 수집 전)"}
    return {"status": "FAIL", "reason": f"최신 {best} — 저녁 수집 후에도 미갱신", "latest": best}


def _verify_credit_kis(today: str) -> dict:
    """KIS 신용잔고(⑭·6/27 신설) — 구조적 T+2~3 지연이 정상 → 7일 이내 PASS."""
    credit_dir = STORE_DIR / "credit"
    if not credit_dir.exists():
        return {"status": "SKIP", "reason": "credit/ 디렉터리 없음"}
    dates = [d for d in (_kis_last_date(credit_dir / f"{c}_credit_bal.csv")
                         for c in _load_universe_codes(5)) if d]
    if not dates:
        return {"status": "FAIL", "reason": "샘플 파일 없음"}
    best = max(dates)
    try:
        lag = (date.fromisoformat(today) - date.fromisoformat(best)).days
    except ValueError:
        return {"status": "FAIL", "reason": f"날짜 파싱 실패: {best}"}
    if lag <= 7:
        return {"status": "PASS", "latest": best, "note": f"T+{lag} (KIS 구조적 지연 정상)"}
    return {"status": "FAIL", "reason": f"최신 {best} ({lag}일 지연 — 구조적 범위 초과)", "latest": best}


def _verify_ranking_kis(today: str) -> dict:
    """KIS 순위 스냅샷(⑫·6/27 신설) — volume.csv 마지막 행 날짜."""
    p = STORE_DIR / "ranking_snapshots" / "volume.csv"
    if not p.exists():
        return {"status": "SKIP", "reason": "ranking_snapshots/volume.csv 없음"}
    best = _kis_last_date(p)
    if not best:
        return {"status": "FAIL", "reason": "마지막 행 날짜 없음"}
    if best >= today:
        return {"status": "PASS", "latest": best}
    if not _evening():
        return {"status": "PASS", "latest": best, "note": "T-1 (저녁 수집 전)"}
    return {"status": "FAIL", "reason": f"최신 {best} — 저녁 수집 후에도 미갱신", "latest": best}


def _verify_flow_market(today: str) -> dict:
    """KIS 시장 11주체(flow_market) — kospi_investor.csv 마지막 행 날짜."""
    p = STORE_DIR / "flow_market" / "kospi_investor.csv"
    if not p.exists():
        return {"status": "SKIP", "reason": "flow_market/kospi_investor.csv 없음"}
    best = _kis_last_date(p)
    if not best:
        return {"status": "FAIL", "reason": "마지막 행 날짜 없음"}
    if best >= today:
        return {"status": "PASS", "latest": best}
    if not _evening():
        return {"status": "PASS", "latest": best, "note": "T-1 (장중/수집 전)"}
    return {"status": "FAIL", "reason": f"최신 {best} — 저녁 수집 후에도 미갱신", "latest": best}


def _verify_nightwatch(today: str) -> dict:
    """NIGHTWATCH — 오늘 리포트 존재"""
    path = STORE_DIR / "nightwatch_report.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    try:
        data = json.loads(path.read_text("utf-8"))
        ts = data.get("timestamp", "")
        file_date = ts[:10] if ts else ""
        if file_date == today:
            return {"status": "PASS", "timestamp": ts}
        return {"status": "FAIL", "reason": f"날짜 불일치: {file_date}", "file_date": file_date}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_tv_scanner(today: str) -> dict:
    """거래대금 스캐너 — 오늘 스캔 결과 존재"""
    path = STORE_DIR / "tv_scanner.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    try:
        data = json.loads(path.read_text("utf-8"))
        scan_date = data.get("scan_date", "")
        if scan_date == today:
            return {"status": "PASS", "signals": data.get("total_signals", 0)}
        return {"status": "FAIL", "reason": f"날짜 불일치: {scan_date}", "scan_date": scan_date}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_sector_history(today: str) -> dict:
    """섹터 히스토리 — 오늘 엔트리 + 최근 5거래일 연속성 확인

    로테이션 분석은 5일 창을 사용하므로 과거 구멍도 탐지.
    """
    path = STORE_DIR / "sector_history.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    try:
        data = json.loads(path.read_text("utf-8"))
        dates = sorted(data.keys())
        latest = dates[-1] if dates else ""

        # 오늘 엔트리 필수
        if latest != today:
            return {"status": "FAIL", "reason": f"최신 날짜: {latest}", "latest": latest,
                    "total_dates": len(dates)}

        # 최근 5거래일 구멍 검사
        try:
            from data.trading_calendar import is_trading_day  # 7/1 검수: utils(없음)→data 정정(공휴일 오판 fix)
            _is_td = is_trading_day
        except Exception:
            _is_td = lambda d: d.weekday() < 5

        today_d = date.fromisoformat(today)
        existing = set(dates)
        missing = []
        checked = 0
        i = 1
        while checked < 5 and i < 15:
            d = today_d - timedelta(days=i)
            i += 1
            try:
                if not _is_td(d):
                    continue
            except Exception:
                if d.weekday() >= 5:
                    continue
            checked += 1
            if d.strftime("%Y-%m-%d") not in existing:
                missing.append(d.strftime("%Y-%m-%d"))

        if missing:
            return {"status": "PARTIAL",
                    "reason": f"최근 5거래일 중 누락: {', '.join(missing)}",
                    "latest": latest, "total_dates": len(dates),
                    "missing_dates": missing}

        return {"status": "PASS", "latest": latest, "total_dates": len(dates)}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_guardian(today: str) -> dict:
    """Position Guardian — 보유종목 있을 때만 검증"""
    path = LEARNING_DIR / "guardian_latest.json"
    if not path.exists():
        return {"status": "SKIP", "reason": "guardian 파일 없음 (보유 없음 가능)"}
    try:
        data = json.loads(path.read_text("utf-8"))
        g_date = data.get("date", "")
        verdicts = data.get("verdicts", [])
        if not verdicts:
            return {"status": "SKIP", "reason": "보유종목 없음"}
        if g_date == today:
            return {"status": "PASS", "date": g_date, "positions": len(verdicts)}
        # 보유종목이 있는데 오늘 실행 안 됨
        return {"status": "FAIL", "reason": f"마지막 실행: {g_date}", "date": g_date}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_brain_report(today: str) -> dict:
    """Market Brain 리포트 — 오늘 생성 여부"""
    path = STORE_DIR / "brain_report.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    try:
        data = json.loads(path.read_text("utf-8"))
        b_date = data.get("date", "")
        if b_date == today:
            return {"status": "PASS", "date": b_date}
        return {"status": "FAIL", "reason": f"날짜 불일치: {b_date}", "date": b_date}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


def _verify_insights(today: str) -> dict:
    """학습 인사이트 — 오늘 업데이트 여부"""
    path = LEARNING_DIR / "insights.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    mtime = _file_mtime_date(path)
    if mtime == today:
        return {"status": "PASS", "updated": mtime}
    return {"status": "FAIL", "reason": f"마지막 수정: {mtime}", "updated": mtime}


def _verify_cycle_scan(today: str) -> dict:
    """C34 수급 사이클 감지기 — 오늘 스캔 + surge_type 필드 존재"""
    path = STORE_DIR / "cycle_scan.json"
    if not path.exists():
        return {"status": "FAIL", "reason": "파일 없음"}
    try:
        data = json.loads(path.read_text("utf-8"))
        updated = data.get("updated", "")
        file_date = updated[:10] if updated else ""
        count = data.get("count", 0)
        results = data.get("results", []) or []

        if file_date != today:
            return {"status": "FAIL", "reason": f"최신 스캔: {file_date}",
                    "updated": updated, "count": count}

        # surge_type 필드 검증 (4/15 추가 필드)
        has_surge_type = any("surge_type" in r for r in results[:5])
        if results and not has_surge_type:
            return {"status": "PARTIAL",
                    "reason": "surge_type 필드 없음 (구버전 결과)",
                    "updated": updated, "count": count}

        return {"status": "PASS", "updated": updated, "count": count}
    except Exception as e:
        return {"status": "FAIL", "reason": str(e)}


# ═══════════════════════════════════════════
#  DataVerifier 메인 클래스
# ═══════════════════════════════════════════

# 검증 체크리스트 정의
CHECKLIST = {
    "daily_ohlcv":    {"description": "일봉 OHLCV CSV",        "priority": PRIORITY_CRITICAL},
    "investor_flow":  {"description": "투자자 수급 CSV",        "priority": PRIORITY_CRITICAL},
    "tv_scanner":     {"description": "거래대금 스캐너",         "priority": PRIORITY_HIGH},      # 16:45+ 추천 파이프라인에서 생성
    "brain_report":   {"description": "Market Brain 리포트",    "priority": PRIORITY_HIGH},      # 16:45+ 추천 파이프라인에서 생성
    # minute_candles / minute_5min — 비활성화 (2026-04-04): C1 분봉 수집 비활성화로 항상 FAIL → 제거
    "nationality":    {"description": "KRX 국적별 수급",       "priority": PRIORITY_LOW},   # 7/7: kill switch 정상 → SKIP 고정(절대룰)
    "short_kis":      {"description": "KIS 공매도 잔고",        "priority": PRIORITY_MEDIUM},  # 7/7 신설 (⑬)
    "credit_kis":     {"description": "KIS 신용잔고(지연)",     "priority": PRIORITY_LOW},     # 7/7 신설 (⑭·T+2~3 정상)
    "ranking_kis":    {"description": "KIS 순위 스냅샷 6종",    "priority": PRIORITY_MEDIUM},  # 7/7 신설 (⑫)
    "flow_market":    {"description": "KIS 시장 11주체",        "priority": PRIORITY_MEDIUM},  # 7/7 신설
    "ticks":          {"description": "장중 체결 스냅샷(판정 원천)", "priority": PRIORITY_HIGH},  # 7/21 F-15: 검증 사각 해소
    "nightwatch":     {"description": "NIGHTWATCH 리포트",     "priority": PRIORITY_HIGH},
    "sector_history": {"description": "섹터 히스토리",          "priority": PRIORITY_HIGH},
    # sector_relay — 비활성화 (2026-04-11): 자동 저장 로직 없음, sector_history가 대체
    "guardian":       {"description": "Position Guardian",     "priority": PRIORITY_HIGH},
    "insights":       {"description": "학습 인사이트",          "priority": PRIORITY_HIGH},
    # short_selling — 비활성화 (2026-04-04): pykrx API 장애 38일+ → 제거
    "news_sentiment": {"description": "뉴스 AI 감성분석",      "priority": PRIORITY_MEDIUM},
    "options_signal": {"description": "옵션 심리 (P/C Ratio)", "priority": PRIORITY_MEDIUM},
    "dart_disclosure":{"description": "DART 공시",             "priority": PRIORITY_LOW},
    "consensus":      {"description": "컨센서스 목표가",        "priority": PRIORITY_LOW},
    "etf_daily":      {"description": "ETF 일봉 데이터",        "priority": PRIORITY_MEDIUM},
    "cycle_scan":     {"description": "C34 수급 사이클 감지",     "priority": PRIORITY_HIGH},
}

# 재수집 매핑 (VER-03)
RETRY_MAP = {
    "daily_ohlcv":    "_job_collect_daily",
    "investor_flow":  "_job_collect_daily",
    # minute_candles / minute_5min — 비활성화
    "nightwatch":     "job_nightwatch_collect",
    "tv_scanner":     "_job_tv_scanner_refresh",  # C5T: G6 독립 갱신
    "sector_history": "_job_record_signals",
    # sector_relay — 비활성화
    "insights":       "_job_daily_learning",
    "etf_daily":      "_job_collect_daily",
    "cycle_scan":     "_job_cycle_scan",
}


class DataVerifier:
    """장마감 후 데이터 파이프라인 검증기"""

    def __init__(self, today: str = None, save_result: bool = True):
        self.today = today or date.today().isoformat()
        # save_result=False = 판정만 반환(상태 파일 무접촉). 소급 검증·dry-run 전용 — 7/30 M-1
        self._save_result = bool(save_result)

    def verify_all(self) -> dict:
        """전체 데이터 소스 검증"""
        t = self.today
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 각 소스별 검증 실행
        details = {}
        details["daily_ohlcv"] = _verify_daily_ohlcv(t)
        details["investor_flow"] = _verify_investor_flow(t)
        details["tv_scanner"] = _verify_tv_scanner(t)
        details["brain_report"] = _verify_brain_report(t)
        # minute_candles / minute_5min — 비활성화 (C1 제거)
        details["nationality"] = _verify_nationality(t)
        details["short_kis"] = _verify_short_kis(t)
        details["credit_kis"] = _verify_credit_kis(t)
        details["ranking_kis"] = _verify_ranking_kis(t)
        details["flow_market"] = _verify_flow_market(t)
        details["ticks"] = _verify_ticks(t)   # 7/21 F-15: 판정 원천 검증(그간 사각)
        details["nightwatch"] = _verify_nightwatch(t)
        details["sector_history"] = _verify_sector_history(t)
        # sector_relay — 비활성화 (sector_history가 대체)
        details["guardian"] = _verify_guardian(t)
        details["insights"] = _verify_insights(t)
        # short_selling — 비활성화 (pykrx API 장애)
        details["news_sentiment"] = _verify_news_sentiment(t)
        details["options_signal"] = _verify_options_signal(t)
        details["dart_disclosure"] = _verify_dart_disclosure()
        details["consensus"] = _verify_consensus()
        details["etf_daily"] = _verify_etf_daily(t)
        details["cycle_scan"] = _verify_cycle_scan(t)

        # 통계 계산
        passed = sum(1 for d in details.values() if d["status"] == "PASS")
        failed = sum(1 for d in details.values() if d["status"] == "FAIL")
        partial = sum(1 for d in details.values() if d["status"] == "PARTIAL")
        skipped = sum(1 for d in details.values() if d["status"] == "SKIP")
        total = len(details)

        # CRITICAL 실패 체크
        critical_failures = []
        for key, info in CHECKLIST.items():
            if info["priority"] == PRIORITY_CRITICAL and details[key]["status"] == "FAIL":
                critical_failures.append(key)

        # HIGH 실패 체크
        high_items = [k for k, v in CHECKLIST.items() if v["priority"] == PRIORITY_HIGH]
        high_fails = sum(1 for k in high_items if details[k]["status"] == "FAIL")

        # 종합 판정
        can_proceed = len(critical_failures) == 0
        if not can_proceed:
            status = "FAIL"
        elif failed > 0 or partial > 0:
            status = "PARTIAL"
        else:
            status = "PASS"

        result = {
            "timestamp": now,
            "date": self.today,
            "status": status,
            "total": total,
            "passed": passed,
            "failed": failed,
            "partial": partial,
            "skipped": skipped,
            "critical_failures": critical_failures,
            "high_failures": high_fails,
            "can_proceed": can_proceed,
            "details": details,
        }

        logger.info(
            f"[DataVerifier] {status} — {passed}/{total} PASS, "
            f"CRITICAL FAIL: {critical_failures or 'none'}"
        )

        # ── 결과 파일 자동 저장 (atomic write) ──
        # ★ 7/30 전체검수 M-1 — 소급 검증(--date 전일)이 이 파일을 전일자로 되돌려 쓰던 문제.
        # 아침 루틴 A1이 매일 `--dry-run --date <전일>`을 돌리므로, 09:3x부터 18:00 nightly까지
        # 파일 내용이 '전일 판정'인데 mtime은 오늘 = 7/7에 ⑳로 고친 "장전 박제" 착시의 재도입.
        # save_result=False면 판정만 반환하고 상태 파일은 건드리지 않는다(읽기전용 검증).
        if not self._save_result:
            logger.info("[DataVerifier] save_result=False — 상태 파일 미갱신(읽기전용 검증)")
            return result
        out_path = STORE_DIR / "data_verify_result.json"
        tmp_path = out_path.with_suffix(".tmp")
        try:
            tmp_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(out_path)
            logger.info(f"[DataVerifier] 결과 저장: {out_path}")
        except Exception as e:
            logger.warning(f"[DataVerifier] 결과 저장 실패: {e}")

        return result

    def get_failed_retry_jobs(self, result: dict) -> List[str]:
        """실패 항목에 대한 재수집 job 이름 목록 (중복 제거)"""
        jobs = set()
        for key, detail in result["details"].items():
            if detail["status"] == "FAIL" and key in RETRY_MAP:
                jobs.add(RETRY_MAP[key])
        return list(jobs)


# ═══════════════════════════════════════════
#  텔레그램 포맷터
# ═══════════════════════════════════════════

_STATUS_ICON = {"PASS": "✅", "FAIL": "❌", "PARTIAL": "⚠️", "SKIP": "⏭️"}

def format_verify_result(result: dict) -> str:
    """검증 결과 → 텔레그램 메시지"""
    icon = _STATUS_ICON.get(result["status"], "❓")
    lines = [f"{icon} 데이터 현황 ({result['timestamp'][-8:]})", "━━━━━━━━━━━━━━━━━━━"]

    for key, info in CHECKLIST.items():
        detail = result["details"][key]
        s = detail["status"]
        si = _STATUS_ICON.get(s, "❓")
        desc = info["description"]

        extra = ""
        if s == "PASS":
            if "checked" in detail:
                extra = f" {detail['ok']}/{detail['checked']}"
            elif "count" in detail:
                extra = f" {detail['count']}건"
            elif "signals" in detail:
                extra = f" {detail['signals']}건"
            elif "positions" in detail:
                extra = f" {detail['positions']}종목"
            if detail.get("note"):
                extra += f" ({detail['note']})"
        elif s == "FAIL":
            reason = detail.get("reason", "")
            if not reason and "checked" in detail:
                reason = f"{detail['ok']}/{detail['checked']} 일치"
            extra = f" — {reason}" if reason else ""
        elif s == "PARTIAL":
            if "checked" in detail:
                extra = f" {detail['ok']}/{detail['checked']}"
            reason = detail.get("reason", "")
            if reason:
                extra += f" — {reason}"
        elif s == "SKIP":
            extra = f" — {detail.get('reason', '')}"

        lines.append(f"  {si} {desc}{extra}")

    # 합계
    lines.append("━━━━━━━━━━━━━━━━━━━")
    lines.append(
        f"합계: {result['passed']}/{result['total']} {_STATUS_ICON['PASS']} "
        f"(실패 {result['failed']}, 스킵 {result['skipped']})"
    )

    if result["critical_failures"]:
        descs = [CHECKLIST[k]["description"] for k in result["critical_failures"]]
        lines.append(f"\n🚨 CRITICAL 실패: {', '.join(descs)}")

    return "\n".join(lines)


def format_verify_oneliner(result: dict) -> str:
    """마감 리포트용 1줄 요약"""
    if result["status"] == "PASS":
        return f"✅ 데이터: {result['passed']}/{result['total']} 수집 완료"
    elif result["status"] == "PARTIAL":
        fails = []
        for key, detail in result["details"].items():
            if detail["status"] in ("FAIL", "PARTIAL"):
                fails.append(CHECKLIST[key]["description"])
        return f"⚠️ 데이터: {result['passed']}/{result['total']} ({', '.join(fails[:3])})"
    else:
        return f"❌ 데이터: CRITICAL 실패 — 분석 중단"


# ═══════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    detail_mode = "--detail" in sys.argv

    verifier = DataVerifier()
    result = verifier.verify_all()

    # 텔레그램 포맷 출력
    print()
    print(format_verify_result(result))
    print()
    print("1줄 요약:", format_verify_oneliner(result))

    if detail_mode:
        print("\n=== 상세 결과 ===")
        for key, detail in result["details"].items():
            pri = CHECKLIST[key]["priority"]
            print(f"  [{pri}] {key}: {json.dumps(detail, ensure_ascii=False)}")

    # 재수집 필요 job 목록
    retry_jobs = verifier.get_failed_retry_jobs(result)
    if retry_jobs:
        print(f"\n재수집 필요: {retry_jobs}")
