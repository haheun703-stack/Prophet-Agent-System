"""
FLOWX 단타봇 → Supabase 업로드 모듈
담당 테이블: short_signals, nationality_flows
QUANT 티어 (₩50,000/월) 프리미엄 시그널
"""
import os
import sys
import json
import logging
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("flowx_upload")

# ── Supabase 클라이언트 (lazy init) ──
_supabase = None


def _get_client():
    """Supabase 클라이언트 lazy 초기화"""
    global _supabase
    if _supabase is not None:
        return _supabase

    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정 (.env 확인)")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    logger.info(f"Supabase 연결: {url[:40]}...")
    return _supabase


# ═══════════════════════════════════════
#  국적별 외인 수급 → FLOWX foreign_detail 변환
# ═══════════════════════════════════════

# 국가 → 지역 매핑 (nationality_profiler.py 기준)
_COUNTRY_REGION = {
    "미국": "us",
    "영국": "eu", "노르웨이": "eu", "스위스": "eu", "프랑스": "eu",
    "독일": "eu", "네덜란드": "eu", "룩셈부르크": "eu", "아일랜드": "eu",
    "중국": "asia", "홍콩": "asia", "싱가포르": "asia",
    "대만": "asia", "일본": "asia",
    "케이맨 제도": "other", "영국령 버진아일랜드": "other",
    "오스트레일리아": "other", "캐나다": "other",
    "사우디아라비아": "other", "쿠웨이트": "other",
    "아랍에미리트": "other", "말레이시아": "other",
}


def _build_foreign_detail(nat_daily: dict) -> dict:
    """nationality_profiler의 일별 데이터 → foreign_detail JSONB 변환

    Args:
        nat_daily: {YYYYMMDD: {국가명: 순매수량}} (최근 5일)

    Returns:
        {"us": 총합, "eu": 총합, "asia": 총합, "other": 총합, "total": 총합}
    """
    region_totals = {"us": 0, "eu": 0, "asia": 0, "other": 0}

    for day_data in nat_daily.values():
        for country, volume in day_data.items():
            region = _COUNTRY_REGION.get(country, "other")
            try:
                region_totals[region] += int(float(volume))
            except (ValueError, TypeError):
                continue

    region_totals["total"] = sum(region_totals.values())
    return region_totals


# ═══════════════════════════════════════
#  스코어 → 등급 변환
# ═══════════════════════════════════════

def _score_to_grade(total_score: float, confidence: str, nat_power_grade: str) -> str:
    """total_score + confidence + nat_power_grade → FLOWX 등급 (AAA~F)"""
    if total_score >= 85 and confidence == "HIGH" and nat_power_grade in ("POWER_BUY", "BUY"):
        return "AAA"
    elif total_score >= 75 and confidence == "HIGH":
        return "AA"
    elif total_score >= 65 and confidence == "HIGH":
        return "A"
    elif total_score >= 55:
        return "BBB"
    elif total_score >= 45:
        return "BB"
    elif total_score >= 35:
        return "B"
    elif total_score >= 25:
        return "C"
    elif total_score >= 15:
        return "D"
    else:
        return "F"


def _determine_signal_type(grade: str, inst_support: bool, volume_ratio: float,
                           tv_pattern: str = "NORMAL") -> str:
    """signal_type 결정 (FORCE_BUY / BUY / WATCH / AVOID)

    tv_pattern이 QUIET_ACCUMULATION이면 FORCE_BUY 조건 완화:
    조용한 매집 + A등급 이상 + 거래대금 2x+ → FORCE_BUY (기관 미동반이어도)
    """
    # 조용한 매집 특별 규칙
    if tv_pattern == "QUIET_ACCUMULATION" and grade in ("AAA", "AA", "A") and volume_ratio >= 2.0:
        return "FORCE_BUY"

    # 기존 로직
    if grade in ("AAA", "AA", "A") and inst_support and volume_ratio >= 1.5:
        return "FORCE_BUY"
    elif grade in ("AAA", "AA", "A", "BBB") and (inst_support or volume_ratio >= 1.3):
        return "BUY"
    elif grade in ("AAA", "AA", "A", "BBB", "BB"):
        return "WATCH"
    else:
        return "AVOID"


def _estimate_holding_days(confidence: str, regime: str) -> int:
    """예상 보유일 산정"""
    if regime == "MOMENTUM":
        return 3  # MOMENTUM은 짧게
    if confidence == "HIGH":
        return 4
    return 5


# ═══════════════════════════════════════
#  recommendation.json → FLOWX 형식 변환
# ═══════════════════════════════════════

def convert_recommendation_to_flowx(
    rec_data: dict,
    nat_daily_all: dict = None,
) -> list:
    """recommendation.json 데이터 → FLOWX short_signals 형식 변환

    Args:
        rec_data: recommendation.json 전체 dict
        nat_daily_all: {code: {YYYYMMDD: {국가명: 순매수량}}} — 국적별 원시 데이터

    Returns:
        FLOWX short_signals 형식의 dict 리스트
    """
    today_str = str(date.today())
    signals = []

    # 1) 메인 추천 종목 (stocks)
    for s in rec_data.get("stocks", []):
        code = s["code"]

        # 국가별 외인 수급
        if nat_daily_all and code in nat_daily_all:
            foreign_detail = _build_foreign_detail(nat_daily_all[code])
        else:
            # 데이터 없으면 nationality_detail에서 추정
            foreign_detail = {"us": 0, "eu": 0, "asia": 0, "other": 0, "total": 0}

        # 기관 동반 매수 판별: nationality_detail "기OK" → True
        nat_detail = s.get("nationality_detail", "")
        inst_support = "기OK" in nat_detail or "기+" in nat_detail

        # 거래대금 비율 (tv_ratio 우선, fallback: regime_detail에서 VOL 파싱)
        tv_ratio = s.get("tv_ratio", 0)
        tv_pattern = s.get("tv_pattern", "NORMAL")
        if not tv_ratio or tv_ratio <= 1.0:
            regime_detail = s.get("regime_detail", "")
            if "TV" in regime_detail:
                try:
                    tv_str = regime_detail.split("TV")[1].split("x")[0]
                    tv_ratio = float(tv_str)
                except (ValueError, IndexError):
                    pass
            if (not tv_ratio or tv_ratio <= 1.0) and "VOL" in regime_detail:
                try:
                    vol_str = regime_detail.split("VOL")[1].split("x")[0]
                    tv_ratio = float(vol_str)
                except (ValueError, IndexError):
                    pass
            if not tv_ratio:
                tv_ratio = 1.0

        # 등급 + 시그널 타입
        grade = _score_to_grade(
            s.get("total_score", 0),
            s.get("confidence", ""),
            s.get("nat_power_grade", ""),
        )
        signal_type = _determine_signal_type(grade, inst_support, tv_ratio, tv_pattern)

        signals.append({
            "date": today_str,
            "code": code,
            "name": s["name"],
            "grade": grade,
            "total_score": round(s.get("total_score", 0), 1),
            "foreign_detail": foreign_detail,
            "inst_support": inst_support,
            "entry_price": s.get("entry", 0),
            "stop_loss": s.get("sl", 0),
            "target_price": s.get("tp", 0),
            "holding_days": _estimate_holding_days(
                s.get("confidence", "MED"),
                s.get("regime", "NORMAL"),
            ),
            "signal_type": signal_type,
            "volume_ratio": round(tv_ratio, 2),
            "momentum_regime": s.get("regime", "NORMAL"),
        })

    # 2) 전쟁릴레이 종목 (war_relay_stocks) — 상위 5개만
    for w in rec_data.get("war_relay_stocks", [])[:5]:
        code = w["code"]
        rr = w.get("rr", 1.0)

        # 전쟁릴레이는 별도 등급 체계
        if rr >= 1.8:
            grade = "AA"
        elif rr >= 1.3:
            grade = "A"
        else:
            grade = "BBB"

        foreign_detail = {"us": 0, "eu": 0, "asia": 0, "other": 0, "total": 0}
        if nat_daily_all and code in nat_daily_all:
            foreign_detail = _build_foreign_detail(nat_daily_all[code])

        signals.append({
            "date": today_str,
            "code": code,
            "name": w["name"],
            "grade": grade,
            "total_score": round(rr * 40, 1),  # R:R 기반 스코어
            "foreign_detail": foreign_detail,
            "inst_support": False,  # 전쟁릴레이는 수급 별도 판별
            "entry_price": w.get("entry", 0),
            "stop_loss": w.get("sl", 0),
            "target_price": w.get("tp1", 0),
            "holding_days": 5,
            "signal_type": "BUY" if rr >= 1.5 else "WATCH",
            "volume_ratio": 1.0,
            "momentum_regime": "NORMAL",
        })

    return signals


# ═══════════════════════════════════════
#  Supabase 업로드
# ═══════════════════════════════════════

def upload_short_signals(rows: list) -> bool:
    """단기 시그널 업로드 (여러 종목 한번에)

    Returns:
        성공 여부
    """
    client = _get_client()
    if not client:
        return False

    if not rows:
        logger.warning("업로드할 시그널 없음")
        return False

    try:
        result = client.table("short_signals").upsert(
            rows, on_conflict="date,code"
        ).execute()

        # 요약 출력
        force_buy = sum(1 for r in rows if r.get("signal_type") == "FORCE_BUY")
        buy = sum(1 for r in rows if r.get("signal_type") == "BUY")
        watch = sum(1 for r in rows if r.get("signal_type") == "WATCH")
        avoid = sum(1 for r in rows if r.get("signal_type") == "AVOID")
        logger.info(
            f"[FLOWX] 단기시그널 업로드 완료: {len(rows)}개 종목\n"
            f"  FORCE_BUY: {force_buy} | BUY: {buy} | WATCH: {watch} | AVOID: {avoid}"
        )
        return True
    except Exception as e:
        logger.error(f"[FLOWX] 단기시그널 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════
#  국적별 수급 → Supabase nationality_flows
# ═══════════════════════════════════════

PREDICTION_PATH = Path(__file__).resolve().parent.parent / "data_store" / "nationality_prediction.json"


def _find_latest_csv_date(code: str) -> str | None:
    """data_store/nationality/{code}_YYYYMMDD.csv 중 가장 최신 날짜 반환"""
    nat_dir = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
    import glob
    files = glob.glob(str(nat_dir / f"{code}_*.csv"))
    if not files:
        return None
    dates = []
    for f in files:
        fname = Path(f).stem  # "005930_20260324"
        parts = fname.split("_")
        if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
            dates.append(parts[-1])
    return max(dates) if dates else None


def _build_countries_detail(code: str) -> tuple:
    """compare_nationality()로 전일 대비 변화 데이터 생성

    Returns:
        (countries_list, date_new, date_old)
        countries_list: [{"country": "영국", "category": "기관", "prev": 64871,
                          "curr": 49337, "change": -15434, "change_pct": -24.0,
                          "direction": "매도"}]
    """
    try:
        from data.nationality_signal import compare_nationality, _find_prev_trading_day

        # 실제 최신 CSV 날짜 사용 (KRX 지연 대응)
        date_new = _find_latest_csv_date(code)
        if not date_new:
            return None, None, None

        date_old = _find_prev_trading_day(date_new)
        if not date_old:
            return None, date_new, None

        changes = compare_nationality(code, date_new, date_old)
        if not changes:
            return None, date_new, date_old

        result = []
        for c in changes:
            delta = c.get("변화", 0)
            if delta == 0:
                direction = "유지"
            elif delta > 0:
                direction = "매수"
            else:
                direction = "매도"

            result.append({
                "country": c["국가"],
                "category": c.get("분류", "기타"),
                "prev": c.get("전일", 0),
                "curr": c.get("금일", 0),
                "change": delta,
                "change_pct": round(c.get("변화율", 0), 1),
                "direction": direction,
            })
        return result, date_new, date_old
    except Exception as e:
        logger.debug(f"국적 변화 데이터 생성 실패({code}): {e}")
        return None, None, None


def _find_all_nationality_codes() -> list[tuple[str, str]]:
    """nationality CSV가 있는 전체 종목 코드 + 최신 날짜 반환

    Returns:
        [(code, latest_date_str), ...] — date_str은 "YYYYMMDD" 형태
    """
    import glob as _glob
    nat_dir = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
    if not nat_dir.exists():
        return []

    # 종목별 최신 날짜 수집
    code_dates: dict[str, str] = {}
    for f in _glob.glob(str(nat_dir / "*.csv")):
        parts = Path(f).stem.split("_")
        if len(parts) >= 2 and parts[-1].isdigit() and len(parts[-1]) == 8:
            code = "_".join(parts[:-1])
            d = parts[-1]
            if code not in code_dates or d > code_dates[code]:
                code_dates[code] = d

    return list(code_dates.items())


def upload_nationality_flows() -> bool:
    """nationality 변화 데이터 → Supabase nationality_flows 업로드

    확장: prediction.json 종목(8개) + nationality CSV 전체 종목(~300개)
    countries JSONB: 텔레그램 픽토그램과 동일한 매수/매도 방향 포함
    """
    client = _get_client()
    if not client:
        return False

    # 1) prediction.json 로드 (signal, score, arch_scores)
    predictions = {}
    if PREDICTION_PATH.exists():
        try:
            pred_list = json.loads(PREDICTION_PATH.read_text("utf-8"))
            for p in pred_list:
                predictions[p["code"]] = p
        except Exception as e:
            logger.warning(f"nationality_prediction.json 로드 실패: {e}")

    # 2) nationality CSV 전체 종목 수집
    all_codes = _find_all_nationality_codes()
    if not all_codes and not predictions:
        logger.warning("nationality 데이터 없음 — 업로드 스킵")
        return False

    # universe에서 종목명 매핑
    uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
    name_map = {}
    if uni_path.exists():
        try:
            uni = json.loads(uni_path.read_text("utf-8"))
            for code, info in uni.items():
                if isinstance(info, dict):
                    name_map[code] = info.get("name", "")
        except Exception:
            pass

    # 3) 전체 종목 빌드: prediction 종목 + CSV 전체 종목 통합
    today_str = str(date.today())
    seen_codes = set()
    rows = []

    # 3a) prediction 종목 (signal/score 있음)
    for code, pred in predictions.items():
        countries, date_new, date_old = _build_countries_detail(code)
        countries_obj = {
            "date_new": date_new,
            "date_old": date_old,
            "items": countries or [],
        }
        rows.append({
            "date": today_str,
            "code": code,
            "name": pred.get("name", ""),
            "countries": countries_obj,
            "arch_scores": pred.get("arch_scores", {}),
            "signal": pred.get("signal", ""),
            "score": round(pred.get("score", 0), 1),
        })
        seen_codes.add(code)

    # 3b) CSV 전체 종목 (prediction에 없는 것만 추가)
    for code, latest_date in all_codes:
        if code in seen_codes:
            continue
        countries, date_new, date_old = _build_countries_detail(code)
        if not countries:
            continue  # 비교 데이터 없으면 스킵
        countries_obj = {
            "date_new": date_new,
            "date_old": date_old,
            "items": countries,
        }
        # 간이 시그널: 순매수 국가 수 vs 순매도 국가 수
        buy_count = sum(1 for c in countries if c["direction"] == "매수")
        sell_count = sum(1 for c in countries if c["direction"] == "매도")
        if buy_count >= sell_count + 3:
            signal = "BUY"
        elif sell_count >= buy_count + 3:
            signal = "SELL"
        else:
            signal = "NEUTRAL"

        rows.append({
            "date": today_str,
            "code": code,
            "name": name_map.get(code, ""),
            "countries": countries_obj,
            "arch_scores": {},
            "signal": signal,
            "score": 0.0,
        })
        seen_codes.add(code)

    if not rows:
        logger.warning("nationality_flows 업로드할 데이터 없음")
        return False

    # 4) Supabase upsert (50개씩 배치)
    try:
        batch_size = 50
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            client.table("nationality_flows").upsert(
                batch, on_conflict="date,code"
            ).execute()
        pred_count = len(predictions)
        extra_count = len(rows) - pred_count
        strong_buy = sum(1 for r in rows if r.get("signal") == "STRONG_BUY")
        logger.info(
            f"[FLOWX] nationality_flows 업로드 완료: "
            f"{len(rows)}종목 (예측{pred_count} + X-ray{extra_count}) "
            f"STRONG_BUY: {strong_buy}"
        )

        # AUTO-RECOVERY freshness 체크용 마커 파일
        marker_dir = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
        marker_dir.mkdir(parents=True, exist_ok=True)
        marker_path = marker_dir / "_last_upload.json"
        try:
            marker_path.write_text(json.dumps({
                "date": today_str,
                "count": len(rows),
                "pred_count": pred_count,
                "xray_count": extra_count,
                "timestamp": datetime.now().isoformat(),
            }, ensure_ascii=False), encoding="utf-8")
        except Exception as me:
            logger.warning(f"nationality _last_upload.json 저장 실패: {me}")

        return True
    except Exception as e:
        logger.error(f"[FLOWX] nationality_flows 업로드 실패: {e}")
        return False


def upload_single_signal(signal: dict) -> bool:
    """단일 종목 시그널 업로드 (장중 긴급 시그널 발생 시)"""
    client = _get_client()
    if not client:
        return False

    try:
        client.table("short_signals").upsert(
            [signal], on_conflict="date,code"
        ).execute()
        logger.info(f"[FLOWX] {signal['signal_type']}: {signal['name']} (등급: {signal['grade']})")
        return True
    except Exception as e:
        logger.error(f"[FLOWX] 단일 시그널 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════
#  듀얼 출력 필터 (구독자용 품질 게이트)
# ═══════════════════════════════════════

def _apply_flowx_filter(signals: list) -> list:
    """FLOWX 구독자용 엄격 필터 — 프리미엄 품질만 통과

    기준:
    - grade A 이상 (AAA, AA, A)
    - total_score 65점 이상
    - signal_type FORCE_BUY 또는 BUY만
    - 기관 미동반(inst_support=False)이면 score 75+ 이어야 통과
    """
    filtered = []
    for s in signals:
        # 등급 필터: A 이상만
        if s["grade"] not in ("AAA", "AA", "A"):
            continue
        # 점수 필터: 65점 이상
        if s["total_score"] < 65:
            continue
        # 시그널 필터: BUY 계열만
        if s["signal_type"] not in ("FORCE_BUY", "BUY"):
            continue
        # 기관 미동반 시 75점 이상만
        if not s["inst_support"] and s["total_score"] < 75:
            continue
        filtered.append(s)

    return filtered


# ═══════════════════════════════════════
#  통합 파이프라인: 추천 → 변환 → 필터 → 업로드
# ═══════════════════════════════════════

def run_flowx_upload(rec_data: dict = None, nat_daily_all: dict = None) -> bool:
    """FLOWX 업로드 원클릭 파이프라인

    듀얼 출력: 전체 변환 → FLOWX 필터(A+/65+/BUY) → 통과분만 업로드

    Args:
        rec_data: recommendation.json dict (None이면 파일에서 로드)
        nat_daily_all: 국적별 원시 데이터 (None이면 스킵)

    Returns:
        성공 여부
    """
    if rec_data is None:
        rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
        if not rec_path.exists():
            logger.error(f"recommendation.json 없음: {rec_path}")
            return False
        rec_data = json.loads(rec_path.read_text("utf-8"))
        logger.info(f"recommendation.json 로드: {len(rec_data.get('stocks', []))}종목")

    all_signals = convert_recommendation_to_flowx(rec_data, nat_daily_all)
    if not all_signals:
        logger.warning("변환된 시그널 없음")
        return False

    # 듀얼 출력: FLOWX 필터 적용 (구독자용 품질 게이트)
    flowx_signals = _apply_flowx_filter(all_signals)
    logger.info(
        f"[FLOWX 듀얼출력] 전체 {len(all_signals)}종목 → 필터 통과 {len(flowx_signals)}종목"
    )

    if not flowx_signals:
        logger.info("[FLOWX] 필터 통과 종목 0개 — 기준 미달 시 업로드 스킵 (정상)")
    else:
        upload_short_signals(flowx_signals)

    # nationality_flows 업로드 (prediction.json + CSV)
    try:
        upload_nationality_flows()
    except Exception as e:
        logger.error(f"[FLOWX] nationality_flows 업로드 실패: {e}")

    return True


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

def _print_signals(signals: list):
    """시그널 테이블 콘솔 출력"""
    print(f"\n{'='*80}")
    print(f"  FLOWX 단기시그널 — {len(signals)}개 종목")
    print(f"{'='*80}")
    print(f"  {'종목':<12} {'등급':>4} {'점수':>5} {'시그널':<10} {'진입가':>8} {'손절':>8} {'목표가':>8} {'보유일':>4} {'레짐':<8}")
    print(f"  {'-'*74}")
    for s in signals:
        print(
            f"  {s['name']:<12} {s['grade']:>4} {s['total_score']:>5.1f} "
            f"{s['signal_type']:<10} {s['entry_price']:>8,} {s['stop_loss']:>8,} "
            f"{s['target_price']:>8,} {s['holding_days']:>4} {s['momentum_regime']:<8}"
        )
        fd = s["foreign_detail"]
        if fd["total"] != 0:
            print(f"          외인: US {fd['us']:+,} | EU {fd['eu']:+,} | ASIA {fd['asia']:+,} | 기타 {fd['other']:+,}")
    print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # CLI 모드: --dry-run (변환만) / --upload (업로드까지)
    dry_run = "--upload" not in sys.argv

    rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
    if not rec_path.exists():
        print(f"recommendation.json 없음: {rec_path}")
        sys.exit(1)

    rec_data = json.loads(rec_path.read_text("utf-8"))
    print(f"recommendation.json 로드: {len(rec_data.get('stocks', []))}종목, "
          f"전쟁릴레이 {len(rec_data.get('war_relay_stocks', []))}종목")

    # 국적 데이터 로드 시도
    nat_daily_all = None
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from data.nationality_profiler import collect_daily_series
        codes = [s["code"] for s in rec_data.get("stocks", [])]
        codes += [w["code"] for w in rec_data.get("war_relay_stocks", [])[:5]]
        codes = list(set(codes))
        if codes:
            print(f"국적별 수급 데이터 수집 중... ({len(codes)}종목)")
            nat_daily_all = collect_daily_series(codes, n_days=5)
            print(f"국적 데이터 수집 완료: {len(nat_daily_all)}종목")
    except Exception as e:
        print(f"국적 데이터 수집 실패 (외인 상세 없이 진행): {e}")

    all_signals = convert_recommendation_to_flowx(rec_data, nat_daily_all)

    # 듀얼 출력: 전체 (개인용) + 필터 (FLOWX 구독자용)
    print(f"\n{'='*80}")
    print(f"  [채널1] 텔레그램 개인용 — 전체 {len(all_signals)}개 종목")
    print(f"{'='*80}")
    _print_signals(all_signals)

    flowx_signals = _apply_flowx_filter(all_signals)
    print(f"{'='*80}")
    print(f"  [채널2] FLOWX 구독자용 — 필터 통과 {len(flowx_signals)}개 종목")
    print(f"  (A등급+ / 65점+ / BUY+ / 기관미동반시 75점+)")
    print(f"{'='*80}")
    if flowx_signals:
        _print_signals(flowx_signals)
    else:
        print("  → 필터 통과 종목 없음 (0종목도 정상 — 기준 미달 시 업로드 스킵)\n")

    if dry_run:
        print("[DRY-RUN] --upload 옵션으로 실행하면 Supabase에 업로드합니다.")
    else:
        success = upload_short_signals(flowx_signals) if flowx_signals else True
        if success:
            print(f"Supabase 업로드 완료! ({len(flowx_signals)}종목)")
        else:
            print("업로드 실패 — .env의 SUPABASE_URL, SUPABASE_KEY 확인하세요.")

        # nationality_flows 업로드
        print(f"\n{'='*80}")
        print("  nationality_flows 업로드")
        print(f"{'='*80}")
        nat_ok = upload_nationality_flows()
        print(f"  → {'성공' if nat_ok else '실패/스킵'}")
