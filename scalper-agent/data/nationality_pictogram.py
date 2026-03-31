"""외국인 국적별 수급 픽토그램 차트 생성기

사람 아이콘 백분율 차트로 어떤 나라가 얼마큼 샀다(빨강) / 팔았다(파랑) 시각화.
총 주식수 비례 스케일링. PIL 기반 이미지 생성 → Telegram / Supabase 전송.
"""
import logging
import json
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger("BH.NatChart")

DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"

# ── 색상 ──
COLOR_BG = (255, 255, 255)
COLOR_BUY = (220, 38, 38)       # 빨강 (매수)
COLOR_SELL = (37, 99, 235)      # 파랑 (매도)
COLOR_HEADER = (30, 30, 30)
COLOR_SUB = (100, 100, 100)
COLOR_LINE = (220, 220, 220)
COLOR_GRAY = (180, 180, 180)

# 분류별 색상
CATEGORY_COLORS = {
    "기관":    (89, 50, 160),    # 보라
    "헤지펀드": (0, 128, 128),   # 청록
    "아시아":  (200, 120, 0),    # 주황
    "북미":    (0, 100, 180),    # 남색
    "기타":    (120, 120, 120),  # 회색
}

# 분류별 이모지 (텍스트 렌더링 대체)
CATEGORY_LABEL = {
    "기관":    "[기관]",
    "헤지펀드": "[헤지]",
    "아시아":  "[아시아]",
    "북미":    "[북미]",
    "기타":    "[기타]",
}

# ── 일반인용 투자 타입 설명 (차트 서브텍스트용 — 짧게) ──
TYPE_DESC = {
    "기관":    "장기 패시브",
    "헤지펀드": "단기 차익",
    "아시아":  "추세 추종",
    "북미":    "글로벌 방향",
    "기타":    "기타",
}

# ── 행동 패턴 표시 ──
PATTERN_DISPLAY = {
    "매집중":   ("매집중 ↗", COLOR_BUY),
    "이탈중":   ("이탈중 ↘", COLOR_SELL),
    "꾸준유지": ("꾸준유지 →", (0, 150, 80)),
    "단타형":   ("단타형 ⟷", (200, 120, 0)),
    "관망":     ("관망 ●", (150, 150, 150)),
    "신규진입": ("신규진입 ★", (180, 0, 180)),
    "일반매매": ("일반매매", (120, 120, 120)),
}

# ── 폰트 로더 ──
_font_cache = {}

def _get_font(size: int, bold: bool = False):
    """크로스 플랫폼 한글 폰트 로더 (Windows + Ubuntu VPS)"""
    from PIL import ImageFont
    key = (size, bold)
    if key in _font_cache:
        return _font_cache[key]

    font_paths = [
        # Windows
        "C:/Windows/Fonts/malgunbd.ttf" if bold else "C:/Windows/Fonts/malgun.ttf",
        "C:/Windows/Fonts/gulim.ttc",
        # Ubuntu (VPS)
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf" if bold else
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for fp in font_paths:
        try:
            f = ImageFont.truetype(fp, size)
            _font_cache[key] = f
            return f
        except (OSError, IOError):
            continue
    f = ImageFont.load_default()
    _font_cache[key] = f
    return f


# ═══════════════════════════════════════
#  상장주식수 조회
# ═══════════════════════════════════════

def get_total_shares(code: str, close_price: int = 0) -> int:
    """총 상장주식수 조회 (3-tier fallback)

    Tier1: short_bal.csv → 상장주식수 컬럼 (정확)
    Tier2: cap_억 * 1억 / close (근사값)
    Tier3: 기본값 반환
    """
    # Tier 1: short_bal.csv
    try:
        import pandas as pd
        csv_path = DATA_STORE / "short" / f"{code}_short_bal.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, encoding="utf-8")
            if "상장주식수" in df.columns and len(df) > 0:
                val = int(df["상장주식수"].iloc[-1])
                if val > 0:
                    return val
    except Exception:
        pass

    # Tier 2: universe.json cap_억 기반 계산
    try:
        uni_path = DATA_STORE / "universe.json"
        with open(uni_path, "r", encoding="utf-8") as f:
            universe = json.load(f)
        info = universe.get(code, {})
        cap_억 = info.get("cap_억", 0)
        if cap_억 > 0 and close_price > 0:
            return int(cap_억 * 100_000_000 / close_price)
    except Exception:
        pass

    # Tier 3: 기본값 (데이터 없음)
    logger.warning(f"[NatChart] {code}: 상장주식수 조회 실패")
    return 0


# ═══════════════════════════════════════
#  사람 아이콘 렌더링
# ═══════════════════════════════════════

def _draw_person(draw, x, y, size, color):
    """사람 아이콘 (머리 원 + 몸통 사다리꼴) 렌더링"""
    # 머리: 원
    head_r = size // 5
    cx = x + size // 2
    cy = y + head_r + 1
    draw.ellipse(
        [cx - head_r, cy - head_r, cx + head_r, cy + head_r],
        fill=color,
    )
    # 몸통: 사다리꼴
    shoulder_y = cy + head_r + 2
    foot_y = y + size - 1
    sw = size * 0.3   # 어깨 반폭
    fw = size * 0.35  # 발 반폭
    draw.polygon(
        [
            (cx - sw, shoulder_y),
            (cx + sw, shoulder_y),
            (cx + fw, foot_y),
            (cx - fw, foot_y),
        ],
        fill=color,
    )


def _format_volume(vol: int) -> str:
    """거래량 → 읽기 좋은 한국어 형식"""
    abs_v = abs(vol)
    sign = "+" if vol > 0 else "-" if vol < 0 else ""
    if abs_v >= 10000:
        return f"{sign}{abs_v / 10000:.1f}만"
    elif abs_v >= 1000:
        return f"{sign}{abs_v / 1000:.1f}천"
    else:
        return f"{sign}{abs_v}"


def _format_shares(shares: int) -> str:
    """총 주식수 → 억/만 형식"""
    if shares >= 100_000_000:
        return f"{shares / 100_000_000:.1f}억주"
    elif shares >= 10_000:
        return f"{shares / 10_000:.0f}만주"
    else:
        return f"{shares:,}주"


# ═══════════════════════════════════════
#  픽토그램 차트 생성
# ═══════════════════════════════════════

def generate_pictogram_chart(
    code: str,
    name: str,
    changes: List[dict],
    total_shares: int,
    date_new: str,
    date_old: str,
    profiles: Optional[List[dict]] = None,
) -> BytesIO:
    """PIL 기반 픽토그램 차트 생성

    Args:
        code: 종목코드
        name: 종목명
        changes: compare_nationality() 결과
        total_shares: 상장주식수
        date_new: 금일 YYYYMMDD
        date_old: 전일 YYYYMMDD
        profiles: analyze_nationality_behavior() 결과 (투자성향+패턴 표시용)

    Returns:
        BytesIO: PNG 이미지
    """
    from PIL import Image, ImageDraw

    # 변화량 0 제외 + 상위 12개국만
    filtered = [c for c in changes if c["변화"] != 0]
    filtered.sort(key=lambda x: abs(x["변화"]), reverse=True)
    filtered = filtered[:12]

    if not filtered:
        # 변화 없으면 간단 이미지
        img = Image.new("RGB", (800, 200), COLOR_BG)
        draw = ImageDraw.Draw(img)
        font = _get_font(20, bold=True)
        draw.text((30, 80), f"{name}({code}) - 국적별 변화 없음", fill=COLOR_HEADER, font=font)
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf

    # 프로파일 국가→패턴 매핑
    profile_map = {}
    if profiles:
        for p in profiles:
            profile_map[p["국가"]] = p

    # ── 레이아웃 계산 ──
    MARGIN = 30
    HEADER_H = 80
    ROW_H = 68 if profiles else 52  # 성향 표시 시 행 높이 증가
    LEGEND_H = 70
    PERSON_SIZE = 28
    PERSON_GAP = 4
    MAX_PERSONS = 20
    LABEL_W = 200  # 국가명+분류 영역
    STATS_W = 180  # 수치 영역

    n_rows = len(filtered)
    img_w = 800
    img_h = HEADER_H + n_rows * ROW_H + LEGEND_H + MARGIN

    img = Image.new("RGB", (img_w, img_h), COLOR_BG)
    draw = ImageDraw.Draw(img)

    # ── 헤더 ──
    font_title = _get_font(22, bold=True)
    font_sub = _get_font(14)
    font_row = _get_font(15)
    font_row_b = _get_font(15, bold=True)
    font_legend = _get_font(13)
    font_pct = _get_font(12)
    font_trait = _get_font(11)  # 투자성향 서브텍스트

    dn = f"{date_new[:4]}/{date_new[4:6]}/{date_new[6:]}" if len(date_new) == 8 else date_new
    do = f"{date_old[4:6]}/{date_old[6:]}" if len(date_old) == 8 else date_old
    shares_str = _format_shares(total_shares) if total_shares > 0 else "N/A"

    draw.text((MARGIN, 15), f"{name}({code}) 외국인 국적별 수급", fill=COLOR_HEADER, font=font_title)
    draw.text((MARGIN, 48), f"{dn} vs {do}  |  총 주식수: {shares_str}", fill=COLOR_SUB, font=font_sub)

    # 구분선
    draw.line([(MARGIN, HEADER_H - 5), (img_w - MARGIN, HEADER_H - 5)], fill=COLOR_LINE, width=2)

    # ── 스케일링 계산 ──
    max_abs_change = max(abs(c["변화"]) for c in filtered)
    if max_abs_change == 0:
        max_abs_change = 1

    # 총 주식수 비례: 1인당 주식수
    if total_shares > 0:
        shares_per_person = total_shares / (MAX_PERSONS * 50)  # 표시용 스케일
    else:
        shares_per_person = max_abs_change / MAX_PERSONS

    # ── 카테고리별 합산 ──
    cat_totals = {}
    for c in changes:
        cat = c["분류"]
        cat_totals[cat] = cat_totals.get(cat, 0) + c["변화"]

    # ── 국가별 행 렌더링 ──
    y = HEADER_H
    for c in filtered:
        country = c["국가"]
        delta = c["변화"]
        cat = c["분류"]
        pct = c["변화율"]

        # 색상
        color = COLOR_BUY if delta > 0 else COLOR_SELL
        cat_color = CATEGORY_COLORS.get(cat, COLOR_GRAY)

        # 사람 수 계산 (총 주식수 비례)
        n_persons = max(1, round(abs(delta) / max_abs_change * MAX_PERSONS))

        # 국가 라벨 (1행)
        cat_label = CATEGORY_LABEL.get(cat, "")
        label_text = f"{cat_label} {country}"
        draw.text((MARGIN, y + 4), label_text, fill=cat_color, font=font_row_b)

        # 투자 성향 + 패턴 서브텍스트 (2행)
        if profiles:
            prof = profile_map.get(country)
            type_desc = TYPE_DESC.get(cat, "")
            if prof:
                pattern = prof.get("패턴", "")
                pred_score = prof.get("예측점수", 0)
                pat_display, pat_color = PATTERN_DISPLAY.get(pattern, (pattern, COLOR_GRAY))
                # 타입 설명
                draw.text((MARGIN + 8, y + 22), type_desc, fill=COLOR_SUB, font=font_trait)
                # 패턴 (정확한 텍스트 너비 계산)
                try:
                    tw = font_trait.getlength(type_desc)
                except AttributeError:
                    tw = len(type_desc) * 7  # PIL 구버전 fallback
                pat_x = MARGIN + 8 + int(tw) + 8
                draw.text((pat_x, y + 22), pat_display, fill=pat_color, font=font_trait)
                # 예측 점수
                if abs(pred_score) >= 1:
                    try:
                        pw = font_trait.getlength(pat_display)
                    except AttributeError:
                        pw = len(pat_display) * 7
                    score_color = COLOR_BUY if pred_score > 0 else COLOR_SELL
                    draw.text(
                        (pat_x + int(pw) + 4, y + 22),
                        f"({pred_score:+.0f})",
                        fill=score_color, font=font_trait,
                    )
            else:
                # 프로파일 없는 국가 → 타입만 표시
                type_desc = TYPE_DESC.get(cat, "")
                if type_desc:
                    draw.text((MARGIN + 8, y + 22), type_desc, fill=COLOR_SUB, font=font_trait)

        # 사람 아이콘 그리기
        icon_y_offset = 2 if not profiles else 8
        icon_x = MARGIN + LABEL_W
        for i in range(n_persons):
            px = icon_x + i * (PERSON_SIZE + PERSON_GAP)
            if px + PERSON_SIZE > img_w - STATS_W - MARGIN:
                break  # 넘치면 중단
            _draw_person(draw, px, y + icon_y_offset, PERSON_SIZE, color)

        # 수치 표시
        stats_x = img_w - STATS_W - MARGIN + 10
        vol_str = _format_volume(delta)
        direction = "매집" if delta > 0 else "이탈"
        if abs(pct) >= 200:
            direction = "급증" if delta > 0 else "급감"
        elif c["전일"] == 0 and delta > 0:
            direction = "신규"

        draw.text((stats_x, y + 5), f"{vol_str}주", fill=color, font=font_row_b)

        pct_str = f"({pct:+.0f}%)" if c["전일"] > 0 else "(NEW)"
        draw.text((stats_x, y + 25), f"{pct_str} {direction}", fill=COLOR_SUB, font=font_pct)

        # 총 주식수 대비 % 표시
        if total_shares > 0:
            pct_total = abs(delta) / total_shares * 100
            if pct_total >= 0.01:
                draw.text((stats_x + 110, y + 5), f"{pct_total:.2f}%", fill=COLOR_SUB, font=font_pct)

        # 예측 점수 표시 (수치 영역 하단)
        if profiles:
            prof = profile_map.get(country)
            if prof and abs(prof.get("예측점수", 0)) >= 1:
                pred = prof["예측점수"]
                pred_color = COLOR_BUY if pred > 0 else COLOR_SELL
                draw.text(
                    (stats_x, y + 45),
                    f"내일예측: {pred:+.0f}점",
                    fill=pred_color, font=font_trait,
                )

        # 행 구분선
        draw.line(
            [(MARGIN, y + ROW_H - 2), (img_w - MARGIN, y + ROW_H - 2)],
            fill=(240, 240, 240), width=1,
        )
        y += ROW_H

    # ── 범례 ──
    legend_y = y + 10
    draw.line([(MARGIN, legend_y - 5), (img_w - MARGIN, legend_y - 5)], fill=COLOR_LINE, width=2)

    # 매수/매도 범례
    _draw_person(draw, MARGIN, legend_y + 2, 20, COLOR_BUY)
    draw.text((MARGIN + 25, legend_y + 2), "= 매수", fill=COLOR_SUB, font=font_legend)
    _draw_person(draw, MARGIN + 90, legend_y + 2, 20, COLOR_SELL)
    draw.text((MARGIN + 115, legend_y + 2), "= 매도", fill=COLOR_SUB, font=font_legend)

    if total_shares > 0:
        per_person_shares = max_abs_change / MAX_PERSONS
        draw.text(
            (MARGIN + 190, legend_y + 2),
            f"| 1인 = {_format_volume(int(per_person_shares))}주 (총주식수의 {per_person_shares/total_shares*100:.3f}%)",
            fill=COLOR_SUB, font=font_legend,
        )

    # 카테고리별 합산
    cat_y = legend_y + 28
    cat_x = MARGIN
    for cat_name, total in sorted(cat_totals.items(), key=lambda x: abs(x[1]), reverse=True):
        if total == 0:
            continue
        cat_color = CATEGORY_COLORS.get(cat_name, COLOR_GRAY)
        sign_color = COLOR_BUY if total > 0 else COLOR_SELL
        text = f"{cat_name} {_format_volume(total)}주"
        draw.text((cat_x, cat_y), text, fill=sign_color, font=font_row)
        cat_x += len(text) * 10 + 30
        if cat_x > img_w - 100:
            break

    # PNG 저장
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


def generate_charts_batch(
    codes: List[str],
    code_names: Dict[str, str],
) -> Dict[str, BytesIO]:
    """여러 종목 차트 일괄 생성 (투자성향+패턴 포함)"""
    from data.nationality_signal import compare_nationality

    # ── 프로파일러로 국가별 행동 패턴 일괄 분석 ──
    # 캐시된 CSV 우선 사용 → KRX API 최소화
    # 200종목 5일치 CSV가 있으면 ~10초, 없으면 KRX API 호출 → 타임아웃 300초
    all_profiles = {}
    try:
        import signal as _sig
        from data.nationality_profiler import analyze_nationality_behavior

        import threading
        result_box = [{}]
        error_box = [None]

        def _run_profile():
            try:
                result_box[0] = analyze_nationality_behavior(
                    codes, n_days=5, code_names=code_names,
                )
            except Exception as e:
                error_box[0] = e

        t = threading.Thread(target=_run_profile, daemon=True)
        t.start()
        t.join(timeout=300)  # 5분 타임아웃

        if t.is_alive():
            logger.warning("[NatChart] 프로파일 분석 타임아웃(300초) — 기본모드로 진행")
        elif error_box[0]:
            raise error_box[0]
        else:
            all_profiles = result_box[0]
            logger.info(f"[NatChart] 프로파일 분석 완료: {len(all_profiles)}종목")
    except Exception as e:
        logger.warning(f"[NatChart] 프로파일 분석 실패 (차트는 기본모드): {e}")

    results = {}
    for code in codes:
        name = code_names.get(code, code)
        try:
            changes = compare_nationality(code)
            if not changes:
                logger.info(f"[NatChart] {name}: 국적 데이터 없음, 스킵")
                continue

            # 종가 조회 (recommendation.json에서)
            close_price = 0
            try:
                rec_path = DATA_STORE / "recommendation.json"
                with open(rec_path, "r", encoding="utf-8") as f:
                    rec = json.load(f)
                for s in rec.get("stocks", []):
                    if s.get("code") == code:
                        close_price = s.get("close", 0)
                        break
            except Exception:
                pass

            total_shares = get_total_shares(code, close_price)

            # 날짜 정보 추출 (파일 기반 fallback 포함)
            from data.nationality_signal import (
                _get_latest_data_date, _find_prev_trading_day,
                _find_latest_snapshot_date,
            )
            date_new = _get_latest_data_date()
            # G6(16:30 T-1 수집) vs G7 NatChart(21:00 T 조회) 날짜 불일치 방지
            fallback = _find_latest_snapshot_date()
            if fallback and fallback != date_new:
                date_new = fallback
            date_old = _find_prev_trading_day(date_new) if date_new else ""

            chart = generate_pictogram_chart(
                code, name, changes, total_shares,
                date_new or "", date_old or "",
                profiles=all_profiles.get(code),
            )
            results[code] = chart
            logger.info(f"[NatChart] {name}: 차트 생성 완료 (프로파일 {'O' if code in all_profiles else 'X'})")
        except Exception as e:
            logger.warning(f"[NatChart] {name}: 차트 생성 실패 - {e}")

    return results


# ═══════════════════════════════════════
#  Supabase 업로드
# ═══════════════════════════════════════

def upload_chart_to_supabase(
    code: str,
    name: str,
    date_str: str,
    image_bytes: BytesIO,
    metadata: dict,
) -> Optional[str]:
    """Supabase Storage 업로드 + nationality_charts 테이블 upsert"""
    import os
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        return None

    try:
        from supabase import create_client
        client = create_client(url, key)

        # Storage 업로드
        storage_path = f"{date_str}/{code}.png"
        image_bytes.seek(0)
        client.storage.from_("nationality-charts").upload(
            storage_path,
            image_bytes.read(),
            {"content-type": "image/png", "upsert": "true"},
        )
        image_url = f"{url}/storage/v1/object/public/nationality-charts/{storage_path}"

        # 테이블 upsert
        row = {
            "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
            "code": code,
            "name": name,
            "image_url": image_url,
            **metadata,
        }
        client.table("nationality_charts").upsert(
            row, on_conflict="date,code"
        ).execute()

        logger.info(f"[NatChart] Supabase 업로드: {name} → {storage_path}")
        return image_url
    except Exception as e:
        logger.debug(f"[NatChart] Supabase 업로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    import glob as _glob
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    code = sys.argv[1] if len(sys.argv) > 1 else "005930"

    # 종목명 조회
    try:
        with open(DATA_STORE / "universe.json", "r", encoding="utf-8") as f:
            uni = json.load(f)
        name = uni.get(code, {}).get("name", code)
    except Exception:
        name = code

    from data.nationality_signal import compare_nationality, _find_prev_trading_day

    # CLI 날짜 인자 또는 파일 기반 최신 날짜 자동 탐색
    date_new = sys.argv[2] if len(sys.argv) > 2 else None
    if not date_new:
        nat_dir = DATA_STORE / "nationality"
        files = sorted(_glob.glob(str(nat_dir / f"{code}_*.csv")))
        if files:
            date_new = Path(files[-1]).stem.split("_")[-1]
        else:
            print(f"국적 데이터 파일 없음: {nat_dir / code}_*.csv")
            sys.exit(1)

    date_old = _find_prev_trading_day(date_new)

    print(f"종목: {name}({code})")
    print(f"날짜: {date_new} vs {date_old}")

    changes = compare_nationality(code, date_new, date_old)
    if not changes:
        print("국적 데이터 없음")
        sys.exit(1)

    total_shares = get_total_shares(code)
    print(f"상장주식수: {total_shares:,}")
    print(f"국가 수: {len(changes)}, 변화 국가: {sum(1 for c in changes if c['변화'] != 0)}")

    # 프로파일 분석 (투자성향+패턴)
    profiles = None
    try:
        from data.nationality_profiler import analyze_nationality_behavior
        all_profiles = analyze_nationality_behavior([code], n_days=5)
        profiles = all_profiles.get(code)
        if profiles:
            print(f"프로파일: {len(profiles)}개국 분석 완료")
            for p in profiles[:5]:
                print(f"  {p['이모지']}{p['국가']}: {p['패턴']} (예측{p['예측점수']:+.0f})")
    except Exception as e:
        print(f"프로파일 분석 스킵: {e}")

    chart = generate_pictogram_chart(
        code, name, changes, total_shares,
        date_new or "", date_old or "",
        profiles=profiles,
    )

    # 파일 저장
    out_path = DATA_STORE / f"nationality_chart_{code}.png"
    with open(out_path, "wb") as f:
        f.write(chart.read())
    print(f"차트 저장: {out_path}")
    print(f"크기: {out_path.stat().st_size:,} bytes")
