# -*- coding: utf-8 -*-
"""Body Hunter v3 바탕화면 아이콘 생성"""
from PIL import Image, ImageDraw, ImageFont
import struct, io, os

def create_icon(output_path):
    sizes = [256, 64, 48, 32, 16]
    images = []

    for sz in sizes:
        img = Image.new("RGBA", (sz, sz), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # 배경 원 (진한 보라 그라데이션 느낌)
        margin = int(sz * 0.05)
        draw.ellipse(
            [margin, margin, sz - margin, sz - margin],
            fill=(88, 28, 135),  # 진보라
        )

        # 안쪽 원 (밝은 보라)
        m2 = int(sz * 0.15)
        draw.ellipse(
            [m2, m2, sz - m2, sz - m2],
            fill=(124, 58, 237),  # 보라
        )

        # 수정구슬 하이라이트
        m3 = int(sz * 0.2)
        m4 = int(sz * 0.5)
        draw.ellipse(
            [m3, m3, m4, m4],
            fill=(167, 139, 250, 120),  # 반투명 밝은 보라
        )

        # 가운데 "BH" 텍스트
        try:
            font_size = max(int(sz * 0.35), 8)
            font = ImageFont.truetype("C:/Windows/Fonts/arialbd.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()

        text = "BH"
        bbox = draw.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        tx = (sz - tw) // 2
        ty = (sz - th) // 2 - int(sz * 0.02)
        draw.text((tx, ty), text, fill=(255, 255, 255), font=font)

        # 아래에 "v3" 작은 텍스트
        try:
            small_size = max(int(sz * 0.18), 6)
            small_font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", small_size)
        except Exception:
            small_font = ImageFont.load_default()

        v3_text = "v3"
        bbox2 = draw.textbbox((0, 0), v3_text, font=small_font)
        vw = bbox2[2] - bbox2[0]
        vx = (sz - vw) // 2
        vy = ty + th + int(sz * 0.02)
        if vy + (bbox2[3] - bbox2[1]) < sz - margin:
            draw.text((vx, vy), v3_text, fill=(216, 180, 254), font=small_font)

        images.append(img)

    # ICO 파일 저장
    images[0].save(
        output_path,
        format="ICO",
        sizes=[(s, s) for s in sizes],
        append_images=images[1:],
    )
    print(f"Icon saved: {output_path}")


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.dirname(__file__)), "body_hunter_v3.ico")
    create_icon(out)
