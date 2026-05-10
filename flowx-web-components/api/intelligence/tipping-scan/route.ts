/**
 * 수급 임계점 스캐너 API
 * GET /api/intelligence/tipping-scan
 *
 * intelligence_tipping_scan 테이블에서 최신 1건 조회.
 * 코일/점화/이륙 3카테고리 종목 반환.
 */
import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
);

export const revalidate = 300; // 5분 캐시

export async function GET() {
  try {
    const { data, error } = await supabase
      .from("intelligence_tipping_scan")
      .select("*")
      .order("date", { ascending: false })
      .limit(1)
      .single();

    if (error) {
      console.error("[tipping-scan] Supabase error:", error.message);
      return NextResponse.json(
        { date: "", coiled: [], warming: [], launched: [], coiled_count: 0, warming_count: 0, launched_count: 0 },
        { status: 200 },
      );
    }

    return NextResponse.json(data);
  } catch (err) {
    console.error("[tipping-scan] Unexpected error:", err);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 },
    );
  }
}
