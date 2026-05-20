import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const ingestorApiBaseUrl = process.env.INGESTOR_API_BASE_URL || "http://localhost:8080";

export async function GET(request: NextRequest) {
  const path = request.nextUrl.pathname.replace("/backend/ingestor", "");
  const target = `${ingestorApiBaseUrl}${path}${request.nextUrl.search}`;
  const response = await fetch(target, {
    headers: {
      Accept: request.headers.get("accept") || "*/*",
    },
    cache: "no-store",
  });

  return new Response(response.body, {
    status: response.status,
    headers: {
      "Content-Type": response.headers.get("content-type") || "application/octet-stream",
      "Cache-Control": response.headers.get("cache-control") || "no-cache",
    },
  });
}
