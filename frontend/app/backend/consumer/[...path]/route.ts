import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const consumerApiBaseUrl = process.env.CONSUMER_API_BASE_URL || "http://localhost:8081";

export async function GET(request: NextRequest) {
  return proxyRequest(request, "/backend/consumer", consumerApiBaseUrl);
}

async function proxyRequest(request: NextRequest, prefix: string, baseUrl: string) {
  const path = request.nextUrl.pathname.replace(prefix, "");
  const target = `${baseUrl}${path}${request.nextUrl.search}`;
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
