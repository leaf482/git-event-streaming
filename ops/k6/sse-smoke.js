import { check } from "k6";
import http from "k6/http";

export const options = {
  scenarios: {
    sse_clients: {
      executor: "constant-vus",
      vus: Number(__ENV.K6_VUS || 5),
      duration: __ENV.K6_DURATION || "20s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.10"],
  },
};

const baseUrl = __ENV.BASE_URL || "http://caddy";

export default function () {
  const response = http.get(`${baseUrl}/api/events/stream?limit=1&once=true`, {
    timeout: __ENV.SSE_TIMEOUT || "10s",
    responseType: "text",
  });

  check(response, {
    "sse returned data": (r) => r.status === 200 && r.body.includes("event:"),
  });
}
