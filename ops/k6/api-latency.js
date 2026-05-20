import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    api_latency: {
      executor: "constant-vus",
      vus: Number(__ENV.K6_VUS || 10),
      duration: __ENV.K6_DURATION || "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(95)<500", "p(99)<1000"],
  },
};

const baseUrl = __ENV.BASE_URL || "http://caddy";

export default function () {
  const responses = http.batch([
    ["GET", `${baseUrl}/api/trending/repos?limit=10`],
    ["GET", `${baseUrl}/api/events/recent?limit=10`],
    ["GET", `${baseUrl}/api/history/trending/repos?limit=10`],
    ["GET", `${baseUrl}/api/history/windows?limit=10`],
  ]);

  for (const response of responses) {
    check(response, {
      "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
  }

  sleep(1);
}
