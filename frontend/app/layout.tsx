import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "GitHub Pulse Operations",
    template: "%s | GitHub Pulse",
  },
  description:
    "Production-style operational dashboard for GitHub event streaming analytics, realtime Redis rankings, PostgreSQL history, and Prometheus/Grafana observability.",
  metadataBase: new URL("https://github-pulse.local"),
  openGraph: {
    title: "GitHub Pulse Operations",
    description:
      "Event-driven GitHub analytics platform with Redpanda, Redis, PostgreSQL, Prometheus, Grafana, and SSE dashboards.",
    type: "website",
  },
};

const navItems = [
  { href: "/", label: "System Overview" },
  { href: "/trending", label: "Trending Repositories" },
  { href: "/events", label: "Live Event Stream" },
  { href: "/metrics", label: "System Metrics" },
  { href: "/grafana/", label: "Grafana" },
  { href: "/prometheus/", label: "Prometheus" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="shell">
          <aside className="sidebar">
            <div className="brand">
              <strong>GitHub Pulse</strong>
              <span>Operational Analytics</span>
            </div>
            <nav className="nav">
              {navItems.map((item) => (
                <Link href={item.href} key={item.href}>
                  {item.label}
                </Link>
              ))}
            </nav>
          </aside>
          <main className="main">{children}</main>
        </div>
      </body>
    </html>
  );
}
