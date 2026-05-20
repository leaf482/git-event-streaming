import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "GitHub Pulse Operations",
  description: "Operational dashboard for GitHub event streaming analytics.",
};

const navItems = [
  { href: "/", label: "System Overview" },
  { href: "/trending", label: "Trending Repositories" },
  { href: "/events", label: "Live Event Stream" },
  { href: "/metrics", label: "System Metrics" },
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
