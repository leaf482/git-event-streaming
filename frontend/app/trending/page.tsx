"use client";

import { useEffect, useState } from "react";
import { StatCard } from "../../components/StatCard";
import { TrendingTable } from "../../components/TrendingTable";
import type { TrendingResponse, RepoTrend } from "../../lib/types";

export default function TrendingPage() {
  const [repos, setRepos] = useState<RepoTrend[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    fetch("/backend/consumer/api/trending/repos?limit=15")
      .then((response) => response.json())
      .then((data: TrendingResponse) => setRepos(data.repos ?? []))
      .catch(() => setRepos([]));

    const source = new EventSource("/backend/consumer/api/trending/repos/stream?limit=15");
    source.addEventListener("open", () => setConnected(true));
    source.addEventListener("error", () => setConnected(false));
    source.addEventListener("trending", (event) => {
      const data = JSON.parse(event.data) as TrendingResponse;
      setRepos(data.repos ?? []);
    });

    return () => source.close();
  }, []);

  const leader = repos[0];
  const totalScore = repos.reduce((sum, repo) => sum + repo.score, 0);

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Trending Repositories</h1>
          <p className="muted">Redis sorted-set ranking updated from idempotent Kafka consumer processing.</p>
        </div>
        <span className={`status ${connected ? "" : "warn"}`}>SSE {connected ? "connected" : "reconnecting"}</span>
      </header>

      <section className="grid cols-3">
        <StatCard label="Tracked repositories" value={String(repos.length)} />
        <StatCard label="Top repository" value={leader?.repo_name ?? "waiting"} detail={leader ? `score ${leader.score}` : undefined} />
        <StatCard label="Visible score total" value={String(totalScore)} />
      </section>

      <section className="panel" style={{ marginTop: 16 }}>
        <h2>Live Ranking</h2>
        <TrendingTable repos={repos} />
      </section>
    </>
  );
}
