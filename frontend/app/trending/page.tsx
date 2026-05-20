"use client";

import { useEffect, useState } from "react";
import { StatCard } from "../../components/StatCard";
import { TrendingTable } from "../../components/TrendingTable";
import type { HistoricalTrend, HistoricalTrendingResponse, RepoHistoryPoint, RepoHistoryResponse, TrendingResponse, RepoTrend } from "../../lib/types";

export default function TrendingPage() {
  const [repos, setRepos] = useState<RepoTrend[]>([]);
  const [historicalRepos, setHistoricalRepos] = useState<HistoricalTrend[]>([]);
  const [selectedRepo, setSelectedRepo] = useState("");
  const [historyPoints, setHistoryPoints] = useState<RepoHistoryPoint[]>([]);
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

  useEffect(() => {
    fetch("/backend/consumer/api/history/trending/repos?limit=10")
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error("history unavailable"))))
      .then((data: HistoricalTrendingResponse) => {
        const nextRepos = data.repos ?? [];
        setHistoricalRepos(nextRepos);
        setSelectedRepo((current) => current || nextRepos[0]?.repo_name || "");
      })
      .catch(() => setHistoricalRepos([]));
  }, []);

  useEffect(() => {
    if (!selectedRepo || !selectedRepo.includes("/")) {
      setHistoryPoints([]);
      return;
    }

    fetch(`/backend/consumer/api/history/repos/${selectedRepo}?limit=12`)
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error("repo history unavailable"))))
      .then((data: RepoHistoryResponse) => setHistoryPoints((data.points ?? []).slice().reverse()))
      .catch(() => setHistoryPoints([]));
  }, [selectedRepo]);

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

      <section className="grid cols-2" style={{ marginTop: 16 }}>
        <div className="panel">
          <h2>Historical Trending</h2>
          <table className="table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Repository</th>
                <th>Score</th>
                <th>Events</th>
              </tr>
            </thead>
            <tbody>
              {historicalRepos.map((repo) => (
                <tr key={repo.repo_name} onClick={() => setSelectedRepo(repo.repo_name)} className={repo.repo_name === selectedRepo ? "selected-row" : ""}>
                  <td>#{repo.rank}</td>
                  <td>{repo.repo_name}</td>
                  <td>{repo.score}</td>
                  <td>{repo.event_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <h2>Repository History</h2>
          <p className="muted">{selectedRepo || "waiting for historical data"}</p>
          <div className="history-chart" aria-label="Historical repository score chart">
            {historyPoints.map((point) => {
              const maxScore = Math.max(...historyPoints.map((item) => item.score), 1);
              const height = Math.max((point.score / maxScore) * 100, 4);
              return (
                <div key={point.window_start} className="history-bar" title={`${new Date(point.window_start).toLocaleString()} score ${point.score}`}>
                  <span style={{ height: `${height}%` }} />
                </div>
              );
            })}
          </div>
        </div>
      </section>
    </>
  );
}
