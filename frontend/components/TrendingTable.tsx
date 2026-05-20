import type { RepoTrend } from "../lib/types";

type TrendingTableProps = {
  repos: RepoTrend[];
};

export function TrendingTable({ repos }: TrendingTableProps) {
  const maxScore = Math.max(...repos.map((repo) => repo.score), 1);

  return (
    <table className="table">
      <thead>
        <tr>
          <th>Rank</th>
          <th>Repository</th>
          <th>Score</th>
          <th>Activity</th>
        </tr>
      </thead>
      <tbody>
        {repos.map((repo) => (
          <tr key={repo.repo_name}>
            <td>#{repo.rank}</td>
            <td>{repo.repo_name}</td>
            <td>{repo.score}</td>
            <td>
              <div className="bar" aria-label={`${repo.repo_name} score ${repo.score}`}>
                <span style={{ width: `${Math.max(6, (repo.score / maxScore) * 100)}%` }} />
              </div>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
