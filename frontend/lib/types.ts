export type RepoTrend = {
  rank: number;
  repo_name: string;
  score: number;
};

export type TrendingResponse = {
  repos: RepoTrend[];
};

export type RecentEvent = {
  event_id: string;
  event_type: string;
  repo_name: string;
  actor_login: string;
  score: number;
  created_at: string;
  processed_at: string;
};

export type RecentEventsResponse = {
  events: RecentEvent[];
};

export type MetricsMap = Record<string, number>;
