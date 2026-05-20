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

export type HistoricalTrend = {
  rank: number;
  repo_name: string;
  score: number;
  event_count: number;
  last_event_at: string;
};

export type HistoricalTrendingResponse = {
  repos: HistoricalTrend[];
};

export type RepoHistoryPoint = {
  window_start: string;
  window_end: string;
  score: number;
  event_count: number;
  rank: number;
};

export type RepoHistoryResponse = {
  repo_name: string;
  points: RepoHistoryPoint[];
};
