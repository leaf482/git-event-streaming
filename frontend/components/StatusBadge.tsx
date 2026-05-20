type StatusBadgeProps = {
  healthy: boolean;
  label: string;
};

export function StatusBadge({ healthy, label }: StatusBadgeProps) {
  return <span className={`status ${healthy ? "" : "warn"}`}>{label}: {healthy ? "ready" : "checking"}</span>;
}
