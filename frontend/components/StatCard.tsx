type StatCardProps = {
  label: string;
  value: string;
  detail?: string;
};

export function StatCard({ label, value, detail }: StatCardProps) {
  return (
    <section className="panel stat">
      <span className="label">{label}</span>
      <span className="value">{value}</span>
      {detail ? <span className="muted">{detail}</span> : null}
    </section>
  );
}
