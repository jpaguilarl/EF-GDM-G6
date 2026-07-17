interface ChartCardProps {
  title: string;
  className?: string;
  children: React.ReactNode;
}

export function ChartCard({ title, className, children }: ChartCardProps) {
  return (
    <div className={`bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 ${className ?? ""}`}>
      <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">{title}</h4>
      {children}
    </div>
  );
}
