import type React from "react";

interface ChartCardProps {
  title: string;
  className?: string;
  subtitle?: string;
  children: React.ReactNode;
}

export function ChartCard({ title, className, subtitle, children }: ChartCardProps) {
  return (
    <div className={`bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 ${className ?? ""}`}>
      <div className="mb-4">
        <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide">{title}</h4>
        {subtitle && <p className="text-caption text-on-surface-variant/70 mt-0.5">{subtitle}</p>}
      </div>
      {children}
    </div>
  );
}

export default ChartCard;
