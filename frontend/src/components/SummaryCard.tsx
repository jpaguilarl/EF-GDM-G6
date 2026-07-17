import type { ReactNode } from "react";

interface SummaryCardProps {
  label: string;
  value: string | number;
  icon?: ReactNode;
  accent?: "primary" | "secondary" | "error" | "warning";
  sublabel?: string;
  children?: ReactNode;
}

const accentStyles: Record<string, string> = {
  primary: "border-l-primary-container",
  secondary: "border-l-secondary-container",
  error: "border-l-error",
  warning: "border-l-amber-500",
};

export function SummaryCard({ label, value, icon, accent = "primary", sublabel, children }: SummaryCardProps) {
  return (
    <div
      className={`bg-surface-container-lowest border border-border-subtle border-l-4 rounded-DEFAULT p-4 flex items-start gap-3 ${accentStyles[accent]}`}
    >
      {icon && <span className="text-on-surface-variant shrink-0 mt-0.5">{icon}</span>}
      <div className="min-w-0 flex-1">
        <div className="text-caption text-on-surface-variant uppercase tracking-wide truncate">{label}</div>
        {children || (
          <>
            <div className="text-headline-sm text-on-surface font-bold tabular-nums truncate">
              {typeof value === "number" ? value.toLocaleString() : value}
            </div>
            {sublabel && (
              <div className="text-caption text-on-surface-variant mt-0.5">{sublabel}</div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
