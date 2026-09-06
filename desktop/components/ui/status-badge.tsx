import type { ReactNode } from "react";

const tones = {
  neutral: "bg-surface-muted text-muted-foreground",
  primary: "bg-primary-soft text-primary",
  success: "bg-success-soft text-success",
  warning: "bg-warning-soft text-warning",
} as const;

type StatusBadgeProps = {
  children: ReactNode;
  tone?: keyof typeof tones;
};

export function StatusBadge({ children, tone = "neutral" }: StatusBadgeProps) {
  return (
    <span className={`inline-flex items-center rounded-md px-2 py-1 text-sm ${tones[tone]}`}>
      {children}
    </span>
  );
}
