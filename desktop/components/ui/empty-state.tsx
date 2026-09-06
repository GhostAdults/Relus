import type { ReactNode } from "react";

import { Icon, type IconName } from "@/components/ui/icon";

type EmptyStateProps = {
  icon?: IconName;
  title: string;
  description: string;
  action?: ReactNode;
  className?: string;
};

export function EmptyState({ icon = "layers", title, description, action, className = "" }: EmptyStateProps) {
  return (
    <div className={`flex min-h-56 flex-col items-center justify-center px-6 py-10 text-center ${className}`}>
      <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-primary-soft text-primary">
        <Icon name={icon} className="h-5 w-5" />
      </div>
      <h2 className="mt-4 text-sm font-semibold text-foreground">{title}</h2>
      <p className="mt-1 max-w-md text-sm leading-6 text-muted-foreground">{description}</p>
      {action ? <div className="mt-5">{action}</div> : null}
    </div>
  );
}
