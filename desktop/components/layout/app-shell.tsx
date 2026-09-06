import type { ReactNode } from "react";

import { Sidebar } from "@/components/layout/sidebar";
import { PageTransition } from "@/components/layout/page-transition";
import { StatusBadge } from "@/components/ui/status-badge";
import { Badge, Button } from "@heroui/react";
import BellIcon from "@gravity-ui/icons/svgs/bell.svg";

const notificationCount = 1;

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="g-root g-root_theme_light app-background-pattern flex h-dvh min-h-0 flex-col overflow-hidden">
      <div
        id="top-title"
        className="flex h-16 shrink-0 items-center justify-between gap-3 border-b border-border bg-surface px-5"
      >
        <div id="avator-title" className="flex items-center gap-3">
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-primary text-sm font-bold text-white">
            R
          </div>
          <div className="min-w-0">
            <div className="text-sm font-semibold tracking-tight text-foreground">Relus</div>
            <div className="truncate text-[11px] text-muted-foreground">
              <StatusBadge tone="warning">v1.0</StatusBadge>
            </div>
          </div>
        </div>
        <Badge.Anchor aria-label={`通知${notificationCount}条`} className="h-8 w-8">
          <Button isIconOnly aria-label="通知" variant="tertiary" size="sm">
            <img src={BellIcon} alt="" className="size-4 object-contain" />
          </Button>
          <Badge color="danger" size="sm" placement="top-right" variant="primary">
            <Badge.Label>{notificationCount}</Badge.Label>
          </Badge>
        </Badge.Anchor>
      </div>
      <div className="flex min-h-0 flex-1">
        <Sidebar />
        <main className="min-h-0 min-w-0 flex-1">
          <div className="mx-auto box-border w-full min-w-0 overflow-x-hidden p-5 sm:p-6">
            <PageTransition>{children}</PageTransition>
          </div>
        </main>
      </div>
    </div>
  );
}
