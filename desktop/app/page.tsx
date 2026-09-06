"use client";

import Link from "next/link";

import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";

const capabilities = [
  {
    title: "当前同步数据条数",
    description: "当前已同步的数据记录总数。",
    value: "0",
    icon: "database" as const,
  },
  {
    title: "任务数",
    description: "当前配置的同步任务总数。",
    value: "0",
    icon: "tasks" as const,
  },
  {
    title: "Task 错误数",
    description: "同步任务执行失败或异常的次数。",
    value: "0",
    icon: "layers" as const,
  },
];

export default function Home() {
  return (
    <div className="grid gap-6">
      <PageHeader title="开始" description="配置并管理 Relus 数据同步任务" />

      <div className="grid gap-4 md:grid-cols-3">
        {capabilities.map((capability) => (
          <Card key={capability.title} className="p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-muted text-primary">
                <Icon name={capability.icon} className="h-5 w-5" />
              </div>
              <span className="text-2xl font-semibold tracking-tight text-primary">{capability.value}</span>
            </div>
            <h2 className="mt-4 text-sm font-semibold text-foreground">{capability.title}</h2>
            <p className="mt-1 text-sm leading-6 text-muted-foreground">{capability.description}</p>
          </Card>
        ))}
      </div>

      <Card>
        <EmptyState
          icon="tasks"
          title="暂无同步任务"
          description="显示一次性任务与 Scheduler 任务的运行状态和执行结果。"
          action={
            <Link href="/tasks/new" className="text-sm font-medium text-primary hover:underline">
              去配置同步任务
            </Link>
          }
        />
      </Card>
    </div>
  );
}
