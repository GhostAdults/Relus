import Link from "next/link";

import { TaskListPreview } from "@/components/tasks/task-list-preview";
import { Icon } from "@/components/ui/icon";
import { PageHeader } from "@/components/ui/page-header";

export default function TasksPage() {
  return (
    <div className="grid gap-6">
      <PageHeader
        title="任务记录"
        description="查看一次性执行与 Scheduler 任务。当前列表为前端框架，尚未读取真实运行状态。"
        actions={
          <Link
            href="/tasks/new"
            className="inline-flex h-9 items-center gap-2 rounded-lg bg-primary px-3.5 text-sm font-medium text-white transition-colors hover:bg-primary-hover"
          >
            <Icon name="plus" className="h-4 w-4" />
            新建同步
          </Link>
        }
      />
      <TaskListPreview />
    </div>
  );
}
