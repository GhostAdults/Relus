import { TaskWizard } from "@/components/tasks/task-wizard";
import { PageHeader } from "@/components/ui/page-header";

export default function NewTaskPage() {
  return (
    <div className="grid gap-6">
      <PageHeader title="新建同步" description="配置数据来源、目标端、字段映射与执行方式，并预览 Relus 任务 JSON。" />
      <TaskWizard />
    </div>
  );
}
