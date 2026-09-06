import { RuntimeSettings } from "@/components/settings/runtime-settings";
import { PageHeader } from "@/components/ui/page-header";

export default function SettingsPage() {
  return (
    <div className="grid gap-6">
      <PageHeader title="运行设置" description="预览 Scheduler 与 Pipeline 运行参数。当前设置仅保存在页面状态中。" />
      <RuntimeSettings />
    </div>
  );
}
