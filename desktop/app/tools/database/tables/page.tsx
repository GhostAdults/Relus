import { DatabaseWorkspace } from "@/components/database/database-workspace";
import { PageHeader } from "@/components/ui/page-header";

export default function DatabaseTablesPage() {
  return (
    <div className="grid gap-6">
      <PageHeader title="表数据工具" description="连接 MySQL 或 PostgreSQL，查看数据表与字段结构。" />
      <DatabaseWorkspace />
    </div>
  );
}
