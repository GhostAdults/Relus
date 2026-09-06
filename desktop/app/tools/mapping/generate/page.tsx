import Link from "next/link";

import { ToolSubnav } from "@/components/layout/tool-subnav";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { FormField } from "@/components/ui/form-field";
import { PageHeader } from "@/components/ui/page-header";
import { StatusBadge } from "@/components/ui/status-badge";
import { databaseTypes } from "@/lib/relus-options";
import { SelectField } from "@/components/ui/select";
import { Input } from "@/components/ui/input";

const mappingNav = [
  { label: "生成映射", href: "/tools/mapping/generate" },
  { label: "编辑映射", href: "/tools/mapping/edit" },
];

const example = {
  column_mapping: { id: "", name: "", updated_at: "" },
  column_types: { id: "int", name: "text", updated_at: "timestamp" },
};

export default function MappingGeneratePage() {
  return (
    <div className="grid gap-6">
      <PageHeader title="生成映射" description="根据目标表结构生成 column_mapping 与 column_types 配置模板。" />
      <ToolSubnav items={mappingNav} />
      <div className="grid gap-4 lg:grid-cols-[minmax(260px,0.8fr)_minmax(0,1.2fr)]">
        <Card className="p-4">
          <div className="grid gap-4">
            <FormField label="数据库类型">
              <SelectField options={databaseTypes} defaultValue="postgres" />
            </FormField>
            <FormField label="数据库 URL">
              <Input placeholder="输入目标数据库连接 URL" />
            </FormField>
            <FormField label="目标表">
              <Input placeholder="public.users" />
            </FormField>
            <Button disabled>生成 Mapping</Button>
            <p className="text-xs leading-5 text-muted-foreground">
              后端接入后将读取目标表字段并生成空映射模板，不会自动猜测源字段关系。
            </p>
          </div>
        </Card>
        <Card className="min-w-0 overflow-hidden">
          <div className="flex items-center justify-between border-b border-border px-4 py-3">
            <h2 className="text-sm font-semibold">示例结果</h2>
            <StatusBadge>示例</StatusBadge>
          </div>
          <pre className="max-h-[420px] overflow-auto bg-[#111827] p-4 font-mono text-xs leading-5 text-slate-200">
            {JSON.stringify(example, null, 2)}
          </pre>
          <div className="border-t border-border p-3 text-right">
            <Link href="/tools/mapping/edit" className="text-sm font-medium text-primary hover:underline">
              进入 Mapping 编辑器
            </Link>
          </div>
        </Card>
      </div>
    </div>
  );
}
