import { ToolSubnav } from "@/components/layout/tool-subnav";
import { MappingEditor } from "@/components/mapping/mapping-editor";
import { PageHeader } from "@/components/ui/page-header";

const mappingNav = [
  { label: "生成映射", href: "/tools/mapping/generate" },
  { label: "映射编辑", href: "/tools/mapping/edit" },
];

export default function MappingEditPage() {
  return (
    <div className="grid gap-6">
      <PageHeader title="Mapping 编辑" description="维护目标字段到源字段或源路径的映射，并实时预览配置 JSON。" />
      <ToolSubnav items={mappingNav} />
      <MappingEditor />
    </div>
  );
}
