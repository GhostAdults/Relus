"use client";

import { useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { JsonPreview } from "@/components/ui/json-preview";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { logicalTypes } from "@/lib/relus-options";
import { SelectField } from "@/components/ui/select";
import { Input } from "@/components/ui/input";

type MappingRow = {
  id: number;
  target: string;
  source: string;
  type: string;
};

const initialRows: MappingRow[] = [
  { id: 1, target: "id", source: "id", type: "int" },
  { id: 2, target: "display_name", source: "profile.name", type: "text" },
  { id: 3, target: "updated_at", source: "updated_at", type: "timestamp" },
];

export function MappingEditor() {
  const [rows, setRows] = useState(initialRows);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    const encodedMappings = new URLSearchParams(window.location.search).get("mappings");
    if (!encodedMappings) return;

    try {
      const parsed = JSON.parse(encodedMappings) as Array<Partial<Omit<MappingRow, "id">>>;
      if (!Array.isArray(parsed)) return;

      const importedRows = parsed
        .filter((row) => row && typeof row === "object")
        .map((row, index) => ({
          id: index + 1,
          target: typeof row.target === "string" ? row.target : "",
          source: typeof row.source === "string" ? row.source : "",
          type:
            typeof row.type === "string" && logicalTypes.some((option) => option.value === row.type)
              ? row.type
              : "text",
        }));

      if (importedRows.length > 0) {
        window.setTimeout(() => setRows(importedRows), 0);
      }
    } catch {
      // Ignore malformed URL state and retain the editor defaults.
    }
  }, []);

  const config = useMemo(
    () => ({
      column_mapping: Object.fromEntries(rows.filter((row) => row.target).map((row) => [row.target, row.source])),
      column_types: Object.fromEntries(rows.filter((row) => row.target).map((row) => [row.target, row.type])),
    }),
    [rows],
  );

  const updateRow = (id: number, key: "target" | "source" | "type", value: string) => {
    setRows((current) => current.map((row) => (row.id === id ? { ...row, [key]: value } : row)));
  };

  const addRow = () => {
    setRows((current) => [
      ...current,
      { id: Math.max(0, ...current.map((row) => row.id)) + 1, target: "", source: "", type: "text" },
    ]);
  };

  const copyConfig = async () => {
    await navigator.clipboard.writeText(JSON.stringify(config, null, 2));
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  };

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(320px,0.75fr)]">
      <Card className="min-w-0 overflow-hidden">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
          <div>
            <h2 className="text-sm font-semibold">字段映射</h2>
            <p className="mt-0.5 text-xs text-muted-foreground">目标字段 → 源字段、嵌套路径或受支持的 DSL 表达式。</p>
          </div>
          <Button variant="secondary" onClick={addRow}>
            <Icon name="plus" className="h-4 w-4" />
            添加字段
          </Button>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[650px] text-left text-sm">
            <thead className="bg-surface-muted text-xs text-muted-foreground">
              <tr>
                <th className="px-3 py-2.5 font-medium">目标字段</th>
                <th className="px-3 py-2.5 font-medium">源字段 / 路径</th>
                <th className="px-3 py-2.5 font-medium">逻辑类型</th>
                <th className="w-14 px-3 py-2.5 font-medium">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {rows.map((row) => (
                <tr key={row.id}>
                  <td className="p-2">
                    <Input
                      value={row.target}
                      onChange={(event) => updateRow(row.id, "target", event.target.value)}
                      placeholder="target_column"
                    />
                  </td>
                  <td className="p-2">
                    <Input
                      value={row.source}
                      onChange={(event) => updateRow(row.id, "source", event.target.value)}
                      placeholder="source.path"
                    />
                  </td>
                  <td className="p-2">
                    <SelectField
                      options={logicalTypes}
                      value={row.type}
                      onChange={(value) => updateRow(row.id, "type", value)}
                    />
                  </td>
                  <td className="p-2 text-center">
                    <Button
                      type="button"
                      variant="ghost"
                      onClick={() => setRows((current) => current.filter((item) => item.id !== row.id))}
                      className="h-8 w-8 rounded-md p-0 text-muted-foreground hover:bg-danger-soft hover:text-danger"
                      aria-label={`删除 ${row.target || "空字段"}`}
                    >
                      <Icon name="trash" className="h-4 w-4" />
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {rows.length === 0 ? (
          <div className="p-8 text-center text-sm text-muted-foreground">暂无字段，点击“添加字段”开始编辑。</div>
        ) : null}
      </Card>

      <Card className="min-w-0 overflow-hidden">
        <div className="flex items-center justify-between gap-3 border-b border-border px-4 py-3">
          <h2 className="text-sm font-semibold">配置预览</h2>
          <Button variant="secondary" onClick={copyConfig}>
            <Icon name="copy" className="h-4 w-4" />
            {copied ? "已复制" : "复制"}
          </Button>
        </div>
        <JsonPreview value={config} className="bg-[#111827]" />
        <div className="border-t border-border bg-warning-soft px-4 py-3 text-xs leading-5 text-warning">
          配置仅存在于当前页面，不会写入文件或提交后端。
        </div>
      </Card>
    </div>
  );
}
