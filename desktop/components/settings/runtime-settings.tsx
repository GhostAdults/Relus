"use client";

import { useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { JsonPreview } from "@/components/ui/json-preview";
import { Input } from "@/components/ui/input";
import { FormField } from "@/components/ui/form-field";
import { StatusBadge } from "@/components/ui/status-badge";

const initialSettings = {
  host: "127.0.0.1",
  port: "3000",
  readerThreads: "4",
  bufferSize: "1000",
  batchSize: "1000",
  useTransaction: true,
};

export function RuntimeSettings() {
  const [settings, setSettings] = useState(initialSettings);

  const preview = useMemo(
    () => ({
      server: { host: settings.host, port: Number(settings.port) },
      pipeline: {
        reader_threads: Number(settings.readerThreads),
        buffer_size: Number(settings.bufferSize),
        batch_size: Number(settings.batchSize),
        use_transaction: settings.useTransaction,
      },
    }),
    [settings],
  );

  const update = (key: keyof typeof settings, value: string | boolean) => {
    setSettings((current) => ({ ...current, [key]: value }));
  };

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(280px,0.85fr)_minmax(0,1.15fr)]">
      <div className="grid content-start gap-4">
        <Card className="p-4">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h2 className="text-sm font-semibold">Scheduler Server</h2>
            <StatusBadge tone="warning">未持久化</StatusBadge>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <FormField label="Host">
              <Input value={settings.host} onChange={(event) => update("host", event.target.value)} />
            </FormField>
            <FormField label="Port">
              <Input type="number" value={settings.port} onChange={(event) => update("port", event.target.value)} />
            </FormField>
          </div>
        </Card>
        <Card className="p-4">
          <h2 className="mb-4 text-sm font-semibold">Pipeline</h2>
          <div className="grid gap-4 sm:grid-cols-2">
            <FormField label="Reader Threads">
              <Input
                type="number"
                min="1"
                value={settings.readerThreads}
                onChange={(event) => update("readerThreads", event.target.value)}
              />
            </FormField>
            <FormField label="Buffer Size">
              <Input
                type="number"
                min="1"
                value={settings.bufferSize}
                onChange={(event) => update("bufferSize", event.target.value)}
              />
            </FormField>
            <FormField label="Batch Size">
              <Input
                type="number"
                min="1"
                value={settings.batchSize}
                onChange={(event) => update("batchSize", event.target.value)}
              />
            </FormField>
            <label className="flex items-center gap-3 self-end rounded-lg border border-border bg-surface-muted px-3 py-2.5 text-sm font-medium">
              <input
                type="checkbox"
                className="h-4 w-4"
                checked={settings.useTransaction}
                onChange={(event) => update("useTransaction", event.target.checked)}
              />
              使用事务
            </label>
          </div>
        </Card>
        <div className="flex flex-wrap gap-2">
          <Button variant="secondary" onClick={() => setSettings(initialSettings)}>
            恢复示例值
          </Button>
          <Button disabled>保存设置</Button>
        </div>
      </div>

      <Card className="min-w-0 overflow-hidden">
        <div className="border-b border-border px-4 py-3">
          <h2 className="text-sm font-semibold">配置 JSON 预览</h2>
        </div>
        <JsonPreview value={preview} className="min-h-80 bg-[#111827]" />
        <div className="border-t border-border bg-warning-soft px-4 py-3 text-xs leading-5 text-warning">
          修改仅作用于当前页面，尚未对接 Relus 本地配置或 HTTP 设置接口。
        </div>
      </Card>
    </div>
  );
}
