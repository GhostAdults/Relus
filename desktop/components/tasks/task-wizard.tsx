"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import { FormField } from "@/components/ui/form-field";
import { Icon } from "@/components/ui/icon";
import { StatusBadge } from "@/components/ui/status-badge";
import { EmptyState } from "@/components/ui/empty-state";
import { databaseTypeIcons, logicalTypes, syncModes, writerModes } from "@/lib/relus-options";
import { useSourceTypes, type SourceTypeOption } from "@/lib/use-source-types";
import { SelectField } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { toast } from "@/components/ui/toast-provider";
import { invoke, isTauri } from "@tauri-apps/api/core";
import { Surface, Table, Tabs } from "@heroui/react";
import { ChipWithIcon } from "@/components/ui/chip-with-icon";
import { JsonPreview } from "@/components/ui/json-preview";
import { CircleFill, Xmark, CircleCheckFill, Check } from "@gravity-ui/icons";

const steps = ["来源目标", "字段映射", "执行配置"];
const TASK_WIZARD_STEP_KEY = "relus:new-task:step";
const TASK_WIZARD_STEP_VERSION_KEY = "relus:new-task:step-version";
const TASK_WIZARD_STEP_VERSION = "3";

type MappingRow = {
  target: string;
  source: string;
  type: string;
};

export function TaskWizard() {
  // Keep the server-rendered and initial client markup identical, then restore
  // the previously selected tab after mount. Persistence is enabled only
  // after that restore completes so the default step cannot overwrite it.
  const [step, setStep] = useState(0);
  const [stepHydrated, setStepHydrated] = useState(false);
  const { sourceTypes, error: sourceTypesError } = useSourceTypes();
  const databaseOptions = sourceTypes.map((sourceType) => {
    const icon = databaseTypeIcons.find((item) => item.value === sourceType.value);
    return { ...sourceType, iconSrc: icon?.src, iconAlt: icon?.alt };
  });
  const [copied, setCopied] = useState(false);
  const [runResult, setRunResult] = useState<unknown>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [form, setForm] = useState({
    sourceType: "mysql",
    sourceHost: "127.0.0.1",
    sourcePort: "3306",
    sourceDatabase: "",
    sourceTable: "",
    sourceUsername: "root",
    sourcePassword: "",
    targetType: "postgres",
    targetHost: "127.0.0.1",
    targetPort: "5432",
    targetDatabase: "",
    targetTable: "",
    targetUsername: "postgres",
    targetPassword: "",
    syncMode: "fullsnapshot",
    writerMode: "upsert",
    keyColumns: "id",
    batchSize: "1000",
    jobId: "mysql_to_postgres_users",
    runMode: "immediate",
    schedule: "0 */5 * * * *",
  });
  const [mappings, setMappings] = useState<MappingRow[]>([
    { target: "id", source: "id", type: "int" },
    { target: "name", source: "name", type: "text" },
    { target: "updated_at", source: "updated_at", type: "timestamp" },
  ]);

  useEffect(() => {
    const restore = window.setTimeout(() => {
      const savedStep = Number(window.sessionStorage.getItem(TASK_WIZARD_STEP_KEY));
      if (Number.isInteger(savedStep) && savedStep >= 0) {
        const isCurrentVersion =
          window.sessionStorage.getItem(TASK_WIZARD_STEP_VERSION_KEY) === TASK_WIZARD_STEP_VERSION;
        // Migrate the previously persisted four-step wizard position only
        // once. The old strategy page now lives on the final page.
        const migratedStep = isCurrentVersion ? savedStep : savedStep === 0 ? 0 : savedStep === 2 ? 1 : 2;
        if (migratedStep < steps.length) setStep(migratedStep);
      }
      window.sessionStorage.setItem(TASK_WIZARD_STEP_VERSION_KEY, TASK_WIZARD_STEP_VERSION);
      setStepHydrated(true);
    }, 0);

    return () => window.clearTimeout(restore);
  }, []);

  useEffect(() => {
    if (!stepHydrated) return;
    window.sessionStorage.setItem(TASK_WIZARD_STEP_KEY, String(step));
    window.sessionStorage.setItem(TASK_WIZARD_STEP_VERSION_KEY, TASK_WIZARD_STEP_VERSION);
  }, [step, stepHydrated]);

  const updateForm = (key: keyof typeof form, value: string) => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  const config = useMemo(() => {
    const columnMapping = Object.fromEntries(
      mappings.filter((row) => row.target).map((row) => [row.target, row.source]),
    );
    const columnTypes = Object.fromEntries(mappings.filter((row) => row.target).map((row) => [row.target, row.type]));

    return {
      job_id: form.jobId,
      source: {
        name: "source_database",
        type: "database",
        config: {
          split_pk: form.keyColumns.split(",")[0]?.trim() || undefined,
          connections: [
            {
              type: form.sourceType,
              host: form.sourceHost,
              port: Number(form.sourcePort),
              database: form.sourceDatabase,
              username: form.sourceUsername,
              password: form.sourcePassword,
              table: form.sourceTable,
              key_columns: form.keyColumns
                .split(",")
                .map((value) => value.trim())
                .filter(Boolean),
              max_connections: 20,
              acquire_timeout_secs: 30,
              use_transaction: true,
            },
          ],
        },
      },
      target: {
        name: "target_database",
        type: "database",
        writer_mode: form.writerMode,
        config: {
          connection: {
            type: form.targetType,
            host: form.targetHost,
            port: Number(form.targetPort),
            database: form.targetDatabase,
            username: form.targetUsername,
            password: form.targetPassword,
            table: form.targetTable,
            key_columns: form.keyColumns
              .split(",")
              .map((value) => value.trim())
              .filter(Boolean),
            max_connections: 20,
            acquire_timeout_secs: 30,
            use_transaction: true,
          },
        },
      },
      column_mapping: columnMapping,
      column_types: columnTypes,
      sync_mode: form.syncMode,
      batch_size: Number(form.batchSize),
      channel_buffer_size: Number(form.batchSize),
      schedule: form.runMode === "scheduler" ? form.schedule : { type: "immediate" },
    };
  }, [form, mappings]);

  const displayConfig = useMemo(() => {
    const masked = structuredClone(config) as typeof config;
    masked.source.config.connections[0].password = "******";
    masked.target.config.connection.password = "******";
    return masked;
  }, [config]);

  const copyConfig = async () => {
    await navigator.clipboard.writeText(JSON.stringify(displayConfig, null, 2));
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  };

  const saveConfig = () => {
    window.localStorage.setItem("relus:new-task:config", JSON.stringify(config));
    toast.success("当前配置已保存");
  };

  const submitTask = async () => {
    if (!isTauri()) {
      toast.danger("当前页面运行在普通浏览器中，请在 Relus Tauri 桌面窗口中提交任务。");
      return;
    }
    setIsSubmitting(true);
    setRunResult(null);
    try {
      const result = await invoke("start_job", { jobConfig: config });
      setRunResult(result);
      toast.success("任务执行完成");
    } catch (error) {
      toast.danger(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  };

  const updateMapping = (index: number, key: keyof MappingRow, value: string) => {
    setMappings((rows) => rows.map((row, rowIndex) => (rowIndex === index ? { ...row, [key]: value } : row)));
  };

  const addMapping = () => {
    setMappings((rows) => [...rows, { target: "", source: "", type: "text" }]);
  };

  const removeMapping = (index: number) => {
    setMappings((rows) => rows.filter((_, rowIndex) => rowIndex !== index));
  };

  const changeStep = (key: React.Key) => {
    const nextStep = Number(key);
    if (!Number.isInteger(nextStep) || nextStep < 0 || nextStep >= steps.length) return;
    setStep(nextStep);
    window.sessionStorage.setItem(TASK_WIZARD_STEP_KEY, String(nextStep));
  };

  const moveStep = (offset: number) => {
    const nextStep = Math.min(steps.length - 1, Math.max(0, step + offset));
    changeStep(String(nextStep));
  };

  return (
    <div className="flex h-[calc(100dvh-10rem)] min-h-0 flex-col gap-4">
      <div className="shrink-0">
        <Tabs selectedKey={String(step)} onSelectionChange={changeStep} className="w-full">
          <Tabs.ListContainer>
            <Tabs.List aria-label="任务创建步骤">
              {steps.map((label, index) => (
                <Tabs.Tab key={String(index)} id={String(index)}>
                  {label}
                  <Tabs.Indicator />
                </Tabs.Tab>
              ))}
            </Tabs.List>
          </Tabs.ListContainer>
        </Tabs>
      </div>

      <Surface className="min-h-1 flex-1 overflow-x-hidden overflow-y-auto rounded-lg border border-border bg-surface min-w-[320px] flex-col gap-3 rounded-2xl p-6">
        <div className="p-5">
          {step === 0 ? (
            <div className="grid gap-5">
              {sourceTypesError ? (
                <p
                  className="rounded-lg border border-danger/20 bg-danger-soft px-3 py-2 text-xs leading-5 text-danger"
                  role="alert"
                >
                  {sourceTypesError}
                </p>
              ) : null}
              <div className="grid gap-5 lg:grid-cols-2">
                <ConnectionSection
                  title="来源数据库"
                  prefix="source"
                  form={form}
                  updateForm={updateForm}
                  sourceTypes={databaseOptions}
                />
                <ConnectionSection
                  title="目标数据库"
                  prefix="target"
                  form={form}
                  updateForm={updateForm}
                  sourceTypes={databaseOptions}
                />
              </div>
            </div>
          ) : null}

          {step === 1 ? (
            <div className="grid gap-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2 className="text-sm font-semibold">字段映射</h2>
                  <p className="mt-1 text-xs text-muted-foreground">方向为目标字段 → 源字段或源路径。</p>
                </div>
                <div className="flex items-center gap-3">
                  <Button variant="secondary" onClick={addMapping}>
                    <Icon name="plus" className="h-4 w-4" />
                    新增字段
                  </Button>
                  <Link
                    href={`/tools/mapping/edit?mappings=${encodeURIComponent(JSON.stringify(mappings))}`}
                    className="text-sm font-medium text-primary hover:underline"
                  >
                    打开完整 Mapping 编辑器
                  </Link>
                </div>
              </div>
              <div className="overflow-hidden border border-border">
                <Table variant="secondary" className="min-w-[580px]">
                  <Table.ScrollContainer>
                    <Table.Content>
                      <Table.Header>
                        <Table.Column className="!rounded-none" isRowHeader>
                          目标字段
                        </Table.Column>
                        <Table.Column className="!rounded-none">源字段 / 路径</Table.Column>
                        <Table.Column className="!rounded-none">逻辑类型</Table.Column>
                        <Table.Column className="w-20 !rounded-none">操作</Table.Column>
                      </Table.Header>
                      <Table.Body
                        renderEmptyState={() => (
                          <EmptyState
                            className="h-full w-full"
                            icon="table"
                            title="暂无字段映射"
                            description="当前还没有配置字段映射，请点击上方“新增字段”添加一行。"
                          />
                        )}
                      >
                        {mappings.map((row, index) => (
                          <Table.Row key={`${index}-${row.target}`}>
                            <Table.Cell className="p-2">
                              <Input
                                value={row.target}
                                onChange={(event) => updateMapping(index, "target", event.target.value)}
                              />
                            </Table.Cell>
                            <Table.Cell className="p-2">
                              <Input
                                value={row.source}
                                onChange={(event) => updateMapping(index, "source", event.target.value)}
                              />
                            </Table.Cell>
                            <Table.Cell className="p-2">
                              <div className="w-40">
                                <SelectField
                                  options={logicalTypes}
                                  value={row.type}
                                  onChange={(value) => updateMapping(index, "type", value)}
                                />
                              </div>
                            </Table.Cell>
                            <Table.Cell className="w-20 p-2 text-center">
                              <Button
                                isIconOnly
                                type="button"
                                variant="ghost"
                                className="group h-8 w-8 p-0 text-muted-foreground hover:bg-danger-soft"
                                onClick={() => removeMapping(index)}
                                aria-label={`删除第 ${index + 1} 行字段映射`}
                              >
                                <Icon name="trash" className="h-4 w-4 transition-colors group-hover:text-danger" />
                              </Button>
                            </Table.Cell>
                          </Table.Row>
                        ))}
                      </Table.Body>
                    </Table.Content>
                  </Table.ScrollContainer>
                </Table>
              </div>
            </div>
          ) : null}

          {step === 2 ? (
            <div className="grid gap-5">
              <div className="grid content-start gap-4 sm:grid-cols-2">
                <FormField label="同步模式" description="全局fullsnapshot,增量incremental,混合mix">
                  <SelectField
                    options={syncModes.map((option) => ({ ...option, label: `${option.label} · ${option.value}` }))}
                    value={form.syncMode}
                    onChange={(value) => updateForm("syncMode", value)}
                  />
                </FormField>
                <FormField label="写入模式" description="选择插入模式">
                  <SelectField
                    options={writerModes}
                    value={form.writerMode}
                    onChange={(value) => updateForm("writerMode", value)}
                  />
                </FormField>
                <FormField label="主键字段" description="upsert/update/delete 通常需要配置">
                  <Input
                    value={form.keyColumns}
                    onChange={(event) => updateForm("keyColumns", event.target.value)}
                    placeholder="id"
                  />
                </FormField>
                <FormField label="批次大小" description="设置每次处理的数据条数">
                  <Input
                    type="number"
                    min="1"
                    value={form.batchSize}
                    onChange={(event) => updateForm("batchSize", event.target.value)}
                  />
                </FormField>
              </div>
              <div className="grid content-start gap-4 sm:grid-cols-2">
                <FormField label="任务 ID" description="配置唯一值(job_id)">
                  <Input value={form.jobId} onChange={(event) => updateForm("jobId", event.target.value)} />
                </FormField>
                <FormField label="执行方式" description="周期性的执行任务">
                  <SelectField
                    options={[
                      { value: "immediate", label: "一次性执行" },
                      { value: "scheduler", label: "Scheduler 调度" },
                    ]}
                    value={form.runMode}
                    onChange={(value) => updateForm("runMode", value)}
                  />
                </FormField>
                {form.runMode === "scheduler" ? (
                  <FormField label="Schedule" description="支持 cron 或 once 时间表达。">
                    <Input value={form.schedule} onChange={(event) => updateForm("schedule", event.target.value)} />
                  </FormField>
                ) : null}
              </div>
              <div className="min-w-0">
                <div className="mb-2 flex items-center justify-between gap-3">
                  <span className="text-sm font-semibold">【配置】job.config(JSON)</span>
                  <Button variant="secondary" onClick={copyConfig}>
                    <Icon name="copy" className="h-4 w-4" />
                    {copied ? "已复制" : "复制 JSON"}
                  </Button>
                </div>
                <JsonPreview value={runResult ?? displayConfig} className="scrollbar-hide" />
              </div>
            </div>
          ) : null}
        </div>
      </Surface>

      <Surface
        variant="tertiary"
        id="footbar"
        className="-mt-2 mb-6 flex shrink-0 flex-wrap items-center justify-between gap-3 border border-border px-5 py-2"
      >
        <div className="flex items-center gap-2">
          <StatusBadge tone="warning">配置</StatusBadge>
          <span className="text-xs text-muted-foreground">
            第 {step + 1} 步，共 {steps.length} 步
          </span>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" onClick={saveConfig}>
            保存当前配置
          </Button>
          <Button variant="secondary" disabled={step === 0} onClick={() => moveStep(-1)}>
            上一步
          </Button>
          {step < steps.length - 1 ? (
            <Button onClick={() => moveStep(1)}>
              下一步
              <Icon name="arrow-right" className="h-4 w-4" />
            </Button>
          ) : (
            <Button disabled={isSubmitting} onClick={submitTask}>
              {isSubmitting ? (
                "执行中…"
              ) : (
                <>
                  <Check />
                  开始同步
                </>
              )}
            </Button>
          )}
        </div>
      </Surface>
    </div>
  );
}

type FormState = {
  sourceType: string;
  sourceHost: string;
  sourcePort: string;
  sourceDatabase: string;
  sourceTable: string;
  sourceUsername: string;
  sourcePassword: string;
  targetType: string;
  targetHost: string;
  targetPort: string;
  targetDatabase: string;
  targetTable: string;
  targetUsername: string;
  targetPassword: string;
};

type ConnectionSectionProps = {
  title: string;
  prefix: "source" | "target";
  form: FormState;
  updateForm: (key: keyof FormState, value: string) => void;
  sourceTypes: readonly SourceTypeOption[];
};

function ConnectionSection({ title, prefix, form, updateForm, sourceTypes }: ConnectionSectionProps) {
  // 连接状态
  const [connectionState, setConnectionState] = useState<"idle" | "success" | "error">("idle");

  const field = (suffix: "Type" | "Host" | "Port" | "Database" | "Table" | "Username" | "Password") =>
    `${prefix}${suffix}` as keyof FormState;

  return (
    <section className="grid gap-4 rounded-xl border border-border bg-surface-muted/50 p-4">
      <h2 className="text-sm font-semibold text-foreground">
        <StatusBadge tone="primary">{title}</StatusBadge>
      </h2>
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="grid grid-cols-2 items-end gap-3 sm:col-span-2">
          <FormField label="数据源类型">
            <SelectField
              options={sourceTypes}
              value={form[field("Type")]}
              variant="secondary"
              onChange={(value) => updateForm(field("Type"), value)}
            />
          </FormField>
          <div className="flex h-10 items-bottom justify-start">
            <ChipWithIcon
              color={connectionState === "success" ? "success" : connectionState === "error" ? "danger" : "accent"}
              icon={
                connectionState === "success" ? (
                  <CircleCheckFill width={12} />
                ) : connectionState === "idle" ? (
                  <CircleFill width={6} />
                ) : (
                  <Xmark width={12} />
                )
              }
              onClick={async () => {
                if (!isTauri()) return;
                try {
                  const dbUrl = `${form[field("Type")]}://${form[field("Username")]}:${form[field("Password")]}@${form[field("Host")]}:${form[field("Port")]}/${form[field("Database")]}`;
                  await invoke("connect_database", { dbUrl, dbType: form[field("Type")] });
                  setConnectionState("success");
                } catch {
                  setConnectionState("error");
                }
              }}
            >
              {connectionState === "success" ? "连接成功" : connectionState === "error" ? "连接失败" : "测试连接"}
            </ChipWithIcon>
          </div>
        </div>
        <FormField label="Host">
          <Input
            value={form[field("Host")]}
            placeholder="主机IP地址"
            onChange={(event) => updateForm(field("Host"), event.target.value)}
          />
        </FormField>
        <FormField label="端口">
          <Input
            value={form[field("Port")]}
            placeholder="端口号"
            onChange={(event) => updateForm(field("Port"), event.target.value)}
          />
        </FormField>
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <FormField label="数据库">
          <Input
            value={form[field("Database")]}
            onChange={(event) => updateForm(field("Database"), event.target.value)}
          />
        </FormField>
        <FormField label="数据表">
          <Input value={form[field("Table")]} onChange={(event) => updateForm(field("Table"), event.target.value)} />
        </FormField>
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <FormField label="用户名">
          <Input
            value={form[field("Username")]}
            onChange={(event) => updateForm(field("Username"), event.target.value)}
          />
        </FormField>
        <FormField label="密码">
          <Input
            type="password"
            value={form[field("Password")]}
            onChange={(event) => updateForm(field("Password"), event.target.value)}
            placeholder="输入数据库密码"
          />
        </FormField>
      </div>
    </section>
  );
}
