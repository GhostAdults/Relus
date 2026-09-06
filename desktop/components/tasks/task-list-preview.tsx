"use client";

import { useMemo, useState } from "react";

import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { FormField } from "@/components/ui/form-field";
import { SelectField } from "@/components/ui/select";
import { Input } from "@/components/ui/input";

export function TaskListPreview() {
  const [keyword, setKeyword] = useState("");
  const [runMode, setRunMode] = useState("all");

  const hint = useMemo(() => {
    const filters = [keyword.trim() ? `关键词“${keyword.trim()}”` : "", runMode !== "all" ? runMode : ""].filter(
      Boolean,
    );
    return filters.length ? `当前筛选：${filters.join("、")}` : "可通过任务 ID 或表名筛选后续接入的任务数据。";
  }, [keyword, runMode]);

  return (
    <Card>
      <div className="grid gap-3 border-b border-border p-4 sm:grid-cols-[minmax(0,1fr)_180px]">
        <FormField label="搜索任务">
          <Input
            value={keyword}
            onChange={(event) => setKeyword(event.target.value)}
            placeholder="任务 ID、来源表或目标表"
          />
        </FormField>
        <FormField label="运行方式">
          <SelectField
            options={[
              { value: "all", label: "全部" },
              { value: "一次性执行", label: "一次性执行" },
              { value: "Scheduler", label: "Scheduler" },
            ]}
            value={runMode}
            onChange={setRunMode}
          />
        </FormField>
      </div>
      <div className="border-b border-border bg-surface-muted px-4 py-2 text-xs text-muted-foreground">{hint}</div>
      <EmptyState
        icon="tasks"
        title="暂无可展示的任务"
        description="当前页面只完成任务列表、搜索和筛选框架。后端接入后将展示任务阶段、同步模式、调度信息与执行统计。"
      />
    </Card>
  );
}
