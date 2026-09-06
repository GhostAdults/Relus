"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { TextArea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { FormField } from "@/components/ui/form-field";
import { PageHeader } from "@/components/ui/page-header";
import { SelectField } from "@/components/ui/select";

export default function ApiSourcePage() {
  const [headers, setHeaders] = useState('{\n  "Accept": "application/json"\n}');

  return (
    <div className="grid gap-6">
      <PageHeader
        title="API Source 测试"
        description="配置 HTTP JSON Source，并预览 items_json_path 提取结果。当前不会从浏览器发起真实请求。"
      />
      <div className="grid gap-4 lg:grid-cols-[minmax(280px,0.9fr)_minmax(0,1.1fr)]">
        <Card className="p-4">
          <div className="grid gap-4">
            <div className="grid gap-4 sm:grid-cols-[130px_minmax(0,1fr)]">
              <FormField label="请求方法">
                <SelectField
                  options={["GET", "POST", "PUT", "DELETE"].map((value) => ({ value, label: value }))}
                  defaultValue="GET"
                />
              </FormField>
              <FormField label="URL">
                <Input placeholder="https://api.example.com/users" />
              </FormField>
            </div>
            <FormField label="Headers" description="JSON 对象格式">
              <TextArea
                className="font-mono text-xs"
                value={headers}
                onChange={(event) => setHeaders(event.target.value)}
              />
            </FormField>
            <FormField label="items_json_path" description="用于从响应 JSON 中提取记录数组。">
              <Input placeholder="data.items" />
            </FormField>
            <FormField label="Body" description="仅作为界面输入框架；当前后端请求体能力不在本阶段对接。">
              <TextArea className="font-mono text-xs" placeholder="{}" />
            </FormField>
            <Button disabled>发送测试</Button>
          </div>
        </Card>
        <Card>
          <div className="border-b border-border px-4 py-3">
            <h2 className="text-sm font-semibold">响应预览</h2>
          </div>
          <EmptyState
            icon="api"
            title="等待 API 测试"
            description="后端接入后将展示原始响应与 items_json_path 提取结果；当前不会执行网络请求。"
          />
        </Card>
      </div>
    </div>
  );
}
