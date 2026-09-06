"use client";

import { invoke, isTauri } from "@tauri-apps/api/core";
import { FormEvent, useState } from "react";
import { Input } from "@/components/ui/input";
import { Form } from "@/components/ui/form";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { ErrorAlert } from "@/components/ui/error-alert";
import { FormField } from "@/components/ui/form-field";
import { StatusBadge } from "@/components/ui/status-badge";
import { databaseTypeIcons, databaseTypes } from "@/lib/relus-options";
import { SelectField } from "@/components/ui/select";

type DatabaseType = (typeof databaseTypes)[number]["value"];
const databaseSelectOptions = databaseTypes.map((option) => {
  const icon = databaseTypeIcons.find((item) => item.value === option.value);
  return { ...option, iconSrc: icon?.src, iconAlt: icon?.alt };
});

function errorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return "查询数据库表失败，请检查连接信息后重试。";
}

export function DatabaseTableList() {
  const [databaseType, setDatabaseType] = useState<DatabaseType>("mysql");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("3306");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [databaseName, setDatabaseName] = useState("");
  const [tables, setTables] = useState<string[]>([]);
  const [hasQueried, setHasQueried] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const queryTables = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const trimmedHost = host.trim();
    const trimmedPort = port.trim();
    const trimmedUsername = username.trim();
    const trimmedDatabaseName = databaseName.trim();
    const numericPort = Number(trimmedPort);

    if (!trimmedHost || !trimmedPort || !trimmedUsername || !trimmedDatabaseName) {
      setError("请填写主机 IP、端口、用户名和数据库名。");
      return;
    }
    if (!Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setError("请输入 1 到 65535 之间的有效端口。");
      return;
    }

    if (!isTauri()) {
      setError("当前页面运行在普通浏览器中，请在 Relus Tauri 桌面窗口中查询数据库表。");
      return;
    }

    setIsLoading(true);
    setError("");
    setHasQueried(false);
    setIsConnected(false);
    setTables([]);

    try {
      const protocol = databaseType === "mysql" ? "mysql" : "postgres";
      const credentials = `${encodeURIComponent(trimmedUsername)}:${encodeURIComponent(password)}@`;
      const dbUrl = `${protocol}://${credentials}${trimmedHost}:${numericPort}/${encodeURIComponent(trimmedDatabaseName)}`;
      const result = await invoke<string[]>("connect_database", {
        dbUrl,
        dbType: databaseType,
      });
      setTables(result);
      setHasQueried(true);
      setIsConnected(true);
    } catch (queryError) {
      setError(errorMessage(queryError));
    } finally {
      setIsLoading(false);
    }
  };

  const defaultPort = databaseType === "mysql" ? "3306" : "5432";

  const changeDatabaseType = (value: DatabaseType) => {
    setDatabaseType(value);
    setPort(value === "mysql" ? "3306" : "5432");
    setIsConnected(false);
    setHasQueried(false);
    setTables([]);
    setError("");
  };

  return (
    <>
      {error ? <ErrorAlert message={error} onClose={() => setError("")} /> : null}
      <Card>
        <Form
          className="grid gap-4 border-b border-border p-4 md:grid-cols-2 xl:grid-cols-[150px_minmax(180px,1fr)_120px_minmax(160px,1fr)_minmax(160px,1fr)_minmax(160px,1fr)_auto] xl:items-end"
          onSubmit={queryTables}
        >
          <FormField label="数据库类型">
            <SelectField
              options={databaseSelectOptions}
              value={databaseType}
              onChange={(value) => changeDatabaseType(value as DatabaseType)}
              disabled={isLoading}
            />
          </FormField>
          <FormField label="主机 IP">
            <Input
              value={host}
              onChange={(event) => setHost(event.target.value)}
              placeholder="127.0.0.1"
              spellCheck={false}
              disabled={isLoading}
            />
          </FormField>
          <FormField label="端口" description={`默认 ${defaultPort}`}>
            <Input
              type="number"
              min="1"
              max="65535"
              value={port}
              onChange={(event) => setPort(event.target.value)}
              placeholder={defaultPort}
              disabled={isLoading}
            />
          </FormField>
          <FormField label="用户名">
            <Input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="输入数据库用户名"
              autoComplete="username"
              disabled={isLoading}
            />
          </FormField>
          <FormField label="密码">
            <Input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="输入数据库密码"
              autoComplete="current-password"
              disabled={isLoading}
            />
          </FormField>
          <FormField label="数据库名">
            <Input
              value={databaseName}
              onChange={(event) => setDatabaseName(event.target.value)}
              placeholder="输入数据库名"
              disabled={isLoading}
            />
          </FormField>
          <Button type="submit" disabled={isLoading}>
            {isLoading ? "测试中…" : "测试连接"}
          </Button>
        </Form>

        {isConnected ? (
          <div
            className="flex items-center gap-2 border-b border-border bg-success-soft px-4 py-2.5 text-sm text-success"
            role="status"
          >
            <StatusBadge tone="success">连接成功</StatusBadge>
            <span>已加载当前数据库的表列表。</span>
          </div>
        ) : null}

        {isConnected && tables.length > 0 ? (
          <div>
            <div className="flex items-center justify-between border-b border-border bg-surface-muted px-4 py-2.5">
              <span className="text-sm font-medium text-foreground">查询结果</span>
              <StatusBadge tone="success">{tables.length} 张表</StatusBadge>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[480px] text-left text-sm">
                <thead className="border-b border-border text-xs text-muted-foreground">
                  <tr>
                    <th className="w-20 px-4 py-2.5 font-medium">序号</th>
                    <th className="px-4 py-2.5 font-medium">表名</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {tables.map((table, index) => (
                    <tr key={`${table}-${index}`} className="hover:bg-surface-muted/70">
                      <td className="px-4 py-3 text-muted-foreground">{index + 1}</td>
                      <td className="px-4 py-3 font-medium text-foreground">{table}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <EmptyState
            icon="database"
            title={hasQueried ? "当前数据库中没有可用表" : "等待查询数据库表"}
            description={
              hasQueried
                ? "连接已成功，但 list_database_tables 没有返回用户数据表。"
                : "输入数据库连接信息后，将通过 Tauri 调用 list_database_tables 并展示真实表名。"
            }
          />
        )}
      </Card>
    </>
  );
}
