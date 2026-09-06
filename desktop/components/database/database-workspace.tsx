"use client";

import { invoke, isTauri } from "@tauri-apps/api/core";
import { FormEvent, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Input } from "@/components/ui/input";
import { Form } from "@/components/ui/form";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { ErrorAlert } from "@/components/ui/error-alert";
import { Alert } from "@/components/ui/alert";
import { FormField } from "@/components/ui/form-field";
import { Icon } from "@/components/ui/icon";
import { StatusBadge } from "@/components/ui/status-badge";
import { databaseTypeIcons, databaseTypes } from "@/lib/relus-options";
import { SelectField } from "@/components/ui/select";

type DatabaseType = (typeof databaseTypes)[number]["value"];
const databaseSelectOptions = databaseTypes.map((option) => {
  const icon = databaseTypeIcons.find((item) => item.value === option.value);
  return { ...option, iconSrc: icon?.src, iconAlt: icon?.alt };
});
type WorkspaceTab = "tables" | "schema";
type ColumnMeta = { name: string; data_type: string; nullable: boolean };

function errorMessage(error: unknown) {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return "数据库操作失败，请检查连接信息后重试。";
}

export function DatabaseWorkspace() {
  const [databaseType, setDatabaseType] = useState<DatabaseType>("mysql");
  const [host, setHost] = useState("127.0.0.1");
  const [port, setPort] = useState("3306");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [databaseName, setDatabaseName] = useState("");
  const [isConnected, setIsConnected] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState("");
  const [columns, setColumns] = useState<ColumnMeta[]>([]);
  const [loadedSchemaTable, setLoadedSchemaTable] = useState("");
  const [activeTab, setActiveTab] = useState<WorkspaceTab>("tables");
  const [search, setSearch] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isSchemaLoading, setIsSchemaLoading] = useState(false);
  const [error, setError] = useState("");
  const [alert, setAlert] = useState<{ status: "danger" | "success"; message: string; title?: string } | null>(null);
  const schemaRequestId = useRef(0);

  const filteredTables = useMemo(() => {
    const keyword = search.trim().toLocaleLowerCase();
    return keyword ? tables.filter((table) => table.toLocaleLowerCase().includes(keyword)) : tables;
  }, [search, tables]);

  const connect = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextHost = host.trim();
    const nextPort = port.trim();
    const nextUsername = username.trim();
    const nextDatabaseName = databaseName.trim();
    const numericPort = Number(nextPort);

    if (!nextHost || !nextPort || !nextUsername || !nextDatabaseName) {
      setAlert({ status: "danger", title: "连接参数无效", message: "请填写主机 IP、端口、用户名和数据库名。" });
      return;
    }
    if (!Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setAlert({ status: "danger", title: "连接参数无效", message: "请输入 1 到 65535 之间的有效端口。" });
      return;
    }
    if (!isTauri()) {
      setAlert({
        status: "danger",
        title: "无法连接数据库",
        message: "当前页面运行在普通浏览器中，请在 Relus Tauri 桌面窗口中连接数据库。",
      });
      return;
    }

    const protocol = databaseType === "mysql" ? "mysql" : "postgres";
    const credentials = `${encodeURIComponent(nextUsername)}:${encodeURIComponent(password)}@`;
    const nextDbUrl = `${protocol}://${credentials}${nextHost}:${numericPort}/${encodeURIComponent(nextDatabaseName)}`;

    setIsLoading(true);
    setError("");
    try {
      const result = await invoke<string[]>("connect_database", { dbUrl: nextDbUrl, dbType: databaseType });
      setIsConnected(true);
      setTables(result);
      setSelectedTable(result[0] ?? "");
      setColumns([]);
      setLoadedSchemaTable("");
      setActiveTab("tables");
      setSearch("");
    } catch (queryError) {
      setAlert({ status: "danger", title: "连接失败", message: errorMessage(queryError) });
    } finally {
      setIsLoading(false);
    }
  };

  const testConnect = async (event: React.MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    const trimmedHost = host.trim();
    const trimmedPort = port.trim();
    const trimmedUsername = username.trim();
    const trimmedDatabaseName = databaseName.trim();
    const numericPort = Number(trimmedPort);
    if (!trimmedHost || !trimmedPort || !trimmedUsername || !trimmedDatabaseName) {
      setAlert({ status: "danger", title: "连接参数无效", message: "请填写主机 IP、端口、用户名和数据库名。" });
      return;
    }
    if (!Number.isInteger(numericPort) || numericPort < 1 || numericPort > 65535) {
      setAlert({ status: "danger", title: "连接参数无效", message: "请输入 1 到 65535 之间的有效端口。" });
      return;
    }
    if (!isTauri()) {
      setAlert({
        status: "danger",
        title: "无法连接数据库",
        message: "当前页面运行在普通浏览器中，请在 Relus 桌面窗口中连接数据库。",
      });
      return;
    }
    try {
      const protocol = databaseType === "mysql" ? "mysql" : "postgres";
      const credentials = `${encodeURIComponent(trimmedUsername)}:${encodeURIComponent(password)}@`;
      const dbUrl = `${protocol}://${credentials}${trimmedHost}:${numericPort}/${encodeURIComponent(trimmedDatabaseName)}`;
      await invoke<string[]>("connect_database", { dbUrl, dbType: databaseType });
      setAlert({ status: "success", title: "连接成功", message: `成功连接到 ${trimmedDatabaseName}` });
    } catch (queryError) {
      setAlert({ status: "danger", title: "连接失败", message: errorMessage(queryError) });
    }
  };

  const loadSchema = async (table: string) => {
    setSelectedTable(table);
    setActiveTab("schema");
    if (!table || loadedSchemaTable === table) return;

    const requestId = ++schemaRequestId.current;
    setIsSchemaLoading(true);
    setError("");
    setColumns([]);
    try {
      const result = await invoke<ColumnMeta[]>("get_database_table_schema", { table });
      if (requestId !== schemaRequestId.current) return;
      setColumns(result);
      setLoadedSchemaTable(table);
    } catch (schemaError) {
      if (requestId !== schemaRequestId.current) return;
      setLoadedSchemaTable("");
      setError(errorMessage(schemaError));
    } finally {
      if (requestId === schemaRequestId.current) setIsSchemaLoading(false);
    }
  };

  const showTab = (tab: WorkspaceTab) => {
    setActiveTab(tab);
    if (tab === "schema" && selectedTable) void loadSchema(selectedTable);
  };

  const disconnect = () => {
    schemaRequestId.current += 1;
    setIsConnected(false);
    setTables([]);
    setSelectedTable("");
    setColumns([]);
    setLoadedSchemaTable("");
    setError("");
  };

  const changeDatabaseType = (value: DatabaseType) => {
    setDatabaseType(value);
    setPort(value === "mysql" ? "3306" : "5432");
  };

  if (!isConnected) {
    const defaultPort = databaseType === "mysql" ? "3306" : "5432";
    return (
      <>
        {alert ? <Alert {...alert} onClose={() => setAlert(null)} /> : null}
        {error ? <ErrorAlert message={error} onClose={() => setError("")} /> : null}
        <Card className="mx-auto w-full max-w-3xl overflow-hidden">
          <div className="border-b border-border bg-surface-muted px-6 py-4">
            <h2 className="text-sm font-semibold text-foreground">数据库连接配置</h2>
            <p className="mt-1 text-sm text-muted-foreground"></p>
          </div>
          <Form className="grid gap-5 p-6 sm:grid-cols-2" onSubmit={connect}>
            <FormField label="类型">
              <SelectField
                options={databaseSelectOptions}
                value={databaseType}
                onChange={(value) => changeDatabaseType(value as DatabaseType)}
                disabled={isLoading}
              />
            </FormField>
            <FormField label="主机地址">
              <Input
                value={host}
                onChange={(event) => setHost(event.target.value)}
                placeholder="127.0.0.2"
                spellCheck={false}
                disabled={isLoading}
              />
            </FormField>
            <FormField label="端口号" description="">
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
            <FormField label="数据库">
              <Input
                value={databaseName}
                onChange={(event) => setDatabaseName(event.target.value)}
                placeholder="输入数据库名"
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
            <div className="flex items-center justify-between border-t border-border pt-5 sm:col-span-2">
              <Link
                href="#test-connect"
                onClick={testConnect}
                className="text-sm font-medium text-primary hover:underline"
              >
                TestConnect
              </Link>
              <Button type="submit" disabled={isLoading}>
                {isLoading ? "连接中…" : "连接"}
              </Button>
            </div>
          </Form>
        </Card>
      </>
    );
  }

  return (
    <>
      {alert ? <Alert {...alert} onClose={() => setAlert(null)} /> : null}
      {error ? <ErrorAlert message={error} onClose={() => setError("")} /> : null}
      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
          <div className="flex min-w-0 items-center gap-3">
            <StatusBadge tone="success">已连接</StatusBadge>
            <div className="min-w-0 text-sm">
              <span className="font-medium text-foreground">{databaseName}</span>
              <span className="ml-2 text-muted-foreground">
                {host}:{port} · {tables.length} 张表
              </span>
            </div>
          </div>
          <Button variant="secondary" onClick={disconnect}>
            更换连接
          </Button>
        </div>

        <div
          className="flex gap-1 border-b border-border bg-surface-muted px-4 pt-2"
          role="tablist"
          aria-label="数据库工具视图"
        >
          {(
            [
              ["tables", "表列表", "database"],
              ["schema", "表结构", "table"],
            ] as const
          ).map(([tab, label, icon]) => (
            <Button
              key={tab}
              type="button"
              role="tab"
              aria-selected={activeTab === tab}
              onClick={() => showTab(tab)}
              variant="ghost"
              className={`h-auto rounded-t-lg border px-4 py-2 ${activeTab === tab ? "-mb-px border-border border-b-surface bg-surface text-primary" : "border-transparent text-muted-foreground hover:text-foreground"}`}
            >
              <Icon name={icon} className="h-4 w-4" />
              {label}
            </Button>
          ))}
        </div>

        {tables.length === 0 ? (
          <EmptyState
            icon="database"
            title="当前数据库中没有可用表"
            description="连接已成功，但没有查询到用户数据表。"
          />
        ) : (
          <div className="grid min-h-[440px] md:grid-cols-[260px_minmax(0,1fr)]">
            <aside
              className="border-b border-border bg-surface-muted/50 md:border-b-0 md:border-r"
              aria-label="数据表列表"
            >
              <div className="border-b border-border p-3">
                <Input
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder="搜索表名"
                  aria-label="搜索表名"
                />
              </div>
              <div className="scrollbar max-h-64 overflow-y-auto p-2 md:max-h-[500px]">
                {filteredTables.map((table) => (
                  <Button
                    key={table}
                    type="button"
                    onClick={() => (activeTab === "schema" ? void loadSchema(table) : setSelectedTable(table))}
                    variant="ghost"
                    className={`h-auto w-full justify-start rounded-lg px-3 py-2.5 text-left ${selectedTable === table ? "bg-primary-soft font-medium text-primary" : "text-foreground hover:bg-surface-muted"}`}
                  >
                    <Icon name="table" className="h-4 w-4 shrink-0" />
                    <span className="truncate">{table}</span>
                  </Button>
                ))}
                {filteredTables.length === 0 ? (
                  <p className="px-3 py-8 text-center text-sm text-muted-foreground">没有匹配的表</p>
                ) : null}
              </div>
            </aside>

            <section className="min-w-0">
              {activeTab === "tables" ? (
                <TableList tables={tables} onOpenSchema={(table) => void loadSchema(table)} />
              ) : (
                <SchemaTable
                  table={selectedTable}
                  columns={columns}
                  loadedTable={loadedSchemaTable}
                  isLoading={isSchemaLoading}
                />
              )}
            </section>
          </div>
        )}
      </Card>
    </>
  );
}

function TableList({ tables, onOpenSchema }: { tables: string[]; onOpenSchema: (table: string) => void }) {
  return (
    <>
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold text-foreground">表列表</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">当前数据库中的全部用户表</p>
        </div>
        <StatusBadge tone="primary">{tables.length} 张表</StatusBadge>
      </div>
      <div className="scrollbar overflow-x-auto">
        <table className="w-full min-w-[480px] text-left text-sm">
          <thead className="border-b border-border bg-surface-muted text-xs text-muted-foreground">
            <tr>
              <th className="w-20 px-4 py-2.5 font-medium">序号</th>
              <th className="px-4 py-2.5 font-medium">表名</th>
              <th className="w-28 px-4 py-2.5 font-medium">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {tables.map((table, index) => (
              <tr key={table} className="hover:bg-surface-muted/70">
                <td className="px-4 py-3 text-muted-foreground">{index + 1}</td>
                <td className="px-4 py-3 font-medium text-foreground">{table}</td>
                <td className="px-4 py-3">
                  <Button
                    type="button"
                    variant="ghost"
                    className="h-auto p-0 text-sm font-medium text-primary hover:bg-transparent hover:underline"
                    onClick={() => onOpenSchema(table)}
                  >
                    查看结构
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function SchemaTable({
  table,
  columns,
  loadedTable,
  isLoading,
}: {
  table: string;
  columns: ColumnMeta[];
  loadedTable: string;
  isLoading: boolean;
}) {
  return (
    <>
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold text-foreground">{table}</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">字段、原生数据类型与可空信息</p>
        </div>
        {loadedTable === table ? <StatusBadge tone="primary">{columns.length} 个字段</StatusBadge> : null}
      </div>
      {isLoading ? (
        <div className="flex min-h-56 items-center justify-center text-sm text-muted-foreground" role="status">
          正在加载表结构…
        </div>
      ) : columns.length > 0 && loadedTable === table ? (
        <div className="scrollbar overflow-x-auto">
          <table className="w-full min-w-[560px] text-left text-sm">
            <thead className="border-b border-border bg-surface-muted text-xs text-muted-foreground">
              <tr>
                <th className="w-20 px-4 py-2.5 font-medium">序号</th>
                <th className="px-4 py-2.5 font-medium">字段名</th>
                <th className="px-4 py-2.5 font-medium">数据类型</th>
                <th className="w-28 px-4 py-2.5 font-medium">允许为空</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {columns.map((column, index) => (
                <tr key={column.name}>
                  <td className="px-4 py-3 text-muted-foreground">{index + 1}</td>
                  <td className="px-4 py-3 font-medium text-foreground">{column.name}</td>
                  <td className="px-4 py-3 text-muted-foreground">{column.data_type}</td>
                  <td className="px-4 py-3">
                    <StatusBadge tone={column.nullable ? "neutral" : "warning"}>
                      {column.nullable ? "是" : "否"}
                    </StatusBadge>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState icon="table" title="暂无字段信息" description="该表没有返回可展示的字段元数据。" />
      )}
    </>
  );
}
