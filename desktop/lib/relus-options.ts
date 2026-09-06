export const databaseTypes = [
  { value: "mysql", label: "MySQL" },
  { value: "postgres", label: "PostgreSQL" },
] as const;

export const databaseTypeIcons = [{ value: "mysql", src: "/icons/mysql.png", alt: "MySQL" }] as const;

export const syncModes = [
  { value: "fullsnapshot", label: "全量快照" },
  { value: "incremental", label: "增量同步" },
  { value: "mix", label: "混合同步" },
] as const;

export const writerModes = [
  { value: "insert", label: "Insert" },
  { value: "upsert", label: "Upsert" },
  { value: "update", label: "Update" },
  { value: "delete", label: "Delete" },
] as const;

export const logicalTypes = [
  { value: "text", label: "Text" },
  { value: "int", label: "Integer" },
  { value: "float", label: "Float" },
  { value: "bool", label: "Boolean" },
  { value: "decimal", label: "Decimal" },
  { value: "timestamp", label: "Timestamp" },
  { value: "json", label: "JSON" },
] as const;
