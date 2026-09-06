"use client";

import { invoke, isTauri } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";

import { databaseTypes } from "@/lib/relus-options";

export type SourceTypeOption = {
  value: string;
  label: string;
  category: string;
};

// 浏览器预览环境无法调用 Tauri command，退回到已确认可用的 RDBMS 选项。
const browserFallback: SourceTypeOption[] = databaseTypes.map((option) => ({
  value: option.value,
  label: option.label,
  category: "rdbms",
}));

type RawSourceType = {
  value: string;
  category: string;
};

export function useSourceTypes() {
  const [sourceTypes, setSourceTypes] = useState<SourceTypeOption[]>(browserFallback);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!isTauri()) return;

    let active = true;
    invoke<RawSourceType[]>("list_source_types")
      .then((options) => {
        if (active) setSourceTypes(options.map((option) => ({ ...option, label: option.value })));
      })
      .catch((loadError) => {
        if (active) setError(`数据源类型加载失败: ${loadError}`);
      });

    return () => {
      active = false;
    };
  }, []);

  return { sourceTypes, error };
}
