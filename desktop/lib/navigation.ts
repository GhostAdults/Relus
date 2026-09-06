import type { IconName } from "@/components/ui/icon";

export type NavigationItem = {
  label: string;
  href: string;
  icon: IconName;
  exact?: boolean;
};

export type NavigationGroup = {
  label: string;
  items: NavigationItem[];
};

export const navigationGroups: NavigationGroup[] = [
  {
    label: "工作区",
    items: [
      { label: "概览", href: "/", icon: "dashboard", exact: true },
      { label: "同步任务", href: "/tasks/new", icon: "plus" },
      { label: "任务记录", href: "/tasks", icon: "tasks", exact: true },
    ],
  },
  {
    label: "数据工具",
    items: [
      { label: "表数据工具", href: "/tools/database/tables", icon: "database" },
      { label: "映射生成", href: "/tools/mapping/generate", icon: "layers" },
      { label: "映射编辑", href: "/tools/mapping/edit", icon: "mapping" },
      { label: "API工具", href: "/tools/api-source", icon: "api" },
    ],
  },
];

export const settingsNavigation: NavigationItem = {
  label: "运行设置",
  href: "/settings",
  icon: "settings",
};
