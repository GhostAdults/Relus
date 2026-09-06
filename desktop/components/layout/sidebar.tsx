"use client";

import { usePathname } from "next/navigation";
import { Fragment, useState } from "react";

import { SidebarButton } from "@/components/ui/sidebar-button";
import { navigationGroups, settingsNavigation, type NavigationItem } from "@/lib/navigation";

function isActive(pathname: string, item: NavigationItem) {
  const normalizedPathname = pathname === "/" ? pathname : pathname.replace(/\/+$/, "");
  const normalizedHref = item.href === "/" ? item.href : item.href.replace(/\/+$/, "");

  if (item.exact) {
    return normalizedPathname === normalizedHref;
  }
  return normalizedPathname === normalizedHref || normalizedPathname.startsWith(`${normalizedHref}/`);
}

export function Sidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);

  const renderItem = (item: NavigationItem) => {
    const active = isActive(pathname, item);

    return (
      <SidebarButton
        key={item.href}
        href={item.href}
        label={item.label}
        icon={item.icon}
        active={active}
        collapsed={collapsed}
      />
    );
  };

  return (
    <aside className="app-sidebar" data-collapsed={collapsed}>
      <nav className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto py-4" aria-label="主导航">
        {navigationGroups.map((group) => (
          <div key={group.label} className="sidebar-button-group">
            <div className="sidebar-label px-3 pb-1 text-[11px] font-semibold uppercase tracking-wider text-subtle-foreground">
              {group.label}
            </div>
            {group.items.map((item, index) => (
              <Fragment key={item.href}>
                {index > 0 ? <div className="sidebar-button-separator" aria-hidden="true" /> : null}
                {renderItem(item)}
              </Fragment>
            ))}
          </div>
        ))}
      </nav>

      <div className="py-3">
        {renderItem(settingsNavigation)}
        <div className="sidebar-button-separator" aria-hidden="true" />
        <SidebarButton
          label={collapsed ? "展开侧边栏" : "收起侧边栏"}
          icon={collapsed ? "chevron-right" : "chevron-left"}
          onClick={() => setCollapsed((value) => !value)}
          className="w-full"
          collapsed={collapsed}
        />
      </div>
    </aside>
  );
}
