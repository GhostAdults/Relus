"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export type ToolSubnavItem = {
  label: string;
  href: string;
};

export function ToolSubnav({ items }: { items: ToolSubnavItem[] }) {
  const pathname = usePathname();

  return (
    <nav className="flex flex-wrap gap-1 rounded-lg border border-border bg-surface-muted p-1" aria-label="工具子导航">
      {items.map((item) => {
        const active = pathname === item.href;
        return (
          <Link
            key={item.href}
            href={item.href}
            aria-current={active ? "page" : undefined}
            className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
              active ? "bg-surface text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}
