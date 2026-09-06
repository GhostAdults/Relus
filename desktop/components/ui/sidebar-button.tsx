"use client";

import Link from "next/link";
import type { MouseEventHandler } from "react";

import { Icon, type IconName } from "@/components/ui/icon";

type SidebarButtonProps = {
  label: string;
  icon: IconName;
  href?: string;
  active?: boolean;
  collapsed?: boolean;
  className?: string;
  onClick?: MouseEventHandler<HTMLButtonElement>;
};

function SidebarButtonContent({ label, icon }: Pick<SidebarButtonProps, "label" | "icon">) {
  return (
    <>
      <span className="sidebar-button-content sidebar-button-content-one">
        <Icon name={icon} className="h-[18px] w-[18px] shrink-0" />
        <span className="sidebar-label truncate">{label}</span>
      </span>
      <span className="sidebar-button-content sidebar-button-content-two" aria-hidden="true">
        <Icon name={icon} className="h-[18px] w-[18px] shrink-0" />
        <span className="sidebar-label truncate">{label}</span>
      </span>
    </>
  );
}

export function SidebarButton({
  label,
  icon,
  href,
  active = false,
  collapsed = false,
  className = "",
  onClick,
}: SidebarButtonProps) {
  const classes = `sidebar-nav-button ${className}`;
  const content = <SidebarButtonContent label={label} icon={icon} />;

  if (href) {
    return (
      <div className="sidebar-button-wrap" data-active={active}>
        <Link
          href={href}
          aria-current={active ? "page" : undefined}
          aria-label={label}
          title={collapsed ? label : undefined}
          className={classes}
          data-active={active}
        >
          {content}
        </Link>
      </div>
    );
  }

  return (
    <div className="sidebar-button-wrap" data-active="false">
      <button
        type="button"
        onClick={onClick}
        className={classes}
        aria-label={label}
        title={collapsed ? label : undefined}
      >
        {content}
      </button>
    </div>
  );
}
