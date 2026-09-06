"use client";

import { TextArea as HeroTextArea } from "@heroui/react";
import type { TextAreaProps } from "@heroui/react";

export type { TextAreaProps };

export function TextArea({ className = "", ...props }: TextAreaProps) {
  return (
    <HeroTextArea
      {...props}
      className={`min-h-24 w-full resize-y rounded-lg border border-border bg-surface px-[11px] py-[9px] text-foreground shadow-none outline-none transition-[border-color,box-shadow] duration-150 placeholder:text-subtle-foreground focus:border-primary focus:ring-3 focus:ring-primary/14 disabled:cursor-not-allowed disabled:bg-surface-muted disabled:text-muted-foreground ${className}`}
    />
  );
}
