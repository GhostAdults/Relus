"use client";

import { Input as HeroInput, Label } from "@heroui/react";
import type { InputProps } from "@heroui/react";

export { Label };
export type { InputProps };

export function Input({ className = "", ...props }: InputProps) {
  return (
    <HeroInput
      {...props}
      className={`h-[38px] w-full rounded-lg border border-border bg-surface px-[11px] text-foreground shadow-none outline-none transition-[border-color,box-shadow] duration-150 placeholder:text-subtle-foreground focus:border-primary focus:ring-3 focus:ring-primary/14 disabled:cursor-not-allowed disabled:bg-surface-muted disabled:text-muted-foreground ${className}`}
    />
  );
}
