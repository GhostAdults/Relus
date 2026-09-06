"use client";

import { Button as HeroButton, type ButtonProps as HeroButtonProps } from "@heroui/react";
import type { ButtonHTMLAttributes, ReactNode } from "react";

const variants = {
  primary: "bg-primary text-white hover:bg-primary-hover",
  secondary: "border border-border bg-surface text-foreground hover:bg-surface-muted",
  ghost: "text-muted-foreground hover:bg-surface-muted hover:text-foreground",
} as const;

type ButtonProps = Omit<ButtonHTMLAttributes<HTMLButtonElement>, "disabled" | "className" | "value"> &
  Pick<HeroButtonProps, "onPress" | "isPending" | "isIconOnly" | "fullWidth" | "size"> & {
    children: ReactNode;
    variant?: keyof typeof variants;
    className?: string;
    disabled?: boolean;
  };

export function Button({
  children,
  className = "",
  type = "button",
  variant = "primary",
  disabled = false,
  ...props
}: ButtonProps) {
  const heroProps = props as unknown as HeroButtonProps;

  return (
    <HeroButton
      type={type}
      variant={variant}
      className={`inline-flex h-9 items-center justify-center gap-2 rounded-lg px-3.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30 disabled:cursor-not-allowed disabled:opacity-45 ${variants[variant]} ${className}`}
      isDisabled={disabled}
      {...heroProps}
    >
      {children}
    </HeroButton>
  );
}
