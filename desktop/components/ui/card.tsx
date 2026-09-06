import type { ReactNode } from "react";
import { Card as HeroCard } from "@heroui/react";

type CardProps = {
  children: ReactNode;
  className?: string;
};

export function Card({ children, className = "" }: CardProps) {
  return <HeroCard className={`rounded-xl border border-border bg-surface ${className}`}>{children}</HeroCard>;
}
