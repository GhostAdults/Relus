"use client";

import { Chip, Link } from "@heroui/react";
import { ChevronDown, CircleCheckFill, CircleFill, Clock, Xmark } from "@gravity-ui/icons";
import type { ReactNode } from "react";

export function ChipWithIcon({
  children,
  icon,
  color,
  onClick,
}: {
  children: ReactNode;
  icon?: ReactNode;
  color?: "default" | "success" | "danger" | "accent";
  onClick?: () => void;
}) {
  const content = (
    <Chip color={color} variant="tertiary" className={onClick ? "cursor-pointer" : undefined}>
      {icon}
      <Chip.Label>
        <Link
          href="#"
          onPress={() => {
            onClick?.();
          }}
        >
          {children}
        </Link>
      </Chip.Label>
    </Chip>
  );
  return content;
}
