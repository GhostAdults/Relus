"use client";

import { Toast } from "@heroui/react";
import type { ComponentProps, ReactNode } from "react";

export { toast } from "@heroui/react";

type ToastPlacement = ComponentProps<typeof Toast.Provider>["placement"];

type ToastProviderProps = {
  children: ReactNode;
  placement?: ToastPlacement;
};

export function ToastProvider({ children, placement = "bottom" }: ToastProviderProps) {
  return (
    <>
      <Toast.Provider placement={placement} />
      {children}
    </>
  );
}
