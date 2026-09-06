"use client";

import { Alert as HeroAlert, CloseButton } from "@heroui/react";
import type { AlertVariants } from "@heroui/styles";

type AlertStatus = AlertVariants["status"];

type AlertProps = {
  status?: AlertStatus;
  title?: string;
  message: string;
  onClose?: () => void;
};

export function Alert({ status = "danger", title, message, onClose }: AlertProps) {
  return (
    <HeroAlert status={status}>
      <HeroAlert.Indicator />
      <HeroAlert.Content>
        {title ? <HeroAlert.Title>{title}</HeroAlert.Title> : null}
        <HeroAlert.Description>{message}</HeroAlert.Description>
      </HeroAlert.Content>
      {onClose ? <CloseButton onPress={onClose} aria-label="关闭提示" /> : null}
    </HeroAlert>
  );
}
