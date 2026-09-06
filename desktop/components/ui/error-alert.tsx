"use client";

import { Alert, CloseButton } from "@heroui/react";

type ErrorAlertProps = {
  message: string;
  onClose: () => void;
};

export function ErrorAlert({ message, onClose }: ErrorAlertProps) {
  return (
    <Alert status="danger">
      <Alert.Indicator />
      <Alert.Content>
        <Alert.Title>连接失败</Alert.Title>
        <Alert.Description>{message}</Alert.Description>
      </Alert.Content>
      <CloseButton onPress={onClose} aria-label="关闭错误提示" />
    </Alert>
  );
}
