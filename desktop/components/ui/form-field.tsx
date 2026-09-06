import type { ReactNode } from "react";
import { Description, Label, TextField } from "@heroui/react";

type FormFieldProps = {
  label: string;
  description?: string;
  children: ReactNode;
};

export function FormField({ label, description, children }: FormFieldProps) {
  return (
    <TextField className="grid gap-1.5 text-sm">
      <Label className="font-medium text-foreground">{label}</Label>
      {children}
      {description ? (
        <Description className="text-xs leading-5 text-muted-foreground">{description}</Description>
      ) : null}
    </TextField>
  );
}
