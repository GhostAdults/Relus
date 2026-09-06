"use client";

import { Avatar, ListBox, Select } from "@heroui/react";
import type { Key, ReactNode } from "react";
import styles from "./scss/select.module.css";

export type SelectOption = { value: string; label: ReactNode; iconSrc?: string; iconAlt?: string };

type SelectFieldProps = {
  options: readonly SelectOption[];
  value?: string;
  defaultValue?: string;
  onChange?: (value: string) => void;
  disabled?: boolean;
  variant?: "primary" | "secondary";
};
export function SelectField({ options, value, defaultValue, onChange, disabled, variant }: SelectFieldProps) {
  const handleChange = (key: Key | null) => {
    onChange?.(key == null ? "" : String(key));
  };

  return (
    <Select
      className={styles.root}
      value={value}
      defaultValue={defaultValue}
      variant={variant}
      placeholder="选择数据类型"
      onChange={handleChange}
      isDisabled={disabled}
    >
      <Select.Trigger className={styles.trigger}>
        <Select.Value className={styles.value}>
          {({ selectedText }) => {
            const selected = options.find((option) => option.value === value);

            // 没有选择任何值时，手动显示 placeholder
            if (!selected) {
              return <span className="text-default-500">选择数据类型</span>;
            }

            return (
              <span className="flex items-center gap-2">
                {selected.iconSrc && (
                  <Avatar className="size-6" size="sm">
                    <Avatar.Image
                      className="size-6 object-cover"
                      src={selected.iconSrc}
                      alt={selected.iconAlt ?? String(selected.label)}
                    />
                  </Avatar>
                )}

                <span>{selectedText}</span>
              </span>
            );
          }}
        </Select.Value>

        <Select.Indicator className={styles.indicator} />
      </Select.Trigger>

      <Select.Popover className={styles.popover}>
        <ListBox className={styles.list}>
          {options.map((option) => (
            <ListBox.Item className={styles.item} key={option.value} id={option.value} textValue={String(option.label)}>
              {option.iconSrc && (
                <Avatar className="size-6" size="sm">
                  <Avatar.Image
                    className="size-6 object-cover"
                    src={option.iconSrc}
                    alt={option.iconAlt ?? String(option.label)}
                  />
                </Avatar>
              )}

              {option.label}

              <ListBox.ItemIndicator />
            </ListBox.Item>
          ))}
        </ListBox>
      </Select.Popover>
    </Select>
  );
}
