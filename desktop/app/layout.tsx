import type { Metadata } from "next";
import localFont from "next/font/local";

import { AppShell } from "@/components/layout/app-shell";
import { ToastProvider } from "@/components/ui/toast-provider";

import "./globals.css";

const mapleMono = localFont({
  src: [
    {
      path: "./fonts/MapleMono-NF-CN-Regular.ttf",
      weight: "400",
      style: "normal",
    },
    {
      path: "./fonts/MapleMono-NF-CN-Medium.ttf",
      weight: "500",
      style: "normal",
    },
    {
      path: "./fonts/MapleMono-NF-CN-SemiBold.ttf",
      weight: "600",
      style: "normal",
    },
  ],
  variable: "--font-maple-mono",
  display: "swap",
});

const miSans = localFont({
  src: "./fonts/MiSans-Semibold.ttf",
  variable: "--font-mi-sans",
  weight: "600",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Relus 数据同步",
  description: "Relus 数据同步任务配置与管理界面",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="zh-CN" suppressHydrationWarning>
      <body className={`${mapleMono.variable} ${miSans.variable} antialiased`}>
        <ToastProvider placement="top">
          <AppShell>{children}</AppShell>
        </ToastProvider>
      </body>
    </html>
  );
}
