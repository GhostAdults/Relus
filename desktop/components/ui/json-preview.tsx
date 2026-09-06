"use client";

import { useEffect, useRef, useState } from "react";
import { EditorState, EditorSelection } from "@codemirror/state";
import { EditorView, keymap, placeholder } from "@codemirror/view";
import { Diagnostic, linter } from "@codemirror/lint";
import { indentWithTab } from "@codemirror/commands";
import { basicSetup } from "codemirror";
import { json } from "@codemirror/lang-json";
import { javascript } from "@codemirror/lang-javascript";
import { oneDark } from "@codemirror/theme-one-dark";

export interface JsonPreviewProps {
  id?: string;
  ariaLabel?: string;
  value: unknown;
  onChange?: (value: string) => void;
  placeholder?: string;
  darkMode?: boolean;
  rows?: number;
  showValidation?: boolean;
  language?: "json" | "javascript";
  height?: string | number;
  showMinimap?: boolean;
  readOnly?: boolean;
  className?: string;
}

function formatValue(value: unknown) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) ?? "";
  } catch {
    return "";
  }
}

function minimalChange(current: string, next: string) {
  let from = 0;
  while (from < current.length && from < next.length && current[from] === next[from]) from += 1;
  let currentTo = current.length;
  let nextTo = next.length;
  while (currentTo > from && nextTo > from && current[currentTo - 1] === next[nextTo - 1]) {
    currentTo -= 1;
    nextTo -= 1;
  }
  return { from, to: currentTo, insert: next.slice(from, nextTo) };
}

export function JsonPreview({
  id,
  ariaLabel,
  value,
  onChange,
  placeholder: placeholderText = "",
  darkMode: initialDarkMode = false,
  rows = 3,
  showValidation = true,
  language = "json",
  height,
  readOnly = false,
  className = "",
}: JsonPreviewProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const [darkMode, setDarkMode] = useState(initialDarkMode);
  const text = formatValue(value);
  onChangeRef.current = onChange;

  useEffect(() => {
    if (!editorRef.current) return;
    const hasHeight = height !== undefined;
    const heightValue = typeof height === "number" ? `${height}px` : height;
    const baseTheme = EditorView.baseTheme({
      "&": {
        border: "1px solid hsl(240 5.9% 90%)",
        background: "transparent",
      },
      "&.cm-focused": {
        outline: "none",
        borderColor: "hsl(210 100% 56%)",
      },
      ".cm-scroller": {
        background: "transparent",
      },
      ".cm-gutters": {
        background: "transparent",
        borderRight: "1px solid hsl(var(--border))",
        color: "hsl(var(--muted-foreground))",
      },
      ".cm-selectionBackground, .cm-content ::selection": {
        background: "hsl(var(--primary-editor) / 0.18)",
      },
      ".cm-selectionMatch": {
        background: "hsl(var(--primary-editor) / 0.12)",
      },
      ".cm-activeLine": {
        background: "hsl(var(--primary-editor) / 0.08)",
      },
      ".cm-activeLineGutter": {
        background: "hsl(var(--primary-editor) / 0.08)",
      },
    });
    const sizingTheme = EditorView.theme({
      "&": hasHeight ? { height: heightValue! } : { minHeight: `${Math.max(1, rows) * 18}px` },
      ".cm-scroller": { overflow: "auto" },
      ".cm-content": {
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace",
        fontSize: "14px",
      },
    });
    // Keep validation in the editor so malformed JSON is reported next to the
    // content while it is being edited. JavaScript previews and validation
    // disabled by the caller intentionally produce no diagnostics.
    const jsonLinter = linter((view): Diagnostic[] => {
      if (!showValidation || language !== "json") return [];

      const document = view.state.doc.toString();
      if (!document.trim()) return [];

      try {
        const parsed: unknown = JSON.parse(document);
        if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) return [];

        return [
          {
            from: 0,
            to: document.length,
            severity: "error",
            message: "JSON 内容必须是对象",
          },
        ];
      } catch (error) {
        return [
          {
            from: 0,
            to: document.length,
            severity: "error",
            message: error instanceof SyntaxError ? error.message : "JSON 格式无效",
          },
        ];
      }
    });
    const extensions = [
      basicSetup,
      keymap.of([indentWithTab]),
      language === "javascript" ? javascript() : json(),
      placeholder(placeholderText),
      baseTheme,
      sizingTheme,
      jsonLinter,
      EditorState.readOnly.of(readOnly),
      EditorView.editable.of(!readOnly),
      EditorView.contentAttributes.of(ariaLabel ? { "aria-label": ariaLabel } : {}),
      EditorView.updateListener.of((update) => {
        if (!readOnly && update.docChanged) onChangeRef.current?.(update.state.doc.toString());
      }),
    ];
    if (darkMode) {
      extensions.push(oneDark);
      extensions.push(
        EditorView.theme({
          "&": { border: "1px solid hsl(var(--border))", borderRadius: "0.5rem", background: "transparent" },
          "&.cm-focused": { outline: "none", borderColor: "hsl(var(--primary-editor))" },
          ".cm-scroller": { background: "transparent" },
          ".cm-gutters": {
            background: "transparent",
            borderRight: "1px solid hsl(var(--border))",
            color: "hsl(var(--muted-foreground))",
          },
          ".cm-selectionBackground, .cm-content ::selection": { background: "hsl(var(--primary-editor) / 0.18)" },
          ".cm-selectionMatch": { background: "hsl(var(--primary-editor) / 0.12)" },
          ".cm-activeLine": { background: "hsl(var(--primary-editor) / 0.08)" },
          ".cm-activeLineGutter": { background: "hsl(var(--primary-editor) / 0.08)" },
        }),
      );
    }
    // CodeMirror scopes theme selectors to the editor root. Add this last so
    // the focus indicator wins over both the light and one-dark themes.
    extensions.push(
      EditorView.theme({
        "&.cm-focused": {
          outline: "none",
          border: "1px dotted #000",
        },
      }),
    );
    const view = new EditorView({ state: EditorState.create({ doc: text, extensions }), parent: editorRef.current });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, [text, darkMode, language, placeholderText, readOnly, rows, height, ariaLabel, showValidation]);

  useEffect(() => {
    const view = viewRef.current;
    if (!view || view.state.doc.toString() === text) return;
    const changes = view.state.changes(minimalChange(view.state.doc.toString(), text));
    view.dispatch({
      changes,
      selection: EditorSelection.cursor(Math.min(text.length, view.state.selection.main.head)),
    });
  }, [text]);

  return (
    <div className={className}>
      <div className="mb-2 flex justify-end">
        <span
          role="button"
          tabIndex={0}
          onClick={() => setDarkMode((current) => !current)}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") setDarkMode((current) => !current);
          }}
          className="cursor-pointer text-xs text-primary hover:underline"
        >
          {darkMode ? "浅色模式" : "深色模式"}
        </span>
      </div>
      <div
        id={id}
        ref={editorRef}
        className="max-h-[520px] min-h-72 overflow-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
      />
    </div>
  );
}
