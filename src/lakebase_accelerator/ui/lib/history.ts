// Run-history persistence helpers.
//
// Two destinations: "browser" (this localStorage module) and "lakebase" (the
// /history/lakebase/* endpoints, called from components via generated hooks).
// This module owns the *preference* (which destination, target schema, whether
// the user consented to Lakebase writes) plus the browser-local run store.

import type { TestReportOut } from "@/lib/api";

export type Destination = "off" | "browser" | "lakebase";

export interface SavedRun {
  id: string;
  created_at: string;
  label?: string | null;
  project?: string | null;
  concurrency_level?: number | null;
  queries: { identifier: string; content: string }[];
  baseline_report: TestReportOut | null;
  optimized_report: TestReportOut | null;
  index_ddls: string[];
  created_by?: string | null;
}

export interface HistoryPref {
  destination: Destination;
  schema: string;
  lakebaseConsented: boolean;
}

const PREF_KEY = "lakebase_accel_history_pref";
const DATA_KEY = "lakebase_accel_history";
const MAX_BROWSER_RUNS = 100;

// Dedicated, least-privilege schema the app service principal owns (see backend
// history.DEFAULT_SCHEMA). Keeps the SP confined to its own history table.
export const DEFAULT_SCHEMA = "accelerator_history";

export const DEFAULT_PREF: HistoryPref = {
  destination: "browser",
  schema: DEFAULT_SCHEMA,
  lakebaseConsented: false,
};

export function loadPref(): HistoryPref {
  try {
    const raw = localStorage.getItem(PREF_KEY);
    if (!raw) return DEFAULT_PREF;
    return { ...DEFAULT_PREF, ...(JSON.parse(raw) as Partial<HistoryPref>) };
  } catch {
    return DEFAULT_PREF;
  }
}

export function savePref(pref: HistoryPref): void {
  try {
    localStorage.setItem(PREF_KEY, JSON.stringify(pref));
  } catch {
    /* storage full / disabled — preference simply won't persist */
  }
}

export function loadBrowserRuns(): SavedRun[] {
  try {
    const raw = localStorage.getItem(DATA_KEY);
    return raw ? (JSON.parse(raw) as SavedRun[]) : [];
  } catch {
    return [];
  }
}

export function saveBrowserRun(run: SavedRun): void {
  const runs = [run, ...loadBrowserRuns()].slice(0, MAX_BROWSER_RUNS);
  try {
    localStorage.setItem(DATA_KEY, JSON.stringify(runs));
  } catch {
    /* storage full — drop the oldest until it fits is overkill for a POC */
  }
}

export function newRunId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ??
    `run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
  );
}
