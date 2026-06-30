// Run-history persistence helpers.
//
// localStorage is the always-on store: completed psycopg and pgbench runs are
// auto-captured here (the "current session" list, which survives reloads). Lakebase
// is the durable archive — the History tab pushes these session runs into a named
// Lakebase table via the /history/lakebase/* endpoints. This module owns the browser
// store plus the small preference (target schema/table + Lakebase consent).

export type Engine = "psycopg" | "pgbench";

export interface SavedRun {
  id: string;
  created_at: string;
  engine: Engine;
  label?: string | null;
  project?: string | null;
  // Engine-specific run parameters: psycopg {concurrency_level}; pgbench
  // {clients, jobs, duration_seconds, protocol, ...}.
  config: Record<string, unknown>;
  queries: { identifier: string; content: string }[];
  // Engine-specific result blobs (TestReportOut for psycopg, pgbench_results for
  // pgbench). Both carry a `per_query` array, so the UI renders either.
  baseline_report: Record<string, unknown> | null;
  optimized_report: Record<string, unknown> | null;
  index_ddls: string[];
  created_by?: string | null;
}

export interface HistoryPref {
  schema: string;
  table: string;
  lakebaseConsented: boolean;
}

const PREF_KEY = "lakebase_accel_history_pref";
const DATA_KEY = "lakebase_accel_history";
const MAX_BROWSER_RUNS = 200;

// Dedicated, least-privilege schema the app service principal owns (see backend
// history.DEFAULT_SCHEMA / DEFAULT_TABLE). Keeps the SP confined to its own schema.
export const DEFAULT_SCHEMA = "accelerator_history";
export const DEFAULT_TABLE = "_accelerator_run_history";

export const DEFAULT_PREF: HistoryPref = {
  schema: DEFAULT_SCHEMA,
  table: DEFAULT_TABLE,
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

// Normalize older records (saved before the engine/config fields existed) so the UI
// can treat every run uniformly.
function normalize(run: Partial<SavedRun> & Record<string, unknown>): SavedRun {
  const engine = (run.engine as Engine) ?? "psycopg";
  let config = (run.config as Record<string, unknown>) ?? {};
  if (Object.keys(config).length === 0 && run.concurrency_level != null) {
    config = { concurrency_level: run.concurrency_level };
  }
  return {
    id: String(run.id ?? newRunId()),
    created_at: String(run.created_at ?? new Date().toISOString()),
    engine,
    label: run.label ?? null,
    project: run.project ?? null,
    config,
    queries: (run.queries as SavedRun["queries"]) ?? [],
    baseline_report: (run.baseline_report as Record<string, unknown> | null) ?? null,
    optimized_report: (run.optimized_report as Record<string, unknown> | null) ?? null,
    index_ddls: (run.index_ddls as string[]) ?? [],
    created_by: run.created_by ?? null,
  };
}

export function loadBrowserRuns(): SavedRun[] {
  try {
    const raw = localStorage.getItem(DATA_KEY);
    if (!raw) return [];
    return (JSON.parse(raw) as Record<string, unknown>[]).map(normalize);
  } catch {
    return [];
  }
}

function writeBrowserRuns(runs: SavedRun[]): void {
  try {
    localStorage.setItem(DATA_KEY, JSON.stringify(runs.slice(0, MAX_BROWSER_RUNS)));
  } catch {
    /* storage full — silently drop (POC) */
  }
}

// Upsert by id: a re-run after applying indexes updates the same entry (so a
// before/after pair is one row), while a fresh run prepends a new entry.
export function saveBrowserRun(run: SavedRun): void {
  const existing = loadBrowserRuns();
  const idx = existing.findIndex((r) => r.id === run.id);
  if (idx >= 0) existing[idx] = run;
  else existing.unshift(run);
  writeBrowserRuns(existing);
}

export function deleteBrowserRun(id: string): void {
  writeBrowserRuns(loadBrowserRuns().filter((r) => r.id !== id));
}

export function clearBrowserRuns(): void {
  try {
    localStorage.removeItem(DATA_KEY);
  } catch {
    /* ignore */
  }
}

export function newRunId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ??
    `run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
  );
}
