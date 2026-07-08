import { useEffect, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { toast } from "sonner";
import {
  Calculator,
  Search,
  Database,
  Info,
  Plus,
  Trash2,
  CheckCircle2,
  AlertTriangle,
  ExternalLink,
  RefreshCw,
} from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  useRecommendSize,
  useCreateProject,
  useGetProjectInfo,
  useListLakebaseProjects,
  useListLakebaseBranches,
  useCreateSyncedTable,
  useCheckSyncRequirements,
  useGetSyncedTableStatus,
  useGetTableSize,
  useListWarehouses,
  useGetWorkspaceInfo,
  type SizingOut,
  type SyncCheckOut,
  type SyncStatusOut,
} from "@/lib/api";

export const Route = createFileRoute("/deployment")({
  component: DeploymentPage,
});

type Mode = "create" | "existing";

const SYNC_MODES = {
  SNAPSHOT: {
    label: "Snapshot",
    use: "Initial setup, historical/static tables, or full refreshes.",
    detail:
      "One-time full copy of the source. Re-run manually to refresh. No source requirements. ~10× more efficient than incremental when you change >10% of rows per refresh.",
    requiresCdf: false,
    // 1 table per pipeline so you can re-snapshot one table without re-syncing all.
    strategy: "1 table per pipeline — don't reuse pipelines (re-snapshot one table without re-syncing everything).",
    cost: "Every run re-copies the whole table (~2,000 rows/s/CU). Cost scales with table size × refresh frequency.",
  },
  TRIGGERED: {
    label: "Triggered",
    use: "Dashboards / data refreshed on a schedule (hourly, daily).",
    detail:
      "Incremental updates on demand or on a schedule. Requires Change Data Feed (CDF) on the source. Supports only additive schema changes.",
    requiresCdf: true,
    strategy: "1 table per pipeline.",
    cost: "One-time initial snapshot, then you pay only to process changed rows (~150 rows/s/CU incremental).",
  },
  CONTINUOUS: {
    label: "Continuous",
    use: "Live applications needing seconds-fresh data.",
    detail:
      "Real-time streaming with seconds of latency. Highest cost, minimum 15s intervals. Requires CDF on the source. Supports only additive schema changes.",
    requiresCdf: true,
    // Pipeline runs 24/7, so bundle many tables into one to amortize the fixed cost.
    strategy: "MANY tables per pipeline — the pipeline runs 24/7, so bundling amortizes the fixed compute cost across tables.",
    cost: "Fixed ~730 hrs/month of serverless pipeline compute regardless of data volume. One table per continuous pipeline is an antipattern — bundle ~10s of tables.",
  },
} as const;

type SyncPolicy = keyof typeof SYNC_MODES;

interface SyncRow {
  source_table_full_name: string;
  target_uc_name: string;
  primary_key_columns: string;
  scheduling_policy: SyncPolicy;
  database: string;
  // Target branch resource name (projects/<id>/branches/<id>). Blank = inferred from a
  // database catalog; required for a standard catalog.
  branch: string;
  storage_catalog: string;
  storage_schema: string;
  check?: SyncCheckOut;
  // Persistent result of the last create attempt (shown inline, not as a toast).
  result?: { ok: boolean; detail: string };
  // Last uncompressed-size measurement of the source table.
  size?: { ok: boolean; message: string };
  // Live replication status of the synced table (pipeline id, state, last sync).
  status?: SyncStatusOut;
}

const emptyRow: SyncRow = {
  source_table_full_name: "",
  target_uc_name: "",
  primary_key_columns: "",
  scheduling_policy: "SNAPSHOT",
  database: "",
  branch: "",
  storage_catalog: "",
  storage_schema: "",
};

// Quickstart default: sync the workspace-portable TPC-DS sample so users can get
// going without hunting for a source table. Only the target UC name needs the
// user's own Lakebase database catalog filled in.
const sampleRow: SyncRow = {
  ...emptyRow,
  source_table_full_name: "samples.tpcds_sf1.store_sales",
  target_uc_name: "",
  primary_key_columns: "ss_item_sk,ss_ticket_number",
  scheduling_policy: "SNAPSHOT",
};

function InfoTip({ text }: { text: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button type="button" className="text-muted-foreground hover:text-foreground" tabIndex={-1}>
          <Info className="h-3.5 w-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">{text}</TooltipContent>
    </Tooltip>
  );
}

function LabelWithTip({ label, tip }: { label: string; tip: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <Label>{label}</Label>
      <InfoTip text={tip} />
    </div>
  );
}

function DeploymentPage() {
  const [mode, setMode] = useState<Mode>("create");

  return (
    <TooltipProvider delayDuration={150}>
      <div>
        <PageHeader
          title="Deployment"
          description="Size an autoscaling endpoint and set up a project, then sync Delta tables in bulk. For rich single-table creation, the app links you to the native Databricks dialog."
        />
        <div className="space-y-6 p-8">
          <Card>
            <CardHeader>
              <CardTitle>What do you want to do?</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-2 sm:max-w-sm">
                <Label>Project</Label>
                <Select value={mode} onValueChange={(v) => setMode(v as Mode)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="create">Create a new project</SelectItem>
                    <SelectItem value="existing">Use an existing project</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </CardContent>
          </Card>

          {mode === "create" ? <CreateProjectFlow /> : <ExistingProjectFlow />}
        </div>
      </div>
    </TooltipProvider>
  );
}

// --- Create new project ------------------------------------------------------

function CreateProjectFlow() {
  const [reads, setReads] = useState(40000);
  const [bulk, setBulk] = useState(50000);
  const [cont, setCont] = useState(3000);
  const [sizing, setSizing] = useState<SizingOut | null>(null);

  const [projectId, setProjectId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [minCu, setMinCu] = useState(0.5);
  const [maxCu, setMaxCu] = useState(4);
  const [createdProject, setCreatedProject] = useState<string | null>(null);

  const recommend = useRecommendSize();
  const create = useCreateProject();

  const spread = maxCu - minCu;
  const cuValid = maxCu >= minCu && spread <= 8 && minCu >= 0.5 && maxCu <= 32;

  const onRecommend = async () => {
    const res = await recommend.mutateAsync({
      reads_per_second: reads,
      bulk_writes_per_second: bulk,
      continuous_writes_per_second: cont,
    });
    setSizing(res.data);
    setMinCu(res.data.recommended_min_cu);
    setMaxCu(res.data.recommended_max_cu);
  };

  const onCreate = async () => {
    try {
      const res = await create.mutateAsync({
        project_id: projectId.trim(),
        display_name: displayName.trim() || projectId.trim(),
        min_cu: minCu,
        max_cu: maxCu,
        pg_version: 16,
      });
      if (res.data.ok) {
        setCreatedProject(res.data.name ?? projectId);
        toast.success(res.data.detail);
      } else {
        toast.error(res.data.detail);
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Calculator className="h-4 w-4" /> 1. Size the compute
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-3">
            <div className="grid gap-2">
              <LabelWithTip
                label="Reads / sec"
                tip="Peak read queries per second (SELECT QPS) the database must serve. Planning heuristic: ~10,000 reads/sec per CU."
              />
              <Input type="number" value={reads} onChange={(e) => setReads(Number(e.target.value))} />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Bulk writes / sec"
                tip="Rows/sec written in batch/bulk loads — reverse-ETL sync refreshes, large INSERT…SELECT. High sequential throughput: ~14,000 rows/sec per CU."
              />
              <Input type="number" value={bulk} onChange={(e) => setBulk(Number(e.target.value))} />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Continuous writes / sec"
                tip="Rows/sec from steady-state transactional writes (OLTP INSERT/UPDATE/DELETE). Lower per-CU throughput due to WAL + index maintenance: ~1,500 rows/sec per CU."
              />
              <Input type="number" value={cont} onChange={(e) => setCont(Number(e.target.value))} />
            </div>
          </div>
          <Button onClick={onRecommend} disabled={recommend.isPending}>
            {recommend.isPending ? "Calculating…" : "Recommend CU"}
          </Button>
          {sizing && (
            <div className="rounded-lg border bg-muted/30 p-4">
              <div className="text-2xl font-semibold">
                {sizing.recommended_min_cu} – {sizing.recommended_max_cu} CU
              </div>
              <p className="mt-1 text-sm text-muted-foreground">{sizing.rationale}</p>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-4 w-4" /> 2. Create the project
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="grid gap-2">
              <LabelWithTip
                label="Project ID"
                tip="1–63 chars, lowercase letters, numbers, and hyphens; must start with a letter. Becomes the project's resource name."
              />
              <Input
                placeholder="my-lakebase-app"
                value={projectId}
                onChange={(e) => setProjectId(e.target.value)}
              />
            </div>
            <div className="grid gap-2">
              <Label>Display name</Label>
              <Input
                placeholder="My Lakebase App"
                value={displayName}
                onChange={(e) => setDisplayName(e.target.value)}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip label="Min CU" tip="Floor the compute scales down to (≥ 0.5). Set high enough to keep your working set cached in RAM." />
              <Input
                type="number"
                step="0.5"
                value={minCu}
                onChange={(e) => setMinCu(Number(e.target.value))}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip label="Max CU" tip="Ceiling the compute scales up to. Autoscale range 0.5–32 CU, and max − min must not exceed 8 CU." />
              <Input
                type="number"
                step="0.5"
                value={maxCu}
                onChange={(e) => setMaxCu(Number(e.target.value))}
              />
            </div>
          </div>
          {!cuValid && (
            <p className="flex items-center gap-2 text-sm text-amber-500">
              <AlertTriangle className="h-4 w-4" />
              Invalid CU range. Require 0.5 ≤ min ≤ max ≤ 32 and (max − min) ≤ 8. Current spread:{" "}
              {spread} CU.
            </p>
          )}
          <Button
            onClick={onCreate}
            disabled={create.isPending || !projectId.trim() || !cuValid}
          >
            {create.isPending ? "Creating…" : "Create project (billable)"}
          </Button>
          {createdProject && (
            <p className="flex items-center gap-2 text-sm text-emerald-500">
              <CheckCircle2 className="h-4 w-4" /> Created {createdProject}. Sync tables into it below.
            </p>
          )}
        </CardContent>
      </Card>

      {createdProject && <SyncSection projectLabel={createdProject} />}
    </>
  );
}

// --- Use existing project ----------------------------------------------------

function ExistingProjectFlow() {
  const { data, isLoading } = useListLakebaseProjects();
  const projects = data?.data.projects ?? [];
  const listError = data?.data.error;

  const [project, setProject] = useState("");
  const [inspectProject, setInspectProject] = useState("");

  const projectInfo = useGetProjectInfo({
    params: { project: inspectProject },
    query: { enabled: !!inspectProject },
  });
  const info = projectInfo.data?.data;

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-4 w-4" /> Select a project
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-end gap-2">
            <div className="grid gap-2">
              <Label>Project you have access to</Label>
              <Select value={project} onValueChange={setProject}>
                <SelectTrigger className="w-96">
                  <SelectValue placeholder={isLoading ? "Loading…" : "Select a project"} />
                </SelectTrigger>
                <SelectContent>
                  {projects.map((p) => (
                    <SelectItem key={p.name} value={p.name}>
                      {p.display_name || p.name}
                      {p.state ? ` · ${p.state}` : ""}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button variant="outline" onClick={() => setInspectProject(project)} disabled={!project}>
              Inspect
            </Button>
          </div>
          {listError && (
            <p className="text-sm text-amber-500">
              Could not list projects ({listError}). Check that you authorized the app with the
              <code> postgres</code> scope.
            </p>
          )}
          {info && !info.error && (
            <div className="rounded-lg border p-4 text-sm">
              <div className="font-medium">{info.name}</div>
              <div className="text-muted-foreground">branch: {info.branch}</div>
              <div className="mt-2 space-y-1">
                {(info.endpoints ?? []).map((e) => (
                  <div key={e.name} className="flex flex-wrap gap-x-4 text-xs">
                    <span className="text-muted-foreground">{e.host ?? e.name}</span>
                    <span>state: {e.state ?? "—"}</span>
                    <span>
                      CU: {e.min_cu ?? "—"}–{e.max_cu ?? "—"}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
          {info?.error && <p className="text-sm text-amber-500">{info.error}</p>}
        </CardContent>
      </Card>

      {project && <SyncSection projectLabel={project} />}
    </>
  );
}

// --- Shared sync section (multiple tables) ------------------------------------

function exploreUrl(host: string | null | undefined, fullName: string): string | null {
  if (!host) return null;
  const parts = fullName.split(".");
  if (parts.length !== 3) return `${host}/explore/data`;
  return `${host}/explore/data/${parts[0]}/${parts[1]}/${parts[2]}`;
}

function branchLabel(fullName: string): string {
  // "projects/<id>/branches/<id>" → "<id>" for display.
  return fullName.split("/").pop() || fullName;
}

// Prefer "production" as the default branch, else the first available.
function defaultBranch(branches: string[]): string {
  return branches.find((b) => branchLabel(b) === "production") ?? branches[0] ?? "";
}

// Turn the raw SYNCED_TABLE_* enum into a short, human label.
function syncStateLabel(state: string | null | undefined): string {
  if (!state) return "Unknown";
  return state
    .replace(/^SYNCED_TABLE_/, "")
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

// Color + wording per status bucket returned by the backend (ok | syncing | failed).
const SYNC_STATUS_STYLES: Record<string, { dot: string; text: string }> = {
  ok: { dot: "bg-emerald-500", text: "text-emerald-600 dark:text-emerald-400" },
  syncing: { dot: "bg-amber-500 animate-pulse", text: "text-amber-600 dark:text-amber-400" },
  failed: { dot: "bg-red-500", text: "text-red-600 dark:text-red-400" },
  unknown: { dot: "bg-muted-foreground", text: "text-muted-foreground" },
};

function SyncSection({ projectLabel }: { projectLabel: string }) {
  const [rows, setRows] = useState<SyncRow[]>([{ ...sampleRow }]);
  const [warehouseId, setWarehouseId] = useState("");
  const createSync = useCreateSyncedTable();
  const checkReq = useCheckSyncRequirements();
  const checkStatus = useGetSyncedTableStatus();
  const measureSize = useGetTableSize();
  const { data: whData, isLoading: whLoading } = useListWarehouses();
  const warehouses = whData?.data.warehouses ?? [];
  const { data: wsInfo } = useGetWorkspaceInfo();
  const host = wsInfo?.data.host;
  // Branches of the target project, for the per-table branch picker.
  const { data: branchData } = useListLakebaseBranches({
    params: { project: projectLabel },
    query: { enabled: projectLabel.trim().length > 0 },
  });
  const branches = branchData?.data.branches ?? [];

  // Default each row's branch to production (or the first available) once branches load.
  useEffect(() => {
    if (branches.length === 0) return;
    const def = defaultBranch(branches);
    setRows((rs) =>
      rs.map((r) => (r.branch ? r : { ...r, branch: def })),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [branchData]);

  const update = (i: number, patch: Partial<SyncRow>) =>
    setRows((rs) => rs.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));

  const onCheck = async (i: number) => {
    const row = rows[i];
    try {
      const res = await checkReq.mutateAsync({
        source_table_full_name: row.source_table_full_name,
        scheduling_policy: row.scheduling_policy,
        warehouse_id: warehouseId || null,
      });
      update(i, { check: res.data });
      if (!res.data.verified) toast.warning(res.data.message);
      else if (res.data.ok) toast.success(res.data.message);
      else toast.warning(res.data.message);
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onMeasureSize = async (i: number) => {
    const row = rows[i];
    update(i, { size: undefined });
    try {
      const res = await measureSize.mutateAsync({
        table_full_name: row.source_table_full_name,
        warehouse_id: warehouseId,
      });
      update(i, { size: { ok: res.data.ok, message: res.data.message } });
    } catch (e) {
      update(i, { size: { ok: false, message: String(e) } });
    }
  };

  const onSyncOne = async (i: number) => {
    const row = rows[i];
    update(i, { result: undefined }); // clear the previous result
    try {
      const res = await createSync.mutateAsync({
        target_uc_name: row.target_uc_name,
        source_table_full_name: row.source_table_full_name,
        primary_key_columns: row.primary_key_columns.split(",").map((s) => s.trim()).filter(Boolean),
        scheduling_policy: row.scheduling_policy,
        database: row.database || null,
        branch: row.branch || null,
        storage_catalog: row.storage_catalog,
        storage_schema: row.storage_schema,
      });
      // Persist the outcome inline (both success and error) so it doesn't vanish
      // like a toast — the user asked to see the message on the page.
      update(i, { result: { ok: res.data.ok, detail: res.data.detail } });
      // Auto-pull the initial pipeline status right after a successful create.
      if (res.data.ok) onCheckStatus(i);
    } catch (e) {
      update(i, { result: { ok: false, detail: String(e) } });
    }
  };

  const onCheckStatus = async (i: number) => {
    const row = rows[i];
    update(i, { status: undefined });
    try {
      const res = await checkStatus.mutateAsync({ target_uc_name: row.target_uc_name });
      update(i, { status: res.data });
    } catch (e) {
      update(i, { status: { ok: false, name: row.target_uc_name, exists: false, kind: "unknown", error: String(e) } });
    }
  };

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Database className="h-4 w-4" /> Bulk sync into{" "}
          <code className="text-sm">{projectLabel}</code>
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setRows((rs) => [...rs, { ...emptyRow }])}>
          <Plus className="mr-1 h-4 w-4" /> Add table
        </Button>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
          The first row is pre-filled with the quickstart sample{" "}
          <code>samples.tpcds_sf1.store_sales</code> (snapshot, PK{" "}
          <code>ss_item_sk, ss_ticket_number</code>) — just set the target to your Lakebase database
          catalog (e.g. <code>lakebase_catalog.public.store_sales</code>) and click Create synced table.
          {" "}Use this to set up <span className="font-medium text-foreground">many tables at once</span> for a
          repeatable POC. For a single table, the native Databricks dialog is richer (primary-key picker,
          live Change-Data-Feed detection, compute provisioning) — open it from the source table's page in
          Catalog Explorer.
          {host && (
            <a
              href={`${host}/explore/data`}
              target="_blank"
              rel="noreferrer"
              className="ml-1 inline-flex items-center gap-1 text-primary hover:underline"
            >
              Open Catalog Explorer <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </div>

        <div className="grid gap-2 sm:max-w-md">
          <LabelWithTip
            label="SQL warehouse (for CDF check)"
            tip="Used to run SHOW TBLPROPERTIES to verify Change Data Feed for Triggered/Continuous modes. Runs as you (your Unity Catalog permissions). Optional for Snapshot."
          />
          <Select value={warehouseId} onValueChange={setWarehouseId}>
            <SelectTrigger>
              <SelectValue placeholder={whLoading ? "Loading…" : "Select a warehouse"} />
            </SelectTrigger>
            <SelectContent>
              {warehouses.map((w) => (
                <SelectItem key={w.id} value={w.id}>
                  {w.name}
                  {w.state ? ` · ${w.state}` : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {rows.map((row, i) => {
          const modeInfo = SYNC_MODES[row.scheduling_policy];
          return (
            <div key={i} className="space-y-4 rounded-lg border p-4">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">Table {i + 1}</span>
                {rows.length > 1 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setRows((rs) => rs.filter((_, idx) => idx !== i))}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="grid gap-2">
                  <div className="flex items-center justify-between">
                    <Label>Source Delta table</Label>
                    {host && row.source_table_full_name.split(".").length === 3 && (
                      <a
                        href={exploreUrl(host, row.source_table_full_name) ?? "#"}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
                      >
                        Create in Databricks <ExternalLink className="h-3 w-3" />
                      </a>
                    )}
                  </div>
                  <Input
                    placeholder="catalog.schema.table"
                    value={row.source_table_full_name}
                    onChange={(e) => update(i, { source_table_full_name: e.target.value, check: undefined })}
                  />
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Target UC name (Lakebase)"
                    tip="Three-part name <catalog>.<schema>.<table>. Use the UC catalog registered to your Lakebase Postgres database (the 'database catalog') — then leave Database blank and it's inferred."
                  />
                  <Input
                    placeholder="lakebase_catalog.public.store_sales"
                    value={row.target_uc_name}
                    onChange={(e) => update(i, { target_uc_name: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Database (optional)"
                    tip="Leave BLANK when the target catalog is your Lakebase database catalog (the database is inferred). REQUIRED when targeting a standard UC catalog — name the Postgres database, e.g. databricks_postgres."
                  />
                  <Input
                    placeholder="blank = inferred from database catalog"
                    value={row.database}
                    onChange={(e) => update(i, { database: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Branch"
                    tip="The branch of this project to sync into (e.g. production)."
                  />
                  <Select
                    value={row.branch}
                    onValueChange={(v) => update(i, { branch: v })}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder={branches.length ? "Select a branch" : "Loading…"} />
                    </SelectTrigger>
                    <SelectContent>
                      {branches.map((b) => (
                        <SelectItem key={b} value={b}>
                          {branchLabel(b)}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Primary key column(s)"
                    tip="Comma-separated. Required — the synced table is keyed on these for upserts."
                  />
                  <Input
                    placeholder="id"
                    value={row.primary_key_columns}
                    onChange={(e) => update(i, { primary_key_columns: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label>Scheduling policy</Label>
                  <Select
                    value={row.scheduling_policy}
                    onValueChange={(v) =>
                      update(i, { scheduling_policy: v as SyncPolicy, check: undefined })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {Object.entries(SYNC_MODES).map(([k, v]) => (
                        <SelectItem key={k} value={k}>
                          {v.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Storage catalog (optional)"
                    tip="Leave blank for Lakebase Autoscaling — the platform auto-manages the sync pipeline's staging storage. Only set this to pin a new pipeline's staging location (a holdover from the Provisioned API)."
                  />
                  <Input
                    placeholder="auto-managed"
                    value={row.storage_catalog}
                    onChange={(e) => update(i, { storage_catalog: e.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <LabelWithTip
                    label="Storage schema (optional)"
                    tip="Leave blank to let Lakebase auto-manage staging storage. Only set alongside Storage catalog to pin the staging location."
                  />
                  <Input
                    placeholder="auto-managed"
                    value={row.storage_schema}
                    onChange={(e) => update(i, { storage_schema: e.target.value })}
                  />
                </div>
              </div>

              {/* Mode explanation */}
              <div className="rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
                <span className="font-medium text-foreground">{modeInfo.label}.</span> {modeInfo.detail}
                <div className="mt-1">
                  <span className="font-medium text-foreground">Use it for:</span> {modeInfo.use}
                </div>
                <div className="mt-1">
                  <span className="font-medium text-foreground">Pipeline strategy:</span> {modeInfo.strategy}
                </div>
                <div className="mt-1">
                  <span className="font-medium text-foreground">Cost:</span> {modeInfo.cost}
                </div>
              </div>

              {/* Continuous single-table antipattern warning */}
              {row.scheduling_policy === "CONTINUOUS" && (
                <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-400">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>
                    A continuous pipeline runs 24/7 (~$2,000/mo of serverless compute). One table per
                    continuous pipeline is an <span className="font-medium">antipattern</span> — you pay the
                    full 24/7 cost for a single table. Bundle multiple tables into one continuous pipeline (via a
                    shared storage catalog/schema) to amortize it, or use Triggered if seconds-fresh isn't required.
                  </span>
                </div>
              )}

              {/* CDF requirement check for triggered/continuous */}
              {modeInfo.requiresCdf && (
                <div className="space-y-2">
                  <Button variant="secondary" size="sm" onClick={() => onCheck(i)} disabled={checkReq.isPending || !row.source_table_full_name}>
                    Check {modeInfo.label} requirements (CDF)
                  </Button>
                  {row.check && (
                    <div
                      className={`rounded-md border p-3 text-xs ${
                        row.check.verified && row.check.ok
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-amber-600 dark:text-amber-400"
                      }`}
                    >
                      <div className="flex items-center gap-2">
                        {row.check.verified && row.check.ok ? (
                          <CheckCircle2 className="h-4 w-4" />
                        ) : (
                          <AlertTriangle className="h-4 w-4" />
                        )}
                        {row.check.message}
                      </div>
                      {row.check.enable_cdf_sql && (
                        <pre className="mt-2 overflow-x-auto rounded bg-background p-2 text-foreground">
                          {row.check.enable_cdf_sql}
                        </pre>
                      )}
                    </div>
                  )}
                </div>
              )}

              {/* Uncompressed source-table size, to inform Lakebase storage sizing. */}
              <div className="space-y-2">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => onMeasureSize(i)}
                  disabled={measureSize.isPending || !row.source_table_full_name || !warehouseId}
                >
                  {measureSize.isPending ? "Measuring…" : "Measure source size (uncompressed)"}
                </Button>
                {!warehouseId && (
                  <p className="text-xs text-muted-foreground">Select a SQL warehouse above to measure size.</p>
                )}
                {row.size && (
                  <div
                    className={`rounded-md border p-3 text-xs ${
                      row.size.ok
                        ? "text-emerald-600 dark:text-emerald-400"
                        : "text-amber-600 dark:text-amber-400"
                    }`}
                  >
                    <div className="flex items-start gap-2">
                      {row.size.ok ? (
                        <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
                      ) : (
                        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                      )}
                      <span className="whitespace-pre-wrap break-words">{row.size.message}</span>
                    </div>
                  </div>
                )}
              </div>

              <Button
                onClick={() => onSyncOne(i)}
                disabled={
                  createSync.isPending ||
                  !row.source_table_full_name ||
                  !row.target_uc_name ||
                  (modeInfo.requiresCdf && row.check ? !row.check.ok : false)
                }
              >
                {createSync.isPending ? "Creating…" : "Create synced table"}
              </Button>

              {row.result && (
                <div
                  className={`rounded-md border p-3 text-xs ${
                    row.result.ok
                      ? "text-emerald-600 dark:text-emerald-400"
                      : "text-red-600 dark:text-red-400"
                  }`}
                >
                  <div className="flex items-start gap-2">
                    {row.result.ok ? (
                      <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
                    ) : (
                      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                    )}
                    <span className="whitespace-pre-wrap break-words">{row.result.detail}</span>
                  </div>
                </div>
              )}

              {/* Live replication status: poll the Lakeflow pipeline behind the synced table. */}
              <div className="space-y-2 border-t pt-4">
                <div className="flex items-center gap-2">
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => onCheckStatus(i)}
                    disabled={checkStatus.isPending || !row.target_uc_name}
                  >
                    <RefreshCw
                      className={`mr-1 h-4 w-4 ${checkStatus.isPending ? "animate-spin" : ""}`}
                    />
                    Check sync status
                  </Button>
                  <InfoTip text="Reads the synced table's Lakeflow pipeline state and last completed sync time. Poll this after creating (snapshot/triggered finish; continuous stays online)." />
                </div>

                {row.status && (
                  <div className="rounded-md border p-3 text-xs">
                    {row.status.error || !row.status.exists ? (
                      <div className="flex items-start gap-2 text-amber-600 dark:text-amber-400">
                        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                        <span className="whitespace-pre-wrap break-words">
                          {row.status.error ?? "Synced table not found yet — it may still be provisioning."}
                        </span>
                      </div>
                    ) : (
                      <div className="space-y-1.5">
                        <div
                          className={`flex items-center gap-2 font-medium ${
                            (SYNC_STATUS_STYLES[row.status.kind ?? "unknown"] ?? SYNC_STATUS_STYLES.unknown).text
                          }`}
                        >
                          <span
                            className={`h-2 w-2 rounded-full ${
                              (SYNC_STATUS_STYLES[row.status.kind ?? "unknown"] ?? SYNC_STATUS_STYLES.unknown).dot
                            }`}
                          />
                          {syncStateLabel(row.status.detailed_state)}
                        </div>
                        {row.status.last_sync_time && (
                          <div className="text-muted-foreground">
                            Last sync: {row.status.last_sync_time}
                          </div>
                        )}
                        {row.status.message && (
                          <div className="text-muted-foreground whitespace-pre-wrap break-words">
                            {row.status.message}
                          </div>
                        )}
                        {row.status.pipeline_id && (
                          <div className="flex items-center gap-2 text-muted-foreground">
                            <span>
                              Pipeline <code>{row.status.pipeline_id}</code>
                            </span>
                            {host && (
                              <a
                                href={`${host}/pipelines/${row.status.pipeline_id}`}
                                target="_blank"
                                rel="noreferrer"
                                className="inline-flex items-center gap-1 text-primary hover:underline"
                              >
                                Open in Lakeflow <ExternalLink className="h-3 w-3" />
                              </a>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}
