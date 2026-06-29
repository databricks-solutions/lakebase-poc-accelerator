import { useEffect, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { toast } from "sonner";
import {
  Plus,
  Trash2,
  Play,
  Wand2,
  Zap,
  ArrowDown,
  ArrowUp,
  ExternalLink,
  Loader2,
  Save,
  History,
  FolderOpen,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";

import { PageHeader } from "@/components/page-header";
import {
  LakebaseConnection,
  emptyConnection,
  type ConnectionConfig,
} from "@/components/lakebase-connection";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  useRunPsycopgTest,
  useOptimizeAnalyze,
  useApplyIndexes,
  useSubmitPgbenchJob,
  useGetPgbenchRunStatus,
  useListClusters,
  useEnableLakebaseHistory,
  useSaveLakebaseHistory,
  useListLakebaseHistory,
  type TestReportOut,
  type OptimizeOut,
  type PgbenchSubmitOut,
} from "@/lib/api";
import {
  type Destination,
  type HistoryPref,
  type SavedRun,
  loadPref,
  savePref,
  loadBrowserRuns,
  saveBrowserRun,
  newRunId,
} from "@/lib/history";

export const Route = createFileRoute("/testing")({
  component: TestingPage,
});

interface QueryRow {
  identifier: string;
  content: string;
}

// One unified, tool-agnostic query format drives BOTH psycopg and pgbench. A query is
// SQL with pgbench-style `:name` placeholders plus optional directives:
//   -- WEIGHT: N      (pgbench relative transaction weight; default 1)
//   -- EXEC_COUNT: N  (psycopg executions of this query; default 5)
//   -- PARAM x = random(min, max)  (named integer generator)
// These 5 OLTP samples run against a synced samples.tpcds_sf1.store_sales table
// (sync it as `store_sales` on the Deployment page), ordered simple → complex.
const SAMPLE_QUERIES: QueryRow[] = [
  {
    identifier: "order_lookup",
    content:
      "-- WEIGHT: 40\n-- EXEC_COUNT: 20\n-- PARAM ticket = random(1, 240000)\nSELECT ss_item_sk, ss_net_paid, ss_quantity\nFROM store_sales\nWHERE ss_ticket_number = :ticket;",
  },
  {
    identifier: "customer_history",
    content:
      "-- WEIGHT: 25\n-- EXEC_COUNT: 15\n-- PARAM cust = random(1, 100000)\nSELECT ss_ticket_number, ss_sold_date_sk, ss_net_paid\nFROM store_sales\nWHERE ss_customer_sk = :cust\nORDER BY ss_sold_date_sk DESC\nLIMIT 50;",
  },
  {
    identifier: "item_sales_agg",
    content:
      "-- WEIGHT: 15\n-- EXEC_COUNT: 10\n-- PARAM item = random(1, 18000)\nSELECT count(*) AS lines, sum(ss_quantity) AS units, avg(ss_sales_price) AS avg_price\nFROM store_sales\nWHERE ss_item_sk = :item;",
  },
  {
    identifier: "store_daily_revenue",
    content:
      "-- WEIGHT: 12\n-- EXEC_COUNT: 8\n-- PARAM store = random(1, 10)\n-- PARAM start = random(2450816, 2452442)\nSELECT ss_sold_date_sk, count(*) AS tickets, sum(ss_net_paid) AS revenue\nFROM store_sales\nWHERE ss_store_sk = :store AND ss_sold_date_sk BETWEEN :start AND :start + 200\nGROUP BY ss_sold_date_sk\nORDER BY ss_sold_date_sk;",
  },
  {
    identifier: "top_items_by_store",
    content:
      "-- WEIGHT: 8\n-- EXEC_COUNT: 5\n-- PARAM store = random(1, 10)\nSELECT ss_item_sk, sum(ss_net_paid) AS revenue, sum(ss_quantity) AS units\nFROM store_sales\nWHERE ss_store_sk = :store\nGROUP BY ss_item_sk\nORDER BY revenue DESC\nLIMIT 20;",
  },
];

const QUERY_FORMAT_HINT =
  "SELECT … WHERE col = :name;  directives: -- WEIGHT:, -- EXEC_COUNT:, -- PARAM name = random(min, max)";

function metric(label: string, value: string, tone?: "good" | "warn" | "bad") {
  const color =
    tone === "good" ? "text-emerald-500" : tone === "bad" ? "text-red-500" : tone === "warn" ? "text-amber-500" : "";
  return (
    <div className="rounded-lg border p-4">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${color}`}>{value}</div>
    </div>
  );
}

function compareRow(
  label: string,
  before: number,
  after: number,
  lowerIsBetter: boolean,
  unit: string,
) {
  const dp = unit === "qps" ? 2 : 0;
  const improved = lowerIsBetter ? after < before : after > before;
  const same = after === before;
  const pct = before === 0 ? 0 : Math.abs((after - before) / before) * 100;
  const Arrow = after < before ? ArrowDown : ArrowUp;
  const color = same ? "text-muted-foreground" : improved ? "text-emerald-500" : "text-red-500";
  return (
    <div className="flex items-center justify-between rounded-md border p-3 text-sm">
      <span>{label}</span>
      <div className="flex items-center gap-3">
        <span className="text-muted-foreground">
          {before.toFixed(dp)} → {after.toFixed(dp)} {unit}
        </span>
        <span className={`flex w-16 items-center justify-end gap-1 ${color}`}>
          {!same && <Arrow className="h-3 w-3" />}
          {pct.toFixed(0)}%
        </span>
      </div>
    </div>
  );
}

function TestingPage() {
  return (
    <div>
      <PageHeader
        title="Concurrency Testing"
        description="Run your query mix against Lakebase at a target concurrency level. Use psycopg for a quick in-app test, or pgbench (Databricks job) for heavier, native PostgreSQL load."
      />
      <div className="p-8">
        <Tabs defaultValue="psycopg">
          <TabsList>
            <TabsTrigger value="psycopg">psycopg (in-app)</TabsTrigger>
            <TabsTrigger value="pgbench">pgbench (Databricks job)</TabsTrigger>
          </TabsList>
          <TabsContent value="psycopg" className="mt-6">
            <PsycopgTab />
          </TabsContent>
          <TabsContent value="pgbench" className="mt-6">
            <PgbenchTab />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}

function PsycopgTab() {
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [concurrency, setConcurrency] = useState(10);
  const [queries, setQueries] = useState<QueryRow[]>(SAMPLE_QUERIES);
  const [report, setReport] = useState<TestReportOut | null>(null);
  const [baseline, setBaseline] = useState<TestReportOut | null>(null);
  const [optimize, setOptimize] = useState<OptimizeOut | null>(null);

  const runTest = useRunPsycopgTest();
  const runOptimize = useOptimizeAnalyze();
  const applyIdx = useApplyIndexes();

  const body = () => ({
    auth_method: conn.auth_method,
    project: conn.project || null,
    database: conn.database || null,
    endpoint_host: conn.endpoint_host || null,
    access_token: conn.access_token || null,
    postgres_user_name: conn.postgres_user_name || null,
  });

  const runOnce = async (): Promise<TestReportOut | null> => {
    const res = await runTest.mutateAsync({ ...body(), concurrency_level: concurrency, queries });
    return res.data;
  };

  const onRun = async () => {
    try {
      const data = await runOnce();
      setReport(data);
      if (data?.error) toast.error(data.error);
      else toast.success("Test complete");
    } catch (e) {
      toast.error(String(e));
    }
  };

  const applyDdls = async (ddls: string[]): Promise<boolean> => {
    const res = await applyIdx.mutateAsync({ ...body(), ddls });
    if (res.data.error) {
      toast.error(res.data.error);
      return false;
    }
    const results = res.data.results ?? [];
    const failed = results.filter((r) => !r.ok);
    if (failed.length) {
      toast.warning(`${results.length - failed.length} applied, ${failed.length} failed`);
      failed.forEach((f) => toast.error(f.detail));
    } else {
      toast.success(`Applied ${results.length} index(es)`);
    }
    return failed.length === 0;
  };

  const onApplyOne = async (ddl: string) => {
    try {
      await applyDdls([ddl]);
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onApplyAllAndRerun = async () => {
    if (!optimize) return;
    try {
      if (report) setBaseline(report); // snapshot "before"
      await applyDdls(optimize.index_suggestions.map((s) => s.ddl));
      const data = await runOnce(); // "after"
      setReport(data);
      if (data?.error) toast.error(data.error);
      else toast.success("Re-ran test after applying indexes");
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onOptimize = async () => {
    try {
      const res = await runOptimize.mutateAsync({
        ...body(),
        run_live: true,
        queries,
      });
      setOptimize(res.data);
      toast.success("Optimization analysis complete");
    } catch (e) {
      toast.error(String(e));
    }
  };

  const updateQuery = (i: number, patch: Partial<QueryRow>) =>
    setQueries((qs) => qs.map((q, idx) => (idx === i ? { ...q, ...patch } : q)));

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Connection</CardTitle>
        </CardHeader>
        <CardContent>
          <LakebaseConnection value={conn} onChange={setConn} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Queries</CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() =>
              setQueries((qs) => [...qs, { identifier: `query_${qs.length + 1}`, content: "" }])
            }
          >
            <Plus className="mr-1 h-4 w-4" /> Add query
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          {queries.map((q, i) => (
            <div key={i} className="space-y-2 rounded-lg border p-3">
              <div className="flex items-center gap-2">
                <Input
                  className="max-w-xs"
                  value={q.identifier}
                  onChange={(e) => updateQuery(i, { identifier: e.target.value })}
                  placeholder="query identifier"
                />
                {queries.length > 1 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setQueries((qs) => qs.filter((_, idx) => idx !== i))}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
              </div>
              <Textarea
                className="font-mono text-xs"
                rows={5}
                value={q.content}
                onChange={(e) => updateQuery(i, { content: e.target.value })}
                placeholder={QUERY_FORMAT_HINT}
              />
            </div>
          ))}
          <div className="flex items-end gap-4">
            <div className="grid gap-2">
              <Label>Concurrency level</Label>
              <Input
                type="number"
                className="w-32"
                min={1}
                max={1000}
                value={concurrency}
                onChange={(e) => setConcurrency(Number(e.target.value))}
              />
            </div>
            <Button onClick={onRun} disabled={runTest.isPending}>
              <Play className="mr-1 h-4 w-4" />
              {runTest.isPending ? "Running…" : "Run test"}
            </Button>
            <Button variant="secondary" onClick={onOptimize} disabled={runOptimize.isPending}>
              <Wand2 className="mr-1 h-4 w-4" />
              {runOptimize.isPending ? "Analyzing…" : "Optimize"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {report && (
        <Card>
          <CardHeader>
            <CardTitle>Results</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-3 lg:grid-cols-4">
            {metric("Total queries", String(report.total_queries_executed))}
            {metric(
              "Success rate",
              `${(report.success_rate * 100).toFixed(1)}%`,
              report.success_rate >= 0.99 ? "good" : "bad",
            )}
            {metric("Throughput", `${report.throughput_queries_per_second.toFixed(2)} qps`)}
            {metric(
              "Avg latency",
              `${report.average_execution_time_ms.toFixed(0)} ms`,
              report.average_execution_time_ms < 50 ? "good" : "warn",
            )}
            {metric("p95 latency", `${report.p95_execution_time_ms.toFixed(0)} ms`)}
            {metric("p99 latency", `${report.p99_execution_time_ms.toFixed(0)} ms`)}
            {metric("Duration", `${report.total_duration_seconds.toFixed(1)} s`)}
          </CardContent>
        </Card>
      )}

      {baseline && report && baseline !== report && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Zap className="h-4 w-4" /> Before / after — indexes applied
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {compareRow("Avg latency", baseline.average_execution_time_ms, report.average_execution_time_ms, true, "ms")}
            {compareRow("p95 latency", baseline.p95_execution_time_ms, report.p95_execution_time_ms, true, "ms")}
            {compareRow("p99 latency", baseline.p99_execution_time_ms, report.p99_execution_time_ms, true, "ms")}
            {compareRow("Throughput", baseline.throughput_queries_per_second, report.throughput_queries_per_second, false, "qps")}
          </CardContent>
        </Card>
      )}

      {optimize && (
        <Card>
          <CardHeader>
            <CardTitle>Optimization suggestions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {optimize.error && (
              <p className="text-sm text-amber-500">{optimize.error}</p>
            )}
            <div>
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-sm font-medium">Candidate indexes</h3>
                {optimize.index_suggestions.length > 0 && (
                  <Button
                    size="sm"
                    onClick={onApplyAllAndRerun}
                    disabled={applyIdx.isPending || runTest.isPending}
                  >
                    <Zap className="mr-1 h-4 w-4" />
                    {applyIdx.isPending || runTest.isPending
                      ? "Applying…"
                      : "Apply all & re-run test"}
                  </Button>
                )}
              </div>
              <div className="space-y-2">
                {optimize.index_suggestions.length === 0 && (
                  <p className="text-sm text-muted-foreground">No index candidates derived.</p>
                )}
                {optimize.index_suggestions.map((s, i) => (
                  <div key={i} className="flex items-start justify-between gap-3 rounded-md border bg-muted/30 p-3">
                    <div className="min-w-0">
                      <code className="text-xs break-all">{s.ddl}</code>
                      <p className="mt-1 text-xs text-muted-foreground">{s.rationale}</p>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => onApplyOne(s.ddl)}
                      disabled={applyIdx.isPending}
                    >
                      Apply
                    </Button>
                  </div>
                ))}
              </div>
              <p className="mt-2 text-xs text-muted-foreground">
                "Apply all &amp; re-run" snapshots the current result as the baseline, creates the
                indexes, and re-runs the test so you can see the before/after impact.
              </p>
            </div>
            {optimize.findings.length > 0 && (
              <>
                <Separator />
                <div>
                  <h3 className="mb-2 text-sm font-medium">Live findings</h3>
                  <div className="space-y-2">
                    {optimize.findings.map((f, i) => (
                      <div key={i} className="rounded-md border p-3">
                        <div className="flex items-center gap-2">
                          <Badge
                            variant={
                              f.severity === "high"
                                ? "destructive"
                                : f.severity === "medium"
                                  ? "default"
                                  : "secondary"
                            }
                          >
                            {f.severity}
                          </Badge>
                          <span className="text-sm font-medium">{f.title}</span>
                        </div>
                        <p className="mt-1 text-xs text-muted-foreground">{f.detail}</p>
                        <ul className="mt-1 list-disc pl-5 text-xs text-muted-foreground">
                          {f.actions.map((a, j) => (
                            <li key={j}>{a}</li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      )}

      <HistorySection
        conn={conn}
        queries={queries}
        concurrency={concurrency}
        report={report}
        baseline={baseline}
        optimize={optimize}
        onOpenRun={(run) => {
          setQueries(
            run.queries.length
              ? run.queries.map((q) => ({ identifier: q.identifier, content: q.content }))
              : queries,
          );
          if (run.concurrency_level) setConcurrency(run.concurrency_level);
          setBaseline(run.baseline_report);
          setReport(run.optimized_report ?? run.baseline_report);
          setOptimize(null);
          toast.success(`Loaded run${run.label ? ` "${run.label}"` : ""}`);
        }}
      />
    </div>
  );
}

// --------------------------------------------------------------------------- //
// Run history (browser localStorage or shared Lakebase table)
// --------------------------------------------------------------------------- //
interface HistorySectionProps {
  conn: ConnectionConfig;
  queries: QueryRow[];
  concurrency: number;
  report: TestReportOut | null;
  baseline: TestReportOut | null;
  optimize: OptimizeOut | null;
  onOpenRun: (run: SavedRun) => void;
}

function HistorySection({
  conn,
  queries,
  concurrency,
  report,
  baseline,
  optimize,
  onOpenRun,
}: HistorySectionProps) {
  const [pref, setPref] = useState<HistoryPref>(loadPref);
  const [label, setLabel] = useState("");
  const [runs, setRuns] = useState<SavedRun[]>([]);

  const enableHistory = useEnableLakebaseHistory();
  const saveHistory = useSaveLakebaseHistory();
  const listHistory = useListLakebaseHistory();

  const updatePref = (patch: Partial<HistoryPref>) => {
    const next = { ...pref, ...patch };
    setPref(next);
    savePref(next);
  };

  const connBody = () => ({
    auth_method: conn.auth_method,
    project: conn.project || null,
    database: conn.database || null,
    endpoint_host: conn.endpoint_host || null,
    access_token: conn.access_token || null,
    postgres_user_name: conn.postgres_user_name || null,
    schema_name: pref.schema || "public",
  });

  // Load the list for whichever destination is active.
  const refresh = async () => {
    if (pref.destination === "browser") {
      setRuns(loadBrowserRuns());
    } else if (pref.destination === "lakebase" && pref.lakebaseConsented) {
      try {
        const res = await listHistory.mutateAsync(connBody());
        if (res.data.error) toast.error(res.data.error);
        else setRuns((res.data.runs ?? []) as SavedRun[]);
      } catch (e) {
        toast.error(String(e));
      }
    } else {
      setRuns([]);
    }
  };

  useEffect(() => {
    if (pref.destination === "browser") setRuns(loadBrowserRuns());
    else setRuns([]);
    // Only auto-load the cheap (local) source; Lakebase is loaded on demand.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pref.destination]);

  const onChangeDestination = (d: Destination) => {
    // Switching to Lakebase keeps any prior consent; the consent panel shows when not yet consented.
    updatePref({ destination: d });
  };

  const onEnableLakebase = async () => {
    try {
      const res = await enableHistory.mutateAsync(connBody());
      if (res.data.ok) {
        updatePref({ lakebaseConsented: true });
        toast.success(res.data.message);
        refresh();
      } else {
        toast.error(res.data.error || res.data.message);
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  const buildRun = (): SavedRun => {
    // If there's a baseline, the current report is the "after"; otherwise it's the baseline.
    const hasBaseline = !!baseline && baseline !== report;
    return {
      id: newRunId(),
      created_at: new Date().toISOString(),
      label: label.trim() || null,
      project: conn.project || null,
      concurrency_level: concurrency,
      queries: queries.map((q) => ({ identifier: q.identifier, content: q.content })),
      baseline_report: hasBaseline ? baseline : report,
      optimized_report: hasBaseline ? report : null,
      index_ddls: optimize?.index_suggestions.map((s) => s.ddl) ?? [],
    };
  };

  const onSave = async () => {
    if (!report) {
      toast.error("Run a test first — there's nothing to save yet.");
      return;
    }
    const run = buildRun();
    if (pref.destination === "browser") {
      saveBrowserRun(run);
      setRuns(loadBrowserRuns());
      setLabel("");
      toast.success("Saved to this browser");
      return;
    }
    if (pref.destination === "lakebase") {
      try {
        const res = await saveHistory.mutateAsync({
          ...connBody(),
          label: run.label,
          concurrency_level: run.concurrency_level,
          queries: run.queries,
          baseline_report: run.baseline_report as unknown as Record<string, unknown> | null,
          optimized_report: run.optimized_report as unknown as Record<string, unknown> | null,
          index_ddls: run.index_ddls,
        });
        if (res.data.ok) {
          setLabel("");
          toast.success("Saved to Lakebase");
          refresh();
        } else {
          toast.error(res.data.error || "Save failed");
        }
      } catch (e) {
        toast.error(String(e));
      }
    }
  };

  const needsConsent = pref.destination === "lakebase" && !pref.lakebaseConsented;
  const canSave = pref.destination !== "off" && !needsConsent;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <History className="h-4 w-4" /> Run history
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-end gap-4">
          <div className="grid gap-2">
            <Label>Save runs to</Label>
            <Select value={pref.destination} onValueChange={(v) => onChangeDestination(v as Destination)}>
              <SelectTrigger className="w-56">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="off">Off — don't save</SelectItem>
                <SelectItem value="browser">This browser (localStorage)</SelectItem>
                <SelectItem value="lakebase">Lakebase table (shared)</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {pref.destination !== "off" && (
            <>
              <div className="grid gap-2">
                <Label>Label (optional)</Label>
                <Input
                  className="w-56"
                  placeholder="e.g. before/after indexes"
                  value={label}
                  onChange={(e) => setLabel(e.target.value)}
                />
              </div>
              <Button onClick={onSave} disabled={!canSave || saveHistory.isPending}>
                <Save className="mr-1 h-4 w-4" />
                {saveHistory.isPending ? "Saving…" : "Save current run"}
              </Button>
              <Button variant="outline" onClick={refresh} disabled={listHistory.isPending}>
                <RefreshCw className="mr-1 h-4 w-4" /> Refresh
              </Button>
            </>
          )}
        </div>

        {pref.destination === "browser" && (
          <p className="text-xs text-muted-foreground">
            Stored in this browser only — private to you, survives refreshes and app redeploys, but
            not shared across machines or users. Clearing browser data removes it.
          </p>
        )}

        {/* Lakebase consent + least-privilege permission gate */}
        {pref.destination === "lakebase" && (
          <div className="rounded-md border bg-muted/30 p-3 text-xs">
            <div className="flex items-center gap-2 font-medium text-foreground">
              <ShieldCheck className="h-4 w-4" />
              {pref.lakebaseConsented ? "Lakebase history enabled" : "Enable shared Lakebase history"}
            </div>
            <p className="mt-2 text-muted-foreground">
              The app connects to the project you're testing as its{" "}
              <span className="font-medium text-foreground">service principal</span> and writes one
              row per saved run. The table is shared across all app users; each run is attributed to
              you via <code>created_by</code>. Set the connection above before enabling.
            </p>
            <div className="mt-2 rounded border border-amber-500/30 bg-amber-500/5 p-2 text-muted-foreground">
              <span className="font-medium text-foreground">Least-privilege by design.</span> This app
              is standalone — it is <span className="font-medium text-foreground">not</span> attached
              to any Lakebase project. The service principal is confined to a single dedicated schema
              it <span className="font-medium text-foreground">owns</span> (
              <code>{pref.schema || "accelerator_history"}</code>) and has{" "}
              <span className="font-medium text-foreground">no access to your other tables</span> —
              Postgres denies everything not explicitly granted. A project owner runs the one-time
              setup below; nothing is granted on your application data.
            </div>
            <div className="mt-3 grid max-w-xs gap-2">
              <Label>Dedicated schema (SP-owned)</Label>
              <Input
                value={pref.schema}
                onChange={(e) => updatePref({ schema: e.target.value, lakebaseConsented: false })}
                placeholder="accelerator_history"
              />
            </div>
            <div className="mt-3">
              <div className="mb-1 text-muted-foreground">
                One-time setup — a project owner runs this once per project (the exact SP role name is
                filled in after you click Enable):
              </div>
              <pre className="overflow-x-auto rounded bg-background p-2 text-foreground">
{`-- Least-privilege role for the app service principal:
CREATE ROLE "<app-service-principal>" WITH LOGIN
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
GRANT CONNECT ON DATABASE databricks_postgres TO "<app-service-principal>";
-- A dedicated schema it OWNS — its entire sandbox:
CREATE SCHEMA IF NOT EXISTS ${pref.schema || "accelerator_history"}
  AUTHORIZATION "<app-service-principal>";`}
              </pre>
            </div>
            {!pref.lakebaseConsented && (
              <Button className="mt-3" size="sm" onClick={onEnableLakebase} disabled={enableHistory.isPending}>
                {enableHistory.isPending ? "Checking permissions…" : "Enable & create table"}
              </Button>
            )}
            {enableHistory.data?.data && !enableHistory.data.data.ok && (
              <div className="mt-3 text-amber-600 dark:text-amber-400">
                <p>{enableHistory.data.data.message}</p>
                {enableHistory.data.data.grant_sql && (
                  <pre className="mt-2 overflow-x-auto rounded bg-background p-2 text-foreground">
                    {enableHistory.data.data.grant_sql}
                  </pre>
                )}
              </div>
            )}
          </div>
        )}

        {/* Saved runs list */}
        {runs.length > 0 && (
          <div className="space-y-2">
            {runs.map((run) => {
              const after = run.optimized_report;
              const before = run.baseline_report;
              return (
                <div
                  key={run.id}
                  className="flex items-center justify-between gap-3 rounded-md border p-3 text-sm"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-medium">{run.label || "(unlabeled run)"}</span>
                      {after && (
                        <Badge variant="secondary" className="text-[10px]">
                          before/after
                        </Badge>
                      )}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {run.created_at ? new Date(run.created_at).toLocaleString() : ""}
                      {run.project ? ` · ${run.project}` : ""}
                      {run.created_by ? ` · ${run.created_by}` : ""}
                      {before
                        ? ` · ${(after ?? before).throughput_queries_per_second.toFixed(0)} qps, p95 ${(after ?? before).p95_execution_time_ms.toFixed(0)} ms`
                        : ""}
                    </div>
                  </div>
                  <Button variant="outline" size="sm" onClick={() => onOpenRun(run)}>
                    <FolderOpen className="mr-1 h-4 w-4" /> Open
                  </Button>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// --------------------------------------------------------------------------- //
// pgbench (Databricks job) tab
// --------------------------------------------------------------------------- //
interface PgbenchConfigState {
  clients: number;
  jobs: number;
  duration_seconds: number;
  progress_interval: number;
  protocol: string;
  per_statement_latency: boolean;
  detailed_logging: boolean;
  connect_per_transaction: boolean;
}

const PGBENCH_DEFAULT_CONFIG: PgbenchConfigState = {
  clients: 8,
  jobs: 8,
  duration_seconds: 30,
  progress_interval: 5,
  protocol: "prepared",
  per_statement_latency: true,
  detailed_logging: true,
  connect_per_transaction: false,
};

function PgbenchTab() {
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [config, setConfig] = useState<PgbenchConfigState>(PGBENCH_DEFAULT_CONFIG);
  const [queries, setQueries] = useState<QueryRow[]>(SAMPLE_QUERIES);
  const [clusterId, setClusterId] = useState<string>("auto");
  const [submitted, setSubmitted] = useState<PgbenchSubmitOut | null>(null);
  const [runId, setRunId] = useState<string | null>(null);

  const submit = useSubmitPgbenchJob();
  const clustersQuery = useListClusters();
  const clusters = clustersQuery.data?.data.clusters ?? [];

  const statusQuery = useGetPgbenchRunStatus({
    params: { run_id: runId ?? "" },
    query: {
      enabled: !!runId,
      refetchInterval: (query) => {
        const s = query.state.data?.data.status;
        return s === "completed" || s === "failed" ? false : 3000;
      },
    },
  });
  const status = statusQuery.data?.data;

  const setCfg = (patch: Partial<PgbenchConfigState>) => setConfig((c) => ({ ...c, ...patch }));
  const updateQuery = (i: number, patch: Partial<QueryRow>) =>
    setQueries((qs) => qs.map((q, idx) => (idx === i ? { ...q, ...patch } : q)));

  const onSubmit = async () => {
    try {
      const res = await submit.mutateAsync({
        auth_method: conn.auth_method,
        project: conn.project || null,
        database: conn.database || null,
        endpoint_host: conn.endpoint_host || null,
        access_token: conn.access_token || null,
        postgres_user_name: conn.postgres_user_name || null,
        config,
        queries,
        cluster_id: clusterId === "auto" ? null : clusterId,
      });
      if (res.data.error) {
        toast.error(res.data.error);
        return;
      }
      setSubmitted(res.data);
      setRunId(res.data.run_id ?? null);
      toast.success("pgbench job submitted");
    } catch (e) {
      toast.error(String(e));
    }
  };

  const r = status?.pgbench_results ?? null;
  const num = (v: unknown): number | null =>
    typeof v === "number" ? v : v != null && !isNaN(Number(v)) ? Number(v) : null;
  const running = !!runId && status?.status !== "completed" && status?.status !== "failed";

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Connection</CardTitle>
        </CardHeader>
        <CardContent>
          <LakebaseConnection value={conn} onChange={setConn} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>pgbench configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-4">
            <div className="grid gap-2">
              <Label>Clients (-c)</Label>
              <Input
                type="number"
                min={1}
                max={1000}
                value={config.clients}
                onChange={(e) => setCfg({ clients: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <Label>Worker threads (-j)</Label>
              <Input
                type="number"
                min={1}
                max={100}
                value={config.jobs}
                onChange={(e) => setCfg({ jobs: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <Label>Duration s (-T)</Label>
              <Input
                type="number"
                min={1}
                max={3600}
                value={config.duration_seconds}
                onChange={(e) => setCfg({ duration_seconds: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <Label>Progress s (-P)</Label>
              <Input
                type="number"
                min={1}
                max={60}
                value={config.progress_interval}
                onChange={(e) => setCfg({ progress_interval: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <Label>Protocol (-M)</Label>
              <Select value={config.protocol} onValueChange={(v) => setCfg({ protocol: v })}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="prepared">prepared</SelectItem>
                  <SelectItem value="simple">simple</SelectItem>
                  <SelectItem value="extended">extended</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label>Cluster</Label>
              <Select value={clusterId} onValueChange={setClusterId}>
                <SelectTrigger>
                  <SelectValue placeholder={clustersQuery.isLoading ? "Loading…" : "Auto job cluster"} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="auto">Auto job cluster</SelectItem>
                  {clusters.map((c) => (
                    <SelectItem key={c.cluster_id} value={c.cluster_id}>
                      {c.cluster_name} · {c.state}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="flex flex-wrap gap-6 text-sm">
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={config.per_statement_latency}
                onChange={(e) => setCfg({ per_statement_latency: e.target.checked })}
              />
              Per-statement latency (-r)
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={config.detailed_logging}
                onChange={(e) => setCfg({ detailed_logging: e.target.checked })}
              />
              Detailed logging (-l)
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={config.connect_per_transaction}
                onChange={(e) => setCfg({ connect_per_transaction: e.target.checked })}
              />
              Reconnect per transaction (-C)
            </label>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Queries</CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() =>
              setQueries((qs) => [...qs, { identifier: `query_${qs.length + 1}`, content: "" }])
            }
          >
            <Plus className="mr-1 h-4 w-4" /> Add query
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted-foreground">
            Same unified query format as the psycopg tab. <code>-- WEIGHT:</code> sets each
            query's relative pgbench transaction weight; <code>-- EXEC_COUNT:</code> is ignored
            here (pgbench runs for the configured duration).
          </p>
          {queries.map((q, i) => (
            <div key={i} className="space-y-2 rounded-lg border p-3">
              <div className="flex items-center gap-2">
                <Input
                  className="max-w-xs"
                  value={q.identifier}
                  onChange={(e) => updateQuery(i, { identifier: e.target.value })}
                  placeholder="query identifier"
                />
                {queries.length > 1 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setQueries((qs) => qs.filter((_, idx) => idx !== i))}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
              </div>
              <Textarea
                className="font-mono text-xs"
                rows={6}
                value={q.content}
                onChange={(e) => updateQuery(i, { content: e.target.value })}
                placeholder={QUERY_FORMAT_HINT}
              />
            </div>
          ))}
          <Button onClick={onSubmit} disabled={submit.isPending || running}>
            <Play className="mr-1 h-4 w-4" />
            {submit.isPending ? "Submitting…" : "Submit pgbench job"}
          </Button>
        </CardContent>
      </Card>

      {submitted && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {running && <Loader2 className="h-4 w-4 animate-spin" />}
              Job run
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap items-center gap-3 text-sm">
              <Badge
                variant={
                  status?.status === "completed"
                    ? "default"
                    : status?.status === "failed"
                      ? "destructive"
                      : "secondary"
                }
              >
                {status?.status ?? submitted.status}
              </Badge>
              <span className="text-muted-foreground">
                {status?.message ?? "Submitting…"}
              </span>
              {submitted.job_run_url && (
                <a
                  className="flex items-center gap-1 text-primary hover:underline"
                  href={submitted.job_run_url}
                  target="_blank"
                  rel="noreferrer"
                >
                  Open in Databricks <ExternalLink className="h-3 w-3" />
                </a>
              )}
            </div>

            {r && (
              <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-4">
                {metric("TPS", num(r.tps) != null ? num(r.tps)!.toFixed(2) : "—")}
                {metric(
                  "Avg latency",
                  num(r.latency_avg_ms) != null ? `${num(r.latency_avg_ms)!.toFixed(2)} ms` : "—",
                )}
                {metric(
                  "p50 latency",
                  num(r.latency_p50_ms) != null ? `${num(r.latency_p50_ms)!.toFixed(2)} ms` : "—",
                )}
                {metric(
                  "p95 latency",
                  num(r.latency_p95_ms) != null ? `${num(r.latency_p95_ms)!.toFixed(2)} ms` : "—",
                )}
                {metric(
                  "p99 latency",
                  num(r.latency_p99_ms) != null ? `${num(r.latency_p99_ms)!.toFixed(2)} ms` : "—",
                )}
                {metric(
                  "Transactions",
                  num(r.total_transactions) != null ? String(num(r.total_transactions)) : "—",
                )}
                {num(r.success_rate) != null &&
                  metric(
                    "Success rate",
                    `${num(r.success_rate)!.toFixed(1)}%`,
                    num(r.success_rate)! >= 99 ? "good" : "bad",
                  )}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
