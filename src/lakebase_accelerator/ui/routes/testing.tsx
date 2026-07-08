import { useEffect, useRef, useState } from "react";
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
  RefreshCw,
  ShieldCheck,
  Info,
  ChevronRight,
  ChevronDown,
  DollarSign,
  AlertTriangle,
  CheckCircle2,
  ScanSearch,
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
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  useRunPsycopgTest,
  useOptimizeAnalyze,
  useApplyIndexes,
  useSubmitPgbenchJob,
  useGetPgbenchRunStatus,
  useSubmitLocalPgbench,
  useGetLocalPgbenchStatus,
  useGetTestingCapabilities,
  useEnableLakebaseHistory,
  useArchiveLakebaseHistory,
  useListLakebaseHistory,
  useListLakebaseHistoryTables,
  useGetWorkspaceInfo,
  useListWarehouses,
  useGetProjectInfo,
  useGetRunCost,
  useExplainQueries,
  type TestReportOut,
  type OptimizeOut,
  type QueryStat,
  type HistoryRunOut,
  type RunCostOut,
  type ExplainResultOut,
} from "@/lib/api";
import {
  type HistoryPref,
  type SavedRun,
  type Engine,
  DEFAULT_SCHEMA,
  DEFAULT_TABLE,
  loadPref,
  savePref,
  loadBrowserRuns,
  saveBrowserRun,
  deleteBrowserRun,
  clearBrowserRuns,
  newRunId,
} from "@/lib/history";

export const Route = createFileRoute("/testing")({
  component: TestingPage,
});

interface QueryRow {
  identifier: string;
  content: string;
}

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

// One unified, tool-agnostic query format drives BOTH psycopg and pgbench. A query is
// SQL with pgbench-style `:name` placeholders plus optional directives:
//   -- WEIGHT: N      (relative mix weight; default 1 — drives both engines)
//   -- PARAM x = random(min, max)  (named integer generator)
// These 5 samples run against a synced samples.tpcds_sf1.store_sales table (sync it as
// `store_sales` on the Deployment page). They are all OLTP-shaped — selective point or
// indexed-range lookups bounded by LIMIT — which is what Lakebase is built for. We
// deliberately avoid full-table count(*) and GROUP BY aggregations: those are analytical
// scans better served by the lakehouse (Delta + SQL warehouse), not a serving DB.
const SAMPLE_QUERIES: QueryRow[] = [
  {
    identifier: "order_lookup",
    content:
      "-- WEIGHT: 40\n-- PARAM ticket = random(1, 240000)\nSELECT ss_item_sk, ss_net_paid, ss_quantity\nFROM store_sales\nWHERE ss_ticket_number = :ticket;",
  },
  {
    identifier: "customer_history",
    content:
      "-- WEIGHT: 25\n-- PARAM cust = random(1, 100000)\nSELECT ss_ticket_number, ss_sold_date_sk, ss_net_paid\nFROM store_sales\nWHERE ss_customer_sk = :cust\nORDER BY ss_sold_date_sk DESC\nLIMIT 50;",
  },
  {
    identifier: "item_recent_sales",
    content:
      "-- WEIGHT: 15\n-- PARAM item = random(1, 18000)\nSELECT ss_ticket_number, ss_sold_date_sk, ss_sales_price, ss_quantity\nFROM store_sales\nWHERE ss_item_sk = :item\nORDER BY ss_sold_date_sk DESC\nLIMIT 50;",
  },
  {
    identifier: "store_day_tickets",
    content:
      "-- WEIGHT: 12\n-- PARAM store = random(1, 10)\n-- PARAM day = random(2450816, 2452442)\nSELECT ss_ticket_number, ss_customer_sk, ss_item_sk, ss_net_paid\nFROM store_sales\nWHERE ss_store_sk = :store AND ss_sold_date_sk BETWEEN :day AND :day + 6\nORDER BY ss_ticket_number\nLIMIT 100;",
  },
  {
    identifier: "customer_item_history",
    content:
      "-- WEIGHT: 8\n-- PARAM cust = random(1, 100000)\n-- PARAM item = random(1, 18000)\nSELECT ss_ticket_number, ss_sold_date_sk, ss_sales_price, ss_quantity\nFROM store_sales\nWHERE ss_customer_sk = :cust AND ss_item_sk = :item\nORDER BY ss_sold_date_sk DESC\nLIMIT 20;",
  },
];

const QUERY_FORMAT_HINT =
  "SELECT … WHERE col = :name;  directives: -- WEIGHT: N, -- PARAM name = random(min, max) " +
  "or values(v1, v2, ...). For a fixed value, inline the literal: WHERE ss_ticket_number = 187425.";

// Shown under the Queries header in both tabs: how to vary a value vs. pin it.
function QueryParamHelp() {
  return (
    <div className="rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
      <p className="font-medium text-foreground">Parameter values</p>
      <p className="mt-1">
        <span className="font-medium text-foreground">Randomized</span> (a fresh value per
        execution) — declare a generator and reference it with <code>:name</code>:
      </p>
      <pre className="mt-1 overflow-x-auto rounded bg-background p-2 text-[11px]">{`-- PARAM ticket = random(1, 240000)
SELECT ss_item_sk, ss_net_paid
FROM store_sales
WHERE ss_ticket_number = :ticket;`}</pre>
      <p className="mt-2">
        <span className="font-medium text-foreground">Pick from a set</span> (one value chosen
        at random per execution) — use <code>values(...)</code> with integers and/or quoted
        strings:
      </p>
      <pre className="mt-1 overflow-x-auto rounded bg-background p-2 text-[11px]">{`-- PARAM store = values(1, 4, 7)
-- PARAM region = values('east', 'west')
SELECT ss_item_sk, ss_net_paid
FROM store_sales
WHERE ss_store_sk = :store AND ss_region = :region;`}</pre>
      <p className="mt-2">
        <span className="font-medium text-foreground">Exact value</span> (same value every
        execution) — just inline the literal; no <code>:name</code> and no{" "}
        <code>-- PARAM</code> line:
      </p>
      <pre className="mt-1 overflow-x-auto rounded bg-background p-2 text-[11px]">{`SELECT ss_item_sk, ss_net_paid
FROM store_sales
WHERE ss_ticket_number = 187425;`}</pre>
      <p className="mt-2">
        You can mix all of these — e.g. pin <code>ss_store_sk = 5</code>, draw{" "}
        <code>:region</code> from a set, and randomize <code>:day</code>. A <code>:name</code>{" "}
        with no matching <code>-- PARAM</code> is flagged before the run with a message naming
        the placeholder, so just declare it or use a literal.
      </p>
    </div>
  );
}

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

// Contextualizes this run's latency/throughput against Lakebase's published
// reference targets (from the Lakebase FAQ), so a number reads as good/marginal
// rather than just a bare figure. Targets assume OLTP point access — range scans
// and heavy joins legitimately run slower, so this is guidance, not a verdict.
function ReferenceBands({ p99, qps }: { p99: number; qps: number }) {
  const latTone = p99 <= 10 ? "good" : p99 <= 50 ? "warn" : "bad";
  const latText =
    p99 <= 10
      ? "within the <10 ms point read/write target"
      : p99 <= 50
        ? "above the <10 ms point-read target but within the tens-of-ms range"
        : "well above Lakebase's typical latency — likely a scan/join, not a point lookup";
  const dot =
    latTone === "good" ? "bg-emerald-500" : latTone === "warn" ? "bg-amber-500" : "bg-red-500";
  return (
    <div className="mt-4 rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
      <div className="mb-1 font-medium text-foreground">Reference (Lakebase FAQ targets)</div>
      <div className="flex items-center gap-2">
        <span className={`inline-block h-2 w-2 rounded-full ${dot}`} />
        <span>
          p99 {p99.toFixed(0)} ms — {latText}.
        </span>
      </div>
      <div className="mt-1">
        Throughput {qps.toFixed(0)} qps. Reference reads run ~10–30k/s per Provisioned CU (≈8 Autoscaling
        CU); &gt;100k QPS or scans over ~100k rows are outside Lakebase's OLTP sweet spot.
      </div>
    </div>
  );
}

// Per-query breakdown table, mirroring Lakebase's query performance view
// (calls / avg / total / p95 / p99). Used by both the psycopg and pgbench results.
function PerQueryTable({ rows }: { rows?: QueryStat[] | null }) {
  if (!rows || rows.length === 0) return null;
  const ms = (v?: number | null) => (v == null ? "—" : `${v.toFixed(2)} ms`);
  return (
    <div className="overflow-x-auto rounded-lg border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Query</TableHead>
            <TableHead className="text-right">Calls</TableHead>
            <TableHead className="text-right">Avg</TableHead>
            <TableHead className="text-right">Total</TableHead>
            <TableHead className="text-right">p95</TableHead>
            <TableHead className="text-right">p99</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((r) => (
            <TableRow key={r.query_identifier}>
              <TableCell className="font-medium">{r.query_identifier}</TableCell>
              <TableCell className="text-right tabular-nums">{r.calls}</TableCell>
              <TableCell className="text-right tabular-nums">{ms(r.avg_time_ms)}</TableCell>
              <TableCell className="text-right tabular-nums">{ms(r.total_time_ms)}</TableCell>
              <TableCell className="text-right tabular-nums">{ms(r.p95_time_ms)}</TableCell>
              <TableCell className="text-right tabular-nums">{ms(r.p99_time_ms)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
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

// Shared optimization panel — candidate indexes + live findings. Both tabs reuse it;
// they differ only in how "Apply all" behaves (psycopg auto-reruns, pgbench snapshots
// a baseline for a manual re-run), expressed via the label/note/onApplyAll props.
function OptimizeCard({
  optimize,
  onApplyOne,
  onApplyAll,
  busy,
  applyAllLabel,
  applyAllNote,
}: {
  optimize: OptimizeOut;
  onApplyOne: (ddl: string) => void;
  onApplyAll: () => void;
  busy: boolean;
  applyAllLabel: string;
  applyAllNote: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization suggestions</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {optimize.error && <p className="text-sm text-amber-500">{optimize.error}</p>}
        <div>
          <div className="mb-2 flex items-center justify-between">
            <h3 className="text-sm font-medium">Candidate indexes</h3>
            {optimize.index_suggestions.length > 0 && (
              <Button size="sm" onClick={onApplyAll} disabled={busy}>
                <Zap className="mr-1 h-4 w-4" />
                {busy ? "Applying…" : applyAllLabel}
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
                <Button variant="outline" size="sm" onClick={() => onApplyOne(s.ddl)} disabled={busy}>
                  Apply
                </Button>
              </div>
            ))}
          </div>
          <p className="mt-2 text-xs text-muted-foreground">{applyAllNote}</p>
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
  );
}

// EXPLAIN (ANALYZE) plans for the tested queries. A Seq Scan on a benchmark query is
// the headline tuning smell — highlight it. Shared by both tabs. When `before` is set
// (from the "apply indexes & compare" loop) each row shows a before→after plan diff.
function ExplainPlansCard({
  results,
  before,
  analyze,
  onAnalyzeChange,
  busy,
  onVerify,
  canVerify,
  verifyBusy,
}: {
  results: ExplainResultOut[] | null;
  before: ExplainResultOut[] | null;
  analyze: boolean;
  onAnalyzeChange: (v: boolean) => void;
  busy: boolean;
  onVerify: () => void;
  canVerify: boolean;
  verifyBusy: boolean;
}) {
  const beforeById = new Map((before ?? []).map((b) => [b.identifier, b]));
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-row items-center justify-between gap-3">
          <CardTitle>Query plans (EXPLAIN)</CardTitle>
          <div className="flex flex-wrap items-center justify-end gap-3">
            <label className="flex items-center gap-2 text-sm" title="Off: plan + planner estimates only, no execution. On: runs the query for real timings, actual row counts, and cache hits — needed for the before/after comparison.">
              <input type="checkbox" checked={analyze} onChange={(e) => onAnalyzeChange(e.target.checked)} />
              ANALYZE (run for real timings)
            </label>
            <Button size="sm" onClick={onVerify} disabled={!canVerify || verifyBusy || busy}>
              <Zap className="mr-1 h-4 w-4" />
              {verifyBusy ? "Comparing…" : "Apply indexes & compare"}
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* The tuning loop, spelled out so the two actions read as one workflow. */}
        <div className="rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
          <span className="font-medium text-foreground">The tuning loop:</span> Explain <em>diagnoses</em> (find
          the Seq Scans) → Optimize <em>prescribes</em> the indexes → Apply <em>fixes</em> → Explain again{" "}
          <em>confirms</em> the plan flipped to an Index Scan. <span className="font-medium text-foreground">Apply
          indexes &amp; compare</span> runs that whole loop in one click and shows the before/after plan per query
          {!canVerify && " (run Optimize first to get index suggestions)"}.
        </div>
        <div className="rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
          <span className="font-medium text-foreground">ANALYZE (run for real timings):</span>{" "}
          <span className="font-medium text-foreground">On</span> — actually runs each query and reports real
          per-node timings, actual row counts, and cache hits vs disk reads. This is what powers the before/after
          timing comparison and the estimate-vs-actual (stale stats) hint. The run happens inside a transaction
          that is rolled back, so writes don't persist.{" "}
          <span className="font-medium text-foreground">Off</span> — plan shape and planner estimates only; the
          query is not executed (faster, and safe for very expensive queries).
          {" "}Sample values are drawn for any <code>:param</code> either way.
        </div>
        {results && <ExplainLegend />}
        {!results && (
          <p className="text-sm text-muted-foreground">Use the "Explain" button above to see each query's execution plan.</p>
        )}
        {results?.map((r) => (
          <ExplainRow key={r.identifier} r={r} before={beforeById.get(r.identifier) ?? null} />
        ))}
      </CardContent>
    </Card>
  );
}

// The scan node that headlines a plan (Seq Scan is worst; report it first).
function scanKind(plan: string): string {
  for (const k of ["Seq Scan", "Bitmap Heap Scan", "Index Only Scan", "Index Scan"]) {
    if (plan.includes(k)) return k;
  }
  return "—";
}

// Top-node total wall time (ANALYZE only): "actual time=start..END".
function topTimeMs(plan: string): number | null {
  const m = plan.match(/actual time=[\d.]+\.\.([\d.]+)/);
  return m ? Number(m[1]) : null;
}

// A collapsible "how to read a plan" cheatsheet, so the guidance is one click away
// without permanently crowding the card.
function ExplainLegend() {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border bg-muted/30 text-xs">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left font-medium"
      >
        {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        How to read a plan
      </button>
      {open && (
        <ul className="list-disc space-y-1 px-3 pb-3 pl-8 text-muted-foreground">
          <li><span className="font-medium text-foreground">Seq Scan</span> on a big table → reads every row; add an index on the WHERE/JOIN columns.</li>
          <li><span className="font-medium text-foreground">Index Scan / Index Only Scan</span> → good; the index is being used.</li>
          <li><span className="font-medium text-foreground">actual time=start..end</span> → real time at that node (ANALYZE only); the top node's end is the query's total time.</li>
          <li><span className="font-medium text-foreground">rows=</span> (planner estimate) far from <span className="font-medium text-foreground">actual rows=</span> → stale statistics; run <code>ANALYZE &lt;table&gt;</code>.</li>
          <li><span className="font-medium text-foreground">Rows Removed by Filter</span> high → rows read then thrown away; a more selective index avoids the work.</li>
          <li><span className="font-medium text-foreground">Sort Method: external merge Disk</span> → spilled to disk; too many rows for memory (work_mem).</li>
          <li><span className="font-medium text-foreground">Buffers: shared hit</span> = from cache, <span className="font-medium text-foreground">read</span> = from disk; lots of reads = cold cache.</li>
          <li>Read the plan <span className="font-medium text-foreground">bottom-up</span> — the most-indented nodes run first.</li>
        </ul>
      )}
    </div>
  );
}

// Heuristic tuning hints derived from the plan text (client-side, best-effort).
function planHints(plan: string): { tone: "warn" | "info"; text: string }[] {
  const hints: { tone: "warn" | "info"; text: string }[] = [];
  if (/Seq Scan/.test(plan))
    hints.push({ tone: "warn", text: "Sequential scan — Postgres reads the whole table. Add an index on the filtered/joined columns for point or range access." });
  if (/(external merge\s+Disk|external sort\s+Disk|Sort Method:[^\n]*Disk)/.test(plan))
    hints.push({ tone: "warn", text: "A sort or hash spilled to disk — the working set exceeded memory. Reduce the rows scanned (a better index) so it fits in work_mem." });
  const removed = [...plan.matchAll(/Rows Removed by Filter:\s*([\d,]+)/g)].map((m) => Number(m[1].replace(/,/g, "")));
  const maxRemoved = removed.length ? Math.max(...removed) : 0;
  if (maxRemoved >= 1000)
    hints.push({ tone: "warn", text: `~${maxRemoved.toLocaleString()} rows were read then discarded by a filter — a more selective index would avoid scanning them.` });
  // Top-node planner estimate vs actual (ANALYZE only) — a large gap = stale stats.
  const est = plan.match(/\brows=(\d+)/);
  const act = plan.match(/actual time=[\d.]+\.\.[\d.]+ rows=(\d+)/);
  if (est && act) {
    const e = Number(est[1]);
    const a = Number(act[1]);
    if (a > 0 && e > 0 && (e / a >= 10 || a / e >= 10))
      hints.push({ tone: "warn", text: `Planner estimate (${e.toLocaleString()} rows) is far from actual (${a.toLocaleString()}) — statistics look stale. Run ANALYZE on the table.` });
  }
  if (!hints.length)
    hints.push({ tone: "info", text: "No obvious red flags — index scans and in-memory operations." });
  return hints;
}

// One collapsible per query (default collapsed) to keep the page uncluttered. When a
// `before` plan is supplied, the header summarizes the before→after change and the body
// shows both plans stacked.
function ExplainRow({ r, before }: { r: ExplainResultOut; before: ExplainResultOut | null }) {
  const [open, setOpen] = useState(false);
  const hints = r.error ? [] : planHints(r.plan);
  const warnCount = hints.filter((h) => h.tone === "warn").length;

  const cmp = before && !before.error && !r.error ? compareExplain(before, r) : null;

  const fmtMs = (n: number | null) => (n != null ? `${n.toFixed(1)} ms` : "");

  return (
    <div className="rounded-md border">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-2 border-b bg-muted/30 px-3 py-2 text-left"
      >
        {open ? <ChevronDown className="h-4 w-4 shrink-0" /> : <ChevronRight className="h-4 w-4 shrink-0" />}
        <span className="text-sm font-medium">{r.identifier}</span>
        {cmp ? (
          <span className="flex items-center gap-1 text-xs">
            <span className="text-muted-foreground">
              {cmp.beforeKind} {fmtMs(cmp.beforeMs)}
            </span>
            <span className="text-muted-foreground">→</span>
            <span className={cmp.improved ? "font-medium text-emerald-600 dark:text-emerald-400" : "text-foreground"}>
              {cmp.afterKind} {fmtMs(cmp.afterMs)}
            </span>
            {cmp.improved && <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />}
          </span>
        ) : (
          <>
            {r.seq_scan && (
              <Badge variant="destructive" className="gap-1">
                <AlertTriangle className="h-3 w-3" /> Seq Scan
              </Badge>
            )}
            {r.error && <Badge variant="secondary">error</Badge>}
            {!open && !r.error && warnCount > 0 && (
              <span className="ml-auto text-xs text-amber-600 dark:text-amber-400">
                {warnCount} hint{warnCount > 1 ? "s" : ""}
              </span>
            )}
          </>
        )}
      </button>
      {open && (
        <div>
          {!r.error && (
            <ul className="space-y-1 border-b p-3 text-xs">
              {hints.map((h, i) => (
                <li key={i} className={h.tone === "warn" ? "text-amber-600 dark:text-amber-400" : "text-muted-foreground"}>
                  • {h.text}
                </li>
              ))}
            </ul>
          )}
          {before && (
            <div className="border-b">
              <div className="px-3 pt-2 text-xs font-medium text-muted-foreground">Before (no index)</div>
              <pre className="overflow-x-auto p-3 text-xs leading-relaxed text-muted-foreground">
                {before.error ? before.error : before.plan}
              </pre>
            </div>
          )}
          {before && <div className="px-3 pt-2 text-xs font-medium text-foreground">After (index applied)</div>}
          <pre className="overflow-x-auto p-3 text-xs leading-relaxed">{r.error ? r.error : r.plan}</pre>
        </div>
      )}
    </div>
  );
}

function compareExplain(before: ExplainResultOut, after: ExplainResultOut) {
  const beforeKind = scanKind(before.plan);
  const afterKind = scanKind(after.plan);
  const beforeMs = topTimeMs(before.plan);
  const afterMs = topTimeMs(after.plan);
  const droppedSeqScan = beforeKind === "Seq Scan" && afterKind !== "Seq Scan";
  const faster = beforeMs != null && afterMs != null && afterMs < beforeMs * 0.9;
  return { beforeKind, afterKind, beforeMs, afterMs, improved: droppedSeqScan || faster };
}

function monitoringHint(host: string | null | undefined) {
  return (
    <p className="text-xs text-muted-foreground">
      Watch live activity (connections, throughput, CPU) in your Lakebase project's{" "}
      <span className="font-medium text-foreground">Monitoring</span> tab in Databricks
      {host ? (
        <>
          {" "}(
          <a
            href={`${host}/compute/database-instances`}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 text-primary hover:underline"
          >
            open Databricks <ExternalLink className="h-3 w-3" />
          </a>{" "}
          → your project → Monitoring)
        </>
      ) : (
        <> → your project → Monitoring</>
      )}
      .
    </p>
  );
}

// Content of the "Monitoring" tab: a deep link straight to the Lakebase project's
// query-history monitoring page for the endpoint this run used, with the generic
// hint as a fallback when the URL couldn't be resolved (oauth auth).
function MonitoringPanel({
  url,
  host,
}: {
  url: string | null | undefined;
  host: string | null | undefined;
}) {
  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        Inspect live activity for this endpoint — per-query latency, connections,
        throughput, and CPU — in your Lakebase project's{" "}
        <span className="font-medium text-foreground">Monitoring → Query history</span>.
      </p>
      {url ? (
        <Button asChild variant="outline" size="sm">
          <a href={url} target="_blank" rel="noreferrer">
            Open query history <ExternalLink className="ml-1 h-3 w-3" />
          </a>
        </Button>
      ) : (
        monitoringHint(host)
      )}
    </div>
  );
}

function TestingPage() {
  // Controlled so the History tab can refresh its session list when reopened.
  const [tab, setTab] = useState("psycopg");
  return (
    <TooltipProvider delayDuration={150}>
      <div>
        <PageHeader
          title="Concurrency Testing"
          description="Run your query mix against Lakebase at a target concurrency level. Use psycopg for a quick in-app test, or pgbench (Databricks job) for heavier, native PostgreSQL load."
        />
        <div className="p-8">
          <Tabs value={tab} onValueChange={setTab}>
            <TabsList>
              <TabsTrigger value="psycopg">psycopg (in-app)</TabsTrigger>
              <TabsTrigger value="pgbench">pgbench (Databricks job)</TabsTrigger>
              <TabsTrigger value="history">History</TabsTrigger>
            </TabsList>
            {/* forceMount keeps each tab mounted (just hidden) so an in-flight psycopg
                test or pgbench status poll keeps running when you switch tabs. */}
            <TabsContent value="psycopg" className="mt-6" forceMount hidden={tab !== "psycopg"}>
              <PsycopgTab />
            </TabsContent>
            <TabsContent value="pgbench" className="mt-6" forceMount hidden={tab !== "pgbench"}>
              <PgbenchTab />
            </TabsContent>
            <TabsContent value="history" className="mt-6" forceMount hidden={tab !== "history"}>
              <HistoryTab active={tab === "history"} />
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </TooltipProvider>
  );
}

const usd = (n: number | null | undefined, digits = 4) =>
  n == null ? "—" : `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: digits })}`;

// Attributes Lakebase *compute* cost to a single benchmark run: a deterministic
// modeled figure (CU x $/CU-hr x duration) plus, once usage lands, the run's share
// of actual billed compute proportionally allocated across the 10-minute buckets it
// spanned. Storage is excluded — it's a function of data size, not the run.
function RunCostCard({
  project,
  totalQueries,
  durationSeconds,
  window,
}: {
  project: string;
  totalQueries: number;
  durationSeconds: number;
  window: { start: string; end: string } | null;
}) {
  const { data: whData, isLoading: whLoading } = useListWarehouses();
  const warehouses = whData?.data.warehouses ?? [];
  const projectInfo = useGetProjectInfo({ params: { project }, query: { enabled: !!project } });
  const endpoints = projectInfo.data?.data.endpoints ?? [];
  // Default CU to the endpoint's max (upper bound). Pinning min=max makes it exact.
  const endpointMaxCu = endpoints.find((e) => e.max_cu)?.max_cu ?? null;
  const endpointMinCu = endpoints.find((e) => e.min_cu)?.min_cu ?? null;
  const pinned = endpointMinCu != null && endpointMinCu === endpointMaxCu;

  const [warehouseId, setWarehouseId] = useState("");
  const [cu, setCu] = useState<string>("");
  const [promo, setPromo] = useState(true); // 50% Lakebase compute promo (through Jan 2027)
  const [result, setResult] = useState<RunCostOut | null>(null);

  // Seed the CU input from the endpoint once project info loads.
  useEffect(() => {
    if (!cu && endpointMaxCu != null) setCu(String(endpointMaxCu));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [endpointMaxCu]);

  const getRunCost = useGetRunCost();

  const onEstimate = async () => {
    setResult(null);
    try {
      const res = await getRunCost.mutateAsync({
        project,
        warehouse_id: warehouseId,
        cu: Number(cu),
        duration_seconds: durationSeconds,
        total_queries: totalQueries,
        discount: promo ? 0.5 : 0,
        start: window?.start ?? null,
        end: window?.end ?? null,
      });
      setResult(res.data);
    } catch (e) {
      setResult({ error: String(e) });
    }
  };

  const est = result?.estimate;
  const rec = result?.reconcile;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <DollarSign className="h-4 w-4" /> Run cost (Lakebase compute)
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-end gap-3">
          <div className="grid gap-2">
            <LabelWithTip label="SQL warehouse" tip="Used to read pricing and billing usage from the system tables." />
            <Select value={warehouseId} onValueChange={setWarehouseId}>
              <SelectTrigger className="w-64">
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
          <div className="grid gap-2">
            <LabelWithTip
              label="CU during run"
              tip="Capacity Units the benchmark ran at. Pin the endpoint min=max (scale-to-zero off) so this is exact; otherwise it's an upper bound."
            />
            <Input
              type="number"
              step="0.5"
              min="0.5"
              value={cu}
              onChange={(e) => setCu(e.target.value)}
              className="w-32"
            />
          </div>
          <label className="flex items-center gap-2 pb-2 text-sm">
            <input type="checkbox" checked={promo} onChange={(e) => setPromo(e.target.checked)} />
            50% compute promo
          </label>
          <Button onClick={onEstimate} disabled={getRunCost.isPending || !warehouseId || !cu}>
            {getRunCost.isPending ? "Estimating…" : "Estimate run cost"}
          </Button>
        </div>

        {!pinned && endpointMaxCu != null && (
          <p className="text-xs text-amber-600 dark:text-amber-400">
            This endpoint autoscales ({endpointMinCu}–{endpointMaxCu} CU); the modeled cost uses the
            value above as an upper bound. Pin min=max for an exact figure, or use the reconciled cost.
          </p>
        )}

        {result?.error && (
          <div className="flex items-start gap-2 rounded-md border p-3 text-sm text-red-600 dark:text-red-400">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span className="whitespace-pre-wrap break-words">{result.error}</span>
          </div>
        )}

        {est && (
          <div>
            <h3 className="mb-2 text-sm font-medium">Modeled (CU × ${est.price_per_cu_hour.toFixed(4)}/CU-hr × duration)</h3>
            <div className="grid gap-4 sm:grid-cols-3">
              {metric("Run cost", usd(est.cost))}
              {metric("$ / 1M queries", usd(est.cost_per_million_queries, 2))}
              {metric("Queries / $", est.queries_per_dollar != null ? est.queries_per_dollar.toLocaleString() : "—")}
            </div>
            {est.price_source === "default" && (
              <p className="mt-2 text-xs text-muted-foreground">
                No compute usage history for this project yet — used the default list price ($0.111/CU-hr).
              </p>
            )}
          </div>
        )}

        {rec && (
          <div className="border-t pt-4">
            <h3 className="mb-2 text-sm font-medium">Reconciled with actual billing</h3>
            {rec.available ? (
              <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-4">
                {metric("Actual cost", usd(rec.cost))}
                {metric("$ / 1M queries", usd(rec.cost_per_million_queries, 2))}
                {metric("Effective avg CU", rec.effective_avg_cu != null ? rec.effective_avg_cu.toString() : "—")}
                {metric("CU-hours", rec.cu_hours.toFixed(4))}
              </div>
            ) : null}
            <p className="mt-2 text-xs text-muted-foreground">{rec.note}</p>
          </div>
        )}

        <div className="flex items-start gap-2 rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
          <Info className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Compute only (storage bills daily by data size, not per run) and from <span className="font-medium text-foreground">list
            prices</span>. The load generator's own compute (pgbench job / this app) is excluded — that's client
            cost, not database cost. Use <span className="font-medium text-foreground">$ / 1M queries</span> to compare
            against other systems.
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

function PsycopgTab() {
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [concurrency, setConcurrency] = useState(10);
  const [totalExecutions, setTotalExecutions] = useState(100);
  const [queries, setQueries] = useState<QueryRow[]>(SAMPLE_QUERIES);
  const [report, setReport] = useState<TestReportOut | null>(null);
  const [baseline, setBaseline] = useState<TestReportOut | null>(null);
  const [optimize, setOptimize] = useState<OptimizeOut | null>(null);
  // Wall-clock window of the last run, for billing reconciliation in the cost card.
  const [runWindow, setRunWindow] = useState<{ start: string; end: string } | null>(null);

  const runTest = useRunPsycopgTest();
  const runOptimize = useOptimizeAnalyze();
  const applyIdx = useApplyIndexes();
  const explain = useExplainQueries();
  const [explainResults, setExplainResults] = useState<ExplainResultOut[] | null>(null);
  const [explainBefore, setExplainBefore] = useState<ExplainResultOut[] | null>(null);
  const [explainAnalyze, setExplainAnalyze] = useState(true);
  const [verifying, setVerifying] = useState(false);
  const { data: wsInfo } = useGetWorkspaceInfo();
  const host = wsInfo?.data.host;

  // Elapsed timer while the (blocking) test request is in flight, so the page
  // visibly shows progress instead of looking hung.
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!runTest.isPending) {
      setElapsed(0);
      return;
    }
    const t = setInterval(() => setElapsed((e) => e + 1), 1000);
    return () => clearInterval(t);
  }, [runTest.isPending]);

  // Total executions are distributed across the queries by weight on the backend.
  const estimatedQueries = Math.max(totalExecutions, queries.length);

  const body = () => ({
    auth_method: conn.auth_method,
    project: conn.project || null,
    database: conn.database || null,
    db_schema: conn.db_schema || null,
    endpoint_host: conn.endpoint_host || null,
    access_token: conn.access_token || null,
    postgres_user_name: conn.postgres_user_name || null,
  });

  const runOnce = async (): Promise<TestReportOut | null> => {
    const start = new Date().toISOString();
    const res = await runTest.mutateAsync({
      ...body(),
      concurrency_level: concurrency,
      total_executions: totalExecutions,
      queries,
    });
    setRunWindow({ start, end: new Date().toISOString() });
    return res.data;
  };

  // Auto-capture completed runs into the local session store (the History tab reads
  // it). The id stays stable across an apply-and-rerun so a before/after pair is one
  // entry; a fresh run starts a new id.
  const runIdRef = useRef<string>("");
  const captureRun = (
    baselineReport: TestReportOut | null,
    optimizedReport: TestReportOut | null,
    ddls: string[],
  ) => {
    saveBrowserRun({
      id: runIdRef.current || (runIdRef.current = newRunId()),
      created_at: new Date().toISOString(),
      engine: "psycopg",
      label: null,
      project: conn.project || null,
      config: { concurrency_level: concurrency, total_executions: totalExecutions },
      queries: queries.map((q) => ({ identifier: q.identifier, content: q.content })),
      baseline_report: baselineReport as unknown as Record<string, unknown> | null,
      optimized_report: optimizedReport as unknown as Record<string, unknown> | null,
      index_ddls: ddls,
    });
  };

  const onRun = async () => {
    try {
      const data = await runOnce();
      setReport(data);
      setBaseline(null); // fresh run — start a new before/after lineage
      if (data?.error) {
        toast.error(data.error);
      } else {
        runIdRef.current = newRunId();
        captureRun(data, null, []);
        toast.success("Test complete");
      }
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
      const before = report; // snapshot "before"
      if (before) setBaseline(before);
      const ddls = optimize.index_suggestions.map((s) => s.ddl);
      await applyDdls(ddls);
      const data = await runOnce(); // "after"
      setReport(data);
      if (data?.error) {
        toast.error(data.error);
      } else {
        captureRun(before, data, ddls); // upsert same entry → one before/after row
        toast.success("Re-ran test after applying indexes");
      }
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

  const runExplain = async (): Promise<ExplainResultOut[] | null> => {
    const res = await explain.mutateAsync({ ...body(), queries, analyze: explainAnalyze });
    if (res.data.error) {
      toast.error(res.data.error);
      return null;
    }
    return res.data.results ?? [];
  };

  const onExplain = async () => {
    try {
      const out = await runExplain();
      if (out) {
        setExplainBefore(null); // single-plan view
        setExplainResults(out);
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  // The full loop in one click: EXPLAIN (before) → apply the suggested indexes →
  // EXPLAIN (after), then show the plan diff per query.
  const onVerifyPlans = async () => {
    if (!optimize?.index_suggestions.length) return;
    setVerifying(true);
    try {
      const beforePlans = await runExplain();
      if (!beforePlans) return;
      const ok = await applyDdls(optimize.index_suggestions.map((s) => s.ddl));
      if (!ok) return; // applyDdls surfaces its own errors
      const afterPlans = await runExplain();
      if (!afterPlans) return;
      setExplainBefore(beforePlans);
      setExplainResults(afterPlans);
      toast.success("Compared plans before/after applying indexes");
    } catch (e) {
      toast.error(String(e));
    } finally {
      setVerifying(false);
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
          <QueryParamHelp />
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
              <LabelWithTip
                label="Concurrency level"
                tip="How many queries run in parallel against Lakebase. The connection pool is sized from this (base pool ≈ level/4, max overflow = level), then the query mix is replayed concurrently to measure latency and throughput."
              />
              <Input
                type="number"
                className="w-32"
                min={1}
                max={1000}
                value={concurrency}
                onChange={(e) => setConcurrency(Number(e.target.value))}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Total executions"
                tip="Total number of query executions for the run, distributed across the queries in proportion to each query's WEIGHT. Every query runs at least once."
              />
              <Input
                type="number"
                className="w-32"
                min={1}
                max={100000}
                value={totalExecutions}
                onChange={(e) => setTotalExecutions(Number(e.target.value))}
              />
            </div>
            <Button onClick={onRun} disabled={runTest.isPending}>
              <Play className="mr-1 h-4 w-4" />
              {runTest.isPending ? "Running…" : "Run test"}
            </Button>
            <Button variant="secondary" onClick={onExplain} disabled={explain.isPending || verifying}>
              <ScanSearch className="mr-1 h-4 w-4" />
              {explain.isPending ? "Explaining…" : "Explain"}
            </Button>
            <Button variant="secondary" onClick={onOptimize} disabled={runOptimize.isPending}>
              <Wand2 className="mr-1 h-4 w-4" />
              {runOptimize.isPending ? "Analyzing…" : "Optimize"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {runTest.isPending && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin" /> Running test…
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="h-2 w-full animate-pulse rounded bg-primary/60" />
            <p className="text-sm text-muted-foreground">
              Executing ~{estimatedQueries.toLocaleString()} queries at concurrency {concurrency}.
              Elapsed {elapsed}s. The whole batch runs in a single request, so results appear only
              when it finishes — higher concurrency and query counts take longer.
            </p>
            {monitoringHint(host)}
          </CardContent>
        </Card>
      )}

      {report && (
        <Card>
          <CardHeader>
            <CardTitle>Results</CardTitle>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="metrics">
              <TabsList>
                <TabsTrigger value="metrics">Metrics</TabsTrigger>
                <TabsTrigger value="monitoring">Monitoring</TabsTrigger>
              </TabsList>
              <TabsContent value="metrics" className="mt-4">
                <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-4">
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
                  {report.cache_hit_pct != null &&
                    metric(
                      "Cache hit (this run)",
                      `${report.cache_hit_pct.toFixed(1)}%`,
                      report.cache_hit_pct >= 99 ? "good" : report.cache_hit_pct >= 90 ? "warn" : "bad",
                    )}
                </div>
                <ReferenceBands
                  p99={report.p99_execution_time_ms}
                  qps={report.throughput_queries_per_second}
                />
                {report.per_query && report.per_query.length > 0 && (
                  <div className="mt-6">
                    <h3 className="mb-2 text-sm font-medium">Per-query breakdown</h3>
                    <PerQueryTable rows={report.per_query} />
                  </div>
                )}
              </TabsContent>
              <TabsContent value="monitoring" className="mt-4">
                <MonitoringPanel url={report.monitoring_url} host={host} />
              </TabsContent>
            </Tabs>
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

      {/* Loop order: Explain (diagnose) → Optimize (prescribe) → Cost. */}
      <ExplainPlansCard
        results={explainResults}
        before={explainBefore}
        analyze={explainAnalyze}
        onAnalyzeChange={setExplainAnalyze}
        busy={explain.isPending}
        onVerify={onVerifyPlans}
        canVerify={!!optimize?.index_suggestions.length}
        verifyBusy={verifying}
      />

      {optimize && (
        <OptimizeCard
          optimize={optimize}
          onApplyOne={onApplyOne}
          onApplyAll={onApplyAllAndRerun}
          busy={applyIdx.isPending || runTest.isPending}
          applyAllLabel="Apply all & re-run test"
          applyAllNote='"Apply all & re-run" snapshots the current result as the baseline, creates the indexes, and re-runs the test so you can see the before/after impact.'
        />
      )}

      {report && !report.error && conn.project && (
        <RunCostCard
          project={conn.project}
          totalQueries={report.total_queries_executed}
          durationSeconds={report.total_duration_seconds}
          window={runWindow}
        />
      )}
    </div>
  );
}

// --------------------------------------------------------------------------- //
// History tab — current-session runs (localStorage) + Lakebase archive
// --------------------------------------------------------------------------- //
const numOf = (v: unknown): number | null => (typeof v === "number" ? v : null);

function reportMetrics(engine: Engine, report: Record<string, unknown> | null): { label: string; value: string }[] {
  if (!report) return [];
  const n = (k: string) => numOf(report[k]);
  if (engine === "pgbench") {
    return [
      { label: "TPS", value: n("tps") != null ? n("tps")!.toFixed(2) : "—" },
      { label: "Avg", value: n("latency_avg_ms") != null ? `${n("latency_avg_ms")!.toFixed(1)} ms` : "—" },
      { label: "p95", value: n("latency_p95_ms") != null ? `${n("latency_p95_ms")!.toFixed(1)} ms` : "—" },
      { label: "p99", value: n("latency_p99_ms") != null ? `${n("latency_p99_ms")!.toFixed(1)} ms` : "—" },
    ];
  }
  return [
    { label: "Throughput", value: n("throughput_queries_per_second") != null ? `${n("throughput_queries_per_second")!.toFixed(2)} qps` : "—" },
    { label: "Avg", value: n("average_execution_time_ms") != null ? `${n("average_execution_time_ms")!.toFixed(0)} ms` : "—" },
    { label: "p95", value: n("p95_execution_time_ms") != null ? `${n("p95_execution_time_ms")!.toFixed(0)} ms` : "—" },
    { label: "p99", value: n("p99_execution_time_ms") != null ? `${n("p99_execution_time_ms")!.toFixed(0)} ms` : "—" },
  ];
}

function configSummary(engine: Engine, config: Record<string, unknown>): string {
  if (engine === "pgbench") {
    return `clients ${config.clients ?? "?"} · jobs ${config.jobs ?? "?"} · ${config.duration_seconds ?? "?"}s · ${config.protocol ?? ""}`;
  }
  return `concurrency ${config.concurrency_level ?? "?"}`;
}

// The before→after comparison rows for an archived run, mirroring the live "Before /
// after — indexes applied" card. Engine-specific metric keys, same compareRow renderer.
function BeforeAfterRows({
  engine,
  baseline,
  optimized,
}: {
  engine: Engine;
  baseline: Record<string, unknown>;
  optimized: Record<string, unknown>;
}) {
  const b = (k: string) => numOf(baseline[k]) ?? 0;
  const a = (k: string) => numOf(optimized[k]) ?? 0;
  if (engine === "pgbench") {
    return (
      <>
        {compareRow("TPS", b("tps"), a("tps"), false, "tps")}
        {compareRow("Avg latency", b("latency_avg_ms"), a("latency_avg_ms"), true, "ms")}
        {compareRow("p95 latency", b("latency_p95_ms"), a("latency_p95_ms"), true, "ms")}
        {compareRow("p99 latency", b("latency_p99_ms"), a("latency_p99_ms"), true, "ms")}
      </>
    );
  }
  return (
    <>
      {compareRow("Avg latency", b("average_execution_time_ms"), a("average_execution_time_ms"), true, "ms")}
      {compareRow("p95 latency", b("p95_execution_time_ms"), a("p95_execution_time_ms"), true, "ms")}
      {compareRow("p99 latency", b("p99_execution_time_ms"), a("p99_execution_time_ms"), true, "ms")}
      {compareRow("Throughput", b("throughput_queries_per_second"), a("throughput_queries_per_second"), false, "qps")}
    </>
  );
}

interface RunView {
  id: string;
  engine: Engine;
  label?: string | null;
  project?: string | null;
  created_at: string;
  created_by?: string | null;
  config: Record<string, unknown>;
  baseline_report: Record<string, unknown> | null;
  optimized_report: Record<string, unknown> | null;
  index_ddls: string[];
}

function RunRow({
  run,
  onDelete,
  onRelabel,
}: {
  run: RunView;
  onDelete?: (id: string) => void;
  onRelabel?: (id: string, label: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const report = run.optimized_report ?? run.baseline_report;
  const metrics = reportMetrics(run.engine, report);
  const pq = (report?.per_query as QueryStat[] | undefined) ?? [];
  const hasBeforeAfter = !!run.optimized_report && !!run.baseline_report;
  const hasDetail = pq.length > 0 || run.index_ddls.length > 0 || hasBeforeAfter;
  return (
    <div className="space-y-2 rounded-lg border p-3">
      <div className="flex flex-wrap items-center gap-2">
        {hasDetail ? (
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 shrink-0"
            onClick={() => setOpen((o) => !o)}
            aria-label={open ? "Collapse details" : "Expand details"}
          >
            {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </Button>
        ) : (
          <span className="w-6 shrink-0" />
        )}
        <Badge variant="secondary">{run.engine}</Badge>
        {onRelabel ? (
          <Input
            className="h-7 max-w-xs"
            placeholder="add a label…"
            defaultValue={run.label ?? ""}
            onBlur={(e) => onRelabel(run.id, e.target.value)}
          />
        ) : (
          run.label && <span className="text-sm font-medium">{run.label}</span>
        )}
        <span className="text-xs text-muted-foreground">
          {run.project ? `${run.project} · ` : ""}
          {run.created_at ? new Date(run.created_at).toLocaleString() : ""}
          {run.created_by ? ` · ${run.created_by}` : ""}
        </span>
        <span className="ml-auto text-xs text-muted-foreground">{configSummary(run.engine, run.config)}</span>
        {onDelete && (
          <Button variant="ghost" size="icon" onClick={() => onDelete(run.id)}>
            <Trash2 className="h-4 w-4" />
          </Button>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-4 text-sm">
        {metrics.map((m) => (
          <span key={m.label} className="text-muted-foreground">
            {m.label}: <span className="font-medium text-foreground">{m.value}</span>
          </span>
        ))}
        {hasBeforeAfter && <Badge>before/after</Badge>}
      </div>
      {open && (
        <>
          {hasBeforeAfter && run.baseline_report && run.optimized_report && (
            <div className="space-y-2">
              <h4 className="flex items-center gap-1.5 text-xs font-medium">
                <Zap className="h-3.5 w-3.5" /> Before / after — indexes applied
              </h4>
              <BeforeAfterRows
                engine={run.engine}
                baseline={run.baseline_report}
                optimized={run.optimized_report}
              />
            </div>
          )}
          {pq.length > 0 && <PerQueryTable rows={pq} />}
          {run.index_ddls.length > 0 && (
            <div className="text-xs text-muted-foreground">
              Indexes:{" "}
              {run.index_ddls.map((d, i) => (
                <code key={i} className="mr-2 break-all">
                  {d}
                </code>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function HistoryTab({ active }: { active: boolean }) {
  const [pref, setPref] = useState<HistoryPref>(loadPref);
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [sessionRuns, setSessionRuns] = useState<SavedRun[]>([]);
  const [lakebaseRuns, setLakebaseRuns] = useState<HistoryRunOut[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [enableInfo, setEnableInfo] = useState<{
    ok: boolean;
    message: string;
    grant_sql?: string | null;
    ddl?: string | null;
  } | null>(null);

  const enable = useEnableLakebaseHistory();
  const archive = useArchiveLakebaseHistory();
  const listRuns = useListLakebaseHistory();
  const listTables = useListLakebaseHistoryTables();

  // The tab stays mounted (forceMount), so reload the session runs whenever it
  // becomes visible — otherwise it would only ever show what existed at first mount.
  useEffect(() => {
    if (active) setSessionRuns(loadBrowserRuns());
  }, [active]);

  const updatePref = (patch: Partial<HistoryPref>) => {
    const next = { ...pref, ...patch };
    setPref(next);
    savePref(next);
  };

  const refreshSession = () => setSessionRuns(loadBrowserRuns());
  const onDeleteSession = (id: string) => {
    deleteBrowserRun(id);
    refreshSession();
  };
  const onClearSession = () => {
    clearBrowserRuns();
    refreshSession();
  };
  const onRelabel = (id: string, label: string) => {
    const r = loadBrowserRuns().find((x) => x.id === id);
    if (r) {
      saveBrowserRun({ ...r, label: label.trim() || null });
      refreshSession();
    }
  };

  const connBody = () => ({
    auth_method: conn.auth_method,
    project: conn.project || null,
    database: conn.database || null,
    db_schema: conn.db_schema || null,
    endpoint_host: conn.endpoint_host || null,
    access_token: conn.access_token || null,
    postgres_user_name: conn.postgres_user_name || null,
    schema_name: pref.schema,
    table_name: pref.table,
  });

  const onLoadTables = async () => {
    try {
      const res = await listTables.mutateAsync(connBody());
      setTables(res.data.tables ?? []);
      if (res.data.error) toast.error(res.data.error);
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onListLakebase = async () => {
    try {
      const res = await listRuns.mutateAsync(connBody());
      setLakebaseRuns(res.data.runs ?? []);
      if (res.data.error) toast.error(res.data.error);
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onEnable = async () => {
    try {
      const res = await enable.mutateAsync(connBody());
      setEnableInfo(res.data);
      if (res.data.ok) {
        updatePref({ lakebaseConsented: true });
        toast.success(res.data.message);
        onListLakebase();
      } else {
        toast.error(res.data.error || res.data.message);
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onArchive = async () => {
    if (sessionRuns.length === 0) {
      toast.error("No session runs to archive.");
      return;
    }
    try {
      const res = await archive.mutateAsync({
        ...connBody(),
        runs: sessionRuns.map((r) => ({
          id: r.id,
          engine: r.engine,
          label: r.label ?? null,
          project: r.project ?? null,
          created_at: r.created_at,
          config: r.config,
          queries: r.queries,
          baseline_report: r.baseline_report,
          optimized_report: r.optimized_report,
          index_ddls: r.index_ddls,
        })),
      });
      if (res.data.ok) {
        toast.success(`Archived ${res.data.inserted} run(s) to ${pref.table}`);
        onListLakebase();
      } else {
        toast.error(res.data.error || "Archive failed");
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  const needsConsent = !pref.lakebaseConsented;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <History className="h-4 w-4" /> Current session runs
            <Badge variant="secondary">{sessionRuns.length}</Badge>
          </CardTitle>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={refreshSession}>
              <RefreshCw className="mr-1 h-4 w-4" /> Refresh
            </Button>
            <Button variant="outline" size="sm" onClick={onClearSession} disabled={sessionRuns.length === 0}>
              <Trash2 className="mr-1 h-4 w-4" /> Clear
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-xs text-muted-foreground">
            Completed psycopg and pgbench runs are saved here automatically (in this browser) and persist
            across reloads. Archive them to Lakebase below to keep them durably and share them.
          </p>
          {sessionRuns.length === 0 ? (
            <p className="text-sm text-muted-foreground">No runs yet — run a test in the psycopg or pgbench tab.</p>
          ) : (
            sessionRuns.map((r) => <RunRow key={r.id} run={r} onDelete={onDeleteSession} onRelabel={onRelabel} />)
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Saved history (Lakebase)</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <LakebaseConnection value={conn} onChange={setConn} />

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="grid gap-2">
              <LabelWithTip
                label="Table"
                tip="History table inside the app service principal's dedicated schema. Pick an existing table or type a new name (created on archive)."
              />
              <Input
                list="history-tables"
                value={pref.table}
                onChange={(e) => updatePref({ table: e.target.value })}
                placeholder={DEFAULT_TABLE}
              />
              <datalist id="history-tables">
                {tables.map((t) => (
                  <option key={t} value={t} />
                ))}
              </datalist>
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Schema (SP-owned)"
                tip="The dedicated schema the app service principal owns. Leave as default unless a project owner provisioned a different one."
              />
              <Input
                value={pref.schema}
                onChange={(e) => updatePref({ schema: e.target.value, lakebaseConsented: false })}
                placeholder={DEFAULT_SCHEMA}
              />
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button variant="outline" size="sm" onClick={onLoadTables} disabled={listTables.isPending}>
              {listTables.isPending ? "Loading…" : "List existing tables"}
            </Button>
            <Button variant="outline" size="sm" onClick={onEnable} disabled={enable.isPending}>
              <ShieldCheck className="mr-1 h-4 w-4" />
              {enable.isPending ? "Checking…" : needsConsent ? "Enable / create table" : "Re-check table"}
            </Button>
            <Button
              size="sm"
              onClick={onArchive}
              disabled={archive.isPending || needsConsent || sessionRuns.length === 0}
            >
              <Save className="mr-1 h-4 w-4" />
              {archive.isPending ? "Archiving…" : `Archive ${sessionRuns.length} session run(s)`}
            </Button>
            <Button variant="outline" size="sm" onClick={onListLakebase} disabled={listRuns.isPending}>
              <RefreshCw className="mr-1 h-4 w-4" /> {listRuns.isPending ? "Loading…" : "Load saved runs"}
            </Button>
          </div>

          <div className="rounded-md border bg-muted/30 p-3 text-xs">
            <div className="flex items-center gap-2 font-medium text-foreground">
              <ShieldCheck className="h-4 w-4" />
              {pref.lakebaseConsented ? "Lakebase history enabled" : "Enable shared Lakebase history"}
            </div>
            <p className="mt-2 text-muted-foreground">
              The app connects to the project above as its{" "}
              <span className="font-medium text-foreground">service principal</span> and writes one row per
              run into{" "}
              <code>
                {pref.schema}.{pref.table}
              </code>
              , attributed to you via <code>created_by</code>. The SP is confined to its own schema and has
              no access to your other tables.
            </p>
            {enableInfo && !enableInfo.ok && enableInfo.message && (
              <p className="mt-2 text-muted-foreground">{enableInfo.message}</p>
            )}
            {enableInfo && !enableInfo.ok && enableInfo.grant_sql && (
              <pre className="mt-2 overflow-x-auto rounded bg-background p-2 text-[11px]">{enableInfo.grant_sql}</pre>
            )}
            {enableInfo?.ddl && (
              <details className="mt-2">
                <summary className="cursor-pointer text-muted-foreground">Table definition</summary>
                <pre className="mt-1 overflow-x-auto rounded bg-background p-2 text-[11px]">{enableInfo.ddl}</pre>
              </details>
            )}
          </div>

          {lakebaseRuns.length > 0 && (
            <div className="space-y-3">
              {lakebaseRuns.map((r) => (
                <RunRow
                  key={r.id}
                  run={{
                    id: r.id,
                    engine: (r.engine as Engine) ?? "psycopg",
                    label: r.label,
                    project: r.project,
                    created_at: r.created_at ?? "",
                    created_by: r.created_by,
                    config: r.config ?? {},
                    baseline_report: r.baseline_report ?? null,
                    optimized_report: r.optimized_report ?? null,
                    index_ddls: r.index_ddls ?? [],
                  }}
                />
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
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

// Either runner returns at least these fields; local runs have no Databricks job URL.
type PgbenchRunInfo = {
  status: string;
  run_id?: string | null;
  monitoring_url?: string | null;
  job_run_url?: string | null;
};

function PgbenchTab() {
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [config, setConfig] = useState<PgbenchConfigState>(PGBENCH_DEFAULT_CONFIG);
  const [queries, setQueries] = useState<QueryRow[]>(SAMPLE_QUERIES);
  const [runMode, setRunMode] = useState<"job" | "local">("job");
  const [submitted, setSubmitted] = useState<PgbenchRunInfo | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [optimize, setOptimize] = useState<OptimizeOut | null>(null);
  const [baseline, setBaseline] = useState<Record<string, unknown> | null>(null);
  // Wall-clock window of the last completed run, for billing reconciliation.
  const [runWindow, setRunWindow] = useState<{ start: string; end: string } | null>(null);
  const runStartRef = useRef<string>("");

  const submit = useSubmitPgbenchJob();
  const submitLocal = useSubmitLocalPgbench();
  const runOptimize = useOptimizeAnalyze();
  const applyIdx = useApplyIndexes();
  const explain = useExplainQueries();
  const [explainResults, setExplainResults] = useState<ExplainResultOut[] | null>(null);
  const [explainBefore, setExplainBefore] = useState<ExplainResultOut[] | null>(null);
  const [explainAnalyze, setExplainAnalyze] = useState(true);
  const [verifying, setVerifying] = useState(false);
  const { data: wsInfo } = useGetWorkspaceInfo();
  const host = wsInfo?.data.host;

  // Local (dev-only) pgbench is offered only when the backend reports the binary is
  // present and it's not running as a deployed Databricks App — never in production.
  const { data: caps } = useGetTestingCapabilities();
  const localAvailable = caps?.data.pgbench_local_available ?? false;
  const isLocal = runMode === "local";

  const refetchInterval = (s: string | undefined) =>
    s === "completed" || s === "failed" ? false : 3000;

  const jobStatusQuery = useGetPgbenchRunStatus({
    params: { run_id: runId ?? "" },
    query: {
      enabled: !!runId && !isLocal,
      refetchInterval: (query) => refetchInterval(query.state.data?.data.status),
    },
  });
  const localStatusQuery = useGetLocalPgbenchStatus({
    params: { run_id: runId ?? "" },
    query: {
      enabled: !!runId && isLocal,
      refetchInterval: (query) => refetchInterval(query.state.data?.data.status),
    },
  });
  const status = (isLocal ? localStatusQuery : jobStatusQuery).data?.data;

  const setCfg = (patch: Partial<PgbenchConfigState>) => setConfig((c) => ({ ...c, ...patch }));
  const updateQuery = (i: number, patch: Partial<QueryRow>) =>
    setQueries((qs) => qs.map((q, idx) => (idx === i ? { ...q, ...patch } : q)));

  const body = () => ({
    auth_method: conn.auth_method,
    project: conn.project || null,
    database: conn.database || null,
    db_schema: conn.db_schema || null,
    endpoint_host: conn.endpoint_host || null,
    access_token: conn.access_token || null,
    postgres_user_name: conn.postgres_user_name || null,
  });

  // Auto-capture lineage (see the capture effect below). A fresh submit starts a new
  // id; after applying indexes, baselineRef holds the "before" so the next completed
  // run is recorded as the "after" of the same entry.
  const runIdRef = useRef<string>("");
  const capturedRef = useRef<string>("");
  const baselineRef = useRef<Record<string, unknown> | null>(null);
  const appliedDdlsRef = useRef<string[]>([]);

  const onSubmit = async () => {
    const payload = {
      ...body(),
      config,
      queries,
    };
    // A fresh run (not the re-run after applying indexes) starts a new lineage.
    if (!baselineRef.current) {
      runIdRef.current = newRunId();
      appliedDdlsRef.current = [];
      setBaseline(null);
    }
    capturedRef.current = "";
    runStartRef.current = new Date().toISOString();
    setRunWindow(null);
    try {
      const res = isLocal
        ? await submitLocal.mutateAsync(payload)
        : await submit.mutateAsync(payload);
      if (res.data.error) {
        toast.error(res.data.error);
        return;
      }
      setSubmitted(res.data);
      setRunId(res.data.run_id ?? null);
      toast.success(isLocal ? "local pgbench started" : "pgbench job submitted");
    } catch (e) {
      toast.error(String(e));
    }
  };

  const runExplain = async (): Promise<ExplainResultOut[] | null> => {
    const res = await explain.mutateAsync({ ...body(), queries, analyze: explainAnalyze });
    if (res.data.error) {
      toast.error(res.data.error);
      return null;
    }
    return res.data.results ?? [];
  };

  const onExplain = async () => {
    try {
      const out = await runExplain();
      if (out) {
        setExplainBefore(null);
        setExplainResults(out);
      }
    } catch (e) {
      toast.error(String(e));
    }
  };

  // EXPLAIN (before) → apply suggested indexes → EXPLAIN (after) → show the plan diff.
  const onVerifyPlans = async () => {
    if (!optimize?.index_suggestions.length) return;
    setVerifying(true);
    try {
      const beforePlans = await runExplain();
      if (!beforePlans) return;
      const ok = await applyDdls(optimize.index_suggestions.map((s) => s.ddl));
      if (!ok) return;
      const afterPlans = await runExplain();
      if (!afterPlans) return;
      setExplainBefore(beforePlans);
      setExplainResults(afterPlans);
      toast.success("Compared plans before/after applying indexes");
    } catch (e) {
      toast.error(String(e));
    } finally {
      setVerifying(false);
    }
  };

  const pending = submit.isPending || submitLocal.isPending;
  const r = status?.pgbench_results ?? null;
  const num = (v: unknown): number | null =>
    typeof v === "number" ? v : v != null && !isNaN(Number(v)) ? Number(v) : null;
  const running = !!runId && status?.status !== "completed" && status?.status !== "failed";

  const applyDdls = async (ddls: string[]): Promise<boolean> => {
    const res = await applyIdx.mutateAsync({ ...body(), ddls });
    if (res.data.error) {
      toast.error(res.data.error);
      return false;
    }
    const failed = (res.data.results ?? []).filter((x) => !x.ok);
    if (failed.length) failed.forEach((f) => toast.error(f.detail));
    else toast.success(`Applied ${ddls.length} index(es)`);
    return failed.length === 0;
  };

  const onOptimize = async () => {
    try {
      const res = await runOptimize.mutateAsync({ ...body(), queries, run_live: true });
      setOptimize(res.data);
      if (res.data.error) toast.warning(res.data.error);
    } catch (e) {
      toast.error(String(e));
    }
  };

  const onApplyOne = async (ddl: string) => {
    try {
      await applyDdls([ddl]);
    } catch (e) {
      toast.error(String(e));
    }
  };

  // pgbench runs are heavy (a job spins up a cluster), so we don't auto-rerun: apply
  // the indexes, snapshot the current result as the baseline, and let the user submit
  // again — the next completed run is recorded as the "after".
  const onApplyAllAndSnapshot = async () => {
    if (!optimize) return;
    try {
      const ddls = optimize.index_suggestions.map((s) => s.ddl);
      if (!(await applyDdls(ddls))) return;
      appliedDdlsRef.current = ddls;
      if (r) {
        baselineRef.current = r as Record<string, unknown>;
        setBaseline(r as Record<string, unknown>);
      }
      toast.success("Indexes applied — re-run pgbench to see the before/after");
    } catch (e) {
      toast.error(String(e));
    }
  };

  // Auto-capture each completed run into the local session store (deduped per backend
  // run id). A before (baselineRef) folds in as the optimized "after" of one entry.
  useEffect(() => {
    if (status?.status !== "completed" || !r || !runId || capturedRef.current === runId) return;
    capturedRef.current = runId;
    setRunWindow({ start: runStartRef.current, end: new Date().toISOString() });
    const before = baselineRef.current;
    saveBrowserRun({
      id: runIdRef.current || (runIdRef.current = newRunId()),
      created_at: new Date().toISOString(),
      engine: "pgbench",
      label: null,
      project: conn.project || null,
      config: {
        clients: config.clients,
        jobs: config.jobs,
        duration_seconds: config.duration_seconds,
        protocol: config.protocol,
      },
      queries: queries.map((q) => ({ identifier: q.identifier, content: q.content })),
      baseline_report: before ?? (r as Record<string, unknown>),
      optimized_report: before ? (r as Record<string, unknown>) : null,
      index_ddls: appliedDdlsRef.current,
    });
    if (before) baselineRef.current = null; // before/after lineage complete
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status?.status, runId, r]);

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
              <LabelWithTip
                label="Clients (-c)"
                tip="Number of concurrent database clients (simulated connections) hammering Lakebase at once. pgbench's -c flag."
              />
              <Input
                type="number"
                min={1}
                max={1000}
                value={config.clients}
                onChange={(e) => setCfg({ clients: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Worker threads (-j)"
                tip="Number of pgbench worker threads driving the clients, spread across CPU cores. Keep ≤ clients. pgbench's -j flag."
              />
              <Input
                type="number"
                min={1}
                max={100}
                value={config.jobs}
                onChange={(e) => setCfg({ jobs: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Duration s (-T)"
                tip="How long the benchmark runs, in seconds. pgbench's -T flag."
              />
              <Input
                type="number"
                min={1}
                max={3600}
                value={config.duration_seconds}
                onChange={(e) => setCfg({ duration_seconds: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Progress s (-P)"
                tip="Interval, in seconds, between progress/latency reports printed during the run. pgbench's -P flag."
              />
              <Input
                type="number"
                min={1}
                max={60}
                value={config.progress_interval}
                onChange={(e) => setCfg({ progress_interval: Number(e.target.value) })}
              />
            </div>
            <div className="grid gap-2">
              <LabelWithTip
                label="Protocol (-M)"
                tip="How queries are sent: simple (one round-trip), extended (parse/bind/execute), or prepared (server-side prepared statements, usually fastest). pgbench's -M flag."
              />
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
            {localAvailable && (
              <div className="grid gap-2">
                <LabelWithTip
                  label="Run via"
                  tip="Where the load is generated. Databricks job runs pgbench on a single-node cluster; Local (dev) runs the pgbench binary on this machine — useful for serverless-only workspaces. Not shown in the deployed app."
                />
                <Select value={runMode} onValueChange={(v) => setRunMode(v as "job" | "local")}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="job">Databricks job</SelectItem>
                    <SelectItem value="local">Local (dev)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
          {isLocal && (
            <p className="text-xs text-muted-foreground">
              Local mode runs the <code>pgbench</code> binary on this dev machine and
              connects straight to Lakebase — no Databricks job or cluster. Intended for
              serverless-only workspaces; not available in the deployed app.
            </p>
          )}
          <div className="flex flex-wrap gap-6 text-sm">
            <div className="flex items-center gap-1.5">
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={config.per_statement_latency}
                  onChange={(e) => setCfg({ per_statement_latency: e.target.checked })}
                />
                Per-statement latency (-r)
              </label>
              <InfoTip text="Report average latency for each statement in the query scripts. pgbench's -r flag." />
            </div>
            <div className="flex items-center gap-1.5">
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={config.detailed_logging}
                  onChange={(e) => setCfg({ detailed_logging: e.target.checked })}
                />
                Detailed logging (-l)
              </label>
              <InfoTip text="Writes a per-transaction log (pgbench's -l flag). Required for the p50/p95/p99 latency percentiles and the per-query breakdown below — leave it on, or those are reported as “—”." />
            </div>
            <div className="flex items-center gap-1.5">
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={config.connect_per_transaction}
                  onChange={(e) => setCfg({ connect_per_transaction: e.target.checked })}
                />
                Reconnect per transaction (-C)
              </label>
              <InfoTip text="Open a new connection for every transaction instead of reusing one — measures connection-setup overhead. pgbench's -C flag." />
            </div>
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
            query's relative transaction weight — pgbench picks scripts in proportion to it over
            the configured duration.
          </p>
          <QueryParamHelp />
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
          <div className="flex gap-2">
            <Button onClick={onSubmit} disabled={pending || running}>
              <Play className="mr-1 h-4 w-4" />
              {pending
                ? isLocal
                  ? "Starting…"
                  : "Submitting…"
                : isLocal
                  ? "Run local pgbench"
                  : "Submit pgbench job"}
            </Button>
            <Button variant="outline" onClick={onExplain} disabled={explain.isPending || verifying}>
              <ScanSearch className="mr-1 h-4 w-4" />
              {explain.isPending ? "Explaining…" : "Explain"}
            </Button>
            <Button variant="outline" onClick={onOptimize} disabled={runOptimize.isPending}>
              <Wand2 className="mr-1 h-4 w-4" />
              {runOptimize.isPending ? "Analyzing…" : "Optimize"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {submitted && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {running && <Loader2 className="h-4 w-4 animate-spin" />}
              {isLocal ? "Local run" : "Job run"}
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

            {status?.status === "failed" && status?.error && (
              <div className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-400">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span className="whitespace-pre-wrap break-words">{status.error}</span>
              </div>
            )}

            <Tabs defaultValue="metrics">
              <TabsList>
                <TabsTrigger value="metrics">Metrics</TabsTrigger>
                <TabsTrigger value="monitoring">Monitoring</TabsTrigger>
              </TabsList>
              <TabsContent value="metrics" className="mt-4">
                {r ? (
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
                    {num(r.cache_hit_pct) != null &&
                      metric(
                        "Cache hit (this run)",
                        `${num(r.cache_hit_pct)!.toFixed(1)}%`,
                        num(r.cache_hit_pct)! >= 99 ? "good" : num(r.cache_hit_pct)! >= 90 ? "warn" : "bad",
                      )}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Metrics appear once the run completes.
                  </p>
                )}
                {r && num(r.latency_p99_ms) != null && num(r.tps) != null && (
                  <ReferenceBands p99={num(r.latency_p99_ms)!} qps={num(r.tps)!} />
                )}
                {r && Array.isArray(r.per_query) && r.per_query.length > 0 && (
                  <div className="mt-6">
                    <h3 className="mb-2 text-sm font-medium">Per-query breakdown</h3>
                    <PerQueryTable rows={r.per_query as QueryStat[]} />
                  </div>
                )}
              </TabsContent>
              <TabsContent value="monitoring" className="mt-4">
                <MonitoringPanel url={submitted.monitoring_url} host={host} />
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      )}

      {baseline && r && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Zap className="h-4 w-4" /> Before / after — indexes applied
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {compareRow("TPS", num(baseline.tps) ?? 0, num(r.tps) ?? 0, false, "tps")}
            {compareRow("Avg latency", num(baseline.latency_avg_ms) ?? 0, num(r.latency_avg_ms) ?? 0, true, "ms")}
            {compareRow("p95 latency", num(baseline.latency_p95_ms) ?? 0, num(r.latency_p95_ms) ?? 0, true, "ms")}
            {compareRow("p99 latency", num(baseline.latency_p99_ms) ?? 0, num(r.latency_p99_ms) ?? 0, true, "ms")}
          </CardContent>
        </Card>
      )}

      {/* Loop order: Explain (diagnose) → Optimize (prescribe) → Cost. */}
      <ExplainPlansCard
        results={explainResults}
        before={explainBefore}
        analyze={explainAnalyze}
        onAnalyzeChange={setExplainAnalyze}
        busy={explain.isPending}
        onVerify={onVerifyPlans}
        canVerify={!!optimize?.index_suggestions.length}
        verifyBusy={verifying}
      />

      {optimize && (
        <OptimizeCard
          optimize={optimize}
          onApplyOne={onApplyOne}
          onApplyAll={onApplyAllAndSnapshot}
          busy={applyIdx.isPending}
          applyAllLabel="Apply all indexes"
          applyAllNote="Applies the indexes and snapshots the current result as the baseline. Re-run pgbench (above) to record the before/after."
        />
      )}

      {r && conn.project && status?.status === "completed" && (
        <RunCostCard
          project={conn.project}
          totalQueries={num(r.total_transactions) ?? 0}
          durationSeconds={config.duration_seconds}
          window={runWindow}
        />
      )}
    </div>
  );
}
