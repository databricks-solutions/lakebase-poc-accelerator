import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { toast } from "sonner";
import { Plus, Trash2, Play, Wand2, Zap, ArrowDown, ArrowUp } from "lucide-react";

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
import {
  useRunPsycopgTest,
  useOptimizeAnalyze,
  useApplyIndexes,
  type TestReportOut,
  type OptimizeOut,
} from "@/lib/api";

export const Route = createFileRoute("/testing")({
  component: TestingPage,
});

interface QueryRow {
  identifier: string;
  content: string;
}

const SAMPLE: QueryRow = {
  identifier: "order_lookup",
  content:
    "-- PARAMETERS: [[1], [437], [12000]]\n-- EXEC_COUNT: 10\nSELECT channel, net_paid FROM tpcds_all_sales WHERE order_number = %s;",
};

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
  const [conn, setConn] = useState<ConnectionConfig>(emptyConnection);
  const [concurrency, setConcurrency] = useState(10);
  const [queries, setQueries] = useState<QueryRow[]>([SAMPLE]);
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
    <div>
      <PageHeader
        title="Concurrency Testing"
        description="Run your query mix against Lakebase at a target concurrency level, then click Optimize for ready-to-run index and tuning suggestions."
      />
      <div className="space-y-6 p-8">
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
                  placeholder="SELECT ... WHERE col = %s;  (use -- PARAMETERS: and -- EXEC_COUNT:)"
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
      </div>
    </div>
  );
}
