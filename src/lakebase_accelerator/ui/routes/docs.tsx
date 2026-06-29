import { createFileRoute } from "@tanstack/react-router";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export const Route = createFileRoute("/docs")({
  component: DocsPage,
});

function DocsPage() {
  return (
    <div>
      <PageHeader
        title="Docs"
        description="How to use the Lakebase POC Accelerator, end to end."
      />
      <div className="mx-auto max-w-3xl space-y-6 p-8">
        <Card>
          <CardHeader>
            <CardTitle>Why this app (vs. the native Databricks UI)</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>
              Databricks already has an excellent UI to <strong className="text-foreground">create and sync</strong>{" "}
              Lakebase tables. This app does not try to replace it — for rich single-table creation it links
              you straight to the native dialog.
            </p>
            <p>
              What this app adds is everything <em>around</em> deployment that the platform doesn't:{" "}
              <strong className="text-foreground">workload→CU sizing</strong>,{" "}
              <strong className="text-foreground">concurrency / load testing</strong> (psycopg &amp; pgbench),
              and <strong className="text-foreground">Optimize</strong> — query-derived indexes plus live
              findings (sequential scans, cache-hit ratio, unused indexes). In short: the native UI deploys
              Lakebase; this app proves it's fast and tells you how to make it faster.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>The flow: Deploy → Test → Optimize</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>
              <strong className="text-foreground">1. Deployment.</strong> Size an autoscaling
              endpoint from your expected reads/writes, inspect an existing project, and sync
              Delta tables into Lakebase (snapshot, triggered, or continuous).
            </p>
            <p>
              <strong className="text-foreground">2. Concurrency Testing.</strong> Point the app
              at a Lakebase project, paste your query mix, pick a concurrency level, and run.
              You get throughput, success rate, and latency percentiles.
            </p>
            <p>
              <strong className="text-foreground">3. Optimize.</strong> Click Optimize after a run
              to get ready-to-run <code>CREATE INDEX</code> statements derived from your queries,
              plus live findings (cache-hit ratio, sequential scans, unused indexes).
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Authentication</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>
              <strong className="text-foreground">My Databricks identity</strong> — when deployed
              as a Databricks App, the app uses your logged-in identity (OBO) to reach Lakebase.
              This is the recommended path.
            </p>
            <p>
              <strong className="text-foreground">Attached app resource</strong> — uses the
              Lakebase database resource bound to the app's service principal. Zero config, no
              token to paste.
            </p>
            <p>
              <strong className="text-foreground">OAuth token (dev)</strong> — paste an endpoint
              host, your Postgres user, and a token from Lakebase Connect. Intended for local
              development only; tokens expire after ~1 hour.
            </p>
            <p className="text-xs">
              Static username &amp; password is intentionally not supported — it is the least
              secure option for Lakebase.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Query file format</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>Each query uses <code>%s</code> placeholders and two optional directives:</p>
            <pre className="overflow-x-auto rounded-md border bg-muted/30 p-3 text-xs">
{`-- PARAMETERS: [[1], [437], ["Electronics"]]
-- EXEC_COUNT: 20
SELECT channel, net_paid FROM tpcds_all_sales WHERE order_number = %s;`}
            </pre>
            <p>
              <code>-- PARAMETERS:</code> is a JSON list of value-sets (one scenario each);
              <code> -- EXEC_COUNT:</code> is how many times to run each scenario (default 5).
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
