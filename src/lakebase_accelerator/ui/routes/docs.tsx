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
            <CardTitle>Unified query format (psycopg &amp; pgbench)</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>
              Both testing engines share <strong className="text-foreground">one query format</strong>,
              so a query you author once runs on either. A query is SQL with pgbench-style
              <code> :name</code> placeholders plus optional comment directives:
            </p>
            <pre className="overflow-x-auto rounded-md border bg-muted/30 p-3 text-xs">
{`-- WEIGHT: 40
-- EXEC_COUNT: 20
-- PARAM ticket = random(1, 240000)
SELECT ss_item_sk, ss_net_paid
FROM store_sales
WHERE ss_ticket_number = :ticket;`}
            </pre>
            <ul className="list-disc space-y-1 pl-5">
              <li>
                <code>-- PARAM name = random(min, max)</code> — a named integer generator. The app
                draws a fresh value per execution (psycopg) or emits <code>\set name random(min, max)</code> (pgbench).
              </li>
              <li>
                <code>-- WEIGHT:</code> — pgbench's relative transaction weight for this query
                (default 1). Ignored by psycopg.
              </li>
              <li>
                <code>-- EXEC_COUNT:</code> — how many times psycopg runs this query (default 5).
                Ignored by pgbench, which runs for the configured duration.
              </li>
            </ul>
            <p>
              Use a synced <code>store_sales</code> table (from <code>samples.tpcds_sf1.store_sales</code>)
              and the 5 bundled sample queries to get started.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Run history &amp; permissions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <p>
              The psycopg tab can save each test run — including the{" "}
              <strong className="text-foreground">before/after (baseline → optimized)</strong> pair —
              to one of two destinations, chosen at runtime:
            </p>
            <ul className="list-disc space-y-1 pl-5">
              <li>
                <strong className="text-foreground">This browser</strong> (default) — stored in
                <code> localStorage</code>. No setup, no database writes; private to your browser,
                survives refreshes and app redeploys, but not shared across machines or users.
              </li>
              <li>
                <strong className="text-foreground">Lakebase table</strong> — shared, durable history
                written into the connected project. Opt-in and consent-gated.
              </li>
            </ul>
            <p>
              <strong className="text-foreground">This app is standalone</strong> — it is not attached
              to any Lakebase project, so it can test many projects. For the Lakebase destination it
              connects as its <strong className="text-foreground">service principal</strong> and is
              held to <strong className="text-foreground">least privilege</strong>: the SP is confined
              to a single dedicated schema it <em>owns</em> (<code>accelerator_history</code>) and has
              no access to your other tables — Postgres denies anything not explicitly granted, and the
              app only ever touches <code>‹schema›._accelerator_run_history</code>.
            </p>
            <p>There are two permission layers a project owner provisions once, per project:</p>
            <pre className="overflow-x-auto rounded-md border bg-muted/30 p-3 text-xs">
{`-- Layer 1 — let the SP connect (it has no role in a project it didn't create).
-- Lakebase identities authenticate with Databricks OAuth tokens, so the role must
-- be created via databricks_auth — a plain CREATE ROLE rejects the token.
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('<app-service-principal>', 'SERVICE_PRINCIPAL');
GRANT CONNECT ON DATABASE databricks_postgres TO "<app-service-principal>";

-- Layer 2 — a dedicated schema it OWNS (its entire sandbox):
CREATE SCHEMA IF NOT EXISTS accelerator_history
  AUTHORIZATION "<app-service-principal>";`}
            </pre>
            <p>
              The Enable step previews the exact statements with the real SP role name filled in. Runs
              are attributed to the actual user via <code>created_by</code>. No setup is needed for the
              browser destination.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
