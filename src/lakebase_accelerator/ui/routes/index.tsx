import { createFileRoute, Link } from "@tanstack/react-router";
import { Database, Gauge, Wand2, ArrowRight } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export const Route = createFileRoute("/")({
  component: Quickstart,
});

const STEPS = [
  {
    icon: Database,
    title: "1. Size & deploy",
    body: "Get a workload→CU sizing recommendation, then sync tables in bulk — or jump to the native Databricks dialog for a rich single-table sync.",
    to: "/deployment",
    cta: "Open Deployment",
  },
  {
    icon: Gauge,
    title: "2. Test concurrency",
    body: "Paste a non-trivial query mix (point lookups, customer history, aggregations) and run it against your project at a chosen concurrency level.",
    to: "/testing",
    cta: "Open Testing",
  },
  {
    icon: Wand2,
    title: "3. Optimize",
    body: "Click Optimize to get ready-to-run CREATE INDEX statements from your queries, plus live findings — then re-run to see index scans replace sequential scans.",
    to: "/testing",
    cta: "Optimize a run",
  },
] as const;

function Quickstart() {
  return (
    <div>
      <PageHeader
        title="Quickstart"
        description="A guided walkthrough of the accelerator. Each step pre-fills a real page — follow them in order to go from a synced table to an optimized, load-tested Lakebase database."
      />
      <div className="space-y-6 p-8">
        <div className="grid gap-4 md:grid-cols-3">
          {STEPS.map(({ icon: Icon, title, body, to, cta }) => (
            <Card key={title} className="flex flex-col">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Icon className="h-4 w-4 text-primary" />
                  {title}
                </CardTitle>
              </CardHeader>
              <CardContent className="flex flex-1 flex-col justify-between gap-4">
                <p className="text-sm text-muted-foreground">{body}</p>
                <Button asChild variant="outline" size="sm" className="w-fit">
                  <Link to={to}>
                    {cta} <ArrowRight className="ml-1 h-4 w-4" />
                  </Link>
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Sample workload</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <p>
              Source table: <code>samples.tpcds_sf1.store_sales</code> (~2.9M rows) — available in
              every Databricks workspace. Sync it into Lakebase as <code>store_sales</code> on the
              Deployment page, then run the testing flow against it.
            </p>
            <p>
              The Testing page ships 5 ready-to-run OLTP sample queries (simple → complex): order
              lookup by <code>ss_ticket_number</code>, customer history by <code>ss_customer_sk</code>,
              per-item sales aggregate, store daily revenue over a date range, and top items per
              store. Optimize derives indexes on <code>ss_ticket_number</code>,
              <code> ss_customer_sk</code>, <code>ss_item_sk</code>, and <code>ss_store_sk</code>.
            </p>
            <p>
              The same query format drives both psycopg and pgbench — <code>:name</code> placeholders
              with <code>-- PARAM name = random(min, max)</code> generators — so a query you write
              once runs on either engine.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
