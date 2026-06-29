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
              Source tables: <code>samples.tpcds_sf1.*</code> (or the unified
              <code> serverless_stable_dvmvgw_catalog.genie.tpcds_all_sales</code> view, ~4.9M rows).
            </p>
            <p>
              Sample queries: point lookup by <code>order_number</code>, customer history by
              <code> customer_sk</code>, monthly revenue by year, top brands by category. Optimize
              derives indexes on <code>order_number</code>, <code>customer_sk</code>,
              <code> category</code>, and <code>sold_date</code>.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
