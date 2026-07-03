import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { DollarSign, Search, AlertTriangle, Info } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  useListLakebaseProjects,
  useListWarehouses,
  useGetLakebaseCost,
  type CostUsageOut,
} from "@/lib/api";

export const Route = createFileRoute("/cost")({
  component: CostPage,
});

const WINDOWS = [
  { value: "7", label: "Last 7 days" },
  { value: "14", label: "Last 14 days" },
  { value: "30", label: "Last 30 days" },
  { value: "60", label: "Last 60 days" },
  { value: "90", label: "Last 90 days" },
];

const usd = (n: number | undefined) =>
  `$${(n ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

function CostPage() {
  const { data: projData, isLoading: projLoading } = useListLakebaseProjects();
  const projects = projData?.data.projects ?? [];
  const projError = projData?.data.error;

  const { data: whData, isLoading: whLoading } = useListWarehouses();
  const warehouses = whData?.data.warehouses ?? [];

  const [project, setProject] = useState("");
  const [warehouseId, setWarehouseId] = useState("");
  const [days, setDays] = useState("30");
  const [report, setReport] = useState<CostUsageOut | null>(null);

  const getCost = useGetLakebaseCost();

  const onFetch = async () => {
    setReport(null);
    try {
      const res = await getCost.mutateAsync({
        project,
        warehouse_id: warehouseId,
        days: Number(days),
      });
      setReport(res.data);
    } catch (e) {
      setReport({ days: Number(days), error: String(e) });
    }
  };

  const rows = report?.rows ?? [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Cost"
        description="Actual Lakebase spend from system.billing.usage — daily compute and storage cost for a project."
      />

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-4 w-4" /> Select a project
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-end gap-3">
            <div className="grid gap-2">
              <Label>Project</Label>
              <Select value={project} onValueChange={setProject}>
                <SelectTrigger className="w-80">
                  <SelectValue placeholder={projLoading ? "Loading…" : "Select a project"} />
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

            <div className="grid gap-2">
              <Label>SQL warehouse</Label>
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
              <Label>Window</Label>
              <Select value={days} onValueChange={setDays}>
                <SelectTrigger className="w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {WINDOWS.map((w) => (
                    <SelectItem key={w.value} value={w.value}>
                      {w.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <Button onClick={onFetch} disabled={getCost.isPending || !project || !warehouseId}>
              {getCost.isPending ? "Fetching…" : "Fetch cost"}
            </Button>
          </div>

          {projError && (
            <p className="text-xs text-muted-foreground">
              Could not list projects ({projError}). You can still fetch cost once billing is accessible.
            </p>
          )}

          <div className="flex items-start gap-2 rounded-md border bg-muted/30 p-3 text-xs text-muted-foreground">
            <Info className="mt-0.5 h-4 w-4 shrink-0" />
            <span>
              Costs are computed from <span className="font-medium text-foreground">list prices</span> in{" "}
              <code>system.billing.list_prices</code> and do not reflect account discounts or the Lakebase
              compute promotion — treat them as an upper bound. Reading billing requires access to the{" "}
              <code>system.billing</code> schema.
            </span>
          </div>
        </CardContent>
      </Card>

      {report?.error && (
        <div className="flex items-start gap-2 rounded-md border p-3 text-sm text-red-600 dark:text-red-400">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <span className="whitespace-pre-wrap break-words">{report.error}</span>
        </div>
      )}

      {report && !report.error && (
        <>
          <div className="grid gap-4 sm:grid-cols-3">
            <SummaryCard label="Compute" value={usd(report.compute_cost)} sub="DBUs (serverless compute)" />
            <SummaryCard label="Storage" value={usd(report.storage_cost)} sub="DSUs (branch + PITR + snapshots)" />
            <SummaryCard
              label={`Total · ${report.days}d`}
              value={usd(report.total_cost)}
              sub="Compute + storage"
              emphasize
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <DollarSign className="h-4 w-4" /> Daily breakdown
              </CardTitle>
            </CardHeader>
            <CardContent>
              {rows.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No Lakebase usage found for this project in the selected window.
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-xs text-muted-foreground">
                        <th className="py-2 pr-4 font-medium">Date</th>
                        <th className="py-2 pr-4 text-right font-medium">Compute DBUs</th>
                        <th className="py-2 pr-4 text-right font-medium">Compute $</th>
                        <th className="py-2 pr-4 text-right font-medium">Branch DSU</th>
                        <th className="py-2 pr-4 text-right font-medium">PITR DSU</th>
                        <th className="py-2 pr-4 text-right font-medium">Storage $</th>
                        <th className="py-2 pr-4 text-right font-medium">Total $</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={r.usage_date} className="border-b last:border-0">
                          <td className="py-2 pr-4">{r.usage_date}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">{r.compute_dbus.toLocaleString()}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">{usd(r.compute_cost)}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">{r.branch_storage_dsu.toLocaleString()}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">{r.pitr_storage_dsu.toLocaleString()}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">{usd(r.storage_cost)}</td>
                          <td className="py-2 pr-4 text-right font-medium tabular-nums">{usd(r.total_cost)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}

function SummaryCard({
  label,
  value,
  sub,
  emphasize,
}: {
  label: string;
  value: string;
  sub: string;
  emphasize?: boolean;
}) {
  return (
    <Card className={emphasize ? "border-primary/50" : undefined}>
      <CardContent className="pt-6">
        <p className="text-xs text-muted-foreground">{label}</p>
        <p className="mt-1 text-2xl font-semibold tabular-nums">{value}</p>
        <p className="mt-1 text-xs text-muted-foreground">{sub}</p>
      </CardContent>
    </Card>
  );
}
