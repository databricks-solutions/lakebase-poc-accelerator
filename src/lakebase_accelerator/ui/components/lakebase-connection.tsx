import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useListLakebaseProjects } from "@/lib/api";

export type AuthMethod = "identity" | "oauth";

export interface ConnectionConfig {
  auth_method: AuthMethod;
  project: string;
  database: string;
  endpoint_host: string;
  access_token: string;
  postgres_user_name: string;
}

export const emptyConnection: ConnectionConfig = {
  auth_method: "identity",
  project: "",
  database: "",
  endpoint_host: "",
  access_token: "",
  postgres_user_name: "",
};

interface Props {
  value: ConnectionConfig;
  onChange: (next: ConnectionConfig) => void;
}

/**
 * Shared Lakebase connection fields: auth method + project picker (identity) or
 * pasted OAuth token (dev fallback). Used by Testing, Optimize, and Deployment flows.
 */
export function LakebaseConnection({ value, onChange }: Props) {
  const { data, isLoading } = useListLakebaseProjects();
  const projects = data?.data.projects ?? [];
  const set = (patch: Partial<ConnectionConfig>) => onChange({ ...value, ...patch });

  return (
    <div className="space-y-4">
      <div className="grid gap-2">
        <Label>Authentication</Label>
        <Select
          value={value.auth_method}
          onValueChange={(v) => set({ auth_method: v as AuthMethod })}
        >
          <SelectTrigger className="w-72">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="identity">My Databricks identity</SelectItem>
            <SelectItem value="oauth">OAuth token (dev)</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {value.auth_method === "identity" && (
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="grid gap-2">
            <Label>Lakebase project</Label>
            <Select value={value.project} onValueChange={(v) => set({ project: v })}>
              <SelectTrigger>
                <SelectValue placeholder={isLoading ? "Loading…" : "Select a project"} />
              </SelectTrigger>
              <SelectContent>
                {projects.map((p) => (
                  <SelectItem key={p.name} value={p.name}>
                    {p.name}
                    {p.state ? ` · ${p.state}` : ""}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-2">
            <Label>Database (optional)</Label>
            <Input
              placeholder="databricks_postgres"
              value={value.database}
              onChange={(e) => set({ database: e.target.value })}
            />
          </div>
        </div>
      )}

      {value.auth_method === "oauth" && (
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="grid gap-2">
            <Label>Endpoint host</Label>
            <Input
              placeholder="ep-xxx.database.cloud.databricks.com"
              value={value.endpoint_host}
              onChange={(e) => set({ endpoint_host: e.target.value })}
            />
          </div>
          <div className="grid gap-2">
            <Label>Postgres user</Label>
            <Input
              placeholder="you@company.com"
              value={value.postgres_user_name}
              onChange={(e) => set({ postgres_user_name: e.target.value })}
            />
          </div>
          <div className="grid gap-2 sm:col-span-2">
            <Label>OAuth token</Label>
            <Input
              type="password"
              placeholder="Paste token from Lakebase Connect → Copy OAuth token"
              value={value.access_token}
              onChange={(e) => set({ access_token: e.target.value })}
            />
          </div>
          <div className="grid gap-2">
            <Label>Database (optional)</Label>
            <Input
              placeholder="databricks_postgres"
              value={value.database}
              onChange={(e) => set({ database: e.target.value })}
            />
          </div>
        </div>
      )}

    </div>
  );
}

export default LakebaseConnection;
