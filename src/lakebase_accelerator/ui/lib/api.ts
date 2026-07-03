import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";
export class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;
    constructor(status: number, statusText: string, body: unknown){
        super(`HTTP ${status}: ${statusText}`);
        this.name = "ApiError";
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
export interface ApplyIndexIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    database?: string | null;
    db_schema?: string | null;
    ddls?: string[];
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
}
export interface ApplyIndexesOut {
    error?: string | null;
    results?: ApplyResultOut[];
}
export interface ApplyResultOut {
    ddl: string;
    detail: string;
    ok: boolean;
}
export interface BranchListOut {
    branches: string[];
    error?: string | null;
}
export interface CapabilitiesOut {
    pgbench_local_available?: boolean;
}
export interface ClusterListOut {
    clusters?: ClusterOut[];
    error?: string | null;
}
export interface ClusterOut {
    cluster_id: string;
    cluster_name: string;
    node_type_id?: string | null;
    state: string;
}
export interface ComplexValue {
    display?: string | null;
    primary?: boolean | null;
    ref?: string | null;
    type?: string | null;
    value?: string | null;
}
export interface CostDayOut {
    branch_storage_dsu: number;
    compute_cost: number;
    compute_dbus: number;
    expiring_storage_dsu: number;
    pitr_storage_dsu: number;
    storage_cost: number;
    storage_dsu: number;
    total_cost: number;
    usage_date: string;
}
export interface CostUsageIn {
    days?: number;
    project: string;
    warehouse_id: string;
}
export interface CostUsageOut {
    compute_cost?: number;
    days: number;
    error?: string | null;
    project_uid?: string | null;
    rows?: CostDayOut[];
    storage_cost?: number;
    total_cost?: number;
}
export interface CreateProjectIn {
    display_name: string;
    max_cu: number;
    min_cu: number;
    pg_version?: number;
    project_id: string;
}
export interface CreateProjectOut {
    detail: string;
    name?: string | null;
    ok: boolean;
}
export interface DatabaseListOut {
    databases: string[];
    error?: string | null;
}
export interface EndpointInfoOut {
    endpoint_type?: string | null;
    host?: string | null;
    max_cu?: number | null;
    min_cu?: number | null;
    name: string;
    state?: string | null;
}
export interface ExplainIn {
    access_token?: string | null;
    analyze?: boolean;
    auth_method?: "identity" | "oauth";
    database?: string | null;
    db_schema?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    queries?: OptimizeQueryIn[];
}
export interface ExplainOut {
    error?: string | null;
    results?: ExplainResultOut[];
}
export interface ExplainResultOut {
    error?: string | null;
    identifier: string;
    plan: string;
    seq_scan: boolean;
}
export interface FindingOut {
    actions: string[];
    category: string;
    detail: string;
    severity: string;
    title: string;
}
export interface HTTPValidationError {
    detail?: ValidationError[];
}
export interface HistoryArchiveIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    database?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    runs?: HistoryRunIn[];
    schema_name?: string;
    table_name?: string;
}
export interface HistoryArchiveOut {
    error?: string | null;
    inserted?: number;
    ok: boolean;
}
export interface HistoryConnIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    database?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    schema_name?: string;
    table_name?: string;
}
export interface HistoryEnableOut {
    ddl?: string | null;
    error?: string | null;
    grant_sql?: string | null;
    message: string;
    ok: boolean;
    table?: string | null;
}
export interface HistoryListOut {
    error?: string | null;
    runs?: HistoryRunOut[];
}
export interface HistoryRunIn {
    baseline_report?: Record<string, unknown> | null;
    config?: Record<string, unknown>;
    created_at?: string | null;
    engine?: string;
    id?: string | null;
    index_ddls?: string[];
    label?: string | null;
    optimized_report?: Record<string, unknown> | null;
    project?: string | null;
    queries?: QueryIn[];
}
export interface HistoryRunOut {
    baseline_report?: Record<string, unknown> | null;
    config?: Record<string, unknown>;
    created_at?: string | null;
    created_by?: string | null;
    engine?: string;
    id: string;
    index_ddls?: string[];
    label?: string | null;
    optimized_report?: Record<string, unknown> | null;
    project?: string | null;
    queries?: Record<string, unknown>[];
}
export interface HistoryTablesOut {
    error?: string | null;
    tables?: string[];
}
export interface IndexSuggestionOut {
    columns: string[];
    ddl: string;
    rationale: string;
    table: string;
}
export interface Name {
    family_name?: string | null;
    given_name?: string | null;
}
export interface OpResultOut {
    detail: string;
    ok: boolean;
}
export interface OptimizeIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    database?: string | null;
    db_schema?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    queries?: OptimizeQueryIn[];
    run_live?: boolean;
}
export interface OptimizeOut {
    error?: string | null;
    findings: FindingOut[];
    index_suggestions: IndexSuggestionOut[];
    live_ran: boolean;
    stats: Record<string, unknown>;
}
export interface OptimizeQueryIn {
    content: string;
    identifier: string;
}
export interface PgbenchConfigIn {
    clients?: number;
    connect_per_transaction?: boolean;
    detailed_logging?: boolean;
    duration_seconds?: number;
    jobs?: number;
    per_statement_latency?: boolean;
    progress_interval?: number;
    protocol?: string;
}
export interface PgbenchLocalSubmitOut {
    error?: string | null;
    monitoring_url?: string | null;
    run_id?: string | null;
    status: string;
}
export interface PgbenchStatusOut {
    error?: string | null;
    message: string;
    pgbench_results?: Record<string, unknown> | null;
    progress: number;
    run_id: string;
    status: string;
}
export interface PgbenchSubmitIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    cluster_id?: string | null;
    config?: PgbenchConfigIn;
    database?: string | null;
    db_schema?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    queries?: QueryIn[];
}
export interface PgbenchSubmitOut {
    error?: string | null;
    job_id?: string | null;
    job_name?: string | null;
    job_run_url?: string | null;
    job_url?: string | null;
    monitoring_url?: string | null;
    run_id?: string | null;
    status: string;
}
export interface ProjectInfoOut {
    branch?: string | null;
    endpoints?: EndpointInfoOut[];
    error?: string | null;
    name: string;
}
export interface ProjectListOut {
    error?: string | null;
    projects: ProjectOut[];
}
export interface ProjectOut {
    display_name?: string | null;
    id?: string | null;
    name: string;
    state?: string | null;
}
export interface PsycopgTestIn {
    access_token?: string | null;
    auth_method?: "identity" | "oauth";
    concurrency_level?: number;
    database?: string | null;
    db_schema?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    queries: QueryIn[];
    total_executions?: number;
}
export interface QueryIn {
    content: string;
    identifier: string;
}
export interface QueryStat {
    avg_time_ms: number;
    calls: number;
    p95_time_ms?: number | null;
    p99_time_ms?: number | null;
    query_identifier: string;
    total_time_ms: number;
}
export interface RunCostEstimateOut {
    cost: number;
    cost_per_million_queries?: number | null;
    cu: number;
    discount: number;
    duration_seconds: number;
    price_per_cu_hour: number;
    price_source: string;
    queries_per_dollar?: number | null;
    total_queries: number;
}
export interface RunCostIn {
    cu: number;
    discount?: number;
    duration_seconds: number;
    end?: string | null;
    project: string;
    start?: string | null;
    total_queries: number;
    warehouse_id: string;
}
export interface RunCostOut {
    error?: string | null;
    estimate?: RunCostEstimateOut | null;
    reconcile?: RunCostReconcileOut | null;
}
export interface RunCostReconcileOut {
    allocated_dbu: number;
    available: boolean;
    buckets: number;
    cost: number;
    cost_per_million_queries?: number | null;
    cu_hours: number;
    effective_avg_cu?: number | null;
    note: string;
    queries_per_dollar?: number | null;
}
export interface SchemaListOut {
    error?: string | null;
    schemas: string[];
}
export interface SetCuIn {
    endpoint_name: string;
    max_cu: number;
    min_cu: number;
}
export interface SizingIn {
    bulk_writes_per_second?: number;
    continuous_writes_per_second?: number;
    reads_per_second?: number;
}
export interface SizingOut {
    bulk_cu: number;
    continuous_cu: number;
    rationale: string;
    read_cu: number;
    recommended_max_cu: number;
    recommended_min_cu: number;
    total_cu: number;
}
export interface SyncCheckIn {
    scheduling_policy?: string;
    source_table_full_name: string;
    warehouse_id?: string | null;
}
export interface SyncCheckOut {
    cdf_enabled: boolean;
    enable_cdf_sql?: string | null;
    message: string;
    ok: boolean;
    table_exists: boolean;
    verified: boolean;
}
export interface SyncTableIn {
    branch?: string | null;
    database?: string | null;
    primary_key_columns: string[];
    scheduling_policy?: string;
    source_table_full_name: string;
    storage_catalog?: string | null;
    storage_schema?: string | null;
    target_uc_name: string;
}
export interface TableSizeIn {
    table_full_name: string;
    warehouse_id: string;
}
export interface TableSizeOut {
    message: string;
    ok: boolean;
    size_mb: number;
    uncompressed_bytes: number;
}
export interface TestReportOut {
    average_execution_time_ms: number;
    cache_hit_pct?: number | null;
    concurrency_level: number;
    connection_pool_metrics: Record<string, unknown>;
    error?: string | null;
    failed_queries: number;
    monitoring_url?: string | null;
    p50_execution_time_ms: number;
    p95_execution_time_ms: number;
    p99_execution_time_ms: number;
    per_query?: QueryStat[];
    success_rate: number;
    successful_queries: number;
    throughput_queries_per_second: number;
    total_duration_seconds: number;
    total_queries_executed: number;
}
export interface TokenScopesOut {
    has_obo_token: boolean;
    has_postgres_scope?: boolean;
    note?: string | null;
    scopes?: string[];
}
export interface User {
    active?: boolean | null;
    display_name?: string | null;
    emails?: ComplexValue[] | null;
    entitlements?: ComplexValue[] | null;
    external_id?: string | null;
    groups?: ComplexValue[] | null;
    id?: string | null;
    name?: Name | null;
    roles?: ComplexValue[] | null;
    schemas?: UserSchema[] | null;
    user_name?: string | null;
}
export const UserSchema = {
    "urn:ietf:params:scim:schemas:core:2.0:User": "urn:ietf:params:scim:schemas:core:2.0:User",
    "urn:ietf:params:scim:schemas:extension:workspace:2.0:User": "urn:ietf:params:scim:schemas:extension:workspace:2.0:User"
} as const;
export type UserSchema = typeof UserSchema[keyof typeof UserSchema];
export interface ValidationError {
    ctx?: Record<string, unknown>;
    input?: unknown;
    loc: (string | number)[];
    msg: string;
    type: string;
}
export interface VersionOut {
    version: string;
}
export interface WarehouseListOut {
    error?: string | null;
    warehouses?: WarehouseOut[];
}
export interface WarehouseOut {
    id: string;
    name: string;
    state?: string | null;
}
export interface WorkspaceInfoOut {
    host?: string | null;
}
export const getRunCost = async (data: RunCostIn, options?: RequestInit): Promise<{
    data: RunCostOut;
}> =>{
    const res = await fetch("/api/cost/run", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGetRunCost(options?: {
    mutation?: UseMutationOptions<{
        data: RunCostOut;
    }, ApiError, RunCostIn>;
}) {
    return useMutation({
        mutationFn: (data)=>getRunCost(data),
        ...options?.mutation
    });
}
export const getLakebaseCost = async (data: CostUsageIn, options?: RequestInit): Promise<{
    data: CostUsageOut;
}> =>{
    const res = await fetch("/api/cost/usage", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGetLakebaseCost(options?: {
    mutation?: UseMutationOptions<{
        data: CostUsageOut;
    }, ApiError, CostUsageIn>;
}) {
    return useMutation({
        mutationFn: (data)=>getLakebaseCost(data),
        ...options?.mutation
    });
}
export interface CurrentUserParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const currentUser = async (params?: CurrentUserParams, options?: RequestInit): Promise<{
    data: User;
}> =>{
    const res = await fetch("/api/current-user", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const currentUserKey = (params?: CurrentUserParams)=>{
    return [
        "/api/current-user",
        params
    ] as const;
};
export function useCurrentUser<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export function useCurrentUserSuspense<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export const checkSyncRequirements = async (data: SyncCheckIn, options?: RequestInit): Promise<{
    data: SyncCheckOut;
}> =>{
    const res = await fetch("/api/deployment/check-sync", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useCheckSyncRequirements(options?: {
    mutation?: UseMutationOptions<{
        data: SyncCheckOut;
    }, ApiError, SyncCheckIn>;
}) {
    return useMutation({
        mutationFn: (data)=>checkSyncRequirements(data),
        ...options?.mutation
    });
}
export const createProject = async (data: CreateProjectIn, options?: RequestInit): Promise<{
    data: CreateProjectOut;
}> =>{
    const res = await fetch("/api/deployment/create-project", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useCreateProject(options?: {
    mutation?: UseMutationOptions<{
        data: CreateProjectOut;
    }, ApiError, CreateProjectIn>;
}) {
    return useMutation({
        mutationFn: (data)=>createProject(data),
        ...options?.mutation
    });
}
export interface GetProjectInfoParams {
    project: string;
}
export const getProjectInfo = async (params: GetProjectInfoParams, options?: RequestInit): Promise<{
    data: ProjectInfoOut;
}> =>{
    const searchParams = new URLSearchParams();
    if (params.project != null) searchParams.set("project", String(params.project));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/deployment/project-info?${queryString}` : "/api/deployment/project-info";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getProjectInfoKey = (params?: GetProjectInfoParams)=>{
    return [
        "/api/deployment/project-info",
        params
    ] as const;
};
export function useGetProjectInfo<TData = {
    data: ProjectInfoOut;
}>(options: {
    params: GetProjectInfoParams;
    query?: Omit<UseQueryOptions<{
        data: ProjectInfoOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getProjectInfoKey(options.params),
        queryFn: ()=>getProjectInfo(options.params),
        ...options?.query
    });
}
export function useGetProjectInfoSuspense<TData = {
    data: ProjectInfoOut;
}>(options: {
    params: GetProjectInfoParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ProjectInfoOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getProjectInfoKey(options.params),
        queryFn: ()=>getProjectInfo(options.params),
        ...options?.query
    });
}
export const recommendSize = async (data: SizingIn, options?: RequestInit): Promise<{
    data: SizingOut;
}> =>{
    const res = await fetch("/api/deployment/recommend-size", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useRecommendSize(options?: {
    mutation?: UseMutationOptions<{
        data: SizingOut;
    }, ApiError, SizingIn>;
}) {
    return useMutation({
        mutationFn: (data)=>recommendSize(data),
        ...options?.mutation
    });
}
export const setEndpointCu = async (data: SetCuIn, options?: RequestInit): Promise<{
    data: OpResultOut;
}> =>{
    const res = await fetch("/api/deployment/set-cu", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useSetEndpointCu(options?: {
    mutation?: UseMutationOptions<{
        data: OpResultOut;
    }, ApiError, SetCuIn>;
}) {
    return useMutation({
        mutationFn: (data)=>setEndpointCu(data),
        ...options?.mutation
    });
}
export const createSyncedTable = async (data: SyncTableIn, options?: RequestInit): Promise<{
    data: OpResultOut;
}> =>{
    const res = await fetch("/api/deployment/sync", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useCreateSyncedTable(options?: {
    mutation?: UseMutationOptions<{
        data: OpResultOut;
    }, ApiError, SyncTableIn>;
}) {
    return useMutation({
        mutationFn: (data)=>createSyncedTable(data),
        ...options?.mutation
    });
}
export const getTableSize = async (data: TableSizeIn, options?: RequestInit): Promise<{
    data: TableSizeOut;
}> =>{
    const res = await fetch("/api/deployment/table-size", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGetTableSize(options?: {
    mutation?: UseMutationOptions<{
        data: TableSizeOut;
    }, ApiError, TableSizeIn>;
}) {
    return useMutation({
        mutationFn: (data)=>getTableSize(data),
        ...options?.mutation
    });
}
export const listWarehouses = async (options?: RequestInit): Promise<{
    data: WarehouseListOut;
}> =>{
    const res = await fetch("/api/deployment/warehouses", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listWarehousesKey = ()=>{
    return [
        "/api/deployment/warehouses"
    ] as const;
};
export function useListWarehouses<TData = {
    data: WarehouseListOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: WarehouseListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listWarehousesKey(),
        queryFn: ()=>listWarehouses(),
        ...options?.query
    });
}
export function useListWarehousesSuspense<TData = {
    data: WarehouseListOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: WarehouseListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listWarehousesKey(),
        queryFn: ()=>listWarehouses(),
        ...options?.query
    });
}
export const archiveLakebaseHistory = async (data: HistoryArchiveIn, options?: RequestInit): Promise<{
    data: HistoryArchiveOut;
}> =>{
    const res = await fetch("/api/history/lakebase/archive", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useArchiveLakebaseHistory(options?: {
    mutation?: UseMutationOptions<{
        data: HistoryArchiveOut;
    }, ApiError, HistoryArchiveIn>;
}) {
    return useMutation({
        mutationFn: (data)=>archiveLakebaseHistory(data),
        ...options?.mutation
    });
}
export const enableLakebaseHistory = async (data: HistoryConnIn, options?: RequestInit): Promise<{
    data: HistoryEnableOut;
}> =>{
    const res = await fetch("/api/history/lakebase/enable", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useEnableLakebaseHistory(options?: {
    mutation?: UseMutationOptions<{
        data: HistoryEnableOut;
    }, ApiError, HistoryConnIn>;
}) {
    return useMutation({
        mutationFn: (data)=>enableLakebaseHistory(data),
        ...options?.mutation
    });
}
export const listLakebaseHistory = async (data: HistoryConnIn, options?: RequestInit): Promise<{
    data: HistoryListOut;
}> =>{
    const res = await fetch("/api/history/lakebase/list", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useListLakebaseHistory(options?: {
    mutation?: UseMutationOptions<{
        data: HistoryListOut;
    }, ApiError, HistoryConnIn>;
}) {
    return useMutation({
        mutationFn: (data)=>listLakebaseHistory(data),
        ...options?.mutation
    });
}
export const listLakebaseHistoryTables = async (data: HistoryConnIn, options?: RequestInit): Promise<{
    data: HistoryTablesOut;
}> =>{
    const res = await fetch("/api/history/lakebase/tables", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useListLakebaseHistoryTables(options?: {
    mutation?: UseMutationOptions<{
        data: HistoryTablesOut;
    }, ApiError, HistoryConnIn>;
}) {
    return useMutation({
        mutationFn: (data)=>listLakebaseHistoryTables(data),
        ...options?.mutation
    });
}
export interface ListLakebaseBranchesParams {
    project: string;
}
export const listLakebaseBranches = async (params: ListLakebaseBranchesParams, options?: RequestInit): Promise<{
    data: BranchListOut;
}> =>{
    const searchParams = new URLSearchParams();
    if (params.project != null) searchParams.set("project", String(params.project));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/lakebase/branches?${queryString}` : "/api/lakebase/branches";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listLakebaseBranchesKey = (params?: ListLakebaseBranchesParams)=>{
    return [
        "/api/lakebase/branches",
        params
    ] as const;
};
export function useListLakebaseBranches<TData = {
    data: BranchListOut;
}>(options: {
    params: ListLakebaseBranchesParams;
    query?: Omit<UseQueryOptions<{
        data: BranchListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listLakebaseBranchesKey(options.params),
        queryFn: ()=>listLakebaseBranches(options.params),
        ...options?.query
    });
}
export function useListLakebaseBranchesSuspense<TData = {
    data: BranchListOut;
}>(options: {
    params: ListLakebaseBranchesParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: BranchListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listLakebaseBranchesKey(options.params),
        queryFn: ()=>listLakebaseBranches(options.params),
        ...options?.query
    });
}
export interface ListLakebaseDatabasesParams {
    project: string;
}
export const listLakebaseDatabases = async (params: ListLakebaseDatabasesParams, options?: RequestInit): Promise<{
    data: DatabaseListOut;
}> =>{
    const searchParams = new URLSearchParams();
    if (params.project != null) searchParams.set("project", String(params.project));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/lakebase/databases?${queryString}` : "/api/lakebase/databases";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listLakebaseDatabasesKey = (params?: ListLakebaseDatabasesParams)=>{
    return [
        "/api/lakebase/databases",
        params
    ] as const;
};
export function useListLakebaseDatabases<TData = {
    data: DatabaseListOut;
}>(options: {
    params: ListLakebaseDatabasesParams;
    query?: Omit<UseQueryOptions<{
        data: DatabaseListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listLakebaseDatabasesKey(options.params),
        queryFn: ()=>listLakebaseDatabases(options.params),
        ...options?.query
    });
}
export function useListLakebaseDatabasesSuspense<TData = {
    data: DatabaseListOut;
}>(options: {
    params: ListLakebaseDatabasesParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: DatabaseListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listLakebaseDatabasesKey(options.params),
        queryFn: ()=>listLakebaseDatabases(options.params),
        ...options?.query
    });
}
export const listLakebaseProjects = async (options?: RequestInit): Promise<{
    data: ProjectListOut;
}> =>{
    const res = await fetch("/api/lakebase/projects", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listLakebaseProjectsKey = ()=>{
    return [
        "/api/lakebase/projects"
    ] as const;
};
export function useListLakebaseProjects<TData = {
    data: ProjectListOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: ProjectListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listLakebaseProjectsKey(),
        queryFn: ()=>listLakebaseProjects(),
        ...options?.query
    });
}
export function useListLakebaseProjectsSuspense<TData = {
    data: ProjectListOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: ProjectListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listLakebaseProjectsKey(),
        queryFn: ()=>listLakebaseProjects(),
        ...options?.query
    });
}
export interface ListLakebaseSchemasParams {
    project: string;
    database?: string | null;
}
export const listLakebaseSchemas = async (params: ListLakebaseSchemasParams, options?: RequestInit): Promise<{
    data: SchemaListOut;
}> =>{
    const searchParams = new URLSearchParams();
    if (params.project != null) searchParams.set("project", String(params.project));
    if (params?.database != null) searchParams.set("database", String(params?.database));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/lakebase/schemas?${queryString}` : "/api/lakebase/schemas";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listLakebaseSchemasKey = (params?: ListLakebaseSchemasParams)=>{
    return [
        "/api/lakebase/schemas",
        params
    ] as const;
};
export function useListLakebaseSchemas<TData = {
    data: SchemaListOut;
}>(options: {
    params: ListLakebaseSchemasParams;
    query?: Omit<UseQueryOptions<{
        data: SchemaListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listLakebaseSchemasKey(options.params),
        queryFn: ()=>listLakebaseSchemas(options.params),
        ...options?.query
    });
}
export function useListLakebaseSchemasSuspense<TData = {
    data: SchemaListOut;
}>(options: {
    params: ListLakebaseSchemasParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SchemaListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listLakebaseSchemasKey(options.params),
        queryFn: ()=>listLakebaseSchemas(options.params),
        ...options?.query
    });
}
export const getTokenScopes = async (options?: RequestInit): Promise<{
    data: TokenScopesOut;
}> =>{
    const res = await fetch("/api/lakebase/token-scopes", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getTokenScopesKey = ()=>{
    return [
        "/api/lakebase/token-scopes"
    ] as const;
};
export function useGetTokenScopes<TData = {
    data: TokenScopesOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: TokenScopesOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getTokenScopesKey(),
        queryFn: ()=>getTokenScopes(),
        ...options?.query
    });
}
export function useGetTokenScopesSuspense<TData = {
    data: TokenScopesOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: TokenScopesOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getTokenScopesKey(),
        queryFn: ()=>getTokenScopes(),
        ...options?.query
    });
}
export const optimizeAnalyze = async (data: OptimizeIn, options?: RequestInit): Promise<{
    data: OptimizeOut;
}> =>{
    const res = await fetch("/api/optimize/analyze", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useOptimizeAnalyze(options?: {
    mutation?: UseMutationOptions<{
        data: OptimizeOut;
    }, ApiError, OptimizeIn>;
}) {
    return useMutation({
        mutationFn: (data)=>optimizeAnalyze(data),
        ...options?.mutation
    });
}
export const applyIndexes = async (data: ApplyIndexIn, options?: RequestInit): Promise<{
    data: ApplyIndexesOut;
}> =>{
    const res = await fetch("/api/optimize/apply-indexes", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useApplyIndexes(options?: {
    mutation?: UseMutationOptions<{
        data: ApplyIndexesOut;
    }, ApiError, ApplyIndexIn>;
}) {
    return useMutation({
        mutationFn: (data)=>applyIndexes(data),
        ...options?.mutation
    });
}
export const explainQueries = async (data: ExplainIn, options?: RequestInit): Promise<{
    data: ExplainOut;
}> =>{
    const res = await fetch("/api/optimize/explain", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useExplainQueries(options?: {
    mutation?: UseMutationOptions<{
        data: ExplainOut;
    }, ApiError, ExplainIn>;
}) {
    return useMutation({
        mutationFn: (data)=>explainQueries(data),
        ...options?.mutation
    });
}
export const getTestingCapabilities = async (options?: RequestInit): Promise<{
    data: CapabilitiesOut;
}> =>{
    const res = await fetch("/api/testing/capabilities", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getTestingCapabilitiesKey = ()=>{
    return [
        "/api/testing/capabilities"
    ] as const;
};
export function useGetTestingCapabilities<TData = {
    data: CapabilitiesOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: CapabilitiesOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getTestingCapabilitiesKey(),
        queryFn: ()=>getTestingCapabilities(),
        ...options?.query
    });
}
export function useGetTestingCapabilitiesSuspense<TData = {
    data: CapabilitiesOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: CapabilitiesOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getTestingCapabilitiesKey(),
        queryFn: ()=>getTestingCapabilities(),
        ...options?.query
    });
}
export const listClusters = async (options?: RequestInit): Promise<{
    data: ClusterListOut;
}> =>{
    const res = await fetch("/api/testing/clusters", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listClustersKey = ()=>{
    return [
        "/api/testing/clusters"
    ] as const;
};
export function useListClusters<TData = {
    data: ClusterListOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: ClusterListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listClustersKey(),
        queryFn: ()=>listClusters(),
        ...options?.query
    });
}
export function useListClustersSuspense<TData = {
    data: ClusterListOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: ClusterListOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listClustersKey(),
        queryFn: ()=>listClusters(),
        ...options?.query
    });
}
export interface GetLocalPgbenchStatusParams {
    run_id: string;
}
export const getLocalPgbenchStatus = async (params: GetLocalPgbenchStatusParams, options?: RequestInit): Promise<{
    data: PgbenchStatusOut;
}> =>{
    const res = await fetch(`/api/testing/pgbench/local/status/${params.run_id}`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getLocalPgbenchStatusKey = (params?: GetLocalPgbenchStatusParams)=>{
    return [
        "/api/testing/pgbench/local/status/{run_id}",
        params
    ] as const;
};
export function useGetLocalPgbenchStatus<TData = {
    data: PgbenchStatusOut;
}>(options: {
    params: GetLocalPgbenchStatusParams;
    query?: Omit<UseQueryOptions<{
        data: PgbenchStatusOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getLocalPgbenchStatusKey(options.params),
        queryFn: ()=>getLocalPgbenchStatus(options.params),
        ...options?.query
    });
}
export function useGetLocalPgbenchStatusSuspense<TData = {
    data: PgbenchStatusOut;
}>(options: {
    params: GetLocalPgbenchStatusParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PgbenchStatusOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getLocalPgbenchStatusKey(options.params),
        queryFn: ()=>getLocalPgbenchStatus(options.params),
        ...options?.query
    });
}
export const submitLocalPgbench = async (data: PgbenchSubmitIn, options?: RequestInit): Promise<{
    data: PgbenchLocalSubmitOut;
}> =>{
    const res = await fetch("/api/testing/pgbench/local/submit", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useSubmitLocalPgbench(options?: {
    mutation?: UseMutationOptions<{
        data: PgbenchLocalSubmitOut;
    }, ApiError, PgbenchSubmitIn>;
}) {
    return useMutation({
        mutationFn: (data)=>submitLocalPgbench(data),
        ...options?.mutation
    });
}
export interface GetPgbenchRunStatusParams {
    run_id: string;
}
export const getPgbenchRunStatus = async (params: GetPgbenchRunStatusParams, options?: RequestInit): Promise<{
    data: PgbenchStatusOut;
}> =>{
    const res = await fetch(`/api/testing/pgbench/status/${params.run_id}`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getPgbenchRunStatusKey = (params?: GetPgbenchRunStatusParams)=>{
    return [
        "/api/testing/pgbench/status/{run_id}",
        params
    ] as const;
};
export function useGetPgbenchRunStatus<TData = {
    data: PgbenchStatusOut;
}>(options: {
    params: GetPgbenchRunStatusParams;
    query?: Omit<UseQueryOptions<{
        data: PgbenchStatusOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getPgbenchRunStatusKey(options.params),
        queryFn: ()=>getPgbenchRunStatus(options.params),
        ...options?.query
    });
}
export function useGetPgbenchRunStatusSuspense<TData = {
    data: PgbenchStatusOut;
}>(options: {
    params: GetPgbenchRunStatusParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PgbenchStatusOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getPgbenchRunStatusKey(options.params),
        queryFn: ()=>getPgbenchRunStatus(options.params),
        ...options?.query
    });
}
export const submitPgbenchJob = async (data: PgbenchSubmitIn, options?: RequestInit): Promise<{
    data: PgbenchSubmitOut;
}> =>{
    const res = await fetch("/api/testing/pgbench/submit", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useSubmitPgbenchJob(options?: {
    mutation?: UseMutationOptions<{
        data: PgbenchSubmitOut;
    }, ApiError, PgbenchSubmitIn>;
}) {
    return useMutation({
        mutationFn: (data)=>submitPgbenchJob(data),
        ...options?.mutation
    });
}
export const runPsycopgTest = async (data: PsycopgTestIn, options?: RequestInit): Promise<{
    data: TestReportOut;
}> =>{
    const res = await fetch("/api/testing/psycopg/run", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useRunPsycopgTest(options?: {
    mutation?: UseMutationOptions<{
        data: TestReportOut;
    }, ApiError, PsycopgTestIn>;
}) {
    return useMutation({
        mutationFn: (data)=>runPsycopgTest(data),
        ...options?.mutation
    });
}
export const version = async (options?: RequestInit): Promise<{
    data: VersionOut;
}> =>{
    const res = await fetch("/api/version", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const versionKey = ()=>{
    return [
        "/api/version"
    ] as const;
};
export function useVersion<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
export function useVersionSuspense<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
export const getWorkspaceInfo = async (options?: RequestInit): Promise<{
    data: WorkspaceInfoOut;
}> =>{
    const res = await fetch("/api/workspace-info", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getWorkspaceInfoKey = ()=>{
    return [
        "/api/workspace-info"
    ] as const;
};
export function useGetWorkspaceInfo<TData = {
    data: WorkspaceInfoOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: WorkspaceInfoOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getWorkspaceInfoKey(),
        queryFn: ()=>getWorkspaceInfo(),
        ...options?.query
    });
}
export function useGetWorkspaceInfoSuspense<TData = {
    data: WorkspaceInfoOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: WorkspaceInfoOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getWorkspaceInfoKey(),
        queryFn: ()=>getWorkspaceInfo(),
        ...options?.query
    });
}
