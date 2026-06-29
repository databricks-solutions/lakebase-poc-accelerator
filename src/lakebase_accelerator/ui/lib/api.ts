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
    auth_method?: "identity" | "app_resource" | "oauth";
    database?: string | null;
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
export interface ComplexValue {
    display?: string | null;
    primary?: boolean | null;
    ref?: string | null;
    type?: string | null;
    value?: string | null;
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
    auth_method?: "identity" | "app_resource" | "oauth";
    database?: string | null;
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
    auth_method?: "identity" | "app_resource" | "oauth";
    concurrency_level?: number;
    database?: string | null;
    endpoint_host?: string | null;
    postgres_user_name?: string | null;
    project?: string | null;
    queries: QueryIn[];
}
export interface QueryIn {
    content: string;
    identifier: string;
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
export interface TestReportOut {
    average_execution_time_ms: number;
    concurrency_level: number;
    connection_pool_metrics: Record<string, unknown>;
    error?: string | null;
    failed_queries: number;
    p50_execution_time_ms: number;
    p95_execution_time_ms: number;
    p99_execution_time_ms: number;
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
