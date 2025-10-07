export interface TableToSync {
  // Stable identifier used for React keys to avoid duplicate-key warnings
  id?: string;
  name: string;
  primary_keys: string[];
  scheduling_policy: 'SNAPSHOT' | 'TRIGGERED' | 'CONTINUOUS';
}

export interface DatabaseInstanceConfig {
  bulk_writes_per_second: number;
  continuous_writes_per_second: number;
  reads_per_second: number;
  number_of_readable_secondaries: number;
  readable_secondary_size_cu: number;
  promotion_percentage: number;
}

export interface DatabaseStorageConfig {
  data_stored_gb: number;
  estimated_data_deleted_daily_gb: number;
  restore_windows_days: number;
}

export interface DeltaSynchronizationConfig {
  number_of_continuous_pipelines: number;
  expected_data_per_sync_gb: number;
  sync_mode: string;
  sync_frequency: string;
  tables_to_sync: TableToSync[];
}

export interface WorkloadConfig {
  database_instance: DatabaseInstanceConfig;
  database_storage: DatabaseStorageConfig;
  delta_synchronization: DeltaSynchronizationConfig;
  databricks_workspace_url: string;
  warehouse_http_path: string;
  // Optional: support profile-based auth to Databricks SDK (localhost only)
  databricks_profile_name?: string;
  lakebase_instance_name: string;
  uc_catalog_name: string;
  database_name: string;
  storage_catalog: string;
  storage_schema: string;
}

export interface CostBreakdown {
  bulk_cu: number;
  continuous_cu: number;
  read_cu: number;
  total_cu: number;
  recommended_cu: number;
  main_instance_cost: number;
  readable_secondaries_cost: number;
  total_compute_cost: number;
  storage_cost: number;
  continuous_sync_cost: number;
  triggered_sync_cost: number;
  total_sync_cost: number;
  estimated_sync_time_hours: number;
  total_monthly_cost: number;
}

export interface TableSizeInfo {
  table_name: string;
  uncompressed_size_mb: number;
  row_count: number;
}

export interface CostEstimationResult {
  config: WorkloadConfig;
  cost_breakdown: CostBreakdown;
  table_sizes?: {
    total_uncompressed_size_mb: number;
    table_details: TableSizeInfo[];
  };
  recommendations: string[];
  timestamp: string;
}