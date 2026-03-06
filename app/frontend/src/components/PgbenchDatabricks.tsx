import React, { useState } from 'react';
import {
  Card,
  Form,
  Input,
  InputNumber,
  Button,
  Select,
  Switch,
  Space,
  Typography,
  Progress,
  message,
  Row,
  Col,
  Collapse,
  Tag,
  Radio,
  Upload,
  Alert,
  Statistic,
  Divider,
  Tooltip
} from 'antd';
import { PlayCircleOutlined, ClusterOutlined, DatabaseOutlined, SettingOutlined, UploadOutlined, FolderOutlined, InfoCircleOutlined } from '@ant-design/icons';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;
const { Panel } = Collapse;

interface PgbenchConfig {
  pgbench_clients: number;
  pgbench_jobs: number;
  pgbench_duration: number;
  pgbench_progress_interval: number;
  pgbench_protocol: string;
  pgbench_per_statement_latency: boolean;
  pgbench_detailed_logging: boolean;
  pgbench_connect_per_transaction: boolean;
}

interface QueryConfig {
  name: string;
  content: string;
  weight: number;
}

type AuthMethod = 'password' | 'oauth';

interface JobSubmissionRequest {
  auth_method?: AuthMethod;
  pghost?: string;
  pgport?: number;
  pgdatabase?: string;
  pguser?: string;
  pgpassword?: string;
  pgsslmode?: string;
  access_token?: string;
  endpoint_host?: string;
  postgres_user_name?: string;
  cluster_id?: string;
  workspace_url: string;
  databricks_profile?: string;
  pgbench_config: PgbenchConfig;
  query_configs?: QueryConfig[];
  query_workspace_path?: string;
}


interface JobStatus {
  job_id?: string;
  run_id?: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  message: string;
  progress?: number;
  results?: any;
  job_run_url?: string;
  job_url?: string;
  workspace_url?: string;
}

const PgbenchDatabricks: React.FC = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [authMethod, setAuthMethod] = useState<AuthMethod>('password');
  const [querySource, setQuerySource] = useState<'predefined' | 'upload' | 'workspace'>('predefined');
  const [uploadedQueries, setUploadedQueries] = useState<QueryConfig[]>([]);
  const [uploadSummary, setUploadSummary] = useState<string>('');

  // Helper function to ensure URL is absolute
  const ensureAbsoluteUrl = (url: string): string => {
    if (!url) return url;

    // If URL already starts with http:// or https://, it's absolute
    if (url.startsWith('http://') || url.startsWith('https://')) {
      return url;
    }

    // If URL starts with //, add https:
    if (url.startsWith('//')) {
      return `https:${url}`;
    }

    // If URL doesn't start with protocol, add https://
    if (!url.includes('://')) {
      return `https://${url}`;
    }

    return url;
  };
  const [queryConfigs, setQueryConfigs] = useState<QueryConfig[]>([
    {
      name: 'point',
      content: '\\set c_customer_sk random(0, 999)\nSELECT *\nFROM public.customer\nWHERE c_customer_sk = :c_customer_sk;',
      weight: 60
    },
    {
      name: 'range',
      content: '\\set c_start random(1, 11)\n\\set c_end :c_start + 10\nSELECT count(*)\nFROM public.customer\nWHERE c_current_hdemo_sk BETWEEN :c_start AND :c_end;',
      weight: 30
    },
    {
      name: 'agg',
      content: 'SELECT c_preferred_cust_flag, count(*)\nFROM public.customer\nGROUP BY c_preferred_cust_flag;',
      weight: 10
    }
  ]);



  const handleSubmitJob = async (values: any) => {
    const currentAuth = (values.auth_method || authMethod) as AuthMethod;
    if (currentAuth === 'oauth') {
      if (!(values.access_token || '').trim()) {
        message.error('Enter the Postgres OAuth token from Lakebase Connect.');
        return;
      }
      if (!(values.endpoint_host || '').trim()) {
        message.error('Enter the endpoint host from the Lakebase Connect dialog.');
        return;
      }
      if (!(values.postgres_user_name || '').trim()) {
        message.error('Enter the Postgres user (Databricks email or username) for OAuth.');
        return;
      }
    } else {
      if (!(values.pghost || '').trim() || !(values.pguser || '').trim() || !(values.pgpassword || '').trim()) {
        message.error('Enter PostgreSQL host, user, and password.');
        return;
      }
    }

    setLoading(true);
    setJobStatus({ status: 'pending', message: 'Submitting job...' });

    try {
      const pgbenchConfig = {
        pgbench_clients: values.pgbench_clients,
        pgbench_jobs: values.pgbench_jobs,
        pgbench_duration: values.pgbench_duration,
        pgbench_progress_interval: values.pgbench_progress_interval,
        pgbench_protocol: values.pgbench_protocol,
        pgbench_per_statement_latency: values.pgbench_per_statement_latency,
        pgbench_detailed_logging: values.pgbench_detailed_logging,
        pgbench_connect_per_transaction: values.pgbench_connect_per_transaction,
      };
      const jobRequest: JobSubmissionRequest = {
        workspace_url: values.workspace_url,
        databricks_profile: values.databricks_profile || 'DEFAULT',
        pgbench_config: pgbenchConfig,
        cluster_id: values.cluster_id,
        pgdatabase: values.pgdatabase || 'databricks_postgres',
      };
      if (currentAuth === 'oauth') {
        jobRequest.auth_method = 'oauth';
        jobRequest.access_token = (values.access_token || '').trim();
        jobRequest.endpoint_host = (values.endpoint_host || '').trim();
        jobRequest.postgres_user_name = (values.postgres_user_name || '').trim();
      } else {
        jobRequest.auth_method = 'password';
        jobRequest.pghost = values.pghost?.trim();
        jobRequest.pgport = values.pgport || 5432;
        jobRequest.pguser = values.pguser?.trim();
        jobRequest.pgpassword = values.pgpassword;
        jobRequest.pgsslmode = values.pgsslmode || 'require';
      }

      // Add query source based on selection
      if (querySource === 'workspace') {
        // User provided workspace path
        if (!values.query_workspace_path) {
          message.error('Please provide a workspace path to queries');
          setLoading(false);
          return;
        }
        jobRequest.query_workspace_path = values.query_workspace_path;
      } else if (querySource === 'upload') {
        // User uploaded queries
        if (uploadedQueries.length === 0) {
          message.error('Please upload at least one query file');
          setLoading(false);
          return;
        }
        jobRequest.query_configs = uploadedQueries;
      } else {
        // Predefined queries
        jobRequest.query_configs = queryConfigs;
      }

      const response = await fetch('/api/databricks/submit-pgbench-job', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(jobRequest),
      });

      if (response.ok) {
        const result = await response.json();

        // Debug: Log the URLs received from backend
        console.log('FRONTEND: Job submission successful - Debug info:', {
          job_url: result.job_url,
          job_run_url: result.job_run_url,
          workspace_url: result.workspace_url,
          submitted_workspace_url: jobRequest.workspace_url,
          submitted_databricks_profile: jobRequest.databricks_profile
        });

        setJobStatus({
          job_id: result.job_id,
          run_id: result.run_id,
          status: 'running',
          message: 'Job submitted successfully. Running pgbench test...',
          progress: 0,
          job_run_url: result.job_run_url,
          job_url: result.job_url,
          workspace_url: result.workspace_url
        });

        // Start polling for job status (use same workspace + profile as submit)
        pollJobStatus(result.run_id, jobRequest.workspace_url, jobRequest.databricks_profile);
        message.success('Job submitted successfully!');
      } else {
        const error = await response.json();

        // Console log for debugging job submission failures
        console.log('FRONTEND: Job submission failed - Debug info:', {
          error: error.detail || 'Unknown error',
          jobRequest: jobRequest,
          responseStatus: response.status,
          responseStatusText: response.statusText,
          workspaceUrl: jobRequest.workspace_url,
          databricksProfile: jobRequest.databricks_profile
        });

        setJobStatus({
          status: 'failed',
          message: `Job submission failed: ${error.detail || 'Unknown error'}`
        });
        message.error('Failed to submit job');
      }
    } catch (error) {
      console.error('Error submitting job:', error);
      setJobStatus({
        status: 'failed',
        message: `Error submitting job: ${error}`
      });
      message.error('Error submitting job');
    } finally {
      setLoading(false);
    }
  };


  // Handle query file uploads
  const handleQueryUpload = (file: File) => {
    const reader = new FileReader();

    reader.onload = (e) => {
      const content = e.target?.result as string;
      const fileName = file.name.replace('.sql', '');

      // Parse query content - look for weight in comments
      let weight = 1;
      const weightMatch = content.match(/--\s*weight:\s*(\d+)/i);
      if (weightMatch) {
        weight = parseInt(weightMatch[1], 10);
      }

      // Add to uploaded queries
      const newQuery: QueryConfig = {
        name: fileName,
        content: content,
        weight: weight
      };

      setUploadedQueries(prev => [...prev, newQuery]);

      // Update summary
      const totalQueries = uploadedQueries.length + 1;
      const totalSize = (JSON.stringify([...uploadedQueries, newQuery]).length / 1024).toFixed(2);
      const queriesWithParams = [...uploadedQueries, newQuery].filter(q =>
        q.content.includes('\\set') || q.content.includes(':')
      ).length;

      setUploadSummary(
        `${totalQueries} queries uploaded (${queriesWithParams} with parameters), ` +
        `total size: ${totalSize} KB`
      );

      message.success(`Uploaded ${fileName}`);
    };

    reader.readAsText(file);
    return false; // Prevent auto upload
  };

  // Clear uploaded queries
  const handleClearUploads = () => {
    setUploadedQueries([]);
    setUploadSummary('');
    message.info('Cleared uploaded queries');
  };

  const pollJobStatus = async (
    runId: string,
    workspaceUrl?: string,
    databricksProfile?: string
  ) => {
    const params = new URLSearchParams();
    if (workspaceUrl) params.set('workspace_url', workspaceUrl);
    if (databricksProfile) params.set('databricks_profile', databricksProfile);
    const query = params.toString();
    const url = `/api/databricks/job-status/${runId}${query ? `?${query}` : ''}`;
    const pollInterval = setInterval(async () => {
      try {
        const response = await fetch(url);
        if (response.ok) {
          const status = await response.json();

          setJobStatus(prevStatus => ({
            ...prevStatus,
            ...status,
            run_id: runId
          }));

          // Stop polling if job is completed or failed
          if (status.status === 'completed' || status.status === 'failed') {
            clearInterval(pollInterval);
          }
        }
      } catch (error) {
        console.error('Error polling job status:', error);
        clearInterval(pollInterval);
      }
    }, 5000); // Poll every 5 seconds
  };

  const updateQueryConfig = (index: number, field: keyof QueryConfig, value: any) => {
    const updated = [...queryConfigs];
    updated[index] = { ...updated[index], [field]: value };
    setQueryConfigs(updated);
  };

  const addQueryConfig = () => {
    setQueryConfigs([
      ...queryConfigs,
      { name: `query_${queryConfigs.length + 1}`, content: '', weight: 1 }
    ]);
  };

  const removeQueryConfig = (index: number) => {
    if (queryConfigs.length > 1) {
      setQueryConfigs(queryConfigs.filter((_, i) => i !== index));
    }
  };

  const renderJobStatus = () => {
    if (!jobStatus) return null;

    const getStatusColor = (status: string) => {
      switch (status) {
        case 'pending': return 'blue';
        case 'running': return 'orange';
        case 'completed': return 'green';
        case 'failed': return 'red';
        default: return 'default';
      }
    };

    return (
      <Card title="Job Status" style={{ marginTop: 24 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <div>
            <Tag color={getStatusColor(jobStatus.status)}>
              {jobStatus.status.toUpperCase()}
            </Tag>
            <Text>{jobStatus.message}</Text>
          </div>

          {jobStatus.job_id && (
            <div>
              <Text type="secondary">Job ID: </Text>
              {jobStatus.job_url ? (
                <a href={ensureAbsoluteUrl(jobStatus.job_url)} target="_blank" rel="noopener noreferrer">
                  {jobStatus.job_id}
                </a>
              ) : (
                <Text type="secondary">{jobStatus.job_id}</Text>
              )}
            </div>
          )}

          {jobStatus.run_id && (
            <div>
              <Text type="secondary">Run ID: </Text>
              {jobStatus.job_run_url ? (
                <a href={ensureAbsoluteUrl(jobStatus.job_run_url)} target="_blank" rel="noopener noreferrer">
                  {jobStatus.run_id}
                </a>
              ) : (
                <Text type="secondary">{jobStatus.run_id}</Text>
              )}
            </div>
          )}

          {jobStatus.status === 'running' && jobStatus.progress !== undefined && (
            <Progress percent={jobStatus.progress} />
          )}

          {jobStatus.results && (
            <Card size="small" title="Test Results">
              <Row gutter={16}>
                <Col span={8}>
                  <Text strong>TPS: </Text>
                  <Text>{jobStatus.results.performance_metrics?.tps || 'N/A'}</Text>
                </Col>
                <Col span={8}>
                  <Text strong>P50 Latency: </Text>
                  <Text>{jobStatus.results.performance_metrics?.latency_p50_ms || 'N/A'}ms</Text>
                </Col>
                <Col span={8}>
                  <Text strong>P95 Latency: </Text>
                  <Text>{jobStatus.results.performance_metrics?.latency_p95_ms || 'N/A'}ms</Text>
                </Col>
              </Row>
              <Row gutter={16} style={{ marginTop: 8 }}>
                <Col span={8}>
                  <Text strong>Total Transactions: </Text>
                  <Text>{jobStatus.results.performance_metrics?.total_transactions || 'N/A'}</Text>
                </Col>
                <Col span={8}>
                  <Text strong>Duration: </Text>
                  <Text>{jobStatus.results.test_parameters?.duration_seconds || 'N/A'}s</Text>
                </Col>
                <Col span={8}>
                  <Text strong>Clients: </Text>
                  <Text>{jobStatus.results.test_parameters?.clients || 'N/A'}</Text>
                </Col>
              </Row>
            </Card>
          )}

          {(jobStatus as any).pgbench_results && (
            <Card size="small" title="Pgbench Summary Stats" style={{ marginTop: 16 }}>
              <Row gutter={[16, 16]}>
                <Col span={6}>
                  <Statistic
                    title="TPS"
                    value={(jobStatus as any).pgbench_results.tps}
                    precision={2}
                    valueStyle={{ color: '#3f8600', fontSize: '24px', fontWeight: 'bold' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Avg Latency"
                    value={(jobStatus as any).pgbench_results.latency_avg_ms}
                    suffix="ms"
                    precision={2}
                    valueStyle={{ color: '#1890ff', fontSize: '24px', fontWeight: 'bold' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Total Transactions"
                    value={(jobStatus as any).pgbench_results.total_transactions}
                    valueStyle={{ color: '#722ed1', fontSize: '24px', fontWeight: 'bold' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Duration"
                    value={(jobStatus as any).pgbench_results.duration}
                    suffix="s"
                    valueStyle={{ fontSize: '24px', fontWeight: 'bold' }}
                  />
                </Col>
              </Row>

              {/* Add p50/p95/p99 from performance_metrics if available */}
              {jobStatus.results?.performance_metrics && (
                <>
                  <Divider style={{ margin: '16px 0' }}>Latency Percentiles</Divider>
                  <Row gutter={[16, 16]}>
                    <Col span={8}>
                      <Statistic
                        title="p50 (Median)"
                        value={jobStatus.results.performance_metrics.latency_p50_ms}
                        suffix="ms"
                        precision={2}
                        valueStyle={{ fontSize: '20px' }}
                      />
                    </Col>
                    <Col span={8}>
                      <Statistic
                        title="p95"
                        value={jobStatus.results.performance_metrics.latency_p95_ms}
                        suffix="ms"
                        precision={2}
                        valueStyle={{ fontSize: '20px' }}
                      />
                    </Col>
                    <Col span={8}>
                      <Statistic
                        title="p99"
                        value={jobStatus.results.performance_metrics.latency_p99_ms}
                        suffix="ms"
                        precision={2}
                        valueStyle={{ fontSize: '20px' }}
                      />
                    </Col>
                  </Row>
                </>
              )}

              <Divider style={{ margin: '16px 0' }} />

              <Row gutter={[16, 12]}>
                <Col span={6}>
                  <Text type="secondary">Test Type:</Text>
                  <br />
                  <Text strong>{(jobStatus as any).pgbench_results.transaction_type || 'N/A'}</Text>
                </Col>
                <Col span={6}>
                  <Text type="secondary">Clients:</Text>
                  <br />
                  <Text strong style={{ fontSize: '16px' }}>{(jobStatus as any).pgbench_results.num_clients || 'N/A'}</Text>
                </Col>
                <Col span={6}>
                  <Text type="secondary">Latency Stddev:</Text>
                  <br />
                  <Text strong style={{ fontSize: '16px' }}>{(jobStatus as any).pgbench_results.latency_stddev_ms?.toFixed(2) || 'N/A'} ms</Text>
                </Col>
                <Col span={6}>
                  <Text type="secondary">Failed Transactions:</Text>
                  <br />
                  <Text strong style={{
                    color: (jobStatus as any).pgbench_results.failed_transactions > 0 ? '#cf1322' : '#52c41a',
                    fontSize: '16px'
                  }}>
                    {(jobStatus as any).pgbench_results.failed_transactions || 0}
                  </Text>
                </Col>
              </Row>

              {(jobStatus as any).pgbench_results.success_rate !== undefined && (
                <div style={{ marginTop: 16 }}>
                  <Text type="secondary">Success Rate: </Text>
                  <Tag color="success" style={{ fontSize: '14px', padding: '4px 12px' }}>
                    {(jobStatus as any).pgbench_results.success_rate}%
                  </Tag>
                </div>
              )}

              {/* Per-Query Statistics */}
              {(jobStatus as any).pgbench_results?.per_query_stats && (jobStatus as any).pgbench_results.per_query_stats.length > 0 && (
                <>
                  <Divider style={{ margin: '16px 0' }}>Per-Query Statistics</Divider>
                  {(jobStatus as any).pgbench_results.per_query_stats.map((queryStat: any, index: number) => (
                    <Card
                      key={index}
                      size="small"
                      style={{ marginBottom: 12, backgroundColor: '#fafafa' }}
                      title={
                        <Space>
                          <Text strong>SQL Script: {queryStat.query_name}.sql</Text>
                          <Tag color="blue">{queryStat.transactions} transactions</Tag>
                        </Space>
                      }
                    >
                      <Row gutter={[16, 8]}>
                        <Col span={6}>
                          <Statistic
                            title="TPS"
                            value={queryStat.tps}
                            precision={2}
                            valueStyle={{ fontSize: '16px' }}
                          />
                        </Col>
                        <Col span={6}>
                          <Statistic
                            title="Avg Latency"
                            value={queryStat.latency_avg_ms}
                            suffix="ms"
                            precision={2}
                            valueStyle={{ fontSize: '16px' }}
                          />
                        </Col>
                        <Col span={6}>
                          <Statistic
                            title="Latency Stddev"
                            value={queryStat.latency_stddev_ms}
                            suffix="ms"
                            precision={2}
                            valueStyle={{ fontSize: '16px' }}
                          />
                        </Col>
                        <Col span={6}>
                          <div style={{ textAlign: 'center' }}>
                            <Text type="secondary" style={{ fontSize: '12px' }}>Weight</Text>
                            <div style={{ fontSize: '16px', fontWeight: 500 }}>{queryStat.weight}</div>
                          </div>
                        </Col>
                      </Row>
                      {queryStat.query_path && (
                        <div style={{ marginTop: 8 }}>
                          <Text type="secondary" style={{ fontSize: '12px' }}>
                            Path: {queryStat.query_path}
                          </Text>
                        </div>
                      )}
                    </Card>
                  ))}
                </>
              )}
            </Card>
          )}
        </Space>
      </Card>
    );
  };

  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <Title level={2}>
        <ClusterOutlined /> Pgbench in Databricks
      </Title>
      <Paragraph>
        <strong>pgbench</strong> is PostgreSQL's built-in benchmarking tool that provides industry-standard database performance testing.
        It simulates realistic database workloads by running multiple concurrent client sessions against your Lakebase Postgres database.
      </Paragraph>
      <Paragraph>
        <ul>
          <li>Run pgbench performance tests against your Lakebase database using Databricks compute clusters.</li>
          <li>Works with both <strong>Provisioned</strong> and <strong>Autoscaling</strong> Lakebase. Use <strong>username &amp; password</strong> or <strong>OAuth</strong> (Lakebase Connect token + endpoint host + Postgres user).</li>
          <li>This will create and submit a Databricks job that executes the pgbench test with your specified parameters.</li>
          <li>Choose this option to test higher concurrency by scaling up the number of driver CPU cores in your Databricks cluster.</li>
        </ul>
      </Paragraph>
      <Alert
        message="Requirements"
        description={
          <>
            To submit Databricks jobs, you need <strong>Databricks authentication</strong>:
            <ul style={{ marginTop: 8, marginBottom: 0 }}>
              <li>If running as <strong>Databricks App</strong>: Authentication is automatic (uses DATABRICKS_HOST env var)</li>
              <li>If running <strong>locally</strong>: Requires Databricks CLI configured with a profile</li>
            </ul>
          </>
        }
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      <Form
        form={form}
        layout="vertical"
        onFinish={handleSubmitJob}
        initialValues={{
          auth_method: 'password',
          pgport: 5432,
          pgdatabase: 'databricks_postgres',
          pgsslmode: 'require',
          pgbench_clients: 8,
          pgbench_jobs: 8,
          pgbench_duration: 30,
          pgbench_progress_interval: 5,
          pgbench_protocol: 'prepared',
          pgbench_per_statement_latency: true,
          pgbench_detailed_logging: true,
          pgbench_connect_per_transaction: false,
        }}
      >
        <Card title={<><DatabaseOutlined /> Connection Configuration</>} style={{ marginBottom: 24 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                name="workspace_url"
                label="Databricks Workspace URL"
                rules={[{ required: true, message: 'Please enter your Databricks workspace URL' }]}
                tooltip="Required for submitting jobs and generating clickable links to job runs."
              >
                <Input
                  className="prefixedinput"
                  placeholder="https://adb-xxxxx.region.azuredatabricks.net"
                  prefix={<ClusterOutlined />}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                name="databricks_profile"
                label="Databricks Profile Name"
                tooltip="Optional. Databricks CLI profile for authentication. Only needed if running locally (not as Databricks App)."
              >
                <Input
                  className="prefixedinput"
                  placeholder="DEFAULT"
                  prefix={<SettingOutlined />}
                />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item name="auth_method" label="Authentication">
            <Radio.Group
              optionType="button"
              onChange={(e) => setAuthMethod(e.target.value)}
              options={[
                { label: 'Username & password', value: 'password' },
                { label: 'OAuth (Databricks)', value: 'oauth' }
              ]}
            />
          </Form.Item>

          {authMethod === 'oauth' ? (
            <>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    label={
                      <span>
                        Postgres OAuth token
                        <Tooltip title="From Lakebase: project → Connect → OAuth → Copy OAuth token. Use as password for the connection.">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    name="access_token"
                    rules={[{ required: true, message: 'Required' }]}
                  >
                    <Input.Password placeholder="Paste token from Lakebase Connect" visibilityToggle />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    label={
                      <span>
                        Endpoint host
                        <Tooltip title="From the same Lakebase Connect dialog (e.g. ep-xxx.databricks.com).">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    name="endpoint_host"
                    rules={[{ required: true, message: 'Required' }]}
                  >
                    <Input placeholder="ep-xxx.databricks.com" />
                  </Form.Item>
                </Col>
              </Row>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item label="Database name" name="pgdatabase">
                    <Input placeholder="databricks_postgres" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    label={
                      <span>
                        Postgres user (Databricks identity)
                        <Tooltip title="Your Databricks email or username — the identity that has the Postgres role in the Lakebase project. Required for OAuth.">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    name="postgres_user_name"
                    rules={[{ required: true, message: 'Required for OAuth' }]}
                  >
                    <Input placeholder="e.g. your-email@company.com" />
                  </Form.Item>
                </Col>
              </Row>
            </>
          ) : (
            <>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    name="pghost"
                    label="PostgreSQL Host (Endpoint)"
                    rules={[{ required: true, message: 'Please enter PostgreSQL host' }]}
                    tooltip="Lakebase endpoint hostname (e.g., ep-*.databricks.com for autoscaling or instance DNS for provisioned)"
                  >
                    <Input placeholder="ep-your-endpoint.databricks.com" />
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item name="pgport" label="PostgreSQL Port">
                    <InputNumber min={1} max={65535} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item
                    name="pgdatabase"
                    label="Database Name"
                    rules={[{ required: true, message: 'Please enter database name' }]}
                  >
                    <Input placeholder="databricks_postgres" />
                  </Form.Item>
                </Col>
              </Row>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    name="pguser"
                    label="PostgreSQL User"
                    rules={[{ required: true, message: 'Please enter PostgreSQL user' }]}
                  >
                    <Input placeholder="analyst" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="pgpassword"
                    label="PostgreSQL Password"
                    rules={[{ required: true, message: 'Please enter PostgreSQL password' }]}
                  >
                    <Input.Password placeholder="Enter password" visibilityToggle />
                  </Form.Item>
                </Col>
              </Row>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item name="pgsslmode" label="SSL Mode">
                    <Select>
                      <Option value="require">Require</Option>
                      <Option value="prefer">Prefer</Option>
                      <Option value="allow">Allow</Option>
                      <Option value="disable">Disable</Option>
                    </Select>
                  </Form.Item>
                </Col>
              </Row>
            </>
          )}

          <Form.Item
            name="cluster_id"
            label="Databricks Cluster ID (Optional)"
            rules={[{ required: false }]}
          >
            <Input
              className="prefixedinput"
              placeholder="Enter Databricks cluster ID or leave empty"
              prefix={<ClusterOutlined />}
            />
          </Form.Item>

          <Collapse
            items={[
              {
                key: 'cluster-config-notes',
                label: 'ℹ️ Cluster Configuration',
                children: (
                  <div>
                    <div style={{ marginBottom: 12 }}>
                      <strong>Job Clusters (recommended):</strong>
                      <ul style={{ marginTop: 4, marginBottom: 0, paddingLeft: 20 }}>
                        <li>
                          Leave empty to create an ephemeral job cluster automatically with auto-installed pgbench on the cluster
                        </li>
                        <li>Recommended for high concurrency testing, cluster size determined by # of clients & jobs selection</li>
                        <li>
                          App service principal needs to be given permissions to <strong>CREATE CLUSTER</strong> in Databricks Workspace. Workspace Admin can go to <strong>Workspace settings &gt; Identity and access &gt; Service principals &gt; app service principal name &gt; Configurations / Entitlements &gt; Allow unrestricted cluster creation</strong>
                        </li>
                      </ul>
                    </div>
                    <div>
                      <strong>Interactive Clusters (used with local mode or for repeatable testing):</strong>
                      <ul style={{ marginTop: 4, marginBottom: 0, paddingLeft: 20 }}>
                        <li>
                          Require users to manually configure as a Single node cluster with <strong>Dedicated</strong> (formerly: Single user) access mode.
                        </li>
                        <li>Recommended for repeatable testing without waiting for job cluster spinup, or used with local mode. </li>
                        <li>
                          If running on Databricks Apps, add the app service principal as the dedicated user to the interactive cluster. If running locally, make sure your Databricks user is added as the dedicated user to the interactive cluster.
                        </li>
                        <li>
                          Make sure to attach the{' '}
                          <a
                            href="https://github.com/databricks-solutions/lakebase-poc-accelerator/blob/main/app/notebooks/init.sh"
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ color: '#1890ff' }}
                          >
                            init script
                          </a>
                          {' '}to the interactive cluster
                        </li>
                      </ul>
                    </div>
                  </div>
                ),
              },
            ]}
            style={{ marginBottom: 16 }}
          />
        </Card>

        <Card title={<><SettingOutlined /> pgbench Configuration</>} style={{ marginBottom: 24 }}>
          <Row gutter={16}>
            <Col span={6}>
              <Form.Item
                name="pgbench_clients"
                label="Clients"
                tooltip="Number of concurrent database sessions (connections to database)"
              >
                <InputNumber min={1} max={1000} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_jobs"
                label="Jobs"
                tooltip="Number of worker threads (should match CPU cores on job cluster, typically 4-16)"
              >
                <InputNumber min={1} max={64} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_duration"
                label="Duration (seconds)"
                tooltip="Test duration in seconds"
              >
                <InputNumber min={1} max={3600} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_progress_interval"
                label="Progress Interval"
                tooltip="Progress report interval in seconds"
              >
                <InputNumber min={1} max={60} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item
                name="pgbench_protocol"
                label="Protocol"
                tooltip="Query protocol mode"
              >
                <Select>
                  <Option value="prepared">Prepared</Option>
                  <Option value="simple">Simple</Option>
                  <Option value="extended">Extended</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                name="pgbench_per_statement_latency"
                label="Per-Statement Latency"
                valuePropName="checked"
              >
                <Switch />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                name="pgbench_detailed_logging"
                label="Detailed Logging"
                valuePropName="checked"
              >
                <Switch />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item
            name="pgbench_connect_per_transaction"
            label="Connect Per Transaction"
            valuePropName="checked"
            tooltip="Establish new connection for each transaction"
          >
            <Switch />
          </Form.Item>
        </Card>

        <Card title="Query Configuration" style={{ marginBottom: 24 }}>
          <Form.Item label="Query Source">
            <Radio.Group value={querySource} onChange={(e) => setQuerySource(e.target.value)}>
              <Radio.Button value="predefined">
                <SettingOutlined /> Predefined
              </Radio.Button>
              <Radio.Button value="upload">
                <UploadOutlined /> Upload Files
              </Radio.Button>
              <Radio.Button value="workspace">
                <FolderOutlined /> Workspace Path
              </Radio.Button>
            </Radio.Group>
          </Form.Item>

          {querySource === 'predefined' && (
            <>
              <Alert
                message="Using predefined queries"
                description="Edit the queries below or add new ones for your workload"
                type="info"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 16 }}
              />
              <Collapse>
                {queryConfigs.map((query, index) => (
                  <Panel
                    key={index}
                    header={
                      <Space>
                        <Text strong>{query.name}</Text>
                        <Tag>Weight: {query.weight}</Tag>
                      </Space>
                    }
                    extra={
                      queryConfigs.length > 1 && (
                        <Button
                          type="link"
                          danger
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            removeQueryConfig(index);
                          }}
                        >
                          Remove
                        </Button>
                      )
                    }
                  >
                    <Row gutter={16}>
                      <Col span={8}>
                        <Input
                          placeholder="Query name"
                          value={query.name}
                          onChange={(e) => updateQueryConfig(index, 'name', e.target.value)}
                        />
                      </Col>
                      <Col span={4}>
                        <InputNumber
                          placeholder="Weight"
                          min={1}
                          max={100}
                          value={query.weight}
                          onChange={(value) => updateQueryConfig(index, 'weight', value || 1)}
                          style={{ width: '100%' }}
                        />
                      </Col>
                    </Row>
                    <Input.TextArea
                      placeholder="SQL query content (use pgbench format with \set for variables)"
                      value={query.content}
                      onChange={(e) => updateQueryConfig(index, 'content', e.target.value)}
                      rows={6}
                      style={{ marginTop: 8 }}
                      className="prefixedinput"
                    />
                  </Panel>
                ))}
              </Collapse>

              <Button
                type="dashed"
                onClick={addQueryConfig}
                style={{ width: '100%', marginTop: 16 }}
              >
                Add Query
              </Button>
            </>
          )}

          {querySource === 'upload' && (
            <>
              <Alert
                message="Upload SQL query files"
                description={
                  <>
                    Upload one or more <code>.sql</code> files. Add <code>-- weight: N</code> comment to specify query weight (default: 1).
                    {uploadSummary && <div style={{ marginTop: 8 }}><strong>{uploadSummary}</strong></div>}
                  </>
                }
                type="info"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 16 }}
              />

              <Upload.Dragger
                multiple
                accept=".sql"
                beforeUpload={handleQueryUpload}
                showUploadList={false}
                style={{ marginBottom: 16 }}
              >
                <p className="ant-upload-drag-icon">
                  <UploadOutlined style={{ fontSize: 48, color: '#1890ff' }} />
                </p>
                <p className="ant-upload-text">Click or drag SQL files to upload</p>
                <p className="ant-upload-hint">
                  Upload .sql files with your pgbench queries
                </p>
              </Upload.Dragger>

              {uploadedQueries.length > 0 && (
                <>
                  <Collapse style={{ marginBottom: 16 }}>
                    {uploadedQueries.map((query, index) => (
                      <Panel
                        key={index}
                        header={
                          <Space>
                            <Text strong>{query.name}</Text>
                            <Tag>Weight: {query.weight}</Tag>
                          </Space>
                        }
                      >
                        <Input.TextArea
                          value={query.content}
                          rows={6}
                          readOnly
                          style={{ fontFamily: 'monospace' }}
                        />
                      </Panel>
                    ))}
                  </Collapse>

                  <Button danger onClick={handleClearUploads} style={{ width: '100%' }}>
                    Clear All Uploads
                  </Button>
                </>
              )}
            </>
          )}

          {querySource === 'workspace' && (
            <>
              <Alert
                message="Use queries from Databricks workspace"
                description="Provide the workspace path to a folder containing your .sql query files. The app service principal must have read access to this location."
                type="warning"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 16 }}
              />

              <Form.Item
                name="query_workspace_path"
                label="Workspace Path"
                rules={[
                  { required: querySource === 'workspace', message: 'Please provide workspace path' },
                  { pattern: /^\//, message: 'Path must start with /' }
                ]}
                tooltip="Path to folder containing .sql files (e.g., /Shared/my_queries/ or /Users/user@company.com/queries/)"
              >
                <Input
                  className="prefixedinput"
                  prefix={<FolderOutlined />}
                  placeholder="/Shared/my_queries/"
                  addonBefore="📁"
                />
              </Form.Item>

              <Alert
                message="Permission Requirements"
                description={
                  <>
                    <p><strong>Option 1 (Recommended):</strong> Store queries in <code>/Shared/</code> folder (accessible to all)</p>
                    <p><strong>Option 2:</strong> Grant the app service principal read access to your personal folder</p>
                  </>
                }
                type="info"
                showIcon
                style={{ marginTop: 16 }}
              />
            </>
          )}
        </Card>

        <Form.Item>
          <Button
            type="primary"
            htmlType="submit"
            loading={loading}
            size="large"
            icon={<PlayCircleOutlined />}
            disabled={jobStatus?.status === 'running'}
          >
            {loading ? 'Submitting Job...' : 'Submit pgbench Job'}
          </Button>
        </Form.Item>
      </Form>

      {renderJobStatus()}

    </div>
  );
};

export default PgbenchDatabricks;
