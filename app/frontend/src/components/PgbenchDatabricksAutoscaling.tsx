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

type AuthMethod = 'secrets' | 'oauth';

interface JobSubmissionRequest {
  auth_method?: AuthMethod;
  pghost?: string;
  pgport?: number;
  pgdatabase?: string;
  secret_scope?: string;
  secret_key_user?: string;
  secret_key_password?: string;
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

const PgbenchDatabricksAutoscaling: React.FC = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [authMethod, setAuthMethod] = useState<AuthMethod>('secrets');
  const [querySource, setQuerySource] = useState<'predefined' | 'upload' | 'workspace'>('predefined');
  const [uploadedQueries, setUploadedQueries] = useState<QueryConfig[]>([]);
  const [uploadSummary, setUploadSummary] = useState<string>('');

  // Helper function to ensure URL is absolute
  const ensureAbsoluteUrl = (url: string): string => {
    if (!url) return url;
    if (url.startsWith('http://') || url.startsWith('https://')) return url;
    if (url.startsWith('//')) return `https:${url}`;
    if (!url.includes('://')) return `https://${url}`;
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
      content: '\\set c_start random(1, 900)\n\\set c_end :c_start + 100\nSELECT count(*)\nFROM public.customer\nWHERE c_customer_sk BETWEEN :c_start AND :c_end;',
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
      if (!(values.pghost || '').trim()) {
        message.error('Enter the PostgreSQL host endpoint.');
        return;
      }
      if (!(values.secret_scope || '').trim() || !(values.secret_key_user || '').trim() || !(values.secret_key_password || '').trim()) {
        message.error('Enter the Databricks secret scope and both secret keys (user and password).');
        return;
      }
    }

    setLoading(true);
    setJobStatus({ status: 'pending', message: 'Submitting autoscaling pgbench job...' });

    try {
      const jobRequest: JobSubmissionRequest = {
        workspace_url: values.workspace_url,
        databricks_profile: values.databricks_profile || 'DEFAULT',
        cluster_id: values.cluster_id,
        pgdatabase: values.pgdatabase || 'databricks_postgres',
        pgbench_config: {
          pgbench_clients: values.pgbench_clients,
          pgbench_jobs: values.pgbench_jobs,
          pgbench_duration: values.pgbench_duration,
          pgbench_progress_interval: values.pgbench_progress_interval,
          pgbench_protocol: values.pgbench_protocol,
          pgbench_per_statement_latency: values.pgbench_per_statement_latency,
          pgbench_detailed_logging: values.pgbench_detailed_logging,
          pgbench_connect_per_transaction: values.pgbench_connect_per_transaction,
        }
      };

      if (currentAuth === 'oauth') {
        jobRequest.auth_method = 'oauth';
        jobRequest.access_token = (values.access_token || '').trim();
        jobRequest.endpoint_host = (values.endpoint_host || '').trim();
        jobRequest.postgres_user_name = (values.postgres_user_name || '').trim();
      } else {
        jobRequest.auth_method = 'secrets';
        jobRequest.pghost = values.pghost?.trim();
        jobRequest.pgport = values.pgport || 5432;
        jobRequest.secret_scope = values.secret_scope?.trim();
        jobRequest.secret_key_user = values.secret_key_user?.trim();
        jobRequest.secret_key_password = values.secret_key_password?.trim();
        jobRequest.pgsslmode = values.pgsslmode || 'require';
      }

      // Add query source based on selection
      if (querySource === 'workspace') {
        if (!values.query_workspace_path) {
          message.error('Please provide a workspace path to queries');
          setLoading(false);
          return;
        }
        jobRequest.query_workspace_path = values.query_workspace_path;
      } else if (querySource === 'upload') {
        if (uploadedQueries.length === 0) {
          message.error('Please upload at least one query file');
          setLoading(false);
          return;
        }
        jobRequest.query_configs = uploadedQueries;
      } else {
        jobRequest.query_configs = queryConfigs;
      }

      const response = await fetch('/api/pgbench-test/autoscaling/submit-job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(jobRequest),
      });

      if (response.ok) {
        const result = await response.json();

        setJobStatus({
          job_id: result.job_id,
          run_id: result.run_id,
          status: 'running',
          message: 'Autoscaling pgbench job submitted successfully!',
          progress: 0,
          job_run_url: result.job_run_url,
          job_url: result.job_url,
          workspace_url: result.workspace_url
        });

        pollJobStatus(result.run_id, jobRequest.workspace_url, jobRequest.databricks_profile);
        message.success('Autoscaling pgbench job submitted successfully!');
      } else {
        const error = await response.json();
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

  const handleQueryUpload = (file: File) => {
    const reader = new FileReader();

    reader.onload = (e) => {
      const content = e.target?.result as string;
      const fileName = file.name.replace('.sql', '');

      let weight = 1;
      const weightMatch = content.match(/--\s*weight:\s*(\d+)/i);
      if (weightMatch) {
        weight = parseInt(weightMatch[1], 10);
      }

      const newQuery: QueryConfig = {
        name: fileName,
        content: content,
        weight: weight
      };

      setUploadedQueries(prev => [...prev, newQuery]);

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
    return false;
  };

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

          if (status.status === 'completed' || status.status === 'failed') {
            clearInterval(pollInterval);
          }
        }
      } catch (error) {
        console.error('Error polling job status:', error);
        clearInterval(pollInterval);
      }
    }, 5000);
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

          {/* Pgbench Results Display */}
          {jobStatus.status === 'completed' && (jobStatus as any).pgbench_results && (
            <>
              <Divider style={{ margin: '16px 0' }} />
              <Title level={4} style={{ marginBottom: 16 }}>pgbench Results</Title>

              <Row gutter={[16, 16]}>
                <Col span={6}>
                  <Statistic
                    title="TPS (Trans/sec)"
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
                            <div style={{ fontSize: '18px', fontWeight: 'bold' }}>{queryStat.weight}%</div>
                          </div>
                        </Col>
                      </Row>
                    </Card>
                  ))}
                </>
              )}
            </>
          )}
        </Space>
      </Card>
    );
  };

  return (
    <div style={{ padding: '24px', maxWidth: '1400px', margin: '0 auto' }}>
      <div style={{ marginBottom: '24px' }}>
        <Title level={2} style={{ marginBottom: '8px' }}>
          <DatabaseOutlined style={{ marginRight: '8px' }} />
          Autoscaling Concurrency Testing (pgbench)
        </Title>
        <Paragraph style={{ marginBottom: 0 }}>
          <ul>
            <li>Run pgbench performance tests against your <strong>Autoscaling Lakebase</strong> compute endpoint.</li>
            <li>Uses <strong>Databricks Secrets</strong> (secret scope + keys) or <strong>OAuth</strong> — no plain-text passwords.</li>
            <li>Executes tests on Databricks cluster for comprehensive performance metrics.</li>
          </ul>
        </Paragraph>
      </div>

      <Form
        form={form}
        layout="vertical"
        onFinish={handleSubmitJob}
        initialValues={{
          auth_method: 'secrets',
          pgport: 5432,
          pgdatabase: 'databricks_postgres',
          pgsslmode: 'require',
          databricks_profile: 'DEFAULT',
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
        {/* Autoscaling Connection Configuration */}
        <Card title={<><DatabaseOutlined /> Autoscaling Connection Configuration</>} style={{ marginBottom: 24 }}>
          <Alert
            message="Autoscaling Endpoint Connection"
            description="Connect to your autoscaling Lakebase endpoint using Databricks Secrets (recommended) or OAuth."
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />

          <Form.Item name="auth_method" label="Authentication">
            <Radio.Group
              optionType="button"
              onChange={(e) => setAuthMethod(e.target.value)}
              options={[
                { label: 'Databricks Secrets', value: 'secrets' },
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
                        <Tooltip title="From Lakebase: project → Connect → OAuth → Copy OAuth token.">
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
                        <Tooltip title="Your Databricks email or username — the identity that has the Postgres role in the Lakebase project.">
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
                    label={
                      <span>
                        PostgreSQL Host (Endpoint)
                        <Tooltip title="Autoscaling compute endpoint hostname (format: ep-*.databricks.com)">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    rules={[{ required: true, message: 'Please enter PostgreSQL host' }]}
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
                    name="secret_scope"
                    label={
                      <span>
                        Secret Scope
                        <Tooltip title="The Databricks secret scope that contains the PostgreSQL credentials.">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    rules={[{ required: true, message: 'Please enter the secret scope' }]}
                  >
                    <Input placeholder="my-lakebase-scope" />
                  </Form.Item>
                </Col>
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
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    name="secret_key_user"
                    label={
                      <span>
                        Secret Key — User
                        <Tooltip title="Key inside the secret scope whose value is the PostgreSQL username.">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    rules={[{ required: true, message: 'Please enter the secret key for the username' }]}
                  >
                    <Input placeholder="pg-username" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="secret_key_password"
                    label={
                      <span>
                        Secret Key — Password
                        <Tooltip title="Key inside the secret scope whose value is the PostgreSQL password.">
                          <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                        </Tooltip>
                      </span>
                    }
                    rules={[{ required: true, message: 'Please enter the secret key for the password' }]}
                  >
                    <Input placeholder="pg-password" />
                  </Form.Item>
                </Col>
              </Row>
            </>
          )}
        </Card>

        {/* Cluster Configuration (same as provisioned) */}
        <Card title={<><ClusterOutlined /> Cluster Configuration</>} style={{ marginBottom: 24 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                name="workspace_url"
                label="Databricks Workspace URL"
                rules={[{ required: true, message: 'Please enter your Databricks workspace URL' }]}
                tooltip="Your Databricks workspace URL for running the pgbench job."
              >
                <Input placeholder="https://your-workspace.cloud.databricks.com" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                name="databricks_profile"
                label="Databricks Profile Name"
                tooltip="Databricks CLI profile for authentication."
              >
                <Input placeholder="DEFAULT" />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={24}>
              <Form.Item
                name="cluster_id"
                label="Databricks Cluster ID (Optional)"
                tooltip="If provided, uses this existing cluster. Otherwise creates a job cluster."
              >
                <Input placeholder="e.g., 1234-567890-abc123" />
              </Form.Item>
            </Col>
          </Row>
        </Card>

        {/* pgbench Configuration (same as provisioned) */}
        <Card title={<><SettingOutlined /> pgbench Configuration</>} style={{ marginBottom: 24 }}>
          <Row gutter={16}>
            <Col span={6}>
              <Form.Item
                name="pgbench_clients"
                label="Number of Clients"
                rules={[{ required: true, type: 'number', min: 1, max: 1000 }]}
                tooltip="Number of concurrent pgbench client connections"
              >
                <InputNumber min={1} max={1000} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_jobs"
                label="Number of Threads"
                rules={[{ required: true, type: 'number', min: 1, max: 100 }]}
                tooltip="Number of worker threads for pgbench"
              >
                <InputNumber min={1} max={100} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_duration"
                label="Duration (seconds)"
                rules={[{ required: true, type: 'number', min: 1 }]}
                tooltip="How long to run the test"
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="pgbench_progress_interval"
                label="Progress Interval (s)"
                tooltip="Report progress every N seconds"
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={6}>
              <Form.Item name="pgbench_protocol" label="Protocol">
                <Select>
                  <Option value="prepared">Prepared</Option>
                  <Option value="simple">Simple</Option>
                  <Option value="extended">Extended</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item name="pgbench_per_statement_latency" label="Per-Statement Latency" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item name="pgbench_detailed_logging" label="Detailed Logging" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item name="pgbench_connect_per_transaction" label="Connect Per Tx" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
          </Row>
        </Card>

        {/* Query Configuration (same as provisioned) */}
        <Card title={<><FolderOutlined /> Query Configuration</>} style={{ marginBottom: 24 }}>
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
                description="Edit the pgbench queries below or add new ones for your workload"
                type="info"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 16 }}
              />
              <Collapse>
                {queryConfigs.map((query, index) => (
                  <Panel
                    key={index}
                    header={<Text strong>{query.name}.sql</Text>}
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
                      <Col span={18}>
                        <Input
                          placeholder="Query name"
                          value={query.name}
                          onChange={(e) => updateQueryConfig(index, 'name', e.target.value)}
                        />
                      </Col>
                      <Col span={6}>
                        <InputNumber
                          addonBefore="Weight"
                          min={1}
                          max={100}
                          value={query.weight}
                          onChange={(value) => updateQueryConfig(index, 'weight', value || 1)}
                          style={{ width: '100%' }}
                        />
                      </Col>
                    </Row>
                    <Input.TextArea
                      placeholder="pgbench query content with \\set variables"
                      value={query.content}
                      onChange={(e) => updateQueryConfig(index, 'content', e.target.value)}
                      rows={6}
                      style={{ marginTop: 8, fontFamily: 'monospace' }}
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
                message="Upload pgbench SQL Files"
                description="Upload one or more .sql files containing pgbench queries. Use -- weight: N comments to set query weights."
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

              {uploadSummary && (
                <Alert
                  message={uploadSummary}
                  type="success"
                  closable
                  onClose={handleClearUploads}
                  style={{ marginBottom: 16 }}
                />
              )}

              {uploadedQueries.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <Text strong>Uploaded Queries ({uploadedQueries.length}):</Text>
                  <div style={{ marginTop: 8 }}>
                    {uploadedQueries.map((q, i) => (
                      <Tag key={i} color="blue" style={{ marginBottom: 4 }}>
                        {q.name}.sql (weight: {q.weight})
                      </Tag>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {querySource === 'workspace' && (
            <>
              <Alert
                message="Use Workspace Path"
                description="Provide the Databricks workspace path to a folder containing your pgbench .sql files."
                type="info"
                showIcon
                icon={<InfoCircleOutlined />}
                style={{ marginBottom: 16 }}
              />

              <Form.Item
                name="query_workspace_path"
                label="Workspace Path to Queries"
                rules={[{ required: querySource === 'workspace', message: 'Please enter workspace path' }]}
              >
                <Input placeholder="/Workspace/Users/your.email@company.com/pgbench_queries/" />
              </Form.Item>
            </>
          )}
        </Card>

        {/* Submit Button */}
        <Form.Item>
          <Button
            type="primary"
            htmlType="submit"
            loading={loading}
            size="large"
            icon={<PlayCircleOutlined />}
            disabled={
              (querySource === 'upload' && uploadedQueries.length === 0) ||
              (querySource === 'predefined' && queryConfigs.length === 0)
            }
            style={{ width: '100%' }}
          >
            {loading ? 'Submitting Job...' : 'Submit pgbench Job'}
          </Button>
        </Form.Item>
      </Form>

      {/* Job Status Display */}
      {renderJobStatus()}
    </div>
  );
};

export default PgbenchDatabricksAutoscaling;
