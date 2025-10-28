import React, { useState, useCallback } from 'react';
import {
  Card,
  Form,
  Input,
  InputNumber,
  Button,
  Upload,
  Alert,
  Typography,
  Row,
  Col,
  Tag,
  message,
  Select,
  Switch,
  Collapse,
  Statistic,
  Radio,
  Tooltip,
} from 'antd';
import {
  UploadOutlined,
  PlayCircleOutlined,
  DatabaseOutlined,
  ClusterOutlined,
  SettingOutlined,
  InfoCircleOutlined
} from '@ant-design/icons';

const { Option } = Select;
const { Panel } = Collapse;

const { Title, Text, Paragraph } = Typography;

interface QueryConfig {
  name: string;
  content: string;
}

const ConcurrencyTestingPsycopg: React.FC = () => {
  const [form] = Form.useForm();
  const [querySource, setQuerySource] = useState<'predefined' | 'upload'>('predefined');
  const [uploadedFiles, setUploadedFiles] = useState<Array<{
    name: string;
    content: string;
    parameter_count: number;
    saved_path: string;
  }>>([]);
  const [isTestRunning, setIsTestRunning] = useState(false);
  const [testResults, setTestResults] = useState<any>(null);
  const [testError, setTestError] = useState<string | null>(null);

  // Predefined queries
  const [queryConfigs, setQueryConfigs] = useState<QueryConfig[]>([
    {
      name: 'customer_lookup_example',
      content: `-- PARAMETERS: [[1, "AAAAAAAABAAAAAAA", 100], [2, "AAAAAAAACAAAAAAA", 50], [3, "AAAAAAAADAAAAAAA", 200], [4, "AAAAAAAAEAAAAAAA", 1000]]
-- EXEC_COUNT: 100

SELECT * FROM customer 
WHERE c_customer_sk = %s 
  AND c_customer_id = %s 
LIMIT %s;`
    },
    {
      name: 'simple_query_example',
      content: `-- EXEC_COUNT: 100

SELECT COUNT(*) as total_customers FROM customer;`
    },
    {
      name: 'customer_flag_example',
      content: `-- PARAMETERS: [["N"], ["Y"]]
-- EXEC_COUNT: 100

SELECT c_preferred_cust_flag, count(*) FROM customer group by c_preferred_cust_flag;`
    }
  ]);

  // Helper functions for predefined queries
  const updateQueryConfig = (index: number, field: keyof QueryConfig, value: any) => {
    const updated = [...queryConfigs];
    updated[index] = { ...updated[index], [field]: value };
    setQueryConfigs(updated);
  };

  const addQueryConfig = () => {
    setQueryConfigs([
      ...queryConfigs,
      { name: `query_${queryConfigs.length + 1}`, content: '' }
    ]);
  };

  const removeQueryConfig = (index: number) => {
    if (queryConfigs.length > 1) {
      setQueryConfigs(queryConfigs.filter((_, i) => i !== index));
    }
  };

  const handleFileUpload = useCallback(async (file: File) => {
    console.log('Starting file upload:', file.name, file.size);

    const formData = new FormData();
    formData.append('file', file);

    try {
      console.log('Sending request to /api/concurrency-test/upload-query');
      const response = await fetch('/api/concurrency-test/upload-query', {
        method: 'POST',
        body: formData,
      });

      console.log('Response status:', response.status);
      console.log('Response headers:', response.headers);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Response error:', errorText);
        message.error(`Upload failed: ${response.status} - ${errorText}`);
        return false;
      }

      const result = await response.json();
      console.log('Response result:', result);

      if (!result.is_valid) {
        message.error(`Query validation failed: ${result.error_message}`);
        return false;
      }

      // Add to uploaded files list (simplified - no parameter configuration needed)
      setUploadedFiles(prev => [...prev, {
        name: result.query_identifier,
        content: result.query_content,
        parameter_count: result.parameter_count,
        saved_path: result.saved_path
      }]);

      message.success(`Query "${result.query_identifier}" uploaded and saved successfully`);
      return false; // Prevent default upload behavior
    } catch (error) {
      console.error('Upload error:', error);
      message.error(`Upload failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return false;
    }
  }, []);

  const handleBeforeUpload = useCallback((file: File) => {
    // Validate file type
    if (!file.name.endsWith('.sql')) {
      message.error('Please upload only .sql files');
      return false;
    }

    // Validate file size (max 1MB)
    if (file.size > 1024 * 1024) {
      message.error('File size must be less than 1MB');
      return false;
    }

    // Process the file
    handleFileUpload(file);
    return false; // Prevent default upload behavior
  }, [handleFileUpload]);


  const handleRunTest = async () => {
    try {
      console.log('Button clicked! Starting test execution...');
      console.log('Uploaded files:', uploadedFiles);

      // Get form values without validation first to see what we have
      const formValues = form.getFieldsValue();
      console.log('Form values (raw):', formValues);
      console.log('workspace_url specifically:', formValues.workspace_url);
      console.log('All form field names:', Object.keys(formValues));

      // Validate required fields manually to avoid workspace_url validation issues
      const requiredFields = ['workspace_url', 'instance_name', 'concurrency_level'];
      const missingFields = requiredFields.filter(field => !formValues[field]);

      console.log('Missing fields check:', missingFields);

      if (missingFields.length > 0) {
        message.error(`Please fill in the following required fields: ${missingFields.join(', ')}`);
        return;
      }

      // Check if we have queries to run
      if (querySource === 'predefined' && queryConfigs.length === 0) {
        message.error('Please add at least one predefined query before running tests');
        return;
      }

      if (querySource === 'upload' && uploadedFiles.length === 0) {
        message.error('Please upload at least one SQL file before running tests');
        return;
      }

      setIsTestRunning(true);
      setTestResults(null);
      setTestError(null); // Clear any previous errors

      const testConfig = {
        databricks_profile: formValues.databricks_profile,
        workspace_url: formValues.workspace_url,
        instance_name: formValues.instance_name,
        database_name: formValues.database_name || 'databricks_postgres',
        concurrency_level: formValues.concurrency_level || 10,
        query_source: querySource,
        // Only include query_configs for predefined mode, never for upload mode
        ...(querySource === 'predefined' && { query_configs: queryConfigs })
      };

      console.log('Sending test config:', testConfig);

      const endpoint = querySource === 'predefined'
        ? '/api/concurrency-test/run-predefined-tests'
        : '/api/concurrency-test/run-uploaded-tests';

      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(testConfig),
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
        let errorMessage = 'Test execution failed';

        if (response.status === 504) {
          errorMessage = 'Request timed out. The test is taking too long. Try reducing concurrency level or test duration.';
        } else {
          try {
            const errorData = await response.json();
            console.error('Error response:', errorData);
            errorMessage = errorData.detail || 'Test execution failed';
          } catch (e) {
            // If response is not JSON (e.g., HTML error page), try to get text
            try {
              const errorText = await response.text();
              console.error('Non-JSON error response:', errorText);
              errorMessage = `Server error (${response.status}): ${errorText.substring(0, 100)}...`;
            } catch (textError) {
              errorMessage = `Server error (${response.status}): Could not read error response`;
            }
          }
        }

        setTestError(errorMessage);
        message.error(`Test execution failed: ${errorMessage}`);
        return; // Don't throw, just return to show error in UI
      }

      const results = await response.json();
      console.log('Test results:', results);
      setTestResults(results);
      setTestError(null); // Clear any previous errors on success
      message.success(`Concurrency test completed successfully! Executed ${uploadedFiles.length} uploaded queries.`);

    } catch (error) {
      console.error('Test execution error:', error);
      let errorMessage = 'Unknown error';

      if (error instanceof Error) {
        if (error.message.includes('504')) {
          errorMessage = 'Request timed out. The test may be taking too long. Try reducing concurrency level,  test duration or scaling up lakebase instance.';
        } else if (error.message.includes('Unexpected token')) {
          errorMessage = 'Server returned invalid response. The test may have timed out or encountered an error.';
        } else {
          errorMessage = error.message;
        }
      }

      setTestError(errorMessage);
      message.error(`Test execution failed: ${errorMessage}`);
    } finally {
      setIsTestRunning(false);
    }
  };


  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <div style={{ marginBottom: '24px' }}>
        <Title level={2} style={{ marginBottom: '8px' }}>
          <DatabaseOutlined style={{ marginRight: '8px' }} />
          Concurrency Testing (psycopg)
        </Title>
        <Paragraph style={{ marginBottom: 0 }}>
          <ul>
            <li>Run concurrency tests against your Lakebase Postgres database using psycopg2 and SQLAlchemy.</li>
            <li>This provides flexible, customizable concurrency testing with detailed performance metrics.</li>
            <li>If running on Databricks Apps, ensure the app service principal has permission to access the database. In Databricks Workspace, navigate to your Lakebase Instance &gt; Permissions &gt; Add PostgreSQL Role, search for the App Service Principal, and grant it the databricks_superuser role.</li>
          </ul>
        </Paragraph>
      </div>

      <Form
        form={form}
        layout="vertical"
        onFinish={handleRunTest}
        initialValues={{
          databricks_profile: "DEFAULT",
          concurrency_level: 10,
          DB_POOL_SIZE: 5,
          DB_MAX_OVERFLOW: 10,
          DB_POOL_TIMEOUT: 30,
          DB_COMMAND_TIMEOUT: 60,
          DB_POOL_RECYCLE_INTERVAL: 3600,
          DB_POOL_PRE_PING: true,
          DB_SSL_MODE: "require"
        }}
      >
        {/* Connection Configuration */}
        <Card
          title={
            <span>
              <ClusterOutlined style={{ marginRight: '8px' }} />
              Connection Configuration
            </span>
          }
          style={{ marginBottom: '24px' }}
        >
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="Databricks Workspace URL"
                name="workspace_url"
                rules={[{ required: true, message: 'Please enter Databricks workspace URL' }]}
              >
                <Input placeholder="https://your-workspace.cloud.databricks.com" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label={
                  <span>
                    Databricks Profile Name
                    <Tooltip title="[Not required if run on Databricks Apps] Databricks CLI profile used for authentication. This should match the profile configured on your machine and align with the Databricks Workspace URL above.">
                      <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                    </Tooltip>
                  </span>
                }
                name="databricks_profile"
              >
                <Input placeholder="DEFAULT" />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="Lakebase Instance Name"
                name="instance_name"
                rules={[
                  { required: true, message: 'Please enter instance name' },
                  { pattern: /^[a-zA-Z0-9-]+$/, message: 'Only alphanumeric characters and hyphens allowed' }
                ]}
              >
                <Input placeholder="lakebase-accelerator-instance" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="Database Name"
                name="database_name"
              >
                <Input placeholder="databricks_postgres" />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="Concurrent Connections"
                name="concurrency_level"
                rules={[
                  { required: true, message: 'Please enter concurrency level' },
                  { type: 'number', min: 1, max: 1000, message: 'Must be between 1 and 1000' }
                ]}
              >
                <InputNumber
                  min={1}
                  max={1000}
                  style={{ width: '100%', borderRadius: '6px', border: 'pink !important' }}
                />
              </Form.Item>
            </Col>
          </Row>
        </Card>

        {/* Concurrency Configuration */}
        <Card
          title={
            <span>
              <SettingOutlined style={{ marginRight: '8px' }} />
              Concurrency Configuration
            </span>
          }
          style={{ marginBottom: '24px' }}
        >

          <Row gutter={16}>
            <Col span={6}>
              <Form.Item
                label="Pool Size"
                name="DB_POOL_SIZE"
                tooltip="Number of persistent connections in the pool"
              >
                <InputNumber min={1} max={100} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                label="Max Overflow"
                name="DB_MAX_OVERFLOW"
                tooltip="Extra connections allowed when pool is busy"
              >
                <InputNumber min={0} max={100} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                label="Pool Timeout (s)"
                name="DB_POOL_TIMEOUT"
                tooltip="Wait time for free connection in seconds"
              >
                <InputNumber min={5} max={300} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                label="Command Timeout (s)"
                name="DB_COMMAND_TIMEOUT"
                tooltip="Max query execution time in seconds"
              >
                <InputNumber min={10} max={3600} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item
                label="Recycle Interval (s)"
                name="DB_POOL_RECYCLE_INTERVAL"
                tooltip="Connection recycle interval in seconds"
              >
                <InputNumber min={300} max={86400} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                label="Pre-ping"
                name="DB_POOL_PRE_PING"
                valuePropName="checked"
                tooltip="Test connections before use"
              >
                <Switch />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item
                label="SSL Mode"
                name="DB_SSL_MODE"
                tooltip="SSL connection mode"
              >
                <Select>
                  <Option value="require">Require</Option>
                  <Option value="prefer">Prefer</Option>
                  <Option value="allow">Allow</Option>
                  <Option value="disable">Disable</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>
        </Card>

        {/* Query Configuration */}
        <Card title="Query Configuration" style={{ marginBottom: '24px' }}>
          <Form.Item label="Query Source">
            <Radio.Group
              value={querySource}
              onChange={async (e) => {
                const newSource = e.target.value;
                setQuerySource(newSource);

                // Clear the opposite mode's data when switching
                if (newSource === 'predefined') {
                  setUploadedFiles([]);
                  // Clear temp directory to prevent mixing with uploaded files
                  try {
                    await fetch('/api/concurrency-test/clear-temp-files', { method: 'POST' });
                  } catch (error) {
                    console.log('Could not clear temp files:', error);
                  }
                } else if (newSource === 'upload') {
                  // Keep predefined queries but they won't be used
                  console.log('Switched to upload mode - predefined queries will not be used');
                }
              }}
            >
              <Radio.Button value="predefined">
                <SettingOutlined /> Predefined
              </Radio.Button>
              <Radio.Button value="upload">
                <UploadOutlined /> Upload Files
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
                      <Text strong>{query.name}</Text>
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
                      <Col span={12}>
                        <Input
                          placeholder="Query name"
                          value={query.name}
                          onChange={(e) => updateQueryConfig(index, 'name', e.target.value)}
                        />
                      </Col>
                    </Row>
                    <Input.TextArea
                      placeholder="SQL query content with -- PARAMETERS: and -- EXEC_COUNT: comments"
                      value={query.content}
                      onChange={(e) => updateQueryConfig(index, 'content', e.target.value)}
                      rows={8}
                      style={{ marginTop: 8, fontFamily: 'monospace' }}
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
                message="Upload SQL Query Files"
                description={
                  <>
                    Upload one or more <code>.sql</code> files. Each file should contain PostgreSQL queries with proper parameterization. Predefined queries are not used in this mode.
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
                beforeUpload={handleBeforeUpload}
                showUploadList={false}
                style={{ marginBottom: 16 }}
              >
                <p className="ant-upload-drag-icon">
                  <UploadOutlined style={{ fontSize: 48, color: '#1890ff' }} />
                </p>
                <p className="ant-upload-text">Click or drag SQL files to upload</p>
                <p className="ant-upload-hint">
                  Upload .sql files with your PostgreSQL queries
                </p>
              </Upload.Dragger>

              {/* Uploaded Files Display */}
              {uploadedFiles.length > 0 && (
                <div style={{ marginTop: '24px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                    <Title level={4} style={{ margin: 0 }}>Uploaded Queries</Title>
                    <Button
                      danger
                      size="small"
                      onClick={async () => {
                        try {
                          // Clear temp directory on backend
                          const response = await fetch('/api/concurrency-test/clear-temp-files', {
                            method: 'POST'
                          });

                          if (response.ok) {
                            setUploadedFiles([]);
                            message.success('Cleared all uploaded files and temp directory');
                          } else {
                            message.error('Failed to clear temp files');
                          }
                        } catch (error) {
                          console.error('Error clearing temp files:', error);
                          message.error('Failed to clear temp files');
                        }
                      }}
                    >
                      Clear All
                    </Button>
                  </div>

                  <Collapse>
                    {uploadedFiles.map((file, index) => (
                      <Panel
                        header={
                          <span>
                            <Text strong>{file.name}</Text>
                            <Tag color="blue" style={{ marginLeft: '8px' }}>
                              Parameter Sets: {file.parameter_count}
                            </Tag>
                          </span>
                        }
                        key={index}
                      >
                        <div style={{ marginBottom: '8px' }}>
                          <Text type="secondary">Saved to: {file.saved_path}</Text>
                        </div>
                        <div style={{ marginBottom: '8px' }}>
                          <Button
                            danger
                            size="small"
                            onClick={async () => {
                              try {
                                const response = await fetch('/api/concurrency-test/delete-query', {
                                  method: 'DELETE',
                                  headers: { 'Content-Type': 'application/json' },
                                  body: JSON.stringify({ file_path: file.saved_path }),
                                });

                                if (response.ok) {
                                  setUploadedFiles(prev => prev.filter((_, i) => i !== index));
                                  message.success('File deleted successfully');
                                } else {
                                  message.error('Failed to delete file');
                                }
                              } catch (error) {
                                message.error('Failed to delete file');
                              }
                            }}
                          >
                            Delete
                          </Button>
                        </div>
                      </Panel>
                    ))}
                  </Collapse>
                </div>
              )}
            </>
          )}
        </Card>

        {/* Submit Button */}
        <Form.Item>
          <Button
            type="primary"
            htmlType="submit"
            loading={isTestRunning}
            size="large"
            icon={<PlayCircleOutlined />}
            disabled={
              (querySource === 'predefined' && queryConfigs.length === 0) ||
              (querySource === 'upload' && uploadedFiles.length === 0)
            }
            style={{ width: '100%' }}
          >
            {isTestRunning ? 'Running Tests...' : 'Run Concurrency Tests'}
          </Button>
        </Form.Item>
      </Form>

      {/* Test Results */}
      {
        testResults && (
          <div style={{ marginTop: '24px' }}>
            {/* Test Configuration Summary */}
            <Card title="Test Configuration Summary" style={{ marginBottom: '16px' }}>
              <Row gutter={16}>
                <Col span={8}>
                  <Statistic title="Total Queries" value={testResults.query_results?.length || 0} />
                </Col>
                <Col span={8}>
                  <Statistic title="Concurrency Level" value={testResults.concurrency_level || 0} />
                </Col>
                <Col span={8}>
                  <Statistic title="Available Connection Pool" value={testResults.connection_pool_metrics?.pool_size || 0} />
                </Col>
              </Row>
            </Card>

            {/* Concurrency Test Results */}
            <Card title="Concurrency Test Results" style={{ marginBottom: '16px' }}>
              <Row gutter={16}>
                <Col span={6}>
                  <Statistic title="Concurrency Level" value={testResults.concurrency_level || 0} />
                </Col>
                <Col span={6}>
                  <Statistic title="Total Queries" value={testResults.total_queries_executed || 0} />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Success Rate"
                    value={(testResults.success_rate || 0) * 100}
                    suffix="%"
                    precision={1}
                    valueStyle={{ color: '#52c41a' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Throughput"
                    value={testResults.throughput_queries_per_second || 0}
                    suffix="qps"
                    precision={2}
                    valueStyle={{ color: '#722ed1' }}
                  />
                </Col>
              </Row>

              <Row gutter={16} style={{ marginTop: '16px' }}>
                <Col span={6}>
                  <Statistic
                    title="Avg Execution Time"
                    value={testResults.average_execution_time_ms || 0}
                    suffix="ms"
                    precision={2}
                    valueStyle={{ color: '#1890ff' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="P95 Latency"
                    value={testResults.p95_execution_time_ms || 0}
                    suffix="ms"
                    precision={2}
                    valueStyle={{ color: '#1890ff' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="P99 Latency"
                    value={testResults.p99_execution_time_ms || 0}
                    suffix="ms"
                    precision={2}
                    valueStyle={{ color: '#ff4d4f' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic
                    title="Test Duration"
                    value={testResults.total_duration_seconds || 0}
                    suffix="s"
                    precision={1}
                  />
                </Col>
              </Row>
            </Card>

            {/* Query Execution Details */}
            {testResults.query_results && testResults.query_results.length > 0 && (
              <Card title="Query Execution Details">
                {(() => {
                  // Aggregate query results by query_identifier
                  const aggregatedResults: { [key: string]: any } = {};

                  testResults.query_results.forEach((result: any) => {
                    const queryId = result.query_identifier;
                    if (!aggregatedResults[queryId]) {
                      aggregatedResults[queryId] = {
                        total_executions: 0,
                        successful_executions: 0,
                        failed_executions: 0,
                        durations: [],
                        parameter_sets: new Set(),
                        error_messages: new Set(),
                        error_types: new Set()
                      };
                    }

                    aggregatedResults[queryId].total_executions++;
                    if (result.success) {
                      aggregatedResults[queryId].successful_executions++;
                      aggregatedResults[queryId].durations.push(result.duration_ms);
                    } else {
                      aggregatedResults[queryId].failed_executions++;
                      // Collect error information for debugging
                      if (result.error_message) {
                        aggregatedResults[queryId].error_messages.add(result.error_message);
                      }
                      if (result.error_type) {
                        aggregatedResults[queryId].error_types.add(result.error_type);
                      }
                    }

                    // Collect parameter sets (if available)
                    if (result.parameters && result.parameters.length > 0) {
                      aggregatedResults[queryId].parameter_sets.add(JSON.stringify(result.parameters));
                    }
                  });

                  // Calculate statistics for each query
                  Object.keys(aggregatedResults).forEach(queryId => {
                    const data = aggregatedResults[queryId];
                    data.avg_duration_ms = data.durations.length > 0
                      ? data.durations.reduce((a: number, b: number) => a + b, 0) / data.durations.length
                      : 0;
                    data.min_duration_ms = data.durations.length > 0 ? Math.min(...data.durations) : 0;
                    data.max_duration_ms = data.durations.length > 0 ? Math.max(...data.durations) : 0;
                    data.parameter_sets = Array.from(data.parameter_sets).map((ps: unknown) => {
                      try {
                        const parsed = JSON.parse(ps as string);
                        return Object.values(parsed).join(', ');
                      } catch {
                        return ps as string;
                      }
                    });
                  });

                  return Object.entries(aggregatedResults).map(([queryId, details]: [string, any]) => (
                    <Card
                      key={queryId}
                      size="small"
                      title={queryId}
                      style={{ marginBottom: '16px' }}
                    >
                      <Row gutter={16}>
                        <Col span={6}>
                          <Statistic title="Total Executions" value={details.total_executions || 0} />
                        </Col>
                        <Col span={6}>
                          <Statistic
                            title="Avg Duration"
                            value={details.avg_duration_ms || 0}
                            suffix="ms"
                            precision={2}
                            valueStyle={{ color: '#1890ff' }}
                          />
                        </Col>
                        <Col span={6}>
                          <Statistic
                            title="Min Duration"
                            value={details.min_duration_ms || 0}
                            suffix="ms"
                            precision={2}
                            valueStyle={{ color: '#52c41a' }}
                          />
                        </Col>
                        <Col span={6}>
                          <Statistic
                            title="Max Duration"
                            value={details.max_duration_ms || 0}
                            suffix="ms"
                            precision={2}
                            valueStyle={{ color: '#ff4d4f' }}
                          />
                        </Col>
                      </Row>

                      <div style={{ marginTop: '12px' }}>
                        <Text strong>Status: </Text>
                        <Tag color="green">
                          {details.successful_executions || 0} successful
                        </Tag>
                        {details.failed_executions > 0 && (
                          <Tag color="red" style={{ marginLeft: '8px' }}>
                            {details.failed_executions} failed
                          </Tag>
                        )}
                      </div>

                      {details.parameter_sets && details.parameter_sets.length > 0 && (
                        <div style={{ marginTop: '8px' }}>
                          <Text strong>Parameter Sets: </Text>
                          {details.parameter_sets.map((paramSet: string, index: number) => (
                            <Tag key={index} style={{ marginRight: '4px' }}>
                              {paramSet}
                            </Tag>
                          ))}
                        </div>
                      )}

                      {/* Error Details for Failed Queries */}
                      {details.failed_executions > 0 && (details.error_messages.size > 0 || details.error_types.size > 0) && (
                        <div style={{ marginTop: '12px' }}>
                          <Collapse
                            size="small"
                            items={[
                              {
                                key: 'error-details',
                                label: (
                                  <Text strong style={{ color: '#ff4d4f' }}>
                                    Error Details ({details.failed_executions} failures) - Click to expand
                                  </Text>
                                ),
                                children: (
                                  <div>
                                    {details.error_types.size > 0 && (
                                      <div style={{ marginBottom: '12px' }}>
                                        <Text strong>Error Types: </Text>
                                        {Array.from(details.error_types).map((errorType: unknown, index: number) => (
                                          <Tag key={index} color="red" style={{ marginRight: '4px' }}>
                                            {errorType as string}
                                          </Tag>
                                        ))}
                                      </div>
                                    )}
                                    {details.error_messages.size > 0 && (
                                      <div>
                                        <Text strong>Error Messages: </Text>
                                        <div style={{ marginTop: '8px' }}>
                                          {Array.from(details.error_messages).map((errorMsg: unknown, index: number) => (
                                            <div key={index} style={{
                                              marginBottom: '8px',
                                              padding: '12px',
                                              backgroundColor: '#fff2f0',
                                              border: '1px solid #ffccc7',
                                              borderRadius: '6px',
                                              fontSize: '12px',
                                              fontFamily: 'monospace',
                                              whiteSpace: 'pre-wrap',
                                              wordBreak: 'break-word'
                                            }}>
                                              <Text style={{ color: '#ff4d4f' }}>{errorMsg as string}</Text>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                )
                              }
                            ]}
                          />
                        </div>
                      )}
                    </Card>
                  ));
                })()}
              </Card>
            )}
          </div>
        )
      }

      {/* Test Error */}
      {
        testError && (
          <Card title="Test Error" style={{ marginTop: '24px' }}>
            <Alert
              message="Test Execution Failed"
              description={testError}
              type="error"
              showIcon
              style={{ color: '#ff4d4f' }}
            />
          </Card>
        )
      }
    </div >
  );
};

export default ConcurrencyTestingPsycopg;
