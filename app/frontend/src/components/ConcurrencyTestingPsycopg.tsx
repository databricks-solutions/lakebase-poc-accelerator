import React, { useState, useCallback } from 'react';
import {
  Card,
  Form,
  Input,
  InputNumber,
  Button,
  Upload,
<<<<<<< HEAD
  Alert,
  Typography,
=======
  Steps,
  Alert,
  Space,
  Typography,
  Divider,
>>>>>>> origin/main
  Row,
  Col,
  Tag,
  message,
<<<<<<< HEAD
  Select,
  Switch,
  Collapse,
  Statistic,
  Radio,
  Tooltip,
=======
  Tooltip,
  Select,
  Switch
>>>>>>> origin/main
} from 'antd';
import {
  UploadOutlined,
  PlayCircleOutlined,
<<<<<<< HEAD
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
=======
  InfoCircleOutlined,
  WarningOutlined
} from '@ant-design/icons';

const { Option } = Select;

const { Title, Text, Paragraph } = Typography;


const ConcurrencyTesting: React.FC = () => {
  const [form] = Form.useForm();
  const [currentStep, setCurrentStep] = useState(0);
>>>>>>> origin/main
  const [uploadedFiles, setUploadedFiles] = useState<Array<{
    name: string;
    content: string;
    parameter_count: number;
    saved_path: string;
  }>>([]);
  const [isTestRunning, setIsTestRunning] = useState(false);
  const [testResults, setTestResults] = useState<any>(null);
  const [testError, setTestError] = useState<string | null>(null);

<<<<<<< HEAD
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
=======
>>>>>>> origin/main

  const handleFileUpload = useCallback(async (file: File) => {
    console.log('Starting file upload:', file.name, file.size);

    const formData = new FormData();
    formData.append('file', file);

    try {
<<<<<<< HEAD
      console.log('Sending request to /api/concurrency-test/upload-query');
      const response = await fetch('/api/concurrency-test/upload-query', {
=======
      console.log('Sending request to http://localhost:8000/api/concurrency-test/upload-query');
      const response = await fetch('http://localhost:8000/api/concurrency-test/upload-query', {
>>>>>>> origin/main
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
<<<<<<< HEAD
      const requiredFields = ['workspace_url', 'instance_name', 'concurrency_level'];
=======
      const requiredFields = ['databricks_profile', 'workspace_url', 'instance_name', 'concurrency_level'];
>>>>>>> origin/main
      const missingFields = requiredFields.filter(field => !formValues[field]);

      console.log('Missing fields check:', missingFields);

      if (missingFields.length > 0) {
        message.error(`Please fill in the following required fields: ${missingFields.join(', ')}`);
        return;
      }

<<<<<<< HEAD
      // Check if we have queries to run
      if (querySource === 'predefined' && queryConfigs.length === 0) {
        message.error('Please add at least one predefined query before running tests');
        return;
      }

      if (querySource === 'upload' && uploadedFiles.length === 0) {
=======
      if (uploadedFiles.length === 0) {
>>>>>>> origin/main
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
<<<<<<< HEAD
        concurrency_level: formValues.concurrency_level || 10,
        query_source: querySource,
        // Only include query_configs for predefined mode, never for upload mode
        ...(querySource === 'predefined' && { query_configs: queryConfigs })
=======
        concurrency_level: formValues.concurrency_level || 10
>>>>>>> origin/main
      };

      console.log('Sending test config:', testConfig);

<<<<<<< HEAD
      const endpoint = querySource === 'predefined'
        ? '/api/concurrency-test/run-predefined-tests'
        : '/api/concurrency-test/run-uploaded-tests';

      const response = await fetch(endpoint, {
=======
      const response = await fetch('http://localhost:8000/api/concurrency-test/run-uploaded-tests', {
>>>>>>> origin/main
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(testConfig),
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
<<<<<<< HEAD
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

=======
        const errorData = await response.json();
        console.error('Error response:', errorData);
        const errorMessage = errorData.detail || 'Test execution failed';
>>>>>>> origin/main
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
<<<<<<< HEAD
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

=======
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
>>>>>>> origin/main
      setTestError(errorMessage);
      message.error(`Test execution failed: ${errorMessage}`);
    } finally {
      setIsTestRunning(false);
    }
  };

<<<<<<< HEAD

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
=======
  const steps = [
    {
      title: 'Connection Setup',
      description: 'Configure Databricks and Lakebase connection'
    },
    {
      title: 'Upload SQL Files',
      description: 'Upload SQL files with embedded parameters and execution counts'
    },
    {
      title: 'Run Tests',
      description: 'Execute concurrency tests using uploaded queries'
    }
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>Concurrency Testing (psycopg and SQLAlchemy)</Title>

      <Alert
        message="Cloud Compatible"
        description="This psycopg framework testing method can be run on Databricks, local machines, or any Python environment. It provides flexible, customizable concurrency testing for Lakebase Postgres databases."
        type="info"
        showIcon
        style={{ marginBottom: '16px' }}
      />
      <Paragraph>
        This approach uses Python's psycopg2 and SQLAlchemy libraries to create custom concurrency tests.
      </Paragraph>

      <Paragraph>
        <ul style={{ marginBottom: 0 }}>
          <li><strong>Asyncio-Based Concurrency:</strong> Uses Python asyncio with semaphore-based concurrency control to execute multiple queries simultaneously</li>
          <li><strong>Connection Pool Management:</strong> SQLAlchemy async engine with dynamic pool sizing (base pool + overflow) based on concurrency level</li>
          <li><strong>Parameterized Query Support:</strong> Processes SQL files with PARAMETERS and EXEC_COUNT comments for comprehensive test scenarios</li>
          <li><strong>Performance Metrics:</strong> Tracks execution times, throughput, latency percentiles (P95, P99), and success rates</li>
          <li><strong>Resource Management:</strong> Prevents database overload through controlled concurrency and automatic connection reuse</li>
          <li><strong>Error Isolation:</strong> Individual query failures don't affect other concurrent executions</li>
        </ul>
      </Paragraph>

      <Steps current={currentStep} items={steps} style={{ marginBottom: '24px' }} />
>>>>>>> origin/main

      <Form
        form={form}
        layout="vertical"
<<<<<<< HEAD
        onFinish={handleRunTest}
        initialValues={{
          databricks_profile: "DEFAULT",
=======
        initialValues={{
          databricks_profile: "DEFAULT",
          instance_name: "lakebase-accelerator-instance",
          database_name: "databricks_postgres",
>>>>>>> origin/main
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
<<<<<<< HEAD
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
=======
        {/* Hidden form fields that are always rendered */}
        <Form.Item name="databricks_profile" style={{ display: 'none' }}>
          <Input />
        </Form.Item>
        <Form.Item name="workspace_url" style={{ display: 'none' }}>
          <Input />
        </Form.Item>
        <Form.Item name="instance_name" style={{ display: 'none' }}>
          <Input />
        </Form.Item>
        <Form.Item name="database_name" style={{ display: 'none' }}>
          <Input />
        </Form.Item>
        <Form.Item name="concurrency_level" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_POOL_SIZE" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_MAX_OVERFLOW" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_POOL_TIMEOUT" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_COMMAND_TIMEOUT" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_POOL_RECYCLE_INTERVAL" style={{ display: 'none' }}>
          <InputNumber />
        </Form.Item>
        <Form.Item name="DB_POOL_PRE_PING" style={{ display: 'none' }}>
          <Switch />
        </Form.Item>
        <Form.Item name="DB_SSL_MODE" style={{ display: 'none' }}>
          <Select />
        </Form.Item>

        {/* Step 1: Connection Configuration */}
        {currentStep === 0 && (
          <Card title="Connection Setup" style={{ marginBottom: '24px' }}>
            <Row gutter={16}>
              <Col span={12}>
                <Form.Item
                  label={
                    <span>
                      Databricks Profile Name{' '}
                      <Tooltip title="Databricks CLI profile used for authentication. This should match the profile configured on your machine and align with the Databricks Workspace URL.">
                        <InfoCircleOutlined />
                      </Tooltip>
                    </span>
                  }
                  name="databricks_profile"
                  rules={[
                    { required: true, message: 'Please enter Databricks CLI profile name' }
                  ]}
                >
                  <Input placeholder="DEFAULT" />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item
                  label="Databricks Workspace URL"
                  name="workspace_url"
                  rules={[
                    { required: true, message: 'Please enter Databricks workspace URL' }
                  ]}
                >
                  <Input
                    placeholder="https://your-workspace.cloud.databricks.com"
                    suffix={
                      <Tooltip title="Enter your Databricks workspace URL">
                        <InfoCircleOutlined />
                      </Tooltip>
                    }
                  />
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
                    {
                      pattern: /^[a-zA-Z0-9-]+$/,
                      message: 'Only alphanumeric characters and hyphens allowed'
                    }
                  ]}
                >
                  <Input
                    placeholder="my-lakebase-instance"
                    suffix={
                      <Tooltip title="Enter an existing Lakebase instance name. Ensure you have proper permissions.">
                        <WarningOutlined />
                      </Tooltip>
                    }
                  />
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
                    style={{ width: '100%' }}
                    suffix={
                      <Tooltip title="Number of concurrent connections to test">
                        <InfoCircleOutlined />
                      </Tooltip>
                    }
                  />
                </Form.Item>
              </Col>
            </Row>

            <Divider>Connection Pool Settings</Divider>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Keep pool size ≤ CPU cores × 2–3
                </Text>
                <Form.Item
                  label={
                    <span>
                      Pool Size
                      <Tooltip title="Number of persistent connections in the pool. Keep pool size ≤ CPU cores on the Postgres server × 2–3.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_POOL_SIZE"
                >
                  <InputNumber min={1} max={100} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Set to about equal to or 2× pool size
                </Text>
                <Form.Item
                  label={
                    <span>
                      Max Overflow
                      <Tooltip title="Extra connections allowed when pool is busy. Set to about equal to or 2× pool size.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_MAX_OVERFLOW"
                >
                  <InputNumber min={0} max={100} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  30–60s for background jobs/analytics
                </Text>
                <Form.Item
                  label={
                    <span>
                      Pool Timeout (s)
                      <Tooltip title="Wait time for free connection in seconds. For background jobs/analytics, keep longer (30–60s).">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_POOL_TIMEOUT"
                >
                  <InputNumber min={5} max={300} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
            </Row>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  30–60s for reporting/batch jobs
                </Text>
                <Form.Item
                  label={
                    <span>
                      Command Timeout (s)
                      <Tooltip title="Max query execution time in seconds. 30–60 seconds for reporting or background batch jobs.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_COMMAND_TIMEOUT"
                >
                  <InputNumber min={10} max={3600} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Default 3600s (1 hour) is good
                </Text>
                <Form.Item
                  label={
                    <span>
                      Recycle Interval (s)
                      <Tooltip title="Connection recycle interval in seconds. Default 3600s (1 hour) is good.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_POOL_RECYCLE_INTERVAL"
                >
                  <InputNumber min={300} max={86400} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Enable to validate connections
                </Text>
                <Form.Item
                  label={
                    <span>
                      Pre-ping
                      <Tooltip title="Validate connections before use to ensure they're still active.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="DB_POOL_PRE_PING"
                  valuePropName="checked"
                >
                  <Switch />
                </Form.Item>
              </Col>
            </Row>

            <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
              'require' for secure connections
            </Text>
            <Form.Item
              label={
                <span>
                  SSL Mode
                  <Tooltip title="SSL connection mode. 'require' is recommended for security.">
                    <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                  </Tooltip>
                </span>
              }
              name="DB_SSL_MODE"
            >
              <Select>
                <Option value="require">Require</Option>
                <Option value="prefer">Prefer</Option>
                <Option value="allow">Allow</Option>
                <Option value="disable">Disable</Option>
              </Select>
            </Form.Item>

            <Form.Item>
              <Button
                type="primary"
                onClick={() => setCurrentStep(1)}
                size="large"
              >
                Next: Upload Queries
              </Button>
            </Form.Item>
          </Card>
        )}

        {/* Step 2: Upload SQL Files */}
        {currentStep === 1 && (
          <Card title="Upload SQL Files" style={{ marginBottom: '24px' }}>
            <div style={{ marginBottom: '24px' }}>
              <Title level={4}>Upload SQL Query Files</Title>
              <Paragraph>
                Upload .sql files containing your PostgreSQL queries. <strong>Each file must include required comments:</strong>
              </Paragraph>

              <Upload
                accept=".sql"
                beforeUpload={handleBeforeUpload}
                showUploadList={false}
                multiple
              >
                <Button icon={<UploadOutlined />} size="large">
                  Upload SQL Files
                </Button>
              </Upload>

              {/* Uploaded Files Display */}
              <div style={{ marginTop: '24px' }}>
                <Title level={4}>Uploaded Files</Title>
                {uploadedFiles.length === 0 ? (
                  <Alert
                    message="No files uploaded yet"
                    description="Upload SQL files to see them listed here"
                    type="info"
                    showIcon
                  />
                ) : (
                  <div>
                    {uploadedFiles.map((file, index) => (
                      <Card key={index} size="small" style={{ marginBottom: '8px' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <div>
                            <Text strong>{file.name}</Text>
                            <br />
                            <Text type="secondary">
                              Parameters: {file.parameter_count} | Saved to: {file.saved_path}
                            </Text>
                          </div>
                          <div>
                            <Tag color="blue">{file.parameter_count} params</Tag>
                          </div>
                        </div>
                      </Card>
                    ))}
                  </div>
                )}
              </div>

              {/* SQL Examples */}
              <Card
                title="SQL Query Examples"
                size="small"
                style={{ marginTop: '16px', backgroundColor: '#fafafa' }}
              >
                <Alert
                  message="Required SQL File Format"
                  description={
                    <div>
                      <p><strong>⚠️ IMPORTANT:</strong> Your SQL files must include these comments at the top:</p>
                      <ul style={{ marginBottom: '8px' }}>
                        <li><code>-- PARAMETERS: [[param1, param2], [param3, param4]]</code> (if using parameters)</li>
                        <li><code>-- EXEC_COUNT: 10</code> (number of executions per scenario)</li>
                      </ul>
                    </div>
                  }
                  type="warning"
                  style={{ marginBottom: '16px' }}
                />

                <Row gutter={16}>
                  <Col span={12}>
                    <Title level={5}>Example 1: Simple Query (No Parameters)</Title>
                    <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                      File: simple_query_example.sql
                    </Text>
                    <pre style={{
                      backgroundColor: '#f5f5f5',
                      padding: '12px',
                      borderRadius: '4px',
                      fontSize: '12px',
                      marginBottom: '8px',
                      overflow: 'auto'
                    }}>
                      {`-- EXEC_COUNT: 5

SELECT COUNT(*) as total_customers FROM customer;`}
                    </pre>
                    <Text type="secondary" style={{ fontSize: '11px' }}>
                      No parameters needed - just specify EXEC_COUNT
                    </Text>
                  </Col>

                  <Col span={12}>
                    <Title level={5}>Example 2: Query with Parameters</Title>
                    <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                      File: customer_flag.sql
                    </Text>
                    <pre style={{
                      backgroundColor: '#f5f5f5',
                      padding: '12px',
                      borderRadius: '4px',
                      fontSize: '12px',
                      marginBottom: '8px',
                      overflow: 'auto'
                    }}>
                      {`-- PARAMETERS: [["N"], ["Y"]]
-- EXEC_COUNT: 1

SELECT * FROM customer where c_preferred_cust_flag = %s limit 1000;`}
                    </pre>
                    <Text type="secondary" style={{ fontSize: '11px' }}>
                      Parameters: flag values "N" and "Y" for preferred customer flag
                    </Text>
                  </Col>
                </Row>

                <Row gutter={16} style={{ marginTop: '16px' }}>
                  <Col span={24}>
                    <Title level={5}>Example 3: Complex Query with Multiple Parameters</Title>
                    <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                      File: customer_lookup_example.sql
                    </Text>
                    <pre style={{
                      backgroundColor: '#f5f5f5',
                      padding: '12px',
                      borderRadius: '4px',
                      fontSize: '12px',
                      marginBottom: '8px',
                      overflow: 'auto'
                    }}>
                      {`-- PARAMETERS: [[1, "AAAAAAAABAAAAAAA", 100], [2, "AAAAAAAACAAAAAAA", 50], [3, "AAAAAAAADAAAAAAA", 200], [4, "AAAAAAAAEAAAAAAA", 1000]]
-- EXEC_COUNT: 40

SELECT * FROM customer 
WHERE c_customer_sk = %s 
  AND c_customer_id = %s 
LIMIT %s;`}
                    </pre>
                    <Text type="secondary" style={{ fontSize: '11px' }}>
                      Parameters: customer_sk (integer), customer_id (string), limit (integer) - 4 different scenarios
                    </Text>
                  </Col>
                </Row>
              </Card>
            </div>


            <div style={{ marginTop: '24px', textAlign: 'center' }}>
              <Space>
                <Button onClick={() => setCurrentStep(0)}>
                  Previous
                </Button>
                <Button
                  type="primary"
                  onClick={() => setCurrentStep(2)}
                  disabled={uploadedFiles.length === 0}
                  size="large"
                >
                  Next: Run Tests
                </Button>
              </Space>
            </div>
          </Card>
        )}

        {/* Step 3: Run Tests */}
        {currentStep === 2 && (
          <Card title="Run Tests" style={{ marginBottom: '24px' }}>
            <div style={{ marginBottom: '24px' }}>
              <Title level={4}>Test Configuration Summary</Title>
              <Row gutter={16}>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>Total Queries</Text>
                    <div style={{ fontSize: '24px', color: '#1890ff' }}>
                      {uploadedFiles.length}
                    </div>
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>Concurrency Level</Text>
                    <div style={{ fontSize: '24px', color: '#fa8c16' }}>
                      {form.getFieldValue('concurrency_level') || 10}
                    </div>
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>Available Connection Pool</Text>
                    <div style={{ fontSize: '24px', color: '#722ed1' }}>
                      {(form.getFieldValue('DB_POOL_SIZE') || 5) + (form.getFieldValue('DB_MAX_OVERFLOW') || 10)}
                    </div>
                  </Card>
                </Col>
              </Row>
            </div>

            {isTestRunning && (
              <Alert
                message="Test Running"
                description="Executing concurrency tests. Please wait..."
                type="info"
                showIcon
                style={{ marginBottom: '24px' }}
              />
            )}

            {testError && (
              <Alert
                message="Test Execution Failed"
                description={
                  <div>
                    <Text strong>Error Details:</Text>
                    <br />
                    <Text code style={{ marginTop: '8px', display: 'block' }}>
                      {testError}
                    </Text>
                    <br />
                    <Text type="secondary" style={{ marginTop: '8px', display: 'block' }}>
                      Please check your configuration and try again. Common issues:
                    </Text>
                    <ul style={{ marginTop: '8px', marginBottom: 0 }}>
                      <li>Invalid Databricks profile name</li>
                      <li>Incorrect Lakebase instance name</li>
                      <li>Missing permissions for the specified instance</li>
                      <li>Network connectivity issues</li>
                    </ul>
                  </div>
                }
                type="error"
                showIcon
                style={{ marginBottom: '24px' }}
                action={
                  <Button
                    size="small"
                    danger
                    onClick={() => setTestError(null)}
                  >
                    Dismiss
                  </Button>
                }
              />
            )}

            {testResults && (
              <div style={{ marginBottom: '24px' }}>
                <Title level={4}>Concurrency Test Results</Title>

                {/* High-level Test Information */}
                <Card style={{ marginBottom: '16px' }}>
                  <Row gutter={16}>
                    <Col span={12}>
                      <div style={{ marginBottom: '8px' }}>
                        <Text strong>Concurrency Level:</Text>
                        <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#1890ff' }}>
                          {testResults.concurrency_level || 'N/A'}
                        </Text>
                      </div>
                    </Col>
                    <Col span={12}>
                      <div style={{ marginBottom: '8px' }}>
                        <Text strong>Total Queries:</Text>
                        <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#52c41a' }}>
                          {testResults.total_queries_executed || 'N/A'}
                        </Text>
                      </div>
                      <div style={{ marginBottom: '8px' }}>
                        <Text strong>Success Rate:</Text>
                        <Text style={{ marginLeft: '8px', fontSize: '16px', color: testResults.success_rate && testResults.success_rate > 0.95 ? '#52c41a' : '#fa8c16' }}>
                          {testResults.success_rate ? (testResults.success_rate * 100).toFixed(1) + '%' : 'N/A'}
                        </Text>
                      </div>
                    </Col>
                  </Row>
                </Card>

                {/* Performance Metrics */}
                <Row gutter={16} style={{ marginBottom: '16px' }}>
                  <Col span={6}>
                    <Card size="small">
                      <Text strong>Avg Execution Time</Text>
                      <div style={{ fontSize: '20px', color: '#1890ff', marginTop: '4px' }}>
                        {testResults.average_execution_time_ms ? testResults.average_execution_time_ms.toFixed(2) + 'ms' : 'N/A'}
                      </div>
                    </Card>
                  </Col>
                  <Col span={6}>
                    <Card size="small">
                      <Text strong>P95 Latency</Text>
                      <div style={{ fontSize: '20px', color: '#fa8c16', marginTop: '4px' }}>
                        {testResults.p95_execution_time_ms ? testResults.p95_execution_time_ms.toFixed(2) + 'ms' : 'N/A'}
                      </div>
                    </Card>
                  </Col>
                  <Col span={6}>
                    <Card size="small">
                      <Text strong>P99 Latency</Text>
                      <div style={{ fontSize: '20px', color: '#f5222d', marginTop: '4px' }}>
                        {testResults.p99_execution_time_ms ? testResults.p99_execution_time_ms.toFixed(2) + 'ms' : 'N/A'}
                      </div>
                    </Card>
                  </Col>
                  <Col span={6}>
                    <Card size="small">
                      <Text strong>Throughput</Text>
                      <div style={{ fontSize: '20px', color: '#722ed1', marginTop: '4px' }}>
                        {testResults.throughput_queries_per_second ? testResults.throughput_queries_per_second.toFixed(2) + ' qps' : 'N/A'}
                      </div>
                    </Card>
                  </Col>
                </Row>

                {/* Query-specific Results */}
                {testResults.query_results && testResults.query_results.length > 0 && (
                  <Card title="Query Execution Details" style={{ marginBottom: '16px' }}>
                    {(() => {
                      // Group results by query identifier
                      const queryGroups = testResults.query_results.reduce((acc: any, result: any) => {
                        if (!acc[result.query_identifier]) {
                          acc[result.query_identifier] = [];
                        }
                        acc[result.query_identifier].push(result);
                        return acc;
                      }, {});

                      return Object.entries(queryGroups).map(([queryId, results]: [string, any]) => {
                        const successfulResults = results.filter((r: any) => r.success);
                        const failedResults = results.filter((r: any) => !r.success);
                        const avgDuration = successfulResults.length > 0
                          ? successfulResults.reduce((sum: number, r: any) => sum + r.duration_ms, 0) / successfulResults.length
                          : 0;
                        const minDuration = successfulResults.length > 0
                          ? Math.min(...successfulResults.map((r: any) => r.duration_ms))
                          : 0;
                        const maxDuration = successfulResults.length > 0
                          ? Math.max(...successfulResults.map((r: any) => r.duration_ms))
                          : 0;

                        return (
                          <Card
                            key={queryId}
                            size="small"
                            style={{ marginBottom: '12px', backgroundColor: '#fafafa' }}
                            title={
                              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <Text strong>{queryId}</Text>
                                <div>
                                  <Tag color="green">{successfulResults.length} successful</Tag>
                                  {failedResults.length > 0 && <Tag color="red">{failedResults.length} failed</Tag>}
                                </div>
                              </div>
                            }
                          >
                            <Row gutter={16}>
                              <Col span={6}>
                                <Text type="secondary">Total Executions:</Text>
                                <div style={{ fontSize: '16px', fontWeight: 'bold' }}>
                                  {results.length}
                                </div>
                              </Col>
                              <Col span={6}>
                                <Text type="secondary">Avg Duration:</Text>
                                <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#1890ff' }}>
                                  {avgDuration.toFixed(2)}ms
                                </div>
                              </Col>
                              <Col span={6}>
                                <Text type="secondary">Min Duration:</Text>
                                <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#52c41a' }}>
                                  {minDuration.toFixed(2)}ms
                                </div>
                              </Col>
                              <Col span={6}>
                                <Text type="secondary">Max Duration:</Text>
                                <div style={{ fontSize: '16px', fontWeight: 'bold', color: '#f5222d' }}>
                                  {maxDuration.toFixed(2)}ms
                                </div>
                              </Col>
                            </Row>

                            {/* Show parameter sets if available */}
                            {(() => {
                              const parameterSets = Array.from(new Set(results.map((r: any) => r.parameter_set_name)));
                              if (parameterSets.length > 1) {
                                return (
                                  <div style={{ marginTop: '8px' }}>
                                    <Text type="secondary">Parameter Sets:</Text>
                                    <div style={{ marginTop: '4px' }}>
                                      {parameterSets.map((paramSet: any) => (
                                        <Tag key={paramSet} color="blue" style={{ marginRight: '4px' }}>
                                          {paramSet}
                                        </Tag>
                                      ))}
                                    </div>
                                  </div>
                                );
                              }
                              return null;
                            })()}
                          </Card>
                        );
                      });
                    })()}
                  </Card>
                )}

              </div>
            )}

            <div style={{ textAlign: 'center' }}>
              <Space>
                <Button onClick={() => setCurrentStep(1)}>
                  Previous
                </Button>
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  onClick={handleRunTest}
                  loading={isTestRunning}
                  size="large"
                >
                  {isTestRunning ? 'Running Test...' : 'Run Concurrency Test'}
                </Button>
              </Space>
            </div>
          </Card>
        )}

      </Form>
    </div>
  );
};

export default ConcurrencyTesting;
>>>>>>> origin/main
