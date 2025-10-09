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
  Divider,
  Row,
  Col,
  Tag,
  message,
  Tooltip,
  Select,
  Switch,
  Collapse,
  Radio
} from 'antd';
import {
  UploadOutlined,
  PlayCircleOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  DeleteOutlined,
  DatabaseOutlined,
  ClusterOutlined,
  SettingOutlined
} from '@ant-design/icons';

const { Option } = Select;
const { Panel } = Collapse;

const { Title, Text, Paragraph } = Typography;

interface QueryConfig {
  name: string;
  content: string;
  weight: number;
}


const ConcurrencyTestingPgbench: React.FC = () => {
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

  // Predefined queries for pgbench
  const [queryConfigs, setQueryConfigs] = useState<QueryConfig[]>([
    {
      name: 'point',
      weight: 60,
      content: `\\set c_customer_sk random(0, 999)
SELECT *
FROM databricks_postgres.public.customer
WHERE c_customer_sk = :c_customer_sk;`
    },
    {
      name: 'range',
      weight: 30,
      content: `\\set c_current_hdemo_sk random(1, 700)
SELECT count(*)
FROM databricks_postgres.public.customer
WHERE c_current_hdemo_sk BETWEEN :c_current_hdemo_sk AND :c_current_hdemo_sk + 1000;`
    },
    {
      name: 'agg',
      weight: 10,
      content: `SELECT c_preferred_cust_flag, count(*)
FROM databricks_postgres.public.customer
GROUP BY c_preferred_cust_flag;`
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
      { name: `query_${queryConfigs.length + 1}`, content: '', weight: 10 }
    ]);
  };

  const removeQueryConfig = (index: number) => {
    if (queryConfigs.length > 1) {
      setQueryConfigs(queryConfigs.filter((_, i) => i !== index));
    }
  };

  const handleDeleteFile = async (index: number, filePath: string) => {
    try {
      // Call backend API to delete the file
      const response = await fetch(`/api/pgbench-test/delete-query`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ file_path: filePath }),
      });

      if (response.ok) {
        // Remove from frontend state
        setUploadedFiles(prev => prev.filter((_, i) => i !== index));
        message.success('File deleted successfully');
      } else {
        const error = await response.json();
        message.error(`Failed to delete file: ${error.detail}`);
      }
    } catch (error) {
      console.error('Delete file error:', error);
      message.error('Failed to delete file');
    }
  };

  const handleClearAllFiles = async () => {
    try {
      // Delete all files from backend
      const deletePromises = uploadedFiles.map(file =>
        fetch(`/api/pgbench-test/delete-query`, {
          method: 'DELETE',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ file_path: file.saved_path }),
        })
      );

      await Promise.all(deletePromises);
      setUploadedFiles([]);
      message.success('All files cleared successfully');
    } catch (error) {
      console.error('Clear all files error:', error);
      message.error('Failed to clear all files');
    }
  };

  const handleFileUpload = useCallback(async (file: File) => {
    console.log('Starting file upload:', file.name, file.size);

    const formData = new FormData();
    formData.append('file', file);

    try {
      console.log('Sending request to /api/pgbench-test/upload-query');
      const response = await fetch('/api/pgbench-test/upload-query', {
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

      // Add to uploaded files list (pgbench format)
      setUploadedFiles(prev => [...prev, {
        name: result.query_identifier,
        content: result.query_content,
        parameter_count: result.variable_count || 0,
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
      const requiredFields = ['databricks_profile', 'workspace_url', 'instance_name', 'concurrency_level'];
      const missingFields = requiredFields.filter(field => !formValues[field]);

      console.log('Missing fields check:', missingFields);

      if (missingFields.length > 0) {
        message.error(`Please fill in the following required fields: ${missingFields.join(', ')}`);
        return;
      }

      if (uploadedFiles.length === 0) {
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
        // pgbench configuration
        pgbench_clients: formValues.pgbench_clients || 8,
        pgbench_jobs: formValues.pgbench_jobs || 8,
        pgbench_duration: formValues.pgbench_duration || 30,
        pgbench_progress_interval: formValues.pgbench_progress_interval || 5,
        pgbench_protocol: formValues.pgbench_protocol || 'prepared',
        pgbench_per_statement_latency: formValues.pgbench_per_statement_latency !== false,
        pgbench_detailed_logging: formValues.pgbench_detailed_logging !== false,
        pgbench_connect_per_transaction: formValues.pgbench_connect_per_transaction === true,
        // Query configuration
        query_source: querySource,
        predefined_queries: querySource === 'predefined' ? queryConfigs : undefined,
        uploaded_files: querySource === 'upload' ? uploadedFiles : undefined
      };

      console.log('Sending test config:', testConfig);

      // Choose the appropriate endpoint based on query source
      const endpoint = querySource === 'predefined'
        ? '/api/pgbench-test/run-predefined-tests'
        : '/api/pgbench-test/run-uploaded-tests';

      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(testConfig),
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
        const errorData = await response.json();
        console.error('Error response:', errorData);
        const errorMessage = errorData.detail || 'Test execution failed';
        setTestError(errorMessage);
        message.error(`Test execution failed: ${errorMessage}`);
        return; // Don't throw, just return to show error in UI
      }

      const results = await response.json();
      console.log('Test results:', results);
      setTestResults(results);
      setTestError(null); // Clear any previous errors on success
      const queryCount = querySource === 'predefined' ? queryConfigs.length : uploadedFiles.length;
      message.success(`pgbench test completed successfully! Executed ${queryCount} ${querySource} queries.`);

    } catch (error) {
      console.error('Test execution error:', error);
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      setTestError(errorMessage);
      message.error(`Test execution failed: ${errorMessage}`);
    } finally {
      setIsTestRunning(false);
    }
  };


  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <Title level={2}>Pgbench on Local Machine</Title>
      <Alert
        message="Local Machine Required"
        description="This pgbench testing method must be run on your local machine with postgres-client installed. To run on Databricks Apps, go to 'Concurrency Testing (pgbench on Databricks Apps)' tab."
        type="warning"
        showIcon
        style={{ marginBottom: '16px' }}
      />
      <Paragraph>
        <strong>pgbench</strong> is PostgreSQL's built-in benchmarking tool that provides industry-standard database performance testing.
        It simulates realistic database workloads by running multiple concurrent client sessions against your Lakebase Postgres database.
      </Paragraph>


      <Form
        form={form}
        layout="vertical"
        initialValues={{
          databricks_profile: "DEFAULT",
          instance_name: "lakebase-accelerator-instance",
          database_name: "databricks_postgres",
          concurrency_level: 10,
          pgbench_clients: 8,
          pgbench_jobs: 8,
          pgbench_duration: 30,
          pgbench_progress_interval: 5,
          pgbench_protocol: "prepared",
          pgbench_per_statement_latency: true,
          pgbench_detailed_logging: false,
          pgbench_connect_per_transaction: false
        }}
      >
        <Collapse defaultActiveKey={['connection', 'queries', 'settings']} style={{ marginBottom: '24px' }}>
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
          <Form.Item name="pgbench_clients" style={{ display: 'none' }}>
            <InputNumber />
          </Form.Item>
          <Form.Item name="pgbench_jobs" style={{ display: 'none' }}>
            <InputNumber />
          </Form.Item>
          <Form.Item name="pgbench_duration" style={{ display: 'none' }}>
            <InputNumber />
          </Form.Item>
          <Form.Item name="pgbench_progress_interval" style={{ display: 'none' }}>
            <InputNumber />
          </Form.Item>
          <Form.Item name="pgbench_protocol" style={{ display: 'none' }}>
            <Select />
          </Form.Item>
          <Form.Item name="pgbench_per_statement_latency" style={{ display: 'none' }}>
            <Switch />
          </Form.Item>
          <Form.Item name="pgbench_detailed_logging" style={{ display: 'none' }}>
            <Switch />
          </Form.Item>
          <Form.Item name="pgbench_connect_per_transaction" style={{ display: 'none' }}>
            <Switch />
          </Form.Item>

          {/* Connection Configuration Panel */}
          <Panel
            header={
              <span>
                <ClusterOutlined style={{ marginRight: '8px' }} />
                Connection Configuration
              </span>
            }
            key="connection"
          >
            <Row gutter={16}>
              <Col span={12}>
                <Form.Item
                  label={
                    <span>
                      Databricks Profile Name
                      <Tooltip title="[Not required if run on Databricks Apps] Databricks CLI profile used for authentication. This should match the profile configured on your machine and align with the Databricks Workspace URL below.">
                        <InfoCircleOutlined style={{ marginLeft: '4px', color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="databricks_profile"
                  rules={[
                    { required: true, message: 'Please enter Databricks CLI profile name, not required for Databricks Apps' }
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



            <Divider>pgbench Benchmark Settings</Divider>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Number of concurrent database sessions (Recommended: 8-50, Max: 100)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Clients (-c)
                      <Tooltip title="Number of concurrent database clients/connections for pgbench test. Higher values simulate more users but may overwhelm the system. Recommended: 8-50 for most tests, 100+ for stress testing.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_clients"
                >
                  <InputNumber min={1} max={1000} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Worker threads (should be ≤ clients, Recommended: 4-8)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Jobs (-j)
                      <Tooltip title="Number of worker threads on client machine that manage the clients. Should be ≤ clients and typically match your CPU cores. Recommended: 4-8 for most systems, or match your CPU core count.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_jobs"
                >
                  <InputNumber min={1} max={100} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Test duration in seconds (Recommended: 30-60 for quick tests)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Duration (-T)
                      <Tooltip title="How long to run the benchmark test. Longer tests provide more stable results but take more time. Recommended: 30-60 seconds for quick tests, 300+ seconds for production-like testing.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_duration"
                >
                  <InputNumber min={1} max={3600} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
            </Row>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Progress report interval (Recommended: 5-10 seconds)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Progress (-P)
                      <Tooltip title="How often to show progress updates during the test. More frequent updates provide better monitoring but add overhead. Recommended: 5-10 seconds for most tests, 1-2 seconds for short tests.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_progress_interval"
                >
                  <InputNumber min={1} max={60} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Query protocol mode (Recommended: Prepared)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Protocol (-M)
                      <Tooltip title="Query execution protocol. Prepared is most efficient for repeated queries, Extended is good for complex queries, Simple is basic but slower. Recommended: Prepared for most use cases.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_protocol"
                >
                  <Select style={{ width: '100%' }}>
                    <Option value="prepared">Prepared</Option>
                    <Option value="extended">Extended</Option>
                    <Option value="simple">Simple</Option>
                  </Select>
                </Form.Item>
              </Col>
            </Row>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Per-statement latency reporting (Recommended: ON)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Latency Stats (-r)
                      <Tooltip title="Show detailed per-statement latency statistics at the end of the test. Provides min/max/avg latency for each query type. Recommended: ON for detailed analysis, OFF for basic throughput testing.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_per_statement_latency"
                  valuePropName="checked"
                >
                  <Switch />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Detailed transaction logging (Recommended: OFF, use ON for debugging)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Log Details (-l)
                      <Tooltip title="Enable detailed per-transaction logging to a file for in-depth analysis. Creates detailed logs but may impact performance. Recommended: ON for debugging, OFF for performance testing.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_detailed_logging"
                  valuePropName="checked"
                >
                  <Switch />
                </Form.Item>
              </Col>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  New connection per transaction (Recommended: OFF)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Connect Mode (-C)
                      <Tooltip title="Establish a new database connection for each transaction instead of reusing connections. Simulates real-world scenarios but adds significant overhead. Recommended: OFF for performance testing, ON for connection stress testing.">
                        <InfoCircleOutlined style={{ marginLeft: 8, color: '#1890ff' }} />
                      </Tooltip>
                    </span>
                  }
                  name="pgbench_connect_per_transaction"
                  valuePropName="checked"
                >
                  <Switch />
                </Form.Item>
              </Col>
            </Row>

          </Panel>

          {/* Query Configuration Panel */}
          <Panel
            header={
              <span>
                <DatabaseOutlined style={{ marginRight: '8px' }} />
                Query Configuration
              </span>
            }
            key="queries"
          >
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
                      await fetch('/api/pgbench-test/clear-temp-files', { method: 'POST' });
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
              <div style={{ marginBottom: '24px' }}>
                <Title level={4}>Predefined Queries</Title>
                <Paragraph>
                  Use predefined pgbench queries or create custom ones. These queries will be used for the pgbench test.
                </Paragraph>
                {queryConfigs.map((config, index) => (
                  <Card key={index} size="small" style={{ marginBottom: '16px' }}>
                    <Row gutter={16} align="middle">
                      <Col span={4}>
                        <Form.Item label="Query Name">
                          <Input
                            value={config.name}
                            onChange={(e) => updateQueryConfig(index, 'name', e.target.value)}
                            placeholder="query_name"
                          />
                        </Form.Item>
                      </Col>
                      <Col span={2}>
                        <Form.Item label="Weight">
                          <InputNumber
                            value={config.weight}
                            onChange={(value) => updateQueryConfig(index, 'weight', value || 10)}
                            min={1}
                            max={100}
                            style={{ width: '100%' }}
                          />
                        </Form.Item>
                      </Col>
                      <Col span={16}>
                        <Form.Item label="SQL Content">
                          <Input.TextArea
                            value={config.content}
                            onChange={(e) => updateQueryConfig(index, 'content', e.target.value)}
                            placeholder="\\set variable_name random(min, max)&#10;SELECT * FROM table WHERE column = :variable_name;"
                            rows={4}
                          />
                        </Form.Item>
                      </Col>
                      <Col span={2}>
                        <Button
                          type="text"
                          danger
                          icon={<DeleteOutlined />}
                          onClick={() => removeQueryConfig(index)}
                          disabled={queryConfigs.length === 1}
                        />
                      </Col>
                    </Row>
                  </Card>
                ))}
                <Button type="dashed" onClick={addQueryConfig} style={{ width: '100%' }}>
                  Add Query
                </Button>
              </div>
            )}

            {querySource === 'upload' && (
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
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                    <Title level={4} style={{ margin: 0 }}>Uploaded Files</Title>
                    {uploadedFiles.length > 0 && (
                      <Button
                        size="small"
                        danger
                        onClick={handleClearAllFiles}
                        title="Clear all files"
                      >
                        Clear All
                      </Button>
                    )}
                  </div>
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
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                              <Tag color="blue">{file.parameter_count} params</Tag>
                              <Button
                                type="text"
                                danger
                                size="small"
                                icon={<DeleteOutlined />}
                                onClick={() => handleDeleteFile(index, file.saved_path)}
                                title="Delete file"
                              />
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
                    message="pgbench SQL File Format"
                    description={
                      <div>
                        <p><strong>⚠️ IMPORTANT:</strong> Use pgbench format with these features:</p>
                        <ul style={{ marginBottom: '8px' }}>
                          <li><code>\set variable_name random(min, max)</code> for random parameters</li>
                          <li><code>:variable_name</code> for parameter placeholders (colon notation)</li>
                          <li>Add -- weight: N comment to specify query weight (default: 1).</li>
                        </ul>
                      </div>
                    }
                    type="warning"
                    style={{ marginBottom: '16px' }}
                  />

                  <Row gutter={16}>
                    <Col span={12}>
                      <Title level={5}>Example 1: Point Query</Title>
                      <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                        File: point_query.sql
                      </Text>
                      <pre style={{
                        backgroundColor: '#f5f5f5',
                        padding: '12px',
                        borderRadius: '4px',
                        fontSize: '12px',
                        marginBottom: '8px',
                        overflow: 'auto'
                      }}>
                        {`\\set customer_sk random(1, 1000000)

SELECT * FROM customer
WHERE c_customer_sk = :customer_sk;`}
                      </pre>
                      <Text type="secondary" style={{ fontSize: '11px' }}>
                        Random customer lookup by primary key using pgbench \\set
                      </Text>
                    </Col>

                    <Col span={12}>
                      <Title level={5}>Example 2: Range Query</Title>
                      <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                        File: range_query.sql
                      </Text>
                      <pre style={{
                        backgroundColor: '#f5f5f5',
                        padding: '12px',
                        borderRadius: '4px',
                        fontSize: '12px',
                        marginBottom: '8px',
                        overflow: 'auto'
                      }}>
                        {`\\set customer_sk random(1, 1000000)

SELECT * FROM customer
WHERE c_customer_sk BETWEEN :customer_sk AND :customer_sk + 1000;`}
                      </pre>
                      <Text type="secondary" style={{ fontSize: '11px' }}>
                        Range query with random start point and fixed range size
                      </Text>
                    </Col>
                  </Row>

                  <Row gutter={16} style={{ marginTop: '16px' }}>
                    <Col span={24}>
                      <Title level={5}>Example 3: Aggregation Query</Title>
                      <Text code style={{ fontSize: '12px', display: 'block', marginBottom: '8px' }}>
                        File: agg_query.sql
                      </Text>
                      <pre style={{
                        backgroundColor: '#f5f5f5',
                        padding: '12px',
                        borderRadius: '4px',
                        fontSize: '12px',
                        marginBottom: '8px',
                        overflow: 'auto'
                      }}>
                        {`SELECT COUNT(*) as total_customers FROM customer;`}
                      </pre>
                      <Text type="secondary" style={{ fontSize: '11px' }}>
                        Simple aggregation query without parameters for baseline performance
                      </Text>
                    </Col>
                  </Row>
                </Card>
              </div>
            )}

            {/* Test Execution Section */}
            <div style={{ marginTop: '24px', textAlign: 'center' }}>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleRunTest}
                loading={isTestRunning}
                size="large"
              >
                {isTestRunning ? 'Running pgbench Test...' : 'Run pgbench Test'}
              </Button>
            </div>

            {/* Test Results Display */}
            {testError && (
              <Alert
                message="Test Execution Error"
                description={testError}
                type="error"
                showIcon
                style={{ marginTop: '16px' }}
              />
            )}

            {testResults && (
              <div style={{ marginTop: '16px' }}>
                <Card title="Test Results" style={{ marginBottom: '16px' }}>
                  <Row gutter={16}>
                    <Col span={12}>
                      <div style={{ textAlign: 'center', marginBottom: '16px' }}>
                        <Text strong style={{ fontSize: '16px' }}>Transactions per Second (TPS)</Text>
                        <div style={{ fontSize: '32px', color: '#1890ff', fontWeight: 'bold' }}>
                          {testResults.tps?.toFixed(2) || 'N/A'}
                        </div>
                      </div>
                    </Col>
                    <Col span={12}>
                      <div style={{ textAlign: 'center', marginBottom: '16px' }}>
                        <Text strong style={{ fontSize: '16px' }}>Average Latency</Text>
                        <div style={{ fontSize: '32px', color: '#fa8c16', fontWeight: 'bold' }}>
                          {testResults.average_latency_ms?.toFixed(2) || 'N/A'}ms
                        </div>
                      </div>
                    </Col>
                  </Row>

                  <Row gutter={16}>
                    <Col span={8}>
                      <div style={{ textAlign: 'center' }}>
                        <Text strong>Total Transactions</Text>
                        <div style={{ fontSize: '20px', color: '#722ed1' }}>
                          {testResults.total_transactions || 'N/A'}
                        </div>
                      </div>
                    </Col>
                    <Col span={8}>
                      <div style={{ textAlign: 'center' }}>
                        <Text strong>Test Duration</Text>
                        <div style={{ fontSize: '20px', color: '#52c41a' }}>
                          {testResults.duration_seconds?.toFixed(1) || 'N/A'}s
                        </div>
                      </div>
                    </Col>
                    <Col span={8}>
                      <div style={{ textAlign: 'center' }}>
                        <Text strong>Connections</Text>
                        <div style={{ fontSize: '20px', color: '#f5222d' }}>
                          {testResults.connections || 'N/A'}
                        </div>
                      </div>
                    </Col>
                  </Row>

                  {/* Detailed Statistics */}
                  {testResults.statement_statistics && (
                    <div style={{ marginTop: '24px' }}>
                      <Text strong>Statement Statistics:</Text>
                      <div style={{ marginTop: '8px' }}>
                        {Object.entries(testResults.statement_statistics).map(([statement, stats]: [string, any]) => (
                          <div key={statement} style={{ marginBottom: '4px', fontSize: '12px' }}>
                            <Text type="secondary">{statement}:</Text>
                            <Text style={{ marginLeft: '8px', fontWeight: 'bold' }}>
                              {stats.average_latency_ms?.toFixed(2)}ms
                            </Text>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Progress Reports */}
                  {testResults.progress_reports && testResults.progress_reports.length > 0 && (
                    <div style={{ marginTop: '16px' }}>
                      <Text strong>Progress Reports:</Text>
                      <div style={{ marginTop: '8px', maxHeight: '200px', overflowY: 'auto' }}>
                        {testResults.progress_reports.map((report: any, index: number) => (
                          <div key={index} style={{ marginBottom: '4px', fontSize: '12px', color: '#666' }}>
                            <Text>{report.time_seconds?.toFixed(1)}s: </Text>
                            <Text style={{ color: '#1890ff' }}>{report.tps?.toFixed(1)} TPS</Text>
                            {report.latency_ms && (
                              <Text style={{ color: '#fa8c16' }}>, {report.latency_ms?.toFixed(2)}ms latency</Text>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </Card>

                {/* Show raw pgbench output for debugging */}
                {testResults.execution_result?.stdout && (
                  <Card title="Raw pgbench Output" style={{ marginBottom: '16px' }} size="small">
                    <div style={{ backgroundColor: '#f5f5f5', padding: '12px', borderRadius: '4px', fontSize: '12px', fontFamily: 'monospace', maxHeight: '300px', overflowY: 'auto' }}>
                      <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                        {testResults.execution_result.stdout}
                      </pre>
                    </div>
                    {testResults.execution_result.stderr && (
                      <div style={{ marginTop: '8px', backgroundColor: '#fff2f0', padding: '8px', borderRadius: '4px', fontSize: '12px', fontFamily: 'monospace' }}>
                        <Text strong style={{ color: '#f5222d' }}>STDERR:</Text>
                        <pre style={{ margin: 0, marginTop: '4px', whiteSpace: 'pre-wrap', color: '#f5222d' }}>
                          {testResults.execution_result.stderr}
                        </pre>
                      </div>
                    )}
                  </Card>
                )}
              </div>
            )}

          </Panel>
        </Collapse>
      </Form>
    </div>
  );
};

export default ConcurrencyTestingPgbench;
