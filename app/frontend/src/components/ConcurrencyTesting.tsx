import React, { useState, useCallback } from 'react';
import {
  Card,
  Form,
  Input,
  InputNumber,
  Button,
  Upload,
  Steps,
  Alert,
  Space,
  Typography,
  Divider,
  Row,
  Col,
  Tag,
  message,
  Tooltip,
  Select,
  Switch
} from 'antd';
import {
  UploadOutlined,
  PlayCircleOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  DeleteOutlined
} from '@ant-design/icons';

const { Option } = Select;

const { Title, Text, Paragraph } = Typography;


const ConcurrencyTesting: React.FC = () => {
  const [form] = Form.useForm();
  const [currentStep, setCurrentStep] = useState(0);
  const [uploadedFiles, setUploadedFiles] = useState<Array<{
    name: string;
    content: string;
    parameter_count: number;
    saved_path: string;
  }>>([]);

  const handleDeleteFile = async (index: number, filePath: string) => {
    try {
      // Call backend API to delete the file
      const response = await fetch(`http://localhost:8000/api/pgbench-test/delete-query`, {
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
        fetch(`http://localhost:8000/api/pgbench-test/delete-query`, {
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
  const [isTestRunning, setIsTestRunning] = useState(false);
  const [testResults, setTestResults] = useState<any>(null);
  const [testError, setTestError] = useState<string | null>(null);


  const handleFileUpload = useCallback(async (file: File) => {
    console.log('Starting file upload:', file.name, file.size);
    
    const formData = new FormData();
    formData.append('file', file);

    try {
      console.log('Sending request to http://localhost:8000/api/pgbench-test/upload-query');
      const response = await fetch('http://localhost:8000/api/pgbench-test/upload-query', {
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
        pgbench_connect_per_transaction: formValues.pgbench_connect_per_transaction === true
      };

      console.log('Sending test config:', testConfig);

      const response = await fetch('http://localhost:8000/api/pgbench-test/run-uploaded-tests', {
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
      message.success(`pgbench test completed successfully! Executed ${uploadedFiles.length} uploaded queries.`);
      
    } catch (error) {
      console.error('Test execution error:', error);
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      setTestError(errorMessage);
      message.error(`Test execution failed: ${errorMessage}`);
    } finally {
      setIsTestRunning(false);
    }
  };

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
      <Title level={2}>pgbench Concurrency Testing</Title>
      <Paragraph>
        Test the performance and concurrency of your Lakebase Postgres queries using pgbench with native PostgreSQL benchmarking.
      </Paragraph>

      <Steps current={currentStep} items={steps} style={{ marginBottom: '24px' }} />

      <Form 
        form={form} 
        layout="vertical"
        initialValues={{
          databricks_profile: "DEFAULT",
          instance_name: "ahc-lakebase-instance",
          database_name: "databricks_postgres",
          concurrency_level: 10,
          pgbench_clients: 8,
          pgbench_jobs: 8,
          pgbench_duration: 30,
          pgbench_progress_interval: 5,
          pgbench_protocol: "prepared",
          pgbench_per_statement_latency: true,
          pgbench_detailed_logging: true,
          pgbench_connect_per_transaction: false
        }}
      >
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

        {/* Step 1: Connection Configuration */}
        {currentStep === 0 && (
          <Card title="Connection Setup" style={{ marginBottom: '24px' }}>
            <Row gutter={16}>
              <Col span={12}>
                  <Form.Item
                    label="Databricks Profile Name"
                    name="databricks_profile"
                    rules={[
                      { required: true, message: 'Please enter Databricks CLI profile name' }
                    ]}
                  >
                  <Input
                    placeholder="DEFAULT"
                    suffix={
                      <Tooltip title="Enter your Databricks CLI profile name (e.g., DEFAULT, DEV, PROD)">
                        <InfoCircleOutlined />
                      </Tooltip>
                    }
                  />
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

            <Divider>pgbench Benchmark Settings</Divider>

            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginBottom: '4px' }}>
                  Number of concurrent database sessions
                </Text>
                <Form.Item
                  label={
                    <span>
                      Clients (-c)
                      <Tooltip title="Number of concurrent database clients/connections for pgbench test">
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
                  Worker threads (should be ≤ clients)
                </Text>
                <Form.Item
                  label={
                    <span>
                      Jobs (-j)
                      <Tooltip title="Number of worker threads. Must be ≤ clients; set to match CPU cores.">
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
                  Test duration in seconds
                </Text>
                <Form.Item
                  label={
                    <span>
                      Duration (-T)
                      <Tooltip title="Run benchmark for specified duration in seconds">
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
                  Progress report interval
                </Text>
                <Form.Item
                  label={
                    <span>
                      Progress (-P)
                      <Tooltip title="Show progress reports every N seconds during test">
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
                  Query protocol mode
                </Text>
                <Form.Item
                  label={
                    <span>
                      Protocol (-M)
                      <Tooltip title="Query protocol: prepared is most efficient">
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
                  Per-statement latency reporting
                </Text>
                <Form.Item
                  label={
                    <span>
                      Latency Stats (-r)
                      <Tooltip title="Show detailed per-statement latencies at the end">
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
                  Detailed transaction logging
                </Text>
                <Form.Item
                  label={
                    <span>
                      Log Details (-l)
                      <Tooltip title="Enable per-transaction logging for detailed analysis">
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
                  New connection per transaction
                </Text>
                <Form.Item
                  label={
                    <span>
                      Connect Mode (-C)
                      <Tooltip title="Establish new connection for each transaction (increases overhead)">
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
                      <li>No special comments needed - pgbench handles execution counts via configuration</li>
                      <li>Each file will be weighted equally unless specified in backend</li>
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
                  <Text strong>pgbench Clients</Text>
                  <div style={{ fontSize: '24px', color: '#fa8c16' }}>
                    {form.getFieldValue('pgbench_clients') || 8}
                  </div>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small">
                  <Text strong>Test Duration</Text>
                  <div style={{ fontSize: '24px', color: '#722ed1' }}>
                    {form.getFieldValue('pgbench_duration') || 30}s
                  </div>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small">
                  <Text strong>Protocol Mode</Text>
                  <div style={{ fontSize: '20px', color: '#52c41a', textTransform: 'capitalize' }}>
                    {form.getFieldValue('pgbench_protocol') || 'prepared'}
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
              <Title level={4}>pgbench Test Results</Title>
              
              {/* High-level Test Information */}
              <Card style={{ marginBottom: '16px' }}>
                <Row gutter={16}>
                  <Col span={12}>
                    <div style={{ marginBottom: '8px' }}>
                      <Text strong>pgbench Clients:</Text>
                      <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#1890ff' }}>
                        {testResults.pgbench_config?.clients || 'N/A'}
                      </Text>
                    </div>
                    <div style={{ marginBottom: '8px' }}>
                      <Text strong>Test Duration:</Text>
                      <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#722ed1' }}>
                        {testResults.total_duration_seconds ? testResults.total_duration_seconds.toFixed(1) + 's' : 'N/A'}
                      </Text>
                    </div>
                  </Col>
                  <Col span={12}>
                    <div style={{ marginBottom: '8px' }}>
                      <Text strong>Queries Tested:</Text>
                      <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#52c41a' }}>
                        {testResults.queries_tested || 'N/A'}
                      </Text>
                    </div>
                    <div style={{ marginBottom: '8px' }}>
                      <Text strong>Protocol Mode:</Text>
                      <Text style={{ marginLeft: '8px', fontSize: '16px', color: '#fa8c16', textTransform: 'capitalize' }}>
                        {testResults.pgbench_config?.protocol || 'N/A'}
                      </Text>
                    </div>
                  </Col>
                </Row>
              </Card>

              {/* Performance Metrics */}
              <Row gutter={16} style={{ marginBottom: '16px' }}>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>TPS</Text>
                    <div style={{ fontSize: '20px', color: '#1890ff', marginTop: '4px' }}>
                      {testResults.tps ? testResults.tps.toFixed(2) : 'N/A'}
                    </div>
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>Avg Latency</Text>
                    <div style={{ fontSize: '20px', color: '#fa8c16', marginTop: '4px' }}>
                      {testResults.average_latency_ms ? testResults.average_latency_ms.toFixed(2) + 'ms' : 'N/A'}
                    </div>
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>P95 Latency</Text>
                    <div style={{ fontSize: '20px', color: '#f5222d', marginTop: '4px' }}>
                      {testResults.latency_percentiles?.p95 ? testResults.latency_percentiles.p95.toFixed(2) + 'ms' : 'N/A'}
                    </div>
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Text strong>P99 Latency</Text>
                    <div style={{ fontSize: '20px', color: '#722ed1', marginTop: '4px' }}>
                      {testResults.latency_percentiles?.p99 ? testResults.latency_percentiles.p99.toFixed(2) + 'ms' : 'N/A'}
                    </div>
                  </Card>
                </Col>
              </Row>

              {/* pgbench Raw Output */}
              {testResults.execution_result && (
                <Card title="pgbench Execution Summary" style={{ marginBottom: '16px' }}>
                  <Row gutter={16}>
                    <Col span={12}>
                      <div style={{ marginBottom: '16px' }}>
                        <Text strong>Execution Details:</Text>
                        <div style={{ marginTop: '8px', backgroundColor: '#f5f5f5', padding: '8px', borderRadius: '4px' }}>
                          <Text type="secondary">Return Code:</Text>
                          <Text style={{ marginLeft: '8px', color: testResults.execution_result.return_code === 0 ? '#52c41a' : '#f5222d' }}>
                            {testResults.execution_result.return_code === 0 ? 'Success' : `Failed (${testResults.execution_result.return_code})`}
                          </Text>
                        </div>
                      </div>

                      {testResults.per_statement_stats && Object.keys(testResults.per_statement_stats).length > 0 && (
                        <div>
                          <Text strong>Per-Statement Statistics:</Text>
                          <div style={{ marginTop: '8px' }}>
                            {Object.entries(testResults.per_statement_stats).map(([statement, stats]: [string, any]) => (
                              <div key={statement} style={{ marginBottom: '8px', backgroundColor: '#fafafa', padding: '8px', borderRadius: '4px' }}>
                                <Text type="secondary">{statement}:</Text>
                                <Text style={{ marginLeft: '8px', fontWeight: 'bold' }}>
                                  {stats.average_latency_ms?.toFixed(2)}ms
                                </Text>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </Col>

                    <Col span={12}>
                      {testResults.progress_reports && testResults.progress_reports.length > 0 && (
                        <div>
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

                    </Col>
                  </Row>
                </Card>
              )}

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
                {isTestRunning ? 'Running pgbench Test...' : 'Run pgbench Test'}
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
