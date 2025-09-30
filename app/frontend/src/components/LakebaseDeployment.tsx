import React, { useState } from 'react';
import {
  Card,
  Button,
  message,
  Typography,
  Row,
  Col,
  Alert,
  Divider,
  Space,
  Tag,
  Tooltip,
  Modal,
  Input,
  Spin
} from 'antd';
import {
  RocketOutlined,
  DownloadOutlined,
  FileTextOutlined,
  CopyOutlined,
  CalculatorOutlined,
  EditOutlined,
  SaveOutlined,
  CloseOutlined
} from '@ant-design/icons';
import { WorkloadConfig } from '../types';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;

interface GeneratedConfigs {
  workload_config?: WorkloadConfig;
  synced_tables?: any;
  databricks_config?: any;
  lakebase_instance?: any;
}

interface Props {
  generatedConfigs: GeneratedConfigs;
}

const LakebaseDeployment: React.FC<Props> = ({ generatedConfigs }) => {
  const [deploying, setDeploying] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [isEditing, setIsEditing] = useState(false);
  const [editedContent, setEditedContent] = useState<string>('');
  const [originalContent, setOriginalContent] = useState<string>('');
  const [savedEdits, setSavedEdits] = useState<Record<string, string>>({});
  const [deploymentProgress, setDeploymentProgress] = useState<string>('');
  const [deploymentOutput, setDeploymentOutput] = useState<string>('');
  const [deploymentModalVisible, setDeploymentModalVisible] = useState(false);
  const [deploymentId, setDeploymentId] = useState<string>('');
  const [deploymentSteps, setDeploymentSteps] = useState<any[]>([]);
  const [currentStep, setCurrentStep] = useState<number>(0);

  // Helper to build workspace monitor URL
  const getWorkspaceMonitorUrl = (fallback?: string): string | null => {
    const cfg = (generatedConfigs as any)?.workload_config;
    const raw = (cfg && cfg.databricks_workspace_url) || fallback;
    if (!raw) return null;
    const base = String(raw).replace(/\/$/, '');

    // Get instance name from config
    const instanceName = cfg?.lakebase_instance_name || 'lakebase-accelerator-instance';

    return `${base}/compute/database-instances/${encodeURIComponent(instanceName)}`;
  };

  // Helper function to get file content
  const getFileContent = (filename: string): string => {
    // Check if there are saved edits for this file first
    if (savedEdits[filename]) {
      return savedEdits[filename];
    }

    // Otherwise return original content
    if (filename === 'databricks.yml') {
      const content = generatedConfigs.databricks_config;
      if (content && content.yaml_content) {
        return content.yaml_content;
      } else {
        return JSON.stringify(content, null, 2);
      }
    } else if (filename === 'synced_delta_tables.yml') {
      const content = generatedConfigs.synced_tables;
      if (content && content.yaml_content) {
        return content.yaml_content;
      } else {
        return JSON.stringify(content, null, 2);
      }
    } else if (filename === 'lakebase_instance.yml') {
      const content = generatedConfigs.lakebase_instance;
      if (content && content.yaml_content) {
        return content.yaml_content;
      } else {
        return JSON.stringify(content, null, 2);
      }
    }
    return '';
  };

  // Handle edit mode toggle
  const handleEditToggle = () => {
    if (!isEditing) {
      // Entering edit mode - load the current content (either saved edits or original)
      const content = getFileContent(selectedFile);
      setOriginalContent(content);
      setEditedContent(content);
      setIsEditing(true);
    } else {
      // Cancel editing - revert to original content
      setEditedContent(originalContent);
      setIsEditing(false);
    }
  };

  // Handle save changes
  const handleSaveChanges = () => {
    // Save the edited content to the savedEdits state
    setSavedEdits(prev => ({
      ...prev,
      [selectedFile]: editedContent
    }));

    message.success('Changes saved successfully!');
    setIsEditing(false);
  };

  const handleDeploy = async () => {
    setDeploying(true);
    setDeploymentProgress('Initializing deployment...');
    setDeploymentOutput('🚀 Starting Lakebase deployment using Databricks SDK...\n\n');
    setDeploymentModalVisible(true);

    try {
      // Prepare deployment request with workload config and tables
      const workloadConfig = (generatedConfigs as any)?.workload_config;
      const tablesConfig = (generatedConfigs as any)?.synced_tables;

      if (!workloadConfig) {
        throw new Error('Workload configuration not found. Please generate configuration first.');
      }

      // Extract tables from the configuration - try multiple possible paths
      const tables = tablesConfig?.config_data?.synced_tables ||
        tablesConfig?.synced_tables ||
        workloadConfig?.delta_synchronization?.tables_to_sync ||
        workloadConfig?.delta_synchronization?.tables ||
        (generatedConfigs as any)?.tables ||
        [];

      console.log('Debug - Deployment tables extraction:', { tablesConfig, workloadConfig, tables });

      const deploymentRequest = {
        workload_config: workloadConfig,
        databricks_profile_name: null, // Using default profile/environment variables
        tables: tables
      };

      // Call the new deployment API
      const response = await fetch('http://localhost:8000/api/deploy', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(deploymentRequest)
      });

      const result = await response.json();

      if (response.ok && result.success) {
        setDeploymentProgress('Deployment completed successfully!');
        setDeploymentSteps(result.progress?.steps || []);
        setCurrentStep(result.progress?.current_step || 0);

        let output = '✅ Deployment completed successfully!\n\n';

        if (result.instance) {
          output += `📊 Database Instance Details:\n`;
          output += `  • Name: ${result.instance.name}\n`;
          output += `  • ID: ${result.instance.id || 'Provisioning...'}\n`;
          output += `  • Host: ${result.instance.host || 'Provisioning...'}\n`;
          output += `  • Port: ${result.instance.port || '5432'}\n`;
          output += `  • State: ${result.instance.state || 'Unknown'}\n`;
          output += `  • Capacity: ${result.instance.capacity || 'N/A'}\n\n`;
        }

        if (result.catalog) {
          output += `📁 Unity Catalog:\n`;
          output += `  • Name: ${result.catalog.name}\n\n`;
        }

        if (result.tables && result.tables.length > 0) {
          output += `📋 Synced Tables:\n`;
          result.tables.forEach((table: any) => {
            output += `  • ${table.name} (${table.status})\n`;
          });
        }

        setDeploymentOutput(output);
        message.success('Lakebase instance deployed successfully!');

      } else {
        setDeploymentProgress('Deployment failed!');
        setDeploymentSteps(result.progress?.steps || []);
        setCurrentStep(result.progress?.current_step || 0);

        let errorOutput = `❌ Deployment failed: ${result.message}\n\n`;

        if (result.progress?.error_message) {
          errorOutput += `Error details: ${result.progress.error_message}\n\n`;
        }

        setDeploymentOutput(errorOutput);
        message.error('Deployment failed. Check the output for details.');
      }
    } catch (error) {
      setDeploymentProgress('Deployment failed!');
      setDeploymentOutput(`❌ Error: ${error}\n\nPlease check your Databricks credentials and workspace configuration.`);
      message.error('Failed to deploy. Please check your connection and try again.');
    } finally {
      setDeploying(false);
    }
  };

  const handleDownloadFile = (filename: string, content: any) => {
    let fileContent;

    // If we're currently viewing this file and it's being edited, use edited content
    if (selectedFile === filename && isEditing) {
      fileContent = editedContent;
    } else if (savedEdits[filename]) {
      // Use saved edits if available
      fileContent = savedEdits[filename];
    } else if (typeof content === 'string') {
      fileContent = content;
    } else if (content && content.yaml_content) {
      fileContent = content.yaml_content;
    } else {
      fileContent = JSON.stringify(content, null, 2);
    }

    const blob = new Blob([fileContent], {
      type: 'text/yaml'
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const handleViewFile = (filename: string, content: any) => {
    setSelectedFile(filename);
    setIsEditing(false);
    setEditedContent('');
    setOriginalContent('');
    setModalVisible(true);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    message.success('Copied to clipboard!');
  };

  const hasConfigs = Object.keys(generatedConfigs).length > 0;

  return (
    <div style={{ padding: '24px' }}>
      <Card
        title={
          <span>
            <RocketOutlined style={{ marginRight: '8px' }} />
            Lakebase Deployment
          </span>
        }
        className="databricks-card"
        style={{ marginBottom: '24px' }}
      >
        <Paragraph>
          Choose your preferred deployment method: automatic deployment using the Python SDK for a seamless experience,
          or manual deployment using the Databricks CLI for more control.
        </Paragraph>
      </Card>

      {!hasConfigs ? (
        <Card className="databricks-card">
          <div style={{ textAlign: 'center', padding: '40px' }}>
            <FileTextOutlined style={{ fontSize: '48px', color: '#d9d9d9', marginBottom: '16px' }} />
            <Title level={4}>No Generated Configurations</Title>
            <Paragraph type="secondary">
              Please use the Lakebase Calculator tab to generate cost estimates and configuration files first.
            </Paragraph>
            <Button
              type="primary"
              icon={<CalculatorOutlined />}
              className="databricks-primary-btn"
              onClick={() => message.info('Please use the Lakebase Calculator tab first')}
            >
              Go to Calculator
            </Button>
          </div>
        </Card>
      ) : (
        <div>
          {/* Automatic Deployment Section */}
          <Card title="🤖 Automatic Deployment (Python SDK)" className="databricks-card" style={{ marginBottom: '24px' }}>
            <Paragraph>
              Deploy directly using the Python SDK. This method automatically creates all resources in your Databricks workspace.
            </Paragraph>

            <Alert
              message="Deployment Process"
              description={
                <div>
                  <p>1. <strong>Initialize</strong> → Connect to Databricks workspace</p>
                  <p>2. <strong>Database Instance</strong> → Create Lakebase instance</p>
                  <p>3. <strong>Database Catalog</strong> → Create catalog for data organization</p>
                  <p>4. <strong>Synced Tables</strong> → Create and configure table synchronization</p>
                  <p>5. <strong>Finalize</strong> → Complete deployment and provide connection details</p>
                </div>
              }
              type="info"
              showIcon
              className="databricks-alert"
              style={{ marginBottom: '16px' }}
            />

            {/* Deployment Summary */}
            <Card size="small" title="Deployment Summary" style={{ marginBottom: '16px', backgroundColor: '#fafafa' }}>
              <Row gutter={[16, 8]}>
                <Col span={12}>
                  <Text strong>Databricks Workspace:</Text>
                  <br />
                  <Text>{(generatedConfigs as any)?.workload_config?.databricks_workspace_url || 'Not specified'}</Text>
                </Col>
                <Col span={12}>
                  <Text strong>Authentication:</Text>
                  <br />
                  <Text>Environment variables / Default profile</Text>
                </Col>
                <Col span={12}>
                  <Text strong>Lakebase Instance:</Text>
                  <br />
                  <Text>{(generatedConfigs as any)?.workload_config?.lakebase_instance_name || 'lakebase-accelerator-instance'}</Text>
                </Col>
                <Col span={12}>
                  <Text strong>Database Catalog:</Text>
                  <br />
                  <Text>{(generatedConfigs as any)?.workload_config?.uc_catalog_name || 'lakebase-accelerator-catalog'}</Text>
                </Col>
                <Col span={12}>
                  <Text strong>Database Name:</Text>
                  <br />
                  <Text>{(generatedConfigs as any)?.workload_config?.database_name || 'databricks_postgres'}</Text>
                </Col>
                <Col span={24}>
                  <Text strong>Tables to Sync:</Text>
                  <br />
                  {(() => {
                    // Try multiple possible paths for tables data
                    const tables = (generatedConfigs as any)?.synced_tables?.synced_tables ||
                      (generatedConfigs as any)?.workload_config?.delta_synchronization?.tables_to_sync ||
                      (generatedConfigs as any)?.workload_config?.delta_synchronization?.tables ||
                      (generatedConfigs as any)?.tables ||
                      [];

                    console.log('Debug - generatedConfigs:', generatedConfigs);
                    console.log('Debug - tables found:', tables);

                    if (tables.length === 0) {
                      return <Text type="secondary">No tables configured</Text>;
                    }

                    return (
                      <div>
                        <Text>{tables.length} table{tables.length !== 1 ? 's' : ''}</Text>
                        <div style={{ marginTop: '8px' }}>
                          {tables.slice(0, 3).map((table: any, index: number) => (
                            <Tag key={index} style={{ marginBottom: '4px' }}>
                              {table.table_name || table.name || `Table ${index + 1}`}
                            </Tag>
                          ))}
                          {tables.length > 3 && (
                            <Tag>+{tables.length - 3} more</Tag>
                          )}
                        </div>
                      </div>
                    );
                  })()}
                </Col>
              </Row>
            </Card>

            <Row gutter={[16, 16]}>
              <Col span={24}>
                <Alert
                  message="Ready to Deploy"
                  description="All configuration has been generated. Click the deploy button to start the automatic deployment process."
                  type="success"
                  showIcon
                  className="databricks-alert"
                  style={{ marginBottom: '16px' }}
                />
              </Col>

              <Col span={24}>
                <Button
                  type="primary"
                  size="large"
                  icon={<RocketOutlined />}
                  loading={deploying}
                  onClick={handleDeploy}
                  className="databricks-primary-btn"
                >
                  {deploying ? 'Deploying...' : 'Deploy to Databricks'}
                </Button>
              </Col>
            </Row>
          </Card>

          {/* Manual Deployment Section */}
          <Card title="⚙️ Manual Deployment (Databricks Asset Bundle)" className="databricks-card">
            <Paragraph>
              Download configuration files and deploy manually using the Databricks CLI and Databricks Asset Bundle for more control over the deployment process.
            </Paragraph>

            <Alert
              message="Manual Deployment Steps"
              description={
                <div>
                  <p>1. <strong>Download</strong> → Download the configuration files below</p>
                  <p>2. <strong>Authenticate</strong> → Set up Databricks CLI authentication</p>
                  <p>3. <strong>Deploy</strong> → Run the deployment command in your terminal</p>
                </div>
              }
              type="warning"
              showIcon
              className="databricks-alert"
              style={{ marginBottom: '16px' }}
            />

            {/* Generated Files Section */}
            <Title level={5}>Generated Configuration Files</Title>
            <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
              <Col span={8}>
                <Card
                  size="small"
                  title={`databricks.yml${savedEdits['databricks.yml'] ? ' (Edited)' : ''}`}
                  className="databricks-card"
                  extra={<Tag className="databricks-tag-secondary">Project Root</Tag>}
                  actions={[
                    <Button
                      key="view"
                      type="link"
                      icon={<FileTextOutlined />}
                      onClick={() => handleViewFile('databricks.yml', generatedConfigs.databricks_config)}
                    >
                      View
                    </Button>,
                    <Button
                      key="download"
                      type="link"
                      icon={<DownloadOutlined />}
                      onClick={() => handleDownloadFile('databricks.yml', generatedConfigs.databricks_config)}
                    >
                      Download
                    </Button>
                  ]}
                >
                  <Text type="secondary">Main bundle configuration</Text>
                </Card>
              </Col>

              <Col span={8}>
                <Card
                  size="small"
                  title={`synced_delta_tables.yml${savedEdits['synced_delta_tables.yml'] ? ' (Edited)' : ''}`}
                  className="databricks-card"
                  extra={<Tag className="databricks-tag">resources/</Tag>}
                  actions={[
                    <Button
                      key="view"
                      type="link"
                      icon={<FileTextOutlined />}
                      onClick={() => handleViewFile('synced_delta_tables.yml', generatedConfigs.synced_tables)}
                    >
                      View
                    </Button>,
                    <Button
                      key="download"
                      type="link"
                      icon={<DownloadOutlined />}
                      onClick={() => handleDownloadFile('synced_delta_tables.yml', generatedConfigs.synced_tables)}
                    >
                      Download
                    </Button>
                  ]}
                >
                  <Text type="secondary">Table sync configurations</Text>
                </Card>
              </Col>

              <Col span={8}>
                <Card
                  size="small"
                  title={`lakebase_instance.yml${savedEdits['lakebase_instance.yml'] ? ' (Edited)' : ''}`}
                  className="databricks-card"
                  extra={<Tag className="databricks-tag">resources/</Tag>}
                  actions={[
                    <Button
                      key="view"
                      type="link"
                      icon={<FileTextOutlined />}
                      onClick={() => handleViewFile('lakebase_instance.yml', generatedConfigs.lakebase_instance)}
                    >
                      View
                    </Button>,
                    <Button
                      key="download"
                      type="link"
                      icon={<DownloadOutlined />}
                      onClick={() => handleDownloadFile('lakebase_instance.yml', generatedConfigs.lakebase_instance)}
                    >
                      Download
                    </Button>
                  ]}
                >
                  <Text type="secondary">Lakebase instance definition</Text>
                </Card>
              </Col>
            </Row>

            <Divider />

            <Title level={5}>Deployment Instructions</Title>
            <Card size="small" style={{ backgroundColor: '#f5f5f5', marginBottom: '16px' }}>
              <ol style={{ margin: 0, paddingLeft: '20px' }}>
                <li>Download all configuration files above</li>
                <li>Authenticate with Databricks CLI: <Text code>databricks auth login</Text></li>
                <li>Run the deployment command below</li>
              </ol>
            </Card>

            <Row gutter={[16, 16]}>
              <Col span={24}>
                <Title level={5}>Deployment Command</Title>
                <Card size="small" style={{ backgroundColor: '#f5f5f5' }}>
                  <Space>
                    <Text code>databricks bundle deploy --target dev</Text>
                    <Tooltip title="Copy to clipboard">
                      <Button
                        size="small"
                        icon={<CopyOutlined />}
                        onClick={() => copyToClipboard('databricks bundle deploy --target dev')}
                      />
                    </Tooltip>
                  </Space>
                </Card>
              </Col>
            </Row>
          </Card>
        </div>
      )}

      {/* File Viewer Modal */}
      <Modal
        title={`Viewing ${selectedFile}${savedEdits[selectedFile] ? ' (Edited)' : ''}`}
        open={modalVisible}
        onCancel={() => setModalVisible(false)}
        width={800}
        footer={[
          <Button key="close" onClick={() => setModalVisible(false)}>
            Close
          </Button>,
          <Button
            key="edit"
            icon={isEditing ? <CloseOutlined /> : <EditOutlined />}
            onClick={handleEditToggle}
          >
            {isEditing ? 'Cancel' : 'Edit'}
          </Button>,
          ...(isEditing ? [
            <Button
              key="save"
              type="primary"
              icon={<SaveOutlined />}
              onClick={handleSaveChanges}
            >
              Save
            </Button>
          ] : []),
          <Button
            key="copy"
            icon={<CopyOutlined />}
            onClick={() => {
              const contentToCopy = isEditing ? editedContent : getFileContent(selectedFile);
              copyToClipboard(contentToCopy);
            }}
          >
            Copy
          </Button>
        ]}
      >
        {isEditing ? (
          <TextArea
            value={editedContent}
            onChange={(e) => setEditedContent(e.target.value)}
            style={{
              fontFamily: 'Monaco, Menlo, "Ubuntu Mono", monospace',
              fontSize: '12px',
              minHeight: '400px',
              maxHeight: '500px'
            }}
            placeholder="Edit the configuration file..."
          />
        ) : (
          <pre style={{
            backgroundColor: '#f5f5f5',
            padding: '16px',
            borderRadius: '4px',
            maxHeight: '400px',
            overflow: 'auto',
            fontSize: '12px'
          }}>
            {getFileContent(selectedFile)}
          </pre>
        )}
      </Modal>

      {/* Deployment Progress Modal */}
      <Modal
        title="Deployment Progress"
        open={deploymentModalVisible}
        onCancel={() => setDeploymentModalVisible(false)}
        width={800}
        footer={[
          <Button
            key="close"
            onClick={() => setDeploymentModalVisible(false)}
            disabled={deploying}
          >
            {deploying ? 'Deploying...' : 'Close'}
          </Button>
        ]}
      >
        <div style={{ marginBottom: '16px' }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            marginBottom: '16px',
            padding: '12px',
            backgroundColor: '#f0f0f0',
            borderRadius: '4px'
          }}>
            {deploying ? (
              <Spin size="small" style={{ marginRight: '8px' }} />
            ) : (
              <div style={{
                width: '16px',
                height: '16px',
                borderRadius: '50%',
                backgroundColor: deploymentProgress.includes('successfully') ? '#52c41a' :
                  deploymentProgress.includes('failed') ? '#ff4d4f' :
                    deploymentProgress.includes('downloaded successfully') ? '#52c41a' : '#1890ff',
                marginRight: '8px'
              }} />
            )}
            <span style={{ fontWeight: 'bold' }}>{deploymentProgress}</span>
          </div>

          {/* Monitor link */}
          {(() => {
            const url = getWorkspaceMonitorUrl();
            return url ? (
              <div style={{ marginBottom: '12px' }}>
                <span>Monitor instance status in your workspace: </span>
                <a href={url} target="_blank" rel="noreferrer">{url}</a>
              </div>
            ) : null;
          })()}

          {deploymentOutput && (
            <div>
              <h4>Deployment Output:</h4>
              <pre style={{
                backgroundColor: '#f5f5f5',
                padding: '12px',
                borderRadius: '4px',
                maxHeight: '300px',
                overflow: 'auto',
                fontSize: '11px',
                whiteSpace: 'pre-wrap'
              }}>
                {deploymentOutput}
              </pre>
            </div>
          )}
        </div>
      </Modal>
    </div>
  );
};

export default LakebaseDeployment;
