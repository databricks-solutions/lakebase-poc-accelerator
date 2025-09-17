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
  
  // Helper to build workspace monitor URL
  const getWorkspaceMonitorUrl = (fallback?: string): string | null => {
    const cfg = (generatedConfigs as any)?.workload_config;
    const raw = (cfg && cfg.databricks_workspace_url) || fallback;
    if (!raw) return null;
    const base = String(raw).replace(/\/$/, '');
    return `${base}/compute/database-instances`;
  };

  // Helper function to get file content
  const getFileContent = (filename: string): string => {
    // Check if there are saved edits for this file first
    if (savedEdits[filename]) {
      return savedEdits[filename];
    }
    
    // Otherwise return original content
    if (filename === 'databricks.yml') {
      return JSON.stringify(generatedConfigs.databricks_config, null, 2);
    } else if (filename === 'synced_delta_tables.yml') {
      return JSON.stringify(generatedConfigs.synced_tables, null, 2);
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
    setDeploymentProgress('Deploying via Databricks CLI...');
    setDeploymentOutput('Running: databricks bundle deploy --force --auto-approve\n\n');
    setDeploymentModalVisible(true);
    
    try {
      // Keep progress focused on CLI deployment; files are saved server-side
      setDeploymentProgress('Deploying via Databricks CLI...');
      
      // Call the real deployment API with generated configurations
      const response = await fetch('http://localhost:8000/api/deploy', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          generatedConfigs: generatedConfigs
        })
      });

      setDeploymentProgress('Processing deployment response...');
      const result = await response.json();

      if (response.ok && result.success) {
        setDeploymentProgress('Deployment completed successfully!');
        let output = result.output || 'No output available';
        
        // Add saved files information to output
        if (result.saved_files && result.saved_files.length > 0) {
          output = `Files saved:\n${result.saved_files.map((f: string) => `- ${f}`).join('\n')}\n\n` + output;
        }
        
        setDeploymentOutput(output);
        message.success(`Deployment completed successfully! Lakebase instance is now available at ${result.workspace_url}`);
        
        if (result.stderr) {
          setDeploymentOutput(prev => prev + '\n\nWarnings:\n' + result.stderr);
          console.warn('Deployment warnings:', result.stderr);
        }
      } else {
        setDeploymentProgress('Deployment failed!');
        let errorOutput = result.stderr || result.message || 'Unknown error';
        
        // Add saved files information even if deployment failed
        if (result.saved_files && result.saved_files.length > 0) {
          errorOutput = `Files saved before deployment:\n${result.saved_files.map((f: string) => `- ${f}`).join('\n')}\n\n` + errorOutput;
        }
        
        setDeploymentOutput(errorOutput);
        message.error(`Deployment failed: ${result.message || 'Unknown error'}`);
        console.error('Deployment error:', result);
      }
    } catch (error) {
      setDeploymentProgress('Deployment failed!');
      setDeploymentOutput(`Network error: ${error}`);
      message.error(`Deployment failed: ${error}`);
      console.error('Deployment error:', error);
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
          Deploy your Lakebase instance and synced tables using the generated Databricks bundle configurations.
          The deployment process will create the necessary YAML files and deploy them to your Databricks workspace.
        </Paragraph>
        
        <Alert
          message="Deployment Process"
          description={
            <div>
              <p>1. <strong>databricks.yml</strong> → Project root</p>
              <p>2. <strong>synced_delta_tables.yml</strong> → resources/ directory</p>
              <p>3. <strong>lakebase_instance.yml</strong> → resources/ directory</p>
              <p>4. Deploy using: <code>databricks bundle deploy --target dev</code></p>
            </div>
          }
          type="info"
          showIcon
          className="databricks-alert"
          style={{ marginBottom: '16px' }}
        />
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
          {/* Generated Files Section */}
          <Card title="Generated Configuration Files" className="databricks-card" style={{ marginBottom: '24px' }}>
            <Row gutter={[16, 16]}>
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
          </Card>

          {/* Deployment Section */}
          <Card title="Deployment Actions" className="databricks-card">
            <Row gutter={[16, 16]}>
              <Col span={24}>
                <Alert
                  message="Ready to Deploy"
                  description="All configuration files have been generated. Click the deploy button to start the deployment process."
                  type="success"
                  showIcon
                  className="databricks-alert"
                  style={{ marginBottom: '16px' }}
                />
              </Col>
              
              <Col span={24}>
                <Space size="large">
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
                  
                  <Button 
                    size="large"
                    icon={<DownloadOutlined />}
                    className="databricks-secondary-btn"
                    onClick={() => {
                      handleDownloadFile('databricks.yml', generatedConfigs.databricks_config);
                      handleDownloadFile('synced_delta_tables.yml', generatedConfigs.synced_tables);
                      handleDownloadFile('lakebase_instance.yml', generatedConfigs.lakebase_instance);
                    }}
                  >
                    Download All Files
                  </Button>
                </Space>
              </Col>
            </Row>
            
            <Divider />
            
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
                                deploymentProgress.includes('failed') ? '#ff4d4f' : '#1890ff',
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
