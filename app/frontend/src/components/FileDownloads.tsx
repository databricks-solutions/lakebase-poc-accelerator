import React from 'react';
import { Card, Button, Space, Typography, Row, Col, Descriptions } from 'antd';
import { DownloadOutlined, FileTextOutlined, CloudOutlined, DatabaseOutlined, SettingOutlined } from '@ant-design/icons';
import yaml from 'js-yaml';

const { Title, Text, Paragraph } = Typography;

interface Props {
  configs: {
    workload_config?: any;
    synced_tables?: any;
    databricks_config?: any;
    lakebase_instance?: any;
  };
}

const FileDownloads: React.FC<Props> = ({ configs }) => {
  const downloadFile = (content: string, filename: string, contentType: string = 'text/plain') => {
    const blob = new Blob([content], { type: contentType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const downloadWorkloadConfig = () => {
    if (!configs.workload_config) return;
    const yamlContent = yaml.dump(configs.workload_config, { indent: 2 });
    downloadFile(yamlContent, 'workload_config.yml', 'text/yaml');
  };

  const downloadSyncedTables = () => {
    if (!configs.synced_tables) return;
    const yamlContent = yaml.dump(configs.synced_tables, { indent: 2 });
    downloadFile(yamlContent, 'synced_delta_tables.yml', 'text/yaml');
  };

  const downloadDatabricksConfig = () => {
    if (!configs.databricks_config) return;
    const yamlContent = yaml.dump(configs.databricks_config, { indent: 2 });
    downloadFile(yamlContent, 'databricks.yml', 'text/yaml');
  };

  const downloadLakebaseInstance = () => {
    if (!configs.lakebase_instance) return;
    const yamlContent = yaml.dump(configs.lakebase_instance, { indent: 2 });
    downloadFile(yamlContent, 'lakebase_instance.yml', 'text/yaml');
  };

  const downloadAllConfigs = () => {
    // Create a zip-like structure by downloading all files
    if (configs.workload_config) downloadWorkloadConfig();
    if (configs.synced_tables) downloadSyncedTables();
    if (configs.databricks_config) downloadDatabricksConfig();
    if (configs.lakebase_instance) downloadLakebaseInstance();
  };

  return (
    <Card title="Download Configuration Files">
      <Paragraph type="secondary">
        Download the generated configuration files to deploy your Lakebase instance and sync tables.
      </Paragraph>

      <Row gutter={16}>
        <Col span={12}>
          <Card size="small" style={{ height: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                <FileTextOutlined style={{ marginRight: '8px', color: '#1890ff' }} />
                <Text strong>Workload Configuration</Text>
              </div>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Contains your workload parameters and table sync definitions
              </Text>
              <Button
                type="default"
                icon={<DownloadOutlined />}
                onClick={downloadWorkloadConfig}
                disabled={!configs.workload_config}
                block
              >
                workload_config.yml
              </Button>
            </Space>
          </Card>
        </Col>

        <Col span={12}>
          <Card size="small" style={{ height: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                <CloudOutlined style={{ marginRight: '8px', color: '#52c41a' }} />
                <Text strong>Databricks Bundle</Text>
              </div>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Main Databricks asset bundle configuration
              </Text>
              <Button
                type="default"
                icon={<DownloadOutlined />}
                onClick={downloadDatabricksConfig}
                disabled={!configs.databricks_config}
                block
              >
                databricks.yml
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginTop: '16px' }}>
        <Col span={12}>
          <Card size="small" style={{ height: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                <DatabaseOutlined style={{ marginRight: '8px', color: '#722ed1' }} />
                <Text strong>Synced Tables</Text>
              </div>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Table synchronization configuration for Delta to Postgres
              </Text>
              <Button
                type="default"
                icon={<DownloadOutlined />}
                onClick={downloadSyncedTables}
                disabled={!configs.synced_tables}
                block
              >
                synced_delta_tables.yml
              </Button>
            </Space>
          </Card>
        </Col>

        <Col span={12}>
          <Card size="small" style={{ height: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                <SettingOutlined style={{ marginRight: '8px', color: '#fa8c16' }} />
                <Text strong>Lakebase Instance</Text>
              </div>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Lakebase instance resource configuration
              </Text>
              <Button
                type="default"
                icon={<DownloadOutlined />}
                onClick={downloadLakebaseInstance}
                disabled={!configs.lakebase_instance}
                block
              >
                lakebase_instance.yml
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>

      <div style={{ marginTop: '24px', textAlign: 'center' }}>
        <Button
          type="primary"
          size="large"
          icon={<DownloadOutlined />}
          onClick={downloadAllConfigs}
          disabled={!configs.workload_config}
        >
          Download All Configuration Files
        </Button>
      </div>

      <div style={{ marginTop: '24px', padding: '16px', backgroundColor: '#f6f8fa', borderRadius: '6px' }}>
        <Title level={5}>Next Steps:</Title>
        <ol style={{ margin: 0, paddingLeft: '20px' }}>
          <li><Text>Place the downloaded files in your project's appropriate directories</Text></li>
          <li><Text code>databricks.yml</Text> → project root</li>
          <li><Text code>synced_delta_tables.yml</Text> → <Text code>resources/</Text> directory</li>
          <li><Text code>lakebase_instance.yml</Text> → <Text code>resources/</Text> directory</li>
          <li><Text>Deploy using: <Text code>databricks bundle deploy --target dev</Text></Text></li>
        </ol>
      </div>
    </Card>
  );
};

export default FileDownloads;