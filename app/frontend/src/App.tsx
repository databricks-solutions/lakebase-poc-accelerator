import React, { useState } from 'react';
import { Layout, Typography, Steps, Card, message } from 'antd';
import 'antd/dist/reset.css';
import './App.css';

import ConfigurationForm from './components/ConfigurationForm';
import CostReport from './components/CostReport';
import FileDownloads from './components/FileDownloads';
import { WorkloadConfig, CostEstimationResult } from './types';

const { Header, Content } = Layout;
const { Title, Text } = Typography;
const { Step } = Steps;

function App() {
  const [currentStep, setCurrentStep] = useState(0);
  const [workloadConfig, setWorkloadConfig] = useState<WorkloadConfig | null>(null);
  const [costReport, setCostReport] = useState<CostEstimationResult | null>(null);
  const [generatedConfigs, setGeneratedConfigs] = useState<any>({});
  const [loading, setLoading] = useState(false);

  const handleConfigurationSubmit = async (config: WorkloadConfig) => {
    setLoading(true);
    try {
      setWorkloadConfig(config);
      
      // Call cost estimation API
      const response = await fetch('http://localhost:8000/api/estimate-cost', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          workload_config: config,
          calculate_table_sizes: true
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const costData = await response.json();
      setCostReport(costData);

      // Generate configuration files
      const [syncedTablesResponse, databricksConfigResponse, lakebaseInstanceResponse] = await Promise.all([
        fetch('http://localhost:8000/api/generate-synced-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config)
        }),
        fetch('http://localhost:8000/api/generate-databricks-config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config)
        }),
        fetch('http://localhost:8000/api/generate-lakebase-instance', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config)
        })
      ]);

      const [syncedTables, databricksConfig, lakebaseInstance] = await Promise.all([
        syncedTablesResponse.json(),
        databricksConfigResponse.json(),
        lakebaseInstanceResponse.json()
      ]);

      setGeneratedConfigs({
        workload_config: config,
        synced_tables: syncedTables,
        databricks_config: databricksConfig,
        lakebase_instance: lakebaseInstance
      });

      setCurrentStep(1);
      message.success('Configuration processed successfully!');
    } catch (error) {
      message.error(`Error processing configuration: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  const steps = [
    {
      title: 'Configuration',
      description: 'Enter workload parameters'
    },
    {
      title: 'Results',
      description: 'View cost estimation and download configs'
    }
  ];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ background: '#fff', padding: '0 24px', borderBottom: '1px solid #f0f0f0' }}>
        <div style={{ display: 'flex', alignItems: 'center', height: '64px' }}>
          <Title level={3} style={{ margin: 0, color: '#1890ff' }}>
            Databricks Lakebase Accelerator
          </Title>
        </div>
      </Header>
      
      <Content style={{ padding: '24px' }}>
        <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
          <Card style={{ marginBottom: '24px' }}>
            <Text type="secondary">
              Configure your OLTP workload parameters to estimate Lakebase costs and generate deployment configurations.
            </Text>
          </Card>

          <Steps current={currentStep} items={steps} style={{ marginBottom: '32px' }} />

          {currentStep === 0 && (
            <ConfigurationForm
              onSubmit={handleConfigurationSubmit}
              loading={loading}
            />
          )}

          {currentStep === 1 && costReport && (
            <div style={{ display: 'grid', gap: '24px' }}>
              <CostReport data={costReport} />
              <FileDownloads configs={generatedConfigs} />
            </div>
          )}
        </div>
      </Content>
    </Layout>
  );
}

export default App;
