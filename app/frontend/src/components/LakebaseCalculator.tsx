import React, { useState } from 'react';
import { Card, message, Spin } from 'antd';
import { CalculatorOutlined } from '@ant-design/icons';
import ConfigurationForm from './ConfigurationForm';
import CostReport from './CostReport';
import { WorkloadConfig, CostEstimationResult } from '../types';

interface Props {
  onConfigGenerated?: (configs: any) => void;
}

const LakebaseCalculator: React.FC<Props> = ({ onConfigGenerated }) => {
  const [costReport, setCostReport] = useState<CostEstimationResult | null>(null);
  const [loading, setLoading] = useState(false);

  const handleConfigurationSubmit = async (config: WorkloadConfig) => {
    setLoading(true);
    try {

      // Call cost estimation API
      const response = await fetch('/api/estimate-cost', {
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

      // Extract recommended CU from cost data
      const recommendedCu = costData.cost_breakdown?.recommended_cu || 1;

      // Generate configuration files
      const [syncedTablesResponse, databricksConfigResponse, lakebaseInstanceResponse] = await Promise.all([
        fetch('/api/generate-synced-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config)
        }),
        fetch('/api/generate-databricks-config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config)
        }),
        fetch('/api/generate-lakebase-instance', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...config,
            recommended_cu: recommendedCu
          })
        })
      ]);

      const [syncedTables, databricksConfig, lakebaseInstance] = await Promise.all([
        syncedTablesResponse.json(),
        databricksConfigResponse.json(),
        lakebaseInstanceResponse.json()
      ]);

      const generatedConfigs = {
        workload_config: config,
        cost_report: costData,
        synced_tables: syncedTables,
        databricks_config: databricksConfig,
        lakebase_instance: lakebaseInstance
      };

      // Notify parent component about generated configs
      if (onConfigGenerated) {
        onConfigGenerated(generatedConfigs);
      }

      message.success('Configuration processed successfully!');
    } catch (error) {
      message.error(`Error processing configuration: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <Card
        title={
          <span>
            <CalculatorOutlined style={{ marginRight: '8px' }} />
            Lakebase Cost Calculator
          </span>
        }
        className="databricks-card"
        style={{ marginBottom: '24px' }}
      >
        <p>
          Configure your OLTP workload parameters to estimate Lakebase costs and analyze table sizes.
          Fill out the form below and click "Generate Cost Estimate" to get detailed cost analysis.
        </p>
      </Card>

      <ConfigurationForm
        onSubmit={handleConfigurationSubmit}
        loading={loading}
      />

      {loading && (
        <Card className="databricks-card" style={{ marginTop: '24px', textAlign: 'center' }}>
          <Spin size="large" className="databricks-spinner" />
          <p style={{ marginTop: '16px' }}>Processing configuration and calculating costs...</p>
        </Card>
      )}

      {costReport && !loading && (
        <div id="cost-results-section" style={{ marginTop: '24px' }}>
          <CostReport data={costReport} />
        </div>
      )}
    </div>
  );
};

export default LakebaseCalculator;
