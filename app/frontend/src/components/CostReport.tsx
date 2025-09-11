import React from 'react';
import { Card, Descriptions, Table, Row, Col, Statistic, Tag, Divider, Alert } from 'antd';
import { DollarOutlined, DatabaseOutlined, SyncOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { CostEstimationResult } from '../types';

interface Props {
  data: CostEstimationResult;
}

const CostReport: React.FC<Props> = ({ data }) => {
  const { cost_breakdown, table_sizes, cost_efficiency_metrics, recommendations } = data;

  const tableColumns = [
    {
      title: 'Table Name',
      dataIndex: 'table_name',
      key: 'table_name',
      ellipsis: true,
    },
    {
      title: 'Uncompressed Size (GB)',
      dataIndex: 'uncompressed_size_gb',
      key: 'uncompressed_size_gb',
      render: (value: number) => (typeof value === 'number' ? value.toFixed(2) : '0.00'),
      sorter: (a: any, b: any) => (a.uncompressed_size_gb || 0) - (b.uncompressed_size_gb || 0),
    },
    {
      title: 'Compressed Size (GB)',
      dataIndex: 'compressed_size_gb',
      key: 'compressed_size_gb',
      render: (value: number) => (typeof value === 'number' ? value.toFixed(2) : '0.00'),
      sorter: (a: any, b: any) => (a.compressed_size_gb || 0) - (b.compressed_size_gb || 0),
    },
    {
      title: 'Row Count',
      dataIndex: 'row_count',
      key: 'row_count',
      render: (value: number) => (typeof value === 'number' ? value.toLocaleString() : '0'),
      sorter: (a: any, b: any) => (a.row_count || 0) - (b.row_count || 0),
    },
  ];

  const formatCurrency = (value: number) => `$${value.toFixed(2)}`;

  return (
    <div style={{ display: 'grid', gap: '24px' }}>
      {/* Cost Summary Cards */}
      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic
              title="Total Monthly Cost"
              value={cost_breakdown.total_monthly_cost}
              precision={2}
              prefix={<DollarOutlined />}
              suffix="USD"
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Recommended CU"
              value={cost_breakdown.recommended_cu}
              prefix={<ThunderboltOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Storage Cost"
              value={cost_breakdown.storage_cost}
              precision={2}
              prefix={<DatabaseOutlined />}
              suffix="USD/mo"
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Sync Cost"
              value={cost_breakdown.total_sync_cost}
              precision={2}
              prefix={<SyncOutlined />}
              suffix="USD/mo"
              valueStyle={{ color: '#fa8c16' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Detailed Cost Breakdown */}
      <Card title="Detailed Cost Breakdown">
        <Row gutter={16}>
          <Col span={12}>
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="Bulk CU Required">{cost_breakdown.bulk_cu.toFixed(2)}</Descriptions.Item>
              <Descriptions.Item label="Continuous CU Required">{cost_breakdown.continuous_cu.toFixed(2)}</Descriptions.Item>
              <Descriptions.Item label="Read CU Required">{cost_breakdown.read_cu.toFixed(2)}</Descriptions.Item>
              <Descriptions.Item label="Total CU Required">{cost_breakdown.total_cu.toFixed(2)}</Descriptions.Item>
              <Descriptions.Item label="Recommended CU">
                <Tag color="blue">{cost_breakdown.recommended_cu}</Tag>
              </Descriptions.Item>
            </Descriptions>
          </Col>
          <Col span={12}>
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="Main Instance Cost">{formatCurrency(cost_breakdown.main_instance_cost)}/mo</Descriptions.Item>
              <Descriptions.Item label="Read Secondaries Cost">{formatCurrency(cost_breakdown.readable_secondaries_cost)}/mo</Descriptions.Item>
              <Descriptions.Item label="Total Compute Cost">{formatCurrency(cost_breakdown.total_compute_cost)}/mo</Descriptions.Item>
              <Descriptions.Item label="Continuous Sync Cost">{formatCurrency(cost_breakdown.continuous_sync_cost)}/mo</Descriptions.Item>
              <Descriptions.Item label="Triggered Sync Cost">{formatCurrency(cost_breakdown.triggered_sync_cost)}/mo</Descriptions.Item>
            </Descriptions>
          </Col>
        </Row>
      </Card>

      {/* Cost Efficiency Metrics */}
      {cost_efficiency_metrics && (
        <Card title="Cost Efficiency Metrics">
          <Row gutter={16}>
            <Col span={8}>
              <Statistic
                title="Cost per GB (Monthly)"
                value={cost_efficiency_metrics?.cost_per_gb_monthly || 0}
                precision={3}
                prefix="$"
                suffix="USD/GB"
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Cost per QPS (Monthly)"
                value={cost_efficiency_metrics?.cost_per_qps_monthly || 0}
                precision={3}
                prefix="$"
                suffix="USD/QPS"
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="Cost per CU (Monthly)"
                value={cost_efficiency_metrics?.cost_per_cu_monthly || 0}
                precision={2}
                prefix="$"
                suffix="USD/CU"
              />
            </Col>
          </Row>
        </Card>
      )}

      {/* Table Size Analysis */}
      {table_sizes && (
        <Card title="Table Size Analysis">
          <Row gutter={16} style={{ marginBottom: '16px' }}>
            <Col span={12}>
              <Statistic
                title="Total Uncompressed Size"
                value={table_sizes.total_uncompressed_size_gb}
                precision={2}
                suffix="GB"
                valueStyle={{ color: '#1890ff' }}
              />
            </Col>
            <Col span={12}>
              <Statistic
                title="Total Compressed Size"
                value={table_sizes.total_compressed_size_gb}
                precision={2}
                suffix="GB"
                valueStyle={{ color: '#52c41a' }}
              />
            </Col>
          </Row>
          
          <Divider />
          
          <Table
            columns={tableColumns}
            dataSource={table_sizes.table_details}
            pagination={{ pageSize: 10 }}
            size="small"
            scroll={{ x: true }}
          />
        </Card>
      )}

      {/* Recommendations */}
      {recommendations && recommendations.length > 0 && (
        <Card title="Recommendations">
          {recommendations.map((recommendation, index) => (
            <Alert
              key={index}
              message={recommendation}
              type="info"
              showIcon
              style={{ marginBottom: index < recommendations.length - 1 ? '8px' : '0' }}
            />
          ))}
        </Card>
      )}
    </div>
  );
};

export default CostReport;