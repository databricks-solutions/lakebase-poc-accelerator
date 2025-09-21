import React from 'react';
import { Card, Typography, Row, Col, Tag, Alert, Divider, List, Statistic } from 'antd';
import { 
  CheckCircleOutlined, 
  ExclamationCircleOutlined, 
  DatabaseOutlined, 
  ThunderboltOutlined,
  SyncOutlined,
  InfoCircleOutlined
} from '@ant-design/icons';

const { Title, Paragraph, Text } = Typography;

const LakebaseOverview: React.FC = () => {
  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>Lakebase Overview</Title>
      
      {/* Qualifications Section */}
      <Card title="Qualifications" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Alert
              message="Dataset Size Support"
              description="Up to 2TB in Postgres. Be aware that Delta tables are highly compressed on cloud storage, when migrated to Lakebase, the physical table size may increase by 5x-10x."
              type="warning"
              showIcon
              className="databricks-alert"
              style={{ marginBottom: '16px' }}
            />
          </Col>
          <Col span={8}>
            <div className="databricks-stat-card">
              <div className="databricks-stat-value">100k</div>
              <div className="databricks-stat-title">
                <ThunderboltOutlined style={{ marginRight: '4px' }} />
                Throughput (QPS)
              </div>
            </div>
          </Col>
          <Col span={8}>
            <div className="databricks-stat-card">
              <div className="databricks-stat-value">2TB</div>
              <div className="databricks-stat-title">
                <DatabaseOutlined style={{ marginRight: '4px' }} />
                Max Dataset Size
              </div>
            </div>
          </Col>
          <Col span={8}>
            <div className="databricks-stat-card">
              <div className="databricks-stat-value">✓</div>
              <div className="databricks-stat-title">
                <CheckCircleOutlined style={{ marginRight: '4px' }} />
                Reverse ETL
              </div>
            </div>
          </Col>
        </Row>
        
        <Divider />
        
        <List
          header={<Title level={4}>Key Features</Title>}
          dataSource={[
            'Reverse ETL is supported: data can be synchronized from Delta tables to Postgres in Lakebase',
            'Low-latency application serving capabilities',
            'Column with UTF8 encoding is not supported'
          ]}
          renderItem={(item) => (
            <List.Item>
              <CheckCircleOutlined style={{ color: '#52c41a', marginRight: '8px' }} />
              {item}
            </List.Item>
          )}
        />
      </Card>

      {/* Lakebase Performance Section */}
      <Card title="Lakebase Performance" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Alert
              message="Performance Assumptions"
              description="1CU of compute capacity = 16GB Memory and uncompressed 1KB row size"
              type="info"
              showIcon
              className="databricks-alert"
              style={{ marginBottom: '16px' }}
            />
          </Col>
        </Row>
        
        <Row gutter={[16, 16]}>
          <Col span={12}>
            <Card size="small" title="Latency & Connections">
              <List size="small">
                <List.Item>
                  <Text strong>Latency:</Text> <Tag color="green">&lt;10ms</Tag>
                </List.Item>
                <List.Item>
                  <Text strong>Max Connections:</Text> <Tag color="blue">1000</Tag>
                </List.Item>
              </List>
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small" title="Read Performance">
              <List size="small">
                <List.Item>
                  <Text strong>Read QPS:</Text> <Tag color="green">around 10K QPS point lookup</Tag>
                </List.Item>
                <List.Item>
                  <Text strong>Range:</Text> <Text type="secondary">2k-30k QPS depending on data size & cache hit ratio</Text>
                </List.Item>
              </List>
            </Card>
          </Col>
        </Row>
        
        <Row gutter={[16, 16]} style={{ marginTop: '16px' }}>
          <Col span={12}>
            <Card size="small" title="Write Performance (Initial)">
              <Statistic
                value="15k"
                suffix="per 1KB rows/sec per CU"
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small" title="Write Performance (Incremental)">
              <Statistic
                value="1.2k"
                suffix="per 1KB rows/sec per CU"
                valueStyle={{ color: '#722ed1' }}
              />
            </Card>
          </Col>
        </Row>
        
        <Divider />
        
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Statistic
              title="Max Size"
              value="2TB"
              suffix="across all databases"
              prefix={<DatabaseOutlined />}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Instances per Workspace"
              value="10"
              prefix={<ThunderboltOutlined />}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Max Connections per Database"
              value="1000"
              prefix={<SyncOutlined />}
            />
          </Col>
        </Row>
      </Card>

      {/* Delta Sync Section */}
      <Card title="Delta Sync" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Paragraph>
          Delta tables can be synced to Lakebase by 3 modes: Snapshot, Triggered, Continuous.
        </Paragraph>
        
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Card 
              title="Snapshot" 
              size="small"
              className="databricks-card"
              extra={<Tag className="databricks-tag">Most Efficient</Tag>}
            >
              <List size="small">
                <List.Item>
                  <Text strong>Description:</Text> Pipeline runs once to take a snapshot
                </List.Item>
                <List.Item>
                  <Text strong>Efficiency:</Text> 10x more efficient than other modes
                </List.Item>
                <List.Item>
                  <Text strong>Use Case:</Text> When modifying &gt;10% of source table. Work for Views and Materialized Views, or tables without Change Data Feed.
                </List.Item>
              </List>
            </Card>
          </Col>
          <Col span={8}>
            <Card 
              title="Triggered" 
              size="small"
              className="databricks-card"
              extra={<Tag className="databricks-tag-secondary">Manual/Scheduled</Tag>}
            >
              <List size="small">
                <List.Item>
                  <Text strong>Description:</Text> User triggers sync manually or on schedule
                </List.Item>
                <List.Item>
                  <Text strong>Timing:</Text> Preferably after table is updated
                </List.Item>
                <List.Item>
                  <Text strong>Requirement:</Text> Change data feed enabled
                </List.Item>
              </List>
            </Card>
          </Col>
          <Col span={8}>
            <Card 
              title="Continuous" 
              size="small"
              className="databricks-card"
              extra={<Tag className="databricks-tag" style={{ background: '#dc2626' }}>Real-time</Tag>}
            >
              <List size="small">
                <List.Item>
                  <Text strong>Description:</Text> All changes synced continuously
                </List.Item>
                <List.Item>
                  <Text strong>Lag:</Text> Up to 10-15 seconds
                </List.Item>
                <List.Item>
                  <Text strong>Cost:</Text> Can be expensive
                </List.Item>
              </List>
            </Card>
          </Col>
        </Row>
        
        <Divider />
        
        <Alert
          message="Important Notes"
          description={
            <div>
              <p>• Sync mode cannot be changed after pipeline is created - requires table deletion and recreation</p>
              <p>• Change data feed must be enabled for Triggered or Continuous sync modes</p>
              <p>• Certain sources (like Views) do not support change data feed so they can only be synced in Snapshot mode</p>
            </div>
          }
          type="info"
          showIcon
        />
      </Card>
    </div>
  );
};

export default LakebaseOverview;
