import React from 'react';
import { Card, Typography, Row, Col, Button, Space } from 'antd';
import { 
  QuestionCircleOutlined, 
  PlusOutlined, 
  BulbOutlined,
  ExperimentOutlined
} from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const TBDTab: React.FC = () => {
  return (
    <div style={{ padding: '24px' }}>
      <Card 
        title={
          <span>
            <QuestionCircleOutlined style={{ marginRight: '8px' }} />
            Tab 4 - To Be Determined
          </span>
        }
        className="databricks-card"
        style={{ marginBottom: '24px' }}
      >
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <ExperimentOutlined style={{ fontSize: '64px', color: '#d9d9d9', marginBottom: '24px' }} />
          <Title level={3}>Coming Soon</Title>
          <Paragraph type="secondary" style={{ fontSize: '16px', marginBottom: '32px' }}>
            This tab is reserved for future features and functionality.
          </Paragraph>
          
          <Row gutter={[16, 16]} justify="center">
            <Col span={8}>
              <Card size="small" className="databricks-card" style={{ textAlign: 'center' }}>
                <BulbOutlined style={{ fontSize: '24px', color: '#FF3621', marginBottom: '8px' }} />
                <Title level={5}>Ideas Welcome</Title>
                <Paragraph type="secondary" style={{ fontSize: '12px' }}>
                  Suggest new features or improvements
                </Paragraph>
              </Card>
            </Col>
            
            <Col span={8}>
              <Card size="small" className="databricks-card" style={{ textAlign: 'center' }}>
                <PlusOutlined style={{ fontSize: '24px', color: '#1B3139', marginBottom: '8px' }} />
                <Title level={5}>Expand Functionality</Title>
                <Paragraph type="secondary" style={{ fontSize: '12px' }}>
                  Add new tools and capabilities
                </Paragraph>
              </Card>
            </Col>
            
            <Col span={8}>
              <Card size="small" className="databricks-card" style={{ textAlign: 'center' }}>
                <ExperimentOutlined style={{ fontSize: '24px', color: '#6B7280', marginBottom: '8px' }} />
                <Title level={5}>Experimental</Title>
                <Paragraph type="secondary" style={{ fontSize: '12px' }}>
                  Test new concepts and prototypes
                </Paragraph>
              </Card>
            </Col>
          </Row>
          
          <div style={{ marginTop: '32px' }}>
            <Space size="large">
              <Button type="primary" icon={<BulbOutlined />} className="databricks-primary-btn">
                Suggest Feature
              </Button>
              <Button icon={<PlusOutlined />} className="databricks-secondary-btn">
                Add Content
              </Button>
            </Space>
          </div>
        </div>
      </Card>
      
      <Card title="Potential Features" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col span={12}>
            <Card size="small" title="Query Conversion" type="inner" className="databricks-card">
              <Paragraph type="secondary">
                Convert Databricks SQL queries to Postgres-compatible SQL using AI-powered translation.
              </Paragraph>
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small" title="Performance Testing" type="inner" className="databricks-card">
              <Paragraph type="secondary">
                Run concurrency tests and performance benchmarks on your Lakebase instance.
              </Paragraph>
            </Card>
          </Col>
        </Row>
        
        <Row gutter={[16, 16]} style={{ marginTop: '16px' }}>
          <Col span={12}>
            <Card size="small" title="Migration Tools" type="inner" className="databricks-card">
              <Paragraph type="secondary">
                Additional tools to help with data migration and schema conversion.
              </Paragraph>
            </Card>
          </Col>
          <Col span={12}>
            <Card size="small" title="Monitoring" type="inner" className="databricks-card">
              <Paragraph type="secondary">
                Real-time monitoring and alerting for your Lakebase deployment.
              </Paragraph>
            </Card>
          </Col>
        </Row>
      </Card>
    </div>
  );
};

export default TBDTab;
