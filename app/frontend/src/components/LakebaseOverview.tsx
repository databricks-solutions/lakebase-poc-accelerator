import React from 'react';
import { Card, Typography, Row, Col, Tag, Alert, Divider, List, Statistic, Tooltip, Collapse } from 'antd';
import {
  CheckCircleOutlined,
  DatabaseOutlined,
  ThunderboltOutlined,
  SyncOutlined
} from '@ant-design/icons';
import { useTheme } from '../contexts/ThemeContext';
import { createStyledComponents } from '../styles/theme';

const { Title, Paragraph, Text } = Typography;

const LakebaseOverview: React.FC = () => {
  const { theme } = useTheme();
  const styled = createStyledComponents(theme);

  return (
    <div style={styled.pageContainer}>
      {/* Background pattern */}
      <div style={styled.backgroundPattern} />

      <Title level={2} style={styled.pageTitle}>
        Lakebase Overview
      </Title>
      <Alert
        message="Official Documentation"
        description={
          <div>
            <Text style={{ color: theme.colors.textSecondary }}>
              Please refer to the official Databricks documentation for the most up-to-date details on Lakebase database instances.
            </Text>
            <div style={{ marginTop: 8 }}>
              <a
                href="https://docs.databricks.com/aws/en/oltp/instances/instance"
                target="_blank"
                rel="noreferrer"
                style={styled.link}
              >
                Database instance overview →
              </a>
            </div>
          </div>
        }
        type="info"
        showIcon
        style={styled.alert}
      />

      {/* Qualifications Section */}
      <Card
        title={<span style={styled.sectionTitle}>Qualifications</span>}
        style={styled.mainCard}
      >
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <div style={styled.statCard(theme.colors.accent1)}>
              <div style={styled.statValue(theme.colors.accent1)}>✓</div>
              <div style={styled.statTitle}>
                <CheckCircleOutlined style={{ marginRight: '6px', color: theme.colors.accent1 }} />
                Reverse ETL
              </div>
            </div>
          </Col>
          <Col span={8}>
            <div style={styled.statCard(theme.colors.accent2)}>
              <div style={styled.statValue(theme.colors.accent2)}>10</div>
              <div style={styled.statTitle}>
                <ThunderboltOutlined style={{ marginRight: '6px', color: theme.colors.accent2 }} />
                Instances per Workspace
              </div>
            </div>
          </Col>
          <Col span={8}>
            <div style={styled.statCard(theme.colors.accent3)}>
              <div style={styled.statValue(theme.colors.accent3)}>100k</div>
              <div style={styled.statTitle}>
                <ThunderboltOutlined style={{ marginRight: '6px', color: theme.colors.accent3 }} />
                Throughput (QPS)
              </div>
            </div>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginTop: '16px' }}>
          <Col span={12}>
            <div style={styled.statCard(theme.colors.accent4)}>
              <div style={styled.statValue(theme.colors.accent4)}>1,000</div>
              <div style={styled.statTitle}>
                <SyncOutlined style={{ marginRight: '6px', color: theme.colors.accent4 }} />
                Max Connections per Instance
              </div>
            </div>
          </Col>
          <Col span={12}>
            <div style={{ ...styled.statCard(theme.colors.accent5), cursor: 'help' }}>
              <div style={styled.statValue(theme.colors.accent5)}>2TB</div>
              <div style={styled.statTitle}>
                <DatabaseOutlined style={{ marginRight: '6px', color: theme.colors.accent5 }} />
                Max Dataset Size
              </div>
            </div>
          </Col>
        </Row>

        <Divider style={styled.divider} />

        <List
          header={<Title level={4} style={{ color: theme.colors.text, marginBottom: '16px', textAlign: 'left' }}>Key Features</Title>}
          dataSource={[
            'Synchronize data from Delta tables (data lake) to Managed Postgres databases in Lakebase, enables your front-end applications and APIs to access real-time data from your data lake through a high-performance Postgres database, bridging the gap between analytical data processing and operational application needs',
            'Low-latency application serving capabilities with sub-10ms response times for front-end applications for point lookups queries',
            'Support up to 1000 connections per instance'
          ]}
          renderItem={(item) => (
            <List.Item style={{ ...styled.listItem, textAlign: 'left' }}>
              <CheckCircleOutlined style={{ color: theme.colors.primary, marginRight: '12px', fontSize: '16px' }} />
              <span style={{ lineHeight: '1.6', textAlign: 'left' }}>{item}</span>
            </List.Item>
          )}
        />
      </Card>


      {/* Delta Sync Section */}
      <Card
        title={<span style={styled.sectionTitle}>Delta Sync</span>}
        style={styled.mainCard}
      >
        <Paragraph style={{ color: theme.colors.textSecondary }}>
          <Text strong style={{ color: theme.colors.text }}>Delta Sync Modes:</Text> Delta tables (data lake) can be synced to Lakebase by 3 modes: Snapshot, Triggered, Continuous.
        </Paragraph>
        <Paragraph style={{ color: theme.colors.textSecondary }}>
          <Text strong style={{ color: theme.colors.text }}>Dataset Size Support:</Text>
          Up to 2TB in Postgres. Be aware that Delta tables are highly compressed on cloud storage, when migrated to Lakebase, the physical table size may increase by 5x-10x
        </Paragraph>
        <Paragraph style={{ color: theme.colors.textSecondary }}>
          <Text>
            For additional instructions, see the official Databricks guide:
            {' '}
            <a
              href="https://docs.databricks.com/aws/en/oltp/instances/sync-data/sync-table"
              target="_blank"
              rel="noreferrer"
              style={styled.link}
            >
              Sync data from Unity Catalog tables to a database instance
            </a>.
          </Text>
        </Paragraph>

        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Card
              title={<span style={{ color: theme.colors.text }}>Triggered</span>}
              size="small"
              style={styled.syncCard}
              extra={<Tag style={styled.tag(theme.colors.success)}>Incremental, Recommended</Tag>}
            >
              <List size="small">
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Description:</Text> User triggers sync manually or on schedule to incrementally sync changes.
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Use Case:</Text> Recommended for tables with Change Data Feed, and updated frequently.
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Requirement:</Text> Delta Tables with Change data feed enabled
                </List.Item>
              </List>
            </Card>
          </Col>
          <Col span={8}>
            <Card
              title={<span style={{ color: theme.colors.text }}>Snapshot</span>}
              size="small"
              style={styled.syncCard}
              extra={<Tag style={styled.tag(theme.colors.warning)}>Efficient but sync full table</Tag>}
            >
              <List size="small">
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Description:</Text> Pipeline take a snapshot of the entire table everytime it runs
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Efficiency:</Text> 10x more efficient than other modes, but sync full table everytime
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Use Case:</Text> When modifying &gt;10% of source table. Work for Views and Materialized Views, or tables without Change Data Feed. For tables with Change Data Feed, use Triggered mode instead.
                </List.Item>
              </List>
            </Card>
          </Col>
          <Col span={8}>
            <Card
              title={<span style={{ color: theme.colors.text }}>Continuous</span>}
              size="small"
              style={styled.syncCard}
              extra={<Tag style={styled.tag(theme.colors.error)}>Real-time</Tag>}
            >
              <List size="small">
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Description:</Text> All changes synced continuously
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Lag:</Text> Up to 10-15 seconds
                </List.Item>
                <List.Item style={{ color: theme.colors.textSecondary }}>
                  <Text strong style={{ color: theme.colors.text }}>Cost:</Text> Can be expensive as pipeline runs continuously, but provide real-time data access
                </List.Item>
              </List>
            </Card>
          </Col>
        </Row>

        <Divider style={styled.divider} />

        <Alert
          message="Important Notes"
          description={
            <div style={{ color: theme.colors.textSecondary }}>
              <p>• Sync mode <strong style={{ color: theme.colors.text }}>cannot be changed</strong> after pipeline is created - requires table deletion and recreation</p>
              <p>• Change data feed <strong style={{ color: theme.colors.text }}>must be enabled</strong> for Triggered or Continuous sync modes</p>
              <p>• Certain sources (like Views) <strong style={{ color: theme.colors.text }}>do not support change data feed</strong> so they can only be synced in Snapshot mode</p>
            </div>
          }
          type="info"
          showIcon
          style={styled.alert}
        />
      </Card>

      {/* Lakebase Performance (Advanced) - Collapsible Section */}
      <Collapse
        items={[
          {
            key: '1',
            label: <Text strong style={{ color: theme.colors.text }}>Lakebase Performance (Advanced)</Text>,
            children: (
              <div>
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
                          <Text strong>Latency:</Text> <Tag color="green" style={{ color: '#000000' }}>&lt;10ms</Tag>
                        </List.Item>
                        <List.Item>
                          <Text strong>Max Connections:</Text> <Tag color="blue" style={{ color: '#000000' }}>1000</Tag>
                        </List.Item>
                      </List>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small" title="Read Performance">
                      <List size="small">
                        <List.Item>
                          <Text strong>Read QPS:</Text> <Tag color="green" style={{ color: '#000000' }}>around 10K QPS point lookup</Tag>
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
              </div>
            ),
          },
        ]}
        style={styled.collapse}
      />
    </div>
  );
};

export default LakebaseOverview;
