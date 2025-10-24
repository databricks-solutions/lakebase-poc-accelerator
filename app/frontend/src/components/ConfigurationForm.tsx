import React, { useState } from 'react';
import {
  Form,
  Input,
  InputNumber,
  Button,
  Card,
  Row,
  Col,
  Select,
  Table,
  message,
  Tooltip
} from 'antd';
import { PlusOutlined, DeleteOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { WorkloadConfig, TableToSync } from '../types';

const { Option } = Select;

interface Props {
  onSubmit: (config: WorkloadConfig) => void;
  loading: boolean;
}

const ConfigurationForm: React.FC<Props> = ({ onSubmit, loading }) => {
  const [form] = Form.useForm();
  const [tables, setTables] = useState<TableToSync[]>([
    {
      id: crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-0`,
      name: 'samples.tpcds_sf1.customer',
      primary_keys: ['c_customer_sk'],
      scheduling_policy: 'SNAPSHOT'
    }
  ]);

  const addTable = () => {
    setTables([...tables, {
      id: crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${tables.length}`,
      name: '',
      primary_keys: [],
      scheduling_policy: 'SNAPSHOT'
    }]);
  };

  const removeTable = (index: number) => {
    const newTables = tables.filter((_, i) => i !== index);
    setTables(newTables);
  };

  const updateTable = (index: number, field: keyof TableToSync, value: any) => {
    const newTables = [...tables];
    newTables[index] = { ...newTables[index], [field]: value };
    setTables(newTables);
  };

  const handleSubmit = (values: any) => {
    if (tables.length === 0) {
      message.error('Please add at least one table to sync');
      return;
    }

    if (tables.some(table => !table.name || table.primary_keys.length === 0)) {
      message.error('Please complete all table configurations');
      return;
    }

    const config: WorkloadConfig = {
      database_instance: {
        bulk_writes_per_second: values.bulk_writes_per_second,
        continuous_writes_per_second: values.continuous_writes_per_second,
        reads_per_second: values.reads_per_second,
        number_of_readable_secondaries: values.number_of_readable_secondaries || 0,
        readable_secondary_size_cu: values.readable_secondary_size_cu || 1,
        promotion_percentage: values.promotion_percentage || 0
      },
      database_storage: {
        data_stored_gb: values.data_stored_gb,
        estimated_data_deleted_daily_gb: values.estimated_data_deleted_daily_gb || 0,
        restore_windows_days: values.restore_windows_days || 0
      },
      delta_synchronization: {
        number_of_continuous_pipelines: values.number_of_continuous_pipelines || 0,
        expected_data_per_sync_gb: values.expected_data_per_sync_gb || 0,
        sync_mode: values.sync_mode || 'SNAPSHOT',
        sync_frequency: values.sync_frequency || 'Per day',
        tables_to_sync: tables
      },
      databricks_workspace_url: values.databricks_workspace_url,
      warehouse_http_path: values.warehouse_http_path,
      databricks_profile_name: values.databricks_profile || 'DEFAULT',
      lakebase_instance_name: values.lakebase_instance_name || 'lakebase-accelerator-instance',
      uc_catalog_name: values.uc_catalog_name || 'lakebase-accelerator-catalog',
      database_name: values.database_name || 'databricks_postgres',
      storage_catalog: values.storage_catalog || 'main',
      storage_schema: values.storage_schema || 'default'
    };

    onSubmit(config);
  };

  const tableColumns = [
    {
      title: 'Delta Table Name',
      dataIndex: 'name',
      render: (text: string, record: TableToSync, index: number) => (
        <Input
          placeholder="e.g., samples.tpcds_sf1.customer"
          value={text}
          onChange={(e) => updateTable(index, 'name', e.target.value)}
          style={{ minWidth: '250px' }}
        />
      )
    },
    {
      title: 'Primary Keys',
      dataIndex: 'primary_keys',
      render: (keys: string[], record: TableToSync, index: number) => (
        <Select
          mode="tags"
          placeholder="Enter primary key columns"
          value={keys}
          onChange={(value) => updateTable(index, 'primary_keys', value)}
          style={{ minWidth: '200px' }}
          tokenSeparators={[',']}
        />
      )
    },
    {
      title: 'Sync Policy',
      dataIndex: 'scheduling_policy',
      render: (policy: string, record: TableToSync, index: number) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Select
            value={policy}
            onChange={(value) => updateTable(index, 'scheduling_policy', value)}
            style={{ width: '120px' }}
          >
            <Option value="SNAPSHOT">Snapshot</Option>
            <Option value="TRIGGERED">
              Triggered
            </Option>
            <Option value="CONTINUOUS">
              Continuous
            </Option>
          </Select>
          {(policy === 'TRIGGERED' || policy === 'CONTINUOUS') && (
            <Tooltip
              title={
                <div>
                  <div style={{ marginBottom: '8px' }}>
                    <strong>{policy}</strong> mode requires Change Data Feed (CDF) enabled on the source table.
                  </div>
                  <div style={{ marginBottom: '8px' }}>
                    <strong>For sample tables, Views and Materialized Views:</strong> Only SNAPSHOT mode works (sample tables are read-only).
                  </div>
                  <div>
                    <strong>For your tables:</strong> Run this command first:
                    <br />
                    <code style={{
                      fontSize: '11px',
                      backgroundColor: '#1f1f1f',
                      color: '#fff',
                      padding: '4px 6px',
                      borderRadius: '3px',
                      display: 'block',
                      marginTop: '4px'
                    }}>
                      ALTER TABLE {record.name}<br />
                      SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
                    </code>
                  </div>
                </div>
              }
              placement="topLeft"
              overlayStyle={{ maxWidth: '400px' }}
            >
              <InfoCircleOutlined
                style={{
                  color: '#faad14',
                  cursor: 'help',
                  fontSize: '16px'
                }}
              />
            </Tooltip>
          )}
        </div>
      )
    },
    {
      title: 'Action',
      render: (_: any, record: TableToSync, index: number) => (
        <Button
          type="link"
          danger
          icon={<DeleteOutlined />}
          onClick={() => removeTable(index)}
          disabled={tables.length <= 1}
        />
      )
    }
  ];

  return (
    <Form
      form={form}
      layout="vertical"
      onFinish={handleSubmit}
      className="databricks-form"
      initialValues={{
        bulk_writes_per_second: 1000,
        continuous_writes_per_second: 0,
        reads_per_second: 1000,
        number_of_readable_secondaries: 1,
        readable_secondary_size_cu: 1,
        promotion_percentage: 50,
        data_stored_gb: 100,
        estimated_data_deleted_daily_gb: 0,
        restore_windows_days: 0,
        number_of_continuous_pipelines: 0,
        expected_data_per_sync_gb: 20,
        sync_mode: 'TRIGGERED',
        sync_frequency: 'Per day'
        // Removed Databricks Configuration initialValues to use placeholders instead
      }}
    >
      {/* Database Instance Configuration */}
      <Card title="Database Instance Configuration" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={16}>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Bulk Writes rows/second{' '}
                  <Tooltip title="Writes for initial data loading and batch updates">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="bulk_writes_per_second"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Continuous Writes rows/second{' '}
                  <Tooltip title="Real-time writes for ongoing operations">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="continuous_writes_per_second"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Reads rows/second{' '}
                  <Tooltip title="Read operations for queries and lookups">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="reads_per_second"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={16}>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Readable Secondaries{' '}
                  <Tooltip title="Number of read replica instances">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="number_of_readable_secondaries"
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Secondary Size (CU){' '}
                  <Tooltip title="Compute units for each read replica">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="readable_secondary_size_cu"
            >
              <InputNumber min={1} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Promotion Discount (%){' '}
                  <Tooltip title="Promotional discount percentage">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="promotion_percentage"
            >
              <InputNumber min={0} max={100} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>
      </Card>

      {/* Database Storage Configuration */}
      <Card title="Database Storage Configuration" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={16}>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Data Stored (GB){' '}
                  <Tooltip title="Total data size including all tables and indexes">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="data_stored_gb"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Daily Data Deleted (GB){' '}
                  <Tooltip title="Daily data cleanup and archiving">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="estimated_data_deleted_daily_gb"
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Restore Windows (Days){' '}
                  <Tooltip title="Data recovery retention period">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="restore_windows_days"
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>
      </Card>

      {/* Continuous Sync Configuration */}
      <Card title="Continuous Sync Configuration" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Continuous Pipelines{' '}
                  <Tooltip title="Number of real-time sync pipelines">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="number_of_continuous_pipelines"
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>
      </Card>

      {/* Batch Sync Configuration */}
      <Card title="Batch Sync Configuration" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={16}>
          <Col span={8}>
            <Form.Item
              label={
                <span>
                  Data per Sync (GB){' '}
                  <Tooltip title="Expected data volume per sync operation">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="expected_data_per_sync_gb"
            >
              <InputNumber min={0} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label="Sync Mode"
              name="sync_mode"
            >
              <Select>
                <Option value="SNAPSHOT">Snapshot</Option>
                <Option value="TRIGGERED">Triggered</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label="Sync Frequency"
              name="sync_frequency"
            >
              <Input placeholder="e.g., Per day, Per hour" />
            </Form.Item>
          </Col>
        </Row>
      </Card>

      {/* Tables to Sync */}
      <Card
        title="Tables to Sync"
        className="databricks-card"
        extra={
          <Button
            type="dashed"
            onClick={addTable}
            icon={<PlusOutlined />}
            className="databricks-secondary-btn"
          >
            Add Table
          </Button>
        }
        style={{ marginBottom: '24px' }}
      >

        <Table
          columns={tableColumns}
          dataSource={tables}
          pagination={false}
          rowKey={(record) => (record.id ?? `table-${record.name}-${Math.random()}`)}
          size="small"
          className="databricks-table"
        />
      </Card>

      {/* Databricks Configuration */}
      <Card title="Databricks Configuration" className="databricks-card" style={{ marginBottom: '24px' }}>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Databricks Workspace URL{' '}
                  <Tooltip title="Your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="databricks_workspace_url"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <Input placeholder="https://your-workspace.cloud.databricks.com" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Warehouse HTTP Path{' '}
                  <Tooltip title="SQL warehouse HTTP path for table size calculation (e.g., /sql/1.0/warehouses/your-warehouse-id)">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="warehouse_http_path"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <Input placeholder="/sql/1.0/warehouses/your-warehouse-id" />
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Databricks Profile Name{' '}
                  <Tooltip title="[Not required if run on Databricks Apps] Databricks CLI profile used for authentication. This should match the profile configured on your machine and align with the Databricks Workspace URL above.">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="databricks_profile"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <Input placeholder="DEFAULT"/>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="Lakebase Instance Name"
              name="lakebase_instance_name"
            >
              <Input placeholder="lakebase-accelerator-instance"/>
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="UC Catalog Name"
              name="uc_catalog_name"
            >
              <Input placeholder="lakebase_accelerator_catalog" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="Database Name"
              name="database_name"
            >
              <Input placeholder="databricks_postgres"/>
            </Form.Item>
          </Col>
        </Row>
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Storage Catalog{' '}
                  <Tooltip title="Unity Catalog where synced table data will be stored during processing">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="storage_catalog"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <Input placeholder="main" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={
                <span>
                  Storage Schema{' '}
                  <Tooltip title="Schema within the storage catalog where synced table data will be stored during processing">
                    <InfoCircleOutlined />
                  </Tooltip>
                </span>
              }
              name="storage_schema"
              rules={[{ required: true, message: 'Required field' }]}
            >
              <Input placeholder="default" />
            </Form.Item>
          </Col>
        </Row>
      </Card>

      <Form.Item>
        <Button
          type="primary"
          htmlType="submit"
          loading={loading}
          size="large"
          className="databricks-primary-btn"
          style={{ width: '200px' }}
        >
          {loading ? 'Processing...' : 'Generate Cost Estimate'}
        </Button>
      </Form.Item>
    </Form>
  );
};

export default ConfigurationForm;