import React, { useState } from 'react';
import { Layout, Typography, Tabs } from 'antd';
import 'antd/dist/reset.css';
import './App.css';

import LakebaseOverview from './components/LakebaseOverview';
import LakebaseCalculator from './components/LakebaseCalculator';
import LakebaseDeployment from './components/LakebaseDeployment';
import ConcurrencyTesting from './components/ConcurrencyTesting';
import ConcurrencyTestingPsycopg from './components/ConcurrencyTestingPsycopg';
import PgbenchDatabricks from './components/PgbenchDatabricks';
import TBDTab from './components/TBDTab';
import DatabricksLogo from './components/DatabricksLogo';

const { Header, Content } = Layout;
const { Title } = Typography;

function App() {
  const [generatedConfigs, setGeneratedConfigs] = useState<any>({});
  const [activeTab, setActiveTab] = useState('overview');

  // Load saved configs from localStorage on component mount
  React.useEffect(() => {
    const savedConfigs = localStorage.getItem('generatedConfigs');
    if (savedConfigs) {
      try {
        const configs = JSON.parse(savedConfigs);
        setGeneratedConfigs(configs);
      } catch (error) {
        console.error('Error loading saved configs:', error);
      }
    }
  }, []);

  // Save configs to localStorage whenever they change
  React.useEffect(() => {
    if (Object.keys(generatedConfigs).length > 0) {
      localStorage.setItem('generatedConfigs', JSON.stringify(generatedConfigs));
    }
  }, [generatedConfigs]);

  const handleConfigGenerated = (configs: any) => {
    setGeneratedConfigs(configs);
    // Stay on calculator tab and scroll to results
    setActiveTab('calculator');
    // Scroll to results section after a brief delay to allow rendering
    setTimeout(() => {
      const resultsElement = document.getElementById('cost-results-section');
      if (resultsElement) {
        resultsElement.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    }, 100);
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header className="databricks-header" style={{ padding: '0 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', height: '64px' }}>
          <DatabricksLogo height={32} width={32} className="databricks-logo" />
          <Title level={3} className="databricks-title">
            Lakebase Reverse ETL Accelerator
          </Title>
        </div>
      </Header>

      <Content style={{ padding: '0', background: '#fafafa' }}>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          className="databricks-tabs"
          items={[
            {
              key: 'overview',
              label: 'Lakebase Overview',
              children: <LakebaseOverview />
            },
            {
              key: 'calculator',
              label: 'Lakebase Calculator',
              children: <LakebaseCalculator onConfigGenerated={handleConfigGenerated} />
            },
            {
              key: 'deployment',
              label: 'Lakebase Deployment',
              children: <LakebaseDeployment generatedConfigs={generatedConfigs} />
            },
            {
              key: 'concurrency',
              label: 'Concurrency Testing (pgbench)',
              children: <ConcurrencyTesting />
            },
            {
              key: 'pgbench-databricks',
              label: 'Pgbench in Databricks',
              children: <PgbenchDatabricks />
            },
            {
              key: 'concurrency-databricks',
              label: 'Concurrency Testing (psycopg)',
              children: <ConcurrencyTestingPsycopg />
            },
            {
              key: 'tbd',
              label: 'TBD',
              children: <TBDTab />
            }
          ]}
          style={{ padding: '0 24px', background: 'white', margin: '24px', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.06)' }}
          size="large"
        />
      </Content>
    </Layout>
  );
}

export default App;
