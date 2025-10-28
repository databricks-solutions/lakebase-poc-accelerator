import React, { useState } from 'react';
import { Layout, Typography, Tabs } from 'antd';
import 'antd/dist/reset.css';
import './App.css';

import { ThemeProvider, useTheme } from './contexts/ThemeContext';
import { createStyledComponents } from './styles/theme';
import LakebaseOverview from './components/LakebaseOverview';
import LakebaseCalculator from './components/LakebaseCalculator';
import LakebaseDeployment from './components/LakebaseDeployment';
<<<<<<< HEAD
import ConcurrencyTestingPsycopg from './components/ConcurrencyTestingPsycopg';
import PgbenchDatabricks from './components/PgbenchDatabricks';
=======
import ConcurrencyTesting from './components/ConcurrencyTesting';
import ConcurrencyTestingPsycopg from './components/ConcurrencyTestingPsycopg';
import TBDTab from './components/TBDTab';
>>>>>>> origin/main
import DatabricksLogo from './components/DatabricksLogo';
import ThemeToggle from './components/ThemeToggle';

const { Header, Content } = Layout;
const { Title } = Typography;

const AppContent: React.FC = () => {
  const { theme, isDark } = useTheme();
  const styled = createStyledComponents(theme);
  const [generatedConfigs, setGeneratedConfigs] = useState<any>({});
  const [activeTab, setActiveTab] = useState('overview');

  // Apply theme to document
  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
  }, [isDark]);

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
    <Layout style={{
      minHeight: '100vh',
      background: theme.gradients.background,
      position: 'relative'
    }}>
      {/* Background pattern */}
      <div style={styled.backgroundPattern} />

      <Header style={{
        padding: '0 24px',
        background: theme.surface,
        borderBottom: `1px solid ${theme.colors.border}`,
        backdropFilter: 'blur(20px)',
        position: 'sticky',
        top: 0,
        zIndex: 1000
      }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: '64px'
        }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <DatabricksLogo height={32} width={32} />
            <Title level={3} style={{
              color: theme.colors.text,
              margin: '0 0 0 16px',
              fontSize: '1.5rem',
              fontWeight: '600'
            }}>
              Lakebase Reverse ETL Accelerator
            </Title>
          </div>
          <ThemeToggle />
        </div>
      </Header>

<<<<<<< HEAD
      <Content style={{ padding: '0' }}>
=======
      <Content style={{ padding: '0', background: '#fafafa' }}>
>>>>>>> origin/main
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
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
<<<<<<< HEAD
              key: 'pgbench-databricks',
              label: 'Concurrency Testing (pgbench)',
              children: <PgbenchDatabricks />
=======
              key: 'concurrency',
              label: 'Concurrency Testing (pgbench)',
              children: <ConcurrencyTesting />
>>>>>>> origin/main
            },
            {
              key: 'concurrency-databricks',
              label: 'Concurrency Testing (psycopg)',
              children: <ConcurrencyTestingPsycopg />
<<<<<<< HEAD
=======
            },
            {
              key: 'tbd',
              label: 'TBD',
              children: <TBDTab />
>>>>>>> origin/main
            }
          ]}
          style={{
            padding: '0 24px',
            background: theme.surface,
            margin: '24px',
            borderRadius: '16px',
            boxShadow: theme.shadows.card,
            backdropFilter: 'blur(20px)',
            border: `1px solid ${theme.colors.border}`
          }}
          size="large"
          tabBarStyle={{
            background: theme.surface,
            margin: 0,
            padding: '0 24px',
            borderBottom: `1px solid ${theme.colors.border}`,
            borderRadius: '16px 16px 0 0'
          }}
        />
      </Content>
    </Layout>
  );
};

function App() {
  return (
    <ThemeProvider>
      <AppContent />
    </ThemeProvider>
  );
}

export default App;
