import React, { useState } from 'react';
import { Layout, Typography, Tabs } from 'antd';
import 'antd/dist/reset.css';
import './App.css';

import { ThemeProvider, useTheme } from './contexts/ThemeContext';
import { createStyledComponents } from './styles/theme';
import ConcurrencyTestingPsycopg from './components/ConcurrencyTestingPsycopg';
import PgbenchDatabricks from './components/PgbenchDatabricks';
import DatabricksLogo from './components/DatabricksLogo';
import ThemeToggle from './components/ThemeToggle';

const { Header, Content } = Layout;
const { Title } = Typography;

const AppContent: React.FC = () => {
  const { theme, isDark } = useTheme();
  const styled = createStyledComponents(theme);
  const [activeTab, setActiveTab] = useState('pgbench-databricks');

  // Apply theme to document
  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
  }, [isDark]);

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

      <Content style={{ padding: '0' }}>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: 'pgbench-databricks',
              label: 'Concurrency Testing (pgbench)',
              children: <PgbenchDatabricks />
            },
            {
              key: 'concurrency-psycopg',
              label: 'Concurrency Testing (psycopg)',
              children: <ConcurrencyTestingPsycopg />
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
          tabBarGutter={8}
          tabBarStyle={{
            background: theme.surface,
            margin: 0,
            padding: '0 24px',
            borderBottom: `1px solid ${theme.colors.border}`,
            borderRadius: '16px 16px 0 0',
            overflowX: 'auto',
            overflowY: 'hidden',
            whiteSpace: 'nowrap',
            display: 'flex',
            flexWrap: 'nowrap'
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
