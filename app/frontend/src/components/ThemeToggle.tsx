import React from 'react';
import { Button, Tooltip } from 'antd';
import { SunOutlined, MoonOutlined } from '@ant-design/icons';
import { useTheme } from '../contexts/ThemeContext';

const ThemeToggle: React.FC = () => {
    const { isDark, toggleTheme } = useTheme();

    return (
        <Tooltip title={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>
            <Button
                type="text"
                icon={isDark ? <SunOutlined /> : <MoonOutlined />}
                onClick={toggleTheme}
                style={{
                    color: isDark ? '#ffffff' : '#000000',
                    border: 'none',
                    background: 'transparent',
                    fontSize: '16px',
                    width: '40px',
                    height: '40px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                }}
            />
        </Tooltip>
    );
};

export default ThemeToggle;
