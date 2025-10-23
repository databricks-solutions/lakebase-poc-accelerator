import React, { createContext, useContext, useState, useEffect } from 'react';

export interface Theme {
    mode: 'light' | 'dark';
    surface: string;
    surfaceSecondary: string;
    colors: {
        background: string;
        surface: string;
        surfaceSecondary: string;
        text: string;
        textSecondary: string;
        textTertiary: string;
        primary: string;
        primaryHover: string;
        border: string;
        borderHover: string;
        success: string;
        warning: string;
        error: string;
        info: string;
        accent1: string;
        accent2: string;
        accent3: string;
        accent4: string;
        accent5: string;
    };
    shadows: {
        card: string;
        cardHover: string;
        glow: string;
    };
    gradients: {
        background: string;
        card: string;
        primary: string;
    };
}

const lightTheme: Theme = {
    mode: 'light',
    surface: '#f8f9fa',
    surfaceSecondary: '#ffffff',
    colors: {
        background: '#ffffff',
        surface: '#f8f9fa',
        surfaceSecondary: '#ffffff',
        text: '#212529',
        textSecondary: '#6c757d',
        textTertiary: '#adb5bd',
        primary: '#007bff',
        primaryHover: '#0056b3',
        border: '#dee2e6',
        borderHover: '#007bff',
        success: '#28a745 !important',
        warning: '#ffc107',
        error: '#dc3545',
        info: '#17a2b8',
        accent1: '#007bff',
        accent2: '#6f42c1',
        accent3: '#fd7e14',
        accent4: '#20c997',
        accent5: '#e83e8c',
    },
    shadows: {
        card: '0 2px 8px rgba(0, 0, 0, 0.1)',
        cardHover: '0 4px 16px rgba(0, 0, 0, 0.15)',
        glow: '0 0 20px rgba(0, 123, 255, 0.3)',
    },
    gradients: {
        background: 'linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%)',
        card: 'linear-gradient(135deg, rgba(255, 255, 255, 0.9) 0%, rgba(248, 249, 250, 0.9) 100%)',
        primary: 'linear-gradient(135deg, #007bff 0%, #0056b3 100%)',
    },
};

const darkTheme: Theme = {
    mode: 'dark',
    surface: 'rgba(255, 255, 255, 0.03)',
    surfaceSecondary: 'rgba(255, 255, 255, 0.05)',
    colors: {
        background: '#0a0a0a',
        surface: 'rgba(255, 255, 255, 0.03)',
        surfaceSecondary: 'rgba(255, 255, 255, 0.05)',
        text: '#ffffff',
        textSecondary: '#e0e0e0',
        textTertiary: '#b0b0b0',
        primary: '#39ff14',
        primaryHover: '#2dd400',
        border: 'rgba(57, 255, 20, 0.2)',
        borderHover: 'rgba(57, 255, 20, 0.4)',
        success: '#39ff14',
        warning: '#ffa500',
        error: '#ff1493',
        info: '#00ffff',
        accent1: '#39ff14',
        accent2: '#00ffff',
        accent3: '#8a2be2',
        accent4: '#ffa500',
        accent5: '#ff1493',
    },
    shadows: {
        card: '0 8px 32px rgba(0, 0, 0, 0.3)',
        cardHover: '0 12px 40px rgba(0, 0, 0, 0.4)',
        glow: '0 0 20px rgba(57, 255, 20, 0.3)',
    },
    gradients: {
        background: 'linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%)',
        card: 'linear-gradient(135deg, rgba(255, 255, 255, 0.03) 0%, rgba(0, 0, 0, 0.3) 100%)',
        primary: 'linear-gradient(135deg, #39ff14 0%, #2dd400 100%)',
    },
};

interface ThemeContextType {
    theme: Theme;
    toggleTheme: () => void;
    isDark: boolean;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export const useTheme = () => {
    const context = useContext(ThemeContext);
    if (!context) {
        throw new Error('useTheme must be used within a ThemeProvider');
    }
    return context;
};

interface ThemeProviderProps {
    children: React.ReactNode;
}

export const ThemeProvider: React.FC<ThemeProviderProps> = ({ children }) => {
    const [isDark, setIsDark] = useState(() => {
        const saved = localStorage.getItem('theme');
        return saved ? saved === 'dark' : false; // Default to light mode
    });

    const theme = isDark ? darkTheme : lightTheme;

    const toggleTheme = () => {
        setIsDark(!isDark);
    };

    useEffect(() => {
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
    }, [isDark]);

    return (
        <ThemeContext.Provider value={{ theme, toggleTheme, isDark }}>
            {children}
        </ThemeContext.Provider>
    );
};
