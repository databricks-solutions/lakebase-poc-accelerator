import { Theme } from '../contexts/ThemeContext';

export const createStyledComponents = (theme: Theme) => ({
  // Container styles
  pageContainer: {
    padding: '24px',
    maxWidth: '1200px',
    margin: '0 auto',
    background: theme.gradients.background,
    minHeight: '100vh',
    position: 'relative' as const,
  },

  // Background effects
  backgroundPattern: {
    position: 'absolute' as const,
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: theme.mode === 'dark' 
      ? `
          radial-gradient(circle at 20% 50%, rgba(57, 255, 20, 0.1) 0%, transparent 50%),
          radial-gradient(circle at 80% 20%, rgba(0, 255, 255, 0.1) 0%, transparent 50%),
          radial-gradient(circle at 40% 80%, rgba(138, 43, 226, 0.1) 0%, transparent 50%)
        `
      : `
          radial-gradient(circle at 20% 50%, rgba(0, 123, 255, 0.05) 0%, transparent 50%),
          radial-gradient(circle at 80% 20%, rgba(40, 167, 69, 0.05) 0%, transparent 50%),
          radial-gradient(circle at 40% 80%, rgba(111, 66, 193, 0.05) 0%, transparent 50%)
        `,
    pointerEvents: 'none' as const,
  },

  // Typography
  pageTitle: {
    color: theme.colors.text,
    textAlign: 'center' as const,
    marginBottom: '32px',
    fontSize: '2.5rem',
    fontWeight: '700',
    textShadow: theme.mode === 'dark' ? '0 0 20px rgba(57, 255, 20, 0.3)' : 'none',
  },

  sectionTitle: {
    color: theme.colors.text,
    fontSize: '1.5rem',
    fontWeight: '600',
    marginBottom: '16px',
  },

  // Card styles
  mainCard: {
    background: theme.surface,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '16px',
    backdropFilter: 'blur(20px)',
    boxShadow: theme.shadows.card,
    marginBottom: '32px',
  },

  statCard: (accentColor: string) => ({
    background: `linear-gradient(135deg, ${accentColor}20 0%, ${theme.mode === 'dark' ? 'rgba(0, 0, 0, 0.3)' : 'rgba(255, 255, 255, 0.8)'} 100%)`,
    border: `1px solid ${accentColor}50`,
    borderRadius: '12px',
    padding: '24px',
    textAlign: 'center' as const,
    position: 'relative' as const,
    overflow: 'hidden' as const,
    backdropFilter: 'blur(10px)',
    boxShadow: `0 4px 16px ${accentColor}20`,
    transition: 'all 0.3s ease',
    '&:hover': {
      transform: 'translateY(-2px)',
      boxShadow: `0 8px 24px ${accentColor}30`,
    },
  }),

  statValue: (accentColor: string) => ({
    fontSize: '2.5rem',
    fontWeight: '700',
    color: accentColor,
    marginBottom: '8px',
    textShadow: theme.mode === 'dark' ? `0 0 20px ${accentColor}80` : 'none',
  }),

  statTitle: {
    fontSize: '0.9rem',
    color: theme.colors.textSecondary,
    fontWeight: '500',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },

  // Alert styles
  alert: {
    background: theme.surfaceSecondary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '12px',
    backdropFilter: 'blur(10px)',
    marginBottom: '24px',
  },

  // List styles
  listItem: {
    border: 'none',
    padding: '12px 0',
    color: theme.colors.textSecondary,
    position: 'relative' as const,
    paddingLeft: '24px',
  },

  // Divider
  divider: {
    borderColor: theme.colors.border,
  },

  // Link styles
  link: {
    color: theme.colors.primary,
    textDecoration: 'none',
    fontWeight: '500',
    '&:hover': {
      color: theme.colors.primaryHover,
    },
  },

  // Tag styles
  tag: (color: string) => ({
    background: color,
    color: theme.mode === 'dark' ? '#000000' : '#ffffff',
    border: 'none',
    borderRadius: '6px',
    fontWeight: '500',
  }),

  // Collapse styles
  collapse: {
    background: theme.surface,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '16px',
    boxShadow: theme.shadows.card,
    marginBottom: '24px',
  },
});

export type StyledComponents = ReturnType<typeof createStyledComponents>;
