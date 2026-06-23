import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

jest.mock('./components/PgbenchDatabricks', () => () => <div>Pgbench content</div>);
jest.mock('./components/ConcurrencyTestingPsycopg', () => () => <div>Psycopg content</div>);

test('renders only concurrency testing tabs', () => {
  render(<App />);

  expect(screen.getByRole('tab', { name: 'Concurrency Testing (pgbench)' })).toBeInTheDocument();
  expect(screen.getByRole('tab', { name: 'Concurrency Testing (psycopg)' })).toBeInTheDocument();

  expect(screen.queryByRole('tab', { name: 'Lakebase Overview' })).not.toBeInTheDocument();
  expect(screen.queryByRole('tab', { name: 'Lakebase Calculator' })).not.toBeInTheDocument();
  expect(screen.queryByRole('tab', { name: 'Lakebase Deployment' })).not.toBeInTheDocument();
});
