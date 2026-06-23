import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

jest.mock('./components/PgbenchDatabricks', () => () => <div>Pgbench page</div>);
jest.mock('./components/ConcurrencyTestingPsycopg', () => () => <div>Psycopg page</div>);

test('renders concurrency tabs without removed Lakebase pages', () => {
  render(<App />);

  expect(screen.getByText(/Concurrency Testing \(pgbench\)/i)).toBeInTheDocument();
  expect(screen.getByText(/Concurrency Testing \(psycopg\)/i)).toBeInTheDocument();
  expect(screen.queryByText(/Lakebase Overview/i)).not.toBeInTheDocument();
  expect(screen.queryByText(/Lakebase Calculator/i)).not.toBeInTheDocument();
  expect(screen.queryByText(/Lakebase Deployment/i)).not.toBeInTheDocument();
});
