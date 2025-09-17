# Lakebase Accelerator Web Application

A full-stack web application for configuring OLTP workload parameters, estimating Lakebase costs, and generating deployment configurations.

## Architecture

- **Backend**: FastAPI with Python integration to existing scripts
- **Frontend**: React + TypeScript + Ant Design
- **Development**: Local development environment with hot reload

## Features

- **Interactive Configuration**: Web form for workload parameters
- **Cost Estimation**: Real-time cost calculation with table size analysis
- **File Generation**: Download YAML configuration files
- **Professional UI**: Enterprise-grade components with Ant Design

## Development Setup

### Prerequisites

- Node.js 18+
- Python 3.11+

### Backend Development

```bash

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp ../../.env.example .env
# Edit .env with your Databricks credentials

# Navigate to backend directory
cd app/backend

# Run development server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Backend will be available at: http://localhost:8000
API documentation: http://localhost:8000/docs

### Frontend Development

```bash
# Navigate to frontend directory
cd app/frontend

# Install dependencies
npm install

# Start development server
npm start
```

Frontend will be available at: http://localhost:3000

### React Development Scripts

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

#### Available Scripts

In the `app/frontend` directory, you can run:

##### `npm start`
Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

##### `npm test`
Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

##### `npm run build`
Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

##### `npm run eject`
**Note: this is a one-way operation. Once you `eject`, you can't go back!**

If you aren't satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you're on your own.

You don't have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn't feel obligated to use this feature. However we understand that this tool wouldn't be useful if you couldn't customize it when you are ready for it.

#### Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).

### Full Stack Development

Run both backend and frontend simultaneously:

```bash
# Terminal 1 - Backend
cd app/backend
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 - Frontend  
cd app/frontend
npm start
```

## Environment Variables

Create a `.env` file in the project root:

```bash
# Required for cost estimation and table size calculation
DATABRICKS_ACCESS_TOKEN=your_databricks_pat
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id

# Required for query conversion
DATABRICKS_ENDPOINT=https://your-workspace.cloud.databricks.com/serving-endpoints

# Optional - LLM model for query conversion
MODEL_NAME=databricks-meta-llama-3-1-70b-instruct
```

## Production Deployment

### Backend Deployment

```bash
# Install production dependencies
cd app/backend
pip install -r requirements.txt

# Run with production server
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Frontend Deployment

```bash
# Build for production
cd app/frontend
npm run build

# Serve static files (using nginx, Apache, or similar)
# The build/ directory contains the production-ready files
```

## API Endpoints

- `GET /` - Root endpoint
- `GET /health` - Health check
- `POST /api/estimate-cost` - Cost estimation with optional table size calculation
- `POST /api/generate-synced-tables` - Generate table sync configuration
- `POST /api/generate-databricks-config` - Generate Databricks bundle config
- `POST /api/generate-lakebase-instance` - Generate Lakebase instance config
- `GET /docs` - Interactive API documentation

## File Structure

```
app/
├── backend/
│   ├── main.py              # FastAPI application
│   └── requirements.txt     # Python dependencies
├── frontend/
│   ├── src/
│   │   ├── components/      # React components
│   │   ├── types/          # TypeScript definitions
│   │   └── App.tsx         # Main application
│   ├── package.json        # Node.js dependencies
│   └── public/             # Static assets
├── test_integration.py     # Integration tests
└── README.md              # This file
```

## Usage

1. **Configure Workload**: Fill in database instance, storage, and sync parameters
2. **Add Tables**: Specify Delta tables to sync with primary keys
3. **Generate Estimate**: Click to run cost estimation and generate configs
4. **Download Files**: Get YAML configuration files for deployment
5. **Deploy**: Use Databricks CLI to deploy your Lakebase instance

## Troubleshooting

### Backend Issues

- Check Python path includes both `backend/` and `src/` directories
- Verify environment variables are set correctly
- Ensure Databricks credentials have appropriate permissions

### Frontend Issues

- Clear npm cache: `npm cache clean --force`
- Delete node_modules and reinstall: `rm -rf node_modules && npm install`
- Check browser console for API connection errors

### Production Issues

- Check server logs for backend errors
- Verify environment variables are set correctly
- Ensure all dependencies are installed
