# Lakebase Accelerator Web Application

A full-stack web application for configuring OLTP workload parameters, estimating Lakebase costs, and generating deployment configurations.

## Architecture

- **Backend**: FastAPI with Python integration to existing scripts
- **Frontend**: React + TypeScript + Ant Design
- **Deployment**: Docker container for Databricks Apps

## Features

- **Interactive Configuration**: Web form for workload parameters
- **Cost Estimation**: Real-time cost calculation with table size analysis
- **File Generation**: Download YAML configuration files
- **Professional UI**: Enterprise-grade components with Ant Design

## Development Setup

### Prerequisites

- Node.js 18+
- Python 3.11+
- Docker (for deployment)

### Backend Development

```bash
# Navigate to backend directory
cd app/backend

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp ../../.env.example .env
# Edit .env with your Databricks credentials

# Run development server
python main.py
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

### Full Stack Development

Run both backend and frontend simultaneously:

```bash
# Terminal 1 - Backend
cd app/backend
source .venv/bin/activate
python main.py

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

## Docker Deployment

### Local Testing

```bash
# Build and run with Docker Compose
cd app
docker-compose up --build

# Access application
open http://localhost:8000
```

### Production Build

```bash
# Build Docker image
cd app
docker build -t lakebase-accelerator .

# Run container
docker run -p 8000:8000 --env-file ../.env lakebase-accelerator
```

## Databricks Apps Deployment

Follow the [Databricks Apps FastAPI guide](https://apps-cookbook.dev/docs/fastapi/getting_started/create):

### 1. Prepare Application

```bash
# Ensure all files are in place
cd app/
ls -la  # Should show: backend/, frontend/, Dockerfile, databricks-app.yml
```

### 2. Create Databricks Secrets

```bash
# Create secret scope for credentials
databricks secrets create-scope lakebase-credentials

# Add secrets
databricks secrets put-secret lakebase-credentials access_token
databricks secrets put-secret lakebase-credentials server_hostname  
databricks secrets put-secret lakebase-credentials http_path
databricks secrets put-secret lakebase-credentials endpoint
```

### 3. Deploy to Databricks Apps

```bash
# Deploy using Databricks CLI
databricks apps create --manifest databricks-app.yml

# Check deployment status
databricks apps list
databricks apps get lakebase-accelerator
```

### 4. Connect from Local (Development)

Follow the [local connection guide](https://apps-cookbook.dev/docs/fastapi/getting_started/connections/connect_from_local) for development access.

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
├── Dockerfile              # Container configuration
├── docker-compose.yml      # Local development setup
├── databricks-app.yml      # Databricks Apps configuration
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

### Docker Issues

- Build with no cache: `docker build --no-cache -t lakebase-accelerator .`
- Check container logs: `docker logs <container_id>`
- Verify environment variables are passed correctly