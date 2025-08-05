# Databricks Lakebase Demo

A demonstration project showcasing how to connect to Databricks PostgreSQL instances and work with stored procedures for candy product analytics.

## Overview

This project demonstrates:
- Connecting to Databricks PostgreSQL instances using Python SDK
- Creating and executing stored procedures with multiple result sets
- Managing database connections with connection pooling
- Analyzing candy product data across divisions and factories

## Project Structure

```
databricks-lakebase-demo/
├── README.md                        # This file
├── lakebase-demo.ipynb              # Main Jupyter notebook with demo code
├── candy_analytics_procedure.sql    # Standalone SQL stored procedure
├── databricks.yml                   # Databricks bundle configuration
├── requirements.txt                 # Python dependencies
└── .venv/                          # Virtual environment (created locally)
```

## Features

### 📊 Analytics Capabilities
- **Overall Statistics**: Total products, divisions, factories, pricing, and profit analysis
- **Division Breakdown**: Analysis by product division (Chocolate, Sugar, Other)
- **Factory Breakdown**: Performance metrics by manufacturing factory
- **Profit Analysis**: Cost, pricing, and profit margin calculations

### 🔧 Technical Features
- PostgreSQL stored procedures with multiple result sets using `refcursor`
- Connection pooling for efficient database access
- Error handling and transaction management
- Databricks SDK integration for credential management

## Prerequisites

1. **Databricks Workspace Access**
   - Access to a Databricks workspace with PostgreSQL instance
   - Databricks identity (user) is added as role in Postgres instance
   - Service Principal with appropriate permissions

2. **Python Environment**
   - Python 3.10+
   - Virtual environment capability

## Setup Instructions

### 1. Environment Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate     # On Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Databricks Configuration

1. **Create a Service Principal**:
   - Go to Databricks Workspace → Settings
   - Navigate to Identity and access → Service Principals → Manage
   - Add service principal

2. **Generate Client Secrets**:
   - Select your Service Principal → Secrets
   - Generate Secrets with appropriate TTL (max 730 days)
   - Save the Client ID and Secret

3. **Environment Variables**:
   Create a `.env` file in the project root:
   ```env
   CLIENT_ID=your_client_id_here
   CLIENT_SECRET=your_client_secret_here
   ```

### 3. Database Configuration

Update the configuration variables in the notebook:
```python
workspace_url = 'https://your-workspace.cloud.databricks.com/'
instance_name = 'your_instance_name'
database = "databricks_postgres"
user = "your.email@company.com"
```

## Usage

### Running the Demo

1. **Start Jupyter Notebook**:
   ```bash
   jupyter notebook lakebase-demo.ipynb
   ```

2. **Execute Cells Sequentially**:
   - Install required packages
   - Connect to Databricks PostgreSQL
   - Create stored procedures
   - Run analytics queries


## Database Schema

The demo works with a `syncedcandy` table containing:
- **Division**: Product category (Chocolate, Sugar, Other)
- **Product**: Product name
- **Factory**: Manufacturing location
- **SKU**: Stock keeping unit identifier
- **Unit Price**: Selling price per unit
- **Unit Cost**: Manufacturing cost per unit

## Stored Procedure Details

The `get_candy_analytics_multiple_results` procedure returns three result sets:

1. **Overall Statistics**: Aggregate metrics across all products
2. **Division Breakdown**: Metrics grouped by product division
3. **Factory Breakdown**: Metrics grouped by manufacturing factory

Each result set includes:
- Product counts
- Average/min/max pricing
- Profit calculations
- Cost analysis

## Dependencies

- `databricks-sdk`: For Databricks workspace integration
- `psycopg2-binary`: PostgreSQL adapter for Python
- `python-dotenv`: Environment variable management
- `pandas`: Data manipulation and analysis

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify Service Principal credentials
   - Check network connectivity to Databricks workspace
   - Ensure PostgreSQL instance is running

2. **Permission Errors**:
   - Verify Service Principal has database access
   - Check procedure execution permissions

3. **Environment Issues**:
   - Ensure virtual environment is activated
   - Verify all dependencies are installed
   - Check Python version compatibility

### Debug Tips

- Enable verbose logging in the Databricks SDK
- Check connection pool status
- Verify environment variables are loaded correctly

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is for demonstration purposes. Please check with your organization's policies before using in production environments.

---

**Note**: This is a demo project. Ensure proper security practices and error handling before using in production environments.