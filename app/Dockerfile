# Multi-stage Docker build for Databricks Apps deployment
FROM node:18-alpine AS frontend-builder

# Build React frontend
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci --only=production
COPY frontend/ ./
RUN npm run build

# Python backend stage
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy backend requirements and install Python dependencies
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source code
COPY backend/ ./backend/

# Copy the original src directory for Python scripts
COPY ../src/ ./src/

# Copy built frontend from frontend-builder stage
COPY --from=frontend-builder /app/frontend/build ./frontend/build

# Create a simple static file server wrapper
RUN echo '#!/usr/bin/env python3\n\
import os\n\
import sys\n\
from pathlib import Path\n\
\n\
# Add backend and src to Python path\n\
sys.path.append(str(Path(__file__).parent / "backend"))\n\
sys.path.append(str(Path(__file__).parent / "src"))\n\
\n\
if __name__ == "__main__":\n\
    import uvicorn\n\
    from backend.main import app\n\
    \n\
    # Serve static files from React build\n\
    from fastapi.staticfiles import StaticFiles\n\
    from fastapi.responses import FileResponse\n\
    \n\
    # Mount static files\n\
    if os.path.exists("frontend/build"):\n\
        app.mount("/static", StaticFiles(directory="frontend/build/static"), name="static")\n\
        \n\
        @app.get("/{path:path}")\n\
        async def serve_react(path: str = ""):\n\
            if path.startswith("api/"):\n\
                # Let API routes handle themselves\n\
                return\n\
            \n\
            # Serve React app for all other routes\n\
            index_file = Path("frontend/build/index.html")\n\
            if index_file.exists():\n\
                return FileResponse(index_file)\n\
            else:\n\
                return {"error": "Frontend not found"}\n\
    \n\
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))\n' > run_app.py

RUN chmod +x run_app.py

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["python", "run_app.py"]