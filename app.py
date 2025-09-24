
#!/usr/bin/env python3
"""
Databricks App entry point for Lakebase Accelerator
Combines FastAPI backend with static frontend serving
"""

import os
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# Add the backend directory to Python path
backend_path = Path(__file__).parent / "app" / "backend"
sys.path.insert(0, str(backend_path))

# Import the FastAPI app from backend
from main import app as backend_app

# Create the main app
app = FastAPI(
    title="Lakebase Accelerator",
    description="Databricks App for Lakebase cost estimation and deployment",
    version="1.0.0"
)

# Configure CORS for Databricks Apps
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Databricks Apps handle CORS
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the backend API
app.mount("/api", backend_app)

# Serve static files from the built frontend
frontend_build_path = Path(__file__).parent / "build"
if frontend_build_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_build_path / "static")), name="static")
    
    @app.get("/")
    async def serve_frontend():
        """Serve the React frontend"""
        return FileResponse(str(frontend_build_path / "index.html"))
    
    @app.get("/{path:path}")
    async def serve_frontend_routes(path: str):
        """Serve React routes (SPA)"""
        # Check if it's an API route
        if path.startswith("api/"):
            return {"error": "API route not found"}
        
        # Serve index.html for all other routes (React Router)
        return FileResponse(str(frontend_build_path / "index.html"))
else:
    @app.get("/")
    async def root():
        return {
            "message": "Lakebase Accelerator API",
            "status": "Backend only - Frontend not built",
            "docs": "/docs"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
