
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

# Always include backend routes first (but not the root route)
app.include_router(backend_app.router)

# Serve static frontend if available
# Try multiple possible locations for the build directory
possible_build_paths = [
    Path(__file__).parent / "build",
    Path.cwd() / "build", 
    Path("/app/python/source_code/build"),  # Common Databricks path
    Path("/app/python/source_code/app/frontend/build")  # Alternative path
]

frontend_build_path = None
for path in possible_build_paths:
    print(f"DEBUG: Checking build path: {path} - exists: {path.exists()}")
    if path.exists():
        frontend_build_path = path
        break

print(f"DEBUG: Selected build path: {frontend_build_path}")

if frontend_build_path and frontend_build_path.exists():
    print(f"DEBUG: Setting up frontend serving with build path: {frontend_build_path}")
    print(f"DEBUG: Static directory exists: {(frontend_build_path / 'static').exists()}")
    print(f"DEBUG: Index.html exists: {(frontend_build_path / 'index.html').exists()}")
    
    app.mount("/static", StaticFiles(directory=str(frontend_build_path / "static")), name="static")

    @app.get("/")
    async def serve_frontend():
        print(f"DEBUG: Serving frontend from: {frontend_build_path / 'index.html'}")
        return FileResponse(str(frontend_build_path / "index.html"))

    @app.get("/{path:path}")
    async def serve_frontend_routes(path: str):
        print(f"DEBUG: Serving frontend route '{path}' from: {frontend_build_path / 'index.html'}")
        return FileResponse(str(frontend_build_path / "index.html"))
    
    print("DEBUG: Frontend routes configured successfully")
else:
    print(f"DEBUG: Build path not found or doesn't exist. Using backend-only mode.")
    
    @app.get("/")
    async def root():
        print("DEBUG: Serving backend-only root route")
        return {
            "message": "Lakebase Accelerator API",
            "status": "Backend only - Frontend not built",
            "docs": "/docs"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
