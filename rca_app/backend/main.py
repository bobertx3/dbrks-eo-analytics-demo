"""
FastAPI backend for Enterprise Root Cause Intelligence.
Serves the React frontend and provides API endpoints.
"""
import os
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from backend.routes.incidents import router as incidents_router
from backend.routes.root_cause import router as root_cause_router
from backend.routes.service_ranking import router as service_ranking_router
from backend.routes.change_correlation import router as change_correlation_router
from backend.routes.domain_summary import router as domain_summary_router
from backend.routes.genie import router as genie_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from backend.db import CATALOG, SCHEMA
    logger.info("Enterprise RCA Intelligence starting up ...")
    logger.info(f"  DATABRICKS_APP_NAME: {os.environ.get('DATABRICKS_APP_NAME', '(local)')}")
    logger.info(f"  DATABRICKS_PROFILE:  {os.environ.get('DATABRICKS_PROFILE', '(not set)')}")
    logger.info(f"  CATALOG.SCHEMA:      {CATALOG}.{SCHEMA}")
    logger.info(f"  WAREHOUSE_ID:        {os.environ.get('DATABRICKS_WAREHOUSE_ID', '(auto-discover)')}")
    logger.info(f"  SERVING_ENDPOINT:    {os.environ.get('SERVING_ENDPOINT', '(default)')}")
    logger.info(f"  GENIE_SPACE_ID:      {os.environ.get('GENIE_SPACE_ID', '(fallback mode)')}")
    yield
    logger.info("Enterprise RCA Intelligence shutting down ...")


app = FastAPI(
    title="Enterprise Root Cause Intelligence",
    description="Correlating signals across domains and time to reveal systemic causes",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
app.include_router(incidents_router)
app.include_router(root_cause_router)
app.include_router(service_ranking_router)
app.include_router(change_correlation_router)
app.include_router(domain_summary_router)
app.include_router(genie_router)


@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "app": "dbrks-eo-analytics-demo", "version": "1.0.0"}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


# Serve React frontend (built with Vite)
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if frontend_dist.exists():
    logger.info(f"Serving frontend from {frontend_dist}")
    # Mount static assets
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # Serve index.html for SPA routing (catch-all must be last)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Don't serve SPA for API routes
        if full_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        # Serve static files if they exist
        file_path = frontend_dist / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        # Fallback to index.html
        return FileResponse(str(frontend_dist / "index.html"))
else:
    logger.warning(f"Frontend dist not found at {frontend_dist}. Run 'npm run build' in frontend/")

    @app.get("/")
    async def no_frontend():
        return {"message": "Frontend not built. Run 'cd frontend && npm run build'", "api_docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
