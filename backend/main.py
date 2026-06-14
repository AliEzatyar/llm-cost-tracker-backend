"""
main.py — Layer 2: FastAPI Backend + Service Layer

Dependencies:
    pip install fastapi uvicorn[standard] slowapi python-dotenv \
                httpx emails asyncpg sqlalchemy[asyncio]

Run:
    uvicorn main:app --reload
"""

from .database.models import (
    get_project_by_api_key,
)
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from .auth.routers import router as auth_router
from .auth.services import ALGORITHM, SECRET_KEY
from jose import jwt, JWTError
from fastapi import Request
from fastapi.responses import JSONResponse
from .database.database import AsyncSessionLocal
from backend.dashboard.routers import router as dashboard_router
from .configs import limiter
from .sdk_api.routers import router as sdk_api_router
from backend.admin.routers import router as admin_router

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# App & middleware
# ---------------------------------------------------------------------------

app = FastAPI(title="Cost Tracker API", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # must be False when origins is "*"
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(sdk_api_router)
app.include_router(admin_router)

# ---- Auth middleware --------------------------------------------------------

BYPASS_PATHS = {
    "/docs",
    "/openapi.json",
    "/redoc",
    "/webhooks/stripe",
    "/health",
    "/auth/login",
    "/auth/register",
}

NO_PROJECT_NEEDED_PATHS = {"/dashboard/create-project", "/dashboard/projects"}


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    if request.url.path in BYPASS_PATHS:
        return await call_next(request)

    # --------------------------------------------------
    # API KEY AUTH
    # --------------------------------------------------
    api_key = request.headers.get("X-API-Key")
    if api_key:
        api_key = api_key.split("Bearer ")[1]
        async with AsyncSessionLocal() as session:
            project = await get_project_by_api_key(session, api_key)
        if not project:
            return JSONResponse(
                status_code=404,
                content={"detail": "Invalid api key id"},
            )

        request.state.auth_type = "api_key"
        request.state.project = project
        request.state.project_id = str(project["id"])
        return await call_next(request)

    # --------------------------------------------------
    # JWT AUTH
    # --------------------------------------------------
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        try:
            payload = jwt.decode(
                token,
                SECRET_KEY,
                algorithms=[ALGORITHM],
            )
            user_id = payload.get("sub")
            if not user_id:
                raise JWTError("Missing sub claim")
            request.state.auth_type = "jwt"
            request.state.user_id = user_id
            request.state.jwt_payload = payload
            return await call_next(request)

        except JWTError:
            return JSONResponse(
                content='{"detail":"Invalid or expired token"}',
                status_code=401,
            )

    return JSONResponse(
        content='{"detail":"Authentication required"}',
        status_code=401,
    )


# @app.middleware("http")
# async def debug_middleware(request: Request, call_next):
#     response = await call_next(request)
#     print("PATH:", request.url.path)
#     print("STATUS:", response.status_code)
#     print("HEADERS:", dict(response.headers))
#     return response


# ---- Startup ---------------------------------------------------------------

# backend/database/seed.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


@app.on_event("startup")
async def startup():
    logger.info("Database tables verified / created.")
    # await create_all_tables()
    # Start background scheduler (imported here to avoid circular imports)
    from .workers import start_scheduler

    start_scheduler()
    logger.info("Background scheduler started.")


@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}
