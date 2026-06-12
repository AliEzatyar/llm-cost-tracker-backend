# ===========================================================================
# ROUTES
# ===========================================================================

import csv
from datetime import date, datetime, timezone
import io
import uuid
import secrets
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import StreamingResponse
from sqlalchemy import select, text
from backend.auth.services import get_current_user
from backend.dashboard.services import (
    BillingService,
    CostService,
    _verify_stripe_signature,
)
from backend.database.database import get_db
from backend.database.models import Project, User, get_project_by_api_key
from backend.configs import (
    STRIPE_WEBHOOK_SECRET,
    CostEventBatch,
    CostEventIn,
    ThresholdIn,
)
from backend.configs import limiter

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


# ── helpers ───────────────────────────────────────────────────────────────────


def _require_user(request: Request) -> str:
    """Return user_id from request.state or raise 401."""
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user_id


def _require_project(request: Request) -> str:
    """Return project_id from request.state or raise 401.

    The auth middleware is responsible for resolving the Bearer token to a
    user and a project_id. Dashboard routes rely on this — no raw API key
    is ever read from request headers inside these routes.
    """
    project_id = getattr(request.state, "project_id", None)
    if not project_id:
        raise HTTPException(
            status_code=404,
            detail="No project resolved from token. Ensure Authorization: Bearer <token> is set.",
        )
    return project_id


# ── POST /ingest ─────────────────────────────────────────────────────────────
# NOTE: This endpoint is intended for SDK use only (via API key), not the
# dashboard UI. It is kept here for completeness but the dashboard never
# calls it.


@router.post("/ingest", status_code=202, tags=["costs"])
@limiter.limit("1000/minute")
async def ingest_cost_event(
    request: Request,
    events: CostEventBatch,
    db: AsyncSession = Depends(get_db),
):
    """Receive a single SDK cost event and persist it (SDK use only)."""
    project_id = request.state.project_id
    billing = BillingService(db)
    await billing.enforce_limit(project_id)
    cost_svc = CostService(db)
    counter = 0
    for ev in events.events:
        cost_usd = 1000
        await cost_svc.write_event(project_id, ev,cost_usd)
        counter += 1
    return counter


# ── GET /costs ────────────────────────────────────────────────────────────────


@router.get("/costs", tags=["costs"])
@limiter.limit("1000/minute")
async def list_costs(
    request: Request,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Return daily cost rollup for all customers in the authenticated project."""
    project_id: str = _require_project(request)
    cost_svc = CostService(db)
    rows = await cost_svc.get_by_tenant(project_id, date_from, date_to)
    print("costs found", rows)
    return {"project_id": project_id, "rows": rows, "count": len(rows)}


# ── GET /costs/export/csv ─────────────────────────────────────────────────────
# IMPORTANT: this route must be declared BEFORE /costs/{tenant_id} so that
# FastAPI does not treat "export" as a tenant_id path segment.


@router.get("/costs/export/csv", tags=["costs"])
@limiter.limit("60/minute")
async def export_costs_csv(
    request: Request,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Download cost rollup as a CSV file. Authenticated via Bearer token."""
    project_id: str = _require_project(request)
    cost_svc = CostService(db)
    rows = await cost_svc.get_by_tenant(project_id, date_from, date_to)

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=["tenant_id", "date", "total_cost_usd", "total_calls"]
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "tenant_id": row["tenant_id"],
                "date": row["date"],
                "total_cost_usd": float(row["total_cost_usd"]),
                "total_calls": row["total_calls"],
            }
        )

    buf.seek(0)
    filename = f"costs_{project_id}_{date.today()}.csv"
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /costs/{tenant_id} ──────────────────────────────────────────────────


@router.get("/costs/{tenant_id}", tags=["costs"])
@limiter.limit("1000/minute")
async def get_customer_costs(
    request: Request,
    tenant_id: str,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Return per-model cost breakdown for a single customer."""
    project_id: str = _require_project(request)
    cost_svc = CostService(db)
    return await cost_svc.get_customer_breakdown(
        project_id, tenant_id, date_from, date_to
    )


# ── POST /thresholds ──────────────────────────────────────────────────────────


@router.post("/thresholds", status_code=201, tags=["alerts"])
@limiter.limit("1000/minute")
async def set_threshold(
    request: Request,
    body: ThresholdIn,
    db: AsyncSession = Depends(get_db),
):
    """Create or update a monthly spend threshold for a customer."""
    project_id: str = _require_project(request)
    threshold_id = str(uuid.uuid4())

    await db.execute(
        text("""
            INSERT INTO thresholds (id, project_id, tenant_id, limit_usd, created_at, updated_at)
            VALUES (:id, :project_id, :tenant_id, :limit_usd, NOW(), NOW())
            ON CONFLICT (project_id, tenant_id)
            DO UPDATE SET limit_usd = EXCLUDED.limit_usd, updated_at = NOW()
        """),
        {
            "id": threshold_id,
            "project_id": project_id,
            "tenant_id": body.tenant_id,
            "limit_usd": body.limit_usd,
        },
    )
    return {
        "project_id": project_id,
        "tenant_id": body.tenant_id,
        "limit_usd": body.limit_usd,
    }


# ── GET /alerts ───────────────────────────────────────────────────────────────


@router.get("/alerts", tags=["alerts"])
@limiter.limit("1000/minute")
async def list_alerts(
    request: Request,
    tenant_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """List triggered alerts for the authenticated project."""
    project_id: str = _require_project(request)
    extra = "AND tenant_id = :tenant_id" if tenant_id else ""
    params: dict = {"project_id": project_id, "limit": limit}
    if tenant_id:
        params["tenant_id"] = tenant_id

    result = await db.execute(
        text(f"""
            SELECT id, tenant_id, triggered_at, cost_at_trigger
            FROM   alerts
            WHERE  project_id = :project_id {extra}
            ORDER  BY triggered_at DESC
            LIMIT  :limit
        """),
        params,
    )
    rows = [dict(r) for r in result.mappings()]
    return {"project_id": project_id, "alerts": rows}


# ── POST /webhooks/stripe ─────────────────────────────────────────────────────


@router.post("/webhooks/stripe", tags=["billing"])
async def stripe_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Receive Stripe subscription events.
    Validates the Stripe-Signature header when STRIPE_WEBHOOK_SECRET is set.
    This endpoint intentionally skips JWT auth — Stripe signs its own payloads.
    """
    payload = await request.body()

    if STRIPE_WEBHOOK_SECRET:
        sig_header = request.headers.get("Stripe-Signature", "")
        _verify_stripe_signature(payload, sig_header, STRIPE_WEBHOOK_SECRET)

    import json

    event = json.loads(payload)
    billing = BillingService(db)
    result = await billing.handle_stripe_event(
        event.get("type", ""), event.get("data", {})
    )
    return result


# ── Project schemas ───────────────────────────────────────────────────────────


class CreateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    plan: str = Field("free", pattern="^(free|pro|enterprise)$")


class CreateProjectResponse(BaseModel):
    id: str
    name: str
    plan: str
    api_key: str  # raw key — shown ONCE, never stored
    created_at: datetime


# ── GET /projects ─────────────────────────────────────────────────────────────


@router.get("/projects")
async def list_projects(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List all projects owned by the authenticated user."""
    user_id = _require_user(request)

    result = await db.execute(
        select(Project)
        .where(Project.owner_id == user_id)
        .order_by(Project.created_at.desc())
    )
    projects = result.scalars().all()

    return [
        {
            "id": project.id,
            "name": project.name,
            "plan": project.plan,
            "created_at": project.created_at.isoformat(),
        }
        for project in projects
    ]


# ── POST /create-project ──────────────────────────────────────────────────────


@router.post(
    "/create-project",
    tags=["project creation"],
    response_model=CreateProjectResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_project(
    request: Request,
    body: CreateProjectRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Create a new project for the authenticated user.

    - Generates a cryptographically random API key (for SDK use).
    - Stores only its SHA-256 hash; the raw key is returned once in the
      response and never persisted.
    - Requires a valid JWT in the ``Authorization: Bearer …`` header.
    """
    if current_user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )

    owner_id: uuid.UUID = (
        current_user.id
        if isinstance(current_user.id, uuid.UUID)
        else uuid.UUID(str(current_user.id))
    )

    # Generate a secure API key: ct_<40 url-safe random chars>
    raw_api_key: str = "ct_" + secrets.token_urlsafe(40)
    key_hash: str = Project.hash_key(raw_api_key)

    # Guard against (astronomically unlikely) hash collision
    existing = await get_project_by_api_key(db, raw_api_key)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Key generation collision — please retry.",
        )

    project_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    raw_api_key = "ct_" + secrets.token_urlsafe(40)

    print("CREATED KEY:", repr(raw_api_key))

    key_hash = Project.hash_key(raw_api_key)
    print("CREATED HASH:", key_hash)
    new_project = Project(
        id=project_id,
        name=body.name.strip(),
        plan=body.plan,
        api_key_hash=key_hash,
        owner_id=owner_id,
        created_at=now,
    )

    db.add(new_project)

    try:
        await db.commit()
        await db.refresh(new_project)
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not save project: {exc}",
        )

    return CreateProjectResponse(
        id=new_project.id,
        name=new_project.name,
        plan=new_project.plan,
        api_key=raw_api_key,  # shown once; frontend must copy it
        created_at=new_project.created_at,
    )
