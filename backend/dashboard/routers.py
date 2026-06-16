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
    CostService,
)
from backend.database.database import get_db
from backend.database.models import (
    Alert,
    Project,
    Threshold,
    User,
    get_project_by_api_key,
)
from backend.configs import (
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


# ── GET /costs ────────────────────────────────────────────────────────────────


@router.get("/costs", tags=["costs"])
@limiter.limit("1000/minute")
async def list_costs(
    request: Request,
    project_id: uuid.UUID,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Return daily cost rollup for all customers in the authenticated project."""
    cost_svc = CostService(db)
    rows = await cost_svc.get_by_tenant(project_id, date_from, date_to)
    return {"project_id": project_id, "rows": rows, "count": len(rows)}


# ── GET /costs/export/csv ─────────────────────────────────────────────────────
# IMPORTANT: this route must be declared BEFORE /costs/{tenant_id} so that
# FastAPI does not treat "export" as a tenant_id path segment.


@router.get("/costs/export/csv", tags=["costs"])
@limiter.limit("60/minute")
async def export_costs_csv(
    request: Request,
    project_id: uuid.UUID,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Download cost rollup as a CSV file. Authenticated via Bearer token."""
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
    project_id: uuid.UUID,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Return per-model cost breakdown for a single customer."""
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
            "project_id": body.project_id,
            "tenant_id": body.tenant_id,
            "limit_usd": body.limit_usd,
        },
    )
    return {
        "project_id": body.project_id,
        "tenant_id": body.tenant_id,
        "limit_usd": body.limit_usd,
    }


from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select


@router.get("/thresholds", tags=["alerts"])
@limiter.limit("1000/minute")
async def list_thresholds(
    request: Request,
    project_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()

    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    if project.owner_id != user.id:
        raise HTTPException(status_code=403, detail="Forbidden")

    result = await db.execute(
        select(Threshold).where(Threshold.project_id == project_id)
    )
    rows = result.scalars().all()

    return [
        {
            "id": t.id,
            "tenant_id": t.tenant_id,
            "limit_usd": float(t.limit_usd),
        }
        for t in rows
    ]


# ── PUT /thresholds/{threshold_id} ────────────────────────────────────────────


class UpdateThresholdRequest(BaseModel):
    limit_usd: float = Field(..., gt=0)


@router.put("/thresholds/{threshold_id}", tags=["alerts"])
@limiter.limit("1000/minute")
async def update_threshold(
    request: Request,
    threshold_id: str,
    body: UpdateThresholdRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update the spend limit for an existing threshold."""
    result = await db.execute(
        select(Threshold)
        .join(Project, Project.id == Threshold.project_id)
        .where(
            Threshold.id == threshold_id,
            Project.owner_id == user.id,
        )
    )
    threshold = result.scalar_one_or_none()

    if not threshold:
        raise HTTPException(status_code=404, detail="Threshold not found")

    threshold.limit_usd = body.limit_usd
    threshold.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(threshold)

    return {
        "id": threshold.id,
        "project_id": threshold.project_id,
        "tenant_id": threshold.tenant_id,
        "limit_usd": float(threshold.limit_usd),
    }


# ── DELETE /thresholds/{threshold_id} ────────────────────────────────────────


@router.delete("/thresholds/{threshold_id}", status_code=204, tags=["alerts"])
@limiter.limit("1000/minute")
async def delete_threshold(
    request: Request,
    threshold_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a threshold. Only the project owner can delete it."""
    result = await db.execute(
        select(Threshold)
        .join(Project, Project.id == Threshold.project_id)
        .where(
            Threshold.id == threshold_id,
            Project.owner_id == user.id,
        )
    )
    threshold = result.scalar_one_or_none()

    if not threshold:
        raise HTTPException(status_code=404, detail="Threshold not found")
    # related alerts
    result = await db.execute(
        select(Alert)
        .join(Project, Project.id == Threshold.project_id)
        .where(
            Threshold.id == threshold_id,
            Project.owner_id == user.id,
        )
    )
    await db.delete(threshold)
    await db.commit()


# ── GET /alerts ───────────────────────────────────────────────────────────────


@router.get("/alerts", tags=["alerts"])
@limiter.limit("1000/minute")
async def list_alerts(
    request: Request,
    project_id: uuid.UUID,
    tenant_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """List triggered alerts for the authenticated project."""
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


# ── Project schemas ───────────────────────────────────────────────────────────


class CreateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    # Plan is informational only at creation time — every new project starts
    # on "free" regardless of what's sent here. Upgrading to a paid plan
    # requires a Stripe Checkout session (POST /billing/checkout), since a
    # paid plan with no Stripe subscription would be an inconsistent state.


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

    # Every project starts on the free plan. Upgrading to a paid plan is a
    # separate step (Stripe Checkout via POST /billing/checkout) so that
    # projects.plan never gets ahead of an actual Stripe subscription.
    new_project = Project(
        id=project_id,
        name=body.name.strip(),
        plan="free",
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


class UpdateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    # Plan is intentionally NOT editable here. Changing plans has billing
    # consequences (Stripe subscription create/upgrade/downgrade) and must
    # go through the /billing endpoints:
    #   free -> paid   : POST /billing/checkout
    #   paid -> paid   : POST /billing/change-plan
    #   paid -> free   : POST /billing/cancel


# ------------------------ update projecy -----------------------


@router.put("/projects/{project_id}", tags=["projects"])
async def update_project(
    project_id: uuid.UUID,
    body: UpdateProjectRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update a project's name. Plan changes go through /billing endpoints."""
    user_id = _require_user(request)

    result = await db.execute(
        select(Project).where(
            Project.id == project_id,
            Project.owner_id == user_id,
        )
    )
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project.name = body.name.strip()
    await db.commit()
    await db.refresh(project)

    return {
        "id": project.id,
        "name": project.name,
        "plan": project.plan,
        "created_at": project.created_at.isoformat(),
    }


# ------------------------------ delete project -------------------------------


@router.delete("/projects/{project_id}", status_code=204, tags=["projects"])
async def delete_project(
    project_id: uuid.UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Delete a project. Only the owner can delete it."""
    user_id = _require_user(request)

    result = await db.execute(
        select(Project).where(
            Project.id == project_id,
            Project.owner_id == user_id,
        )
    )
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    await db.delete(project)
    await db.commit()
