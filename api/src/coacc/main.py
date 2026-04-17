import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncSession
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from coacc.config import settings
from coacc.dependencies import close_driver, get_session, init_driver
from coacc.middleware.rate_limit import limiter
from coacc.middleware.security_headers import SecurityHeadersMiddleware
from coacc.routers import (
    auth,
    baseline,
    cases,
    entity,
    graph,
    investigation,
    meta,
    patterns,
    public,
    search,
    signals,
)
from coacc.services.neo4j_service import ensure_schema, execute_query_single

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    weak_or_default_jwt = (
        settings.jwt_secret_key == "change-me-in-production"
        or len(settings.jwt_secret_key) < 32
    )
    if weak_or_default_jwt:
        msg = "JWT secret is weak or default — set JWT_SECRET_KEY env var (>= 32 chars)"
        app_env = settings.app_env.strip().lower()
        if app_env in {"dev", "test"}:
            _logger.warning("%s [allowed in %s]", msg, app_env)
        else:
            _logger.critical(msg)
            raise RuntimeError(msg)
    app_env = settings.app_env.strip().lower()
    if app_env not in {"dev", "test"} and settings.neo4j_password == "changeme":
        msg = "Neo4j default password not allowed in production — set NEO4J_PASSWORD"
        _logger.critical(msg)
        raise RuntimeError(msg)
    driver = await init_driver()
    app.state.neo4j_driver = driver
    await ensure_schema(driver)
    yield
    await close_driver()


app = FastAPI(
    title="CO-ACC API",
    description="Colombian public data graph analysis tool",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SecurityHeadersMiddleware, app_env=settings.app_env)

app.include_router(meta.router)
app.include_router(public.router)
app.include_router(auth.router)
app.include_router(entity.router)
app.include_router(search.router)
app.include_router(graph.router)
app.include_router(patterns.router)
app.include_router(signals.router)
app.include_router(baseline.router)
app.include_router(investigation.router)
app.include_router(investigation.shared_router)
app.include_router(cases.router)


@app.get("/health")
async def health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str | int | None]:
    latest_run = await execute_query_single(session, "signal_latest_completed_run")
    return {
        "status": "ok",
        "last_signal_run_id": (
            str(latest_run["run_id"]) if latest_run and latest_run["run_id"] is not None else None
        ),
        "last_signal_run_at": (
            str(latest_run["finished_at"])
            if latest_run and latest_run["finished_at"] is not None
            else None
        ),
        "last_signal_run_status": (
            str(latest_run["status"])
            if latest_run and "status" in latest_run and latest_run["status"] is not None
            else None
        ),
        "last_signal_hit_count": (
            latest_run["hit_count"]
            if latest_run
            and "hit_count" in latest_run
            and latest_run["hit_count"] is not None
            else None
        ),
    }
