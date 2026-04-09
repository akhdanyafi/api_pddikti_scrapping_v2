"""
PDDikti Dosen Explorer — FastAPI Main Entry Point
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from database import init_db, AsyncSessionLocal
from auth import create_default_admin

from routers.auth_router import router as auth_router
from routers.admin_router import router as admin_router
from routers.dosen_router import router as dosen_router
from routers.scrape_router import router as scrape_router
from routers.stats_router import router as stats_router
from routers.prodi_router import router as prodi_router

settings = get_settings()


# ── Background task: auto-logout inactive sessions ──
async def session_cleanup_task():
    """Periodically check and invalidate expired sessions."""
    from datetime import datetime, timezone
    from sqlalchemy import select, update, and_
    from models import UserSession, User

    while True:
        try:
            await asyncio.sleep(60)  # Check every minute
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(UserSession).where(UserSession.is_active == True)
                )
                sessions = result.scalars().all()

                for session in sessions:
                    if session.last_activity:
                        inactivity = (
                            datetime.now(timezone.utc) -
                            session.last_activity.replace(tzinfo=timezone.utc)
                        ).total_seconds()

                        if inactivity > settings.INACTIVITY_TIMEOUT:
                            session.is_active = False
                            await db.execute(
                                update(User).where(User.id == session.user_id).values(
                                    is_online=False,
                                    last_logout=datetime.now(timezone.utc)
                                )
                            )

                await db.commit()
        except Exception as e:
            print(f"Session cleanup error: {e}")


# ── Background task: check if scraping is active for auto-logout ──
async def scraping_activity_checker():
    """
    If no scraping job is running and no recent activity,
    auto-logout idle users.
    """
    from datetime import datetime, timezone
    from sqlalchemy import select, and_
    from models import UserSession, User, ScrapeJob

    while True:
        try:
            await asyncio.sleep(120)  # Check every 2 minutes
            async with AsyncSessionLocal() as db:
                # Check if any scraping is active
                running_jobs = await db.execute(
                    select(ScrapeJob).where(ScrapeJob.status == "running")
                )
                has_running = running_jobs.scalar_one_or_none() is not None

                if has_running:
                    # If scraping is active, extend all active sessions
                    result = await db.execute(
                        select(UserSession).where(UserSession.is_active == True)
                    )
                    for session in result.scalars().all():
                        session.last_activity = datetime.now(timezone.utc)
                    await db.commit()

        except Exception as e:
            print(f"Scraping activity checker error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    print("🚀 Starting PDDikti Dosen Explorer...")
    await init_db()
    print("✅ Database tables created/verified")

    # Create default admin
    async with AsyncSessionLocal() as db:
        await create_default_admin(db)

    # Start background tasks
    cleanup_task = asyncio.create_task(session_cleanup_task())
    activity_task = asyncio.create_task(scraping_activity_checker())

    yield

    # Shutdown
    cleanup_task.cancel()
    activity_task.cancel()
    print("🛑 Shutting down...")


app = FastAPI(
    title="PDDikti Dosen Explorer API",
    description="API untuk scraping dan menampilkan data dosen dari PDDikti",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow Vercel frontend to call backend API
cors_origins = (
    ["*"] if settings.CORS_ORIGINS == "*"
    else [o.strip() for o in settings.CORS_ORIGINS.split(",")]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(dosen_router)
app.include_router(scrape_router)
app.include_router(stats_router)
app.include_router(prodi_router)


@app.get("/")
async def root():
    return {
        "message": "PDDikti Dosen Explorer API",
        "version": "1.0.0",
        "docs": "/docs"
    }


@app.get("/api_v2/health")
async def health():
    return {"status": "ok"}
