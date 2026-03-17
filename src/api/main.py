"""
FastAPI Dashboard for Dragon Pulse — Deterministic A-Share Scanner

Serves v4 scan results (MR-only, acceptance layer, 0.95 ATR stop).
"""

from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

from src.utils.time import utc_now

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    logger.warning("FastAPI not installed. Install with: pip install fastapi uvicorn")


if FASTAPI_AVAILABLE:

    # ----- Pydantic Models -----

    class HealthResponse(BaseModel):
        status: str
        timestamp: str
        version: str

    class RunSummary(BaseModel):
        date: str
        regime: str = ""
        picks_count: int = 0
        signals_total: int = 0
        acceptance_mode: str = ""
        day_quality_score: float = 0
        eligible_count: int = 0

    # ----- Helper Functions -----

    def get_outputs_dir() -> Path:
        return Path("outputs")

    def get_available_dates() -> list[str]:
        outputs_dir = get_outputs_dir()
        if not outputs_dir.exists():
            return []
        dates = []
        for p in outputs_dir.iterdir():
            if p.is_dir() and len(p.name) == 10 and p.name[4] == '-':
                dates.append(p.name)
        return sorted(dates, reverse=True)

    def load_scan_results(date_str: str) -> Optional[dict]:
        """Load scan_results JSON for a date (v4 format)."""
        path = get_outputs_dir() / date_str / f"scan_results_{date_str}.json"
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to load scan results for %s: %s", date_str, e)
            return None

    def load_watchlist(date_str: str) -> Optional[dict]:
        """Load execution_watchlist JSON for a date."""
        path = get_outputs_dir() / date_str / f"execution_watchlist_{date_str}.json"
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to load watchlist for %s: %s", date_str, e)
            return None

    # ----- FastAPI App -----

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Starting Dragon Pulse API...")
        yield
        logger.info("Shutting down API...")

    def create_app(
        title: str = "Dragon Pulse API",
        version: str = "4.0.0",
        cors_origins: list[str] = None,
    ) -> FastAPI:
        app = FastAPI(
            title=title,
            version=version,
            description="REST API for Dragon Pulse — Deterministic A-Share Scanner",
            lifespan=lifespan,
        )

        if cors_origins is None:
            cors_origins = ["http://localhost:3000", "http://localhost:8000"]

        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Mount static files
        static_dir = Path(__file__).resolve().parent.parent.parent / "static"
        if static_dir.exists():
            app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @app.get("/", response_class=HTMLResponse)
        async def root():
            index = static_dir / "dashboard.html"
            if index.exists():
                return FileResponse(str(index), media_type="text/html")
            return HTMLResponse("<h1>Dashboard not found.</h1>")

        @app.get("/api/latest")
        async def get_latest_run():
            """Return the most recent scan results."""
            dates = get_available_dates()
            if not dates:
                raise HTTPException(status_code=404, detail="No runs available")
            latest = dates[0]
            scan = load_scan_results(latest)
            if not scan:
                raise HTTPException(status_code=404, detail=f"No data for {latest}")
            scan["date"] = latest
            return scan

        @app.post("/api/alert/test")
        async def send_test_alert():
            try:
                from src.core.alerts import AlertManager, AlertConfig
                cfg = AlertConfig(enabled=True, channels=["telegram"])
                mgr = AlertManager(cfg)
                results = mgr.send_alert(
                    title="Dragon Pulse Test Alert",
                    message="If you see this, Telegram is configured correctly.",
                    priority="low",
                )
                success = results.get("telegram", False)
                return {"success": success, "results": results}
            except Exception as e:
                return {"success": False, "error": str(e)}

        @app.get("/health", response_model=HealthResponse)
        async def health_check():
            return HealthResponse(
                status="healthy",
                timestamp=utc_now().isoformat().replace("+00:00", ""),
                version=version,
            )

        @app.get("/runs")
        async def list_runs(
            limit: int = Query(default=30, ge=1, le=100),
            offset: int = Query(default=0, ge=0),
        ):
            dates = get_available_dates()
            paginated = dates[offset:offset + limit]

            summaries = []
            for d in paginated:
                scan = load_scan_results(d)
                if scan:
                    rd = scan.get("regime_detail", {})
                    summaries.append(RunSummary(
                        date=d,
                        regime=scan.get("regime", ""),
                        picks_count=len(scan.get("picks", [])),
                        signals_total=scan.get("signals_total", 0),
                        acceptance_mode=rd.get("acceptance_mode", ""),
                        day_quality_score=rd.get("day_quality_score", 0),
                        eligible_count=rd.get("acceptance_eligible_count", 0),
                    ))

            return {
                "total": len(dates),
                "limit": limit,
                "offset": offset,
                "runs": summaries,
            }

        @app.get("/runs/{date_str}")
        async def get_run(date_str: str):
            scan = load_scan_results(date_str)
            if not scan:
                raise HTTPException(status_code=404, detail=f"Run not found: {date_str}")
            return scan

        @app.get("/runs/{date_str}/picks")
        async def get_picks(date_str: str):
            """Get execution watchlist picks for a date."""
            wl = load_watchlist(date_str)
            if not wl:
                scan = load_scan_results(date_str)
                if scan:
                    return {"date": date_str, "picks": scan.get("picks", [])}
                raise HTTPException(status_code=404, detail=f"No picks for {date_str}")
            return wl

        @app.get("/tickers/{ticker}")
        async def get_ticker_history(
            ticker: str,
            limit: int = Query(default=10, ge=1, le=50),
        ):
            ticker = ticker.upper()
            dates = get_available_dates()[:50]

            appearances = []
            for d in dates:
                scan = load_scan_results(d)
                if not scan:
                    continue
                for p in scan.get("picks", []):
                    if p.get("ticker") == ticker:
                        appearances.append({
                            "date": d,
                            "engine": p.get("engine", ""),
                            "score": p.get("score", 0),
                            "entry_price": p.get("entry_price", 0),
                        })
                        break
                if len(appearances) >= limit:
                    break

            return {
                "ticker": ticker,
                "appearances": appearances,
                "count": len(appearances),
            }

        # Register engine endpoint for cross-engine integration (MAS compat)
        try:
            from src.api.engine_endpoint import (
                _to_legacy_picks_payload,
                get_engine_results as _get_engine_results,
                router as engine_router,
            )
            app.include_router(engine_router)

            @app.get("/api/picks")
            async def get_legacy_picks():
                """Compatibility endpoint used by MAS KooCore adapter."""
                payload = await _get_engine_results()
                return _to_legacy_picks_payload(payload)
        except ImportError:
            pass

        return app

    def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
        try:
            import uvicorn
        except ImportError:
            logger.error("uvicorn not installed.")
            return
        app = create_app()
        uvicorn.run(app, host=host, port=port, reload=reload)

    # Module-level app instance for Heroku / uvicorn deployment
    app = create_app(cors_origins=["*"])

else:
    app = None

    def create_app(*args, **kwargs):
        raise ImportError("FastAPI not installed.")

    def run_server(*args, **kwargs):
        raise ImportError("FastAPI not installed.")
