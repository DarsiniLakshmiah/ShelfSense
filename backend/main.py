"""
ShelfSense FastAPI backend.

Run: uvicorn backend.main:app --reload --port 8000
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routers import prices, recommendations, signals, summary

app = FastAPI(title="ShelfSense API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(summary.router,         prefix="/api/summary",         tags=["summary"])
app.include_router(prices.router,          prefix="/api/prices",          tags=["prices"])
app.include_router(recommendations.router, prefix="/api/recommendations", tags=["recommendations"])
app.include_router(signals.router,         prefix="/api/signals",         tags=["signals"])


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}
