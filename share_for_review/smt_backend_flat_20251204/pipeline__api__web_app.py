from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, PlainTextResponse
from pathlib import Path

from pipeline.api.web_v2 import router as web_v2_router

BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = BASE_DIR / "web" / "static"

app = FastAPI(title="SMT Local Web v2")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.include_router(web_v2_router, prefix="/web")


@app.get("/", response_class=HTMLResponse)
async def root():
    # minimal landing
    html = '<!doctype html><html><head><meta charset="utf-8"><title>SMT Web</title></head><body><a href="/web/monitoring/">Открыть Monitoring v2</a></body></html>'
    return HTMLResponse(html)


@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"
