from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, PlainTextResponse

from pipeline.api.web_v2 import router as web_v2_router

BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = BASE_DIR / "web" / "static"

app = FastAPI(title="SMT Local Web v2")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), check_dir=False), name="static")
app.include_router(web_v2_router, prefix="/web")


@app.get("/", response_class=HTMLResponse)
async def root():
    # Minimal landing: backend status + hint about SPA frontend.
    html = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>SMT Web Backend</title>
  </head>
  <body>
    <h1>SMT Web backend работает</h1>
    <p>API доступно под префиксом <code>/web/api/...</code>.</p>
    <p>Для нового фронтенда SPA используйте каталог <code>web-ui/</code> (Vite/React) и команду <code>npm run dev</code>.</p>
  </body>
</html>
"""
    return HTMLResponse(html)


@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"
