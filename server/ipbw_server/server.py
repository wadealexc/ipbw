import os

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security.api_key import APIKey
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from .background import batch_insert_worker
from .broadcast import broadcaster
from .database import Repository
from .schema import CrawlResult
from .util import validate_api_key

IPBW_DSN = os.environ.get(
    "IPBW_DATABASE", "postgres://postgres:postgres@localhost:5432/ipbw"
)

app = FastAPI(title="IPBW Ingestion API", version="1.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    repo = Repository(IPBW_DSN)
    repo.initialize_tables()
    await broadcaster.broadcaster.asend(None)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", context={"request": request})


@app.get("/healthcheck")
async def healthcheck(api_key: APIKey = Depends(validate_api_key)):
    logger.info("Handling healthcheck request")
    return {"status": "ok"}


@app.post("/batch")
async def post_crawl_results(
    result: CrawlResult,
    background: BackgroundTasks,
    api_key: APIKey = Depends(validate_api_key),
):
    logger.info("Handling crawler batch submission")
    background.add_task(batch_insert_worker, result)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await broadcaster.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Message text was: {data}")
    except WebSocketDisconnect:
        broadcaster.remove(websocket)
