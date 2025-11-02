import os
import random
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application starting up...")
    yield
    print("Application shutting down...")
    await client.aclose()

app = FastAPI(title="Proxy service", version="1.0.0", lifespan=lifespan)

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))

client = httpx.AsyncClient(timeout=30)

@app.get("/health", tags=["health"])
async def health() -> Response:
    return Response(content="Proxy service is healthy", media_type="text/plain")

@app.api_route("/api/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def proxy(full_path: str, request: Request) -> Response:
    service_url = MONOLITH_URL
    if full_path.startswith("events"):
        service_url = EVENTS_SERVICE_URL
    if GRADUAL_MIGRATION and full_path.startswith("movies"):
        rnd = random.randint(1, 100)
        if rnd <= MOVIES_MIGRATION_PERCENT:
            service_url = MOVIES_SERVICE_URL

    return await _response(request, service_url)

async def _response(request: Request, base_url: str) -> Response:
    url = httpx.URL(base_url + request.url.path)
    if request.url.query:
        url = url.copy_with(query=request.url.query.encode('utf-8') if request.url.query else None)

    try:
        resp = await client.request(
            url=url,
            method=request.method,
            content=await request.body(),
            headers={key: value for key, value in request.headers.items() if
                   key.lower() not in {"host", "content-length", "transfer-encoding"}},
        )

        return Response(content=resp.content, status_code=resp.status_code, headers=resp.headers)
    except httpx.RequestError as e:
        return JSONResponse(status_code=status.HTTP_502_BAD_GATEWAY, content={"error": f"Request failed: {e}"})
