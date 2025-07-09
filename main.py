from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime
import re
import logging
import db
from fetch_level4_for_date import calculate_level4_visits
from io import StringIO
import requests

app = FastAPI(title="Metrika Score API", version="1.0.0")

# --- Логирование ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrika-api")

@app.on_event("startup")
def startup():
    db.init_db()

# --- Pydantic модели ---
class TaskRequest(BaseModel):
    date: str = Field(..., example="2025-07-01")
    token: Optional[str]
    counter: Optional[int]

    @validator("date")
    def validate_date(cls, v):
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("date must be in YYYY-MM-DD format")
        return v

    @validator("counter")
    def validate_counter(cls, v):
        if v is not None and v <= 0:
            raise ValueError("counter must be positive integer")
        return v

class TaskResponse(BaseModel):
    task_id: str

class TaskStatusResponse(BaseModel):
    status: str
    progress: Optional[int] = 0
    message: Optional[str] = ""
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str] = None

class VisitResult(BaseModel):
    visitId: str
    clientId: str
    dateTime: str
    visitDuration: int

# Конверсии всегда отправляются на цель "4plus", без цены.

# --- Глобальный обработчик ошибок ---
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error", "error": str(exc)})

# --- Эндпоинты ---
@app.post("/tasks", response_model=TaskResponse, status_code=202)
def create_task(req: TaskRequest, background_tasks: BackgroundTasks):
    # Проверка обязательных параметров
    if not req.token:
        raise HTTPException(status_code=400, detail="token is required (in body or env)")
    if not req.counter:
        raise HTTPException(status_code=400, detail="counter is required (in body or env)")
    task_id = str(uuid4())
    db.create_task(task_id, req.dict())
    background_tasks.add_task(run_task, task_id, req)
    return {"task_id": task_id}

def run_task(task_id: str, req: TaskRequest):
    try:
        logger.info(f"[task {task_id}] Старт расчёта для {req.date}")
        db.update_task_status(task_id, status="running", progress=10, message="Выполнение расчёта в Метрике")
        def log_api(msg):
            logger.info(f"[task {task_id}] {msg}")
        result = calculate_level4_visits(req.date, req.token, req.counter, logger=log_api)
        db.save_result(task_id, result)
        logger.info(f"[task {task_id}] Готово! Визитов 4+ = {len(result)}")
        db.update_task_status(task_id, status="done", progress=100, message="Готово", finished_at=datetime.utcnow().isoformat())
    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
        db.update_task_status(task_id, status="failed", progress=100, message="Ошибка", error=str(e), finished_at=datetime.utcnow().isoformat())

@app.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
def get_task_status(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # Преобразуем даты в datetime
    started_at = datetime.fromisoformat(task["started_at"]) if task["started_at"] else None
    finished_at = datetime.fromisoformat(task["finished_at"]) if task["finished_at"] else None
    return TaskStatusResponse(
        status=task["status"],
        progress=task["progress"],
        message=task["message"],
        started_at=started_at,
        finished_at=finished_at,
        error=task["error"]
    )

@app.get("/tasks/{task_id}/result", response_model=List[VisitResult])
def get_task_result(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != "done":
        return JSONResponse(status_code=202, content={"detail": "Task not finished"})
    result = db.get_result(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return result

@app.post("/tasks/{task_id}/offline-conversions", status_code=202)
def send_offline_conversions(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != "done":
        raise HTTPException(status_code=409, detail="Task not finished")
    result = db.get_result(task_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")

    params = task["params"]
    token = params.get("token")
    counter = params.get("counter")
    if not token or not counter:
        raise HTTPException(status_code=400, detail="Missing token or counter in task params")

    # Build CSV: ClientId;Target;DateTime (DateTime = Unix timestamp, c заголовком)
    csv_buffer = StringIO()
    csv_buffer.write("ClientId,Target,DateTime\n")
    for row in result:
        dt = int(datetime.strptime(row['dateTime'], "%Y-%m-%d %H:%M:%S").timestamp())
        csv_buffer.write(f"{row['clientId']},4plus,{dt}\n")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/offline_conversions/upload?client_id_type=CLIENT_ID&format=csv&return_result=true"
    headers = {"Authorization": f"OAuth {token}"}
    files = {"file": ("conversions.csv", csv_bytes, "text/csv")}

    # --- Новое логирование содержимого запроса ---
    masked_token = token[:4] + "***" + token[-4:]
    headers_for_log = {"Authorization": f"OAuth {masked_token}"}
    csv_preview_lines = 5
    csv_preview = "\n".join(csv_buffer.getvalue().splitlines()[:csv_preview_lines])
    logger.info(
        f"[task {task_id}] Отправка офлайн-конверсий в Метрику.\n"
        f"URL: {url}\n"
        f"Headers: {headers_for_log}\n"
        f"CSV (первые {csv_preview_lines} строк):\n{csv_preview}"
    )

    r = requests.post(url, headers=headers, files=files)
    if not r.ok:
        logger.error(f"[task {task_id}] Offline conversions error: {r.status_code} {r.text}")
        raise HTTPException(status_code=502, detail=f"Metrika error: {r.text}")

    logger.info(f"[task {task_id}] Offline conversions uploaded: goal=4plus, rows={len(result)}")
    return {"uploaded": len(result), "response": r.json()} 