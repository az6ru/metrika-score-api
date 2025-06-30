from fastapi import FastAPI, BackgroundTasks, HTTPException, Request, Query, Header, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import uuid4
from datetime import datetime
import re
import logging
import tempfile
import os
import aiohttp
from app.supabase_db import create_task as db_create_task, update_task_status, save_result, get_task, get_result
from app.supabase_db import create_conversion_upload, update_conversion_status, get_conversion_upload, get_conversions_by_task
from app.fetch_level4_for_date import calculate_level4_visits
from app.send_conversions import send_conversions_to_metrika, check_conversion_status, format_conversion_csv, format_single_conversion_csv
from contextlib import asynccontextmanager
from app.pydantic_models import TaskRequest, TaskResponse, TaskStatusResponse, VisitResult, PaginatedVisitResults
from app.pydantic_models import BulkConversionRequest, SingleConversionRequest, ConversionResponse, ConversionStatusResponse
from app.pydantic_models import WebhookCreateRequest, WebhookCreateResponse, OfflineConversionWebhookRequest, OfflineConversionWebhookResponse, OfflineConversionStatusResponse
from app.supabase_db import create_webhook, get_webhook, save_webhook_conversions, get_webhook_batch, update_webhook_batch_status
from app.send_webhook_conversions import process_webhook_batch, check_webhook_batch_status

# --- Логирование ---
# Настройка логирования в файл и консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("metrika-api")

# --- Настройки API ---
API_BASE_URL = os.environ.get("API_BASE_URL", "http://0.0.0.0:8000")

# --- Middleware для логирования запросов и ответов ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

# Middleware для логирования запросов и ответов
async def log_request_middleware(request: Request, call_next):
    # Логируем входящий запрос
    request_id = str(uuid4())
    logger.info(f"Request [{request_id}]: {request.method} {request.url.path}")
    
    # Пытаемся логировать тело запроса для POST/PUT запросов
    if request.method in ["POST", "PUT"]:
        try:
            body = await request.body()
            if body:
                # Ограничиваем размер логируемого тела запроса
                body_str = body.decode('utf-8')
                if len(body_str) > 1000:
                    body_str = body_str[:1000] + "... [truncated]"
                logger.info(f"Request body [{request_id}]: {body_str}")
                # Восстанавливаем тело запроса, чтобы его можно было прочитать снова
                await request._body_reset()
        except Exception as e:
            logger.warning(f"Failed to log request body [{request_id}]: {str(e)}")
    
    # Обрабатываем запрос
    response = await call_next(request)
    
    # Логируем ответ
    logger.info(f"Response [{request_id}]: {response.status_code}")
    
    return response

app = FastAPI(
    title="Metrika Score API", 
    version="1.0.0", 
    lifespan=lifespan,
    description="""
    API для работы с Яндекс.Метрикой:
    
    1. Расчет скоринга визитов на основе ML-моделей
    2. Отправка конверсий в Яндекс.Метрику (скоринговые и одиночные)
    3. Прием офлайн-конверсий через вебхуки
    
    Документация по API Яндекс.Метрики: https://yandex.ru/dev/metrika/doc/api2/
    """
)

# Добавляем middleware для логирования
app.middleware("http")(log_request_middleware)

# --- Совместимость pydantic v1/v2 ---
try:
    from pydantic import field_validator  # type: ignore
except ImportError:  # pydantic v1
    from pydantic import validator as field_validator  # type: ignore

# --- Pydantic модели ---
class TaskRequest(BaseModel):
    date: str = Field(..., json_schema_extra={"example": "2025-07-01"})
    token: Optional[str]
    counter: Optional[int]

    @field_validator("date")
    @classmethod
    def validate_date(cls, v):
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("date must be in YYYY-MM-DD format")
        return v

    @field_validator("counter")
    @classmethod
    def validate_counter(cls, v):
        if v is not None and v <= 0:
            raise ValueError("counter must be positive integer")
        return v

    # Для совместимости с pydantic v1
    def model_dump(self, *args, **kwargs):  # type: ignore
        return self.dict(*args, **kwargs)

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

# --- Глобальный обработчик ошибок ---
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error", "error": str(exc)})

# --- Эндпоинты ---
@app.post("/tasks", response_model=TaskResponse, status_code=202)
async def create_task(req: TaskRequest, background_tasks: BackgroundTasks):
    if req.token is None:
        raise HTTPException(status_code=422, detail="token is required (in body or env)")
    if req.token == "":
        raise HTTPException(status_code=400, detail="token must be non-empty")
    if not req.counter:
        raise HTTPException(status_code=422, detail="counter is required (in body or env)")
    task_id = str(uuid4())
    await db_create_task(task_id, req.model_dump())
    background_tasks.add_task(run_task, task_id, req)
    return {"task_id": task_id}

async def run_task(task_id: str, req: TaskRequest):
    try:
        logger.info(f"[task {task_id}] Старт расчёта для {req.date}")
        await update_task_status(task_id, status="running", progress=10, message="Выполнение расчёта в Метрике")
        def log_api(msg):
            logger.info(f"[task {task_id}] {msg}")
        result = calculate_level4_visits(req.date, req.token, req.counter, logger=log_api)
        await save_result(task_id, result)
        logger.info(f"[task {task_id}] Готово! Визитов 4+ = {len(result)}")
        await update_task_status(task_id, status="done", progress=100, message="Готово", finished_at=datetime.utcnow().isoformat())
    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
        await update_task_status(task_id, status="failed", progress=100, message="Ошибка", error=str(e), finished_at=datetime.utcnow().isoformat())

@app.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    task = await get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
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

@app.get(
    "/tasks/{task_id}/result", 
    response_model=PaginatedVisitResults,
    summary="Получить результаты задачи с пагинацией",
    description="""
    Возвращает результаты выполненной задачи с поддержкой пагинации.
    
    - Если задача не найдена, возвращает 404 Not Found
    - Если задача еще не завершена, возвращает 202 Accepted
    - Если задача завершена, но результаты не найдены, возвращает 404 Not Found
    
    Параметры пагинации:
    - limit: количество записей на странице (от 1 до 1000, по умолчанию 100)
    - offset: смещение от начала списка (от 0, по умолчанию 0)
    """
)
async def get_task_result(
    task_id: str, 
    limit: int = Query(100, ge=1, le=1000, description="Количество записей на странице (от 1 до 1000)"), 
    offset: int = Query(0, ge=0, description="Смещение (начиная с 0)")
):
    task = await get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    result = await get_result(task_id)
    if result is None:
        if task["status"] != "done":
            return JSONResponse(status_code=202, content={"detail": "Task not finished"})
        else:
            raise HTTPException(status_code=404, detail="Result not found")
    
    # Применяем пагинацию к результату
    total = len(result)
    paginated = result[offset:offset+limit]
    
    return {
        "data": paginated,
        "pagination": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total
        }
    }

# --- Эндпоинты для работы с конверсиями ---

@app.post(
    "/conversions/bulk", 
    response_model=ConversionResponse,
    summary="Отправка массовых конверсий на основе результатов задачи",
    description="""
    Отправляет массовые конверсии в Яндекс.Метрику на основе результатов задачи.
    
    - Получает результаты задачи по task_id
    - Формирует CSV с конверсиями для всех визитов
    - Отправляет данные в API Яндекс.Метрики
    - Возвращает ID загрузки и статус
    
    Для проверки статуса загрузки используйте эндпоинт `/conversions/{upload_id}/status`
    """
)
async def send_bulk_conversions(req: BulkConversionRequest, background_tasks: BackgroundTasks):
    """Отправляет массовые конверсии на основе результатов задачи"""
    # Получаем результаты задачи
    task = await get_task(req.task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    result = await get_result(req.task_id)
    if result is None:
        if task["status"] != "done":
            raise HTTPException(status_code=202, detail="Task not finished yet")
        else:
            raise HTTPException(status_code=404, detail="Task results not found")
    
    # Формируем CSV
    csv_data = format_conversion_csv(result, req.target)
    
    # Отправляем в Метрику
    try:
        response = await send_conversions_to_metrika(req.counter, req.token, csv_data)
        
        # Сохраняем информацию о загрузке
        upload_id = await create_conversion_upload(
            metrika_upload_id=response["upload_id"],
            task_id=req.task_id,
            counter=req.counter,
            target=req.target,
            total_conversions=len(result)
        )
        
        # Запускаем фоновую задачу для проверки статуса
        background_tasks.add_task(
            check_conversion_upload_status, 
            upload_id=upload_id,
            metrika_upload_id=response["upload_id"],
            counter=req.counter,
            token=req.token
        )
        
        return {
            "upload_id": upload_id,
            "status": response["status"]
        }
    except HTTPException as e:
        logger.error(f"Error sending conversions: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending conversions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error sending conversions: {str(e)}")

@app.post(
    "/conversions/single", 
    response_model=ConversionResponse,
    summary="Отправка одиночной конверсии",
    description="""
    Отправляет одиночную конверсию в Яндекс.Метрику.
    
    - Формирует CSV с одной конверсией на основе переданных параметров
    - Отправляет данные в API Яндекс.Метрики
    - Возвращает ID загрузки и статус
    
    Для проверки статуса загрузки используйте эндпоинт `/conversions/{upload_id}/status`
    """
)
async def send_single_conversion(req: SingleConversionRequest, background_tasks: BackgroundTasks):
    """Отправляет одиночную конверсию"""
    # Формируем CSV
    csv_data = format_single_conversion_csv(
        target=req.target,
        date_time=req.date_time,
        client_id=req.client_id,
        user_id=req.user_id,
        yclid=req.yclid,
        purchase_id=req.purchase_id,
        price=req.price,
        currency=req.currency
    )
    
    # Отправляем в Метрику
    try:
        response = await send_conversions_to_metrika(req.counter, req.token, csv_data)
        
        # Сохраняем информацию о загрузке
        upload_id = await create_conversion_upload(
            metrika_upload_id=response["upload_id"],
            task_id=None,  # Для одиночной конверсии нет task_id
            counter=req.counter,
            target=req.target,
            total_conversions=1
        )
        
        # Запускаем фоновую задачу для проверки статуса
        background_tasks.add_task(
            check_conversion_upload_status, 
            upload_id=upload_id,
            metrika_upload_id=response["upload_id"],
            counter=req.counter,
            token=req.token
        )
        
        return {
            "upload_id": upload_id,
            "status": response["status"]
        }
    except HTTPException as e:
        logger.error(f"Error sending conversion: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending conversion: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error sending conversion: {str(e)}")

@app.get(
    "/conversions/{upload_id}/status", 
    response_model=ConversionStatusResponse,
    summary="Проверка статуса загрузки конверсий",
    description="""
    Проверяет статус загрузки конверсий в Яндекс.Метрику.
    
    - Получает информацию о загрузке из базы данных
    - Если загрузка еще в процессе, запрашивает актуальный статус из API Яндекс.Метрики
    - Возвращает статус, количество обработанных конверсий и ошибки (если есть)
    """
)
async def get_conversion_status(upload_id: str):
    """Проверяет статус загрузки конверсий"""
    # Получаем информацию о загрузке
    upload = await get_conversion_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Conversion upload not found")
    
    return {
        "upload_id": upload_id,
        "status": upload["status"],
        "errors": upload.get("errors"),
        "processed": upload.get("processed_conversions"),
        "total": upload.get("total_conversions")
    }

@app.get(
    "/tasks/{task_id}/conversions", 
    response_model=List[ConversionStatusResponse],
    summary="Получение всех конверсий для задачи",
    description="""
    Возвращает список всех загрузок конверсий для указанной задачи.
    
    - Получает все загрузки конверсий, связанные с задачей
    - Возвращает список с информацией о каждой загрузке
    """
)
async def get_task_conversions(task_id: str):
    """Получает все конверсии для задачи"""
    # Проверяем существование задачи
    task = await get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Получаем все загрузки для задачи
    conversions = await get_conversions_by_task(task_id)
    
    # Формируем ответ
    return [
        {
            "upload_id": conv["id"],
            "status": conv["status"],
            "errors": conv.get("errors"),
            "processed": conv.get("processed_conversions"),
            "total": conv.get("total_conversions")
        }
        for conv in conversions
    ]

async def check_conversion_upload_status(
    upload_id: str,
    metrika_upload_id: str,
    counter: int,
    token: str,
    max_attempts: int = 10,
    delay_seconds: int = 30
):
    """
    Фоновая задача для проверки статуса загрузки конверсий.
    
    Args:
        upload_id: ID загрузки в базе данных
        metrika_upload_id: ID загрузки в Яндекс.Метрике
        counter: ID счетчика
        token: OAuth-токен
        max_attempts: Максимальное количество попыток
        delay_seconds: Задержка между попытками в секундах
    """
    import asyncio
    
    for attempt in range(max_attempts):
        try:
            # Проверяем статус в Метрике
            status_info = await check_conversion_status(counter, token, metrika_upload_id)
            
            # Обновляем статус в базе
            await update_conversion_status(
                upload_id=upload_id,
                status=status_info["status"],
                processed_conversions=status_info.get("processed"),
                errors=status_info.get("errors")
            )
            
            # Если загрузка завершена, выходим из цикла
            if status_info["status"] in ("processed", "failed"):
                logger.info(f"Conversion upload {upload_id} finished with status: {status_info['status']}")
                break
                
            logger.info(f"Conversion upload {upload_id} status: {status_info['status']}, attempt {attempt+1}/{max_attempts}")
            
        except Exception as e:
            logger.error(f"Error checking conversion status for {upload_id}: {str(e)}")
            
        # Ждем перед следующей попыткой
        await asyncio.sleep(delay_seconds) 

# --- Эндпоинты для работы с вебхуками офлайн-конверсий ---

@app.post(
    "/webhook/offline-conversions",
    response_model=WebhookCreateResponse,
    summary="Создать новый вебхук для офлайн-конверсий",
    description="""
    Создает новый вебхук для приема офлайн-конверсий.
    
    - Генерирует уникальный идентификатор вебхука и секретный ключ
    - Сохраняет информацию о вебхуке в базе данных
    - Возвращает URL для отправки конверсий и секретный ключ
    
    Полученный URL можно использовать для настройки вебхука в вашей системе.
    Секретный ключ необходимо передавать в заголовке X-Webhook-Secret при отправке конверсий.
    """
)
async def create_offline_conversion_webhook(req: WebhookCreateRequest):
    """Создает новый вебхук для офлайн-конверсий"""
    try:
        # Создаем вебхук
        webhook_data = await create_webhook(
            name=req.name,
            counter_id=req.counter_id,
            token=req.token,
            description=req.description
        )
        
        return WebhookCreateResponse(
            webhook_id=webhook_data["webhook_id"],
            secret=webhook_data["secret"],
            url=webhook_data["url"]
        )
    except Exception as e:
        logger.error(f"Error creating webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating webhook: {str(e)}")

async def verify_webhook_secret(webhook_id: str, x_webhook_secret: str = Header(None)):
    """
    Проверяет секретный ключ вебхука.
    
    Args:
        webhook_id: ID вебхука
        x_webhook_secret: Секретный ключ из заголовка
        
    Returns:
        Информация о вебхуке
    """
    # Получаем информацию о вебхуке
    webhook = await get_webhook(webhook_id)
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    # Проверяем активность вебхука
    if not webhook.get("is_active", True):
        raise HTTPException(status_code=403, detail="Webhook is disabled")
    
    # Проверяем секретный ключ
    if not x_webhook_secret:
        raise HTTPException(status_code=401, detail="X-Webhook-Secret header is required")
    
    if webhook["secret"] != x_webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")
    
    return webhook

@app.post(
    "/webhook/offline-conversions/{webhook_id}",
    response_model=OfflineConversionWebhookResponse,
    summary="Приём офлайн-конверсий через вебхук",
    description="""
    Принимает офлайн-конверсии через вебхук и отправляет их в Яндекс.Метрику.
    
    - Проверяет секретный ключ вебхука в заголовке X-Webhook-Secret
    - Валидирует входящие данные
    - Сохраняет конверсии в базе данных
    - Запускает фоновую задачу для отправки конверсий в Яндекс.Метрику
    
    Для проверки статуса отправки используйте эндпоинт `/webhook/offline-conversions/{webhook_id}/status?batch_id={batch_id}`
    """
)
async def receive_offline_conversions(
    webhook_id: str, 
    req: OfflineConversionWebhookRequest, 
    background_tasks: BackgroundTasks,
    webhook: dict = Depends(verify_webhook_secret)
):
    """Принимает офлайн-конверсии через вебхук"""
    try:
        # Проверяем наличие конверсий
        if not req.conversions:
            raise HTTPException(status_code=400, detail="No conversions provided")
        
        # Сохраняем конверсии
        batch_id = await save_webhook_conversions(webhook_id, [conv.dict() for conv in req.conversions])
        
        # Запускаем фоновую задачу для отправки конверсий
        background_tasks.add_task(process_webhook_batch, batch_id)
        
        return OfflineConversionWebhookResponse(
            batch_id=batch_id,
            status="accepted",
            accepted_count=len(req.conversions)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing webhook conversions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing webhook conversions: {str(e)}")

@app.get(
    "/webhook/offline-conversions/{webhook_id}/status",
    response_model=OfflineConversionStatusResponse,
    summary="Получить статус загрузки офлайн-конверсий",
    description="""
    Проверяет статус загрузки офлайн-конверсий, отправленных через вебхук.
    
    - Проверяет секретный ключ вебхука в заголовке X-Webhook-Secret
    - Получает информацию о пакете конверсий
    - Если пакет в статусе "uploaded", проверяет статус загрузки в Яндекс.Метрике
    
    Параметры:
    - batch_id: ID пакета конверсий (обязательный)
    """
)
async def get_offline_conversion_status(
    webhook_id: str, 
    batch_id: str = Query(..., description="ID пакета конверсий"),
    webhook: dict = Depends(verify_webhook_secret)
):
    """Получает статус загрузки офлайн-конверсий"""
    try:
        # Получаем информацию о пакете
        batch = await get_webhook_batch(batch_id)
        if not batch:
            raise HTTPException(status_code=404, detail="Batch not found")
        
        # Проверяем, что пакет принадлежит указанному вебхуку
        if batch["webhook_id"] != webhook_id:
            raise HTTPException(status_code=403, detail="Batch does not belong to this webhook")
        
        # Если пакет в статусе "uploaded", проверяем статус в Метрике
        if batch["status"] == "uploaded" and batch.get("metrika_upload_id"):
            status_info = await check_webhook_batch_status(batch_id)
        else:
            status_info = {
                "status": batch["status"],
                "processed": batch.get("processed", 0),
                "total": batch.get("total", 0),
                "errors": batch.get("errors", [])
            }
        
        return OfflineConversionStatusResponse(
            batch_id=batch_id,
            status=status_info["status"],
            webhook_id=webhook_id,
            counter_id=batch["counter_id"],
            created_at=datetime.fromisoformat(batch["created_at"]) if isinstance(batch["created_at"], str) else batch["created_at"],
            updated_at=datetime.fromisoformat(batch["updated_at"]) if isinstance(batch["updated_at"], str) else batch["updated_at"],
            metrika_upload_id=batch.get("metrika_upload_id"),
            total=batch.get("total", 0),
            processed=status_info.get("processed", 0),
            errors=status_info.get("errors", [])
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting webhook batch status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting webhook batch status: {str(e)}")

# --- Фоновые задачи для вебхуков ---

async def process_webhook_batches():
    """
    Фоновая задача для обработки пакетов конверсий из вебхуков.
    
    Запускается периодически и обрабатывает все пакеты в статусе "pending".
    """
    # TODO: Реализовать периодическую обработку пакетов 
    pass 