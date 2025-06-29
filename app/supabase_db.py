import os
from supabase import create_client, Client
from typing import Any, Dict, Optional, List
import asyncio
from dotenv import load_dotenv
from postgrest.exceptions import APIError
from collections import defaultdict
import logging

load_dotenv()

# --- OFFLINE / TEST MODE ---
OFFLINE = bool(os.environ.get("OFFLINE_TEST")) or bool(os.environ.get("PYTEST_CURRENT_TEST"))

# Простое хранилище в памяти для офлайн-режима
_tasks_mem: Dict[str, Dict[str, Any]] = {}
_results_mem: Dict[str, Any] = {}
_conversions_mem: Dict[str, Dict[str, Any]] = {}  # Хранилище для конверсий

if OFFLINE:
    # Заглушка вместо real Supabase Client
    class _StubResp:
        def __init__(self, data):
            self.data = data

    class _StubTable:
        def __init__(self, name):
            self.name = name

        def insert(self, data):
            if self.name == "tasks":
                _tasks_mem[data["id"]] = data
            elif self.name == "results":
                _results_mem[data["task_id"]] = data["result_json"]
            elif self.name == "conversion_uploads":
                _conversions_mem[data["id"]] = data
            return self

        def update(self, data):
            self._upd = data
            return self

        def upsert(self, data):
            return self.insert(data if isinstance(data, dict) else data[0])

        def eq(self, col, val):
            self._eq_col, self._eq_val = col, val
            return self

        def select(self, *_):
            return self

        def single(self):
            return self

        def execute(self):
            if self.name == "tasks":
                if hasattr(self, "_upd"):
                    if self._eq_val in _tasks_mem:
                        _tasks_mem[self._eq_val].update(self._upd)
                if hasattr(self, "_eq_val"):
                    return _StubResp(_tasks_mem.get(self._eq_val))
            if self.name == "results":
                if hasattr(self, "_eq_val"):
                    val = _results_mem.get(self._eq_val)
                    return _StubResp({"result_json": val} if val is not None else None)
            if self.name == "conversion_uploads":
                if hasattr(self, "_upd"):
                    if self._eq_val in _conversions_mem:
                        _conversions_mem[self._eq_val].update(self._upd)
                if hasattr(self, "_eq_val"):
                    return _StubResp(_conversions_mem.get(self._eq_val))
            return _StubResp(None)

    class _StubClient:
        def table(self, name):
            return _StubTable(name)

    supabase: Any = _StubClient()
else:
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "<your-local-service-role-key>")
    from supabase import create_client, Client  # noqa
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def create_task(task_id: str, params: Dict[str, Any]):
    from datetime import datetime
    data = {
        "id": task_id,
        "status": "pending",
        "progress": 0,
        "message": "",
        "started_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "error": None,
        "params": params,
    }
    loop = asyncio.get_event_loop()
    try:
        if OFFLINE:
            _tasks_mem[task_id] = data
        else:
            await loop.run_in_executor(None, lambda: supabase.table("tasks").insert(data).execute())
    except APIError as e:
        raise Exception(str(e))

async def update_task_status(task_id: str, status: str, progress: int = 0, message: str = "", error: Optional[str] = None, finished_at: Optional[str] = None):
    data = {
        "status": status,
        "progress": progress,
        "message": message,
        "error": error,
        "finished_at": finished_at,
    }
    loop = asyncio.get_event_loop()
    try:
        if OFFLINE:
            if task_id in _tasks_mem:
                _tasks_mem[task_id].update(data)
        else:
            await loop.run_in_executor(None, lambda: supabase.table("tasks").update(data).eq("id", task_id).execute())
    except APIError as e:
        raise Exception(str(e))

async def save_result(task_id: str, result: Any):
    data = {"task_id": task_id, "result_json": result}
    loop = asyncio.get_event_loop()
    try:
        if OFFLINE:
            _results_mem[task_id] = result
        else:
            await loop.run_in_executor(None, lambda: supabase.table("results").upsert(data).execute())
    except APIError as e:
        raise Exception(str(e))

async def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    if OFFLINE:
        return _tasks_mem.get(task_id)
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: supabase.table("tasks").select("*").eq("id", task_id).execute())
        if resp.data and len(resp.data) > 0:
            return resp.data[0]
        return None
    except APIError as e:
        logger = logging.getLogger("metrika-api")
        logger.error(f"get_task APIError: {str(e)}")
        return None

async def get_result(task_id: str) -> Optional[Any]:
    if OFFLINE:
        return _results_mem.get(task_id)
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: supabase.table("results").select("result_json").eq("task_id", task_id).single().execute())
        return resp.data["result_json"]
    except APIError:
        return None

# --- Функции для работы с конверсиями ---

async def create_conversion_upload(
    metrika_upload_id: str,
    task_id: Optional[str],
    counter: int,
    target: str,
    total_conversions: Optional[int] = None
) -> str:
    """
    Создает запись о загрузке конверсий в базе данных.
    
    Args:
        metrika_upload_id: ID загрузки в Яндекс.Метрике
        task_id: ID задачи (может быть None для одиночных конверсий)
        counter: ID счетчика Яндекс.Метрики
        target: Название цели
        total_conversions: Общее количество конверсий
        
    Returns:
        ID записи в базе данных
    """
    from datetime import datetime
    import uuid
    
    upload_id = str(uuid.uuid4())
    data = {
        "id": upload_id,
        "metrika_upload_id": metrika_upload_id,
        "task_id": task_id,
        "counter": counter,
        "target": target,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "total_conversions": total_conversions,
        "processed_conversions": 0,
        "errors": None
    }
    
    loop = asyncio.get_event_loop()
    try:
        if OFFLINE:
            _conversions_mem[upload_id] = data
        else:
            await loop.run_in_executor(None, lambda: supabase.table("conversion_uploads").insert(data).execute())
        return upload_id
    except APIError as e:
        logger = logging.getLogger("metrika-api")
        logger.error(f"create_conversion_upload APIError: {str(e)}")
        raise Exception(str(e))

async def update_conversion_status(
    upload_id: str,
    status: str,
    processed_conversions: Optional[int] = None,
    errors: Optional[List[str]] = None
) -> None:
    """
    Обновляет статус загрузки конверсий.
    
    Args:
        upload_id: ID записи в базе данных
        status: Новый статус
        processed_conversions: Количество обработанных конверсий
        errors: Список ошибок
    """
    data = {
        "status": status,
        "updated_at": datetime.utcnow().isoformat(),
    }
    
    if processed_conversions is not None:
        data["processed_conversions"] = processed_conversions
    
    if errors is not None:
        data["errors"] = errors
    
    loop = asyncio.get_event_loop()
    try:
        if OFFLINE:
            if upload_id in _conversions_mem:
                _conversions_mem[upload_id].update(data)
        else:
            await loop.run_in_executor(None, lambda: supabase.table("conversion_uploads").update(data).eq("id", upload_id).execute())
    except APIError as e:
        logger = logging.getLogger("metrika-api")
        logger.error(f"update_conversion_status APIError: {str(e)}")
        raise Exception(str(e))

async def get_conversion_upload(upload_id: str) -> Optional[Dict[str, Any]]:
    """
    Получает информацию о загрузке конверсий.
    
    Args:
        upload_id: ID записи в базе данных
        
    Returns:
        Информация о загрузке или None, если запись не найдена
    """
    if OFFLINE:
        return _conversions_mem.get(upload_id)
    
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: supabase.table("conversion_uploads").select("*").eq("id", upload_id).execute())
        if resp.data and len(resp.data) > 0:
            return resp.data[0]
        return None
    except APIError as e:
        logger = logging.getLogger("metrika-api")
        logger.error(f"get_conversion_upload APIError: {str(e)}")
        return None

async def get_conversions_by_task(task_id: str) -> List[Dict[str, Any]]:
    """
    Получает все загрузки конверсий для задачи.
    
    Args:
        task_id: ID задачи
        
    Returns:
        Список загрузок конверсий
    """
    if OFFLINE:
        return [conv for conv in _conversions_mem.values() if conv.get("task_id") == task_id]
    
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: supabase.table("conversion_uploads").select("*").eq("task_id", task_id).execute())
        return resp.data or []
    except APIError as e:
        logger = logging.getLogger("metrika-api")
        logger.error(f"get_conversions_by_task APIError: {str(e)}")
        return [] 