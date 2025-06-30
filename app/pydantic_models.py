from pydantic import BaseModel, Field

# --- Совместимость pydantic v1/v2 ---
try:
    from pydantic import field_validator  # type: ignore
except ImportError:  # pydantic v1
    from pydantic import validator as field_validator  # type: ignore

from typing import Optional, List, Dict, Any
from datetime import datetime
import re

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

    # Совместимость: в Pydantic v1 нет model_dump
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

class PaginationInfo(BaseModel):
    total: int
    limit: int
    offset: int
    has_more: bool

class PaginatedVisitResults(BaseModel):
    data: List[VisitResult]
    pagination: PaginationInfo

# --- Модели для офлайн-конверсий ---

class BulkConversionRequest(BaseModel):
    task_id: str = Field(..., description="ID задачи, результаты которой нужно отправить как конверсии")
    target: str = Field("4plus", description="Название цели в Яндекс.Метрике")
    counter: int = Field(..., description="ID счетчика Яндекс.Метрики")
    token: str = Field(..., description="OAuth-токен с правами на отправку конверсий")

class SingleConversionRequest(BaseModel):
    client_id: Optional[str] = Field(None, description="ClientId посетителя из Яндекс.Метрики")
    user_id: Optional[str] = Field(None, description="UserId посетителя, назначенный владельцем сайта")
    yclid: Optional[str] = Field(None, description="Идентификатор клика по рекламе Яндекс.Директа")
    purchase_id: Optional[str] = Field(None, description="Идентификатор покупки")
    target: str = Field(..., description="Название цели в Яндекс.Метрике")
    date_time: datetime = Field(..., description="Дата и время конверсии")
    price: Optional[float] = Field(None, description="Ценность цели")
    currency: Optional[str] = Field(None, description="Валюта в формате ISO 4217 (например, RUB)")
    counter: int = Field(..., description="ID счетчика Яндекс.Метрики")
    token: str = Field(..., description="OAuth-токен с правами на отправку конверсий")
    
    @field_validator("client_id", "user_id", "yclid", "purchase_id")
    @classmethod
    def validate_identifiers(cls, v, values=None, **kwargs):
        # Pydantic v1 версия валидатора
        if values is None:
            values = {}
            
        # Проверяем наличие хотя бы одного идентификатора
        if not any([values.get("client_id"), values.get("user_id"), values.get("yclid"), values.get("purchase_id")]):
            if v is None:  # Проверяем только если текущее поле None
                raise ValueError("At least one identifier (client_id, user_id, yclid, purchase_id) must be provided")
        return v

class ConversionResponse(BaseModel):
    upload_id: str = Field(..., description="ID загрузки в Яндекс.Метрике")
    status: str = Field(..., description="Статус загрузки")

class ConversionStatusResponse(BaseModel):
    upload_id: str
    status: str
    errors: Optional[List[str]] = None
    processed: Optional[int] = None
    total: Optional[int] = None

# Модели для вебхуков офлайн-конверсий
class OfflineConversionItem(BaseModel):
    """Модель для одной офлайн-конверсии"""
    client_id: Optional[str] = None
    user_id: Optional[str] = None
    yclid: Optional[str] = None
    purchase_id: Optional[str] = None
    target: str
    date_time: datetime
    price: Optional[float] = None
    currency: Optional[str] = None
    
    @field_validator('client_id', 'user_id', 'yclid', 'purchase_id', pre=True)
    @classmethod
    def check_identifiers(cls, v, values, **kwargs):
        """Проверяет, что указан только один из идентификаторов"""
        # Получаем имя поля из kwargs для pydantic v1 или из info для v2
        field_name = None
        if 'field' in kwargs:
            field_name = kwargs['field'].name  # pydantic v1
        else:
            # Пробуем получить из контекста для pydantic v2
            try:
                field_name = kwargs.get('info').field_name
            except (AttributeError, KeyError):
                pass
        
        # Проверяем только для последнего поля из четырех
        if field_name == 'purchase_id':
            # Подсчитываем количество заполненных идентификаторов
            filled_ids = sum(1 for id_field in ['client_id', 'user_id', 'yclid', 'purchase_id'] 
                            if values.get(id_field) is not None)
            
            # Проверяем, что заполнен хотя бы один идентификатор
            if filled_ids == 0:
                raise ValueError("Должен быть указан хотя бы один из идентификаторов: client_id, user_id, yclid или purchase_id")
            
            # Проверяем, что заполнен только один идентификатор
            if filled_ids > 1:
                raise ValueError("Должен быть указан только один из идентификаторов: client_id, user_id, yclid или purchase_id")
                
        return v

class OfflineConversionWebhookRequest(BaseModel):
    """Модель для запроса вебхука офлайн-конверсий"""
    conversions: List[OfflineConversionItem]

class WebhookCreateRequest(BaseModel):
    """Модель для создания нового вебхука"""
    name: str
    counter_id: int
    token: str
    description: Optional[str] = None

class WebhookCreateResponse(BaseModel):
    """Модель ответа при создании вебхука"""
    webhook_id: str
    secret: str
    url: str

class OfflineConversionWebhookResponse(BaseModel):
    """Модель ответа на запрос вебхука"""
    batch_id: str
    status: str
    accepted_count: int
    errors: Optional[List[str]] = None

class OfflineConversionStatusResponse(BaseModel):
    """Модель для статуса загрузки офлайн-конверсий"""
    batch_id: str
    status: str
    webhook_id: str
    counter_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    metrika_upload_id: Optional[str] = None
    total: int
    processed: int
    errors: Optional[List[str]] = None 