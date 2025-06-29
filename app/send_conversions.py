#!/usr/bin/env python3
"""
Модуль для работы с API офлайн-конверсий Яндекс.Метрики.
Предоставляет функции для отправки и проверки статуса загрузки конверсий.
"""
import os
import tempfile
import logging
import aiohttp
from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import HTTPException

logger = logging.getLogger("metrika-api")

async def send_conversions_to_metrika(counter: int, token: str, csv_content: str) -> Dict[str, Any]:
    """
    Отправляет CSV с конверсиями в Яндекс.Метрику.
    
    Args:
        counter: ID счетчика Яндекс.Метрики
        token: OAuth-токен с правами на отправку конверсий
        csv_content: Содержимое CSV-файла с конверсиями
        
    Returns:
        Dict с информацией о загрузке (upload_id, status)
        
    Raises:
        HTTPException: В случае ошибки API Яндекс.Метрики
    """
    # Создаем временный файл с CSV
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w', encoding='utf-8') as temp_file:
        temp_file.write(csv_content)
        temp_path = temp_file.name
    
    try:
        # Формируем multipart/form-data запрос
        url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/offline_conversions/upload"
        headers = {"Authorization": f"OAuth {token}"}
        
        logger.info(f"Sending conversions to Metrika: counter={counter}, url={url}")
        
        # Асинхронная отправка файла
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field('file', open(temp_path, 'rb'), filename='conversions.csv')
            async with session.post(url, headers=headers, data=form) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Metrika API error: {error_text}")
                    raise HTTPException(status_code=response.status, 
                                        detail=f"Metrika API error: {error_text}")
                result = await response.json()
                logger.info(f"Conversions sent successfully: {result}")
                return {
                    "upload_id": result.get("uploadId", ""),
                    "status": result.get("status", "unknown")
                }
    finally:
        # Удаляем временный файл
        os.unlink(temp_path)

async def check_conversion_status(counter: int, token: str, upload_id: str) -> Dict[str, Any]:
    """
    Проверяет статус загрузки конверсий.
    
    Args:
        counter: ID счетчика Яндекс.Метрики
        token: OAuth-токен с правами на отправку конверсий
        upload_id: ID загрузки, полученный от API Яндекс.Метрики
        
    Returns:
        Dict с информацией о статусе загрузки
        
    Raises:
        HTTPException: В случае ошибки API Яндекс.Метрики
    """
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/offline_conversions/uploading/{upload_id}"
    headers = {"Authorization": f"OAuth {token}"}
    
    logger.info(f"Checking conversion status: counter={counter}, upload_id={upload_id}")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Metrika API error: {error_text}")
                raise HTTPException(status_code=response.status, 
                                    detail=f"Metrika API error: {error_text}")
            result = await response.json()
            logger.info(f"Conversion status: {result}")
            return {
                "upload_id": upload_id,
                "status": result.get("status", "unknown"),
                "errors": result.get("errors"),
                "processed": result.get("processed"),
                "total": result.get("total")
            }

def format_conversion_csv(visits: List[Dict[str, Any]], target: str = "4plus") -> str:
    """
    Форматирует список визитов в CSV для отправки конверсий.
    
    Args:
        visits: Список визитов (результаты задачи)
        target: Название цели в Яндекс.Метрике
        
    Returns:
        Строка с CSV-данными
    """
    # Формируем CSV
    csv_data = "ClientId,Target,DateTime\n"
    for visit in visits:
        # Преобразуем datetime в Unix timestamp
        dt_str = visit["dateTime"].replace(" ", "T")
        if not dt_str.endswith("Z") and "+" not in dt_str:
            dt_str += "Z"  # Добавляем UTC если не указана зона
        
        try:
            dt_obj = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            unix_time = int(dt_obj.timestamp())
            csv_data += f"{visit['clientId']},{target},{unix_time}\n"
        except (ValueError, KeyError) as e:
            logger.warning(f"Error formatting visit: {e}, visit={visit}")
            continue
    
    return csv_data

def format_single_conversion_csv(
    target: str,
    date_time: datetime,
    client_id: Optional[str] = None,
    user_id: Optional[str] = None,
    yclid: Optional[str] = None,
    purchase_id: Optional[str] = None,
    price: Optional[float] = None,
    currency: Optional[str] = None
) -> str:
    """
    Форматирует одиночную конверсию в CSV.
    
    Args:
        target: Название цели
        date_time: Время конверсии
        client_id: ClientId посетителя
        user_id: UserId посетителя
        yclid: Идентификатор клика
        purchase_id: Идентификатор покупки
        price: Ценность цели
        currency: Валюта
        
    Returns:
        Строка с CSV-данными
    """
    # Формируем заголовки CSV
    headers = ["Target", "DateTime"]
    values = [target, int(date_time.timestamp())]
    
    # Добавляем идентификаторы
    if client_id:
        headers.append("ClientId")
        values.append(client_id)
    if user_id:
        headers.append("UserId")
        values.append(user_id)
    if yclid:
        headers.append("Yclid")
        values.append(yclid)
    if purchase_id:
        headers.append("PurchaseId")
        values.append(purchase_id)
    
    # Добавляем опциональные поля
    if price is not None:
        headers.append("Price")
        values.append(str(price))
    if currency:
        headers.append("Currency")
        values.append(currency)
    
    # Формируем CSV
    csv_data = ",".join(headers) + "\n" + ",".join(map(str, values))
    return csv_data 