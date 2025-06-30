import os
import asyncio
import logging
import csv
import tempfile
import aiohttp
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

from app.supabase_db import (
    get_webhook, 
    get_webhook_batch, 
    get_webhook_conversions, 
    update_webhook_batch_status
)

logger = logging.getLogger("metrika-api")

async def prepare_conversions_csv(conversions: List[Dict[str, Any]]) -> str:
    """
    Подготавливает CSV-файл с конверсиями для отправки в Яндекс.Метрику.
    
    Args:
        conversions: Список конверсий
        
    Returns:
        Путь к временному CSV-файлу
    """
    # Создаем временный файл
    fd, path = tempfile.mkstemp(suffix='.csv')
    try:
        with open(fd, 'w', newline='\n', encoding='utf-8') as f:
            # ВАЖНО: Формат CSV для Яндекс.Метрики очень специфичен
            # 1. Заголовок должен быть точно "ClientId,Target,DateTime"
            # 2. Разделитель - запятая
            # 3. Переносы строк - LF (не CRLF)
            # 4. DateTime должен быть в формате Unix timestamp (целое число)
            f.write("ClientId,Target,DateTime\n")
            
            # Записываем данные
            for conv in conversions:
                client_id = conv.get('client_id', '')
                if not client_id:
                    logger.warning(f"Пропуск конверсии без client_id: {conv}")
                    continue
                
                target = conv.get('target', '')
                if not target:
                    logger.warning(f"Пропуск конверсии без target: {conv}")
                    continue
                
                # Получаем дату из конверсии
                date_time = conv.get('date_time', '')
                if not date_time:
                    logger.warning(f"Пропуск конверсии без date_time: {conv}")
                    continue
                
                # Преобразуем дату в Unix timestamp
                try:
                    # Предполагаем, что date_time приходит в формате ISO (например, "2023-07-01T12:34:56Z")
                    # или в формате с микросекундами (например, "2023-07-01T12:34:56.789Z")
                    if isinstance(date_time, str):
                        dt = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
                    else:
                        # Если date_time уже является объектом datetime
                        dt = date_time
                    
                    # Получаем Unix timestamp (целое число секунд с эпохи Unix)
                    unix_time = int(dt.timestamp())
                    logger.info(f"Преобразована дата {date_time} в Unix timestamp: {unix_time}")
                except Exception as e:
                    logger.error(f"Ошибка преобразования даты {date_time}: {str(e)}")
                    logger.warning(f"Пропуск конверсии с некорректной датой: {conv}")
                    continue
                
                # Записываем строку с данными
                row = f"{client_id},{target},{unix_time}\n"
                logger.info(f"Записываем строку в CSV: '{row.strip()}'")
                f.write(row)
        
        # Для отладки: выводим содержимое CSV-файла и его бинарное представление
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.info(f"Сгенерированный CSV:\n{content}")
        
        # Добавляем бинарный дамп файла для отладки
        import subprocess
        try:
            result = subprocess.run(['hexdump', '-C', path], capture_output=True, text=True)
            logger.info(f"Бинарное представление CSV:\n{result.stdout}")
        except Exception as e:
            logger.warning(f"Не удалось получить бинарное представление файла: {str(e)}")
        
        return path
    except Exception as e:
        os.close(fd)
        os.unlink(path)
        logger.error(f"Ошибка подготовки CSV: {str(e)}")
        raise

async def upload_conversions_to_metrika(counter_id: int, token: str, csv_path: str) -> Tuple[str, str]:
    """
    Отправляет CSV-файл с конверсиями в Яндекс.Метрику.
    
    Args:
        counter_id: ID счетчика Яндекс.Метрики
        token: OAuth-токен
        csv_path: Путь к CSV-файлу
        
    Returns:
        Tuple[upload_id, status]
    """
    upload_url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/offline_conversions/upload"
    
    headers = {
        "Authorization": f"OAuth {token}"
    }
    
    # Для отладки: выводим содержимое файла перед отправкой
    with open(csv_path, 'r', encoding='utf-8') as f:
        content = f.read()
        logger.info(f"Отправляем CSV в API Метрики:\n{content}")
    
    try:
        # Используем тот же подход, что и в успешном curl-запросе
        async with aiohttp.ClientSession() as session:
            # Формируем запрос точно как в curl
            form_data = aiohttp.FormData()
            # Важно: читаем файл внутри блока try, чтобы избежать ошибки с закрытым файлом
            # Открываем файл непосредственно перед отправкой
            
            # Генерируем эквивалентную curl-команду для отладки
            with open(csv_path, 'r', encoding='utf-8') as f:
                curl_content = f.read().replace('\n', '\\n').replace('"', '\\"')
                curl_cmd = f'curl -X POST "{upload_url}" -H "Authorization: OAuth {token[:5]}..." -F "file=@-" <<< "{curl_content}"'
                logger.info(f"Эквивалент команды curl:\n{curl_cmd}")
            
            # Добавляем файл в форму
            form_data.add_field(
                'file', 
                open(csv_path, 'rb'), 
                filename='conversions.csv', 
                content_type='text/csv'
            )
            
            # Выводим отладочную информацию
            logger.info(f"Отправляем запрос на {upload_url}")
            logger.info(f"Заголовки запроса: {headers}")
            
            # Логируем детали запроса и формы
            logger.info(f"Отправляем multipart/form-data с файлом: {csv_path}")
            logger.info(f"Параметры формы: {form_data._fields}")
            
            try:
                # Создаем копию файла для сохранения отправляемого содержимого
                debug_path = csv_path + ".debug"
                with open(csv_path, 'rb') as src, open(debug_path, 'wb') as dst:
                    dst.write(src.read())
                logger.info(f"Сохранена копия отправляемого файла: {debug_path}")
                
                async with session.post(upload_url, headers=headers, data=form_data) as response:
                    response_text = await response.text()
                    logger.info(f"Статус ответа: {response.status}, тело: {response_text}")
                    
                    if response.status != 200:
                        logger.error(f"Ошибка загрузки конверсий: {response.status}, {response_text}")
                        raise Exception(f"Ошибка API: {response.status}, {response_text}")
                    
                    result = await response.json()
                    # Исправлено: правильные ключи в ответе API
                    upload_id = str(result.get('uploading', {}).get('id', ''))
                    status = result.get('uploading', {}).get('status', 'unknown')
                    
                    logger.info(f"Успешная загрузка: upload_id={upload_id}, status={status}")
                    return upload_id, status
            except Exception as e:
                logger.error(f"Ошибка при выполнении запроса: {str(e)}")
                raise
            finally:
                # Удаляем отладочную копию файла
                try:
                    if os.path.exists(debug_path):
                        os.unlink(debug_path)
                except Exception as e:
                    logger.warning(f"Не удалось удалить отладочный файл {debug_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Ошибка загрузки конверсий: {str(e)}")
        raise
    finally:
        # Удаляем временный файл
        try:
            os.unlink(csv_path)
            logger.info(f"Временный файл удален: {csv_path}")
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл {csv_path}: {str(e)}")

async def check_metrika_upload_status(counter_id: int, token: str, upload_id: str) -> Dict[str, Any]:
    """
    Проверяет статус загрузки конверсий в Яндекс.Метрике.
    
    Args:
        counter_id: ID счетчика Яндекс.Метрики
        token: OAuth-токен
        upload_id: ID загрузки
        
    Returns:
        Информация о загрузке
    """
    # Исправлен URL для проверки статуса загрузки
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/offline_conversions/uploading/{upload_id}"
    
    headers = {
        "Authorization": f"OAuth {token}"
    }
    
    logger.info(f"Проверка статуса загрузки: counter_id={counter_id}, upload_id={upload_id}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                logger.info(f"Ответ проверки статуса: {response.status}, тело: {response_text}")
                
                if response.status != 200:
                    logger.error(f"Ошибка проверки статуса загрузки: {response.status}, {response_text}")
                    raise Exception(f"Ошибка API: {response.status}, {response_text}")
                
                result = await response.json()
                upload_info = result.get('uploading', {})
                logger.info(f"Статус загрузки: {upload_info}")
                return upload_info
    except Exception as e:
        logger.error(f"Ошибка проверки статуса загрузки: {str(e)}")
        raise

async def process_webhook_batch(batch_id: str) -> Dict[str, Any]:
    """
    Обрабатывает пакет конверсий из вебхука.
    
    Args:
        batch_id: ID пакета
        
    Returns:
        Информация о результате обработки
    """
    # Получаем информацию о пакете
    batch = await get_webhook_batch(batch_id)
    if not batch:
        raise Exception(f"Batch {batch_id} not found")
    
    # Получаем информацию о вебхуке
    webhook = await get_webhook(batch['webhook_id'])
    if not webhook:
        raise Exception(f"Webhook {batch['webhook_id']} not found")
    
    # Получаем конверсии
    try:
        conversions = await get_webhook_conversions(batch_id)
        if not conversions:
            await update_webhook_batch_status(batch_id, "error", errors=["No conversions found"])
            return {"status": "error", "message": "No conversions found"}
    except Exception as e:
        logger.error(f"Error getting webhook conversions for batch {batch_id}: {str(e)}")
        await update_webhook_batch_status(batch_id, "error", errors=[f"Error getting conversions: {str(e)}"])
        return {"status": "error", "message": f"Error getting conversions: {str(e)}"}
    
    try:
        # Подготавливаем CSV-файл
        csv_path = await prepare_conversions_csv(conversions)
        
        # Логируем содержимое CSV-файла
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
            logger.info(f"CSV content for batch {batch_id}:\n{csv_content}")
        
        # Отправляем конверсии в Яндекс.Метрику
        metrika_upload_id, status = await upload_conversions_to_metrika(
            webhook['counter_id'], 
            webhook['token'], 
            csv_path
        )
        
        # Обновляем статус пакета
        await update_webhook_batch_status(
            batch_id, 
            "uploaded", 
            metrika_upload_id=metrika_upload_id, 
            processed=0
        )
        
        return {
            "status": "uploaded",
            "metrika_upload_id": metrika_upload_id,
            "batch_id": batch_id
        }
    except Exception as e:
        logger.error(f"Error processing webhook batch {batch_id}: {str(e)}")
        await update_webhook_batch_status(batch_id, "error", errors=[str(e)])
        return {"status": "error", "message": str(e)}

async def check_webhook_batch_status(batch_id: str) -> Dict[str, Any]:
    """
    Проверяет статус пакета конверсий.
    
    Args:
        batch_id: ID пакета
        
    Returns:
        Информация о статусе
    """
    # Получаем информацию о пакете
    batch = await get_webhook_batch(batch_id)
    if not batch:
        return {"status": "error", "message": "Batch not found"}
    
    # Если статус не "uploaded", возвращаем текущий статус
    if batch['status'] != "uploaded":
        return {
            "status": batch['status'],
            "processed": batch.get('processed', 0),
            "total": batch.get('total', 0),
            "errors": batch.get('errors', [])
        }
    
    # Если нет ID загрузки в Метрике, возвращаем ошибку
    if not batch.get('metrika_upload_id'):
        return {"status": "error", "message": "No Metrika upload ID"}
    
    # Получаем информацию о вебхуке
    webhook = await get_webhook(batch['webhook_id'])
    if not webhook:
        return {"status": "error", "message": "Webhook not found"}
    
    try:
        # Проверяем статус загрузки в Яндекс.Метрике
        upload_status = await check_metrika_upload_status(
            webhook['counter_id'], 
            webhook['token'], 
            batch['metrika_upload_id']
        )
        
        metrika_status = upload_status.get('status', '')
        processed = upload_status.get('line_quantity', 0)
        
        # Маппинг статусов Метрики на наши статусы
        status_mapping = {
            'PREPARED': 'uploaded',
            'UPLOADED': 'uploaded',
            'EXPORTED': 'processing',
            'MATCHED': 'processing',
            'PROCESSED': 'completed',
            'LINKAGE_FAILURE': 'error'
        }
        
        new_status = status_mapping.get(metrika_status, 'processing')
        
        # Обновляем статус пакета
        await update_webhook_batch_status(
            batch_id, 
            new_status, 
            processed=processed,
            errors=[f"Metrika status: {metrika_status}"] if new_status == "error" else None
        )
        
        return {
            "status": new_status,
            "metrika_status": metrika_status,
            "processed": processed,
            "total": batch.get('total', 0),
            "errors": [f"Metrika status: {metrika_status}"] if new_status == "error" else None
        }
    except Exception as e:
        logger.error(f"Error checking webhook batch status {batch_id}: {str(e)}")
        return {"status": "error", "message": str(e)} 