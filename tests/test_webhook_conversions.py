import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import uuid
from datetime import datetime, timedelta

from app.main import app

client = TestClient(app)

# Мок-данные для тестов
MOCK_WEBHOOK_ID = str(uuid.uuid4())
MOCK_SECRET = "test_secret_key"
MOCK_BATCH_ID = str(uuid.uuid4())
MOCK_COUNTER_ID = 12345678
MOCK_TOKEN = "test_token"

# Мок-функции для Supabase
@pytest.fixture
def mock_supabase_webhook_functions():
    with patch("app.supabase_db.create_webhook") as mock_create_webhook, \
         patch("app.supabase_db.get_webhook") as mock_get_webhook, \
         patch("app.supabase_db.save_webhook_conversions") as mock_save_webhook_conversions, \
         patch("app.supabase_db.get_webhook_batch") as mock_get_webhook_batch, \
         patch("app.supabase_db.update_webhook_batch_status") as mock_update_webhook_batch_status:
        
        # Настраиваем моки
        mock_create_webhook.return_value = {
            "webhook_id": MOCK_WEBHOOK_ID,
            "secret": MOCK_SECRET,
            "url": f"https://api.example.com/webhook/offline-conversions/{MOCK_WEBHOOK_ID}"
        }
        
        mock_get_webhook.return_value = {
            "id": MOCK_WEBHOOK_ID,
            "name": "Test Webhook",
            "counter_id": MOCK_COUNTER_ID,
            "token": MOCK_TOKEN,
            "description": "Test webhook description",
            "secret": MOCK_SECRET,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "is_active": True
        }
        
        mock_save_webhook_conversions.return_value = MOCK_BATCH_ID
        
        mock_get_webhook_batch.return_value = {
            "id": MOCK_BATCH_ID,
            "webhook_id": MOCK_WEBHOOK_ID,
            "counter_id": MOCK_COUNTER_ID,
            "status": "completed",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "total": 2,
            "processed": 2,
            "metrika_upload_id": "987654",
            "errors": None
        }
        
        mock_update_webhook_batch_status.return_value = None
        
        yield {
            "create_webhook": mock_create_webhook,
            "get_webhook": mock_get_webhook,
            "save_webhook_conversions": mock_save_webhook_conversions,
            "get_webhook_batch": mock_get_webhook_batch,
            "update_webhook_batch_status": mock_update_webhook_batch_status
        }

# Мок-функции для отправки конверсий в Метрику
@pytest.fixture
def mock_webhook_conversion_functions():
    with patch("app.send_webhook_conversions.process_webhook_batch") as mock_process_webhook_batch, \
         patch("app.send_webhook_conversions.check_webhook_batch_status") as mock_check_webhook_batch_status:
        
        # Настраиваем моки
        mock_process_webhook_batch.return_value = {
            "status": "uploaded",
            "metrika_upload_id": "987654",
            "batch_id": MOCK_BATCH_ID
        }
        
        mock_check_webhook_batch_status.return_value = {
            "status": "completed",
            "processed": 2,
            "total": 2,
            "errors": None
        }
        
        yield {
            "process_webhook_batch": mock_process_webhook_batch,
            "check_webhook_batch_status": mock_check_webhook_batch_status
        }

def test_create_webhook(mock_supabase_webhook_functions):
    """Тест создания вебхука"""
    response = client.post(
        "/webhook/offline-conversions",
        json={
            "name": "Test Webhook",
            "counter_id": MOCK_COUNTER_ID,
            "token": MOCK_TOKEN,
            "description": "Test webhook description"
        }
    )
    
    assert response.status_code == 200
    assert "webhook_id" in response.json()
    assert "secret" in response.json()
    assert "url" in response.json()
    
    # Проверяем, что функция создания вебхука была вызвана с правильными параметрами
    mock_supabase_webhook_functions["create_webhook"].assert_called_once_with(
        name="Test Webhook",
        counter_id=MOCK_COUNTER_ID,
        token=MOCK_TOKEN,
        description="Test webhook description"
    )

def test_receive_webhook_conversions(mock_supabase_webhook_functions, mock_webhook_conversion_functions):
    """Тест приема конверсий через вебхук"""
    response = client.post(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}",
        headers={"X-Webhook-Secret": MOCK_SECRET},
        json={
            "conversions": [
                {
                    "client_id": "1234567890.1234567890",
                    "target": "purchase",
                    "date_time": "2025-07-01T12:34:56",
                    "price": 1500,
                    "currency": "RUB",
                    "purchase_id": "order123"
                },
                {
                    "user_id": "user123",
                    "target": "registration",
                    "date_time": "2025-07-01T12:30:00"
                }
            ]
        }
    )
    
    assert response.status_code == 200
    assert "batch_id" in response.json()
    assert "status" in response.json()
    assert "accepted_count" in response.json()
    assert response.json()["accepted_count"] == 2
    
    # Проверяем, что функция получения вебхука была вызвана с правильными параметрами
    mock_supabase_webhook_functions["get_webhook"].assert_called_once_with(MOCK_WEBHOOK_ID)
    
    # Проверяем, что функция сохранения конверсий была вызвана
    mock_supabase_webhook_functions["save_webhook_conversions"].assert_called_once()

def test_get_webhook_conversion_status(mock_supabase_webhook_functions, mock_webhook_conversion_functions):
    """Тест получения статуса конверсий"""
    response = client.get(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}/status?batch_id={MOCK_BATCH_ID}",
        headers={"X-Webhook-Secret": MOCK_SECRET}
    )
    
    assert response.status_code == 200
    assert "batch_id" in response.json()
    assert "status" in response.json()
    assert "webhook_id" in response.json()
    assert "counter_id" in response.json()
    assert "total" in response.json()
    assert "processed" in response.json()
    
    # Проверяем, что функция получения вебхука была вызвана с правильными параметрами
    mock_supabase_webhook_functions["get_webhook"].assert_called_once_with(MOCK_WEBHOOK_ID)
    
    # Проверяем, что функция получения пакета была вызвана с правильными параметрами
    mock_supabase_webhook_functions["get_webhook_batch"].assert_called_once_with(MOCK_BATCH_ID)

def test_webhook_without_secret(mock_supabase_webhook_functions):
    """Тест запроса без секретного ключа"""
    response = client.post(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}",
        json={
            "conversions": [
                {
                    "client_id": "1234567890.1234567890",
                    "target": "purchase",
                    "date_time": "2025-07-01T12:34:56"
                }
            ]
        }
    )
    
    assert response.status_code == 401
    assert "detail" in response.json()
    assert response.json()["detail"] == "X-Webhook-Secret header is required"

def test_webhook_with_invalid_secret(mock_supabase_webhook_functions):
    """Тест запроса с неверным секретным ключом"""
    # Настраиваем мок для проверки секретного ключа
    mock_supabase_webhook_functions["get_webhook"].return_value["secret"] = "correct_secret"
    
    response = client.post(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}",
        headers={"X-Webhook-Secret": "wrong_secret"},
        json={
            "conversions": [
                {
                    "client_id": "1234567890.1234567890",
                    "target": "purchase",
                    "date_time": "2025-07-01T12:34:56"
                }
            ]
        }
    )
    
    assert response.status_code == 403
    assert "detail" in response.json()
    assert response.json()["detail"] == "Invalid webhook secret"

def test_webhook_with_invalid_data(mock_supabase_webhook_functions):
    """Тест запроса с некорректными данными"""
    response = client.post(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}",
        headers={"X-Webhook-Secret": MOCK_SECRET},
        json={
            "conversions": [
                {
                    # Отсутствует обязательное поле target
                    "client_id": "1234567890.1234567890",
                    "date_time": "2025-07-01T12:34:56"
                }
            ]
        }
    )
    
    assert response.status_code == 422  # Validation Error

def test_webhook_with_empty_conversions(mock_supabase_webhook_functions):
    """Тест запроса с пустым списком конверсий"""
    response = client.post(
        f"/webhook/offline-conversions/{MOCK_WEBHOOK_ID}",
        headers={"X-Webhook-Secret": MOCK_SECRET},
        json={
            "conversions": []
        }
    )
    
    assert response.status_code == 400
    assert "detail" in response.json()
    assert response.json()["detail"] == "No conversions provided" 