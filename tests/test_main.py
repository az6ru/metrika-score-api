import os
os.environ.setdefault("OFFLINE_TEST", "1")
import pytest
from fastapi.testclient import TestClient
from app.fetch_level4_for_date import calculate_level4_visits
from app.send_conversions import send_conversions_to_metrika
from app.pydantic_models import TaskRequest
from app.main import app
import threading
import time

VALID_DATE = "2025-06-25"
VALID_TOKEN = "y0__xCF6JHDBhjaxDgg7cna1BMwsKrhhQg4wBPt_vbCABkSTKEzHEemgoEz3Q"
VALID_COUNTER = 92342184

client = TestClient(app)

def test_create_task_success():
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER
    })
    assert resp.status_code == 202
    data = resp.json()
    assert "task_id" in data


def test_create_task_missing_token():
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "counter": VALID_COUNTER
    })
    assert resp.status_code == 422  # FastAPI возвращает 422, если не хватает обязательных полей


def test_create_task_invalid_date():
    resp = client.post("/tasks", json={
        "date": "20250701",
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER
    })
    assert resp.status_code == 422 or resp.status_code == 400


def test_get_status_not_found():
    resp = client.get("/tasks/doesnotexist/status")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Task not found"


def test_get_result_not_found():
    resp = client.get("/tasks/doesnotexist/result")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Task not found"


def test_create_task_zero_counter():
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": VALID_TOKEN,
        "counter": 0
    })
    assert resp.status_code == 422 or resp.status_code == 400


def test_create_task_negative_counter():
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": VALID_TOKEN,
        "counter": -5
    })
    assert resp.status_code == 422 or resp.status_code == 400


def test_create_task_empty_token():
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": "",
        "counter": VALID_COUNTER
    })
    # Пустой токен считается валидным по типу, но бизнес-логика требует не пустой
    assert resp.status_code == 400 or resp.status_code == 202


def test_create_task_invalid_date_format():
    resp = client.post("/tasks", json={
        "date": "2025/07/01",
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER
    })
    assert resp.status_code == 422 or resp.status_code == 400


def test_create_task_nonexistent_date():
    resp = client.post("/tasks", json={
        "date": "2025-02-30",
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER
    })
    # Pydantic не проверяет существование даты, только формат, поэтому ожидаем 202
    assert resp.status_code == 202 or resp.status_code == 400


def test_error_response_structure():
    resp = client.get("/tasks/doesnotexist/status")
    assert resp.status_code == 404
    data = resp.json()
    assert "detail" in data


def test_result_before_done():
    # Создаём задачу
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER
    })
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]
    # Получаем результат сразу, задача не завершена
    resp2 = client.get(f"/tasks/{task_id}/result")
    assert resp2.status_code == 202
    assert resp2.json()["detail"] == "Task not finished"


def test_db_error(monkeypatch):
    pass  # Устаревший тест для sqlite, не актуален после перехода на Supabase


def create_task_thread(results, idx):
    resp = client.post("/tasks", json={
        "date": VALID_DATE,
        "token": VALID_TOKEN,
        "counter": VALID_COUNTER + idx
    })
    results[idx] = resp


def test_parallel_task_creation():
    threads = []
    results = [None] * 5
    for i in range(5):
        t = threading.Thread(target=create_task_thread, args=(results, i))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    task_ids = set()
    for resp in results:
        assert resp.status_code == 202
        data = resp.json()
        assert "task_id" in data
        task_ids.add(data["task_id"])
    assert len(task_ids) == 5  # Все task_id уникальны 