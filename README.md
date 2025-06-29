# metrika-scrore-api

## Описание

Этот проект содержит скрипт для автоматического сбора и классификации визитов Яндекс.Метрики по уровню активности (4+). Скрипт скачивает сырые логи за выбранную дату, рассчитывает slot-признаки и применяет ML-модель для выделения визитов с высокой активностью.

## Структура
- `app/fetch_level4_for_date.py` — основной скрипт для выгрузки и классификации визитов за 1 день.
- `app/send_conversions.py` — функции для отправки конверсий в Метрику.
- `app/pydantic_models.py` — Pydantic-модели.

## Требования
- Python 3.8+
- Библиотеки: `requests`, `pandas`, `numpy`, `joblib`, `scikit-learn`

Установить зависимости:
```bash
pip install requests pandas numpy joblib scikit-learn
```

## Переменные окружения
- `METRIKA_TOKEN` — OAuth-токен Яндекс.Метрики (или передать через --token)
- `METRIKA_COUNTER` — ID счётчика (или передать через --counter)

## Использование

```bash
export METRIKA_TOKEN=...   # ваш OAuth-токен
export METRIKA_COUNTER=... # ваш номер счётчика
python app/fetch_level4_for_date.py --date 2025-07-01
```
или явно:
```bash
python app/fetch_level4_for_date.py --date 2025-07-01 --token <TOKEN> --counter <ID>
```

## Выходной файл
- `level4_visits_<DATE>.json` — список визитов уровня 4+ за выбранную дату.
  - Пример структуры:
```json
[
  {
    "visitId": "...",
    "clientId": "...",
    "dateTime": "2025-07-01 12:34:56",
    "visitDuration": 123
  },
  ...
]
```

## Примечания
- Для работы нужны обученные модели (`level4_desktop_slot*.joblib`, `level4_mobile_slot*.joblib`) и файл порогов `level4_thresholds.json` в той же папке.
- Скрипт не требует ручной подготовки данных — всё скачивает и обрабатывает автоматически.

## API (FastAPI)

### Примеры запросов

#### 1. Создать задачу
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"date": "2025-07-01", "token": "<TOKEN>", "counter": 123456}'
```
Ответ:
```json
{"task_id": "..."}
```

#### 2. Получить статус задачи
```bash
curl http://localhost:8000/tasks/<task_id>/status
```
Ответ:
```json
{
  "status": "pending",
  "progress": 10,
  "message": "...",
  "started_at": "2025-07-01T12:00:00",
  "finished_at": null,
  "error": null
}
```

#### 3. Получить результат задачи
```bash
curl http://localhost:8000/tasks/<task_id>/result
```
Ответ (если задача завершена):
```json
{
  "data": [
    {"visitId": "...", "clientId": "...", "dateTime": "...", "visitDuration": 123},
    ...
  ],
  "pagination": {
    "total": 31,
    "limit": 100,
    "offset": 0,
    "has_more": false
  }
}
```
Если задача не завершена:
```json
{"detail": "Task not finished"}
```

#### 4. Получить результат задачи с пагинацией
```bash
curl http://localhost:8000/tasks/<task_id>/result?limit=10&offset=20
```
Ответ:
```json
{
  "data": [
    {"visitId": "...", "clientId": "...", "dateTime": "...", "visitDuration": 123},
    ...
  ],
  "pagination": {
    "total": 31,
    "limit": 10,
    "offset": 20,
    "has_more": false
  }
}
```

### Офлайн-конверсии

API предоставляет возможность отправки офлайн-конверсий в Яндекс.Метрику:

#### 1. Отправить массовые конверсии на основе результатов задачи
```bash
curl -X POST http://localhost:8000/conversions/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "<task_id>",
    "target": "4plus",
    "counter": 123456,
    "token": "<TOKEN>"
  }'
```
Ответ:
```json
{
  "upload_id": "...",
  "status": "pending"
}
```

#### 2. Отправить одиночную конверсию
```bash
curl -X POST http://localhost:8000/conversions/single \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "175085690441127503",
    "target": "purchase",
    "date_time": "2025-06-28T12:00:00",
    "price": 1500.50,
    "currency": "RUB",
    "counter": 123456,
    "token": "<TOKEN>"
  }'
```
Ответ:
```json
{
  "upload_id": "...",
  "status": "pending"
}
```

#### 3. Проверить статус загрузки конверсий
```bash
curl http://localhost:8000/conversions/<upload_id>/status
```
Ответ:
```json
{
  "upload_id": "...",
  "status": "processed",
  "errors": null,
  "processed": 31,
  "total": 31
}
```

#### 4. Получить все конверсии для задачи
```bash
curl http://localhost:8000/tasks/<task_id>/conversions
```
Ответ:
```json
[
  {
    "upload_id": "...",
    "status": "processed",
    "errors": null,
    "processed": 31,
    "total": 31
  },
  ...
]
```

### Примеры ошибок

- 400: Не передан обязательный параметр
  ```json
  {"detail": "token is required (in body or env)"}
  ```
- 404: Не найдена задача
  ```json
  {"detail": "Task not found"}
  ```
- 422: Ошибка валидации (например, неверный формат даты)
  ```json
  {"detail": [ ... ]}
  ```
- 500: Внутренняя ошибка сервера
  ```json
  {"detail": "Internal server error", "error": "..."}
  ```

## Тестирование

Установить dev-зависимости:
```bash
pip install -r requirements.txt
```

Запустить все тесты:
```bash
pytest test_main.py
```

---
Автор: [ваше имя] 