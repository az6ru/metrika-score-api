# metrika-scrore-api

## Описание

Этот проект содержит скрипт для автоматического сбора и классификации визитов Яндекс.Метрики по уровню активности (4+). Скрипт скачивает сырые логи за выбранную дату, рассчитывает slot-признаки и применяет ML-модель для выделения визитов с высокой активностью.

## Структура
- `fetch_level4_for_date.py` — основной скрипт для выгрузки и классификации визитов за 1 день.

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
python fetch_level4_for_date.py --date 2025-07-01
```
или явно:
```bash
python fetch_level4_for_date.py --date 2025-07-01 --token <TOKEN> --counter <ID>
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

---
Автор: [ваше имя] 