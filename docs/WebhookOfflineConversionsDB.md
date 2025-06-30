# Хранение офлайн-конверсий по вебхуку: структура БД и рекомендации

---

## 1. Общая архитектура хранения

В системе реализуется отдельный поток для офлайн-конверсий, поступающих через вебхук. Для этого используются специализированные таблицы, обеспечивающие:
- Привязку событий к пользователю, Яндекс-аккаунту, счетчику и вебхуку
- Аудит, статусы отправки, повторные попытки
- Масштабируемость и прозрачность хранения

---

## 2. Основные таблицы

### 2.1. Таблица аккаунтов Яндекс
```sql
CREATE TABLE IF NOT EXISTS yandex_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id),
    yandex_uid TEXT NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    token_expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```
- Связывает пользователя системы с его Яндекс-аккаунтом и токенами для API.

### 2.2. Таблица счетчиков Метрики
```sql
CREATE TABLE IF NOT EXISTS metrika_counters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES yandex_accounts(id),
    counter_id INT NOT NULL,
    name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```
- Позволяет хранить несколько счетчиков на один Яндекс-аккаунт.

### 2.3. Таблица вебхуков для офлайн-конверсий
```sql
CREATE TABLE IF NOT EXISTS offline_webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    counter_id UUID NOT NULL REFERENCES metrika_counters(id),
    url TEXT NOT NULL,
    secret TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```
- Для каждого счетчика можно создать отдельный вебхук с уникальным URL и секретом.

### 2.4. Таблица принятых офлайн-конверсий (webhook_conversions)
```sql
CREATE TABLE IF NOT EXISTS webhook_conversions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id UUID NOT NULL REFERENCES offline_webhooks(id),
    counter_id UUID NOT NULL REFERENCES metrika_counters(id),
    payload JSONB NOT NULL, -- исходные данные конверсии
    status TEXT NOT NULL DEFAULT 'pending', -- pending/sent/error
    metrika_upload_id TEXT, -- id загрузки в Метрику (если применимо)
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_conversions_status ON webhook_conversions(status);
CREATE INDEX IF NOT EXISTS idx_webhook_conversions_counter_id ON webhook_conversions(counter_id);
```
- Хранит каждую поступившую офлайн-конверсию, её статус отправки, ошибки, id загрузки в Метрику.
- Позволяет отслеживать историю, повторные попытки, аудит.

---

## 3. Логика работы и статусы
- **pending** — конверсия принята, ожидает отправки в Метрику
- **sent** — успешно отправлена в Метрику
- **error** — ошибка при отправке (детали в error_message)
- Возможна реализация повторных попыток, TTL, логирования изменений

---

## 4. Связи и индексация
- Все ключевые поля индексируются для быстрого поиска по статусу, счетчику, времени
- Внешние ключи обеспечивают целостность между пользователем, аккаунтом, счетчиком, вебхуком и конверсией

---

## 5. Рекомендации по миграциям и эксплуатации
- Использовать UUID для всех id (масштабируемость, безопасность)
- Хранить payload как JSONB для гибкости структуры данных
- Для аудита можно добавить отдельную таблицу логов событий (опционально)
- Для массовых загрузок — реализовать батч-обработку и асинхронную отправку в Метрику

---

## 6. Пример сценария работы
1. Пользователь создаёт Яндекс-аккаунт и привязывает счетчик
2. Для счетчика создаётся вебхук
3. Вебхук принимает POST-запрос с офлайн-конверсией, сохраняет в webhook_conversions со статусом pending
4. Сервис отправляет конверсию в Метрику, обновляет статус на sent или error
5. В любой момент можно получить историю событий, статус отправки, детали ошибок

---

## 7. Пример запроса к таблице для мониторинга
```sql
SELECT * FROM webhook_conversions WHERE status = 'error' AND created_at > NOW() - INTERVAL '1 day';
```

---

## 8. Возможные расширения
- Добавить поля для интеграции с CRM (order_id, external_source и т.д.)
- Вести историю изменений статусов (audit trail)
- Реализовать TTL для автоматического удаления старых записей
- Добавить web-интерфейс для мониторинга и ручного управления статусами 