-- Таблица вебхуков для офлайн-конверсий
CREATE TABLE IF NOT EXISTS webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    counter_id INTEGER NOT NULL,
    token TEXT NOT NULL,
    description TEXT,
    secret TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Таблица пакетов конверсий
CREATE TABLE IF NOT EXISTS webhook_batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id UUID NOT NULL REFERENCES webhooks(id),
    counter_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    total INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    metrika_upload_id TEXT,
    errors TEXT[]
);

-- Таблица офлайн-конверсий, принятых через вебхук
CREATE TABLE IF NOT EXISTS webhook_conversions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL REFERENCES webhook_batches(id),
    client_id TEXT,
    user_id TEXT,
    yclid TEXT,
    purchase_id TEXT,
    target TEXT NOT NULL,
    date_time TEXT NOT NULL,
    price NUMERIC,
    currency TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    error TEXT
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_webhook_batches_webhook_id ON webhook_batches(webhook_id);
CREATE INDEX IF NOT EXISTS idx_webhook_batches_status ON webhook_batches(status);
CREATE INDEX IF NOT EXISTS idx_webhook_conversions_batch_id ON webhook_conversions(batch_id);
CREATE INDEX IF NOT EXISTS idx_webhook_conversions_status ON webhook_conversions(status);

-- Триггер для обновления updated_at в таблице webhook_batches
CREATE OR REPLACE FUNCTION update_webhook_batches_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_webhook_batches_updated_at
BEFORE UPDATE ON webhook_batches
FOR EACH ROW
EXECUTE PROCEDURE update_webhook_batches_updated_at();

-- Триггер для обновления updated_at в таблице webhook_conversions
CREATE OR REPLACE FUNCTION update_webhook_conversions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_webhook_conversions_updated_at
BEFORE UPDATE ON webhook_conversions
FOR EACH ROW
EXECUTE PROCEDURE update_webhook_conversions_updated_at();

-- Функция для автоматического обновления статуса пакета при обновлении статуса конверсий
CREATE OR REPLACE FUNCTION update_webhook_batch_status()
RETURNS TRIGGER AS $$
DECLARE
    total_count INTEGER;
    processed_count INTEGER;
    batch_id UUID;
BEGIN
    batch_id := NEW.batch_id;
    
    -- Подсчитываем общее количество конверсий в пакете с явным указанием таблицы
    SELECT COUNT(*) INTO total_count
    FROM webhook_conversions
    WHERE webhook_conversions.batch_id = NEW.batch_id;
    
    -- Подсчитываем количество обработанных конверсий с явным указанием таблицы
    SELECT COUNT(*) INTO processed_count
    FROM webhook_conversions
    WHERE webhook_conversions.batch_id = NEW.batch_id AND webhook_conversions.status != 'pending';
    
    -- Обновляем статус пакета с явным указанием таблицы
    UPDATE webhook_batches
    SET 
        total = total_count,
        processed = processed_count
    WHERE webhook_batches.id = batch_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_webhook_batch_status
AFTER INSERT OR UPDATE ON webhook_conversions
FOR EACH ROW
EXECUTE PROCEDURE update_webhook_batch_status(); 