-- Создание таблицы для хранения информации о загрузках конверсий
CREATE TABLE IF NOT EXISTS conversion_uploads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metrika_upload_id TEXT NOT NULL,
    task_id UUID REFERENCES tasks(id),
    counter INT NOT NULL,
    target TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    total_conversions INT,
    processed_conversions INT,
    errors JSONB
);

-- Создание индексов
CREATE INDEX IF NOT EXISTS idx_conversion_uploads_task_id ON conversion_uploads(task_id);
CREATE INDEX IF NOT EXISTS idx_conversion_uploads_metrika_upload_id ON conversion_uploads(metrika_upload_id);

-- Создание триггера для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_conversion_uploads_updated_at
BEFORE UPDATE ON conversion_uploads
FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column(); 