#!/bin/bash

# Конфигурация по умолчанию
HOST="0.0.0.0"
PORT=8000
PID_FILE=".api_pid"
LOG_FILE="api.log"
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Функция для вывода сообщений
log() {
    local color="$1"
    local message="$2"
    echo -e "${color}${message}${NC}"
}

# Функция для запуска API
start() {
    # Проверяем, не запущен ли уже API
    if [ -f "$PID_FILE" ] && ps -p $(cat "$PID_FILE") > /dev/null; then
        log "$YELLOW" "API уже запущен (PID: $(cat "$PID_FILE"))"
        log "$YELLOW" "Для перезапуска используйте: $0 restart"
        return 1
    fi

    # Запускаем API с uvicorn
    log "$GREEN" "Запуск API на $HOST:$PORT..."
    nohup uvicorn app.main:app --host "$HOST" --port "$PORT" > "$LOG_FILE" 2>&1 &
    
    # Сохраняем PID процесса
    PID=$!
    echo $PID > "$PID_FILE"
    
    # Ждем несколько секунд чтобы убедиться, что API запустился
    sleep 2
    if ps -p $PID > /dev/null; then
        log "$GREEN" "✓ API успешно запущен (PID: $PID)"
        log "$GREEN" "API доступен по адресу: http://$HOST:$PORT"
        log "$GREEN" "Документация API: http://$HOST:$PORT/docs"
        log "$GREEN" "Логи сохраняются в файле: $LOG_FILE"
    else
        log "$RED" "✗ Ошибка при запуске API. Проверьте файл логов: $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

# Функция для остановки API
stop() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        
        # Проверяем, запущен ли процесс
        if ps -p $PID > /dev/null; then
            log "$YELLOW" "Остановка API (PID: $PID)..."
            kill $PID
            
            # Ждем завершения процесса
            for i in {1..5}; do
                if ! ps -p $PID > /dev/null; then
                    break
                fi
                sleep 1
            done
            
            # Если процесс всё еще работает, принудительно завершаем
            if ps -p $PID > /dev/null; then
                log "$YELLOW" "Принудительное завершение процесса..."
                kill -9 $PID
                sleep 1
            fi
            
            if ! ps -p $PID > /dev/null; then
                log "$GREEN" "✓ API успешно остановлен"
            else
                log "$RED" "✗ Не удалось остановить API"
                return 1
            fi
        else
            log "$YELLOW" "API не запущен или был остановлен извне (PID: $PID не найден)"
        fi
        
        rm -f "$PID_FILE"
    else
        log "$YELLOW" "API не запущен (файл $PID_FILE не найден)"
    fi
}

# Функция для проверки статуса API
status() {
    if [ -f "$PID_FILE" ] && ps -p $(cat "$PID_FILE") > /dev/null; then
        PID=$(cat "$PID_FILE")
        log "$GREEN" "✓ API запущен (PID: $PID)"
        log "$GREEN" "API доступен по адресу: http://$HOST:$PORT"
        log "$GREEN" "Документация API: http://$HOST:$PORT/docs"
    else
        if [ -f "$PID_FILE" ]; then
            log "$RED" "✗ API не запущен (PID из файла не найден)"
            rm -f "$PID_FILE"
        else
            log "$RED" "✗ API не запущен (файл $PID_FILE не найден)"
        fi
    fi
}

# Основная логика скрипта
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 2
        start
        ;;
    status)
        status
        ;;
    *)
        echo "Использование: $0 {start|stop|restart|status}"
        echo ""
        echo "  start   - запуск API-сервера"
        echo "  stop    - остановка API-сервера"
        echo "  restart - перезапуск API-сервера"
        echo "  status  - проверка статуса API-сервера"
        exit 1
        ;;
esac

exit 0 