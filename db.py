import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, Any

DB_PATH = "metrika_tasks.db"

def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        status TEXT,
        progress INTEGER,
        message TEXT,
        started_at TEXT,
        finished_at TEXT,
        error TEXT,
        params TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS results (
        task_id TEXT PRIMARY KEY,
        result_json TEXT
    )''')
    conn.commit()
    conn.close()

def create_task(task_id: str, params: Dict[str, Any]):
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''INSERT INTO tasks (id, status, progress, message, started_at, finished_at, error, params)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (task_id, 'pending', 0, '', now, None, None, json.dumps(params)))
    conn.commit()
    conn.close()

def update_task_status(task_id: str, status: str, progress: int = 0, message: str = '', error: Optional[str] = None, finished_at: Optional[str] = None):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''UPDATE tasks SET status=?, progress=?, message=?, error=?, finished_at=? WHERE id=?''',
              (status, progress, message, error, finished_at, task_id))
    conn.commit()
    conn.close()

def save_result(task_id: str, result: Any):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO results (task_id, result_json) VALUES (?, ?)''',
              (task_id, json.dumps(result, ensure_ascii=False)))
    conn.commit()
    conn.close()

def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''SELECT id, status, progress, message, started_at, finished_at, error, params FROM tasks WHERE id=?''', (task_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        'id': row[0],
        'status': row[1],
        'progress': row[2],
        'message': row[3],
        'started_at': row[4],
        'finished_at': row[5],
        'error': row[6],
        'params': json.loads(row[7]) if row[7] else {}
    }

def get_result(task_id: str) -> Optional[Any]:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''SELECT result_json FROM results WHERE task_id=?''', (task_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return json.loads(row[0]) 