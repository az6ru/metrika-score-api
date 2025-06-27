import requests
import time
import sys

API_URL = "http://127.0.0.1:8000"
DATE = "2025-06-26"
TOKEN = "y0__xCF6JHDBhjaxDgg7cna1BMwsKrhhQg4wBPt_vbCABkSTKEzHEemgoEz3Q"
COUNTER = 92342184

if len(sys.argv) > 1:
    DATE = sys.argv[1]

print(f"[test] Запуск задачи на {DATE}")
resp = requests.post(f"{API_URL}/tasks", json={
    "date": DATE,
    "token": TOKEN,
    "counter": COUNTER
})
resp.raise_for_status()
task_id = resp.json()["task_id"]
print(f"[test] task_id: {task_id}")

# Poll status
for i in range(60):
    status = requests.get(f"{API_URL}/tasks/{task_id}/status").json()
    print(f"[test] [{i}] status: {status['status']} {status.get('message','')}")
    if status["status"] == "done":
        break
    if status["status"] == "failed":
        print(f"[test] Ошибка: {status.get('error')}")
        sys.exit(1)
    time.sleep(5)
else:
    print("[test] Timeout waiting for task to finish")
    sys.exit(1)

# Get result
result = requests.get(f"{API_URL}/tasks/{task_id}/result").json()
print(f"[test] Визитов 4+: {len(result)}")
if result:
    print(f"[test] Пример: {result[0]}") 