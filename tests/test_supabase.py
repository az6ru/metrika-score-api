#!/usr/bin/env python3
import os
import sys
from supabase import create_client, Client
from app.fetch_level4_for_date import calculate_level4_visits
from app.send_conversions import send_conversions_to_metrika
from app.pydantic_models import TaskRequest

# Получаем URL и ключ из переменных окружения
SUPABASE_URL = os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImV4cCI6MTk4MzgxMjk5Nn0.EGIM96RAZx35lJzdJsyH-qQwv8Hdp7fsn3W0YpN81IU")

print(f"Connecting to Supabase: {SUPABASE_URL}")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Если передан ID задачи, получаем её данные
if len(sys.argv) > 1:
    task_id = sys.argv[1]
    print(f"Getting task {task_id}...")
    try:
        resp = supabase.table("tasks").select("*").eq("id", task_id).execute()
        print(f"Response type: {type(resp)}")
        print(f"Response: {resp}")
        
        if hasattr(resp, 'data') and resp.data:
            print(f"Data found: {resp.data}")
            if len(resp.data) > 0:
                print(f"First item: {resp.data[0]}")
            else:
                print("Data is empty list")
        else:
            print("No data attribute or data is None")
    except Exception as e:
        print(f"Error: {e}")
else:
    # Иначе получаем список всех задач
    print("Getting all tasks...")
    try:
        resp = supabase.table("tasks").select("*").execute()
        print(f"Response type: {type(resp)}")
        print(f"Response: {resp}")
        
        if hasattr(resp, 'data') and resp.data:
            print(f"Found {len(resp.data)} tasks")
            for i, task in enumerate(resp.data[:3]):  # Показываем только первые 3 задачи
                print(f"Task {i+1}: {task}")
        else:
            print("No data attribute or data is None")
    except Exception as e:
        print(f"Error: {e}") 