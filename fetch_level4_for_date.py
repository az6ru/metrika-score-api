#!/usr/bin/env python3
"""fetch_level4_for_date.py

Сценарий «всё-в-одном»: за заданную дату скачивает сырые логи Яндекс.Метрики (visits + hits),
вычисляет признаки активности и сохраняет JSON-файл со списком визитов уровня 4+.

• Использует API-токен и номер счётчика из переменных окружения (METRIKA_TOKEN, METRIKA_COUNTER)
  либо значения по умолчанию, указанные ниже.
• Прогоняет упрощённый pipeline: правило 180-5-5 + ML-модели `level4_desktop_slot*.joblib` /
  `level4_mobile_slot*.joblib` с порогами из `level4_thresholds.json`.
• В выходе на каждый визит пишется: visitId, clientId, dateTime (полное), visitDuration.

Пример запуска:
    python scripts/fetch_level4_for_date.py --date 2025-07-01
"""
import csv
import gzip
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import requests
from joblib import load

def calculate_level4_visits(date: str, token: str, counter: int, logger=print) -> List[dict]:
    logger(f"[metrika] Старт расчёта за {date}, counter={counter}")
    HEADERS = {"Authorization": f"OAuth {token}"}
    WATCH_RE = re.compile(r"\d+")

    def create_request(source: str) -> int:
        url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequests"
        if source == "visits":
            fields = (
                "ym:s:visitID,ym:s:clientID,ym:s:watchIDs,ym:s:dateTime,"
                "ym:s:visitDuration,ym:s:bounce,ym:s:pageViews,"
                "ym:s:deviceCategory"
            )
        else:  # hits
            fields = (
                "ym:pv:watchID,ym:pv:dateTime,ym:pv:deviceCategory"
            )
        params = {"date1": date, "date2": date, "fields": fields, "source": source}
        logger(f"[metrika] POST {source} logrequest ...")
        r = requests.post(url, headers=HEADERS, params=params)
        r.raise_for_status()
        req_id = r.json()["log_request"]["request_id"]
        logger(f"[metrika] {source} req_id={req_id}")
        return req_id

    def wait_processed(request_id: int, source: str):
        url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}"
        logger(f"[metrika] Ожидание обработки {source} ...")
        while True:
            status = requests.get(url, headers=HEADERS).json()["log_request"]["status"]
            logger(f"[metrika] {source} status={status}")
            if status == "processed":
                return
            if status not in ("created", "processing", "processed_with_errors", "awaiting_retry"):
                raise RuntimeError(f"Request {request_id} bad status {status}")
            time.sleep(8)

    def download_data(request_id: int, source: str) -> List[dict]:
        url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/part/0/download"
        logger(f"[metrika] Скачивание {source} ...")
        r = requests.get(url, headers=HEADERS, stream=True)
        r.raise_for_status()
        with gzip.open(r.raw, mode="rt") as f:
            data = list(csv.DictReader(f, delimiter="\t"))
        logger(f"[metrika] {source} получено {len(data):,}")
        return data

    def clean_request(request_id: int, source: str):
        url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/clean"
        logger(f"[metrika] Чистка {source} ...")
        requests.post(url, headers=HEADERS).raise_for_status()

    req_vis = create_request("visits")
    req_hit = create_request("hits")
    wait_processed(req_vis, "visits")
    wait_processed(req_hit, "hits")
    visits_raw = download_data(req_vis, "visits")
    hits_raw = download_data(req_hit, "hits")
    clean_request(req_vis, "visits")
    clean_request(req_hit, "hits")

    logger(f"[metrika] Формирование вспомогательных карт ...")
    watch_time: Dict[str, pd.Timestamp] = {h["ym:pv:watchID"]: pd.to_datetime(h["ym:pv:dateTime"]) for h in hits_raw}
    device_map: Dict[str, int] = {}
    for h in hits_raw:
        vid = h["ym:pv:watchID"][:19]
        cat = h["ym:pv:deviceCategory"] or "0"
        if vid not in device_map:
            device_map[vid] = int(cat)

    def active_slots(ids: List[str]) -> int:
        times = [watch_time.get(i) for i in ids if i in watch_time]
        if not times:
            return 0
        t0 = min(times)
        return len({int((t - t0).total_seconds() // 15) for t in times})

    logger(f"[metrika] Подготовка признаков ...")
    rows = []
    for v in visits_raw:
        vid = v["ym:s:visitID"]
        wids = WATCH_RE.findall(v["ym:s:watchIDs"] or "")
        slots_cnt = active_slots(wids)
        times = [watch_time.get(i) for i in wids if i in watch_time]
        if times:
            t0 = min(times)
            slot_range_val = max(int((t - t0).total_seconds() // 15) for t in times) - min(int((t - t0).total_seconds() // 15) for t in times)
            pauses = np.diff(sorted([t.value for t in times])) / 1e9 if len(times) > 1 else np.array([])
            median_pause = float(np.median(pauses)) if pauses.size else 0.0
            mean_pause = float(np.mean(pauses)) if pauses.size else 0.0
            std_pause = float(np.std(pauses)) if pauses.size else 0.0
        else:
            slot_range_val = 0
            median_pause = mean_pause = std_pause = 0.0
        duration_sec = int(v["ym:s:visitDuration"])
        density = slots_cnt / ((duration_sec / 60) + 1)
        row = {
            "visitId": vid,
            "clientId": v["ym:s:clientID"],
            "dateTime": v["ym:s:dateTime"],
            "visitDuration": duration_sec,
            "duration": duration_sec,
            "bounce": int(v["ym:s:bounce"]),
            "pageViews": int(v["ym:s:pageViews"]),
            "device": device_map.get(vid, 0),
            "slots": slots_cnt,
            "slot_range": slot_range_val,
            "slot_density": density,
            "median_pause": median_pause,
            "mean_pause": mean_pause,
            "std_pause": std_pause,
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    logger(f"[metrika] Таблица shape={df.shape}")
    rule_mask = (df["visitDuration"] >= 180) & (df["pageViews"] >= 5) & (df["slots"] >= 5)
    pred_high = pd.Series(rule_mask, dtype=bool)
    thresholds = json.loads(Path("level4_thresholds.json").read_text()) if Path("level4_thresholds.json").exists() else {"desktop": 0.48, "mobile": 0.36}
    for code, name in [(1, "desktop"), (2, "mobile")]:
        idx = df[(df["device"] == code) & (~rule_mask)].index
        if idx.empty:
            continue
        for cand in [f"level4_{name}_slot_enhanced.joblib", f"level4_{name}_slot.joblib", f"level4_{name}.joblib"]:
            if Path(cand).exists():
                model_path = cand
                break
        else:
            logger(f"[metrika] Нет модели для {name}")
            raise FileNotFoundError(f"Нет модели для {name}")
        logger(f"[metrika] Применение модели {model_path} для {name} ...")
        model = load(model_path)
        model_feats = list(model.booster_.feature_name()) if hasattr(model, "booster_") else list(model.feature_name_)
        feat_names = [f for f in model_feats if f in df.columns]
        X = df.loc[idx, feat_names]
        proba = model.predict_proba(X)[:, 1]
        pred_high.loc[idx] = proba > thresholds.get(name, 0.5)
    sel = df.loc[pred_high, ["visitId", "clientId", "dateTime", "visitDuration"]]
    logger(f"[metrika] Готово! Визитов 4+ = {len(sel):,}")
    return sel.to_dict(orient="records") 