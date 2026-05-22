# full_test.py
import random
import time
import os
import importlib.util
from datetime import datetime, timezone, timedelta
from queue import Queue, Empty
import pandas as pd

# Загружаем tsproc.py как модуль (без необходимости ставить в sys.path)
spec = importlib.util.spec_from_file_location("tsproc_module", "tsproc.py")
tsproc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tsproc)

Event = tsproc.Event
Aggregator = tsproc.Aggregator

# Параметры теста (меняйте при необходимости)
NUM_EVENTS = 100000
DURATION_S = 60.0
METRICS = ['cpu', 'mem', 'io']
WINDOW = 10.0
SLIDE = 5.0
AGGS = ['count', 'avg', 'min', 'max', 'p95']

out_q = Queue()
agg = Aggregator(window_s=WINDOW, slide_s=SLIDE, aggs=AGGS, out_q=out_q)

start_time = datetime.now(timezone.utc)
# генерируем timestamps равномерно
timestamps = [start_time + timedelta(seconds=(i / NUM_EVENTS) * DURATION_S) for i in range(NUM_EVENTS)]
values = [random.random() * 100 for _ in range(NUM_EVENTS)]
metrics = [METRICS[i % len(METRICS)] for i in range(NUM_EVENTS)]

print('Feeding events...')
t0 = time.time()
for i in range(NUM_EVENTS):
    ev = Event(metrics[i], values[i], timestamps[i])
    agg.add(ev)
# Завершаем
agg.stop()
# Ждём чтобы flusher успел
time.sleep(0.5)

# Считываем результаты
results = []
while True:
    try:
        item = out_q.get(timeout=1.0)
    except Empty:
        break
    if item is None:
        break
    results.append(item)

print('Collected', len(results), 'windows')

# Сохраняем в CSV
rows = []
for it in results:
    rows.append({
        'window_start': it['window_start'],
        'window_end': it['window_end'],
        'metric': it['metric'],
        'count': it.get('count'),
        'avg': it.get('avg'),
        'min': it.get('min'),
        'max': it.get('max'),
        'p95': it.get('p95')
    })

if rows:
    df = pd.DataFrame(rows)
    df.to_csv('test_results.csv', index=False)
    print('Saved test_results.csv')
else:
    print('No windows produced, nothing saved')
