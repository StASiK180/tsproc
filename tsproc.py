"""
tsproc.py -- прототип консольного процессора временных метрик (Python)
Простой, самодостаточный скрипт.
"""
import argparse
import sys
import csv
import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
import math
from queue import Queue, Empty

def parse_duration(s: str) -> float:
    s = s.strip()
    if s.endswith('ms'):
        return float(s[:-2]) / 1000.0
    if s.endswith('s'):
        return float(s[:-1])
    if s.endswith('m'):
        return float(s[:-1]) * 60.0
    if s.endswith('h'):
        return float(s[:-1]) * 3600.0
    return float(s)


def try_parse_time(ts: str, timefmt: str, unix_opt: str):
    ts = (ts or '').strip()
    if ts == '':
        return datetime.now(timezone.utc)
    if unix_opt in ('s', 'ms'):
        try:
            n = int(ts)
            if unix_opt == 's':
                return datetime.fromtimestamp(n, timezone.utc)
            else:
                return datetime.fromtimestamp(n / 1000.0, timezone.utc)
        except Exception:
            pass
    else:
        if ts.isdigit():
            n = int(ts)
            if n > 1_000_000_000_000:
                return datetime.fromtimestamp(n / 1000.0, timezone.utc)
            return datetime.fromtimestamp(n, timezone.utc)
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except Exception:
        pass
    try:
        return datetime.strptime(ts, timefmt).replace(tzinfo=timezone.utc)
    except Exception as e:
        raise ValueError(f"cannot parse time '{ts}': {e}")


def percentile(sorted_vals, p: float):
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    n = len(sorted_vals)
    rank = p / 100.0 * (n - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return sorted_vals[lo]
    frac = rank - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


class Event:
    __slots__ = ('metric', 'value', 'time')

    def __init__(self, metric: str, value: float, time: datetime):
        self.metric = metric
        self.value = value
        self.time = time


def parser_worker(infile, fmt, metric_field, value_field, ts_field, timefmt, unix_opt, out_q: Queue):
    try:
        if fmt == 'csv':
            reader = csv.reader(infile)
            try:
                header = next(reader)
            except StopIteration:
                return
            hdr = [h.strip() for h in header]
            for rec in reader:
                if not rec:
                    continue
                m = {hdr[i]: rec[i].strip() for i in range(min(len(hdr), len(rec)))}
                metric = m.get(metric_field, '').strip()
                val = m.get(value_field, '').strip()
                ts = m.get(ts_field, '').strip()
                if metric == '' or val == '':
                    continue
                try:
                    valf = float(val)
                except Exception:
                    continue
                try:
                    t = try_parse_time(ts, timefmt, unix_opt)
                except Exception:
                    t = datetime.now(timezone.utc)
                out_q.put(Event(metric, valf, t))
        else:
            for raw in infile:
                line = raw.strip()
                if not line:
                    continue
                if fmt == 'json':
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    metric = str(obj.get(metric_field, '')).strip()
                    val = obj.get(value_field, '')
                    ts = obj.get(ts_field, '')
                    try:
                        valf = float(val)
                    except Exception:
                        continue
                    tsstr = str(ts) if ts is not None else ''
                    try:
                        t = try_parse_time(tsstr, timefmt, unix_opt)
                    except Exception:
                        t = datetime.now(timezone.utc)
                    out_q.put(Event(metric, valf, t))
                else:  # line
                    parts = line.split(',') if ',' in line else line.split()
                    if len(parts) < 2:
                        continue
                    metric = parts[0].strip()
                    val = parts[1].strip()
                    ts = parts[2].strip() if len(parts) >= 3 else ''
                    try:
                        valf = float(val)
                    except Exception:
                        continue
                    try:
                        t = try_parse_time(ts, timefmt, unix_opt)
                    except Exception:
                        t = datetime.now(timezone.utc)
                    out_q.put(Event(metric, valf, t))
    finally:
        out_q.put(None)


class Aggregator:
    def __init__(self, window_s: float, slide_s: float, aggs, out_q: Queue):
        self.window_s = window_s
        self.slide_s = slide_s if slide_s is not None else window_s
        self.aggs = aggs
        self.out_q = out_q
        self.lock = threading.Lock()
        self.storage = defaultdict(list)
        self.running = True
        self._start_flusher()

    def _bucket_start(self, t: datetime):
        ts = t.timestamp()
        s = self.slide_s
        start = math.floor(ts / s) * s
        return start

    def add(self, ev: Event):
        start = self._bucket_start(ev.time)
        key = (start, ev.metric)
        with self.lock:
            self.storage[key].append(ev.value)

    def _start_flusher(self):
        self.ticker = threading.Thread(target=self._flusher_loop, daemon=True)
        self.ticker.start()

    def _flusher_loop(self):
        interval = self.slide_s
        while self.running:
            now = time.time()
            self.flush_up_to(now)
            time.sleep(interval)

    def flush_up_to(self, now_ts: float):
        to_emit = []
        with self.lock:
            keys = list(self.storage.keys())
            for key in keys:
                start_ts, metric = key
                end_ts = start_ts + self.window_s
                if now_ts >= end_ts:
                    vals = self.storage.pop(key, [])
                    to_emit.append((start_ts, end_ts, metric, vals))
        for start_ts, end_ts, metric, vals in to_emit:
            if not vals:
                continue
            vals_sorted = sorted(vals)
            res = {'window_start': datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
                   'window_end': datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
                   'metric': metric,
                   'count': len(vals)}
            s = sum(vals)
            if 'sum' in self.aggs:
                res['sum'] = s
            if 'avg' in self.aggs:
                res['avg'] = s / len(vals)
            if 'min' in self.aggs:
                res['min'] = vals_sorted[0]
            if 'max' in self.aggs:
                res['max'] = vals_sorted[-1]
            if 'p95' in self.aggs:
                res['p95'] = percentile(vals_sorted, 95.0)
            self.out_q.put(res)

    def stop(self):
        self.running = False
        self.flush_up_to(time.time() + self.window_s)


def output_worker(out_q: Queue, out_mode: str):
    first_csv = True
    csv_writer = None
    while True:
        try:
            item = out_q.get(timeout=1.0)
        except Empty:
            continue
        if item is None:
            break
        if out_mode == 'json':
            print(json.dumps(item, ensure_ascii=False))
        elif out_mode == 'csv':
            if first_csv:
                keys = ['window_start', 'window_end', 'metric', 'count'] + [k for k in sorted(k for k in item.keys() if k not in ('window_start', 'window_end', 'metric', 'count'))]
                csv_writer = csv.writer(sys.stdout)
                csv_writer.writerow(keys)
                first_csv = False
            row = [item.get('window_start'), item.get('window_end'), item.get('metric'), item.get('count')]
            for k in sorted(k for k in item.keys() if k not in ('window_start', 'window_end', 'metric', 'count')):
                row.append(item.get(k))
            csv_writer.writerow(row)
        else:
            parts = [f"[{item['window_start']} - {item['window_end']}] {item['metric']} count={item['count']}"]
            for k, v in item.items():
                if k in ('window_start', 'window_end', 'metric', 'count'):
                    continue
                parts.append(f"{k}={v}")
            print(' '.join(parts))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', '-i', default='-', help='input file path or - for stdin')
    ap.add_argument('--format', '-f', default='line', choices=['csv', 'json', 'line'])
    ap.add_argument('--window', default='10s', help='window size, e.g. 10s, 1m')
    ap.add_argument('--slide', default='', help='slide size for sliding windows, e.g. 5s (if empty -> tumbling)')
    ap.add_argument('--aggs', default='count,avg,min,max,sum,p95', help='comma separated aggregates')
    ap.add_argument('--out', default='console', choices=['console', 'csv', 'json'], help='output format')
    ap.add_argument('--metric', default='metric', help='metric field name (csv/json)')
    ap.add_argument('--value', default='value', help='value field name (csv/json)')
    ap.add_argument('--timestamp', default='timestamp', help='timestamp field name (csv/json)')
    ap.add_argument('--timefmt', default="%Y-%m-%dT%H:%M:%S%z", help='time format for parsing (used if not ISO)')
    ap.add_argument('--unix', default='auto', help="set to 's' or 'ms' to parse numeric unix timestamps, 'auto' to detect")
    args = ap.parse_args(argv)

    window_s = parse_duration(args.window)
    slide_s = None if args.slide == '' else parse_duration(args.slide)
    aggs = [a.strip() for a in args.aggs.split(',') if a.strip()]

    if args.input == '-':
        infile = sys.stdin
    else:
        infile = open(args.input, 'r', encoding='utf-8')

    ev_q = Queue(maxsize=10000)
    out_q = Queue()

    parser_t = threading.Thread(target=parser_worker, args=(infile, args.format, args.metric, args.value, args.timestamp, args.timefmt, args.unix, ev_q), daemon=True)
    parser_t.start()

    aggregator = Aggregator(window_s, slide_s, aggs, out_q)

    out_t = threading.Thread(target=output_worker, args=(out_q, args.out), daemon=True)
    out_t.start()

    while True:
        ev = ev_q.get()
        if ev is None:
            break
        aggregator.add(ev)

    aggregator.stop()
    time.sleep(0.05)
    out_q.put(None)
    out_t.join(timeout=5.0)
    if infile is not sys.stdin:
        infile.close()


if __name__ == '__main__':
    main()