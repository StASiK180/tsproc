# plot_results.py
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

CSV_PATH = 'test_results.csv'
OUT_DIR = 'figures'
os.makedirs(OUT_DIR, exist_ok=True)

if not os.path.exists(CSV_PATH):
    raise SystemExit('CSV not found: ' + CSV_PATH)

# Чтение
df = pd.read_csv(CSV_PATH, parse_dates=['window_start', 'window_end'])
for col in ['count', 'avg', 'min', 'max', 'p95']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

metrics = sorted(df['metric'].unique())

for m in metrics:
    d = df[df['metric'] == m].sort_values('window_start')
    if d.empty:
        continue
    fig = go.Figure()
    fig.add_trace(go.Bar(x=d['window_start'], y=d['count'], name='count', marker_color='rgba(55,83,109,0.7)'))
    if 'avg' in d.columns:
        fig.add_trace(go.Scatter(x=d['window_start'], y=d['avg'], mode='lines+markers', name='avg', line=dict(color='orange'), yaxis='y2'))
    if 'p95' in d.columns:
        fig.add_trace(go.Scatter(x=d['window_start'], y=d['p95'], mode='lines+markers', name='p95', line=dict(color='green'), yaxis='y2'))
    fig.update_layout(title=f'Metric={m}: count (bar) and avg/p95 (lines)',
                      xaxis_title='window_start',
                      yaxis=dict(title='count'),
                      yaxis2=dict(title='value', overlaying='y', side='right'))
    png_path = os.path.join(OUT_DIR, f'{m}_summary.png')
    html_path = os.path.join(OUT_DIR, f'{m}_summary.html')
    try:
        # Попытка сохранить PNG (требуется kaleido)
        fig.write_image(png_path, width=1200, height=600)
        print('Saved', png_path)
    except Exception as e:
        fig.write_html(html_path)
        print('Saved', html_path, '(PNG failed:', e, ')')

# Pie chart
if 'count' in df.columns:
    agg_counts = df.groupby('metric', as_index=False)['count'].sum()
    pie = px.pie(agg_counts, names='metric', values='count', title='Total events per metric')
    pie_png = os.path.join(OUT_DIR, 'total_events_per_metric.png')
    pie_html = os.path.join(OUT_DIR, 'total_events_per_metric.html')
    try:
        pie.write_image(pie_png, width=800, height=600)
        print('Saved', pie_png)
    except Exception as e:
        pie.write_html(pie_html)
        print('Saved', pie_html, '(PNG failed:', e, ')')

# Summary
summary = df.groupby('metric').agg(total_events=('count', 'sum'),
                                   windows_count=('count', 'size'),
                                   avg_of_avg=('avg', 'mean'),
                                   mean_p95=('p95', 'mean')).reset_index()
summary_csv = os.path.join(OUT_DIR, 'summary_stats.csv')
summary.to_csv(summary_csv, index=False)
print('Saved summary stats to', summary_csv)
