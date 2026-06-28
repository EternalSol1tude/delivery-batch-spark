"""
Графики результатов для README (docs/): из витрин ClickHouse и benchmark.json.
Запуск:  python scripts/make_charts.py   (нужны matplotlib + pandas)
"""
import io
import json
import os
import urllib.request
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CH = os.getenv("CLICKHOUSE_HTTP_URL", "http://localhost:8124")
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs"
OUT.mkdir(exist_ok=True)

plt.rcParams.update({"figure.dpi": 130, "font.size": 10, "axes.grid": True,
                     "grid.alpha": 0.3, "axes.spines.top": False, "axes.spines.right": False})


def q(sql: str) -> pd.DataFrame:
    req = urllib.request.Request(CH, data=(sql + " FORMAT CSVWithNames").encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return pd.read_csv(io.BytesIO(r.read()))


def chart_benchmark():
    path = ROOT / "data" / "metrics" / "benchmark.json"
    if not path.exists():
        print("skip benchmark: нет data/metrics/benchmark.json")
        return
    m = json.loads(path.read_text())
    fig, ax = plt.subplots(figsize=(7, 4))
    pairs = [
        ("Join:\nshuffle", m["testA_shuffle_join_sec"], "#c0392b"),
        ("Join:\nbroadcast", m["testA_broadcast_join_sec"], "#27ae60"),
        ("Чтение:\nCSV", m["testB_csv_read_sec"], "#c0392b"),
        ("Чтение:\nParquet", m["testB_parquet_read_sec"], "#27ae60"),
    ]
    ax.bar([p[0] for p in pairs], [p[1] for p in pairs], color=[p[2] for p in pairs])
    for i, p in enumerate(pairs):
        ax.text(i, p[1], f" {p[1]}s", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("секунды (меньше — лучше)")
    ax.set_title(f"Оптимизация Spark: broadcast join ×{m['testA_speedup_x']}, "
                 f"Parquet vs CSV ×{m['testB_speedup_x']}")
    fig.tight_layout(); fig.savefig(OUT / "benchmark.png"); plt.close(fig)
    print("ok: docs/benchmark.png")


def chart_hourly():
    df = q("SELECT order_dow, order_hour, n_orders FROM analytics.mart_hourly_demand")
    piv = df.pivot(index="order_dow", columns="order_hour", values="n_orders").fillna(0)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    im = ax.imshow(piv.values, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(0, 24, 2)); ax.set_xticklabels(range(0, 24, 2))
    ax.set_yticks(range(7)); ax.set_yticklabels(range(7))
    # в датасете не задокументировано, какой календарный день = 0, поэтому 0–6
    ax.set_xlabel("час (order_hour_of_day)")
    ax.set_ylabel("день недели (order_dow, 0–6)")
    ax.set_title("Спрос: число заказов по дню недели × часу")
    fig.colorbar(im, ax=ax, label="заказов"); fig.tight_layout()
    fig.savefig(OUT / "hourly_demand.png"); plt.close(fig)
    print("ok: docs/hourly_demand.png")


def chart_department_reorder():
    df = q("SELECT department, reorder_rate FROM analytics.mart_department_kpis "
           "ORDER BY reorder_rate DESC")
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(df["department"], df["reorder_rate"], color="#2c7fb8")
    ax.invert_yaxis()
    ax.set_xlabel("доля повторных покупок (reorder rate)")
    ax.set_title("Какие категории чаще перезаказывают")
    for y, v in enumerate(df["reorder_rate"]):
        ax.text(v, y, f" {v:.2f}", va="center", fontsize=8)
    fig.tight_layout(); fig.savefig(OUT / "department_reorder.png"); plt.close(fig)
    print("ok: docs/department_reorder.png")


if __name__ == "__main__":
    chart_benchmark()
    chart_hourly()
    chart_department_reorder()
