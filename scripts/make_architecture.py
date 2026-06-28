"""Схема архитектуры Project 2 -> docs/architecture.png.  Запуск: python scripts/make_architecture.py"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parent.parent / "docs"
OUT.mkdir(exist_ok=True)

C = {"land": "#5b8c5a", "orch": "#e3b23c", "spark": "#c84630", "dq": "#8e5572",
     "dwh": "#30638e", "ui": "#5c6672"}

fig, ax = plt.subplots(figsize=(13, 6))
ax.set_xlim(0, 16); ax.set_ylim(0, 9); ax.axis("off")


def box(x, y, w, h, label, color, fs=10.5):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                linewidth=0, facecolor=color))
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", color="white",
            fontsize=fs, fontweight="bold")
    return (x, y, w, h)


def arrow(p1, p2, color="#333", dashed=False):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=16, lw=1.6,
                                 color=color, linestyle="--" if dashed else "-"))


y, h, w, gap = 4.0, 1.5, 2.45, 0.5
xs = [0.3 + i * (w + gap) for i in range(5)]
labels = [
    ("CSV landing\n(FileSensor)", C["land"], 10),
    ("Spark: clean\nCSV → Parquet", C["spark"], 10),
    ("Spark: marts\nbroadcast + window", C["spark"], 9.5),
    ("Data Quality\n(контракты)", C["dq"], 10),
    ("ClickHouse\n(витрины)", C["dwh"], 10),
]
boxes = [box(x, y, w, h, lab, col, fs) for x, (lab, col, fs) in zip(xs, labels)]
for a, b in zip(boxes, boxes[1:]):
    arrow((a[0] + a[2], y + h / 2), (b[0], y + h / 2))

# Airflow сверху
orch = box(xs[1], 7.0, xs[3] + w - xs[1], 1.1,
           "Apache Airflow — оркестрация: FileSensor · зависимости · retry", C["orch"], 10)
for bx in boxes[1:4]:
    arrow((bx[0] + bx[2] / 2, orch[1]), (bx[0] + bx[2] / 2, y + h), color=C["orch"], dashed=True)

# Spark History снизу
ui = box(xs[1], 1.3, xs[2] + w - xs[1], 0.95,
         "Spark History UI — стадии, шафлы, замер до/после", C["ui"], 10)
for bx in (boxes[1], boxes[2]):
    arrow((bx[0] + bx[2] / 2, y), (bx[0] + bx[2] / 2, ui[1] + ui[3]), color=C["ui"], dashed=True)

ax.text(8, 8.6, "Delivery Batch Analytics on Spark", ha="center", fontsize=14,
        fontweight="bold", color="#222")
fig.tight_layout(); fig.savefig(OUT / "architecture.png", dpi=140, bbox_inches="tight")
print("ok: docs/architecture.png")
