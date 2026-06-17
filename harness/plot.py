"""Render the pitch graph: checkout latency over time, per scenario.

Optional — only runs if matplotlib is installed (``pip install -e '.[plot]'``).
"""

from __future__ import annotations


def render_latency_plot(
    series_by_label: dict[str, list[tuple[float, float]]],
    out_path: str,
) -> bool:
    """Write a PNG comparing checkout latency timelines. Returns False if matplotlib missing."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001 - plotting is strictly optional
        return False

    colors = {"ungoverned": "#d62728", "governed": "#2ca02c", "observe": "#ff7f0e"}

    fig, ax = plt.subplots(figsize=(10, 5))
    for label, series in series_by_label.items():
        if not series:
            continue
        xs = [t for t, _ in series]
        ys = [ms for _, ms in series]
        ax.plot(xs, ys, label=label, color=colors.get(label), linewidth=1.2, alpha=0.85)

    ax.set_title("Checkout latency during a backfill — ungoverned vs governed")
    ax.set_xlabel("seconds since backfill start")
    ax.set_ylabel("checkout latency (ms)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return True
