"""README charts, drawn from the rows of the KPI queries. Pure: rows in, matplotlib figure out.

Why static PNGs and not a dashboard service (docs/decisions.md, ADR-008): QuickSight is a monthly
subscription that breaks the < $1 budget. A PNG in docs/images/ is readable by anyone on GitHub.

Style: neutral grey for context, one highlight color for the finding, every value labelled with
its unit, and a title that states the question the chart answers.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import matplotlib

matplotlib.use("Agg")  # draw to files, no window (works on servers and in CI)

import matplotlib.pyplot as plt  # noqa: E402  (must come after choosing the backend)
from matplotlib.figure import Figure  # noqa: E402

Rows = list[dict[str, Any]]

# Colors: context vs the thing the reader should look at
GREY = "#9AA0A6"
HIGHLIGHT = "#C0392B"
SECOND = "#2C3E50"

# Shift order on the floor (a day starts with the morning shift)
SHIFT_ORDER = ("MORNING", "EVENING", "NIGHT")


def _rows_at(rows: Rows, level: str) -> Rows:
    """Keep the rows of one hierarchy level of a ROLLUP query (e.g. "3 Lot")."""
    return [row for row in rows if row["level"] == level]


def _finish(fig: Figure, title: str, subtitle: str) -> Figure:
    """Common layout: a question as title, the data source as subtitle, tight margins."""
    fig.suptitle(title, x=0.02, ha="left", fontsize=13, fontweight="bold")
    fig.text(0.02, 0.91, subtitle, ha="left", fontsize=9, color=GREY)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    return fig


def supplier_lot_chart(rows: Rows, top: int = 10) -> Figure:
    """Which supplier lots send the most defective parts? (rows of 02_reject_rate_by_supplier_lot)

    Args:
        rows: Query rows.
        top: How many lots to show, worst first.

    Returns:
        A horizontal bar chart; the worst lot is highlighted, the overall rate drawn as a line.
    """
    # Lots, worst supplier defect rate first
    lots = sorted(_rows_at(rows, "3 Lot"), key=lambda row: row["supplier_defect_rate_pct"] or 0, reverse=True)[:top]
    overall = _rows_at(rows, "0 All suppliers")[0]["supplier_defect_rate_pct"]

    # One bar per lot, labelled supplier · part · lot, worst on top
    labels = [f"{row['supplier']} · {row['part']} · {row['lot']}" for row in lots][::-1]
    values = [row["supplier_defect_rate_pct"] for row in lots][::-1]
    colors = [GREY] * (len(values) - 1) + [HIGHLIGHT]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars = ax.barh(labels, values, color=colors)
    ax.bar_label(bars, labels=[f"{value:.1f}%" for value in values], padding=3, fontsize=9)

    # The plant-wide rate, so "high" has a reference
    ax.axvline(overall, color=SECOND, linestyle="--", linewidth=1)
    ax.text(overall, len(values) - 0.4, f" all lots: {overall:.1f}%", color=SECOND, fontsize=9, va="bottom")
    ax.set_xlabel("Supplier defect rate (% of parts taken from the lot)")
    ax.spines[["top", "right"]].set_visible(False)
    return _finish(fig, "Which supplier lots send defective parts?", "Supplier → Part → Lot · sql/02")


def scrap_cost_chart(rows: Rows) -> Figure:
    """How much did scrap cost per supplier, and how much can be claimed back? (rows of 05)

    Returns:
        A stacked bar per supplier: recoverable (supplier defects) and internal loss (our process).
    """
    # Supplier totals across plants (level "2 Supplier" has one row per plant and supplier)
    recoverable: dict[str, float] = defaultdict(float)
    internal: dict[str, float] = defaultdict(float)
    for row in _rows_at(rows, "2 Supplier"):
        recoverable[row["supplier"]] += row["recoverable_usd"]
        internal[row["supplier"]] += row["internal_loss_usd"]

    # Biggest total cost first
    suppliers = sorted(recoverable, key=lambda name: recoverable[name] + internal[name], reverse=True)
    claim = [recoverable[name] for name in suppliers]
    loss = [internal[name] for name in suppliers]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.bar(suppliers, claim, color=HIGHLIGHT, label="Recoverable from supplier (supplier defect)")
    ax.bar(suppliers, loss, bottom=claim, color=GREY, label="Internal loss (process defect)")

    # Total on top of each bar, in thousands of dollars
    for index, total in enumerate(recovered + lost for recovered, lost in zip(claim, loss, strict=True)):
        ax.text(index, total, f"${total / 1000:,.1f}k", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Scrap cost (USD)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda value, _: f"${value / 1000:,.0f}k"))
    ax.legend(frameon=False, fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    return _finish(fig, "How much did scrap cost, and who pays for it?", "Plant → Supplier → Part · sql/05")


def throughput_chart(rows: Rows) -> Figure:
    """Which line and shift are behind plan? (rows of 04_throughput_vs_plan)

    Returns:
        Grouped bars: one group per line, one bar per shift; bars under 90% are highlighted.
    """
    # Sum both plants per line and shift (level "3 Shift" has one row per plant, line and shift)
    planned: dict[tuple[str, str], float] = defaultdict(float)
    produced: dict[tuple[str, str], float] = defaultdict(float)
    for row in _rows_at(rows, "3 Shift"):
        key = (row["line"], row["shift"])
        planned[key] += row["planned_units"]
        produced[key] += row["produced_units"]
    lines = sorted({line for line, _shift in planned})

    fig, ax = plt.subplots(figsize=(9, 4.8))
    width = 0.26
    for offset, shift in enumerate(SHIFT_ORDER):
        # Throughput % of every line on this shift
        pct = [100 * produced[(line, shift)] / planned[(line, shift)] for line in lines]
        positions = [index + (offset - 1) * width for index in range(len(lines))]
        colors = [HIGHLIGHT if value < 90 else GREY for value in pct]
        bars = ax.bar(positions, pct, width=width * 0.95, color=colors)
        ax.bar_label(bars, labels=[f"{shift[:3]}\n{value:.0f}%" for value in pct], fontsize=8, padding=2)

    # The plan itself, as the reference line
    ax.axhline(100, color=SECOND, linestyle="--", linewidth=1)
    ax.set_xticks(range(len(lines)), lines)
    ax.set_ylim(0, 118)
    ax.set_ylabel("Produced vs planned units (%)")
    ax.spines[["top", "right"]].set_visible(False)
    return _finish(fig, "Which line and shift are behind the production plan?", "Plant → Line → Shift · sql/04")


def coverage_chart(rows: Rows, threshold_days: float) -> Figure:
    """Which parts are running out? (rows of 03_stock_coverage)

    Args:
        rows: Query rows (one per dt, plant and part).
        threshold_days: The alert threshold (config: alerts.stock_coverage_threshold_days).

    Returns:
        Every plant and part as a thin grey line (context), the one with the lowest coverage on the
        last day highlighted and labelled, and the alert threshold as a dashed line.
    """
    # One time series per plant and part, in date order
    by_item: dict[tuple[str, str], Rows] = defaultdict(list)
    for row in sorted(rows, key=lambda row: row["dt"]):
        by_item[(row["plant"], row["part"])].append(row)

    # The item with the least coverage at the end of the period is the story
    lowest = min(by_item, key=lambda item: by_item[item][-1]["coverage_days"])

    fig, ax = plt.subplots(figsize=(9, 4.8))
    for item, points in by_item.items():
        days = [row["dt"][5:] for row in points]
        coverage = [row["coverage_days"] for row in points]

        # Context: every other part, thin and grey, unlabelled
        if item != lowest:
            ax.plot(days, coverage, color=GREY, linewidth=0.8, alpha=0.6)
            continue

        # The finding: thick, colored, named at its last point with its value
        ax.plot(days, coverage, color=HIGHLIGHT, linewidth=2.2, marker="o", zorder=3)
        ax.annotate(
            f"{item[1]} ({item[0]})\n{coverage[-1]:.2f} days left",
            (len(days) - 1, coverage[-1]),
            xytext=(-12, -28),
            textcoords="offset points",
            ha="right",
            fontsize=9,
            color=HIGHLIGHT,
        )

    # The alert threshold, and a note that grey lines are the other parts
    ax.axhline(threshold_days, color=SECOND, linestyle="--", linewidth=1)
    ax.text(0, threshold_days, f" alert below {threshold_days:g} day", color=SECOND, fontsize=9, va="bottom")
    ax.text(0, ax.get_ylim()[1], " grey: every other plant and part", color=GREY, fontsize=8, va="top")
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Day (UTC, end of day)")
    ax.set_ylabel("Stock coverage (days of planned usage)")
    ax.spines[["top", "right"]].set_visible(False)
    return _finish(fig, "Which parts are running out?", "Plant → Part → Supplier · sql/03")
