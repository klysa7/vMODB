#!/usr/bin/env python3
"""
HATtrick-style throughput frontier plotter.

Reproduces the visualization style of Milkai et al. (SIGMOD 2022),
"How Good is My HTAP System?", Figure 2.

Reads a CSV with columns:
    tau, alpha, t_tps, a_qps, saturated

Produces a single chart containing:
  - Fixed-T lines  (orange dashed, circles)        — series with tau fixed, alpha varying
  - Fixed-A lines  (sky-blue dashed, diamonds)     — series with alpha fixed, tau varying
  - Bounding box   (red dashed rectangle)          — (0,0) to (X^T, X^A)
  - Proportional line (blue dashed)                — from (X^T, 0) to (0, X^A)
  - Throughput frontier (green solid)              — Pareto skyline of all points
  - AUC            (gray shaded area)              — area under frontier
  - Saturated cells (red ×)                        — cells that crashed

Usage:
    python frontier_plot.py results.csv
    python frontier_plot.py results.csv -o frontier.pdf -t "vMODB Config 1, sleep=500"
"""

import argparse
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.ticker import FuncFormatter


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def read_csv(path):
    """Return list of dicts with keys: tau, alpha, t_tps, a_qps, saturated."""
    rows = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                'tau': int(r['tau']),
                'alpha': int(r['alpha']),
                't_tps': float(r['t_tps']),
                'a_qps': float(r['a_qps']),
                'saturated': r.get('saturated', 'false').strip().lower() == 'true',
            })
    return rows


# ---------------------------------------------------------------------------
# Geometry: lines, baselines, frontier
# ---------------------------------------------------------------------------

def fixed_t_lines(rows):
    """For each tau > 0, return points sorted by alpha. Skip saturated cells."""
    by_tau = {}
    for r in rows:
        if r['tau'] == 0 or r['saturated']:
            continue
        by_tau.setdefault(r['tau'], []).append(r)
    for tau in by_tau:
        by_tau[tau].sort(key=lambda r: r['alpha'])
    return by_tau


def fixed_a_lines(rows):
    """For each alpha > 0, return points sorted by tau. Skip saturated cells."""
    by_alpha = {}
    for r in rows:
        if r['alpha'] == 0 or r['saturated']:
            continue
        by_alpha.setdefault(r['alpha'], []).append(r)
    for alpha in by_alpha:
        by_alpha[alpha].sort(key=lambda r: r['tau'])
    return by_alpha


def baselines(rows):
    """X^T = max T-tps when alpha=0.   X^A = max A-qps when tau=0."""
    xt_pool = [r['t_tps'] for r in rows
               if r['alpha'] == 0 and not r['saturated']]
    xa_pool = [r['a_qps'] for r in rows
               if r['tau'] == 0 and not r['saturated']]
    xt = max(xt_pool, default=0.0)
    xa = max(xa_pool, default=0.0)
    return xt, xa


def pareto_frontier(rows):
    """
    Pareto skyline of all non-saturated, non-trivial measured points,
    plus the (X^T, 0) and (0, X^A) anchor points so the frontier always
    touches both axes. Returns sorted list of (T-tps, A-qps).
    """
    xt, xa = baselines(rows)
    candidates = set()
    for r in rows:
        if r['saturated']:
            continue
        if r['t_tps'] == 0 and r['a_qps'] == 0:
            continue
        candidates.add((r['t_tps'], r['a_qps']))
    if xt > 0:
        candidates.add((xt, 0.0))
    if xa > 0:
        candidates.add((0.0, xa))

    frontier = []
    for p in candidates:
        dominated = False
        for q in candidates:
            if q == p:
                continue
            if (q[0] >= p[0] and q[1] >= p[1]
                    and (q[0] > p[0] or q[1] > p[1])):
                dominated = True
                break
        if not dominated:
            frontier.append(p)

    frontier.sort(key=lambda p: p[0])
    return frontier


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot(rows, output=None, title=None, x_label=None, y_label=None,
         show=True):
    fig, ax = plt.subplots(figsize=(8, 5.5))

    xt, xa = baselines(rows)
    fT = fixed_t_lines(rows)
    fA = fixed_a_lines(rows)
    front = pareto_frontier(rows)

    max_x = max((r['t_tps'] for r in rows if not r['saturated']), default=1.0)
    max_y = max((r['a_qps'] for r in rows if not r['saturated']), default=1.0)
    max_x = max(max_x, xt)
    max_y = max(max_y, xa)

    # ---- 1. AUC: shaded area under frontier ----
    if front:
        fx = [p[0] for p in front]
        fy = [p[1] for p in front]
        # Polygon: down to x-axis at endpoints, fill region under curve
        poly_x = [fx[0]] + fx + [fx[-1]]
        poly_y = [0.0] + fy + [0.0]
        ax.fill(poly_x, poly_y, color='lightgray', alpha=0.35,
                label='AUC', zorder=1, linewidth=0)

    # ---- 2. Bounding box (red dashed rectangle) ----
    if xt > 0 and xa > 0:
        rect = patches.Rectangle(
            (0, 0), xt, xa,
            linewidth=1.5, edgecolor='#d62728',
            facecolor='none', linestyle='--',
            label='Bounding box', zorder=2)
        ax.add_patch(rect)

    # ---- 3. Proportional line: (X^T, 0) -> (0, X^A) ----
    if xt > 0 and xa > 0:
        ax.plot([xt, 0], [0, xa],
                color='#1f77b4', linestyle='--', linewidth=1.5,
                label='Proportional line', zorder=3)

    # ---- 4. Fixed-T lines (orange dashed, circles) ----
    first = True
    for tau, pts in sorted(fT.items()):
        xs = [r['t_tps'] for r in pts]
        ys = [r['a_qps'] for r in pts]
        ax.plot(xs, ys,
                color='#ff7f0e', linestyle='--', linewidth=1.6,
                marker='o', markersize=7,
                markerfacecolor='white', markeredgecolor='#ff7f0e',
                markeredgewidth=1.5,
                label='Fixed-T lines' if first else None,
                zorder=4, alpha=0.9)
        first = False

    # ---- 5. Fixed-A lines (sky-blue dashed, diamonds) ----
    first = True
    for alpha, pts in sorted(fA.items()):
        xs = [r['t_tps'] for r in pts]
        ys = [r['a_qps'] for r in pts]
        ax.plot(xs, ys,
                color='#17becf', linestyle='--', linewidth=1.6,
                marker='D', markersize=6,
                markerfacecolor='white', markeredgecolor='#17becf',
                markeredgewidth=1.5,
                label='Fixed-A lines' if first else None,
                zorder=4, alpha=0.9)
        first = False

    # ---- 6. Frontier (green solid, thick) ----
    if front:
        fx = [p[0] for p in front]
        fy = [p[1] for p in front]
        ax.plot(fx, fy,
                color='#2ca02c', linewidth=3,
                label='Frontier', zorder=5,
                solid_capstyle='round')

    # ---- 7. Saturated cells (red x) ----
    sat = [(r['t_tps'], r['a_qps']) for r in rows if r['saturated']]
    if sat:
        ax.scatter([p[0] for p in sat], [p[1] for p in sat],
                   marker='x', s=90, color='#d62728', linewidths=2.5,
                   label='Saturated', zorder=6)

    # ---- Axis labels ----
    ax.set_xlabel(x_label or 'T-Throughput (tps)', fontsize=11)
    ax.set_ylabel(y_label or 'A-Throughput (qps)', fontsize=11)
    if title:
        ax.set_title(title, fontsize=12, pad=10)

    ax.set_xlim(0, max_x * 1.08)
    ax.set_ylim(0, max_y * 1.12)

    # k-formatting on x-axis if values are large
    def fmt_k(x, _):
        if x >= 1000:
            return f'{x/1000:g}K'
        return f'{x:g}'
    if max_x >= 5000:
        ax.xaxis.set_major_formatter(FuncFormatter(fmt_k))

    ax.grid(True, alpha=0.3, linestyle=':')
    ax.set_axisbelow(True)

    # Legend: place outside-right or upper-right depending on space
    ax.legend(loc='upper right', fontsize=9,
              framealpha=0.95, edgecolor='gray')

    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=200, bbox_inches='tight')
        print(f"Saved: {output}", file=sys.stderr)
    if show:
        plt.show()
    plt.close(fig)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Generate a HATtrick-style throughput frontier plot from a CSV.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Input CSV columns:
  tau         number of T-clients
  alpha       number of A-clients
  t_tps       transactional throughput (tps)
  a_qps       analytical throughput (qps)
  saturated   'true' if the cell crashed/saturated, else 'false'

Examples:
  python frontier_plot.py results.csv
  python frontier_plot.py results.csv -o frontier.pdf -t "vMODB Config 1, sleep=2000"
  python frontier_plot.py results.csv -o frontier.png --no-show
""")
    parser.add_argument('csv', help='Input CSV file')
    parser.add_argument('-o', '--output', default=None,
                        help='Save plot to this path (.png/.pdf/.svg)')
    parser.add_argument('-t', '--title', default=None, help='Plot title')
    parser.add_argument('--x-label', default=None, help='Custom x-axis label')
    parser.add_argument('--y-label', default=None, help='Custom y-axis label')
    parser.add_argument('--no-show', action='store_true',
                        help="Don't display interactively (useful with -o)")
    args = parser.parse_args()

    if not Path(args.csv).exists():
        print(f"Error: file not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    rows = read_csv(args.csv)
    if not rows:
        print(f"Error: no rows in {args.csv}", file=sys.stderr)
        sys.exit(1)

    plot(rows, output=args.output, title=args.title,
         x_label=args.x_label, y_label=args.y_label,
         show=not args.no_show)


if __name__ == '__main__':
    main()