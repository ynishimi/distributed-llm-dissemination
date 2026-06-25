"""Plots for the non-uniform crash-rate experiment.

Compares the per-node downtime of the fragile node (node 0) between the uniform
crash-rate case (result.parquet) and the non-uniform case (result_nonuniform.parquet,
fragile node weighted RHO times more). The expected value is NOT compared across the
two cases because it is reweighted by RHO and therefore not commensurable; instead the
directly comparable per-node downtime (in seconds) is shown.

All outputs are written with a _nonuniform suffix so that the existing figures
produced by plot.py are not overwritten.
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

# Common figure size, shared with the evaluation figures (see
# backup/plot/essentials/config.py and distributor/plots/config.py).
figsize = (2.4 * 0.9 * 2, 2.4)

HERE = os.path.dirname(os.path.abspath(__file__))

SCENARIOS = ["mid", "large"]
SCENARIO_LABELS = {"mid": "Medium", "large": "Large"}
# disk range to display per scenario (matches plot.py cropping)
DISK_MAX = {"mid": 26, "large": 84}
RAM_FOCUS = 0  # the constrained, disk-only regime where prioritisation is clearest


def node_downtime(df, node):
    """Return a DataFrame column of the downtime of a given node index."""
    return df["downtime"].apply(lambda v: float(np.asarray(v, dtype=float)[node]))


def load(scenario):
    uni = pd.read_parquet(f"{HERE}/results/{scenario}/result.parquet")
    non = pd.read_parquet(
        f"{HERE}/results/{scenario}/result_nonuniform.parquet")
    uni = uni[(uni["ram_size"] == RAM_FOCUS) & (
        uni["disk_size"] < DISK_MAX[scenario])].copy()
    non = non[(non["ram_size"] == RAM_FOCUS) & (
        non["disk_size"] < DISK_MAX[scenario])].copy()
    uni = uni.sort_values("disk_size")
    non = non.sort_values("disk_size")
    return uni, non


def main():
    for scenario in SCENARIOS:
        uni, non = load(scenario)
        n = int(uni["n"].iloc[0])
        rho = float(non["rho"].iloc[0])

        # node 0 downtime, and the mean over the remaining nodes
        d0_uni = np.asarray(node_downtime(uni, 0), dtype=float)
        d0_non = np.asarray(node_downtime(non, 0), dtype=float)
        rest_uni = np.mean(
            np.array([np.asarray(node_downtime(uni, k), dtype=float) for k in range(1, n)]), axis=0)
        rest_non = np.mean(
            np.array([np.asarray(node_downtime(non, k), dtype=float) for k in range(1, n)]), axis=0)
        disk = np.asarray(uni["disk_size"], dtype=float)

        # Keep the same width and plotting-area height as the other figures,
        # adding extra height below for the legend so the drawing area is not
        # compressed (the 1.36 factor makes the axes height match a plain
        # figsize plot).
        fig, ax = plt.subplots(figsize=(figsize[0], figsize[1] * 1.36))
        # fragile node 0
        ax.plot(disk, d0_uni, color="tab:blue", linestyle="--", marker="o", markersize=4,
                label="node 0, uniform")
        ax.plot(disk, d0_non, color="tab:blue", linestyle="-", marker="o", markersize=4,
                label=f"node 0, non-uniform")
        # the other nodes (trade-off)
        ax.plot(disk, rest_uni, color="tab:orange", linestyle="--", marker="s", markersize=3,
                label="other nodes (mean), uniform")
        ax.plot(disk, rest_non, color="tab:orange", linestyle="-", marker="s", markersize=3,
                label="other nodes (mean), non-uniform")

        ax.set_xlabel("Disk size (GiB)")
        ax.set_ylabel("Downtime (s)")
        ax.set_yscale("log")
        ax.set_title(
            f"{SCENARIO_LABELS[scenario]} setup (RAM = {RAM_FOCUS} GiB)")
        # Place the legend below the axes so the figure width is unchanged.
        ax.legend(fontsize=8, loc="upper center",
                  bbox_to_anchor=(0.5, -0.42), ncol=2)
        ax.grid(True, which="both", alpha=0.3)
        fig.tight_layout()
        out = f"{HERE}/results/{scenario}/plot_node0_downtime_nonuniform.pdf"
        fig.savefig(out)
        plt.close(fig)
        print("saved", out)

        # brief numeric summary of the prioritisation effect at constrained budgets
        for d in [4, 8, 12]:
            if d in disk:
                i = list(disk).index(d)
                print(f"  [{scenario}] disk={d:2d}GiB: node0 downtime "
                      f"{d0_uni[i]:8.1f}s (uniform) -> {d0_non[i]:8.1f}s (non-uniform); "
                      f"others mean {rest_uni[i]:8.1f} -> {rest_non[i]:8.1f}")


def total_downtime(df):
    """Sum of per-node downtime for each row (the unweighted total)."""
    return df["downtime"].apply(lambda v: float(np.sum(np.asarray(v, dtype=float)))).values


def plot_total_overlap():
    """Show that the total (unweighted) downtime is identical for the uniform and
    non-uniform weightings: the weighting only redistributes protection, it does
    not change the achievable total."""
    for scenario in SCENARIOS:
        uni, non = load(scenario)
        rho = float(non["rho"].iloc[0])
        disk = np.asarray(uni["disk_size"], dtype=float)
        tot_uni = np.asarray(total_downtime(uni), dtype=float)
        tot_non = np.asarray(total_downtime(non), dtype=float)

        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(disk, tot_uni, linestyle="--", marker="o",
                markersize=5, label="uniform")
        ax.plot(disk, tot_non, linestyle="-", marker="x",
                markersize=5, label=f"non-uniform")
        ax.set_xlabel("Disk size (GiB)")
        ax.set_ylabel("Total downtime over all crash scenarios (s)")
        ax.set_title(
            f"{SCENARIO_LABELS[scenario]} setup (RAM = {RAM_FOCUS} GiB)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        out = f"{HERE}/results/{scenario}/plot_total_downtime_nonuniform.pdf"
        fig.savefig(out)
        plt.close(fig)
        max_gap = float(np.max(np.abs(tot_uni - tot_non)))
        print(f"saved {out}  (max |uniform - non-uniform| = {max_gap:.4g} s)")


def plot_heatmap_nonuniform(scenario, disk_size, ram_size):
    """Heatmap of backup layer placement for the non-uniform case (same style as
    plot.py's plot_assignment). Saved with a _nonuniform suffix so the existing
    uniform heatmaps are not overwritten."""
    df = pd.read_parquet(
        f"{HERE}/results/{scenario}/result_nonuniform.parquet")
    fragile = int(df["fragile_node"].iloc[0])
    row = df[(df["disk_size"] == disk_size) & (df["ram_size"] == ram_size)]
    if row.empty:
        print(f"  no row for {scenario} disk={disk_size} ram={ram_size}")
        return
    x_assignment = row["x_assignment"].iloc[0]
    x_ram_assignment = row["x_ram_assignment"].iloc[0]
    if x_assignment is None or x_ram_assignment is None:
        return
    x_val = np.array([list(r) for r in x_assignment], dtype=float)
    x_ram_val = np.array([list(r) for r in x_ram_assignment], dtype=float)

    fig, axes = plt.subplots(2, 1, figsize=(figsize[0], figsize[1] * 2))
    sns.heatmap(x_val * 100, ax=axes[0], cmap="Blues", vmin=0, vmax=100,
                cbar_kws={"label": "Stored (%)"}, xticklabels=10, yticklabels=True)
    axes[0].set_xlabel("Layer")
    axes[0].set_ylabel("Node")
    axes[0].set_title(
        f"Backup on Disk (non-uniform, fragile node {fragile}, "
        f"disk={disk_size:02d}GiB, ram={ram_size:02d}GiB)")
    sns.heatmap(x_ram_val * 100, ax=axes[1], cmap="Oranges", vmin=0, vmax=100,
                cbar_kws={"label": "Stored (%)"}, xticklabels=10, yticklabels=True)
    axes[1].set_xlabel("Layer")
    axes[1].set_ylabel("Node")
    axes[1].set_title(
        f"Backup on RAM (non-uniform, fragile node {fragile}, "
        f"disk={disk_size:02d}GiB, ram={ram_size:02d}GiB)")
    plt.tight_layout()
    out = (f"{HERE}/results/{scenario}/images/"
           f"heatmap_disk{disk_size:02d}GiB_ram{ram_size:02d}GiB_nonuniform.pdf")
    plt.savefig(out, bbox_inches="tight")
    plt.close("all")
    print("saved", out)


# (disk, ram) cases to render, chosen in the constrained regime where the
# prioritisation of the fragile node's recovery layers is visible.
HEATMAP_CASES = {
    "mid": [(5, 0), (16, 8)],
    "large": [(12, 0), (40, 0)],
}


if __name__ == "__main__":
    main()
    plot_total_overlap()
    for sc, cases in HEATMAP_CASES.items():
        for d, r in cases:
            plot_heatmap_nonuniform(sc, d, r)
