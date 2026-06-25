import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

# Common figure size, shared with the evaluation figures (see
# backup/plot/essentials/config.py and distributor/plots/config.py).
figsize = (2.4 * 0.9 * 2, 2.4)


def plot_assignment(result: pd.DataFrame, disk_size: int, ram_size: int):
    '''
    Draw a heatmap of layer assignment (%) per node.
    Rows = nodes, columns = layers, values = percentage of each layer stored on the node.

    Parameters

    ----------
    result    : DataFrame for one scenario (one config name)
    disk_size : disk size (GiB) to select
    ram_size  : RAM size (GiB) to select
    '''
    config_name = result["name"].iloc[0]

    row = result[(result["disk_size"] == disk_size)
                 & (result["ram_size"] == ram_size)]
    if row.empty:
        return

    x_assignment = row["x_assignment"].iloc[0]
    x_ram_assignment = row["x_ram_assignment"].iloc[0]
    if x_assignment is None or x_ram_assignment is None:
        return

    x_val = np.array([list(row) for row in x_assignment],
                     dtype=float)      # shape: (n, l)
    x_ram_val = np.array([list(row) for row in x_ram_assignment], dtype=float)

    fig, axes = plt.subplots(2, 1, figsize=(figsize[0], figsize[1] * 1.5))

    sns.heatmap(x_val * 100, ax=axes[0], cmap="Blues", vmin=0, vmax=100,
                cbar_kws={"label": "Stored (%)"},
                xticklabels=10, yticklabels=True)
    axes[0].set_xlabel("Layer")
    axes[0].set_ylabel("Node")
    axes[0].set_title(
        f"Backup layer placement on Disk (disk={disk_size:02d}GiB, ram={ram_size:02d}GiB)")

    sns.heatmap(x_ram_val * 100, ax=axes[1], cmap="Oranges", vmin=0, vmax=100,
                cbar_kws={"label": "Stored (%)"},
                xticklabels=10, yticklabels=True)
    axes[1].set_xlabel("Layer")
    axes[1].set_ylabel("Node")
    axes[1].set_title(
        f"Backup layer placement on RAM (disk={disk_size:02d}GiB, ram={ram_size:02d}GiB)")

    plt.tight_layout()
    plt.savefig(
        f"results/{config_name}/images/heatmap_disk{disk_size:02d}GiB_ram{ram_size:02d}GiB.pdf", bbox_inches='tight')
    plt.close('all')


SCENARIO_LABELS = {
    "mid": "Medium",
    "large": "Large",
}

fps = [f"results/{name}/result.parquet" for name in ["mid", "large"]]
results = [pd.read_parquet(fp) for fp in fps]
# print(results)

results[0] = results[0][results[0]["disk_size"] < 26]
results[1] = results[1][results[1]["disk_size"] < 84]

for result in results:
    # Fig: heatmap of layer assignment per node
    # for disk_size in sorted(result["disk_size"].unique()):
    #     for ram_size in sorted(result["ram_size"].unique()):
    #         plot_assignment(result, disk_size, ram_size)

    result["ram_size"] = result["ram_size"].astype(str)
    scenario_name = result["name"].iloc[0]
    # Fig: disk backup effectiveness for fixed RAM size
    plt.figure(figsize=figsize)
    ax = sns.scatterplot(data=result, x="disk_size",
                         y="expected_time", hue="ram_size")
    ax.set_xlabel("Disk size (GiB)")
    ax.set_ylabel("Expected time (s)")
    ax.legend(title="RAM size (GiB)")
    plt.yscale('log')
    plt.grid(True)
    plt.savefig(
        f"results/{scenario_name}/plot_e_new.pdf", bbox_inches='tight')
    plt.clf()

    plt.figure(figsize=figsize)
    ax = sns.scatterplot(data=result, x="disk_size",
                         y="expected_time", hue="ram_size")
    ax.set_xlabel("Disk size (GiB)")
    ax.set_ylabel("Expected time (s)")
    ax.legend(title="RAM size (GiB)")
    plt.grid(True)
    plt.savefig(
        f"results/{scenario_name}/plot_e_new_linear.pdf", bbox_inches='tight')
    plt.clf()

    # Fig: downtime
    result_discrete = result[result["disk_size"] % 5 == 0]

    plt.figure(figsize=figsize)
    # result_discrete = result_discrete[result_discrete["ram_size"] == 0]
    ax = sns.boxplot(
        data=result_discrete.explode("downtime"),
        x="disk_size", y="downtime", hue="ram_size", whis=(0, 100)
    )
    sns.stripplot(data=result_discrete.explode("downtime"),
                  x="disk_size", y="downtime", hue="ram_size", dodge=True, jitter=True, legend=False)

    ax.set_xlabel("Disk size (GiB)")
    ax.set_ylabel("Downtime (s)")
    ax.legend(title="RAM size (GiB)")

    plt.yscale('log')
    plt.grid(True)
    plt.savefig(
        f"results/{scenario_name}/plot_d_new.pdf", bbox_inches='tight')
    plt.clf()


# Fig: computation time
plt.figure(figsize=figsize)

merged_results = pd.concat([results[0], results[1]], ignore_index=True)
merged_results["scenario"] = merged_results["name"].map(
    SCENARIO_LABELS).fillna(merged_results["name"])

plt.yscale('log')
plt.grid(True)

ax = sns.boxplot(data=merged_results, x="scenario",
                 y="solve_time", whis=(0, 100)
                 )
ax.set_xlabel("Scenario")
ax.set_ylabel("Solve time (s)")
# sns.stripplot(data=merged_results.explode("downtime"),
#               x="name",
#               y="solve_time", dodge=True, jitter=True, legend=False)
plt.savefig(
    f"results/plot_computation_time_new.pdf", bbox_inches='tight')
plt.clf()
