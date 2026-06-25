import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import seaborn as sns  # noqa: E402
from config import figsize  # noqa: E402

HERE = os.path.join(os.path.dirname(__file__), '../..')

SCENARIO_LABELS = {
    "mid": "Medium",
    "large": "Large",
}

RAM_KEEP = {"mid": [0, 4, 16], "large": [0, 8, 32]}

# disk_size selection for each panel, controlled independently per scenario.
# DISK_TOP keeps the upper panel sparse for readability, while DISK_BOTTOM
# samples the stable region more densely so the lower panel is not too empty.
DISK_TOP = {
    "mid": [i * 2 for i in range(11)], "large": [i * 8 for i in range(11)]}

# Disk size in the steady state, used for the RAM comparison in the lower panel.
STEADY_DISK = {"mid": 20, "large": 80}

fps = [f"{HERE}/results/{name}/result.parquet" for name in ["mid", "large"]]
results = [pd.read_parquet(fp) for fp in fps]

results[0] = results[0][
    (results[0]["ram_size"].isin(RAM_KEEP["mid"]))
]
results[1] = results[1][
    (results[1]["ram_size"].isin(RAM_KEEP["large"]))
]

for result in results:
    result["ram_size"] = result["ram_size"].astype(str)
    scenario_name = result["name"].iloc[0]
    top = result[result["disk_size"].isin(DISK_TOP[scenario_name])]

    fig, ax = plt.subplots(figsize=figsize)

    sns.lineplot(data=top, x="disk_size", y="expected_time",
                 hue="ram_size", style="ram_size", ax=ax, markers=True)
    ax.set_xlabel("Disk size (GiB)")
    ax.set_ylabel("Expected time (s)")
    ax.legend(title="RAM size (GiB)")
    # Place the x-axis gridlines at round intervals of 10 GiB.
    ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(
        f"{HERE}/results/{scenario_name}/plot_e_new_linear_e.pdf", bbox_inches="tight")
    plt.close(fig)

    result_discrete = top

    plt.figure(figsize=figsize)
    ax = sns.boxplot(
        data=result_discrete.explode("downtime"),
        x="disk_size", y="downtime", hue="ram_size", whis=(0, 100)
    )
    sns.stripplot(data=result_discrete.explode("downtime"),
                  x="disk_size", y="downtime", hue="ram_size",
                  dodge=True, jitter=True, legend=False)
    ax.set_xlabel("Disk size (GiB)")
    ax.set_ylabel("Downtime (s)")
    ax.legend(title="RAM size (GiB)")
    plt.yscale("log")
    plt.grid(True)
    plt.savefig(f"{HERE}/results/{scenario_name}/plot_d_new_e.pdf",
                bbox_inches="tight")
    plt.clf()
