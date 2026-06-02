import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

SCENARIO_LABELS = {
    "mid": "Medium",
    "large": "Large",
}

fps = [f"results/{name}/result_new.parquet" for name in ["mid", "large"]]
results = [pd.read_parquet(fp) for fp in fps]
# print(results)

results[0] = results[0][results[0]["disk_size"] < 26]
results[1] = results[1][results[1]["disk_size"] < 84]

for result in results:
    result["ram_size"] = result["ram_size"].astype(str)
    scenario_name = result["name"].iloc[0]
    # Fig: disk backup effectiveness for fixed RAM size
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

    plt.figure(figsize=(6.4*3, 4.8))
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
plt.figure(figsize=(6.4, 4.8))

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
