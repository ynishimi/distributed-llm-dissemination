import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

fps = [f"results/{name}/result.parquet" for name in ["mid", "large"]]
results = [pd.read_parquet(fp) for fp in fps]
# print(results)

results[0] = results[0][results[0]["disk_size"] < 30]
results[1] = results[1][results[1]["disk_size"] < 100]

for result in results:
    result["ram_size"] = result["ram_size"].astype(str)
    # Fig: disk backup effectiveness for fixed RAM size
    sns.scatterplot(data=result, x="disk_size",
                    y="expected_time", hue="ram_size")
    plt.yscale('log')
    plt.grid(True)
    plt.savefig(
        f"results/{result["name"].iloc[0]}/plot_e.png", bbox_inches='tight')
    plt.clf()

    # Fig: downtime
    result_discrete = result[result["disk_size"] % 5 == 0]

    # result_discrete = result_discrete[result_discrete["ram_size"] == 0]
    plt.figure(figsize=(24, 8))
    sns.boxplot(
        data=result_discrete.explode("downtime"),
        x="disk_size", y="downtime", hue="ram_size", whis=(0, 100)
    )
    sns.stripplot(data=result_discrete.explode("downtime"),
                  x="disk_size", y="downtime", hue="ram_size", dodge=True, jitter=True, legend=False)

    plt.yscale('log')
    plt.grid(True)
    plt.savefig(
        f"results/{result_discrete["name"].iloc[0]}/plot_d.png", bbox_inches='tight')
    plt.clf()

    # TODO: Fig: model switching time


# Fig: computation time

merged_results = pd.concat([results[0], results[1]], ignore_index=True)

plt.figure(figsize=(8, 8))
plt.yscale('log')
plt.grid(True)

sns.boxplot(data=merged_results, x="name",
            y="solve_time", hue="name", whis=(0, 100)
            )
# sns.stripplot(data=merged_results.explode("downtime"),
#               x="name",
#               y="solve_time", dodge=True, jitter=True, legend=False)
plt.savefig(
    f"results/plot_computation_time.png", bbox_inches='tight')
plt.clf()
