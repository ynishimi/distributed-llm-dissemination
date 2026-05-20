import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# sns.set_theme()
# sns.set_context("paper")

fps = [f"results/{name}/result.csv" for name in ["mid", "large"]]
results = [pd.read_csv(fp) for fp in fps]

results[0] = results[0][results[0]["disk_size"] < 30]
results[1] = results[1][results[1]["disk_size"] < 100]

for result in results:
    ram_sizes = result["ram_size"].unique()
    for ram_size in ram_sizes:
        result_by_ram = result[result["ram_size"] == ram_size]
        result_by_ram.plot.scatter(x="disk_size", y="expected_time")
        # plt.xscale('log')
        # plt.yscale('log')
        plt.grid(True)
        plt.savefig(
            f"results/{result_by_ram["name"].iloc[0]}/plot_ram{ram_size}GiB.png", bbox_inches='tight')
        # plt.show()
