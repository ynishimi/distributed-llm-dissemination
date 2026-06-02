import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

c = ["Estimation", "Performance"]

mid = "Medium"
large = "Large"

hf = "15.5"
hdd = "200"
ssd = "1000"
single = "Single"
full = "Full"
nums = [single, full]

cold_start_mid = pd.DataFrame(
    {
        "Category": [c[0]] + [c[i % 2] for i in range(6)],
        "Setup": [mid for _ in range(6+1)],
        "DiskSpeed": [hf, hf, hf, hdd, hdd, ssd, ssd],
        "NumClient": [single] + [full for i in range(6)],
        "TTD": [
            1.81 * 1024 * 36 / 15.50,
            1.81 * 1024 * 36 / 4 / 15.50, 1079.442,
            1.81 * 1024 * 36 / 4 / 200, 83.68417,
            1.81 * 1024 * 36 / 4 / 1000, 16.739456084
        ]
    }
)

cold_start_large = pd.DataFrame(
    {
        "Category": [c[0]] + [c[i % 2] for i in range(6)],
        "Setup": [large for _ in range(6+1)],
        "DiskSpeed": [hf, hf, hf, hdd, hdd, ssd, ssd],
        "NumClient": [single] + [full for i in range(6)],
        "TTD": [
            10.18 * 1024 * 61 / 15.50,
            10.18 * 1024 * 61 / 8 / 15.50, 5378.363,
            10.18 * 1024 * 61 / 8 / 200, 416.946,
            10.18 * 1024 * 61 / 8 / 1000, 83.393
        ]
    }
)

cold_start = pd.concat(
    [cold_start_mid, cold_start_large], axis=0)

print(cold_start)

cold_start_single_vs_multi = cold_start[cold_start["DiskSpeed"] == hf]

plt.grid(True)
g = sns.catplot(data=cold_start_single_vs_multi, kind="bar", x="NumClient",
                y="TTD", hue="Category", col="Setup", sharey=False)

g.set_axis_labels("# of clients", "TTD [s]")
plt.savefig(
    f"results/cold_start_single_vs_multi.pdf", bbox_inches='tight')

cold_start_multi = cold_start[cold_start["NumClient"] == full]

g2 = sns.catplot(data=cold_start_multi, kind="bar", x="DiskSpeed",
                 y="TTD", hue="Category", col="Setup", sharey=False)

g2.set_axis_labels("# of clients", "TTD [s]")
plt.savefig(
    f"results/cold_start_multi.pdf", bbox_inches='tight')

# Overhead
estimation = cold_start_multi[
    cold_start_multi["Category"] == "Estimation"
][["Setup", "DiskSpeed", "TTD"]].rename(columns={"TTD": "TTD_Estimation"})

overhead = cold_start_multi[
    cold_start_multi["Category"] != "Estimation"
].merge(estimation, on=["Setup", "DiskSpeed"])

overhead["Overhead [%]"] = (
    (overhead["TTD"] - overhead["TTD_Estimation"]) /
    overhead["TTD_Estimation"] * 100
)

overhead["DiskSpeed"] = overhead["DiskSpeed"].astype(float)
overhead = overhead.sort_values("DiskSpeed")
overhead["DiskSpeed"] = overhead["DiskSpeed"].astype(str)

print(overhead)
