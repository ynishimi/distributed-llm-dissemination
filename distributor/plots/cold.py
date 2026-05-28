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
ssd = "1024"
single = "Single"
full = "Full"

cold_start_mid = pd.DataFrame(
    {
        "Category": [c[i % 2] for i in range(4)],
        "Setup": [mid for _ in range(4)],
        "NumClient": [single, single, full, full],
        "TTD": [1.81 * 1024 * 36 / 15.50, 0, 1.81 * 1024 * 36 / 4 / 15.50, 119.971 * 36 / 4]
    }
)

cold_start_large = pd.DataFrame(
    {
        "Category": [c[i % 2] for i in range(4)],
        "Setup": [large for _ in range(4)],
        "NumClient": [single, single, full, full],
        "TTD": [10.18 * 1024 * 61 / 15.50, 0, 10.18 * 1024 * 61 / 8 / 15.50, 672.337 * 61 / 8]
    }
)

cold_start = pd.concat(
    [cold_start_mid, cold_start_large], axis=0)

print(cold_start)

plt.figure(figsize=(24, 8))
plt.grid(True)
g = sns.catplot(data=cold_start, kind="bar", x="NumClient",
                y="TTD", hue="Category", col="Setup")

g.set_axis_labels("# of clients", "TTD [s]")

plt.savefig(
    f"results/cold_start.png", bbox_inches='tight')
