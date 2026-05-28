import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

c = ["Estimation", "Performance", "Performance (Adaptive)"]

mid = "Medium"
large = "Large"

hf = "15.5"
hdd = "200"
ssd = "1000"

num_result = 9

crash_recovery_mid = pd.DataFrame(
    {
        "Category": [c[i % 3] for i in range(num_result)],
        "Setup": [mid for _ in range(num_result)],
        "DiskSpeed": [hf, hf, hf, hdd, hdd, hdd, ssd, ssd, ssd],
        "TTD": [360, 364.661, 368.529,
                28, 28.091, 30.067,
                28,
                28.007106466,
                30.086767009]
    }
)

crash_recovery_large = pd.DataFrame(
    {
        "Category": [c[i % 3] for i in range(num_result)],
        "Setup": [large for _ in range(num_result)],
        "DiskSpeed": [hf, hf, hf, hdd, hdd, hdd, ssd, ssd, ssd],
        "TTD": [10.18 * 8 * 1024 / 7 / 15.5,
                769.08,
                803.553,
                10.18 * 8 * 1024 / 7 / 200,
                60.006,
                71.079,
                56,
                56.001,
                56.035]
    }
)

crash_recovery_adaptive = pd.concat(
    [crash_recovery_mid, crash_recovery_large], axis=0)

print(crash_recovery_adaptive)

plt.figure(figsize=(24, 8))
plt.grid(True)
g = sns.catplot(data=crash_recovery_adaptive, kind="bar", x="DiskSpeed",
                y="TTD", hue="Category", col="Setup")

g.set_axis_labels("Disk Speed [MiB/s]", "TTD [s]")

plt.savefig(
    f"results/crash_recovery_adaptive.png", bbox_inches='tight')
