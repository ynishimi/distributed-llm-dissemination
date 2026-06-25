import json
import re

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import figsize

c = ["Limited bandwidth", "Limited bandwidth for first 10s"]

mid = "Medium"
large = "Large"

hf = "15.5"
hdd = "200"
ssd = "1000"

crash_recovery_tc = pd.DataFrame(
    {
        "Category": [c[i] for i in range(2)],
        "Setup": [mid for _ in range(2)],
        "DiskSpeed": [hdd, hdd],
        "TTD": [49.196369652,
                48.85860896]
    }
)

print(crash_recovery_tc.to_latex(index=False,
                                 formatters={"name": str.upper},
                                 float_format="{:.3f}".format,
                                 ))


# --- Reported sending rate of the throttled node (n1) over time ---
# The coordinator re-estimates each edge from the rate at which the
# assigned job is transferred. Once n1 is judged slow, it is given only a
# small job, so its reported rate stays pinned at the throttled level even
# after the bandwidth limit is lifted.

LOG_DIR = "../logs"
RUNS = {
    "Without limit": "mid_crash_200_adaptive",
    "Throttled throughout": "mid_crash_200_adaptive_slow",
    "Throttled for first 10s": "mid_crash_200_adaptive_slow_10s",
}
THROTTLED_NODE = 1
RESTORE_TIME = 10.0  # [s] when the limit is lifted in the 10s condition
CLRATE_RE = re.compile(r"clRate:map\[%d:(\d+)\]" % THROTTLED_NODE)


def load_reported_rate(run_dir, label):
    """Return a DataFrame of n1's reported sending rate [Mbps] over the
    dissemination-relative time (origin = first job assignment)."""
    path = f"{LOG_DIR}/{run_dir}/merged.jsonl"
    records = []
    t_assign = None
    for line in open(path):
        try:
            o = json.loads(line)
        except json.JSONDecodeError:
            continue
        if t_assign is None and "destJobs" in o:
            t_assign = o["time"]
        if o.get("node") == THROTTLED_NODE:
            m = CLRATE_RE.search(o.get("msg", ""))
            if m:
                records.append((o["time"], int(m.group(1))))
    rows = [
        {"Time": (t - t_assign) / 1000.0, "Rate": r *
         8 / 1e6, "Condition": label}
        for t, r in records
    ]
    return pd.DataFrame(rows)


plt.figure(figsize=figsize)
rate = pd.concat([load_reported_rate(d, label) for label, d in RUNS.items()],
                 ignore_index=True)
ax = sns.lineplot(data=rate, x="Time", y="Rate",
                  hue="Condition", style="Condition", markers=True)
ax.axvline(RESTORE_TIME, color="gray", linestyle="--", linewidth=1)
ax.set(xlabel="Time [s]", ylabel="Reported sending rate of n1 [Mbps]",
       yscale="log")
ax.set_ylim(bottom=10**2/2, top=10**4/2)

plt.savefig("results/crash_recovery_tc_rate.pdf", bbox_inches="tight")
