from typing import Any
import seaborn as sns

sns.set_theme()
sns.set_context("paper")

catplot: dict[str, Any] = dict(height=2.4, aspect=0.9)
figsize = (2.4 * 0.9 * 2, 2.4)
