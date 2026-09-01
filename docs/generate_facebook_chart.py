#!/usr/bin/env python3
"""Generate the Facebook metrics before/after chart for the incident report."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# -- Post Insights Data (weekly averages from BigQuery, Oct 2025 - Mar 2026) --
post_data = [
    ("2025-09-29", 146, 5992.8, 3890.2, 368.8, 5293, 483),
    ("2025-10-06", 253, 6263.3, 4001.6, 432.6, 7847, 937),
    ("2025-10-13", 246, 7025.7, 4521.8, 397.6, 8761, 1157),
    ("2025-10-20", 320, 7022.6, 4513.4, 424.9, 11248, 1624),
    ("2025-10-27", 310, 7739.3, 4969.7, 429.6, 11701, 1162),
    ("2025-11-03", 290, 6966.9, 4402.1, 406.8, 11160, 1449),
    ("2025-11-10", 302, 6905.7, 4234.4, 486.3, 10482, 1093),
    ("2025-11-17", 304, 7447.2, 4828.3, 412.1, 11846, 1171),
    ("2025-11-24", 297, 6345.4, 4009.1, 326.0, 9171, 1067),
    ("2025-12-01", 320, 4996.5, 3244.0, 255.8, 9291, 1015),
    ("2025-12-08", 318, 5558.6, 3673.1, 262.0, 10148, 1132),
    ("2025-12-15", 372, 5112.4, 3360.3, 249.6, 11286, 1039),
    ("2025-12-22", 341, 4159.5, 2808.6, 207.4, 7044, 496),
    ("2025-12-29", 283, 4092.9, 2735.2, 208.8, 5766, 391),
    ("2026-01-05", 378, 3980.4, 2635.5, 198.2, 8988, 650),
    ("2026-01-12", 367, 5034.0, 3305.1, 249.6, 11344, 1611),
    ("2026-01-19", 344, 4773.9, 3157.4, 240.4, 9272, 874),
    ("2026-01-26", 349, 4268.9, 2794.2, 196.9, 9235, 983),
    ("2026-02-02", 341, 5610.2, 3631.5, 317.5, 11299, 1452),
    ("2026-02-09", 337, 4703.2, 3144.1, 255.4, 9220, 989),
    ("2026-02-16", 376, 4121.1, 2757.7, 204.0, 8899, 915),
    ("2026-02-23", 505, 3557.5, 2403.7, 174.3, 9438, 1092),
    ("2026-03-02", 208, 2748.7, 1861.1, 146.8, 3387, 475),
]

# -- Page Insights Data (weekly averages) --
page_data = [
    ("2025-09-29", 0.0, 3925.2, 448.8, 0.0, 4.5),
    ("2025-10-06", 0.0, 5698.0, 651.5, 0.0, 5.9),
    ("2025-10-13", 0.0, 6010.8, 633.5, 0.0, 5.3),
    ("2025-10-20", 0.0, 5216.2, 641.6, 0.0, 5.8),
    ("2025-10-27", 1384.7, 2931.5, 325.7, 590.1, 3.6),
    ("2025-11-03", 8664.7, 5396.1, 647.5, 4148.3, 6.2),
    ("2025-11-10", 9241.9, 5772.8, 734.2, 4181.9, 6.2),
    ("2025-11-17", 8917.0, 5431.6, 614.7, 4219.0, 7.8),
    ("2025-11-24", 7014.2, 3827.2, 437.9, 4264.9, 6.8),
    ("2025-12-01", 7351.6, 4459.9, 513.6, 4287.8, 2.8),
    ("2025-12-08", 8187.0, 5193.6, 586.2, 4312.0, 7.9),
    ("2025-12-15", 7283.4, 4533.0, 519.1, 4390.7, 13.1),
    ("2025-12-22", 6217.6, 3614.9, 441.9, 4459.0, 8.1),
    ("2025-12-29", 3418.6, 1627.8, 201.6, 4466.0, 2.2),
    ("2026-01-05", 4766.2, 2645.0, 324.0, 4476.8, 3.4),
    ("2026-01-12", 7119.5, 4103.6, 521.6, 4529.0, 17.9),
    ("2026-01-19", 6965.5, 4241.6, 523.3, 4653.8, 17.7),
    ("2026-01-26", 5674.8, 3236.2, 409.1, 4739.8, 10.8),
    ("2026-02-02", 6354.4, 3650.0, 462.3, 4760.3, 4.1),
    ("2026-02-09", 6056.5, 3570.8, 442.2, 4783.6, 7.8),
    ("2026-02-16", 6838.6, 4195.7, 518.6, 4840.8, 9.3),
    ("2026-02-23", 6014.6, 3505.3, 436.0, 4869.8, 4.1),
    ("2026-03-02", 5425.8, 3104.2, 371.0, 4888.2, 4.0),
]

dates_post = [datetime.strptime(d[0], "%Y-%m-%d") for d in post_data]
dates_page = [datetime.strptime(d[0], "%Y-%m-%d") for d in page_data]

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle(
    "Facebook Metrics - Before & After November 2025 API Changes",
    fontsize=16, fontweight="bold", y=0.98,
)

c_views = "#1877F2"
c_reach = "#42B72A"
c_clicks = "#F5533D"
c_follows = "#8B5CF6"
cutoff = datetime(2025, 11, 1)


def add_cutoff(ax):
    ax.axvline(x=cutoff, color="red", linestyle="--", alpha=0.7, linewidth=1.5)
    ymin, ymax = ax.get_ylim()
    ax.text(
        cutoff, ymax * 0.95, "  Facebook API\n  Changes",
        color="red", fontsize=8, va="top", fontweight="bold",
    )


# --- Chart 1: Post Views & Reach ---
ax = axes[0][0]
ax.plot(dates_post, [d[2] for d in post_data], color=c_views, linewidth=2,
        marker="o", markersize=4, label="Avg Post Views (post_media_view)")
ax.plot(dates_post, [d[3] for d in post_data], color=c_reach, linewidth=2,
        marker="s", markersize=4, label="Avg Post Reach (unique)")
ax.set_title("Post Views & Reach (Weekly Avg)", fontweight="bold")
ax.set_ylabel("People")
ax.legend(fontsize=8, loc="upper right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax.tick_params(axis="x", rotation=45)
add_cutoff(ax)

# --- Chart 2: Post Clicks & Likes ---
ax = axes[0][1]
ax.plot(dates_post, [d[4] for d in post_data], color=c_clicks, linewidth=2,
        marker="o", markersize=4, label="Avg Post Clicks (all types)")
ax2 = ax.twinx()
ax2.bar(dates_post, [d[5] for d in post_data], width=5, alpha=0.3,
        color=c_views, label="Total Likes")
ax.set_title("Post Clicks & Likes (Weekly)", fontweight="bold")
ax.set_ylabel("Avg Clicks per Post", color=c_clicks)
ax2.set_ylabel("Total Likes (all posts)", color=c_views)
ax.legend(fontsize=8, loc="upper left")
ax2.legend(fontsize=8, loc="upper right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax.tick_params(axis="x", rotation=45)
add_cutoff(ax)

# --- Chart 3: Page Views (shows zero-to-populated transition) ---
ax = axes[1][0]
ax.fill_between(dates_page, [d[1] for d in page_data], alpha=0.3, color=c_views)
ax.plot(dates_page, [d[1] for d in page_data], color=c_views, linewidth=2,
        marker="o", markersize=4, label="Avg Page Views (page_media_view)")
ax.plot(dates_page, [d[2] for d in page_data], color=c_reach, linewidth=2,
        marker="s", markersize=4, label="Avg Page Reach (unique)")
ax.annotate(
    "Old metric (page_impressions)\nwas REMOVED - shows as zero",
    xy=(datetime(2025, 10, 13), 0),
    xytext=(datetime(2025, 10, 1), 5000),
    fontsize=8, color="red", fontweight="bold",
    arrowprops=dict(arrowstyle="->", color="red", lw=1.5),
)
ax.annotate(
    "New metric (page_media_view)\npopulated from Nov 2025",
    xy=(datetime(2025, 11, 10), 9241),
    xytext=(datetime(2025, 12, 15), 9500),
    fontsize=8, color=c_views, fontweight="bold",
    arrowprops=dict(arrowstyle="->", color=c_views, lw=1.5),
)
ax.set_title("Page Views - Metric Transition (Key Chart)", fontweight="bold")
ax.set_ylabel("Daily Avg")
ax.legend(fontsize=8, loc="center right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax.tick_params(axis="x", rotation=45)
add_cutoff(ax)

# --- Chart 4: Page Followers Growth ---
ax = axes[1][1]
ax.plot(dates_page, [d[3] for d in page_data], color=c_follows, linewidth=2.5,
        marker="o", markersize=4, label="Follower Count (page_follows)")
ax2 = ax.twinx()
ax2.bar(dates_page, [d[4] for d in page_data], width=5, alpha=0.3,
        color=c_follows, label="Daily New Follows")
ax.annotate(
    "Old metric (page_fans)\nwas REMOVED - shows as zero",
    xy=(datetime(2025, 10, 13), 0),
    xytext=(datetime(2025, 10, 1), 3000),
    fontsize=8, color="red", fontweight="bold",
    arrowprops=dict(arrowstyle="->", color="red", lw=1.5),
)
ax.set_title("Page Followers Growth", fontweight="bold")
ax.set_ylabel("Total Followers", color=c_follows)
ax2.set_ylabel("Daily New Follows", color=c_follows)
ax.legend(fontsize=8, loc="upper left")
ax2.legend(fontsize=8, loc="center right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
ax.tick_params(axis="x", rotation=45)
add_cutoff(ax)

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig("c:/orchestrator/docs/facebook_metrics_chart.png", dpi=150, bbox_inches="tight")
print("Chart saved to docs/facebook_metrics_chart.png")
