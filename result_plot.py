import re
import csv
import os
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# Input files
index = "4"
log_file = f"/tmp/{index}.txt"
config_file = "config/EX_topics.csv"
output_dir = f"/tmp/{index}"

os.makedirs(output_dir, exist_ok=True)

plot_config = {
    "Critical": {
        "xlim": 1000, "xtick": 100,
        "ylim": 0.10, "ytick": 0.01
    },
    "Time-Critical-Small": {
        "xlim": 3000, "xtick": 300,
        "ylim": 0.01, "ytick": 0.001
    },
    "Time-Critical-Large": {
        "xlim": 100000, "xtick": 10000,
        "ylim": 0.001, "ytick": 0.0001
    },
    "Less-Critical": {
        "xlim": 5000, "xtick": 500,
        "ylim": 0.01, "ytick": 0.001
    },
    "Uncategorized": {
        "xlim": 5000, "xtick": 500,
        "ylim": 0.01, "ytick": 0.001
    }
}

topic_to_criticality = {}
with open(config_file) as f:
    reader = csv.reader(f)
    for row in reader:
        topic, criticality = row[0].strip(), row[1].strip()
        topic_to_criticality[topic] = criticality

stats_by_criticality = defaultdict(lambda: defaultdict(list))
pattern = re.compile(r'\[(?P<topic>.+?)\]\s+latency:\s+(?P<latency>\d+)\s+µs')

with open(log_file) as f:
    for line in f:
        match = pattern.match(line.strip())
        if match:
            topic = match.group("topic").strip()
            latency = int(match.group("latency"))
            crit = topic_to_criticality.get(topic, "Uncategorized")
            stats_by_criticality[crit][topic].append(latency)

for crit, topic_latencies in stats_by_criticality.items():
    plt.figure(figsize=(12, 6))
    for topic, latencies in topic_latencies.items():
        if len(latencies) < 2:
            continue
        hist, bin_edges = np.histogram(latencies, bins=100, density=True)
        bin_centers = 0.5 * (bin_edges[1:] + bin_edges[:-1])
        plt.plot(bin_centers, hist, label=topic)

    conf = plot_config.get(crit, plot_config["Uncategorized"])

    plt.xlim(0, conf["xlim"])
    plt.xticks(np.arange(0, conf["xlim"] + 1, conf["xtick"]))

    plt.ylim(0, conf["ylim"])
    plt.yticks(np.arange(0, conf["ylim"] + conf["ytick"], conf["ytick"]))

    plt.xlabel("Latency (µs)")
    plt.ylabel("Density")
    plt.title(f"Latency Distribution (Line Plot) for {crit}")
    # plt.legend(fontsize="xx-small", loc='upper right', ncol=2)
    plt.tight_layout()
    plt.grid(True)

    filename = os.path.join(output_dir, f"latency_{crit.replace(' ', '_')}.png")
    plt.savefig(filename)
    print(f"Saved: {filename}")
    plt.close()

