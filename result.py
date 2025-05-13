import re
import csv
from collections import defaultdict

log_file = "/tmp/1.txt"
output_file = "/tmp/1.csv"

stats = defaultdict(list)

pattern = re.compile(r'\[(?P<topic>.+?)\]\s+latency:\s+(?P<latency>\d+)\s+µs')

with open(log_file) as f:
    for line in f:
        match = pattern.match(line.strip())
        if match:
            topic = match.group("topic").strip()
            latency = int(match.group("latency"))
            stats[topic].append(latency)

with open(output_file, mode='w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Topic", "Avg Latency (µs)", "Min Latency", "Max Latency", "Count"])

    for topic, latencies in sorted(stats.items()):
        writer.writerow([
            topic,
            sum(latencies) // len(latencies),
            min(latencies),
            max(latencies),
            len(latencies)
        ])

