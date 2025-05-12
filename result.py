import re
import csv
from collections import defaultdict

log_file = "experiment/2tcclass_0.98_0.02/can_not_use_big/raw.txt"
output_file = "experiment/2tcclass_0.98_0.02/can_not_use_big/raw.csv"

stats = defaultdict(lambda: {
    "latencies": [],
    "received_seq": set(),
})

pattern = re.compile(r'(?P<topic>.+?)\s*\|\s*seq\s*=\s*(?P<seq>\d+)\s*\|\s*latency\s*=\s*(?P<latency>\d+)\s*µs')

with open(log_file) as f:
    for line in f:
        match = pattern.match(line.strip())
        if match:
            topic = match.group("topic").strip()
            seq = int(match.group("seq"))
            latency = int(match.group("latency"))

            stats[topic]["latencies"].append(latency)
            stats[topic]["received_seq"].add(seq)

with open(output_file, mode='w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Topic", "Avg Latency (µs)", "Min Latency", "Max Latency", "Received", "Expected", "Reach Rate (%)"])

    for topic, data in stats.items():
        latencies = data["latencies"]
        received = len(data["received_seq"])
        max_seq = max(data["received_seq"])
        expected = max_seq
        reach_rate = (received / expected) * 100 if expected > 0 else 0.0

        writer.writerow([
            topic,
            sum(latencies) // len(latencies),
            min(latencies),
            max(latencies),
            received,
            expected,
            f"{reach_rate:.2f}"
        ])

