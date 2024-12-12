import pandas as pd
import matplotlib.pyplot as plt

data = []
with open("output.txt", "r") as file:
    for line in file:
        parts = line.strip().split(", ")
        priority = parts[0].split(" ")[1]
        message_count = int(parts[1].split(" ")[0])
        latency = float(parts[2].split(" ")[0])
        data.append({"Priority": priority, "MessageCount": message_count, "Latency": latency})

df = pd.DataFrame(data)

df['CumulativeLatency'] = df.groupby('Priority')['Latency'].cumsum()

plt.figure(figsize=(10, 6))
for priority in df['Priority'].unique():
    subset = df[df['Priority'] == priority].reset_index()
    plt.plot(subset.index, subset['Latency'], label=f"{priority} Priority", linewidth=1)

plt.ylim(0, 300)

plt.title("Cumulative Latency by Priority")
plt.xlabel("Packet Number")
plt.ylabel("Cumulative Time (ms)")
plt.legend(title="Priority")
plt.grid()

plt.show()

