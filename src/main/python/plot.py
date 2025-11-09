import sqlite3
import datetime
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import ast  # safe parsing of dict string

DB_PATH = "/home/r00t/Downloads/kafka_2.13-4.1.0/java-code/kafkastreams/mydatabase.db"
BAR_WIDTH = 0.4


# --- Fetch data ---
def fetch_last_5min():
    """Fetch last 5 minutes of errors and extract error types safely."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    five_min_ago = (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    query = "SELECT windowstart, value FROM top_errors WHERE windowstart >= ? ORDER BY windowstart ASC"
    cur.execute(query, (five_min_ago,))
    rows = cur.fetchall()
    conn.close()

    parsed_rows = []
    for ts, val in rows:
        try:
            d = ast.literal_eval(val)  # convert string to dict
            key = list(d.keys())[0]  # extract error type
            parsed_rows.append((ts, key))
        except Exception as e:
            print(f"Skipping invalid value {val}: {e}")
            continue

    return parsed_rows  # timestamps as strings for X-axis


# --- Prepare data ---
def prepare_data(rows):
    """Prepare counts per error type for each windowstart."""
    timestamps = sorted(list(set(row[0] for row in rows)))  # unique windowstart
    error_types = sorted(list(set(row[1] for row in rows)))

    data = {et: [0] * len(timestamps) for et in error_types}
    ts_index = {ts: i for i, ts in enumerate(timestamps)}

    for ts, et in rows:
        idx = ts_index[ts]
        data[et][idx] += 1

    return timestamps, error_types, data


# --- Plot ---
fig, ax = plt.subplots(figsize=(10, 5))


def update(frame):
    ax.clear()

    rows = fetch_last_5min()
    if not rows:
        ax.set_title("No data in last 5 minutes")
        return

    timestamps, error_types, data = prepare_data(rows)

    bottom = [0] * len(timestamps)
    for et in error_types:
        counts = data[et]
        ax.bar(timestamps, counts, width=BAR_WIDTH, bottom=bottom, label=et)
        bottom = [sum(x) for x in zip(bottom, counts)]

    ax.set_title("Errors in Last 5 Minutes")
    ax.set_xlabel("Window Start")
    ax.set_ylabel("Count")
    ax.set_ylim(0, max(bottom) + 1)
    ax.legend()

    ax.set_xticks(timestamps)
    ax.set_xticklabels([ts for ts in timestamps], rotation=45)
    plt.tight_layout()


# --- Animate ---
ani = FuncAnimation(fig, update, interval=60000)  # refresh every 60 seconds
plt.show()
