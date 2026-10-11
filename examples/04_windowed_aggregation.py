"""
Example: Windowed Aggregation - Pure Python streaming windows
"""
import liutang
import random


def main():
    events = []
    base_ts = 1000.0
    for i in range(50):
        events.append({
            "name": random.choice(["Alice", "Bob", "Charlie"]),
            "amount": round(random.uniform(10, 500), 2),
            "ts": base_ts + i * 2.0,
        })

    flow = liutang.Flow(name="window-agg", mode=liutang.RuntimeMode.BATCH)
    stream = flow.from_collection(events)
    windowed = stream.window(liutang.WindowType.tumbling(size=10.0, time_field="ts"))
    result = windowed.sum(field="amount", name="sum_per_window")
    sink = result.collect()

    flow.execute()
    print("\n--- Tumbling Window Sum ---")
    for item in sink.results:
        print(f"  {item}")

    print(f"\nTotal windows: {len(sink.results)}")


if __name__ == "__main__":
    main()