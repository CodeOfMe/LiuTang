"""
Example: Windowed Aggregation with liutang (流淌)

Demonstrates tumbling and session windows using the local engine,
similar to PyFlink-Tutorial window examples.
"""
import liutang
import random
import time


def main():
    events = []
    base_ts = 1000.0
    for i in range(50):
        events.append({
            "name": random.choice(["Alice", "Bob", "Charlie"]),
            "amount": round(random.uniform(10, 500), 2),
            "ts": base_ts + i * 2.0,
        })

    flow = liutang.Flow(name="windowed-aggregation", engine="local", mode=liutang.RuntimeMode.BATCH)
    flow.set_parallelism(1)

    stream = flow.from_collection(events)
    windowed = stream.window(liutang.WindowType.tumbling(size=10.0, time_field="ts"))
    result = windowed.sum(field="amount", name="sum_per_window")
    result.print()

    results = flow.execute()
    print("\n--- Tumbling Window Sum (10s) ---")
    for source_id, data in results.items():
        for item in data:
            print(f"  {item}")


if __name__ == "__main__":
    main()