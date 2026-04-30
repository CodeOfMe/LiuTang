"""
Example: Switch between Local, Flink, and Spark engines with the same API

The key design principle of liutang: write once, run anywhere.
"""
import liutang


def create_pipeline(engine: str = "local") -> liutang.Flow:
    flow = liutang.Flow(name="cross-engine-pipeline", engine=engine)
    flow.set_parallelism(2)

    data = [
        {"name": "Alice", "value": 10},
        {"name": "Bob", "value": 20},
        {"name": "Alice", "value": 30},
        {"name": "Bob", "value": 15},
        {"name": "Charlie", "value": 50},
    ]

    stream = flow.from_collection(data)
    result = (
        stream
        .key_by(lambda row: row["name"], name="by_name")
        .sum(field="value", name="total_value")
    )
    result.print()
    return flow


def main():
    engines = ["local"]

    for engine in engines:
        if not liutang.is_engine_available(engine):
            print(f"Engine '{engine}' not available, skipping...")
            continue
        print(f"\n=== Running with {engine} engine ===")
        flow = create_pipeline(engine)
        try:
            results = flow.execute()
            for source_id, data in results.items():
                for item in data:
                    print(f"  {item}")
        except liutang.EngineNotAvailableError as e:
            print(f"  Engine unavailable: {e}")

    if liutang.is_engine_available("flink"):
        print(f"\n=== Running with Flink engine ===")
        flow = create_pipeline("flink")
        try:
            flow.execute()
        except liutang.EngineNotAvailableError as e:
            print(f"  Engine unavailable: {e}")

    if liutang.is_engine_available("spark"):
        print(f"\n=== Running with Spark engine ===")
        flow = create_pipeline("spark")
        try:
            flow.execute()
        except liutang.EngineNotAvailableError as e:
            print(f"  Engine unavailable: {e}")


if __name__ == "__main__":
    main()