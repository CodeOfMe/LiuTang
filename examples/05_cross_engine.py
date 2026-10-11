"""
Example: Pure Python streaming - same API for batch and streaming
"""
import liutang


def create_pipeline(engine: str = "local") -> liutang.Flow:
    flow = liutang.Flow(name="demo-pipeline", engine=engine, mode=liutang.RuntimeMode.BATCH)
    data = [
        {"name": "Alice", "value": 10},
        {"name": "Bob", "value": 20},
        {"name": "Alice", "value": 30},
        {"name": "Bob", "value": 15},
        {"name": "Charlie", "value": 50},
    ]
    stream = flow.from_collection(data)
    result = stream.key_by(lambda row: row["name"]).sum(field="value")
    sink = result.collect()
    return flow, sink


def main():
    print("=== liutang: Pure Python Streaming ===\n")
    print("Available engines: local (always), no external deps needed!\n")

    flow, sink = create_pipeline("local")
    flow.execute()
    print("Results:")
    for item in sink.results:
        print(f"  {item}")


if __name__ == "__main__":
    main()