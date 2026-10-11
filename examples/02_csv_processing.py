"""
Example: CSV Data Processing with liutang (流淌)

Pipeline pattern from PyFlink-Tutorial: read CSV, filter, aggregate.
Uses the local engine with native threading for parallel map/filter.
"""
import liutang
from liutang import FieldType


def main():
    schema = liutang.Schema.from_pairs([
        ("name", FieldType.STRING),
        ("age", FieldType.INTEGER),
        ("score", FieldType.FLOAT),
    ])

    sample_data = [
        {"name": "Alice", "age": 25, "score": 88.5},
        {"name": "Bob", "age": 30, "score": 92.0},
        {"name": "Charlie", "age": 22, "score": 76.3},
        {"name": "Diana", "age": 28, "score": 95.1},
        {"name": "Eve", "age": 35, "score": 67.8},
    ]

    flow = liutang.Flow(name="csv-processing", engine="local", mode=liutang.RuntimeMode.BATCH)
    flow.set_parallelism(2)

    stream = flow.from_collection(sample_data, schema=schema)

    high_scorers = (
        stream
        .filter(lambda row: row["score"] >= 80, name="high_scorers")
        .map(lambda row: {**row, "grade": "A" if row["score"] >= 90 else "B"}, name="add_grade")
    )
    high_scorers.print()

    results = flow.execute()
    print("\n--- High Scorers ---")
    for source_id, data in results.items():
        for item in data:
            print(f"  {item}")


if __name__ == "__main__":
    main()