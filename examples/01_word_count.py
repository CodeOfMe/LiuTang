"""
Example: Word Count with liutang (流淌)

Demonstrates the simplest possible streaming pipeline using the local engine
with pure Python concurrency - no Flink or Spark required.
"""
import liutang


def main():
    text = [
        "hello world",
        "hello liutang",
        "streaming data flows",
        "hello spark flink",
        "data flows like water",
    ]

    flow = liutang.Flow(name="word-count", engine="local", mode=liutang.RuntimeMode.BATCH)
    flow.set_parallelism(4)

    stream = flow.from_collection(text)
    result = (
        stream
        .flat_map(lambda line: line.split(), name="split_words")
        .map(lambda word: (word.lower(), 1), name="to_pair")
        .key_by(lambda pair: pair[0], name="group_by_word")
        .reduce(lambda a, b: (a[0], a[1] + b[1]), name="sum_counts")
    )
    result.print()

    results = flow.execute()
    print("\n--- Word Count Results ---")
    for source_id, data in results.items():
        for item in data:
            print(f"  {item}")


if __name__ == "__main__":
    main()