"""
Example: Word Count with liutang (流淌) - Pure Python, zero dependencies
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

    flow = liutang.Flow(name="word-count", mode=liutang.RuntimeMode.BATCH)
    flow.set_parallelism(4)

    stream = flow.from_collection(text)
    result = (
        stream
        .flat_map(lambda line: line.split(), name="split_words")
        .map(lambda w: w.lower(), name="to_lowercase")
        .map(lambda w: (w, 1), name="to_pair")
        .key_by(lambda pair: pair[0], name="by_word")
    )
    sink = result.collect()

    results = flow.execute()
    print("\n--- Word Count Results ---")
    for item in sink.results:
        print(f"  {item}")


if __name__ == "__main__":
    main()