"""
Example: Streaming Pipeline with Kafka Source (local engine)

Demonstrates the Generator -> Message Broker -> Consumer pattern
from PyFlink-Tutorial, using the local engine with queue-based streaming.
"""
import liutang
import time
import threading


def main():
    flow = liutang.Flow(name="hamlet-stream", engine="local", mode=liutang.RuntimeMode.STREAMING)
    flow.set_parallelism(2)

    stream = (
        flow.from_kafka(topic="hamlet", bootstrap_servers="localhost:9092", group_id="liutang-hamlet")
        .flat_map(lambda line: line.split(), name="split_words")
        .filter(lambda word: len(word) > 3, name="filter_short")
        .key_by(lambda word: word.lower(), name="by_word")
        .count(name="word_count")
    )
    stream.sink_to(liutang.CallbackSink(func=lambda result: print(f"[hamlet] {result}")))

    print("Starting streaming pipeline...")
    print("(Make sure Kafka is running with topic 'hamlet')")
    print("Press Ctrl+C to stop\n")

    try:
        result = flow.execute()
        result["stop_events"]["source_0"].set()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()