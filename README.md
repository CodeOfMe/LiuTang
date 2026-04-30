# LiuTang (流淌)

A pure-Python streaming data framework with native concurrency, watermark, windowing, and stateful processing. Zero external dependencies.

**流淌** (liútǎng) means "to flow" — data flows like water.

## Design Principles

- **Zero dependencies** — `pip install liutang` needs only Python stdlib; no version hell
- **Native concurrency** — local engine uses `threading` / `multiprocessing` / `concurrent.futures`
- **Full streaming** — Watermark, event-time windows, keyed state, timers, checkpointing — all pure Python
- **Unified API** — Same `Flow` / `Stream` API for batch and streaming modes

## Quick Start

```python
import liutang

# Batch mode — zero dependencies
flow = liutang.Flow(name="word-count", mode=liutang.RuntimeMode.BATCH)
flow.set_parallelism(4)

stream = flow.from_collection(["hello world", "hello liutang", "data flows"])

result = (
    stream
    .flat_map(lambda line: line.split())    # split words
    .map(lambda w: (w.lower(), 1))          # word -> (word, 1)
    .key_by(lambda pair: pair[0])           # group by word
)

sink = result.collect()  # collect results
flow.execute()

for item in sink.results:
    print(item)
```

## Core Features

### Windowing

```python
# Tumbling window
stream.window(liutang.WindowType.tumbling(size=10.0, time_field="ts"))

# Sliding window
stream.window(liutang.WindowType.sliding(size=10.0, slide=5.0, time_field="ts"))

# Session window
stream.window(liutang.WindowType.session(gap=30.0, time_field="ts"))

# Window aggregations
windowed = stream.window(liutang.WindowType.tumbling(size=60.0, time_field="ts"))
windowed.sum(field="amount")
windowed.count()
windowed.min(field="price")
windowed.max(field="price")
windowed.aggregate(my_func)
windowed.apply(lambda window_data: process(window_data))
```

### Watermarks

```python
# Monotonous watermarks
stream.assign_timestamps(
    extractor=lambda row: row["ts"],
    watermark_strategy=liutang.WatermarkStrategy.monotonous(time_field="ts")
)

# Bounded out-of-orderness watermarks
stream.assign_timestamps(
    extractor=lambda row: row["ts"],
    watermark_strategy=liutang.WatermarkStrategy.bounded_out_of_orderness(2.0, time_field="ts")
)
```

### Stateful Processing

```python
# ValueState with TTL
state = ctx.get_state("my_state")
state.value = "hello"

# KeyedProcessFunction
class CountFunc(liutang.KeyedProcessFunction):
    def process_element(self, value, ctx):
        count = ctx.get_state("count")
        count.value = (count.value or 0) + 1
        return (ctx.current_key(), count.value)

# ListState / MapState / ReducingState / AggregatingState
ctx.get_list_state("events").add("event1")
ctx.get_map_state("counts").put("word", 5)
```

### Timers

```python
class TimerFunc(liutang.KeyedProcessFunction):
    def process_element(self, value, ctx):
        ctx.timer_service.register_event_time_timer(value["ts"] + 10.0)
        return value

    def on_timer(self, timestamp, ctx):
        return {"alert": f"Timer fired at {timestamp}"}
```

### Checkpointing

```python
backend = liutang.MemoryStateBackend()
backend.set_value("key", "value")
snapshot = backend.checkpoint()  # save state
backend2 = liutang.MemoryStateBackend()
backend2.restore(snapshot)       # restore state
```

### Sources

```python
flow.from_collection([1, 2, 3, 4, 5])                # In-memory
flow.from_generator(gen, max_items=100)                # Generator
flow.from_file("/data/input.csv", fmt="csv")            # File (txt/csv/json)
flow.from_kafka(topic="events", bootstrap_servers="...")  # Kafka (optional dep)
flow.from_source(liutang.DatagenSource(rows_per_second=100))  # Synthetic
flow.from_source(liutang.SocketSource(port=9999))       # Socket
```

### Sinks

```python
stream.print()                                           # Print
sink = stream.collect()                                  # Collect to list
stream.sink_to(liutang.CallbackSink(func=handle))        # Callback
stream.sink_to(liutang.FileSink(path="out.jsonl"))       # File
stream.sink_to(liutang.KafkaSink(topic="results"))       # Kafka (optional dep)
```

### Streaming Mode

```python
flow = liutang.Flow(name="realtime", mode=liutang.RuntimeMode.STREAMING)
stream = flow.from_kafka(topic="events")
result = stream.filter(lambda x: x["amount"] > 5000)
result.sink_to(liutang.CallbackSink(func=handle_alert))

handles = flow.execute()  # returns immediately, runs in background
# ... streaming ...
handles["stop_events"]["source_0"].set()  # graceful stop
```

### Explain

```python
flow = liutang.Flow(mode=liutang.RuntimeMode.BATCH)
stream = flow.from_collection([1, 2, 3]).map(lambda x: x * 2)
result = stream.filter(lambda x: x > 2)
result.print()
print(flow.explain())
# Output:
# Engine: liutang (pure Python)
# Mode: batch
# ...
```

## Installation

```bash
# Base install — zero dependencies
pip install liutang

# With Kafka connector (optional)
pip install liutang[kafka]

# Development
pip install liutang[dev]
```

## Project Structure

```
liutang/
├── src/liutang/
│   ├── __init__.py              # Public API + version
│   ├── core/
│   │   ├── flow.py              # Flow — pipeline definition & execution
│   │   ├── stream.py            # Stream / KeyedStream / WindowedStream / TableStream
│   │   ├── schema.py            # Schema / Field / FieldType
│   │   ├── window.py            # WindowType (tumbling/sliding/session/over/global)
│   │   ├── connector.py         # Source/Sink connectors (pure Python)
│   │   ├── state.py             # Full state management (Value/List/Map/Reducing/Aggregating)
│   │   ├── errors.py            # Exception hierarchy
│   │   └── (no external dependencies!)
│   └── engine/
│       ├── executor.py           # Main executor (batch + streaming)
│       ├── runner.py            # StreamRunner — parallel pipeline execution
│       └── watermark.py         # WatermarkTracker
├── examples/                    # Example pipelines
├── tests/                       # 60 tests
├── upload_pypi.sh               # Unix publish script
├── upload_pypi.bat              # Windows publish script
└── pyproject.toml               # Zero required dependencies
```

## Version

Version is controlled exclusively in `src/liutang/__init__.py`. The publish scripts auto-bump the patch version.

## License

GPL-3.0-or-later

## Full Comparison

See [COMPARISON.md](COMPARISON.md) — comprehensive comparison with PyFlink / PySpark / Beam / Bytewax / Faust / Streamz.