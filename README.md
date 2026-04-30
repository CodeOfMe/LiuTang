# LiuTang (流淌)

A unified streaming data framework with native Python concurrency, optional Apache Flink and Spark backends.

**流淌**（liútǎng）means "to flow" in Chinese — data flows like water, through any engine.

## Design Principles

- **Zero hard dependencies** — `pip install liutang` just works; Flink/Spark are lazy-imported extras
- **Native concurrency** — local engine uses `threading` / `multiprocessing` / `concurrent.futures`, no external libs required
- **API parity** — same `Flow` / `Stream` API works across local, Flink, and Spark engines
- **Version isolation** — engine adapters catch version mismatches with clear error messages, preventing version disasters

## Architecture

```
┌─────────────────────────────────────┐
│         User API Layer              │
│  Flow · Stream · Schema · Window   │
├─────────────────────────────────────┤
│       Engine Adapter Layer          │
│  ┌─────────┬─────────┬───────────┐ │
│  │  Local  │  Flink  │   Spark   │ │
│  │(thread) │ (lazy)  │  (lazy)   │ │
│  └─────────┴─────────┴───────────┘ │
├─────────────────────────────────────┤
│      Connector Layer (lazy)         │
│  Kafka · File · Collection · Socket │
└─────────────────────────────────────┘
```

## Quick Start

```python
import liutang

# Create a flow with local engine (no dependencies needed!)
flow = liutang.Flow(name="word-count", engine="local", mode=liutang.RuntimeMode.BATCH)
flow.set_parallelism(4)

# Build pipeline
stream = flow.from_collection(["hello world", "hello liutang", "data flows"])
result = (
    stream
    .flat_map(lambda line: line.split())    # split lines into words
    .map(lambda w: (w.lower(), 1))          # word -> (word, 1)
    .key_by(lambda pair: pair[0])           # group by word
    .reduce(lambda a, b: (a[0], a[1]+b[1])) # sum counts
)
result.print()

# Execute
results = flow.execute()
```

## Switch Engines

Same API, different engine — just change one parameter:

```python
# Local engine — pure Python, no dependencies
flow = liutang.Flow(engine="local")

# Flink engine — needs: pip install liutang[flink]
flow = liutang.Flow(engine="flink")

# Spark engine — needs: pip install liutang[spark]
flow = liutang.Flow(engine="spark")
```

Check engine availability:

```python
liutang.is_engine_available("flink")   # True if apache-flink is installed
liutang.is_engine_available("spark")   # True if pyspark is installed
liutang.list_engines()                  # ["local", "flink", "spark"]
```

## Core API

### Source Connectors

```python
# From in-memory collection
stream = flow.from_collection([1, 2, 3, 4, 5])

# From file (text, csv, json)
stream = flow.from_file("/data/input.csv", fmt="csv")

# From Kafka (needs: pip install liutang[kafka])
stream = flow.from_kafka(topic="events", bootstrap_servers="localhost:9092")

# From datagen (synthetic data)
stream = flow.from_source(liutang.DatagenSource(rows_per_second=100))
```

### Transformations

```python
# Map — one-to-one transform
stream.map(lambda x: x * 2)

# Flat map — one-to-many
stream.flat_map(lambda line: line.split())

# Filter — keep matching
stream.filter(lambda x: x > 0)

# Key by — partition for aggregation
stream.key_by(lambda row: row["user_id"])

# Reduce — combine pairs
stream.reduce(lambda a, b: a + b)

# Process — stateful processing
stream.process(my_process_function)
```

### Windowing

```python
# Tumbling window — fixed, non-overlapping
stream.window(liutang.WindowType.tumbling(size=10.0, time_field="ts"))

# Sliding window — overlapping
stream.window(liutang.WindowType.sliding(size=10.0, slide=5.0, time_field="ts"))

# Session window — gap-based
stream.window(liutang.WindowType.session(gap=30.0, time_field="ts"))

# Windowed aggregation
windowed = stream.window(liutang.WindowType.tumbling(size=60.0, time_field="ts"))
result = windowed.sum(field="amount")
result = windowed.count()
result = windowed.aggregate(my_agg_func)
```

### Sink Connectors

```python
# Print to console
stream.print()

# Write to file
stream.sink_to(liutang.FileSink(path="/data/output.csv", fmt="csv"))

# Write to Kafka
stream.sink_to(liutang.KafkaSink(topic="results", bootstrap_servers="localhost:9092"))

# Custom callback
stream.sink_to(liutang.CallbackSink(func=lambda x: print(f"Result: {x}")))
```

### State Management (local engine)

```python
backend = liutang.MemoryStateBackend()
state = liutang.KeyedState(backend, "user:123")

state.value = 42                # ValueState
state.append("event1")          # ListState
state.put("field", "value")     # MapState
```

## Schema Definition

```python
schema = (
    liutang.Schema()
    .add("name", liutang.FieldType.STRING)
    .add("age", liutang.FieldType.INTEGER)
    .add("score", liutang.FieldType.FLOAT)
    .add("ts", liutang.FieldType.TIMESTAMP)
)

# Or from dict
schema = liutang.Schema.from_dict({
    "name": liutang.FieldType.STRING,
    "value": liutang.FieldType.DOUBLE,
})
```

## Streaming Mode

```python
flow = liutang.Flow(name="realtime", engine="local", mode=liutang.RuntimeMode.STREAMING)
stream = flow.from_kafka(topic="events")
result = stream.filter(lambda x: x["amount"] > 5000)
result.sink_to(liutang.CallbackSink(func=alert_handler))

# Returns control immediately with thread handles
handles = flow.execute()
# ... runs until you stop
handles["stop_events"]["source_0"].set()
```

## Examples

| # | File | Description |
|---|------|------------|
| 01 | `word_count.py` | Basic word count with local engine |
| 02 | `csv_processing.py` | CSV-style data with filtering and mapping |
| 03 | `kafka_streaming.py` | Kafka source with streaming pipeline |
| 04 | `windowed_aggregation.py` | Tumbling window aggregation |
| 05 | `cross_engine.py` | Same pipeline across local/Flink/Spark |
| 06 | `fraud_detection.py` | Transaction fraud detection |

## Installation

```bash
# Base install — zero dependencies
pip install liutang

# With Flink support
pip install liutang[flink]

# With Spark support
pip install liutang[spark]

# With Kafka connector
pip install liutang[kafka]

# Everything
pip install liutang[all]

# Development
pip install liutang[dev]
```

## Project Structure

```
liutang/
├── src/liutang/
│   ├── __init__.py              # Public API
│   ├── core/
│   │   ├── flow.py              # Flow — pipeline definition
│   │   ├── stream.py            # Stream, KeyedStream, WindowedStream, TableStream
│   │   ├── schema.py            # Schema, Field, FieldType
│   │   ├── window.py            # WindowType (tumbling, sliding, session, over)
│   │   ├── connector.py         # Source/Sink connectors
│   │   ├── state.py             # State backends
│   │   ├── serializer.py        # JSON/pickle serialization
│   │   └── errors.py            # Exception hierarchy
│   └── engine/
│       ├── registry.py          # Engine discovery (lazy import)
│       ├── local/
│       │   ├── executor.py      # LocalExecutor — pure Python threading/mp
│       │   └── runner.py        # StreamRunner — batch/streaming pipeline runner
│       ├── flink/
│       │   ├── executor.py      # FlinkExecutor — PyFlink backend (lazy)
│       │   └── adapter.py       # FlinkTypeAdapter — schema translation
│       └── spark/
│           ├── executor.py      # SparkExecutor — PySpark backend (lazy)
│           └── adapter.py       # SparkTypeAdapter — schema translation
├── examples/                    # Example pipelines
├── tests/                       # Test suite
└── pyproject.toml               # Project config (zero required deps)
```

## Version Safety

liutang isolates you from version conflicts:

- Flink and Spark are **never imported** unless you actually use that engine
- Engine adapters validate minimum versions and give clear error messages
- Base installation has **zero** third-party dependencies
- Each optional dependency has its own version range

## License

GPL-3.0-or-later