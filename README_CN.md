# LiuTang (流淌)

**纯 Python 流数据框架**——零外部依赖，原生实现窗口、水位线、有状态处理。

**流淌**（liútǎng）意为"流动"——数据如水，流淌不息。

## 设计原则

- **零依赖** — `pip install liutang` 只需 Python 标准库，没有版本灾难
- **原生并发** — 本地引擎用 `threading` / `multiprocessing` / `concurrent.futures` 实现并行
- **完整流式** — 窗口、水位线(Watermark)、KeyedState、定时器、Checkpoint、可切换投递语义 — 全部纯 Python 实现
- **API 统一** — 同一套 `Flow` / `Stream` API 同时支持批处理和流处理
- **架构融合** — Lambda（批+速）、Kappa（日志+回放）和自适应（颗粒度可调）模式共用一套API

## 快速开始

```python
import liutang

# 批处理模式 — 零依赖
flow = liutang.Flow(name="word-count", mode=liutang.RuntimeMode.BATCH)
flow.set_parallelism(4)

stream = flow.from_collection(["hello world", "hello liutang", "data flows"])

result = (
    stream
    .flat_map(lambda line: line.split())    # 拆词
    .map(lambda w: (w.lower(), 1))         # 映射为 (词, 1)
    .key_by(lambda pair: pair[0])           # 按词分组
)

sink = result.collect()  # 收集结果
flow.execute()

for item in sink.results:
    print(item)
```

## 核心功能

### 窗口 (Windowing)

```python
# 滚动窗口
stream.window(liutang.WindowType.tumbling(size=10.0, time_field="ts"))

# 滑动窗口
stream.window(liutang.WindowType.sliding(size=10.0, slide=5.0, time_field="ts"))

# 会话窗口
stream.window(liutang.WindowType.session(gap=30.0, time_field="ts"))

# 窗口聚合
windowed = stream.window(liutang.WindowType.tumbling(size=60.0, time_field="ts"))
windowed.sum(field="amount")
windowed.count()
windowed.min(field="price")
windowed.max(field="price")
windowed.aggregate(my_func)
windowed.apply(lambda window_data: process(window_data))
```

### 水位线 (Watermark)

```python
# 单调递增水位线
stream.assign_timestamps(
    extractor=lambda row: row["ts"],
    watermark_strategy=liutang.WatermarkStrategy.monotonous(time_field="ts")
)

# 有界乱序水位线
stream.assign_timestamps(
    extractor=lambda row: row["ts"],
    watermark_strategy=liutang.WatermarkStrategy.bounded_out_of_orderness(2.0, time_field="ts")
)
```

### 有状态处理 (Stateful Processing)

```python
# ValueState -- 带 TTL 的状态
state = ctx.get_state("my_state")
state.value = "hello"
state.value  # "hello"

# KeyedState -- 按键隔离的状态
class CountFunc(liutang.KeyedProcessFunction):
    def process_element(self, value, ctx):
        count = ctx.get_state("count")
        count.value = (count.value or 0) + 1
        return (ctx.current_key(), count.value)

# ListState / MapState / ReducingState / AggregatingState
ctx.get_list_state("events").add("event1")
ctx.get_map_state("counts").put("word", 5)
```

### 定时器 (Timers)

```python
class TimerFunc(liutang.KeyedProcessFunction):
    def process_element(self, value, ctx):
        ctx.timer_service.register_event_time_timer(value["ts"] + 10.0)
        return value

    def on_timer(self, timestamp, ctx):
        return {"alert": f"Timer fired at {timestamp}"}
```

### Checkpoint

```python
# 内存状态后端支持快照和恢复
backend = liutang.MemoryStateBackend()
backend.set_value("key", "value")
snapshot = backend.checkpoint()  # 保存快照
backend2 = liutang.MemoryStateBackend()
backend2.restore(snapshot)      # 恢复快照
```

### 投递语义 (Delivery Semantics)

```python
# 至少一次 (默认): 失败重试，可能产生重复
flow = liutang.Flow(delivery_mode=liutang.DeliveryMode.AT_LEAST_ONCE, max_retries=3)

# 最多一次: 跳过失败记录，尽力投递
flow = liutang.Flow(delivery_mode=liutang.DeliveryMode.AT_MOST_ONCE)

# 恰好一次: 基于哈希去重
flow = liutang.Flow(delivery_mode=liutang.DeliveryMode.EXACTLY_ONCE)
```

### Lambda 架构

双层处理：批处理层处理历史数据 + 速度层处理实时数据，通过 ServingView 合并。

```python
# Lambda: 批处理层 + 速度层
lf = liutang.LambdaFlow(
    name="analytics",
    batch_layer_fn=lambda f: f.from_collection(historical_data).map(transform),
    speed_layer_fn=lambda f: f.from_collection(realtime_data).map(transform),
    key_fn=lambda x: x["key"],
    merge_fn=liutang.MergeView.latest,
)
result = lf.execute()  # 批+速并行执行，结果合并到服务视图
result = lf.query("user_123")  # 查询合并结果
```

### Kappa 架构

单流处理 + 事件日志回放，支持重新处理。

```python
# Kappa: 事件日志 + 回放
kf = liutang.KappaFlow(
    name="pipeline",
    stream_fn=lambda f: f.from_kafka(topic="events").map(transform),
    event_log_path="/data/events.log",
)
kf.execute()           # 流处理 + 日志记录
kf.append_to_log({"event": "new"})  # 追加事件
records = kf.replay()  # 从日志回放
records = kf.replay(offset=100)  # 从偏移量回放
```

### 架构模式

直接在 Flow 上设置架构模式：

```python
flow = liutang.Flow(architecture=liutang.ArchitectureMode.LAMBDA)
flow = liutang.Flow(architecture=liutang.ArchitectureMode.KAPPA)

# 或将已有 Flow 转换
lf = flow.as_lambda()   # Flow -> LambdaFlow
kf = flow.as_kappa(event_log_path="/data/events.log")  # Flow -> KappaFlow
```

### 自适应颗粒度架构

颗粒度可在微流到宏批之间连续调节，由实时指标自动控制。

```python
# 自适应：根据吞吐/延迟自动调节颗粒度
af = liutang.AdaptiveFlow(
    name="adaptive-pipeline",
    stream_fn=lambda f: f.from_collection(data).map(transform),
    policy=liutang.GranularityPolicy.BALANCED,
    initial_granularity=liutang.GranularityLevel.MEDIUM,
)

# 以当前颗粒度执行
result = af.execute()

# 或显式设置颗粒度
af.set_granularity(liutang.GranularityLevel.MICRO)   # 纯流式 (batch_size=1)
af.set_granularity(liutang.GranularityLevel.MACRO)   # 近批处理 (batch_size=100000)

# 快捷方式
result = af.execute_stream_like()  # 设置为 MICRO 后执行
result = af.execute_batch_like()   # 设置为 MACRO 后执行
```

**5 级颗粒度：**

| 级别 | 批大小 | 超时 | 模式 |
|------|--------|------|------|
| MICRO | 1 | 10ms | 纯流式 |
| FINE | 10 | 100ms | 低延迟流式 |
| MEDIUM | 100 | 500ms | 均衡 |
| COARSE | 1,000 | 2s | 高吞吐 |
| MACRO | 100,000 | 10s | 近批处理 |

**3 种调节策略：**
- `THROUGHPUT` — 数据量大时倾向更大批次
- `LATENCY` — 延迟要求低时倾向更小批次
- `BALANCED` — 综合吞吐+延迟信号调节

### 数据源 (Sources)

```python
# 内存集合
flow.from_collection([1, 2, 3, 4, 5])

# 生成器
def gen():
    for i in range(100):
        yield {"id": i, "value": random.random() * 100}
flow.from_generator(gen, max_items=100)

# 文件 (txt/csv/json)
flow.from_file("/data/input.csv", fmt="csv")

# Kafka (需要 kafka-python)
flow.from_kafka(topic="events", bootstrap_servers="localhost:9092")

# 数据生成器
flow.from_source(liutang.DatagenSource(rows_per_second=100, fields={"temp": "float"}))

# Socket
flow.from_source(liutang.SocketSource(host="localhost", port=9999))
```

### 数据汇 (Sinks)

```python
# 控制台输出
stream.print()

# 收集到列表
sink = stream.collect()
flow.execute()
print(sink.results)

# 回调函数
stream.sink_to(liutang.CallbackSink(func=lambda x: web_push(x)))

# 文件输出
stream.sink_to(liutang.FileSink(path="/data/output.jsonl", fmt="json"))

# Kafka (需要 kafka-python)
stream.sink_to(liutang.KafkaSink(topic="results"))
```

### 流处理模式

```python
flow = liutang.Flow(name="realtime", mode=liutang.RuntimeMode.STREAMING)
flow.set_parallelism(2)

stream = flow.from_kafka(topic="events")
result = stream.filter(lambda x: x["amount"] > 5000)
result.sink_to(liutang.CallbackSink(func=handle_alert))

# 返回控制句柄，可随时停止
handles = flow.execute()
# ... 运行中 ...
handles["stop_events"]["source_0"].set()  # 停止
```

### API 解释

```python
flow = liutang.Flow(mode=liutang.RuntimeMode.BATCH)
stream = flow.from_collection([1, 2, 3]).map(lambda x: x * 2)
result = stream.filter(lambda x: x > 2)
result.print()
print(flow.explain())
# 输出:
# Flow: liutang-flow
# Engine: liutang (pure Python)
# Mode: batch
# Parallelism: 1
# ...
```

## 安装

```bash
# 基础安装 — 零依赖
pip install liutang

# Kafka 连接器 (可选)
pip install liutang[kafka]

# 开发
pip install liutang[dev]
```

## 项目结构

```
liutang/
├── src/liutang/
│   ├── __init__.py              # 公开 API + 版本号
│   ├── core/
│   │   ├── flow.py              # Flow — 管道定义与执行
│   │   ├── stream.py            # Stream / KeyedStream / WindowedStream / TableStream
│   │   ├── schema.py            # Schema / Field / FieldType
│   │   ├── window.py            # WindowType (滚动/滑动/会话/Over/Global)
│   │   ├── connector.py         # Source/Sink 连接器 (纯 Python 实现)
│   │   ├── state.py             # 完整的状态管理 (Value/List/Map/Reducing/Aggregating)
│   │   ├── eventlog.py          # EventLog — 只追加分段日志
│   │   ├── serving.py           # ServingView + MergeView (批/速合并)
│   │   ├── lambda_flow.py       # LambdaFlow + KappaFlow
│   │   ├── granularity.py     # GranularityLevel + GranularityController
│   │   ├── adaptive_flow.py  # AdaptiveFlow — 颗粒度可调架构
│   │   ├── errors.py            # 异常层级
│   │   └── (无外部依赖!)
│   └── engine/
│       ├── executor.py           # 主执行器 (批处理 + 流处理)
│       ├── runner.py            # StreamRunner — 并行管道执行
│       └── watermark.py         # WatermarkTracker
├── examples/                    # 示例管道
├── tests/                       # 183 个测试
├── upload_pypi.sh               # Unix 发布脚本
├── upload_pypi.bat              # Windows 发布脚本
└── pyproject.toml               # 零依赖!
```

## License

GPL-3.0-or-later

## 完整对比

详见 [COMPARISON.md](COMPARISON.md) — 与 PyFlink / PySpark / Beam / Bytewax / Faust / Streamz 的全方位对比。

liutang 是唯一原生支持 Lambda、Kappa 和自适应架构的纯 Python 流框架。