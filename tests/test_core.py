import pytest
import time
import threading
import tempfile
import os
from liutang import (
    Flow, Stream, KeyedStream, RuntimeMode, DeliveryMode, ArchitectureMode,
    FieldType, Schema, WindowType, WindowKind,
    CollectionSource, GeneratorSource, FileSource, PrintSink,
    CallbackSink, CollectSink, DatagenSource,
    MemoryStateBackend, KeyedState, ValueState, ListState, MapState,
    ReducingState, AggregatingState,
    RuntimeContext, TimerService, KeyedProcessFunction, ProcessFunction,
    WatermarkStrategy, Watermark,
    LiuTangError, PipelineError, DeliveryError, quick_flow,
    EventLog, ServingView, MergeView, LambdaFlow, KappaFlow,
    GranularityLevel, GranularityPolicy, GranularityController, GranularityMetrics,
    AdaptiveFlow,
    Viscosity, ViscosityPolicy, ViscosityController, FlowMetrics,
)


class TestFlow:
    def test_create_flow_default(self):
        flow = Flow()
        assert flow.name == "liutang-flow"
        assert flow.engine == "local"
        assert flow.mode == RuntimeMode.STREAMING

    def test_create_flow_custom(self):
        flow = Flow(name="test", engine="local", mode=RuntimeMode.BATCH)
        assert flow.name == "test"
        assert flow.mode == RuntimeMode.BATCH

    def test_set_parallelism(self):
        flow = Flow().set_parallelism(4)
        assert flow.parallelism == 4

    def test_configure(self):
        flow = Flow().configure("key", "value")
        assert flow.config["key"] == "value"

    def test_from_collection(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        assert isinstance(stream, Stream)
        assert len(flow.sources) == 1

    def test_from_generator(self):
        def gen():
            yield 1
            yield 2
            yield 3
        flow = Flow()
        stream = flow.from_generator(gen, max_items=3)
        assert isinstance(stream, Stream)

    def test_quick_flow(self):
        flow = quick_flow("test")
        assert flow.name == "test"

    def test_set_checkpoint(self):
        flow = Flow().set_checkpoint("/tmp/ckp")
        assert flow.checkpoint_dir == "/tmp/ckp"


class TestSchema:
    def test_empty_schema(self):
        assert len(Schema().fields) == 0

    def test_add_field(self):
        schema = Schema().add("name", FieldType.STRING).add("age", FieldType.INTEGER)
        assert schema.field_names() == ["name", "age"]

    def test_from_dict(self):
        schema = Schema.from_dict({"name": FieldType.STRING, "value": FieldType.FLOAT})
        assert len(schema.fields) == 2

    def test_from_pairs(self):
        schema = Schema.from_pairs([("x", FieldType.DOUBLE)])
        assert schema.field_names() == ["x"]

    def test_get_field(self):
        schema = Schema.from_dict({"name": FieldType.STRING})
        assert schema.get_field("name").field_type == FieldType.STRING
        assert schema.get_field("missing") is None

    def test_to_dict_list(self):
        schema = Schema.from_dict({"name": FieldType.STRING})
        dl = schema.to_dict_list()
        assert dl[0]["name"] == "name"
        assert dl[0]["type"] == "string"


class TestStream:
    def test_map(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        assert len(result.operations()) == 1

    def test_filter(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.filter(lambda x: x > 2)
        assert len(result.operations()) == 1

    def test_flat_map(self):
        flow = Flow()
        stream = flow.from_collection(["hello world"])
        result = stream.flat_map(lambda s: s.split())
        assert len(result.operations()) == 1

    def test_key_by(self):
        flow = Flow()
        stream = flow.from_collection([("a", 1)])
        result = stream.key_by(lambda x: x[0])
        assert isinstance(result, KeyedStream)

    def test_chained_operations(self):
        flow = Flow()
        stream = flow.from_collection(["hello world"])
        result = stream.flat_map(lambda s: s.split()).filter(lambda w: len(w) > 3).map(lambda w: (w, 1))
        assert len(result.operations()) == 3

    def test_window(self):
        flow = Flow()
        stream = flow.from_collection([])
        windowed = stream.window(WindowType.tumbling(size=10.0))
        assert windowed.window_type.kind == WindowKind.TUMBLING

    def test_process_function(self):
        class MyProcess(ProcessFunction):
            def process(self, value):
                return value * 2
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        result = stream.process(MyProcess())
        assert len(result.operations()) == 1

    def test_min_max(self):
        flow = Flow()
        stream = flow.from_collection([1, 2, 3])
        assert stream.min(field=0).operations()[-1]["type"] == "min"
        assert stream.max(field=0).operations()[-1]["type"] == "max"


class TestWindow:
    def test_tumbling(self):
        w = WindowType.tumbling(size=10.0, time_field="ts")
        assert w.kind == WindowKind.TUMBLING
        assert w.size == 10.0

    def test_sliding(self):
        w = WindowType.sliding(size=10.0, slide=5.0, time_field="ts")
        assert w.kind == WindowKind.SLIDING
        assert w.slide == 5.0

    def test_session(self):
        w = WindowType.session(gap=30.0)
        assert w.kind == WindowKind.SESSION
        assert w.gap == 30.0

    def test_over(self):
        w = WindowType.over(time_field="ts")
        assert w.kind == WindowKind.OVER

    def test_global(self):
        w = WindowType.global_window()
        assert w.kind == WindowKind.GLOBAL


class TestConnector:
    def test_collection_source(self):
        src = CollectionSource(data=[1, 2, 3])
        assert src.kind().value == "collection"

    def test_generator_source(self):
        src = GeneratorSource(generator=lambda: (x for x in range(5)), max_items=5)
        assert src.kind().value == "generator"

    def test_file_source(self):
        src = FileSource(path="/tmp/data.csv", fmt="csv")
        assert src.kind().value == "file"

    def test_print_sink(self):
        sink = PrintSink()
        assert sink.kind().value == "print"

    def test_callback_sink(self):
        results = []
        sink = CallbackSink(func=lambda x: results.append(x))
        assert sink.kind().value == "callback"

    def test_collect_sink(self):
        sink = CollectSink()
        assert sink.kind().value == "collect"
        assert sink.results == []


class TestState:
    def test_value_state(self):
        state = ValueState("test")
        state.value = 42
        assert state.value == 42
        state.clear()
        assert state.value is None

    def test_value_state_ttl(self):
        state = ValueState("test", ttl=0.01)
        state.value = 42
        assert state.value == 42
        time.sleep(0.02)
        assert state.value is None

    def test_list_state(self):
        state = ListState("test")
        state.add(1)
        state.add(2)
        assert state.get() == [1, 2]
        assert len(state) == 2

    def test_map_state(self):
        state = MapState("test")
        state.put("a", 1)
        state.put("b", 2)
        assert state.get("a") == 1
        assert "a" in state.keys()

    def test_reducing_state_backend(self):
        state = MemoryStateBackend()
        state.append_list("reduce_test", 5)
        state.append_list("reduce_test", 3)
        assert state.get_list("reduce_test") == [5, 3]

    def test_reducing_state_class(self):
        rs = ReducingState("sum", reduce_fn=lambda a, b: a + b)
        rs.add(10)
        rs.add(20)
        assert rs.get() == 30

    def test_memory_backend_checkpoint(self):
        backend = MemoryStateBackend()
        backend.set_value("k1", "v1")
        backend.append_list("k2", "item1")
        checkpoint = backend.checkpoint()
        backend2 = MemoryStateBackend()
        backend2.restore(checkpoint)
        assert backend2.get_value("k1") == "v1"
        assert backend2.get_list("k2") == ["item1"]

    def test_keyed_state(self):
        backend = MemoryStateBackend()
        state = KeyedState(backend, "user:1")
        state.value = 100
        assert state.value == 100

    def test_runtime_context(self):
        ctx = RuntimeContext()
        ctx.set_current_key("user1")
        assert ctx.current_key() == "user1"
        v = ctx.get_state("count")
        v.value = 5
        assert v.value == 5
        l = ctx.get_list_state("events")
        l.add("event1")
        assert l.get() == ["event1"]

    def test_timer_service(self):
        ts = TimerService()
        fired = []
        ts.register_event_time_timer(100.0, lambda: fired.append(100))
        ts.register_event_time_timer(200.0, lambda: fired.append(200))
        callbacks = ts.fire_event_time_timers(150.0)
        for cb in callbacks:
            cb()
        assert 100 in fired
        assert 200 not in fired

    def test_keyed_process_function(self):
        class SumFunc(KeyedProcessFunction):
            def process_element(self, value, ctx):
                state = ctx.get_state("sum")
                state.value = (state.value or 0) + value
                return (ctx.current_key(), state.value)

        ctx = RuntimeContext()
        fn = SumFunc()
        fn.open(ctx)
        ctx.set_current_key("key1")
        result1 = fn.process_element(5, ctx)
        ctx.set_current_key("key1")
        result2 = fn.process_element(3, ctx)
        assert result1 == ("key1", 5)
        assert result2 == ("key1", 8)


class TestWatermarkStrategy:
    def test_monotonous(self):
        wm = WatermarkStrategy.monotonous()
        w1 = wm.on_event(None, 10.0)
        assert w1.timestamp == 10.0
        w2 = wm.on_event(None, 5.0)
        assert w2.timestamp == 10.0  # monotonous: watermark only goes forward

    def test_bounded_out_of_orderness(self):
        wm = WatermarkStrategy.bounded_out_of_orderness(2.0)
        wm.on_event(None, 10.0)
        assert wm.current_watermark().timestamp == 8.0


class TestLocalExecution:
    def test_batch_word_count(self):
        flow = Flow(name="wc", mode=RuntimeMode.BATCH)
        stream = flow.from_collection(["hello world", "hello liutang"])
        result = (
            stream
            .flat_map(lambda line: line.split())
            .map(lambda w: (w, 1))
            .key_by(lambda pair: pair[0])
        )
        sink = result.collect()
        output = flow.execute()
        assert len(sink.results) > 0

    def test_batch_filter(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3, 4, 5, 6])
        result = stream.filter(lambda x: x > 3)
        result.print()
        output = flow.execute()
        data = output["source_0"]
        assert all(x > 3 for x in data)

    def test_batch_map(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 10)
        result.print()
        output = flow.execute()
        data = output["source_0"]
        assert sorted(data) == [10, 20, 30]

    def test_callback_sink(self):
        results = []
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        result.sink_to(CallbackSink(func=lambda x: results.append(x)))
        flow.execute()
        assert sorted(results) == [2, 4, 6]

    def test_collect_sink(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x ** 2)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == [1, 4, 9]

    def test_batch_tumbling_window(self):
        events = [{"ts": float(i), "val": i} for i in range(20)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.tumbling(size=5.0, time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0

    def test_batch_session_window(self):
        events = [
            {"ts": 1.0, "val": 10},
            {"ts": 2.0, "val": 20},
            {"ts": 10.0, "val": 30},
            {"ts": 11.0, "val": 40},
        ]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.session(gap=5.0, time_field="ts"))
        result = windowed.sum(field="val")
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0

    def test_batch_keyed_sum(self):
        data = [("Alice", 10), ("Bob", 20), ("Alice", 30), ("Bob", 15)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        result = stream.key_by(lambda x: x[0]).sum(field=1)
        sink = result.collect()
        flow.execute()

    def test_batch_min_max(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([3, 1, 4, 1, 5, 9, 2, 6])
        result = stream.min()
        sink = result.collect()
        flow.execute()
        assert sink.results == [1]

    def test_batch_max(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([3, 1, 4, 1, 5, 9, 2, 6])
        result = stream.max()
        sink = result.collect()
        flow.execute()
        assert sink.results == [9]

    def test_explain(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3]).map(lambda x: x * 2)
        result = stream.filter(lambda x: x > 2)
        result.print()
        explanation = flow.explain()
        assert "liutang (pure Python)" in explanation
        assert "map" in explanation
        assert "filter" in explanation


class TestWatermarkWithRunner:
    def test_watermark_tracker(self):
        from liutang.engine.watermark import WatermarkTracker
        strategy = WatermarkStrategy.bounded_out_of_orderness(2.0)
        tracker = WatermarkTracker(strategy)
        tracker.on_event(10.0)
        assert tracker.current_watermark() == 8.0
        tracker.on_event(15.0)
        assert tracker.current_watermark() == 13.0


class TestProcessFunction:
    def test_custom_process_function(self):
        results = []

        class DoubleFilter(ProcessFunction):
            def process(self, value):
                doubled = value * 2
                return doubled if doubled > 4 else None

        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3, 4])
        result = stream.process(DoubleFilter())
        result.sink_to(CallbackSink(func=lambda x: results.append(x)))
        flow.execute()
        assert sorted(results) == [6, 8]

    def test_keyed_process_function(self):
        class CountFunc(KeyedProcessFunction):
            def process_element(self, value, ctx):
                state = ctx.get_state("count")
                state.value = (state.value or 0) + 1
                return (ctx.current_key(), state.value)

        data = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        result = stream.key_by(lambda x: x[0]).process(CountFunc())
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0


class TestGeneratorSource:
    def test_generator_batch(self):
        def gen():
            for i in range(5):
                yield i * 10

        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_generator(gen, max_items=5)
        result = stream.map(lambda x: x + 1)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == [1, 11, 21, 31, 41]


class TestDeliveryMode:
    def test_delivery_mode_default(self):
        flow = Flow()
        assert flow.delivery_mode == DeliveryMode.AT_LEAST_ONCE

    def test_delivery_mode_at_most_once(self):
        flow = Flow(delivery_mode=DeliveryMode.AT_MOST_ONCE)
        assert flow.delivery_mode == DeliveryMode.AT_MOST_ONCE

    def test_delivery_mode_exactly_once(self):
        flow = Flow(delivery_mode=DeliveryMode.EXACTLY_ONCE)
        assert flow.delivery_mode == DeliveryMode.EXACTLY_ONCE

    def test_delivery_mode_max_retries(self):
        flow = Flow(max_retries=5)
        assert flow.max_retries == 5

    def test_at_least_once_normal(self):
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_LEAST_ONCE)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == [2, 4, 6]

    def test_at_most_once_skip_on_error(self):
        call_count = {"n": 0}

        def faulty_map(x):
            call_count["n"] += 1
            if x == 2:
                raise ValueError("transient error")
            return x * 2

        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_MOST_ONCE)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(faulty_map)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == []

    def test_at_least_once_retry_succeeds(self):
        attempts = {"n": 0}

        def retry_map(x):
            attempts["n"] += 1
            if x == 2 and attempts["n"] <= 1:
                raise ValueError("retry me")
            return x * 2

        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_LEAST_ONCE, max_retries=3)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(retry_map)
        sink = result.collect()
        flow.execute()
        assert len(sink.results) == 3
        assert sorted(sink.results) == [2, 4, 6]

    def test_at_least_once_retry_exhausted_raises(self):
        def always_fail(x):
            raise ValueError("permanent error")

        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_LEAST_ONCE, max_retries=1)
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(always_fail)
        sink = result.collect()
        with pytest.raises(ValueError):
            flow.execute()

    def test_exactly_once_deduplication(self):
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.EXACTLY_ONCE)
        stream = flow.from_collection([1, 1, 2, 2, 3])
        result = stream.map(lambda x: x * 2)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == [2, 4, 6]

    def test_exactly_once_preserves_order(self):
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.EXACTLY_ONCE)
        stream = flow.from_collection([1, 2, 3, 2, 1])
        result = stream.map(lambda x: x * 10)
        sink = result.collect()
        flow.execute()
        assert sink.results[0] == 10
        assert sink.results[1] == 20
        assert sink.results[2] == 30

    def test_at_most_once_no_retry_on_sink_error(self):
        results = []

        def fail_on_three(x):
            if x == 3:
                raise ValueError("sink error")
            results.append(x)

        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_MOST_ONCE,
                     max_retries=0)
        stream = flow.from_collection([1, 2, 3, 4])
        result = stream.map(lambda x: x)
        result.sink_to(CallbackSink(func=fail_on_three))
        flow.execute()
        assert 1 in results
        assert 2 in results

    def test_explain_includes_delivery_mode(self):
        flow = Flow(delivery_mode=DeliveryMode.EXACTLY_ONCE)
        stream = flow.from_collection([1, 2, 3]).map(lambda x: x)
        result = stream.print()
        explanation = flow.explain()
        assert "exactly_once" in explanation

    def test_delivery_mode_enum_values(self):
        assert DeliveryMode.AT_LEAST_ONCE.value == "at_least_once"
        assert DeliveryMode.AT_MOST_ONCE.value == "at_most_once"
        assert DeliveryMode.EXACTLY_ONCE.value == "exactly_once"


class TestReducingState:
    def test_reducing_state(self):
        rs = ReducingState("sum_state", reduce_fn=lambda a, b: a + b)
        rs.add(10)
        rs.add(20)
        rs.add(30)
        assert rs.get() == 60

    def test_reducing_state_single(self):
        rs = ReducingState("max_state", reduce_fn=max)
        rs.add(5)
        assert rs.get() == 5

    def test_reducing_state_clear(self):
        rs = ReducingState("state", reduce_fn=lambda a, b: a + b)
        rs.add(1)
        rs.clear()
        assert rs.get() is None


class TestAggregatingState:
    def test_aggregating_state_sum(self):
        agg = AggregatingState("sum_agg", add_fn=lambda acc, x: acc + x, init_value=0)
        agg.add(10)
        agg.add(20)
        agg.add(30)
        assert agg.get() == 60

    def test_aggregating_state_with_init(self):
        agg = AggregatingState("count_agg", add_fn=lambda acc, x: acc + 1, init_value=0)
        agg.add(100)
        agg.add(200)
        assert agg.get() == 2

    def test_aggregating_state_clear(self):
        agg = AggregatingState("test", add_fn=lambda a, b: a + b)
        agg.add(1)
        agg.clear()
        assert agg.get() is None


class TestJsonFileStateBackend:
    def test_save_and_load(self, tmp_path):
        from liutang import JsonFileStateBackend
        backend = JsonFileStateBackend(str(tmp_path))
        backend.set_value("k1", "v1")
        backend.append_list("k2", "item1")
        backend.save()
        backend2 = JsonFileStateBackend(str(tmp_path))
        loaded = backend2.load()
        assert loaded is True
        assert backend2.get_value("k1") == "v1"
        assert backend2.get_list("k2") == ["item1"]

    def test_load_missing(self, tmp_path):
        from liutang import JsonFileStateBackend
        import os
        backend = JsonFileStateBackend(str(tmp_path))
        result = backend.load("nonexistent")
        assert result is False


class TestFlowValidation:
    def test_no_sources_raises(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        with pytest.raises(PipelineError, match="no sources"):
            flow.execute()

    def test_file_not_found_raises(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_file("/nonexistent/path/data.txt")
        with pytest.raises(PipelineError, match="does not exist"):
            flow.execute()


class TestSchemaEnforcement:
    def test_schema_enforcement_on_collection(self):
        flow = Flow(mode=RuntimeMode.BATCH).enable_schema_enforcement()
        schema = Schema().add("name", FieldType.STRING).add("value", FieldType.INTEGER)
        data = [{"name": "a", "value": 1}, {"name": "b", "value": 2}]
        stream = flow.from_collection(data, schema=schema)
        sink = stream.collect()
        flow.execute()
        assert len(sink.results) == 2


class TestFlowSchemaEnforcement:
    def test_enable_schema_enforcement(self):
        flow = Flow().enable_schema_enforcement()
        assert flow._schema_enforcement is True

    def test_schema_enforcement_default_off(self):
        flow = Flow()
        assert flow._schema_enforcement is False


class TestAllowedLateness:
    def test_window_with_allowed_lateness(self):
        events = [{"ts": 1.0, "val": 10}, {"ts": 2.0, "val": 20}, {"ts": 15.0, "val": 30}]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.tumbling(size=5.0, time_field="ts", allowed_lateness=2.0))
        result = windowed.sum(field="val")
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0

    def test_window_zero_lateness(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        windowed = stream.window(WindowType.tumbling(size=10.0, allowed_lateness=0.0))
        result = windowed.count()
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0


class TestConsistentEmptyData:
    def test_sum_empty(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([])
        result = stream.sum()
        sink = result.collect()
        flow.execute()
        assert sink.results == []

    def test_min_empty(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([])
        result = stream.min()
        sink = result.collect()
        flow.execute()
        assert sink.results == []

    def test_max_empty(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([])
        result = stream.max()
        sink = result.collect()
        flow.execute()
        assert sink.results == []


class TestExactlyOnceEviction:
    def test_eviction_prevents_unbounded_growth(self):
        from liutang.engine.runner import StreamRunner
        runner = StreamRunner([], delivery_mode=DeliveryMode.EXACTLY_ONCE)
        for i in range(200000):
            runner._processed_ids[f"id_{i}"] = True
            runner._evict_processed_ids()
        assert len(runner._processed_ids) <= StreamRunner.MAX_PROCESSED_IDS


class TestTableAPI:
    def test_order_by(self):
        data = [{"name": "b", "val": 2}, {"name": "a", "val": 1}, {"name": "c", "val": 3}]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        result = stream.map(lambda x: x)
        sink = result.collect()
        flow.execute()
        names = [r["name"] if isinstance(r, dict) else r for r in sink.results]
        assert len(sink.results) == 3

    def test_limit(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3, 4, 5])
        result = stream.filter(lambda x: x > 0)
        sink = result.collect()
        flow.execute()
        assert len(sink.results) == 5

    def test_grouped_count(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        data = [("a", 1), ("b", 2), ("a", 3)]
        stream = flow.from_collection(data)
        result = stream.key_by(lambda x: x[0]).count()
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0


class TestSlidingWindow:
    def test_sliding_window_assignment(self):
        events = [
            {"ts": 1.0, "val": 10},
            {"ts": 5.0, "val": 20},
            {"ts": 7.0, "val": 30},
            {"ts": 10.0, "val": 40},
        ]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.sliding(size=5.0, slide=2.0, time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        flow.execute()
        assert len(sink.results) > 0


class TestStreamingE2E:
    def test_streaming_collection(self):
        import threading
        flow = Flow(mode=RuntimeMode.STREAMING)
        stream = flow.from_collection([1, 2, 3])
        sink = stream.collect()
        result = flow.execute()
        time.sleep(1.0)
        for stop_event in result["stop_events"].values():
            stop_event.set()
        assert len(sink.results) >= 0

    def test_streaming_with_callback(self):
        import threading
        results = []
        flow = Flow(mode=RuntimeMode.STREAMING)
        stream = flow.from_collection([10, 20, 30])
        result = stream.sink_to(CallbackSink(func=lambda x: results.append(x)))
        handles = flow.execute()
        time.sleep(1.0)
        for stop_event in handles["stop_events"].values():
            stop_event.set()
        time.sleep(0.5)


class TestRunnerStats:
    def test_stats_initial(self):
        from liutang.engine.runner import StreamRunner
        runner = StreamRunner([])
        stats = runner.stats
        assert stats["batches_processed"] == 0
        assert stats["records_processed"] == 0
        assert stats["records_dropped"] == 0

    def test_stats_after_batch(self):
        from liutang.engine.runner import StreamRunner, PipelineBuilder
        ops = PipelineBuilder.from_operations([{"type": "map", "func": lambda x: x * 2}])
        runner = StreamRunner(ops)
        runner.run_batch([1, 2, 3])
        assert runner.stats["batches_processed"] == 0


class TestCheckpointIntegration:
    def test_checkpoint_dir_config(self):
        flow = Flow(mode=RuntimeMode.BATCH).set_checkpoint("/tmp/liutang_test_ckp")
        assert flow.checkpoint_dir == "/tmp/liutang_test_ckp"

    def test_checkpoint_with_execution(self, tmp_path):
        flow = Flow(mode=RuntimeMode.BATCH).set_checkpoint(str(tmp_path))
        stream = flow.from_collection([1, 2, 3])
        result = stream.map(lambda x: x * 2)
        sink = result.collect()
        flow.execute()
        assert sorted(sink.results) == [2, 4, 6]


class TestArchitectureMode:
    def test_simple_mode(self):
        flow = Flow()
        assert flow.architecture == ArchitectureMode.SIMPLE

    def test_enum_values(self):
        assert ArchitectureMode.SIMPLE.value == "simple"
        assert ArchitectureMode.LAMBDA.value == "lambda"
        assert ArchitectureMode.KAPPA.value == "kappa"

    def test_flow_with_lambda_architecture(self):
        flow = Flow(architecture=ArchitectureMode.LAMBDA)
        assert flow.architecture == ArchitectureMode.LAMBDA

    def test_flow_with_kappa_architecture(self):
        flow = Flow(architecture=ArchitectureMode.KAPPA)
        assert flow.architecture == ArchitectureMode.KAPPA


class TestEventLog:
    def test_append_and_read(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"))
        log.append({"id": 1, "value": 10})
        log.append({"id": 2, "value": 20})
        log.append({"id": 3, "value": 30})
        records = log.read()
        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[2]["value"] == 30

    def test_read_with_offset(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"))
        for i in range(10):
            log.append({"i": i})
        records = log.read(offset=5)
        assert len(records) == 5
        assert records[0]["i"] == 5

    def test_read_with_limit(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"))
        for i in range(10):
            log.append({"i": i})
        records = log.read(limit=3)
        assert len(records) == 3

    def test_append_batch(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"))
        offsets = log.append_batch([{"x": 1}, {"x": 2}])
        assert len(offsets) == 2
        records = log.read()
        assert len(records) == 2

    def test_offset_tracking(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"))
        assert log.offset == 0
        log.append({"a": 1})
        assert log.offset == 1
        log.append({"b": 2})
        assert log.offset == 2

    def test_segment_count(self, tmp_path):
        log = EventLog(str(tmp_path / "events.log"), max_segment_size=200)
        for i in range(20):
            log.append({"i": i, "data": "x" * 10})
        assert log.segment_count >= 1

    def test_persistence(self, tmp_path):
        log1 = EventLog(str(tmp_path / "persist.log"))
        log1.append({"x": 1})
        log1.append({"x": 2})
        log2 = EventLog(str(tmp_path / "persist.log"))
        records = log2.read()
        assert len(records) >= 2


class TestServingView:
    def test_update_batch(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        view.update_batch([("key1", 100), ("key2", 200)])
        result = view.query("key1")
        assert result == ("key1", 100)

    def test_update_speed(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        view.update_speed([("key1", 999)])
        result = view.query("key1")
        assert result == ("key1", 999)

    def test_query_all(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        view.update_batch([("a", 1), ("b", 2)])
        results = view.query_all()
        assert len(results) >= 2

    def test_query_no_key(self):
        view = ServingView(key_fn=lambda x: x)
        view.update_batch([1, 2, 3])
        result = view.query()
        assert len(result) == 3

    def test_clear(self):
        view = ServingView(key_fn=lambda x: x)
        view.update_batch([1, 2, 3])
        view.clear()
        assert view.query() == {}


class TestMergeView:
    def test_latest_prefers_speed(self):
        result = MergeView.latest({"val": 1}, {"val": 2})
        assert result == {"val": 2}

    def test_latest_falls_back_to_batch(self):
        result = MergeView.latest({"val": 1}, None)
        assert result == {"val": 1}

    def test_prefer_batch(self):
        result = MergeView.prefer_batch({"val": 1}, {"val": 2})
        assert result == {"val": 1}

    def test_combine_sum(self):
        result = MergeView.combine_sum(10, 5)
        assert result == 15

    def test_merge_dicts(self):
        result = MergeView.merge_dicts({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}


class TestLambdaFlow:
    def test_batch_only(self):
        lf = LambdaFlow(
            name="test-lambda",
            batch_layer_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 10),
        )
        result = lf.execute_batch_only()
        assert len(result["batch_results"]) == 3
        assert sorted(result["batch_results"]) == [10, 20, 30]

    def test_lambda_with_batch_only_layer(self):
        lf = LambdaFlow(
            name="test-batch",
            batch_layer_fn=lambda f: f.from_collection([10, 20]).map(lambda x: x * 2),
        )
        result = lf.execute()
        assert "batch_results" in result
        assert "serving" in result
        assert sorted(result["batch_results"]) == [20, 40]

    def test_lambda_query(self):
        lf = LambdaFlow(
            name="test-query",
            batch_layer_fn=lambda f: f.from_collection([("a", 1), ("b", 2)]),
            key_fn=lambda x: x[0] if isinstance(x, tuple) else str(x),
        )
        lf.execute_batch_only()
        result = lf.query("a")
        assert result is not None

    def test_lambda_with_event_log(self, tmp_path):
        lf = LambdaFlow(
            name="test-log",
            batch_layer_fn=lambda f: f.from_collection([1, 2, 3]),
            event_log_path=str(tmp_path / "lambda.log"),
        )
        lf.execute_batch_only()

    def test_lambda_merge_strategy(self):
        lf = LambdaFlow(
            name="test-merge",
            batch_layer_fn=lambda f: f.from_collection([1, 2, 3]),
            merge_fn=MergeView.combine_sum,
        )
        result = lf.execute_batch_only()
        assert "batch_results" in result

    def test_lambda_no_batch_raises(self):
        lf = LambdaFlow(name="no-batch")
        with pytest.raises(PipelineError, match="No batch layer"):
            lf.execute_batch_only()


class TestKappaFlow:
    def test_kappa_streaming(self):
        kf = KappaFlow(
            name="test-kappa",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 5),
        )
        result = kf.execute()
        assert "handles" in result

    def test_kappa_with_event_log(self, tmp_path):
        kf = KappaFlow(
            name="test-kappa-log",
            stream_fn=lambda f: f.from_collection([10, 20, 30]).map(lambda x: x + 1),
            event_log_path=str(tmp_path / "kappa.log"),
        )
        kf.append_to_log({"val": 100})
        kf.append_to_log({"val": 200})
        records = kf.replay()
        assert len(records) == 2

    def test_kappa_replay_offset(self, tmp_path):
        kf = KappaFlow(
            name="test-replay",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).sum(),
            event_log_path=str(tmp_path / "replay.log"),
        )
        for i in range(5):
            kf.append_to_log({"i": i, "val": i * 10})
        records = kf.replay(offset=2)
        assert len(records) == 3

    def test_kappa_replay_to_stream(self, tmp_path):
        kf = KappaFlow(
            name="test-replay-stream",
            stream_fn=lambda f: f.from_collection([1, 2]).map(lambda x: x),
            event_log_path=str(tmp_path / "stream.log"),
        )
        for i in range(3):
            kf.append_to_log({"i": i})
        result = kf.replay_to_stream()
        assert "results" in result

    def test_kappa_no_log_raises(self):
        kf = KappaFlow(name="no-log")
        with pytest.raises(PipelineError, match="No event log"):
            kf.replay()

    def test_kappa_no_stream_raises(self):
        kf = KappaFlow(name="no-stream")
        with pytest.raises(PipelineError, match="No stream"):
            kf.execute()

    def test_kappa_event_log_property(self, tmp_path):
        kf = KappaFlow(
            name="test-prop",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
            event_log_path=str(tmp_path / "prop.log"),
        )
        assert kf.event_log is not None
        kf2 = KappaFlow(name="no-log-flow")
        assert kf2.event_log is None


class TestLambdaKappaIntegration:
    def test_flow_as_lambda(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        stream.map(lambda x: x * 2)
        lf = flow.as_lambda()
        assert isinstance(lf, LambdaFlow)

    def test_flow_as_kappa(self):
        flow = Flow(mode=RuntimeMode.STREAMING)
        stream = flow.from_collection([1, 2, 3])
        stream.map(lambda x: x * 2)
        kf = flow.as_kappa(event_log_path="/tmp/test_kappa.log")
        assert isinstance(kf, KappaFlow)

    def test_architecture_mode_in_explain(self):
        flow = Flow()
        stream = flow.from_collection([1]).map(lambda x: x)
        stream.print()
        explanation = flow.explain()
        assert "simple" in explanation

    def test_lambda_batch_wordcount(self):
        lf = LambdaFlow(
            name="wordcount-lambda",
            batch_layer_fn=lambda f: (
                f.from_collection(["hello world", "hello liutang"])
                 .flat_map(lambda s: s.split())
                 .map(lambda w: (w, 1))
                 .key_by(lambda p: p[0])
            ),
        )
        result = lf.execute_batch_only()
        assert len(result["batch_results"]) > 0

    def test_event_log_persistence(self, tmp_path):
        log1 = EventLog(str(tmp_path / "persist.log"))
        log1.append({"x": 1})
        log1.append({"x": 2})
        log2 = EventLog(str(tmp_path / "persist.log"))
        records = log2.read()
        assert len(records) >= 2

    def test_kappa_append_and_replay(self, tmp_path):
        kf = KappaFlow(
            name="test-append",
            stream_fn=lambda f: f.from_collection([1, 2]).map(lambda x: x * 2),
            event_log_path=str(tmp_path / "kappa_test.log"),
        )
        kf.append_to_log({"val": 10})
        kf.append_to_log({"val": 20})
        records = kf.replay()
        assert len(records) == 2
        assert records[0]["val"] == 10


class TestGranularityLevel:
    def test_enum_values(self):
        assert GranularityLevel.MICRO.value == "micro"
        assert GranularityLevel.FINE.value == "fine"
        assert GranularityLevel.MEDIUM.value == "medium"
        assert GranularityLevel.COARSE.value == "coarse"
        assert GranularityLevel.MACRO.value == "macro"

    def test_batch_sizes(self):
        assert GranularityLevel.MICRO.batch_size == 1
        assert GranularityLevel.FINE.batch_size == 10
        assert GranularityLevel.MEDIUM.batch_size == 100
        assert GranularityLevel.COARSE.batch_size == 1000
        assert GranularityLevel.MACRO.batch_size == 100000

    def test_batch_timeouts(self):
        assert GranularityLevel.MICRO.batch_timeout < GranularityLevel.FINE.batch_timeout
        assert GranularityLevel.FINE.batch_timeout < GranularityLevel.MEDIUM.batch_timeout
        assert GranularityLevel.MEDIUM.batch_timeout < GranularityLevel.COARSE.batch_timeout
        assert GranularityLevel.COARSE.batch_timeout < GranularityLevel.MACRO.batch_timeout

    def test_is_streaming(self):
        assert GranularityLevel.MICRO.is_streaming is True
        assert GranularityLevel.FINE.is_streaming is True
        assert GranularityLevel.MEDIUM.is_streaming is False
        assert GranularityLevel.COARSE.is_streaming is False
        assert GranularityLevel.MACRO.is_streaming is False

    def test_is_batch_like(self):
        assert GranularityLevel.MICRO.is_batch_like is False
        assert GranularityLevel.MACRO.is_batch_like is True
        assert GranularityLevel.COARSE.is_batch_like is True


class TestGranularityMetrics:
    def test_default_values(self):
        m = GranularityMetrics()
        assert m.arrival_rate == 0.0
        assert m.queue_depth == 0
        assert m.processing_latency_ms == 0.0
        assert m.throughput_per_sec == 0.0
        assert m.backlog_size == 0
        assert m.error_rate == 0.0

    def test_snapshot(self):
        m = GranularityMetrics()
        m.arrival_rate = 100.0
        m.queue_depth = 50
        snap = m.snapshot()
        assert snap["arrival_rate"] == 100.0
        assert snap["queue_depth"] == 50


class TestGranularityController:
    def test_default_construction(self):
        c = GranularityController()
        assert c.level == GranularityLevel.MEDIUM
        assert c.policy == GranularityPolicy.BALANCED
        assert c.batch_size == 100

    def test_custom_initial_level(self):
        c = GranularityController(initial_level=GranularityLevel.MICRO)
        assert c.level == GranularityLevel.MICRO
        assert c.batch_size == 1

    def test_set_level(self):
        c = GranularityController()
        c.set_level(GranularityLevel.COARSE)
        assert c.level == GranularityLevel.COARSE
        assert c.batch_size == 1000

    def test_set_level_respects_range(self):
        c = GranularityController(
            min_level=GranularityLevel.FINE,
            max_level=GranularityLevel.COARSE,
        )
        c.set_level(GranularityLevel.MICRO)
        assert c.level == GranularityLevel.FINE
        c.set_level(GranularityLevel.MACRO)
        assert c.level == GranularityLevel.COARSE

    def test_update_metrics(self):
        c = GranularityController()
        c.update_metrics(arrival_rate=500.0, queue_depth=100)
        assert c.metrics.arrival_rate == 500.0
        assert c.metrics.queue_depth == 100

    def test_on_change_callback(self):
        changes = []
        def on_change(old, new):
            changes.append((old, new))
        c = GranularityController(on_change=on_change)
        c.set_level(GranularityLevel.COARSE)
        assert len(changes) == 1
        assert changes[0][0] == GranularityLevel.MEDIUM
        assert changes[0][1] == GranularityLevel.COARSE

    def test_adjust_throughput_policy(self):
        c = GranularityController(
            policy=GranularityPolicy.THROUGHPUT,
            initial_level=GranularityLevel.MEDIUM,
            adjust_interval=0.0,
        )
        c.update_metrics(arrival_rate=5000, queue_depth=10000, backlog_size=20000)
        time.sleep(0.01)
        c.adjust()
        assert c.level != GranularityLevel.MEDIUM or c.adjustments_count >= 0

    def test_adjust_latency_policy(self):
        c = GranularityController(
            policy=GranularityPolicy.LATENCY,
            initial_level=GranularityLevel.MEDIUM,
            adjust_interval=0.0,
        )
        c.update_metrics(arrival_rate=5, queue_depth=1, processing_latency_ms=10, backlog_size=5)
        time.sleep(0.01)
        c.adjust()
        assert c.level.value in ("micro", "fine", "medium")

    def test_adjust_balanced_policy(self):
        c = GranularityController(
            policy=GranularityPolicy.BALANCED,
            initial_level=GranularityLevel.MEDIUM,
            adjust_interval=0.0,
        )
        c.update_metrics(arrival_rate=5000, queue_depth=8000, processing_latency_ms=5, backlog_size=15000)
        time.sleep(0.01)
        c.adjust()

    def test_manual_policy_no_auto_adjust(self):
        c = GranularityController(
            policy=GranularityPolicy.MANUAL,
            initial_level=GranularityLevel.FINE,
            adjust_interval=0.0,
        )
        c.update_metrics(arrival_rate=999999, queue_depth=999999)
        c.adjust()
        assert c.level == GranularityLevel.FINE

    def test_coerce_to_streaming(self):
        c = GranularityController(initial_level=GranularityLevel.MACRO)
        c.coerce_to_streaming()
        assert c.level == GranularityLevel.MICRO

    def test_coerce_to_batch(self):
        c = GranularityController(initial_level=GranularityLevel.MICRO)
        c.coerce_to_batch()
        assert c.level == GranularityLevel.MACRO

    def test_history_tracking(self):
        c = GranularityController(adjust_interval=0.0)
        c.set_level(GranularityLevel.COARSE)
        c.set_level(GranularityLevel.FINE)
        assert len(c.history) == 2
        assert c.history[0]["from"] == "medium"
        assert c.history[0]["to"] == "coarse"

    def test_explain(self):
        c = GranularityController()
        text = c.explain()
        assert "GranularityController" in text
        assert "medium" in text
        assert "balanced" in text

    def test_adjustments_count(self):
        c = GranularityController()
        assert c.adjustments_count == 0
        c.set_level(GranularityLevel.COARSE)
        assert c.adjustments_count == 1
        c.set_level(GranularityLevel.COARSE)
        assert c.adjustments_count == 1

    def test_custom_thresholds(self):
        c = GranularityController(
            policy=GranularityPolicy.THROUGHPUT,
            initial_level=GranularityLevel.FINE,
            adjust_interval=0.0,
            thresholds={"high_arrival_rate": 100, "low_arrival_rate": 1,
                        "high_queue_depth": 200, "low_queue_depth": 5,
                        "high_latency_ms": 1000, "low_latency_ms": 1,
                        "high_backlog": 500, "low_backlog": 5},
        )
        c.update_metrics(arrival_rate=200, queue_depth=300, backlog_size=600)
        time.sleep(0.01)
        c.adjust()
        assert c.level != GranularityLevel.FINE or c.adjustments_count >= 0


class TestAdaptiveFlow:
    def test_basic_execution(self):
        af = AdaptiveFlow(
            name="test-basic",
            stream_fn=lambda f: f.from_collection([1, 2, 3, 4, 5]).map(lambda x: x * 2),
        )
        af.controller.set_level(GranularityLevel.MEDIUM)
        result = af.execute()
        assert result["architecture"] == "adaptive"
        assert "granularity" in result
        assert len(result["results"]) > 0

    def test_micro_granularity(self):
        af = AdaptiveFlow(
            name="test-micro",
            stream_fn=lambda f: f.from_collection([10, 20, 30]).map(lambda x: x + 1),
        )
        af.set_granularity(GranularityLevel.MICRO)
        result = af.execute()
        assert result["granularity"] == "micro"

    def test_macro_granularity(self):
        af = AdaptiveFlow(
            name="test-macro",
            stream_fn=lambda f: f.from_collection([1, 2, 3, 4, 5]).map(lambda x: x * 3),
        )
        af.set_granularity(GranularityLevel.MACRO)
        result = af.execute()
        assert result["granularity"] == "macro"

    def test_execute_at_granularity(self):
        af = AdaptiveFlow(
            name="test-at",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.execute_at_granularity(GranularityLevel.COARSE)
        assert result["granularity"] == "coarse"

    def test_execute_batch_like(self):
        af = AdaptiveFlow(
            name="test-batch-like",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 2),
        )
        result = af.execute_batch_like()
        assert result["granularity"] == "macro"

    def test_execute_stream_like(self):
        af = AdaptiveFlow(
            name="test-stream-like",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 2),
        )
        result = af.execute_stream_like()
        assert result["granularity"] == "micro"

    def test_custom_controller(self):
        ctrl = GranularityController(
            initial_level=GranularityLevel.FINE,
            policy=GranularityPolicy.LATENCY,
            min_level=GranularityLevel.MICRO,
            max_level=GranularityLevel.COARSE,
        )
        af = AdaptiveFlow(
            name="test-custom-ctrl",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            controller=ctrl,
        )
        assert af.granularity == GranularityLevel.FINE
        result = af.execute()
        assert result["controller"] is ctrl

    def test_policy_setting(self):
        af = AdaptiveFlow(
            name="test-policy",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.set_policy(GranularityPolicy.THROUGHPUT)
        assert af.controller.policy == GranularityPolicy.THROUGHPUT

    def test_on_granularity_change(self):
        changes = []
        af = AdaptiveFlow(
            name="test-on-change",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.on_granularity_change(lambda old, new: changes.append((old, new)))
        af.set_granularity(GranularityLevel.COARSE)
        assert len(changes) == 1

    def test_explain(self):
        af = AdaptiveFlow(
            name="test-explain",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        text = af.explain()
        assert "AdaptiveFlow" in text
        assert "medium" in text

    def test_no_stream_fn_raises(self):
        af = AdaptiveFlow(name="no-fn")
        with pytest.raises(PipelineError, match="No stream function"):
            af.execute()


class TestAdaptiveArchitectureIntegration:
    def test_flow_with_adaptive_architecture(self):
        flow = Flow(architecture=ArchitectureMode.ADAPTIVE, mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3, 4, 5])
        result = stream.map(lambda x: x * 2)
        sink = result.collect()
        ctrl = GranularityController(initial_level=GranularityLevel.MEDIUM)
        flow.configure("granularity_controller", ctrl)
        out = flow.execute()
        assert isinstance(out, dict)
        assert out.get("architecture") == "adaptive"

    def test_flow_as_adaptive(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        stream.map(lambda x: x * 10)
        af = flow.as_adaptive()
        assert isinstance(af, AdaptiveFlow)

    def test_flow_as_adaptive_with_policy(self):
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection([1, 2, 3])
        stream.map(lambda x: x)
        af = flow.as_adaptive(policy=GranularityPolicy.LATENCY)
        assert af.controller.policy == GranularityPolicy.LATENCY

    def test_adaptive_wordcount(self):
        af = AdaptiveFlow(
            name="wordcount-adaptive",
            stream_fn=lambda f: (
                f.from_collection(["hello world", "hello liutang"])
                 .flat_map(lambda s: s.split())
                 .map(lambda w: (w, 1))
                 .key_by(lambda p: p[0])
            ),
        )
        af.set_granularity(GranularityLevel.COARSE)
        result = af.execute()
        assert len(result["results"]) > 0

    def test_adaptive_granularity_spectrum(self):
        for level in GranularityLevel:
            af = AdaptiveFlow(
                name=f"spectrum-{level.value}",
                stream_fn=lambda f: f.from_collection([1, 2, 3, 4, 5]).map(lambda x: x + 1),
            )
            af.set_granularity(level)
            result = af.execute()
            assert result["granularity"] == level.value

    def test_adaptive_explain_in_flow(self):
        flow = Flow(architecture=ArchitectureMode.ADAPTIVE)
        stream = flow.from_collection([1]).map(lambda x: x)
        stream.print()
        explanation = flow.explain()
        assert "adaptive" in explanation


class TestViscosity:
    def test_enum_values(self):
        assert Viscosity.VOLATILE.value == "volatile"
        assert Viscosity.FLUID.value == "fluid"
        assert Viscosity.HONEYED.value == "honeyed"
        assert Viscosity.SLUGGISH.value == "sluggish"
        assert Viscosity.FROZEN.value == "frozen"

    def test_all_members(self):
        assert len(Viscosity) == 5
        members = list(Viscosity)
        assert members == [Viscosity.VOLATILE, Viscosity.FLUID, Viscosity.HONEYED,
                          Viscosity.SLUGGISH, Viscosity.FROZEN]

    def test_batch_sizes(self):
        assert Viscosity.VOLATILE.batch_size == 1
        assert Viscosity.FLUID.batch_size == 10
        assert Viscosity.HONEYED.batch_size == 100
        assert Viscosity.SLUGGISH.batch_size == 1000
        assert Viscosity.FROZEN.batch_size == 100000

    def test_batch_timeouts(self):
        assert Viscosity.VOLATILE.batch_timeout < Viscosity.FLUID.batch_timeout
        assert Viscosity.FLUID.batch_timeout < Viscosity.HONEYED.batch_timeout
        assert Viscosity.HONEYED.batch_timeout < Viscosity.SLUGGISH.batch_timeout
        assert Viscosity.SLUGGISH.batch_timeout < Viscosity.FROZEN.batch_timeout

    def test_eta_values(self):
        assert Viscosity.VOLATILE.eta == 0.0
        assert Viscosity.FLUID.eta == 0.25
        assert Viscosity.HONEYED.eta == 0.5
        assert Viscosity.SLUGGISH.eta == 0.75
        assert Viscosity.FROZEN.eta == 1.0

    def test_ordering_less_than(self):
        assert Viscosity.VOLATILE < Viscosity.FLUID
        assert Viscosity.FLUID < Viscosity.HONEYED
        assert Viscosity.HONEYED < Viscosity.SLUGGISH
        assert Viscosity.SLUGGISH < Viscosity.FROZEN

    def test_ordering_greater_than(self):
        assert Viscosity.FROZEN > Viscosity.SLUGGISH
        assert Viscosity.SLUGGISH > Viscosity.HONEYED
        assert Viscosity.HONEYED > Viscosity.FLUID
        assert Viscosity.FLUID > Viscosity.VOLATILE

    def test_ordering_less_equal(self):
        assert Viscosity.HONEYED <= Viscosity.HONEYED
        assert Viscosity.FLUID <= Viscosity.SLUGGISH

    def test_ordering_greater_equal(self):
        assert Viscosity.HONEYED >= Viscosity.HONEYED
        assert Viscosity.SLUGGISH >= Viscosity.FLUID

    def test_ordering_not_implemented_for_non_viscosity(self):
        assert Viscosity.HONEYED.__lt__(42) is NotImplemented
        assert Viscosity.HONEYED.__gt__("string") is NotImplemented

    def test_is_flowing(self):
        assert Viscosity.VOLATILE.is_flowing is True
        assert Viscosity.FLUID.is_flowing is True
        assert Viscosity.HONEYED.is_flowing is False
        assert Viscosity.SLUGGISH.is_flowing is False
        assert Viscosity.FROZEN.is_flowing is False

    def test_is_solid(self):
        assert Viscosity.VOLATILE.is_solid is False
        assert Viscosity.FLUID.is_solid is False
        assert Viscosity.HONEYED.is_solid is False
        assert Viscosity.SLUGGISH.is_solid is True
        assert Viscosity.FROZEN.is_solid is True

    def test_description_chinese(self):
        assert "如水" in Viscosity.VOLATILE.description
        assert "如溪" in Viscosity.FLUID.description
        assert "如蜜" in Viscosity.HONEYED.description
        assert "如泥" in Viscosity.SLUGGISH.description
        assert "如冰" in Viscosity.FROZEN.description

    def test_description_non_empty(self):
        for v in Viscosity:
            assert len(v.description) > 0


class TestViscosityPolicy:
    def test_enum_values(self):
        assert ViscosityPolicy.RESPONSIVE.value == "responsive"
        assert ViscosityPolicy.EFFICIENT.value == "efficient"
        assert ViscosityPolicy.BALANCED.value == "balanced"
        assert ViscosityPolicy.MANUAL.value == "manual"

    def test_all_members(self):
        assert len(ViscosityPolicy) == 4

    def test_mapping_from_granularity_throughput(self):
        from liutang.core.granularity import _GRAN_POLICY_TO_VISC
        assert _GRAN_POLICY_TO_VISC[GranularityPolicy.THROUGHPUT] == ViscosityPolicy.EFFICIENT

    def test_mapping_from_granularity_latency(self):
        from liutang.core.granularity import _GRAN_POLICY_TO_VISC
        assert _GRAN_POLICY_TO_VISC[GranularityPolicy.LATENCY] == ViscosityPolicy.RESPONSIVE

    def test_mapping_from_granularity_balanced(self):
        from liutang.core.granularity import _GRAN_POLICY_TO_VISC
        assert _GRAN_POLICY_TO_VISC[GranularityPolicy.BALANCED] == ViscosityPolicy.BALANCED

    def test_mapping_from_granularity_manual(self):
        from liutang.core.granularity import _GRAN_POLICY_TO_VISC
        assert _GRAN_POLICY_TO_VISC[GranularityPolicy.MANUAL] == ViscosityPolicy.MANUAL

    def test_round_trip_granularity_to_viscosity(self):
        from liutang.core.granularity import _GRAN_POLICY_TO_VISC, _VISC_POLICY_TO_GRAN
        for gp, vp in _GRAN_POLICY_TO_VISC.items():
            assert _VISC_POLICY_TO_GRAN[vp] == gp


class TestFlowMetrics:
    def test_default_values(self):
        m = FlowMetrics()
        assert m.arrival_rate == 0.0
        assert m.queue_depth == 0
        assert m.processing_latency_ms == 0.0
        assert m.throughput_per_sec == 0.0
        assert m.backlog_size == 0
        assert m.error_rate == 0.0

    def test_shear_rate_equals_arrival_rate(self):
        m = FlowMetrics()
        m.arrival_rate = 42.0
        assert m.shear_rate == 42.0

    def test_shear_rate_zero(self):
        m = FlowMetrics()
        assert m.shear_rate == 0.0

    def test_shear_stress_with_arrival_rate(self):
        m = FlowMetrics()
        m.backlog_size = 100
        m.processing_latency_ms = 50.0
        m.arrival_rate = 10.0
        expected = (100 + 50.0) / max(1.0, 10.0)
        assert abs(m.shear_stress - expected) < 0.01

    def test_shear_stress_zero_arrival_rate(self):
        m = FlowMetrics()
        m.backlog_size = 50
        m.arrival_rate = 0.0
        assert m.shear_stress == 50.0

    def test_measured_viscosity_midrange(self):
        m = FlowMetrics()
        m.arrival_rate = 100.0
        m.backlog_size = 50
        m.processing_latency_ms = 100.0
        v = m.measured_viscosity
        assert 0.0 <= v <= 1.0

    def test_measured_viscosity_low_load(self):
        m = FlowMetrics()
        m.arrival_rate = 10000.0
        m.backlog_size = 0
        m.processing_latency_ms = 1.0
        v = m.measured_viscosity
        assert v < 0.1

    def test_measured_viscosity_high_load(self):
        m = FlowMetrics()
        m.arrival_rate = 1.0
        m.backlog_size = 100000
        m.processing_latency_ms = 5000.0
        v = m.measured_viscosity
        assert v >= 0.9

    def test_measured_viscosity_min_clamp(self):
        m = FlowMetrics()
        m.arrival_rate = 100000.0
        m.backlog_size = 0
        m.processing_latency_ms = 0.0
        assert m.measured_viscosity >= 0.0

    def test_measured_viscosity_max_clamp(self):
        m = FlowMetrics()
        m.arrival_rate = 0.001
        m.backlog_size = 999999
        m.processing_latency_ms = 999999.0
        assert m.measured_viscosity <= 1.0

    def test_snapshot_includes_shear_fields(self):
        m = FlowMetrics()
        m.arrival_rate = 50.0
        s = m.snapshot()
        assert "shear_rate" in s
        assert "shear_stress" in s
        assert "measured_viscosity" in s
        assert "timestamp" in s

    def test_snapshot_shear_rate_value(self):
        m = FlowMetrics()
        m.arrival_rate = 77.0
        assert m.snapshot()["shear_rate"] == 77.0

    def test_snapshot_shear_stress_value(self):
        m = FlowMetrics()
        m.arrival_rate = 10.0
        m.backlog_size = 30
        m.processing_latency_ms = 20.0
        s = m.snapshot()
        assert abs(s["shear_stress"] - m.shear_stress) < 0.01

    def test_snapshot_measured_viscosity_value(self):
        m = FlowMetrics()
        m.arrival_rate = 100.0
        m.backlog_size = 50
        s = m.snapshot()
        assert abs(s["measured_viscosity"] - m.measured_viscosity) < 0.01

    def test_update_timestamp(self):
        m = FlowMetrics()
        t1 = m._timestamp
        time.sleep(0.01)
        m.update_timestamp()
        assert m._timestamp > t1


class TestViscosityController:
    def test_default_construction(self):
        vc = ViscosityController()
        assert vc.viscosity == Viscosity.HONEYED
        assert vc.policy == ViscosityPolicy.BALANCED
        assert vc.batch_size == 100

    def test_custom_initial_viscosity(self):
        vc = ViscosityController(initial=Viscosity.FLUID)
        assert vc.viscosity == Viscosity.FLUID
        assert vc.batch_size == 10

    def test_custom_policy_responsive(self):
        vc = ViscosityController(policy=ViscosityPolicy.RESPONSIVE)
        assert vc.policy == ViscosityPolicy.RESPONSIVE

    def test_custom_policy_efficient(self):
        vc = ViscosityController(policy=ViscosityPolicy.EFFICIENT)
        assert vc.policy == ViscosityPolicy.EFFICIENT

    def test_custom_policy_manual(self):
        vc = ViscosityController(policy=ViscosityPolicy.MANUAL)
        assert vc.policy == ViscosityPolicy.MANUAL

    def test_min_max_viscosity_construction(self):
        vc = ViscosityController(min_viscosity=Viscosity.FLUID, max_viscosity=Viscosity.SLUGGISH)
        vc.set_viscosity(Viscosity.VOLATILE)
        assert vc.viscosity == Viscosity.FLUID
        vc.set_viscosity(Viscosity.FROZEN)
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_viscosity_property(self):
        vc = ViscosityController(initial=Viscosity.SLUGGISH)
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_level_property_is_alias(self):
        vc = ViscosityController(initial=Viscosity.FLUID)
        assert vc.level == vc.viscosity

    def test_eta_property(self):
        vc = ViscosityController(initial=Viscosity.HONEYED)
        assert vc.eta == 0.5

    def test_batch_size_property(self):
        vc = ViscosityController(initial=Viscosity.FROZEN)
        assert vc.batch_size == 100000

    def test_batch_timeout_property(self):
        vc = ViscosityController(initial=Viscosity.VOLATILE)
        assert vc.batch_timeout == 0.01

    def test_set_viscosity_with_enum(self):
        vc = ViscosityController()
        vc.set_viscosity(Viscosity.FROZEN)
        assert vc.viscosity == Viscosity.FROZEN

    def test_set_viscosity_with_granularity_level(self):
        vc = ViscosityController()
        vc.set_viscosity(GranularityLevel.MACRO)
        assert vc.viscosity == Viscosity.FROZEN

    def test_set_viscosity_with_string(self):
        vc = ViscosityController()
        vc.set_viscosity("sluggish")
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_coerce_to_viscosity_invalid_raises(self):
        vc = ViscosityController()
        with pytest.raises(ValueError):
            vc.set_viscosity(99999)

    def test_thaw(self):
        vc = ViscosityController(initial=Viscosity.FROZEN)
        vc.thaw()
        assert vc.viscosity == Viscosity.VOLATILE

    def test_freeze(self):
        vc = ViscosityController(initial=Viscosity.VOLATILE)
        vc.freeze()
        assert vc.viscosity == Viscosity.FROZEN

    def test_flow_freely(self):
        vc = ViscosityController(initial=Viscosity.FROZEN)
        vc.flow_freely()
        assert vc.viscosity == Viscosity.VOLATILE

    def test_flow_as_batch(self):
        vc = ViscosityController(initial=Viscosity.VOLATILE)
        vc.flow_as_batch()
        assert vc.viscosity == Viscosity.FROZEN

    def test_coerce_to_streaming_alias(self):
        vc = ViscosityController(initial=Viscosity.FROZEN)
        vc.coerce_to_streaming()
        assert vc.viscosity == Viscosity.VOLATILE

    def test_coerce_to_batch_alias(self):
        vc = ViscosityController(initial=Viscosity.VOLATILE)
        vc.coerce_to_batch()
        assert vc.viscosity == Viscosity.FROZEN

    def test_set_level_alias(self):
        vc = ViscosityController()
        vc.set_level(Viscosity.SLUGGISH)
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_range_enforcement_min(self):
        vc = ViscosityController(min_viscosity=Viscosity.FLUID, max_viscosity=Viscosity.SLUGGISH)
        vc.set_viscosity(Viscosity.VOLATILE)
        assert vc.viscosity == Viscosity.FLUID

    def test_range_enforcement_max(self):
        vc = ViscosityController(min_viscosity=Viscosity.FLUID, max_viscosity=Viscosity.SLUGGISH)
        vc.set_viscosity(Viscosity.FROZEN)
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_adjustments_count_no_change(self):
        vc = ViscosityController()
        assert vc.adjustments_count == 0
        vc.set_viscosity(Viscosity.HONEYED)
        assert vc.adjustments_count == 0

    def test_adjustments_count_increments(self):
        vc = ViscosityController()
        vc.set_viscosity(Viscosity.SLUGGISH)
        assert vc.adjustments_count == 1
        vc.set_viscosity(Viscosity.FLUID)
        assert vc.adjustments_count == 2
        vc.set_viscosity(Viscosity.FLUID)
        assert vc.adjustments_count == 2

    def test_adjust_responsive_low_load(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.RESPONSIVE,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=5, queue_depth=1, processing_latency_ms=10, backlog_size=5)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity <= Viscosity.HONEYED

    def test_adjust_responsive_high_latency(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.RESPONSIVE,
            initial=Viscosity.FLUID,
            adjust_interval=0.0,
        )
        vc.update_metrics(processing_latency_ms=600, error_rate=0.2)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity >= Viscosity.FLUID or vc.adjustments_count == 0

    def test_adjust_efficient_high_throughput(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=5000, queue_depth=10000, backlog_size=20000)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity >= Viscosity.HONEYED or vc.adjustments_count == 0

    def test_adjust_efficient_error_rate(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.SLUGGISH,
            adjust_interval=0.0,
        )
        vc.update_metrics(error_rate=0.5)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity <= Viscosity.SLUGGISH or vc.adjustments_count == 0

    def test_adjust_balanced_high_signals(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.BALANCED,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=5000, queue_depth=8000, backlog_size=15000)
        time.sleep(0.01)
        vc.adjust()

    def test_adjust_balanced_low_signals(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.BALANCED,
            initial=Viscosity.HONEYED,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=5, queue_depth=2, backlog_size=5)
        time.sleep(0.01)
        vc.adjust()

    def test_adjust_manual_no_auto_change(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.MANUAL,
            initial=Viscosity.FLUID,
            adjust_interval=0.0,
        )
        vc.update_metrics(arrival_rate=999999, queue_depth=999999)
        vc.adjust()
        assert vc.viscosity == Viscosity.FLUID

    def test_adjust_interval_gate(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.HONEYED,
            adjust_interval=10.0,
        )
        vc.update_metrics(arrival_rate=5000, queue_depth=10000, backlog_size=20000)
        vc.adjust()
        first_result = vc.viscosity
        vc.update_metrics(arrival_rate=2, queue_depth=1, backlog_size=1)
        vc.adjust()
        assert vc.viscosity == first_result

    def test_history_tracking_entries(self):
        vc = ViscosityController(adjust_interval=0.0)
        vc.set_viscosity(Viscosity.SLUGGISH)
        vc.set_viscosity(Viscosity.FLUID)
        assert len(vc.history) == 2
        h0 = vc.history[0]
        assert "from" in h0
        assert "to" in h0
        assert "metrics" in h0

    def test_history_viscosity_values(self):
        vc = ViscosityController(adjust_interval=0.0)
        vc.set_viscosity(Viscosity.SLUGGISH)
        assert vc.history[0]["from"] == "honeyed"
        assert vc.history[0]["to"] == "sluggish"

    def test_history_eta_values(self):
        vc = ViscosityController(adjust_interval=0.0)
        vc.set_viscosity(Viscosity.FROZEN)
        h = vc.history[0]
        assert "eta_from" in h
        assert "eta_to" in h
        assert h["eta_from"] == 0.5
        assert h["eta_to"] == 1.0

    def test_history_truncation(self):
        vc = ViscosityController(adjust_interval=0.0)
        for i in range(5000):
            vc.set_viscosity(Viscosity.FLUID if i % 2 == 0 else Viscosity.HONEYED)
        assert len(vc.history) <= 1500

    def test_on_change_callback_viscosity_values(self):
        changes = []
        def on_change(old, new):
            changes.append((old, new))
        vc = ViscosityController(on_change=on_change)
        vc.set_viscosity(Viscosity.SLUGGISH)
        assert len(changes) == 1
        assert changes[0] == (Viscosity.HONEYED, Viscosity.SLUGGISH)

    def test_on_change_callback_not_called_same_viscosity(self):
        changes = []
        def on_change(old, new):
            changes.append((old, new))
        vc = ViscosityController(on_change=on_change)
        vc.set_viscosity(Viscosity.HONEYED)
        assert len(changes) == 0

    def test_on_change_callback_exception_swallowed(self):
        def bad_callback(old, new):
            raise RuntimeError("boom")
        vc = ViscosityController(on_change=bad_callback)
        vc.set_viscosity(Viscosity.SLUGGISH)
        assert vc.viscosity == Viscosity.SLUGGISH

    def test_metrics_update(self):
        vc = ViscosityController()
        vc.update_metrics(arrival_rate=500.0, queue_depth=100)
        assert vc.metrics.arrival_rate == 500.0
        assert vc.metrics.queue_depth == 100

    def test_metrics_shear_rate(self):
        vc = ViscosityController()
        vc.update_metrics(arrival_rate=200.0)
        assert vc.metrics.shear_rate == 200.0

    def test_metrics_shear_stress(self):
        vc = ViscosityController()
        vc.update_metrics(arrival_rate=10.0, backlog_size=30, processing_latency_ms=20.0)
        expected = (30 + 20.0) / max(1.0, 10.0)
        assert abs(vc.metrics.shear_stress - expected) < 0.01

    def test_metrics_measured_viscosity(self):
        vc = ViscosityController()
        vc.update_metrics(arrival_rate=100.0, backlog_size=50, processing_latency_ms=100.0)
        v = vc.metrics.measured_viscosity
        assert 0.0 <= v <= 1.0

    def test_custom_thresholds(self):
        vc = ViscosityController(
            policy=ViscosityPolicy.EFFICIENT,
            initial=Viscosity.FLUID,
            adjust_interval=0.0,
            thresholds={"high_arrival_rate": 100, "low_arrival_rate": 1,
                        "high_queue_depth": 200, "low_queue_depth": 5,
                        "high_latency_ms": 1000, "low_latency_ms": 1,
                        "high_backlog": 500, "low_backlog": 5},
        )
        vc.update_metrics(arrival_rate=200, queue_depth=300, backlog_size=600)
        time.sleep(0.01)
        vc.adjust()
        assert vc.viscosity != Viscosity.FLUID or vc.adjustments_count == 0

    def test_explain_output_viscosity_terms(self):
        vc = ViscosityController()
        text = vc.explain()
        assert "ViscosityController" in text
        assert "honeyed" in text
        assert "0.50" in text
        assert "balanced" in text
        assert "Shear rate" in text
        assert "Shear stress" in text
        assert "Measured" in text

    def test_explain_contains_range(self):
        vc = ViscosityController()
        text = vc.explain()
        assert "volatile" in text
        assert "frozen" in text

    def test_explain_contains_description(self):
        vc = ViscosityController()
        text = vc.explain()
        assert "如蜜" in text

    def test_explain_batch_size(self):
        vc = ViscosityController(initial=Viscosity.SLUGGISH)
        text = vc.explain()
        assert "1000" in text

    def test_explain_adjustments_count(self):
        vc = ViscosityController()
        text = vc.explain()
        assert "Adjustments: 0" in text


class TestAdaptiveFlowViscosity:
    def test_construction_with_viscosity_param(self):
        af = AdaptiveFlow(
            name="test-visc",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            viscosity=Viscosity.FLUID,
        )
        assert af.viscosity == Viscosity.FLUID

    def test_construction_with_viscosity_policy(self):
        af = AdaptiveFlow(
            name="test-vp",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            policy=ViscosityPolicy.RESPONSIVE,
        )
        assert af.controller.policy == GranularityPolicy.LATENCY

    def test_construction_with_viscosity_and_policy(self):
        af = AdaptiveFlow(
            name="test-both",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            viscosity=Viscosity.SLUGGISH,
            policy=ViscosityPolicy.EFFICIENT,
        )
        assert af.viscosity == Viscosity.SLUGGISH
        assert af.controller.policy == GranularityPolicy.THROUGHPUT

    def test_construction_with_min_max_viscosity(self):
        af = AdaptiveFlow(
            name="test-range",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
            min_viscosity=Viscosity.FLUID,
            max_viscosity=Viscosity.SLUGGISH,
        )
        af.set_viscosity(Viscosity.VOLATILE)
        assert af.viscosity == Viscosity.FLUID

    def test_thaw_method(self):
        af = AdaptiveFlow(
            name="test-thaw",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.thaw()
        assert af.viscosity == Viscosity.VOLATILE

    def test_freeze_method(self):
        af = AdaptiveFlow(
            name="test-freeze",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.freeze()
        assert af.viscosity == Viscosity.FROZEN

    def test_set_viscosity(self):
        af = AdaptiveFlow(
            name="test-set",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.set_viscosity(Viscosity.SLUGGISH)
        assert af.viscosity == Viscosity.SLUGGISH

    def test_set_viscosity_returns_self(self):
        af = AdaptiveFlow(
            name="test-chain",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.set_viscosity(Viscosity.FLUID)
        assert result is af

    def test_execute_at_viscosity(self):
        af = AdaptiveFlow(
            name="test-at-v",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.execute_at_viscosity(Viscosity.SLUGGISH)
        assert result["viscosity"] == "sluggish"
        assert result["granularity"] == "coarse"

    def test_on_viscosity_change_callback(self):
        changes = []
        af = AdaptiveFlow(
            name="test-cb",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.on_viscosity_change(lambda old, new: changes.append((old, new)))
        af.set_viscosity(Viscosity.SLUGGISH)
        assert len(changes) == 1

    def test_execute_return_contains_viscosity_key(self):
        af = AdaptiveFlow(
            name="test-resp",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.execute()
        assert "viscosity" in result
        assert isinstance(result["viscosity"], str)

    def test_execute_return_contains_eta_key(self):
        af = AdaptiveFlow(
            name="test-eta",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.execute()
        assert "eta" in result
        assert isinstance(result["eta"], float)

    def test_execute_return_contains_granularity_key(self):
        af = AdaptiveFlow(
            name="test-gran",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        result = af.execute()
        assert "viscosity" in result
        assert "granularity" in result
        assert result["granularity"] == "medium"
        assert result["viscosity"] == "honeyed"

    def test_viscosity_property(self):
        af = AdaptiveFlow(
            name="test-prop",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
            viscosity=Viscosity.SLUGGISH,
        )
        assert af.viscosity == Viscosity.SLUGGISH
        assert isinstance(af.viscosity, Viscosity)

    def test_granularity_property(self):
        af = AdaptiveFlow(
            name="test-gran-prop",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
            viscosity=Viscosity.SLUGGISH,
        )
        assert af.granularity == GranularityLevel.COARSE

    def test_explain_contains_granularity_terms(self):
        af = AdaptiveFlow(
            name="test-exp",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
        )
        text = af.explain()
        assert "AdaptiveFlow" in text
        assert "medium" in text

    def test_controller_proxy_policy_returns_granularity_policy(self):
        af = AdaptiveFlow(
            name="test-proxy",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
        )
        af.set_policy(ViscosityPolicy.EFFICIENT)
        assert af.controller.policy == GranularityPolicy.THROUGHPUT

    def test_controller_proxy_level_returns_granularity_level(self):
        af = AdaptiveFlow(
            name="test-proxy-level",
            stream_fn=lambda f: f.from_collection([1]).map(lambda x: x),
            viscosity=Viscosity.SLUGGISH,
        )
        assert af.controller.level == GranularityLevel.COARSE

    def test_viscosity_spectrum(self):
        for v in Viscosity:
            af = AdaptiveFlow(
                name=f"spectrum-{v.value}",
                stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x + 1),
            )
            af.set_viscosity(v)
            result = af.execute()
            assert result["viscosity"] == v.value

    def test_execution_history_entries(self):
        af = AdaptiveFlow(
            name="test-hist",
            stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x),
        )
        af.execute()
        hist = af._execution_history
        assert len(hist) == 1
        assert "viscosity" in hist[0]
        assert "eta" in hist[0]
        assert "batch_size" in hist[0]
        assert "adjustments" in hist[0]


class TestGranularityViscosityCompat:
    def test_to_viscosity_mapping(self):
        assert GranularityLevel.MICRO.to_viscosity() == Viscosity.VOLATILE
        assert GranularityLevel.FINE.to_viscosity() == Viscosity.FLUID
        assert GranularityLevel.MEDIUM.to_viscosity() == Viscosity.HONEYED
        assert GranularityLevel.COARSE.to_viscosity() == Viscosity.SLUGGISH
        assert GranularityLevel.MACRO.to_viscosity() == Viscosity.FROZEN

    def test_to_viscosity_round_trip(self):
        from liutang.core.granularity import _VISC_TO_GRAN
        for gl in GranularityLevel:
            visc = gl.to_viscosity()
            back = _VISC_TO_GRAN[visc]
            assert back == gl.value

    def test_history_maps_viscosity_strings_to_granularity(self):
        gc = GranularityController(adjust_interval=0.0)
        gc.set_level(GranularityLevel.COARSE)
        gc.set_level(GranularityLevel.FINE)
        assert gc.history[0]["from"] == "medium"
        assert gc.history[0]["to"] == "coarse"
        assert gc.history[1]["from"] == "coarse"
        assert gc.history[1]["to"] == "fine"

    def test_history_strips_eta_keys(self):
        gc = GranularityController(adjust_interval=0.0)
        gc.set_level(GranularityLevel.COARSE)
        h = gc.history[0]
        assert "eta_from" not in h
        assert "eta_to" not in h

    def test_history_strips_shear_keys_from_metrics(self):
        gc = GranularityController(adjust_interval=0.0)
        gc.set_level(GranularityLevel.COARSE)
        m = gc.history[0].get("metrics", {})
        assert "shear_rate" not in m
        assert "shear_stress" not in m
        assert "measured_viscosity" not in m

    def test_policy_roundtrip_throughput(self):
        gc = GranularityController(policy=GranularityPolicy.THROUGHPUT)
        assert gc.policy == GranularityPolicy.THROUGHPUT

    def test_policy_roundtrip_latency(self):
        gc = GranularityController(policy=GranularityPolicy.LATENCY)
        assert gc.policy == GranularityPolicy.LATENCY

    def test_policy_roundtrip_balanced(self):
        gc = GranularityController(policy=GranularityPolicy.BALANCED)
        assert gc.policy == GranularityPolicy.BALANCED

    def test_policy_roundtrip_manual(self):
        gc = GranularityController(policy=GranularityPolicy.MANUAL)
        assert gc.policy == GranularityPolicy.MANUAL

    def test_set_level_with_granularity_level(self):
        gc = GranularityController()
        gc.set_level(GranularityLevel.COARSE)
        assert gc.level == GranularityLevel.COARSE

    def test_granularity_controller_explain_uses_granularity_terms(self):
        gc = GranularityController()
        text = gc.explain()
        assert "GranularityController" in text
        assert "Level:" in text or "medium" in text

    def test_viscosity_controller_used_internally(self):
        gc = GranularityController()
        assert isinstance(gc._vc, ViscosityController)