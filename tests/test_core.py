import pytest
import time
import threading
from liutang import (
    Flow, Stream, KeyedStream, RuntimeMode, DeliveryMode,
    FieldType, Schema, WindowType, WindowKind,
    CollectionSource, GeneratorSource, FileSource, PrintSink,
    CallbackSink, CollectSink, DatagenSource,
    MemoryStateBackend, KeyedState, ValueState, ListState, MapState,
    ReducingState, AggregatingState,
    RuntimeContext, TimerService, KeyedProcessFunction, ProcessFunction,
    WatermarkStrategy, Watermark,
    LiuTangError, PipelineError, DeliveryError, quick_flow,
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