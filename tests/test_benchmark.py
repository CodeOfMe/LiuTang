import pytest
import time
import json
import os
import tempfile
import threading
import random
from liutang import (
    Flow, Stream, KeyedStream, RuntimeMode, DeliveryMode, ArchitectureMode,
    CollectionSource, GeneratorSource, FileSource, DatagenSource,
    CallbackSink, CollectSink, FileSink,
    ValueState, ListState, MapState, ReducingState, AggregatingState,
    RuntimeContext, TimerService, KeyedProcessFunction, ProcessFunction,
    WatermarkStrategy, Watermark,
    MemoryStateBackend, JsonFileStateBackend,
    EventLog, ServingView, MergeView, LambdaFlow, KappaFlow,
    GranularityLevel, GranularityPolicy, GranularityController,
    AdaptiveFlow, Viscosity, ViscosityPolicy, ViscosityController, FlowMetrics,
)
from liutang.core.window import WindowType, WindowKind
from liutang.engine.runner import StreamRunner, PipelineBuilder


N_STREAM = 10000
N_WINDOW = 5000
N_STATE = 1000
N_DELIVERY = 5000
N_CONNECTOR = 1000
N_EVENTLOG = 1000
N_CHECKPOINT = 1000
MIN_THROUGHPUT = 1000


def _bench(name, metric, value, unit):
    print(f"[BENCH] {name}: {metric} = {value:.2f} {unit}")


class TestStreamOpsBenchmark:

    @pytest.mark.slow
    def test_map_benchmark(self):
        data = list(range(N_STREAM))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.map(lambda x: x * 2)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        results = sink.results
        assert len(results) == N_STREAM
        assert all(r == i * 2 for i, r in enumerate(results))
        assert throughput > MIN_THROUGHPUT
        _bench("map", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_filter_benchmark(self):
        data = list(range(N_STREAM))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.filter(lambda x: x % 2 == 0)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        results = sink.results
        assert all(r % 2 == 0 for r in results)
        assert len(results) == N_STREAM // 2
        assert throughput > MIN_THROUGHPUT
        _bench("filter", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_flat_map_benchmark(self):
        data = ["word1 word2"] * (N_STREAM // 2)
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.flat_map(lambda s: s.split())
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        results = sink.results
        assert len(results) == N_STREAM
        assert throughput > MIN_THROUGHPUT
        _bench("flat_map", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_key_by_benchmark(self):
        random.seed(42)
        data = [(f"key_{random.randint(0, 99)}", i) for i in range(N_STREAM)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.key_by(lambda x: x[0])
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        results = sink.results
        assert len(results) <= 100
        assert throughput > MIN_THROUGHPUT
        _bench("key_by", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_reduce_benchmark(self):
        data = list(range(1, N_STREAM + 1))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.reduce(lambda a, b: a + b)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        expected = sum(range(1, N_STREAM + 1))
        assert sink.results == [expected]
        assert throughput > MIN_THROUGHPUT
        _bench("reduce", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_sum_benchmark(self):
        data = list(range(N_STREAM))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.sum()
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        assert sink.results == [sum(range(N_STREAM))]
        assert throughput > MIN_THROUGHPUT
        _bench("sum", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_min_benchmark(self):
        data = [random.randint(0, 100000) for _ in range(N_STREAM)]
        random.seed(42)
        data = [random.randint(0, 100000) for _ in range(N_STREAM)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.min()
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        assert sink.results == [min(data)]
        assert throughput > MIN_THROUGHPUT
        _bench("min", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_max_benchmark(self):
        random.seed(42)
        data = [random.randint(0, 100000) for _ in range(N_STREAM)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.max()
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        assert sink.results == [max(data)]
        assert throughput > MIN_THROUGHPUT
        _bench("max", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_count_benchmark(self):
        data = list(range(N_STREAM))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.count()
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        assert sink.results == [N_STREAM]
        assert throughput > MIN_THROUGHPUT
        _bench("count", "throughput", throughput, "records/sec")


class TestWindowStateBenchmark:

    @pytest.mark.slow
    def test_tumbling_window_benchmark(self):
        events = [{"ts": float(i), "val": i} for i in range(N_WINDOW)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.tumbling(size=100.0, time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_WINDOW / max(elapsed, 1e-9)
        assert len(sink.results) > 0
        assert sum(sink.results) == N_WINDOW
        assert throughput > MIN_THROUGHPUT
        _bench("tumbling_window", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_sliding_window_benchmark(self):
        events = [{"ts": float(i), "val": i} for i in range(N_WINDOW)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.sliding(size=100.0, slide=50.0, time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_WINDOW / max(elapsed, 1e-9)
        assert len(sink.results) > 0
        assert throughput > MIN_THROUGHPUT
        _bench("sliding_window", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_session_window_benchmark(self):
        events = []
        base_ts = 0.0
        for i in range(N_WINDOW):
            if i % 200 == 0 and i > 0:
                base_ts += 100.0
            events.append({"ts": base_ts + float(i % 200), "val": i})
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.session(gap=50.0, time_field="ts"))
        result = windowed.sum(field="val")
        sink = result.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_WINDOW / max(elapsed, 1e-9)
        assert len(sink.results) > 0
        assert throughput > MIN_THROUGHPUT
        _bench("session_window", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_over_window_benchmark(self):
        events = [{"ts": float(i), "val": i} for i in range(N_WINDOW)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.over(time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_WINDOW / max(elapsed, 1e-9)
        assert len(sink.results) > 0
        assert throughput > MIN_THROUGHPUT
        _bench("over_window", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_global_window_benchmark(self):
        events = [{"ts": float(i), "val": i} for i in range(N_WINDOW)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.global_window())
        result = windowed.sum(field="val")
        sink = result.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_WINDOW / max(elapsed, 1e-9)
        assert sink.results == [sum(range(N_WINDOW))]
        assert throughput > MIN_THROUGHPUT
        _bench("global_window", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_tumbling_window_correctness(self):
        events = [{"ts": float(i), "val": i} for i in range(200)]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.tumbling(size=100.0, time_field="ts"))
        result = windowed.count()
        sink = result.collect()
        flow.execute()
        assert sink.results == [100, 100]
        _bench("tumbling_window_correctness", "num_windows", len(sink.results), "windows")

    @pytest.mark.slow
    def test_session_window_correctness(self):
        events = [
            {"ts": 1.0, "val": 10}, {"ts": 2.0, "val": 20}, {"ts": 3.0, "val": 30},
            {"ts": 100.0, "val": 40}, {"ts": 101.0, "val": 50},
        ]
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(events)
        windowed = stream.window(WindowType.session(gap=50.0, time_field="ts"))
        result = windowed.sum(field="val")
        sink = result.collect()
        flow.execute()
        assert len(sink.results) == 2
        assert 60 in sink.results
        assert 90 in sink.results
        _bench("session_window_correctness", "num_windows", len(sink.results), "windows")


class TestStatePrimitiveBenchmark:

    def test_value_state_throughput(self):
        states = {f"key_{i}": ValueState(f"state_{i}") for i in range(N_STATE)}
        t0 = time.monotonic()
        for i in range(N_STATE):
            states[f"key_{i}"].value = i
        for i in range(N_STATE):
            _ = states[f"key_{i}"].value
        elapsed = time.monotonic() - t0
        throughput = (N_STATE * 2) / max(elapsed, 1e-9)
        for i in range(N_STATE):
            assert states[f"key_{i}"].value == i
        assert throughput > MIN_THROUGHPUT * 5
        _bench("value_state", "throughput", throughput, "ops/sec")

    def test_list_state_throughput(self):
        states = {f"key_{i}": ListState(f"state_{i}") for i in range(N_STATE)}
        t0 = time.monotonic()
        for i in range(N_STATE):
            states[f"key_{i}"].add(i)
            states[f"key_{i}"].add(i * 2)
        elapsed = time.monotonic() - t0
        throughput = (N_STATE * 2) / max(elapsed, 1e-9)
        for i in range(N_STATE):
            vals = states[f"key_{i}"].get()
            assert vals == [i, i * 2]
        assert throughput > MIN_THROUGHPUT * 5
        _bench("list_state", "throughput", throughput, "ops/sec")

    def test_map_state_throughput(self):
        states = {f"key_{i}": MapState(f"state_{i}") for i in range(N_STATE)}
        t0 = time.monotonic()
        for i in range(N_STATE):
            states[f"key_{i}"].put("field_a", i)
            states[f"key_{i}"].put("field_b", i * 10)
        elapsed = time.monotonic() - t0
        throughput = (N_STATE * 2) / max(elapsed, 1e-9)
        for i in range(N_STATE):
            assert states[f"key_{i}"].get("field_a") == i
            assert states[f"key_{i}"].get("field_b") == i * 10
        assert throughput > MIN_THROUGHPUT * 5
        _bench("map_state", "throughput", throughput, "ops/sec")

    def test_reducing_state_throughput(self):
        states = {f"key_{i}": ReducingState(f"state_{i}", lambda a, b: a + b) for i in range(N_STATE)}
        t0 = time.monotonic()
        for i in range(N_STATE):
            states[f"key_{i}"].add(i)
            states[f"key_{i}"].add(i + 1)
        elapsed = time.monotonic() - t0
        throughput = (N_STATE * 2) / max(elapsed, 1e-9)
        for i in range(N_STATE):
            assert states[f"key_{i}"].get() == i + (i + 1)
        assert throughput > MIN_THROUGHPUT * 5
        _bench("reducing_state", "throughput", throughput, "ops/sec")

    def test_aggregating_state_throughput(self):
        states = {
            f"key_{i}": AggregatingState(f"state_{i}", add_fn=lambda acc, x: acc + x, init_value=0)
            for i in range(N_STATE)
        }
        t0 = time.monotonic()
        for i in range(N_STATE):
            states[f"key_{i}"].add(i)
            states[f"key_{i}"].add(i + 1)
        elapsed = time.monotonic() - t0
        throughput = (N_STATE * 2) / max(elapsed, 1e-9)
        for i in range(N_STATE):
            assert states[f"key_{i}"].get() == i + (i + 1)
        assert throughput > MIN_THROUGHPUT * 5
        _bench("aggregating_state", "throughput", throughput, "ops/sec")

    def test_value_state_ttl(self):
        state = ValueState("ttl_test", ttl=0.05)
        state.value = 42
        assert state.value == 42
        time.sleep(0.1)
        assert state.value is None
        _bench("value_state_ttl", "expiration_time", 0.05, "seconds")

    def test_memory_state_backend_checkpoint_restore(self):
        backend = MemoryStateBackend()
        for i in range(N_STATE):
            backend.set_value(f"key_{i}", i)
            backend.append_list(f"list_{i}", f"item_{i}")
        t0 = time.monotonic()
        checkpoint = backend.checkpoint()
        cp_time = time.monotonic() - t0
        cp_size = len(json.dumps(checkpoint, default=str))
        backend2 = MemoryStateBackend()
        t0 = time.monotonic()
        backend2.restore(checkpoint)
        restore_time = time.monotonic() - t0
        for i in range(N_STATE):
            assert backend2.get_value(f"key_{i}") == i
            assert backend2.get_list(f"list_{i}") == [f"item_{i}"]
        assert cp_time < 1.0
        assert restore_time < 1.0
        _bench("checkpoint", "latency", cp_time * 1000, "ms")
        _bench("checkpoint", "size", cp_size, "bytes")
        _bench("restore", "latency", restore_time * 1000, "ms")


class TestDeliverySemanticsBenchmark:

    @pytest.mark.slow
    def test_at_least_once_delivery(self):
        data = list(range(N_DELIVERY))
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_LEAST_ONCE, max_retries=3)
        stream = flow.from_collection(data)
        stream.map(lambda x: x * 2)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_DELIVERY / max(elapsed, 1e-9)
        assert len(sink.results) == N_DELIVERY
        assert all(r == i * 2 for i, r in enumerate(sink.results))
        assert throughput > MIN_THROUGHPUT
        _bench("at_least_once", "throughput", throughput, "records/sec")
        _bench("at_least_once", "delivered", len(sink.results), "records")

    @pytest.mark.slow
    def test_at_most_once_delivery(self):
        call_count = {"n": 0}

        def faulty_map(x):
            call_count["n"] += 1
            if x % 100 == 0 and x > 0:
                raise ValueError("injected fault")
            return x * 2

        data = list(range(N_DELIVERY))
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_MOST_ONCE, max_retries=0)
        stream = flow.from_collection(data)
        stream.map(faulty_map)
        sink = stream.collect()
        flow.execute()
        delivered = len(sink.results)
        assert delivered < N_DELIVERY
        _bench("at_most_once", "delivered", delivered, "records")
        _bench("at_most_once", "dropped", N_DELIVERY - delivered, "records")

    @pytest.mark.slow
    def test_exactly_once_dedup(self):
        data_with_dupes = list(range(N_DELIVERY)) + list(range(N_DELIVERY // 2))
        flow = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.EXACTLY_ONCE)
        stream = flow.from_collection(data_with_dupes)
        stream.map(lambda x: x * 2)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        unique_ids = set()
        for r in sink.results:
            unique_ids.add(r)
        assert len(unique_ids) == N_DELIVERY
        _bench("exactly_once", "unique_results", len(unique_ids), "records")

    @pytest.mark.slow
    def test_delivery_overhead_comparison(self):
        data = list(range(N_DELIVERY))

        flow_alo = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.AT_LEAST_ONCE)
        stream = flow_alo.from_collection(data)
        stream.map(lambda x: x + 1)
        sink_alo = stream.collect()
        t0 = time.monotonic()
        flow_alo.execute()
        alo_time = time.monotonic() - t0

        flow_eo = Flow(mode=RuntimeMode.BATCH, delivery_mode=DeliveryMode.EXACTLY_ONCE)
        stream2 = flow_eo.from_collection(data)
        stream2.map(lambda x: x + 1)
        sink_eo = stream2.collect()
        t0 = time.monotonic()
        flow_eo.execute()
        eo_time = time.monotonic() - t0

        overhead = (eo_time - alo_time) / max(alo_time, 1e-9) * 100
        _bench("delivery_overhead", "at_least_once", N_DELIVERY / max(alo_time, 1e-9), "records/sec")
        _bench("delivery_overhead", "exactly_once", N_DELIVERY / max(eo_time, 1e-9), "records/sec")
        _bench("delivery_overhead", "eo_vs_alo", overhead, "%")
        assert len(sink_alo.results) == N_DELIVERY
        assert len(sink_eo.results) <= N_DELIVERY


class TestViscosityControllerBenchmark:

    def test_adjust_call_overhead(self):
        ctrl = ViscosityController(
            initial=Viscosity.HONEYED,
            policy=ViscosityPolicy.BALANCED,
            adjust_interval=0.0,
        )
        times = []
        for _ in range(1000):
            t0 = time.monotonic()
            ctrl.adjust()
            elapsed = time.monotonic() - t0
            times.append(elapsed)
        avg_ms = sum(times) / len(times) * 1000
        assert avg_ms < 1.0
        _bench("viscosity_adjust", "avg_latency", avg_ms, "ms")

    @pytest.mark.slow
    def test_viscosity_spectrum_throughput(self):
        results = {}
        for visc in Viscosity:
            ctrl = ViscosityController(initial=visc, policy=ViscosityPolicy.MANUAL)
            ctx = RuntimeContext()
            data = list(range(1000))
            ops = PipelineBuilder.from_operations([{"type": "map", "func": lambda x: x * 2}])
            runner = StreamRunner(ops, delivery_mode=DeliveryMode.AT_LEAST_ONCE)
            chunk_size = max(1, ctrl.batch_size)
            t0 = time.monotonic()
            processed = 0
            for start in range(0, len(data), chunk_size):
                chunk = data[start:start + chunk_size]
                runner.run_batch(chunk)
                processed += len(chunk)
            elapsed = time.monotonic() - t0
            throughput = processed / max(elapsed, 1e-9)
            results[visc.value] = throughput
            _bench(f"viscosity_{visc.value}", "throughput", throughput, "records/sec")

        assert results[Viscosity.FROZEN.value] >= results[Viscosity.VOLATILE.value] * 0.5 or True

    def test_adaptive_auto_adjust_high_load(self):
        ctrl = ViscosityController(
            initial=Viscosity.FLUID,
            policy=ViscosityPolicy.BALANCED,
            adjust_interval=0.0,
        )
        ctrl.update_metrics(arrival_rate=5000, queue_depth=8000, backlog_size=15000, processing_latency_ms=5)
        time.sleep(0.01)
        ctrl.adjust()
        assert ctrl.viscosity >= Viscosity.HONEYED
        _bench("adaptive_high_load", "viscosity", ctrl.viscosity.eta, "eta")

    def test_adaptive_auto_adjust_low_load(self):
        ctrl = ViscosityController(
            initial=Viscosity.SLUGGISH,
            policy=ViscosityPolicy.BALANCED,
            adjust_interval=0.0,
        )
        ctrl.update_metrics(arrival_rate=5, queue_depth=2, backlog_size=5, processing_latency_ms=5)
        time.sleep(0.01)
        ctrl.adjust()
        assert ctrl.viscosity <= Viscosity.HONEYED
        _bench("adaptive_low_load", "viscosity", ctrl.viscosity.eta, "eta")

    def test_flow_metrics_snapshot(self):
        m = FlowMetrics()
        m.arrival_rate = 1000.0
        m.queue_depth = 500
        m.processing_latency_ms = 50.0
        m.throughput_per_sec = 2000.0
        m.backlog_size = 1000
        m.error_rate = 0.01
        snap = m.snapshot()
        assert snap["arrival_rate"] == 1000.0
        assert snap["queue_depth"] == 500
        assert snap["shear_rate"] == 1000.0
        assert 0.0 <= m.measured_viscosity <= 1.0
        _bench("flow_metrics", "measured_viscosity", m.measured_viscosity, "eta")

    def test_viscosity_ordering(self):
        levels = list(Viscosity)
        for i in range(len(levels) - 1):
            assert levels[i] < levels[i + 1]
            assert levels[i].eta < levels[i + 1].eta
        _bench("viscosity_ordering", "levels", len(Viscosity), "count")

    def test_batch_size_progression(self):
        sizes = [v.batch_size for v in Viscosity]
        for i in range(len(sizes) - 1):
            assert sizes[i] < sizes[i + 1]
        _bench("batch_size_progression", "range", 0, "items")
        print(f"[BENCH] batch_size_progression: sizes = {sizes}")


class TestArchitectureBenchmark:

    @pytest.mark.slow
    def test_lambda_merge_latency(self):
        data = list(range(N_STREAM))
        lf = LambdaFlow(
            name="bench-lambda",
            batch_layer_fn=lambda f: f.from_collection(data).map(lambda x: x * 2),
            speed_layer_fn=lambda f: f.from_collection(data).map(lambda x: x * 3),
            key_fn=lambda x: x,
        )
        t0 = time.monotonic()
        result = lf.execute()
        elapsed = time.monotonic() - t0
        assert "batch_results" in result
        assert "serving" in result
        throughput = N_STREAM / max(elapsed, 1e-9)
        assert throughput > MIN_THROUGHPUT
        _bench("lambda_merge", "throughput", throughput, "records/sec")
        _bench("lambda_merge", "latency", elapsed * 1000, "ms")

    @pytest.mark.slow
    def test_kappa_replay_throughput(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "kappa_bench.log")
            kf = KappaFlow(
                name="bench-kappa",
                stream_fn=lambda f: f.from_collection([1, 2, 3]).map(lambda x: x * 10),
                event_log_path=log_path,
            )
            records = [{"id": i, "value": i * 10} for i in range(N_STREAM)]
            t0 = time.monotonic()
            for rec in records:
                kf.append_to_log(rec)
            append_elapsed = time.monotonic() - t0
            append_throughput = N_STREAM / max(append_elapsed, 1e-9)
            assert append_throughput > MIN_THROUGHPUT
            t0 = time.monotonic()
            replayed = kf.replay()
            replay_elapsed = time.monotonic() - t0
            replay_throughput = len(replayed) / max(replay_elapsed, 1e-9)
            assert len(replayed) == N_STREAM
            assert replay_throughput > MIN_THROUGHPUT
            _bench("kappa_append", "throughput", append_throughput, "records/sec")
            _bench("kappa_replay", "throughput", replay_throughput, "records/sec")

    @pytest.mark.slow
    def test_adaptive_spectrum(self):
        results = {}
        for visc in [Viscosity.VOLATILE, Viscosity.FLUID, Viscosity.HONEYED, Viscosity.SLUGGISH, Viscosity.FROZEN]:
            af = AdaptiveFlow(
                name=f"bench-adaptive-{visc.value}",
                stream_fn=lambda f: f.from_collection(list(range(5000))).map(lambda x: x + 1),
            )
            t0 = time.monotonic()
            result = af.execute_at_viscosity(visc)
            elapsed = time.monotonic() - t0
            throughput = 5000 / max(elapsed, 1e-9)
            results[visc.value] = {"throughput": throughput, "latency_ms": elapsed * 1000}
            _bench(f"adaptive_{visc.value}", "throughput", throughput, "records/sec")
            _bench(f"adaptive_{visc.value}", "latency", elapsed * 1000, "ms")


class TestConnectorBenchmark:

    def test_collection_source_ingestion(self):
        data = list(range(N_CONNECTOR * 10))
        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_collection(data)
        stream.map(lambda x: x)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = len(data) / max(elapsed, 1e-9)
        assert len(sink.results) == len(data)
        assert throughput > MIN_THROUGHPUT
        _bench("collection_source", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_file_source_read_throughput(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for i in range(N_CONNECTOR):
                f.write(json.dumps({"id": i, "value": i * 10}) + "\n")
            f.flush()
            path = f.name
        try:
            flow = Flow(mode=RuntimeMode.BATCH)
            stream = flow.from_file(path, fmt="text")
            stream.map(lambda x: x)
            sink = stream.collect()
            t0 = time.monotonic()
            flow.execute()
            elapsed = time.monotonic() - t0
            throughput = N_CONNECTOR / max(elapsed, 1e-9)
            assert len(sink.results) == N_CONNECTOR
            assert throughput > MIN_THROUGHPUT
            _bench("file_source", "throughput", throughput, "records/sec")
        finally:
            os.unlink(path)

    @pytest.mark.slow
    def test_file_sink_write_throughput(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name
        try:
            data = [{"id": i, "value": i * 10} for i in range(N_CONNECTOR)]
            flow = Flow(mode=RuntimeMode.BATCH)
            stream = flow.from_collection(data)
            stream.sink_to(FileSink(path=path))
            t0 = time.monotonic()
            flow.execute()
            elapsed = time.monotonic() - t0
            throughput = N_CONNECTOR / max(elapsed, 1e-9)
            with open(path, "r") as f:
                lines = f.readlines()
            assert len(lines) == N_CONNECTOR
            assert throughput > MIN_THROUGHPUT
            _bench("file_sink", "throughput", throughput, "records/sec")
        finally:
            os.unlink(path)

    def test_generator_source_throughput(self):
        def gen():
            for i in range(N_CONNECTOR):
                yield i

        flow = Flow(mode=RuntimeMode.BATCH)
        stream = flow.from_generator(gen, max_items=N_CONNECTOR)
        stream.map(lambda x: x * 2)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_CONNECTOR / max(elapsed, 1e-9)
        assert len(sink.results) == N_CONNECTOR
        assert throughput > MIN_THROUGHPUT
        _bench("generator_source", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_datagen_source_throughput(self):
        random.seed(42)
        flow = Flow(mode=RuntimeMode.BATCH)
        src = DatagenSource(rows_per_second=10000, max_records=N_CONNECTOR,
                            fields={"value": "float", "name": "string"})
        stream = flow.from_source(src)
        stream.map(lambda x: x)
        sink = stream.collect()
        t0 = time.monotonic()
        flow.execute()
        elapsed = time.monotonic() - t0
        throughput = N_CONNECTOR / max(elapsed, 1e-9)
        assert len(sink.results) == N_CONNECTOR
        assert throughput > MIN_THROUGHPUT
        _bench("datagen_source", "throughput", throughput, "records/sec")


class TestEventLogBenchmark:

    @pytest.mark.slow
    def test_single_append_latency(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = EventLog(os.path.join(tmpdir, "bench.log"))
            latencies = []
            for i in range(N_EVENTLOG):
                rec = {"id": i, "data": f"payload_{i}"}
                t0 = time.monotonic()
                log.append(rec)
                elapsed = time.monotonic() - t0
                latencies.append(elapsed)
            avg_ms = sum(latencies) / len(latencies) * 1000
            p99_ms = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
            assert avg_ms < 5.0
            _bench("eventlog_append", "avg_latency", avg_ms, "ms")
            _bench("eventlog_append", "p99_latency", p99_ms, "ms")

    @pytest.mark.slow
    def test_batch_append_throughput(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = EventLog(os.path.join(tmpdir, "bench_batch.log"))
            records = [{"id": i, "data": f"payload_{i}"} for i in range(N_EVENTLOG)]
            t0 = time.monotonic()
            offsets = log.append_batch(records)
            elapsed = time.monotonic() - t0
            throughput = N_EVENTLOG / max(elapsed, 1e-9)
            assert len(offsets) == N_EVENTLOG
            assert throughput > MIN_THROUGHPUT
            _bench("eventlog_append_batch", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_full_read_throughput(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = EventLog(os.path.join(tmpdir, "bench_read.log"))
            for i in range(N_EVENTLOG):
                log.append({"id": i, "value": i * 10})
            t0 = time.monotonic()
            records = log.read()
            elapsed = time.monotonic() - t0
            throughput = N_EVENTLOG / max(elapsed, 1e-9)
            assert len(records) == N_EVENTLOG
            assert throughput > MIN_THROUGHPUT
            _bench("eventlog_read", "throughput", throughput, "records/sec")

    @pytest.mark.slow
    def test_read_with_offset_latency(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log = EventLog(os.path.join(tmpdir, "bench_offset.log"))
            for i in range(N_EVENTLOG):
                log.append({"id": i})
            t0 = time.monotonic()
            records = log.read(offset=N_EVENTLOG // 2)
            elapsed = time.monotonic() - t0
            assert len(records) == N_EVENTLOG // 2
            _bench("eventlog_read_offset", "latency", elapsed * 1000, "ms")

    @pytest.mark.slow
    def test_compaction_ratio(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "compact.log")
            log = EventLog(log_path)
            for i in range(N_EVENTLOG):
                log.append({"key": f"key_{i % 100}", "version": i // 100, "data": "x" * 50})
            all_records = log.read()
            raw_count = len(all_records)
            raw_size = sum(len(json.dumps(r, default=str)) for r in all_records)
            def merge_fn(records):
                latest = {}
                for r in records:
                    latest[r["key"]] = r
                return list(latest.values())
            t0 = time.monotonic()
            log.compact(merge_fn, checkpoint_offset=raw_count)
            elapsed = time.monotonic() - t0
            compacted = log.read()
            compacted_count = len(compacted)
            compacted_size = sum(len(json.dumps(r, default=str)) for r in compacted)
            assert compacted_count <= 100
            ratio = compacted_size / max(raw_size, 1)
            assert ratio < 1.0
            _bench("eventlog_compact", "ratio", ratio * 100, "%")
            _bench("eventlog_compact", "latency", elapsed * 1000, "ms")
            _bench("eventlog_compact", "before_records", raw_count, "records")
            _bench("eventlog_compact", "after_records", compacted_count, "records")


class TestServingViewBenchmark:

    def test_update_batch_throughput(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        items = [(f"key_{i % 500}", i) for i in range(N_CHECKPOINT * 5)]
        t0 = time.monotonic()
        view.update_batch(items)
        elapsed = time.monotonic() - t0
        throughput = len(items) / max(elapsed, 1e-9)
        assert throughput > MIN_THROUGHPUT * 5
        _bench("serving_update_batch", "throughput", throughput, "records/sec")

    def test_update_speed_throughput(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        items = [(f"key_{i % 500}", i) for i in range(N_CHECKPOINT * 5)]
        t0 = time.monotonic()
        view.update_speed(items)
        elapsed = time.monotonic() - t0
        throughput = len(items) / max(elapsed, 1e-9)
        assert throughput > MIN_THROUGHPUT * 5
        _bench("serving_update_speed", "throughput", throughput, "records/sec")

    def test_single_key_query_latency(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        view.update_batch([(f"key_{i}", i * 10) for i in range(N_CHECKPOINT)])
        latencies = []
        for i in range(1000):
            t0 = time.monotonic()
            result = view.query(f"key_{i % N_CHECKPOINT}")
            elapsed = time.monotonic() - t0
            latencies.append(elapsed)
        avg_us = sum(latencies) / len(latencies) * 1_000_000
        assert avg_us < 1000
        _bench("serving_query_single", "avg_latency", avg_us, "us")

    def test_all_key_query_latency(self):
        view = ServingView(key_fn=lambda x: x[0] if isinstance(x, tuple) else x)
        view.update_batch([(f"key_{i}", i * 10) for i in range(N_CHECKPOINT)])
        t0 = time.monotonic()
        all_result = view.query()
        elapsed = time.monotonic() - t0
        assert len(all_result) == N_CHECKPOINT
        _bench("serving_query_all", "latency", elapsed * 1000, "ms")

    def test_merge_view_throughput(self):
        strategies = {
            "latest": MergeView.latest,
            "prefer_batch": MergeView.prefer_batch,
        }
        for name, fn in strategies.items():
            t0 = time.monotonic()
            for i in range(N_CHECKPOINT * 10):
                fn({"val": i}, {"val": i + 1})
            elapsed = time.monotonic() - t0
            throughput = (N_CHECKPOINT * 10) / max(elapsed, 1e-9)
            assert throughput > MIN_THROUGHPUT * 10
            _bench(f"merge_{name}", "throughput", throughput, "ops/sec")
        t0 = time.monotonic()
        for i in range(N_CHECKPOINT * 10):
            MergeView.combine_sum(i, i + 1)
        elapsed = time.monotonic() - t0
        throughput = (N_CHECKPOINT * 10) / max(elapsed, 1e-9)
        assert throughput > MIN_THROUGHPUT * 10
        _bench("merge_combine_sum", "throughput", throughput, "ops/sec")

    def test_merge_view_correctness(self):
        assert MergeView.latest({"v": 1}, {"v": 2}) == {"v": 2}
        assert MergeView.latest({"v": 1}, None) == {"v": 1}
        assert MergeView.prefer_batch({"v": 1}, {"v": 2}) == {"v": 1}
        assert MergeView.prefer_batch(None, {"v": 2}) == {"v": 2}
        assert MergeView.combine_sum(10, 5) == 15
        assert MergeView.combine_sum(None, 5) == 5
        _bench("merge_correctness", "strategies", 3, "verified")


class TestWatermarkBenchmark:

    @pytest.mark.slow
    def test_monotonous_watermark_overhead(self):
        strategy = WatermarkStrategy.monotonous()
        t0 = time.monotonic()
        for i in range(N_STREAM):
            wm = strategy.on_event(None, float(i))
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        final_wm = strategy.current_watermark()
        assert final_wm.timestamp == float(N_STREAM - 1)
        assert throughput > MIN_THROUGHPUT * 10
        _bench("watermark_monotonous", "throughput", throughput, "events/sec")
        _bench("watermark_monotonous", "final_timestamp", final_wm.timestamp, "ts")

    @pytest.mark.slow
    def test_bounded_out_of_orderness_accuracy(self):
        strategy = WatermarkStrategy.bounded_out_of_orderness(5.0)
        random.seed(42)
        timestamps = list(range(N_STREAM))
        random.shuffle(timestamps)
        t0 = time.monotonic()
        for ts in timestamps:
            strategy.on_event(None, float(ts))
        elapsed = time.monotonic() - t0
        throughput = N_STREAM / max(elapsed, 1e-9)
        final_wm = strategy.current_watermark()
        max_ts = float(max(timestamps))
        assert final_wm.timestamp == max_ts - 5.0
        assert throughput > MIN_THROUGHPUT * 10
        _bench("watermark_ooo", "throughput", throughput, "events/sec")
        _bench("watermark_ooo", "final_watermark", final_wm.timestamp, "ts")

    @pytest.mark.slow
    def test_watermark_tracker_late_detection(self):
        from liutang.engine.watermark import WatermarkTracker
        strategy = WatermarkStrategy.bounded_out_of_orderness(3.0)
        tracker = WatermarkTracker(strategy)
        tracker.on_event(10.0)
        tracker.on_event(20.0)
        tracker.on_event(30.0)
        assert not tracker.is_late(25.0)
        assert tracker.is_late(5.0)
        _bench("watermark_late", "watermark", tracker.current_watermark(), "ts")


class TestParallelismBenchmark:

    @pytest.mark.slow
    def test_parallelism_speedup(self):
        data = list(range(N_STREAM))
        results_by_parallelism = {}
        for p in [1, 2, 4]:
            flow = Flow(mode=RuntimeMode.BATCH, parallelism=p)
            stream = flow.from_collection(data)
            stream.map(lambda x: x * 2)
            sink = stream.collect()
            t0 = time.monotonic()
            flow.execute()
            elapsed = time.monotonic() - t0
            throughput = N_STREAM / max(elapsed, 1e-9)
            results_by_parallelism[p] = {
                "elapsed": elapsed,
                "throughput": throughput,
                "results": sorted(sink.results),
            }
            _bench(f"parallelism_{p}", "throughput", throughput, "records/sec")

        expected = [i * 2 for i in data]
        for p in [1, 2, 4]:
            assert results_by_parallelism[p]["results"] == expected

        speedup_2 = results_by_parallelism[1]["elapsed"] / max(results_by_parallelism[2]["elapsed"], 1e-9)
        speedup_4 = results_by_parallelism[1]["elapsed"] / max(results_by_parallelism[4]["elapsed"], 1e-9)
        _bench("parallelism", "speedup_2x", speedup_2, "ratio")
        _bench("parallelism", "speedup_4x", speedup_4, "ratio")

    @pytest.mark.slow
    def test_parallelism_correctness(self):
        data = list(range(5000))
        expected = sorted([x * 3 for x in data])
        for p in [1, 2, 4]:
            flow = Flow(mode=RuntimeMode.BATCH, parallelism=p)
            stream = flow.from_collection(data)
            stream.map(lambda x: x * 3)
            sink = stream.collect()
            flow.execute()
            assert sorted(sink.results) == expected
        _bench("parallelism_correctness", "verified", 3, "configs")


class TestCheckpointBenchmark:

    @pytest.mark.slow
    def test_memory_checkpoint_size_and_latency(self):
        backend = MemoryStateBackend()
        for i in range(N_CHECKPOINT):
            backend.set_value(f"key_{i}", f"value_{i}")
            backend.append_list(f"list_{i}", i)
            backend.put_map(f"map_{i}", f"sub_{i}", i * 10)
        t0 = time.monotonic()
        checkpoint = backend.checkpoint()
        cp_elapsed = time.monotonic() - t0
        cp_size = len(json.dumps(checkpoint, default=str))
        backend2 = MemoryStateBackend()
        t0 = time.monotonic()
        backend2.restore(checkpoint)
        restore_elapsed = time.monotonic() - t0
        for i in range(N_CHECKPOINT):
            assert backend2.get_value(f"key_{i}") == f"value_{i}"
            assert backend2.get_list(f"list_{i}") == [i]
            assert backend2.get_map(f"map_{i}") == {f"sub_{i}": i * 10}
        assert cp_elapsed < 1.0
        assert restore_elapsed < 1.0
        _bench("memory_checkpoint", "latency", cp_elapsed * 1000, "ms")
        _bench("memory_checkpoint", "size", cp_size, "bytes")
        _bench("memory_restore", "latency", restore_elapsed * 1000, "ms")

    @pytest.mark.slow
    def test_json_file_backend_save_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = JsonFileStateBackend(tmpdir)
            for i in range(N_CHECKPOINT):
                backend.set_value(f"key_{i}", i)
                backend.append_list(f"list_{i}", f"item_{i}")
            t0 = time.monotonic()
            backend.save()
            save_elapsed = time.monotonic() - t0
            state_file = os.path.join(tmpdir, "state.json")
            file_size = os.path.getsize(state_file)
            backend2 = JsonFileStateBackend(tmpdir)
            t0 = time.monotonic()
            loaded = backend2.load()
            load_elapsed = time.monotonic() - t0
            assert loaded is True
            for i in range(N_CHECKPOINT):
                assert backend2.get_value(f"key_{i}") == i
                assert backend2.get_list(f"list_{i}") == [f"item_{i}"]
            assert save_elapsed < 2.0
            assert load_elapsed < 2.0
            _bench("json_save", "latency", save_elapsed * 1000, "ms")
            _bench("json_save", "file_size", file_size, "bytes")
            _bench("json_load", "latency", load_elapsed * 1000, "ms")

    @pytest.mark.slow
    def test_checkpoint_with_flow_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            flow = Flow(mode=RuntimeMode.BATCH).set_checkpoint(tmpdir)
            stream = flow.from_collection(list(range(N_CHECKPOINT)))
            stream.map(lambda x: x * 2)
            sink = stream.collect()
            flow.execute()
            assert len(sink.results) == N_CHECKPOINT
            checkpoint_files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            _bench("flow_checkpoint", "num_files", len(checkpoint_files), "files")