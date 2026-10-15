import contextlib as cl
import unittest.mock as mk

import opentelemetry as otl
import opentelemetry.sdk.metrics
import opentelemetry.sdk.trace.export
import opentelemetry.trace.propagation.tracecontext
import pytest
from opentelemetry.sdk.trace.export import in_memory_span_exporter

import schedex.contrib.opentelemetry as sxot
from tests.factories import ContextFactory, JobFactory, TaskFactory, TaskInfoFactory

pytestmark = pytest.mark.unit


class CustomException(Exception):
    pass


@pytest.mark.parametrize("failed", [False, True])
async def test_job_schedule_tracing_middleware(failed: bool) -> None:
    exporter = in_memory_span_exporter.InMemorySpanExporter()
    processor = otl.sdk.trace.export.SimpleSpanProcessor(exporter)

    tracer_provider = otl.sdk.trace.TracerProvider()
    tracer_provider.add_span_processor(processor)
    trace_propagator = otl.trace.propagation.tracecontext.TraceContextTextMapPropagator()

    middleware = sxot.build_job_schedule_tracing_middleware(tracer_provider, trace_propagator)
    job = JobFactory.build(meta={})

    if failed:
        function = mk.AsyncMock(side_effect=CustomException("test error"))
    else:
        function = mk.AsyncMock(return_value=job.id)

    with tracer_provider.get_tracer("schedex").start_as_current_span("parent") as parent_span:
        with cl.suppress(CustomException):
            actual_job_id = await middleware(job, wrapped=function)
            expected_job_id = job.id
            assert actual_job_id == expected_job_id

    function.assert_awaited_once_with(job)

    spans = exporter.get_finished_spans()
    assert len(spans) == 2
    span = spans[0]

    assert span.parent is parent_span.get_span_context()
    assert span.name == job.task.name
    assert span.kind == otl.trace.SpanKind.PRODUCER
    assert span.attributes == {
        "scheduler.job.id": job.id,
        "scheduler.job.name": job.task.name,
    }
    if failed:
        assert span.status.status_code == otl.trace.StatusCode.ERROR
        assert span.status.description == "CustomException: test error"

    assert job.meta == {
        "traceparent": mk.ANY,
    }


@pytest.mark.parametrize("failed", [False, True])
async def test_job_schedule_metrics_collecting_middleware(failed: bool) -> None:
    metrics_reader = otl.sdk.metrics.export.InMemoryMetricReader()
    meter_provider = otl.sdk.metrics.MeterProvider()
    meter_provider.add_metric_reader(metrics_reader)

    middleware = sxot.build_job_schedule_metrics_collecting_middleware(meter_provider)
    job = JobFactory.build(meta={})

    if failed:
        function = mk.AsyncMock(side_effect=CustomException("test error"))
    else:
        function = mk.AsyncMock(return_value=job.id)

    with cl.suppress(CustomException):
        actual_job_id = await middleware(job, wrapped=function)
        expected_job_id = job.id
        assert actual_job_id == expected_job_id

    function.assert_awaited_once_with(job)

    metrics_data = metrics_reader.get_metrics_data()
    assert metrics_data

    metrics = {metric.name: metric for metric in metrics_data.resource_metrics[0].scope_metrics[0].metrics}
    expected_attributes = {
        "scheduler.job.name": job.task.name,
    }

    publish_counter = metrics["scheduler.submit.jobs"]
    assert isinstance(publish_counter.data, otl.sdk.metrics.export.Sum)
    assert len(publish_counter.data.data_points) == 1
    data_point = publish_counter.data.data_points[0]
    assert data_point.attributes == expected_attributes
    assert data_point.value == 1

    duration_histogram = metrics["scheduler.submit.duration"]
    assert isinstance(duration_histogram.data, otl.sdk.metrics.export.Histogram)
    assert len(duration_histogram.data.data_points) == 1
    data_point = duration_histogram.data.data_points[0]
    assert data_point.attributes == expected_attributes
    assert data_point.count == 1

    if failed:
        publish_counter = metrics["scheduler.submit.errors"]
        assert isinstance(publish_counter.data, otl.sdk.metrics.export.Sum)
        assert len(publish_counter.data.data_points) == 1
        data_point = publish_counter.data.data_points[0]
        assert data_point.attributes == expected_attributes
        assert data_point.value == 1


@pytest.mark.parametrize("error", [False, True])
async def test_task_execution_tracing_middleware(error: bool) -> None:
    exporter = in_memory_span_exporter.InMemorySpanExporter()
    processor = otl.sdk.trace.export.SimpleSpanProcessor(exporter)

    tracer_provider = otl.sdk.trace.TracerProvider()
    tracer_provider.add_span_processor(processor)
    trace_propagator = otl.trace.propagation.tracecontext.TraceContextTextMapPropagator()

    middleware = sxot.build_task_execution_tracing_middleware(tracer_provider, trace_propagator)

    if error:
        function = mk.AsyncMock(side_effect=CustomException("test error"))
    else:
        function = mk.AsyncMock()

    version = 0
    remote_trace_id = 0x0AF7651916CD43DD8448EB211C80319C
    remote_span_id = 0xB7AD6B7169203331
    remote_flags = 0x01
    traceparent = f"{version:02}-{remote_trace_id:032x}-{remote_span_id:016x}-{remote_flags:02}"

    meta = {"traceparent": traceparent}
    context = ContextFactory.build(task=TaskInfoFactory.build(meta=meta))
    task = TaskFactory.build()

    with tracer_provider.get_tracer("schedex").start_as_current_span("parent") as parent_span:
        with cl.suppress(CustomException):
            await middleware(context, task, wrapped=function)

    function.assert_awaited_once_with(context, task)

    spans = exporter.get_finished_spans()
    assert len(spans) == 2
    span = spans[0]

    assert span.parent is parent_span.get_span_context()
    assert span.name == task.name
    assert len(span.links) == 1
    assert span.links[0].context == otl.trace.SpanContext(
        trace_id=remote_trace_id,
        span_id=remote_span_id,
        is_remote=bool(remote_flags & 0x01),
        trace_flags=otl.trace.TraceFlags.SAMPLED,
    )
    assert span.kind == otl.trace.SpanKind.CONSUMER
    assert span.attributes == {
        "scheduler.job.id": context.task.job_id,
        "scheduler.task.id": context.task.id,
        "scheduler.task.name": task.name,
        "scheduler.task.attempt": context.task.attempts,
    }
    if error:
        assert span.status.status_code == otl.trace.StatusCode.ERROR
        assert span.status.description == "CustomException: test error"


@pytest.mark.parametrize("failed", [False, True])
async def test_task_execution_metrics_collecting_middleware(failed: bool) -> None:
    metrics_reader = otl.sdk.metrics.export.InMemoryMetricReader()
    meter_provider = otl.sdk.metrics.MeterProvider()
    meter_provider.add_metric_reader(metrics_reader)

    middleware = sxot.build_task_execution_metrics_collecting_middleware(meter_provider)

    if failed:
        function = mk.AsyncMock(side_effect=CustomException("test error"))
    else:
        function = mk.AsyncMock()

    context = ContextFactory.build(task=TaskInfoFactory.build())
    task = TaskFactory.build()

    with cl.suppress(CustomException):
        await middleware(context, task, wrapped=function)

    function.assert_awaited_once_with(context, task)

    metrics_data = metrics_reader.get_metrics_data()
    assert metrics_data

    metrics = {metric.name: metric for metric in metrics_data.resource_metrics[0].scope_metrics[0].metrics}
    expected_attributes = {
        "scheduler.task.name": task.name,
    }

    publish_counter = metrics["scheduler.execution.tasks"]
    assert isinstance(publish_counter.data, otl.sdk.metrics.export.Sum)
    assert len(publish_counter.data.data_points) == 1
    data_point = publish_counter.data.data_points[0]
    assert data_point.attributes == expected_attributes
    assert data_point.value == 1

    duration_histogram = metrics["scheduler.execution.duration"]
    assert isinstance(duration_histogram.data, otl.sdk.metrics.export.Histogram)
    assert len(duration_histogram.data.data_points) == 1
    data_point = duration_histogram.data.data_points[0]
    assert data_point.attributes == expected_attributes
    assert data_point.count == 1

    if failed:
        publish_counter = metrics["scheduler.execution.errors"]
        assert isinstance(publish_counter.data, otl.sdk.metrics.export.Sum)
        assert len(publish_counter.data.data_points) == 1
        data_point = publish_counter.data.data_points[0]
        assert data_point.attributes == expected_attributes
        assert data_point.value == 1
