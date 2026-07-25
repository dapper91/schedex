import timeit
from typing import Any, Optional

import opentelemetry as otl
import opentelemetry.baggage
import opentelemetry.metrics
import opentelemetry.propagate
import opentelemetry.propagators.textmap
import opentelemetry.trace.propagation.tracecontext
from opentelemetry.util import types

from schedex import (
    Context,
    ExecutorMiddleware,
    ExecutorMiddlewareWrappedFunc,
    Job,
    Schedule,
    SchedulerMiddleware,
    SchedulerMiddlewareWrappedFunc,
    Task,
)

INSTRUMENTING_MODULE_NAME = "schedex"


def build_job_schedule_tracing_middleware[SchT: Schedule](
    tracer_provider: Optional[otl.trace.TracerProvider] = None,
    propagator: Optional[otl.propagators.textmap.TextMapPropagator] = None,
) -> SchedulerMiddleware[SchT]:
    """
    Builds a middleware for job scheduling tracing.

    :param tracer_provider: tracer provider. If not passed the global one will be used.
    :param propagator: trace propagator. If not passed the global one will be used.
    """

    tracer_provider = tracer_provider or otl.trace.get_tracer_provider()
    tracer = tracer_provider.get_tracer(INSTRUMENTING_MODULE_NAME)

    propagator = propagator or otl.propagate.get_global_textmap()

    async def trace_job_schedule(
        job: Job[SchT],
        /,
        *,
        wrapped: SchedulerMiddlewareWrappedFunc[SchT],
    ) -> Optional[str]:
        attributes: types.Attributes = {
            "scheduler.job.id": job.id,
            "scheduler.job.name": job.task.name,
        }

        with tracer.start_as_current_span(job.task.name, kind=otl.trace.SpanKind.PRODUCER, attributes=attributes):
            propagator.inject(job.meta)
            return await wrapped(job)

    return trace_job_schedule


def build_job_schedule_metrics_collecting_middleware[SchT: Schedule](
    meter_provider: Optional[otl.metrics.MeterProvider] = None,
) -> SchedulerMiddleware[SchT]:
    """
    Builds a middleware for job scheduling metrics collecting.

    :param meter_provider: metrics provider. If not passed the global one will be used.
    """

    meter_provider = meter_provider or otl.metrics.get_meter_provider()
    meter = meter_provider.get_meter(INSTRUMENTING_MODULE_NAME)

    publish_counter = meter.create_up_down_counter(
        name="scheduler.submit.jobs",
        unit="jobs",
        description="Measures the total number of scheduled jobs",
    )
    duration_histogram = meter.create_histogram(
        name="scheduler.submit.duration",
        unit="ms",
        description="Measures the duration of scheduling",
    )

    async def collect_metrics(
        job: Job[SchT],
        /,
        *,
        wrapped: SchedulerMiddlewareWrappedFunc[SchT],
    ) -> Optional[str]:
        attributes: types.Attributes = {
            "scheduler.job.name": job.task.name,
        }

        publish_counter.add(amount=1, attributes=attributes)

        started_at = timeit.default_timer()
        try:
            return await wrapped(job)
        finally:
            duration_histogram.record(amount=timeit.default_timer() - started_at, attributes=attributes)

    return collect_metrics


def build_task_execution_tracing_middleware[CtxT: Context[Any, Any]](
    tracer_provider: Optional[otl.trace.TracerProvider] = None,
    propagator: Optional[otl.propagators.textmap.TextMapPropagator] = None,
) -> ExecutorMiddleware[CtxT]:
    """
    Builds a middleware for tracing scheduler task executions.

    :param tracer_provider: tracer provider. If not passed the global one will be used.
    :param propagator: trace propagator. If not passed the global one will be used.
    """

    tracer_provider = tracer_provider or otl.trace.get_tracer_provider()
    tracer = tracer_provider.get_tracer(INSTRUMENTING_MODULE_NAME)

    propagator = propagator or otl.propagate.get_global_textmap()

    def copy_baggage(
        src_context: otl.context.Context, dst_context: Optional[otl.context.Context] = None
    ) -> otl.context.Context:
        dst_context = dst_context or otl.context.get_current()

        src_baggage = otl.baggage.get_all(src_context)
        for key, value in src_baggage.items():
            dst_context = otl.baggage.set_baggage(key, value, dst_context)

        return dst_context

    async def trace_task_execution(
        context: CtxT,
        task: Task[CtxT, Any, Any],
        /,
        *,
        wrapped: ExecutorMiddlewareWrappedFunc[CtxT],
    ) -> None:
        attributes: types.Attributes = {
            "scheduler.job.id": context.task.job_id,
            "scheduler.task.id": context.task.id,
            "scheduler.task.name": task.name,
        }

        propagated_context = propagator.extract(context.task.meta)
        linked_span = otl.trace.get_current_span(propagated_context)
        linked_context = linked_span.get_span_context()

        token = otl.context.attach(copy_baggage(propagated_context))
        try:
            with tracer.start_as_current_span(
                task.name,
                links=[otl.trace.Link(linked_context)],
                kind=otl.trace.SpanKind.CONSUMER,
                attributes=attributes,
            ):
                return await wrapped(context, task)
        finally:
            otl.context.detach(token)

    return trace_task_execution


def build_task_execution_metrics_collecting_middleware[CtxT: Context[Any, Any]](
    meter_provider: Optional[otl.metrics.MeterProvider] = None,
) -> ExecutorMiddleware[CtxT]:
    """
    Builds a middleware for task execution metrics collecting.

    :param meter_provider: metrics provider. If not passed the global one will be used.
    """

    meter_provider = meter_provider or otl.metrics.get_meter_provider()
    meter = meter_provider.get_meter(INSTRUMENTING_MODULE_NAME)

    publish_counter = meter.create_up_down_counter(
        name="scheduler.execution.tasks",
        unit="tasks",
        description="Measures the total number of scheduler executed tasks",
    )
    error_counter = meter.create_counter(
        name="scheduler.execution.errors",
        unit="errors",
        description="Measures the number of scheduler task execution errors",
    )
    duration_histogram = meter.create_histogram(
        name="scheduler.execution.duration",
        unit="ms",
        description="Measures the duration of scheduler task execution",
    )

    async def collect_metrics(
        context: CtxT,
        task: Task[CtxT, Any, Any],
        /,
        *,
        wrapped: ExecutorMiddlewareWrappedFunc[CtxT],
    ) -> None:
        attributes: types.Attributes = {
            "scheduler.task.name": task.name,
        }

        publish_counter.add(amount=1, attributes=attributes)

        started_at = timeit.default_timer()
        try:
            return await wrapped(context, task)
        except Exception:
            error_counter.add(amount=1, attributes=attributes)
            raise
        finally:
            duration_histogram.record(amount=timeit.default_timer() - started_at, attributes=attributes)

    return collect_metrics
