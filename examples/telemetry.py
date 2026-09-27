import argparse
import asyncio as aio
import contextvars as cv
import datetime as dt
import logging
from typing import Any, MutableMapping

import opentelemetry as otl
import opentelemetry.baggage.propagation
import opentelemetry.exporter.otlp.proto.http.trace_exporter as http_trace_exporter
import opentelemetry.metrics
import opentelemetry.propagate
import opentelemetry.sdk.metrics.export
import opentelemetry.sdk.trace.export
import opentelemetry.trace.propagation.tracecontext
import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.opentelemetry as sxot
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()

client_id = cv.ContextVar[str]("client_id")


def build_tracer_provider(exporter_endpoint: str) -> otl.trace.TracerProvider:
    exporter = http_trace_exporter.OTLPSpanExporter(endpoint=exporter_endpoint)
    processor = otl.sdk.trace.export.SimpleSpanProcessor(exporter)

    provider = otl.sdk.trace.TracerProvider()
    provider.add_span_processor(processor)

    otl.trace.set_tracer_provider(provider)

    return provider


def build_meter_provider() -> otl.metrics.MeterProvider:
    reader = otl.sdk.metrics.export.PeriodicExportingMetricReader(
        otl.sdk.metrics.export.ConsoleMetricExporter(),
        export_interval_millis=5000,
    )

    provider = otl.sdk.metrics.MeterProvider(metric_readers=[reader])
    otl.metrics.set_meter_provider(provider)

    return provider


async def set_contextvars(
    context: Context,
    task: sx.Task[Context, Any, Any],
    /,
    *,
    wrapped: sx.ExecutorMiddlewareWrappedFunc[Context],
) -> None:
    if (raw_client_id := otl.baggage.get_baggage("client_id")) and isinstance(raw_client_id, str):
        client_id.set(raw_client_id)

    return await wrapped(context, task)


@tasks.task(name="print")
async def print_message(context: Context, message: str, /) -> None:
    print(f"client-id: {client_id.get('unknown')} - {message}")


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    meter_provider = build_meter_provider()
    tracer_provider = build_tracer_provider("http://localhost:4318/v1/traces")
    propagator = otl.propagate.composite.CompositePropagator(
        [
            otl.trace.propagation.tracecontext.TraceContextTextMapPropagator(),
            otl.baggage.propagation.W3CBaggagePropagator(),
        ]
    )

    scheduler = sxsa.SqlAlchemyTransactionalScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
        middlewares=[
            sxot.build_job_schedule_metrics_collecting_middleware(meter_provider),
            sxot.build_job_schedule_tracing_middleware(tracer_provider, propagator),
        ],
    )

    client_id.set("482918459")
    token = otl.context.attach(otl.baggage.set_baggage(client_id.name, client_id.get()))
    try:
        await scheduler.add_job(
            sx.Job(
                sx.PeriodicSchedule.periodically(
                    start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0),
                    delay=dt.timedelta(seconds=2),
                ).with_max_count(5),
                print_message.with_params("hello world!!!"),
            )
        )
    finally:
        otl.context.detach(token)


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    meter_provider = build_meter_provider()
    tracer_provider = build_tracer_provider("http://localhost:4318/v1/traces")
    propagator = otl.propagate.composite.CompositePropagator(
        [
            otl.trace.propagation.tracecontext.TraceContextTextMapPropagator(),
            otl.baggage.propagation.W3CBaggagePropagator(),
        ]
    )

    app_state: AppState = {}
    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        state=app_state,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
        middlewares=[
            sxot.build_task_execution_metrics_collecting_middleware(meter_provider),
            sxot.build_task_execution_tracing_middleware(tracer_provider, propagator),
            set_contextvars,
        ],
    )

    await executor.run()


logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["scheduler", "executor"])
args = parser.parse_args()

if args.action == "scheduler":
    aio.run(start_scheduler())
elif args.action == "executor":
    aio.run(start_executor())
else:
    raise AssertionError("unreachable")
