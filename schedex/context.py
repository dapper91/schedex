import dataclasses as dc
import datetime as dt

from .metadata import Metadata


@dc.dataclass(frozen=True)
class TaskInfo:
    """
    Executing task information.

    :param id: task identifier
    :param job_id: job identifier which spawned the task
    :param sequence_number: task sequence number (unique within a job)
    :param created_at: task creation time
    :param attempts: number of task execution attempts
    :param meta: task metadata
    """

    id: str
    job_id: str
    sequence_number: int
    created_at: dt.datetime
    attempts: int
    meta: Metadata


@dc.dataclass(frozen=True)
class Context[StT, LkT]:
    """
    Task context.

    :param state: application state
    :param task: executing task
    :param lock: executing task lock
    """

    state: StT
    task: TaskInfo
    lock: LkT
