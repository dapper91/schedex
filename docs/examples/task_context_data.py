from typing import Any

from schedex import Context, TaskRegistry
from schedex.contrib.sqlalchemy import SqlAlchemySfuTaskLock

tasks = TaskRegistry[dict[str, Any], SqlAlchemySfuTaskLock]()


@tasks.task()
async def task_function(context: Context) -> None:
    task = context.task

    print(f"task identifier: {task.id}")
    print(f"job identifier: {task.job_id}")
    print(f"task sequence number: {task.sequence_number}")
    print(f"task created at: {task.created_at}")
    print(f"task execution attempt: {task.attempts}")
