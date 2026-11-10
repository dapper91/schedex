from typing import Any

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

tasks = sx.TaskRegistry[dict[str, Any], sxsa.SqlAlchemyLeasingTaskLock]()


@tasks.task()
async def task_function(context: sx.Context) -> None:
    lock: sxsa.SqlAlchemyLeasingTaskLock = context.lock

    print(f"lock is acquired for {await lock.remain()}")
