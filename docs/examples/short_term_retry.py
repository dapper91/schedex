import logging
import random as rnd
from typing import Any, MutableMapping

import tenacity as tc

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

logger = logging.getLogger(__name__)

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task(name="print")
@tc.retry(
    wait=tc.wait_exponential(multiplier=2, min=1, max=15),
    stop=tc.stop_after_attempt(5),
    before_sleep=tc.before_sleep_log(logger, logging.WARNING),
)
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    if rnd.random() < 0.9:
        raise Exception("print message error")

    print(message, end=end)
