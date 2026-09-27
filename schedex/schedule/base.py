import abc
import datetime as dt
from typing import Optional

from schedex.serializer.serializable import Serializable


class Schedule(Serializable, abc.ABC):
    """
    Abstract base class for a schedule.
    """

    @abc.abstractmethod
    def next_run(self, now: dt.datetime, prev: Optional[dt.datetime], count: int) -> Optional[dt.datetime]:
        """
        Returns the next run time.

        :param now: current time
        :param prev: previous run time
        :param count: number of previous runs
        :return: next run time or `None` if the task must not be executed anymore.
        """
