from typing import Any

import sqlalchemy as sa
import sqlalchemy.ext.asyncio


class TrackingConnectionPool(sa.AsyncAdaptedQueuePool):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._checked_out_connections: set[sa.pool.ConnectionPoolEntry] = set()

    @property
    def checked_out_connections(self) -> set[sa.pool.ConnectionPoolEntry]:
        return self._checked_out_connections

    def _do_get(self) -> sa.pool.ConnectionPoolEntry:
        self._checked_out_connections.add(conn := super()._do_get())
        return conn

    def _do_return_conn(self, record: sa.pool.ConnectionPoolEntry) -> None:
        self._checked_out_connections.discard(record)
        super()._do_return_conn(record)

    async def invalidate_checked_out(self) -> int:
        for entry in self._checked_out_connections:
            await sa.ext.asyncio.engine.greenlet_spawn(entry.invalidate)

        return len(self._checked_out_connections)
