import datetime as dt
import zoneinfo

from schedex.schedule import cron

schedule = cron.CronSchedule(
    "30 0/12 1-7 * *",
    timezone=zoneinfo.ZoneInfo("UTC"),
    start_at=dt.datetime.now(tz=dt.UTC),
)
