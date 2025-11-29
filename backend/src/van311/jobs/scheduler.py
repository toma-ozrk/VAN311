from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ..db.aggregation import METRICS_INTERVAL_MINUTES, calculate_metrics
from ..db.updates import (
    GRACE_TIME_SECONDS,
    UPDATE_INTERVAL_MINUTES,
    update_service_requests,
)


def main():
    scheduler = BlockingScheduler(
        job_defaults={"misfire_grace_time": GRACE_TIME_SECONDS}
    )

    scheduler.add_job(
        func=update_service_requests,
        trigger=IntervalTrigger(minutes=UPDATE_INTERVAL_MINUTES),
        id="ingest_job",
        replace_existing=True,
    )
    scheduler.add_job(
        func=calculate_metrics,
        trigger=IntervalTrigger(minutes=METRICS_INTERVAL_MINUTES),
        id="recalculate_metrics",
        replace_existing=True,
    )

    print("------ Scheduler has started ------")
    scheduler.start()


if __name__ == "__main__":
    main()
