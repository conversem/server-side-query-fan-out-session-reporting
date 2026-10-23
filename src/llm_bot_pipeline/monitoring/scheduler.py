"""
Local pipeline scheduler using APScheduler.

Provides:
- Cron-like scheduling for pipeline jobs
- Job persistence across restarts
- Graceful shutdown handling
- Job history tracking

Requires apscheduler (pip install apscheduler).
"""

import atexit
import logging
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Types of schedules."""

    INTERVAL = "interval"
    CRON = "cron"
    DAILY = "daily"
    ONCE = "once"


@dataclass
class ScheduleConfig:
    """Configuration for a scheduled job."""

    schedule_type: ScheduleType
    hours: Optional[int] = None
    minutes: Optional[int] = None
    seconds: Optional[int] = None
    cron_expression: Optional[str] = None
    hour: int = 6
    minute: int = 0
    run_date: Optional[datetime] = None
    timezone: str = "UTC"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @classmethod
    def hourly(cls, minute: int = 0) -> "ScheduleConfig":
        """Create hourly schedule that runs at specified minute past each hour."""
        return cls(
            schedule_type=ScheduleType.CRON,
            cron_expression=f"{minute} * * * *",
        )

    @classmethod
    def daily(cls, hour: int = 6, minute: int = 0) -> "ScheduleConfig":
        """Create daily schedule."""
        return cls(
            schedule_type=ScheduleType.DAILY,
            hour=hour,
            minute=minute,
        )

    @classmethod
    def every_minutes(cls, minutes: int) -> "ScheduleConfig":
        """Create interval schedule in minutes."""
        return cls(
            schedule_type=ScheduleType.INTERVAL,
            minutes=minutes,
        )

    @classmethod
    def cron(cls, expression: str) -> "ScheduleConfig":
        """Create cron schedule from expression."""
        return cls(
            schedule_type=ScheduleType.CRON,
            cron_expression=expression,
        )


@dataclass
class JobExecution:
    """Record of a job execution."""

    job_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    duration_seconds: Optional[float] = None

    def complete(self, success: bool, error: Optional[str] = None) -> None:
        """Mark execution as complete."""
        self.completed_at = datetime.now(timezone.utc)
        self.success = success
        self.error = error
        self.duration_seconds = (self.completed_at - self.started_at).total_seconds()


@dataclass
class ScheduledJob:
    """A scheduled job with execution history."""

    job_id: str
    name: str
    func: Callable
    schedule: ScheduleConfig
    enabled: bool = True
    max_instances: int = 1
    history: list[JobExecution] = field(default_factory=list)
    max_history: int = 100

    def add_execution(self, execution: JobExecution) -> None:
        """Add execution to history."""
        self.history.append(execution)
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history :]

    @property
    def last_execution(self) -> Optional[JobExecution]:
        """Get most recent execution."""
        return self.history[-1] if self.history else None

    @property
    def success_rate(self) -> float:
        """Calculate success rate over history."""
        if not self.history:
            return 0.0
        successful = sum(1 for e in self.history if e.success)
        return successful / len(self.history)


class PipelineScheduler:
    """
    Scheduler for pipeline jobs using APScheduler.

    Features:
    - Multiple scheduling modes (interval, cron, daily)
    - Job persistence (optional)
    - Graceful shutdown
    - Execution history tracking
    """

    def __init__(
        self,
        persist_jobs: bool = False,
        job_store_path: Optional[Path] = None,
        max_concurrent_jobs: int = 3,
    ):
        """
        Initialize the scheduler.

        Args:
            persist_jobs: Whether to persist jobs to SQLite
            job_store_path: Path for job store database
            max_concurrent_jobs: Maximum concurrent job executions
        """
        self.persist_jobs = persist_jobs
        self.job_store_path = job_store_path or Path("data/scheduler_jobs.db")
        self.max_concurrent_jobs = max_concurrent_jobs

        self._jobs: dict[str, ScheduledJob] = {}
        self._scheduler = None
        self._running = False

    def _init_scheduler(self):
        """Initialize APScheduler with configuration."""
        try:
            from apscheduler.executors.pool import ThreadPoolExecutor
            from apscheduler.schedulers.background import BackgroundScheduler

            executors = {
                "default": ThreadPoolExecutor(self.max_concurrent_jobs),
            }

            job_defaults = {
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 3600,
            }

            jobstores = {}
            if self.persist_jobs:
                from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

                self.job_store_path.parent.mkdir(parents=True, exist_ok=True)
                jobstores["default"] = SQLAlchemyJobStore(
                    url=f"sqlite:///{self.job_store_path}"
                )

            self._scheduler = BackgroundScheduler(
                executors=executors,
                job_defaults=job_defaults,
                jobstores=jobstores,
                timezone="UTC",
            )

        except ImportError as e:
            logger.error(
                f"APScheduler not available: {e}. "
                "Install with: pip install apscheduler"
            )
            raise

    def add_job(
        self,
        job_id: str,
        name: str,
        func: Callable,
        schedule: ScheduleConfig,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        enabled: bool = True,
    ) -> ScheduledJob:
        """
        Add a job to the scheduler.

        Args:
            job_id: Unique job identifier
            name: Human-readable job name
            func: Function to execute
            schedule: Schedule configuration
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            enabled: Whether job is enabled

        Returns:
            ScheduledJob instance
        """
        job = ScheduledJob(
            job_id=job_id,
            name=name,
            func=func,
            schedule=schedule,
            enabled=enabled,
        )
        self._jobs[job_id] = job

        if enabled and self._running:
            self._schedule_job(job, args, kwargs)

        logger.info(f"Added job: {job_id} ({name})")
        return job

    def _schedule_job(
        self,
        job: ScheduledJob,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ) -> None:
        """Add job to APScheduler."""
        if self._scheduler is None:
            return

        def job_wrapper(*args, **kwargs):
            execution = JobExecution(
                job_id=job.job_id,
                started_at=datetime.now(timezone.utc),
            )
            try:
                result = job.func(*args, **kwargs)
                execution.complete(success=True)
                logger.info(
                    f"Job {job.job_id} completed successfully "
                    f"({execution.duration_seconds:.2f}s)"
                )
                return result
            except Exception as e:
                execution.complete(success=False, error=str(e))
                logger.error(f"Job {job.job_id} failed: {e}")
                raise
            finally:
                job.add_execution(execution)

        schedule = job.schedule

        if schedule.schedule_type == ScheduleType.INTERVAL:
            self._scheduler.add_job(
                job_wrapper,
                "interval",
                id=job.job_id,
                hours=schedule.hours or 0,
                minutes=schedule.minutes or 0,
                seconds=schedule.seconds or 0,
                args=args,
                kwargs=kwargs,
                max_instances=job.max_instances,
                replace_existing=True,
            )
        elif schedule.schedule_type == ScheduleType.DAILY:
            self._scheduler.add_job(
                job_wrapper,
                "cron",
                id=job.job_id,
                hour=schedule.hour,
                minute=schedule.minute,
                args=args,
                kwargs=kwargs,
                max_instances=job.max_instances,
                replace_existing=True,
            )
        elif schedule.schedule_type == ScheduleType.CRON:
            from apscheduler.triggers.cron import CronTrigger

            self._scheduler.add_job(
                job_wrapper,
                CronTrigger.from_crontab(schedule.cron_expression),
                id=job.job_id,
                args=args,
                kwargs=kwargs,
                max_instances=job.max_instances,
                replace_existing=True,
            )
        elif schedule.schedule_type == ScheduleType.ONCE:
            self._scheduler.add_job(
                job_wrapper,
                "date",
                id=job.job_id,
                run_date=schedule.run_date,
                args=args,
                kwargs=kwargs,
                replace_existing=True,
            )

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job from the scheduler.

        Args:
            job_id: Job identifier

        Returns:
            True if job was removed
        """
        if job_id in self._jobs:
            del self._jobs[job_id]
            if self._scheduler and self._running:
                try:
                    self._scheduler.remove_job(job_id)
                except Exception:
                    logger.debug(
                        "Non-critical scheduler operation failed", exc_info=True
                    )
            logger.info(f"Removed job: {job_id}")
            return True
        return False

    def enable_job(self, job_id: str) -> bool:
        """Enable a disabled job."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = True
            if self._scheduler and self._running:
                try:
                    self._scheduler.resume_job(job_id)
                except Exception:
                    logger.debug(
                        "Non-critical scheduler operation failed", exc_info=True
                    )
                    self._schedule_job(self._jobs[job_id])
            return True
        return False

    def disable_job(self, job_id: str) -> bool:
        """Disable a job without removing it."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = False
            if self._scheduler and self._running:
                try:
                    self._scheduler.pause_job(job_id)
                except Exception:
                    logger.debug(
                        "Non-critical scheduler operation failed", exc_info=True
                    )
            return True
        return False

    def run_job_now(self, job_id: str) -> bool:
        """
        Trigger immediate execution of a job.

        Args:
            job_id: Job identifier

        Returns:
            True if job was triggered
        """
        if job_id in self._jobs:
            job = self._jobs[job_id]
            execution = JobExecution(
                job_id=job_id,
                started_at=datetime.now(timezone.utc),
            )
            try:
                job.func()
                execution.complete(success=True)
                logger.info(f"Manual execution of {job_id} completed")
                return True
            except Exception as e:
                execution.complete(success=False, error=str(e))
                logger.error(f"Manual execution of {job_id} failed: {e}")
                raise
            finally:
                job.add_execution(execution)
        return False

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    def get_all_jobs(self) -> list[ScheduledJob]:
        """Get all registered jobs."""
        return list(self._jobs.values())

    def get_job_status(self, job_id: str) -> Optional[dict]:
        """
        Get status information for a job.

        Args:
            job_id: Job identifier

        Returns:
            Status dictionary or None
        """
        job = self._jobs.get(job_id)
        if not job:
            return None

        next_run = None
        if self._scheduler and self._running:
            try:
                apjob = self._scheduler.get_job(job_id)
                if apjob and apjob.next_run_time:
                    next_run = apjob.next_run_time.isoformat()
            except Exception:
                logger.debug("Non-critical scheduler operation failed", exc_info=True)

        return {
            "job_id": job.job_id,
            "name": job.name,
            "enabled": job.enabled,
            "schedule_type": job.schedule.schedule_type.value,
            "next_run": next_run,
            "last_execution": (
                {
                    "started_at": job.last_execution.started_at.isoformat(),
                    "success": job.last_execution.success,
                    "duration_seconds": job.last_execution.duration_seconds,
                }
                if job.last_execution
                else None
            ),
            "success_rate": round(job.success_rate, 4),
            "total_executions": len(job.history),
        }

    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            logger.warning("Scheduler is already running")
            return

        logger.info("Starting scheduler...")
        self._init_scheduler()

        for job in self._jobs.values():
            if job.enabled:
                self._schedule_job(job)

        self._scheduler.start()
        self._running = True

        atexit.register(self.shutdown)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(
            f"Scheduler started with {len(self._jobs)} jobs "
            f"({sum(1 for j in self._jobs.values() if j.enabled)} enabled)"
        )

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler gracefully.

        Args:
            wait: Whether to wait for running jobs to complete
        """
        if not self._running:
            return

        logger.info("Shutting down scheduler...")
        self._running = False

        if self._scheduler:
            self._scheduler.shutdown(wait=wait)
            self._scheduler = None

        logger.info("Scheduler shutdown complete")

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
        sys.exit(0)

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running


def create_daily_pipeline_scheduler(
    pipeline_func: Callable,
    hour: int = 6,
    minute: int = 0,
    job_name: str = "daily_pipeline",
) -> PipelineScheduler:
    """
    Create a scheduler configured for daily pipeline runs.

    Args:
        pipeline_func: Function to run daily
        hour: Hour to run (0-23)
        minute: Minute to run (0-59)
        job_name: Name for the job

    Returns:
        Configured PipelineScheduler
    """
    scheduler = PipelineScheduler(persist_jobs=True)
    scheduler.add_job(
        job_id=job_name,
        name=f"Daily Pipeline ({hour:02d}:{minute:02d})",
        func=pipeline_func,
        schedule=ScheduleConfig.daily(hour=hour, minute=minute),
    )
    return scheduler
