#!/usr/bin/env python3
"""
CLI script to run the pipeline scheduler.

Usage:
    # Start scheduler with daily pipeline at 6 AM
    python scripts/run_scheduler.py --project-id PROJECT_ID --daily-hour 6

    # Start scheduler with hourly aggregations
    python scripts/run_scheduler.py --project-id PROJECT_ID --hourly-aggregations

    # Run pipeline immediately (one-off)
    python scripts/run_scheduler.py --project-id PROJECT_ID --run-now

    # List scheduled jobs
    python scripts/run_scheduler.py --project-id PROJECT_ID --list-jobs
"""

import argparse
import logging
import signal
import sys
import time
from datetime import date, timedelta
from pathlib import Path


def _handle_sigterm(signum, frame):
    raise KeyboardInterrupt()


signal.signal(signal.SIGTERM, _handle_sigterm)

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.monitoring import (
    PipelineScheduler,
    ScheduleConfig,
)
from llm_bot_pipeline.monitoring.pipeline_metrics import track_execution
from llm_bot_pipeline.monitoring.quality_runner import run_quality_checks_with_backend
from llm_bot_pipeline.pipeline.orchestrator import ETLPipeline

logger = logging.getLogger(__name__)


def create_pipeline_job(project_id: str, credentials_path: Path | None):
    """Create the daily pipeline job function."""

    def run_daily_pipeline():
        """Run the daily ETL pipeline with monitoring."""
        with track_execution("daily_etl_pipeline") as tracker:
            try:
                tracker.set_parameters(
                    project_id=project_id,
                    target_date=str(date.today() - timedelta(days=1)),
                )

                tracker.start_stage("initialize")
                pipeline = ETLPipeline(
                    project_id=project_id,
                    credentials_path=credentials_path,
                )
                tracker.complete_stage()

                tracker.start_stage("execute")
                result = pipeline.run_daily()
                tracker.complete_stage(
                    success=result.success,
                    rows_transformed=result.transformed_rows,
                )

                tracker.record_rows(
                    input_rows=result.raw_rows,
                    output_rows=result.transformed_rows,
                )

                if not result.success:
                    raise Exception(f"Pipeline failed: {result.errors}")

                logger.info(f"Daily pipeline completed: {result.transformed_rows} rows")

            except Exception as e:
                tracker.fail_stage(e)
                raise

    return run_daily_pipeline


def create_quality_check_job(project_id: str, credentials_path: Path | None):
    """Create the data quality check job function."""

    def run_quality_checks():
        """Run data quality checks."""
        with track_execution("data_quality_checks") as tracker:
            try:
                tracker.start_stage("quality_checks")

                from llm_bot_pipeline.config.settings import get_settings

                report = run_quality_checks_with_backend(
                    backend_type="bigquery",
                    domain_configs=get_settings().domains,
                    check_date=date.today() - timedelta(days=1),
                    project_id=project_id,
                    credentials_path=credentials_path,
                )

                tracker.complete_stage(
                    passed=report.summary["passed"],
                    warnings=report.summary["warnings"],
                    failed=report.summary["failed"],
                )

                tracker.record_metric("quality_status", report.overall_status.value)

                if not report.passed:
                    logger.warning(f"Quality check issues: {report.summary}")

            except Exception as e:
                tracker.fail_stage(e)
                raise

    return run_quality_checks


def create_aggregation_job(project_id: str, credentials_path: Path | None):
    """Create the hourly aggregation job function."""

    def run_aggregations():
        """Run aggregations."""
        from llm_bot_pipeline.reporting.aggregations import ReportingAggregator

        with track_execution("hourly_aggregations") as tracker:
            try:
                tracker.start_stage("aggregate")

                aggregator = ReportingAggregator(
                    project_id=project_id,
                    credentials_path=credentials_path,
                )

                result = aggregator.run_daily_summary(date.today())

                tracker.complete_stage(rows=result.get("rows_affected", 0))
                tracker.record_rows(output_rows=result.get("rows_affected", 0))

            except Exception as e:
                tracker.fail_stage(e)
                raise

    return run_aggregations


def setup_scheduler(
    project_id: str,
    credentials_path: Path | None,
    daily_hour: int = 6,
    daily_minute: int = 0,
    enable_quality_checks: bool = True,
    enable_hourly_aggregations: bool = False,
) -> PipelineScheduler:
    """
    Set up the pipeline scheduler with configured jobs.

    Args:
        project_id: GCP project ID
        credentials_path: Path to service account credentials
        daily_hour: Hour to run daily pipeline (0-23)
        daily_minute: Minute to run daily pipeline
        enable_quality_checks: Enable quality checks after pipeline
        enable_hourly_aggregations: Enable hourly aggregation runs

    Returns:
        Configured PipelineScheduler
    """
    scheduler = PipelineScheduler(persist_jobs=True)

    scheduler.add_job(
        job_id="daily_pipeline",
        name=f"Daily ETL Pipeline ({daily_hour:02d}:{daily_minute:02d})",
        func=create_pipeline_job(project_id, credentials_path),
        schedule=ScheduleConfig.daily(hour=daily_hour, minute=daily_minute),
    )
    logger.info(f"Added daily pipeline job at {daily_hour:02d}:{daily_minute:02d}")

    if enable_quality_checks:
        check_hour = daily_hour
        check_minute = daily_minute + 30
        if check_minute >= 60:
            check_minute -= 60
            check_hour = (check_hour + 1) % 24

        scheduler.add_job(
            job_id="quality_checks",
            name=f"Data Quality Checks ({check_hour:02d}:{check_minute:02d})",
            func=create_quality_check_job(project_id, credentials_path),
            schedule=ScheduleConfig.daily(hour=check_hour, minute=check_minute),
        )
        logger.info(f"Added quality check job at {check_hour:02d}:{check_minute:02d}")

    if enable_hourly_aggregations:
        scheduler.add_job(
            job_id="hourly_aggregations",
            name="Hourly Aggregations",
            func=create_aggregation_job(project_id, credentials_path),
            schedule=ScheduleConfig.hourly(minute=15),
        )
        logger.info("Added hourly aggregation job")

    return scheduler


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run the pipeline scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start scheduler with daily pipeline at 6 AM
  python scripts/run_scheduler.py --project-id my-project --daily-hour 6

  # Start scheduler with quality checks disabled
  python scripts/run_scheduler.py --project-id my-project --no-quality-checks

  # Run pipeline immediately
  python scripts/run_scheduler.py --project-id my-project --run-now

  # List scheduled jobs
  python scripts/run_scheduler.py --project-id my-project --list-jobs
        """,
    )

    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )

    parser.add_argument(
        "--daily-hour",
        type=int,
        default=6,
        help="Hour to run daily pipeline (0-23, default: 6)",
    )
    parser.add_argument(
        "--daily-minute",
        type=int,
        default=0,
        help="Minute to run daily pipeline (0-59, default: 0)",
    )
    parser.add_argument(
        "--no-quality-checks",
        action="store_true",
        help="Disable automated quality checks",
    )
    parser.add_argument(
        "--hourly-aggregations",
        action="store_true",
        help="Enable hourly aggregation runs",
    )

    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the pipeline immediately and exit",
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="List configured jobs and exit",
    )

    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="Path to service account JSON key file",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    credentials_path = Path(args.credentials) if args.credentials else None

    if args.run_now:
        print("\nRunning pipeline immediately...")
        try:
            job_func = create_pipeline_job(args.project_id, credentials_path)
            job_func()
            print("Pipeline completed successfully")
            return 0
        except Exception as e:
            print(f"Pipeline failed: {e}")
            return 1

    scheduler = setup_scheduler(
        project_id=args.project_id,
        credentials_path=credentials_path,
        daily_hour=args.daily_hour,
        daily_minute=args.daily_minute,
        enable_quality_checks=not args.no_quality_checks,
        enable_hourly_aggregations=args.hourly_aggregations,
    )

    if args.list_jobs:
        print("\nConfigured Jobs:")
        print("=" * 60)
        for job in scheduler.get_all_jobs():
            status = scheduler.get_job_status(job.job_id)
            enabled = "[ON]" if job.enabled else "[OFF]"
            print(f"  {enabled} {job.job_id}: {job.name}")
            print(f"      Schedule: {job.schedule.schedule_type.value}")
            if status and status.get("next_run"):
                print(f"      Next run: {status['next_run']}")
        return 0

    print("\nStarting Pipeline Scheduler")
    print("=" * 60)
    print(f"  Project: {args.project_id}")
    print(f"  Daily pipeline: {args.daily_hour:02d}:{args.daily_minute:02d} UTC")
    print(
        f"  Quality checks: {'Enabled' if not args.no_quality_checks else 'Disabled'}"
    )
    print(
        f"  Hourly aggregations: {'Enabled' if args.hourly_aggregations else 'Disabled'}"
    )
    print()
    print("Press Ctrl+C to stop the scheduler")
    print()

    try:
        scheduler.start()

        while scheduler.is_running:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
        scheduler.shutdown()
        print("Scheduler stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
