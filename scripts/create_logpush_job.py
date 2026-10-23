#!/usr/bin/env python3
"""
CLI script to create and manage Cloudflare Logpush job for LLM bot traffic.

Usage:
    python scripts/create_logpush_job.py create    # Create new job
    python scripts/create_logpush_job.py list      # List all jobs
    python scripts/create_logpush_job.py enable    # Enable job
    python scripts/create_logpush_job.py disable   # Disable job
    python scripts/create_logpush_job.py delete    # Delete job
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.cloudflare.logpush_job import (
    create_logpush_job,
    delete_logpush_job,
    disable_logpush_job,
    enable_logpush_job,
    find_job_by_name,
    list_logpush_jobs,
)
from llm_bot_pipeline.config.settings import get_settings


def cmd_create(settings) -> int:
    """Create new Logpush job."""
    print(f"Creating Logpush job: {settings.logpush_job_name}")
    print(f"   Zone: {settings.cloudflare_zone_id}")
    print(f"   Dataset: http_requests")
    print(f"   Destination: BigQuery {settings.gcp_project_id}.{settings.dataset_raw}")
    print()

    result = create_logpush_job(settings)

    if result.success:
        print(f"  {result.message}")
        print(f"   Job ID: {result.job_id}")
        return 0
    else:
        print(f"  {result.message}")
        if result.error:
            print(f"   Error: {result.error}")
        return 1


def cmd_list(settings) -> int:
    """List all Logpush jobs."""
    print(f"Listing Logpush jobs for zone: {settings.cloudflare_zone_id}")
    print()

    jobs = list_logpush_jobs(settings)

    if not jobs:
        print("   No jobs found.")
        return 0

    for job in jobs:
        status = "enabled" if job["enabled"] else "disabled"
        print(f"   [{job['id']}] {job['name']} - {status}")
        print(f"       Dataset: {job['dataset']}")
        print(f"       Destination: {job['destination']}")
        print()

    return 0


def cmd_enable(settings) -> int:
    """Enable the Logpush job."""
    result = find_job_by_name(settings, settings.logpush_job_name)

    if not result.job_id:
        print(f"Job not found: {settings.logpush_job_name}")
        return 1

    result = enable_logpush_job(settings, result.job_id)

    if result.success:
        print(f"Enabled job: {settings.logpush_job_name}")
        return 0
    else:
        print(f"Failed to enable job: {result.error}")
        return 1


def cmd_disable(settings) -> int:
    """Disable the Logpush job."""
    result = find_job_by_name(settings, settings.logpush_job_name)

    if not result.job_id:
        print(f"Job not found: {settings.logpush_job_name}")
        return 1

    result = disable_logpush_job(settings, result.job_id)

    if result.success:
        print(f"Disabled job: {settings.logpush_job_name}")
        return 0
    else:
        print(f"Failed to disable job: {result.error}")
        return 1


def cmd_delete(settings, yes: bool = False) -> int:
    """Delete the Logpush job."""
    result = find_job_by_name(settings, settings.logpush_job_name)

    if not result.job_id:
        print(f"Job not found: {settings.logpush_job_name}")
        return 1

    if not yes:
        confirm = input(
            f"Delete job '{settings.logpush_job_name}' (ID: {result.job_id})? [y/N]: "
        )
        if confirm.lower() != "y":
            print("   Cancelled.")
            return 0

    result = delete_logpush_job(settings, result.job_id)

    if result.success:
        print(f"Deleted job: {settings.logpush_job_name}")
        return 0
    else:
        print(f"Failed to delete job: {result.error}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Manage Cloudflare Logpush job for LLM bot traffic"
    )
    parser.add_argument(
        "command",
        choices=["create", "list", "enable", "disable", "delete"],
        help="Command to execute",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to SOPS-encrypted config file",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip interactive prompts",
    )

    args = parser.parse_args()

    settings = get_settings(args.config)

    errors = settings.validate()
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"   - {error}")
        return 1

    if not settings.cloudflare_api_token:
        print("Cloudflare API token not configured")
        return 1

    if not settings.cloudflare_zone_id:
        print("Cloudflare zone ID not configured")
        return 1

    if args.command == "delete":
        return cmd_delete(settings, yes=args.yes)

    commands = {
        "create": cmd_create,
        "list": cmd_list,
        "enable": cmd_enable,
        "disable": cmd_disable,
    }

    return commands[args.command](settings)


if __name__ == "__main__":
    sys.exit(main())
