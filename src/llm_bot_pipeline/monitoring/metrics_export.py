"""
Metrics export backends for pipeline metrics.

Supports Cloud Monitoring and Prometheus push gateway.
Export is optional and non-blocking; failures are logged and never raised.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Key metric names
RECORDS_PROCESSED = "records_processed"
RECORDS_FAILED = "records_failed"
PIPELINE_DURATION_SECONDS = "pipeline_duration_seconds"
BATCH_FLUSH_ERRORS = "batch_flush_errors"


def _extract_metrics(execution: Any) -> dict[str, float | int]:
    """Extract key metrics from ExecutionMetrics for export."""
    records_processed = getattr(execution, "output_rows", 0) or getattr(
        execution, "input_rows", 0
    )
    records_failed = getattr(execution, "error_rows", 0)
    duration = getattr(execution, "duration_seconds", None)
    pipeline_duration = float(duration) if duration is not None else 0.0
    custom = getattr(execution, "custom_metrics", {}) or {}
    batch_flush_errors = int(custom.get(BATCH_FLUSH_ERRORS, 0))

    return {
        RECORDS_PROCESSED: int(records_processed),
        RECORDS_FAILED: int(records_failed),
        PIPELINE_DURATION_SECONDS: pipeline_duration,
        BATCH_FLUSH_ERRORS: batch_flush_errors,
    }


def _export_prometheus(
    metrics: dict[str, float | int],
    pushgateway_url: str,
    job: str,
    pipeline_name: str,
) -> None:
    """Push metrics to Prometheus push gateway."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        g_records = Gauge(
            "llm_bot_pipeline_records_processed",
            "Records processed by pipeline",
            ["pipeline"],
            registry=registry,
        )
        g_failed = Gauge(
            "llm_bot_pipeline_records_failed",
            "Records failed by pipeline",
            ["pipeline"],
            registry=registry,
        )
        g_duration = Gauge(
            "llm_bot_pipeline_duration_seconds",
            "Pipeline duration in seconds",
            ["pipeline"],
            registry=registry,
        )
        g_batch_errors = Gauge(
            "llm_bot_pipeline_batch_flush_errors",
            "Batch flush errors",
            ["pipeline"],
            registry=registry,
        )

        g_records.labels(pipeline=pipeline_name).set(metrics[RECORDS_PROCESSED])
        g_failed.labels(pipeline=pipeline_name).set(metrics[RECORDS_FAILED])
        g_duration.labels(pipeline=pipeline_name).set(
            metrics[PIPELINE_DURATION_SECONDS]
        )
        g_batch_errors.labels(pipeline=pipeline_name).set(metrics[BATCH_FLUSH_ERRORS])

        push_to_gateway(pushgateway_url, job=job, registry=registry)
        logger.debug(
            "Exported metrics to Prometheus push gateway: %s",
            pushgateway_url,
        )
    except ImportError as e:
        logger.warning(
            "Prometheus export skipped: prometheus_client not installed: %s",
            e,
        )
    except Exception as e:
        logger.warning(
            "Prometheus metrics export failed (non-blocking): %s",
            e,
            exc_info=True,
        )


def _export_cloud_monitoring(
    metrics: dict[str, float | int],
    project_id: str,
    pipeline_name: str,
) -> None:
    """Push metrics to Google Cloud Monitoring."""
    try:
        from google.cloud import monitoring_v3
        from google.protobuf.timestamp_pb2 import Timestamp

        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{project_id}"

        now = Timestamp()
        now.GetCurrentTime()

        time_series_list = []
        for metric_type, value_type, value in [
            (
                "custom.googleapis.com/llm_bot_pipeline/records_processed",
                monitoring_v3.MetricDescriptor.ValueType.INT64,
                metrics[RECORDS_PROCESSED],
            ),
            (
                "custom.googleapis.com/llm_bot_pipeline/records_failed",
                monitoring_v3.MetricDescriptor.ValueType.INT64,
                metrics[RECORDS_FAILED],
            ),
            (
                "custom.googleapis.com/llm_bot_pipeline/duration_seconds",
                monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
                metrics[PIPELINE_DURATION_SECONDS],
            ),
            (
                "custom.googleapis.com/llm_bot_pipeline/batch_flush_errors",
                monitoring_v3.MetricDescriptor.ValueType.INT64,
                metrics[BATCH_FLUSH_ERRORS],
            ),
        ]:
            s = monitoring_v3.TimeSeries()
            s.metric.type = metric_type
            s.metric.labels["pipeline"] = pipeline_name
            s.resource.type = "global"
            s.resource.labels["project_id"] = project_id
            s.metric_kind = monitoring_v3.MetricDescriptor.MetricKind.GAUGE
            s.value_type = value_type
            p = monitoring_v3.Point()
            p.interval.end_time.CopyFrom(now)
            if value_type == monitoring_v3.MetricDescriptor.ValueType.INT64:
                p.value.int64_value = int(value)
            else:
                p.value.double_value = float(value)
            s.points = [p]
            time_series_list.append(s)

        client.create_time_series(name=project_name, time_series=time_series_list)

        logger.debug(
            "Exported metrics to Cloud Monitoring: project=%s",
            project_id,
        )
    except ImportError as e:
        logger.warning(
            "Cloud Monitoring export skipped: google-cloud-monitoring not installed: %s",
            e,
        )
    except Exception as e:
        logger.warning(
            "Cloud Monitoring metrics export failed (non-blocking): %s",
            e,
            exc_info=True,
        )


def export_metrics(
    execution: Any,
    backend: str,
    *,
    project_id: str = "",
    pushgateway_url: str = "http://localhost:9091",
    job: str = "llm_bot_pipeline",
) -> None:
    """
    Export execution metrics to the configured backend.

    Non-blocking: logs warnings on failure, never raises.

    Args:
        execution: ExecutionMetrics instance
        backend: "prometheus" or "cloud_monitoring"
        project_id: GCP project ID (required for cloud_monitoring)
        pushgateway_url: Prometheus push gateway URL
        job: Job name for Prometheus grouping
    """
    if execution is None:
        return

    metrics = _extract_metrics(execution)
    pipeline_name = getattr(execution, "pipeline_name", "unknown")

    if backend == "prometheus":
        _export_prometheus(metrics, pushgateway_url, job, pipeline_name)
    elif backend == "cloud_monitoring":
        if not project_id:
            logger.warning("Cloud Monitoring export skipped: project_id required")
            return
        _export_cloud_monitoring(metrics, project_id, pipeline_name)
    else:
        logger.warning("Unknown metrics backend: %s", backend)
