"""
Retry handling with exponential backoff for pipeline operations.

Provides robust error handling with:
- Exponential backoff with jitter
- Configurable retry limits
- Circuit breaker pattern
- Error classification
"""

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

# Type variable for generic function decorators
F = TypeVar("F", bound=Callable[..., Any])


class ErrorCategory(Enum):
    """Categories of errors for retry decisions."""

    TRANSIENT = "transient"  # Retry immediately
    RATE_LIMITED = "rate_limited"  # Wait and retry
    SERVICE_UNAVAILABLE = "service_unavailable"  # Wait longer and retry
    PERMANENT = "permanent"  # Do not retry
    UNKNOWN = "unknown"  # Retry with caution


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay before next retry attempt.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = min(
            self.base_delay_seconds * (self.exponential_base**attempt),
            self.max_delay_seconds,
        )

        if self.jitter:
            jitter_range = delay * self.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)


@dataclass
class RetryResult:
    """Result of a retry operation."""

    success: bool
    result: Any = None
    attempts: int = 0
    total_delay_seconds: float = 0.0
    last_error: Optional[Exception] = None
    errors: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "attempts": self.attempts,
            "total_delay_seconds": round(self.total_delay_seconds, 2),
            "last_error": str(self.last_error) if self.last_error else None,
            "error_count": len(self.errors),
        }


@dataclass
class CircuitBreakerState:
    """State for circuit breaker pattern."""

    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "closed"  # closed, open, half-open
    success_count_in_half_open: int = 0


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    Prevents cascading failures by temporarily blocking calls
    after a threshold of failures is reached.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_seconds: int = 30,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout_seconds: Time to wait before attempting recovery
            success_threshold: Successes needed in half-open to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = timedelta(seconds=recovery_timeout_seconds)
        self.success_threshold = success_threshold
        self.state = CircuitBreakerState()

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking calls)."""
        if self.state.state == "open":
            # Check if recovery timeout has passed
            if self.state.last_failure_time:
                elapsed = datetime.utcnow() - self.state.last_failure_time
                if elapsed >= self.recovery_timeout:
                    self.state.state = "half-open"
                    self.state.success_count_in_half_open = 0
                    logger.info("Circuit breaker moving to half-open state")
                    return False
            return True
        return False

    def record_success(self) -> None:
        """Record a successful operation."""
        if self.state.state == "half-open":
            self.state.success_count_in_half_open += 1
            if self.state.success_count_in_half_open >= self.success_threshold:
                self.state.state = "closed"
                self.state.failure_count = 0
                logger.info("Circuit breaker closed after recovery")
        elif self.state.state == "closed":
            self.state.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.state.failure_count += 1
        self.state.last_failure_time = datetime.utcnow()

        if self.state.state == "half-open":
            self.state.state = "open"
            logger.warning("Circuit breaker opened again after half-open failure")
        elif self.state.failure_count >= self.failure_threshold:
            self.state.state = "open"
            logger.warning(
                f"Circuit breaker opened after {self.state.failure_count} failures"
            )

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self.state = CircuitBreakerState()

    def get_state(self) -> dict:
        """Get current state as dictionary."""
        return {
            "state": self.state.state,
            "failure_count": self.state.failure_count,
            "last_failure_time": (
                self.state.last_failure_time.isoformat()
                if self.state.last_failure_time
                else None
            ),
        }


class ErrorClassifier:
    """
    Classifies errors to determine retry behavior.

    Maps exception types and error messages to retry categories.
    """

    # Patterns for transient errors (network issues, timeouts)
    TRANSIENT_PATTERNS = [
        "timeout",
        "connection refused",
        "connection reset",
        "temporary failure",
        "retry",
        "unavailable",
        "503",
        "504",
    ]

    # Patterns for rate limiting
    RATE_LIMIT_PATTERNS = [
        "rate limit",
        "too many requests",
        "quota exceeded",
        "429",
    ]

    # Patterns for permanent errors (do not retry)
    PERMANENT_PATTERNS = [
        "not found",
        "404",
        "invalid",
        "forbidden",
        "403",
        "unauthorized",
        "401",
        "permission denied",
        "bad request",
        "400",
    ]

    @classmethod
    def classify(cls, error: Exception) -> ErrorCategory:
        """
        Classify an error to determine retry behavior.

        Args:
            error: The exception to classify

        Returns:
            ErrorCategory for retry decisions
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        # Check patterns
        for pattern in cls.PERMANENT_PATTERNS:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.PERMANENT

        for pattern in cls.RATE_LIMIT_PATTERNS:
            if pattern in error_str:
                return ErrorCategory.RATE_LIMITED

        for pattern in cls.TRANSIENT_PATTERNS:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.TRANSIENT

        # Check specific exception types
        if isinstance(error, (TimeoutError, ConnectionError)):
            return ErrorCategory.TRANSIENT

        if isinstance(error, (ValueError, TypeError, KeyError)):
            return ErrorCategory.PERMANENT

        return ErrorCategory.UNKNOWN


class RetryManager:
    """
    Manages retry logic for operations with configurable strategies.

    Combines exponential backoff, error classification, and circuit breaker.
    """

    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        """
        Initialize retry manager.

        Args:
            config: Retry configuration
            circuit_breaker: Optional circuit breaker instance
        """
        self.config = config or RetryConfig()
        self.circuit_breaker = circuit_breaker

    def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args,
        retry_on: Optional[list[ErrorCategory]] = None,
        **kwargs,
    ) -> RetryResult:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            retry_on: Error categories to retry on (default: transient, rate_limited)
            **kwargs: Keyword arguments for the function

        Returns:
            RetryResult with outcome and statistics
        """
        if retry_on is None:
            retry_on = [
                ErrorCategory.TRANSIENT,
                ErrorCategory.RATE_LIMITED,
                ErrorCategory.SERVICE_UNAVAILABLE,
                ErrorCategory.UNKNOWN,
            ]

        result = RetryResult(success=False)

        for attempt in range(self.config.max_retries + 1):
            result.attempts = attempt + 1

            # Check circuit breaker
            if self.circuit_breaker and self.circuit_breaker.is_open:
                logger.warning("Circuit breaker is open, skipping execution")
                result.last_error = Exception("Circuit breaker is open")
                result.errors.append(
                    {
                        "attempt": attempt + 1,
                        "error": "Circuit breaker is open",
                        "category": "circuit_breaker",
                    }
                )
                break

            try:
                result.result = func(*args, **kwargs)
                result.success = True

                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                logger.debug(f"Operation succeeded on attempt {attempt + 1}")
                break

            except Exception as e:
                category = ErrorClassifier.classify(e)
                error_info = {
                    "attempt": attempt + 1,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "category": category.value,
                }
                result.errors.append(error_info)
                result.last_error = e

                logger.warning(
                    f"Attempt {attempt + 1} failed: {e} (category: {category.value})"
                )

                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()

                # Check if we should retry
                if category not in retry_on:
                    logger.info(f"Not retrying: error category {category.value}")
                    break

                # Check if we've exhausted retries
                if attempt >= self.config.max_retries:
                    logger.error(f"Max retries ({self.config.max_retries}) exhausted")
                    break

                # Calculate and apply delay
                delay = self.config.calculate_delay(attempt)

                # Rate limited errors get extra delay
                if category == ErrorCategory.RATE_LIMITED:
                    delay *= 2
                elif category == ErrorCategory.SERVICE_UNAVAILABLE:
                    delay *= 3

                logger.info(f"Retrying in {delay:.2f} seconds...")
                result.total_delay_seconds += delay
                time.sleep(delay)

        return result


def with_retry(
    config: Optional[RetryConfig] = None,
    circuit_breaker: Optional[CircuitBreaker] = None,
) -> Callable[[F], F]:
    """
    Decorator to add retry logic to a function.

    Args:
        config: Retry configuration
        circuit_breaker: Optional circuit breaker instance

    Returns:
        Decorated function with retry logic

    Example:
        @with_retry(RetryConfig(max_retries=3))
        def fetch_data():
            return api.get_data()
    """
    manager = RetryManager(config=config, circuit_breaker=circuit_breaker)

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = manager.execute_with_retry(func, *args, **kwargs)
            if result.success:
                return result.result
            raise result.last_error or Exception("Operation failed after retries")

        return wrapper  # type: ignore

    return decorator


# Convenience functions for common retry patterns


def retry_transient(
    func: Callable[..., Any],
    *args,
    max_retries: int = 3,
    **kwargs,
) -> RetryResult:
    """
    Execute function with retry for transient errors only.

    Args:
        func: Function to execute
        *args: Function arguments
        max_retries: Maximum retry attempts
        **kwargs: Function keyword arguments

    Returns:
        RetryResult with outcome
    """
    manager = RetryManager(config=RetryConfig(max_retries=max_retries))
    return manager.execute_with_retry(
        func,
        *args,
        retry_on=[ErrorCategory.TRANSIENT],
        **kwargs,
    )


def retry_with_backoff(
    func: Callable[..., Any],
    *args,
    max_retries: int = 5,
    base_delay: float = 1.0,
    **kwargs,
) -> RetryResult:
    """
    Execute function with exponential backoff.

    Args:
        func: Function to execute
        *args: Function arguments
        max_retries: Maximum retry attempts
        base_delay: Initial delay in seconds
        **kwargs: Function keyword arguments

    Returns:
        RetryResult with outcome
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay_seconds=base_delay,
    )
    manager = RetryManager(config=config)
    return manager.execute_with_retry(func, *args, **kwargs)
