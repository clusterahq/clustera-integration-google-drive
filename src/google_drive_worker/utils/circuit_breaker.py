"""Circuit breaker pattern for API calls.

Prevents cascading failures by temporarily blocking calls to failing services.
"""

from enum import Enum
from datetime import datetime, timedelta
from typing import Callable, Any, Optional, TypeVar, Coroutine
import asyncio
import structlog

T = TypeVar("T")

logger = structlog.get_logger()


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


class CircuitBreaker:
    """Simple circuit breaker for API calls.

    Monitors failures and temporarily blocks calls when a threshold is exceeded,
    preventing cascading failures and giving the service time to recover.
    """

    def __init__(
        self,
        name: str = "circuit_breaker",
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
        half_open_max_calls: int = 1,
    ):
        """Initialize circuit breaker.

        Args:
            name: Name for this circuit breaker (for logging)
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to count as failure
            half_open_max_calls: Max calls allowed in half-open state
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.half_open_max_calls = half_open_max_calls

        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
        self.half_open_calls = 0
        self.success_count = 0
        self.total_calls = 0

    async def call(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args,
        **kwargs,
    ) -> T:
        """Execute function through circuit breaker.

        Args:
            func: Async function to call
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result of func

        Raises:
            Exception: If circuit is open
            expected_exception: If func fails
        """
        self.total_calls += 1

        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
            else:
                logger.warning(
                    "Circuit breaker is OPEN",
                    name=self.name,
                    failure_count=self.failure_count,
                    last_failure=self.last_failure_time.isoformat()
                    if self.last_failure_time
                    else None,
                )
                raise Exception(f"Circuit breaker '{self.name}' is OPEN")

        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.half_open_max_calls:
                logger.debug(
                    "Circuit breaker half-open limit reached",
                    name=self.name,
                    calls=self.half_open_calls,
                    limit=self.half_open_max_calls,
                )
                raise Exception(
                    f"Circuit breaker '{self.name}' half-open limit reached"
                )
            self.half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result

        except self.expected_exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful call."""
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            logger.info(
                "Circuit breaker recovering",
                name=self.name,
                state=self.state.value,
            )
            self._reset()
        else:
            # In CLOSED state, reset failure count on success
            self.failure_count = 0

    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()

        if self.state == CircuitState.HALF_OPEN:
            logger.warning(
                "Circuit breaker recovery failed, reopening",
                name=self.name,
                failure_count=self.failure_count,
            )
            self._transition_to_open()

        elif self.failure_count >= self.failure_threshold:
            logger.error(
                "Circuit breaker threshold exceeded, opening",
                name=self.name,
                failure_count=self.failure_count,
                threshold=self.failure_threshold,
            )
            self._transition_to_open()

    def _transition_to_open(self):
        """Transition to OPEN state."""
        self.state = CircuitState.OPEN
        self.half_open_calls = 0

    def _transition_to_half_open(self):
        """Transition to HALF_OPEN state."""
        logger.info(
            "Circuit breaker attempting recovery",
            name=self.name,
            previous_state=self.state.value,
        )
        self.state = CircuitState.HALF_OPEN
        self.half_open_calls = 0

    def _reset(self):
        """Reset circuit breaker to CLOSED state."""
        logger.info(
            "Circuit breaker reset",
            name=self.name,
            previous_failures=self.failure_count,
            total_calls=self.total_calls,
            success_count=self.success_count,
        )
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
        self.last_failure_time = None

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset.

        Returns:
            True if recovery timeout has passed
        """
        if not self.last_failure_time:
            return False

        elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
        return elapsed >= self.recovery_timeout

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED

    def get_status(self) -> dict:
        """Get current circuit breaker status.

        Returns:
            Status dictionary
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_calls": self.total_calls,
            "last_failure": self.last_failure_time.isoformat()
            if self.last_failure_time
            else None,
            "threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }