"""Rate limiting utilities for Google Drive API integration.

Provides exponential backoff and rate limit handling for API requests.
"""

import asyncio
import logging
import random
from typing import Optional, TypeVar, Callable, Any
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExponentialBackoff:
    """Configurable exponential backoff implementation.

    Used for handling rate limits and transient failures with the Google Drive API.
    Follows best practices for API retry strategies.
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True,
    ):
        """Initialize exponential backoff configuration.

        Args:
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            multiplier: Multiplier for each retry (e.g., 2.0 for doubling)
            jitter: Whether to add random jitter to prevent thundering herd
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self._attempt = 0

    def reset(self) -> None:
        """Reset the backoff counter to initial state."""
        self._attempt = 0

    def next_delay(self) -> float:
        """Calculate the next delay based on attempt count.

        Returns:
            Delay in seconds for the next retry
        """
        # Calculate exponential delay
        delay = min(
            self.base_delay * (self.multiplier ** self._attempt),
            self.max_delay
        )

        # Add jitter if enabled (±25% random variation)
        if self.jitter:
            jitter_range = delay * 0.25
            delay = delay + random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay)  # Ensure non-negative

        self._attempt += 1

        logger.debug(
            "Calculated backoff delay",
            attempt=self._attempt,
            delay_seconds=round(delay, 2),
        )

        return delay

    async def wait(self) -> None:
        """Wait for the next backoff delay.

        This is an async method that sleeps for the calculated delay.
        """
        delay = self.next_delay()
        logger.info(
            f"Backing off for {delay:.2f} seconds",
            attempt=self._attempt,
        )
        await asyncio.sleep(delay)

    def get_delay_for_attempt(self, attempt: int) -> float:
        """Calculate delay for a specific attempt number without changing state.

        Args:
            attempt: The attempt number (0-based)

        Returns:
            Delay in seconds for that attempt
        """
        delay = min(
            self.base_delay * (self.multiplier ** attempt),
            self.max_delay
        )

        if self.jitter:
            jitter_range = delay * 0.25
            delay = delay + random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay)

        return delay


class RateLimitHandler:
    """Handle rate limits with respect for Retry-After headers.

    This class manages rate limit responses from the Google Drive API,
    respecting Retry-After headers and applying exponential backoff
    when the header is not present.
    """

    def __init__(
        self,
        default_retry_after: int = 60,
        backoff: Optional[ExponentialBackoff] = None,
    ):
        """Initialize rate limit handler.

        Args:
            default_retry_after: Default seconds to wait if no Retry-After header
            backoff: Optional ExponentialBackoff instance for calculating delays
        """
        self.default_retry_after = default_retry_after
        self.backoff = backoff or ExponentialBackoff(
            base_delay=2.0,
            max_delay=120.0,
            multiplier=2.0,
        )

    async def handle_rate_limit(
        self,
        retry_after: Optional[int] = None,
        use_backoff: bool = True,
    ) -> None:
        """Handle a rate limit by waiting the appropriate time.

        Args:
            retry_after: Seconds to wait from Retry-After header
            use_backoff: Whether to use exponential backoff if no retry_after
        """
        if retry_after is not None:
            # Respect the Retry-After header
            logger.info(
                f"Rate limited - waiting {retry_after} seconds as requested",
                retry_after=retry_after,
            )
            await asyncio.sleep(retry_after)
            # Reset backoff since we got explicit guidance
            self.backoff.reset()
        elif use_backoff:
            # Use exponential backoff
            await self.backoff.wait()
        else:
            # Use default wait time
            logger.info(
                f"Rate limited - waiting default {self.default_retry_after} seconds",
                default_retry_after=self.default_retry_after,
            )
            await asyncio.sleep(self.default_retry_after)

    def reset(self) -> None:
        """Reset the rate limit handler state."""
        self.backoff.reset()


def with_rate_limit_retry(
    max_attempts: int = 3,
    backoff: Optional[ExponentialBackoff] = None,
):
    """Decorator for adding rate limit retry logic to async functions.

    This decorator catches RateLimitError exceptions and automatically
    retries with appropriate backoff.

    Args:
        max_attempts: Maximum number of attempts including the initial call
        backoff: Optional ExponentialBackoff configuration

    Example:
        @with_rate_limit_retry(max_attempts=5)
        async def fetch_data():
            # API call that might hit rate limits
            pass
    """
    from ..utils.errors import RateLimitError

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            handler = RateLimitHandler(backoff=backoff)
            last_error = None

            for attempt in range(max_attempts):
                try:
                    # Reset handler if this is a retry after success
                    if attempt == 0:
                        handler.reset()

                    result = await func(*args, **kwargs)
                    return result

                except RateLimitError as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Rate limit hit in {func.__name__}, attempt {attempt + 1}/{max_attempts}, retry_after=%s",
                            e.retry_after,
                        )
                        await handler.handle_rate_limit(
                            retry_after=e.retry_after,
                            use_backoff=e.retry_after is None,
                        )
                    else:
                        logger.error(
                            f"Rate limit exhausted after {max_attempts} attempts in {func.__name__}",
                        )
                        raise

            # Should not reach here, but handle gracefully
            if last_error:
                raise last_error
            raise RuntimeError(f"Unexpected retry loop exit in {func.__name__}")

        return wrapper
    return decorator


class AdaptiveRateLimiter:
    """Adaptive rate limiter that learns from API responses.

    This class tracks successful requests and rate limit responses
    to adaptively adjust request rates and prevent hitting limits.
    """

    def __init__(
        self,
        initial_rate: float = 10.0,  # requests per second
        min_rate: float = 0.1,
        max_rate: float = 100.0,
        decrease_factor: float = 0.5,
        increase_factor: float = 1.1,
    ):
        """Initialize adaptive rate limiter.

        Args:
            initial_rate: Initial requests per second
            min_rate: Minimum requests per second
            max_rate: Maximum requests per second
            decrease_factor: Factor to decrease rate on rate limit
            increase_factor: Factor to increase rate on success
        """
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.decrease_factor = decrease_factor
        self.increase_factor = increase_factor
        self._last_request_time = 0.0

    def get_delay(self) -> float:
        """Calculate delay needed before next request.

        Returns:
            Seconds to wait before making next request
        """
        if self.current_rate <= 0:
            return float("inf")

        interval = 1.0 / self.current_rate
        return interval

    async def acquire(self) -> None:
        """Wait if necessary to respect rate limit."""
        delay = self.get_delay()
        if delay > 0:
            await asyncio.sleep(delay)

    def record_success(self) -> None:
        """Record a successful request and potentially increase rate."""
        new_rate = min(
            self.current_rate * self.increase_factor,
            self.max_rate
        )
        if new_rate != self.current_rate:
            logger.debug(
                "Increasing request rate after success",
                old_rate=round(self.current_rate, 2),
                new_rate=round(new_rate, 2),
            )
            self.current_rate = new_rate

    def record_rate_limit(self) -> None:
        """Record a rate limit response and decrease rate."""
        new_rate = max(
            self.current_rate * self.decrease_factor,
            self.min_rate
        )
        if new_rate != self.current_rate:
            logger.info(
                "Decreasing request rate after rate limit",
                old_rate=round(self.current_rate, 2),
                new_rate=round(new_rate, 2),
            )
            self.current_rate = new_rate

    def reset(self, rate: Optional[float] = None) -> None:
        """Reset the rate limiter to initial or specified rate.

        Args:
            rate: Optional new rate to set
        """
        if rate is not None:
            self.current_rate = max(self.min_rate, min(rate, self.max_rate))
        else:
            self.current_rate = 10.0  # Default initial rate

        logger.info(
            "Rate limiter reset",
            new_rate=round(self.current_rate, 2),
        )