"""Unit tests for rate limiting utilities."""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, patch, MagicMock

from google_drive_worker.utils.rate_limit import (
    ExponentialBackoff,
    RateLimitHandler,
    AdaptiveRateLimiter,
    with_rate_limit_retry,
)
from google_drive_worker.utils.errors import RateLimitError


@pytest.mark.asyncio
class TestExponentialBackoff:
    """Test cases for ExponentialBackoff."""

    def test_backoff_calculation(self):
        """Test exponential backoff delay calculation."""
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=30.0, multiplier=2.0, jitter=False)

        # First attempt
        assert backoff.next_delay() == 1.0
        # Second attempt
        assert backoff.next_delay() == 2.0
        # Third attempt
        assert backoff.next_delay() == 4.0
        # Fourth attempt
        assert backoff.next_delay() == 8.0
        # Fifth attempt
        assert backoff.next_delay() == 16.0
        # Sixth attempt (should cap at max_delay)
        assert backoff.next_delay() == 30.0
        # Seventh attempt (should still be capped)
        assert backoff.next_delay() == 30.0

    def test_backoff_with_jitter(self):
        """Test that jitter adds randomness to delays."""
        backoff = ExponentialBackoff(base_delay=10.0, jitter=True)

        delays = [backoff.get_delay_for_attempt(0) for _ in range(10)]

        # With jitter, delays should vary
        assert len(set(delays)) > 1
        # But should be within expected range (±25%)
        for delay in delays:
            assert 7.5 <= delay <= 12.5

    def test_backoff_reset(self):
        """Test resetting backoff counter."""
        backoff = ExponentialBackoff(base_delay=1.0, jitter=False)

        # Advance a few attempts
        backoff.next_delay()
        backoff.next_delay()
        assert backoff._attempt == 2

        # Reset
        backoff.reset()
        assert backoff._attempt == 0

        # Should start from beginning
        assert backoff.next_delay() == 1.0

    async def test_backoff_wait(self):
        """Test async wait method."""
        backoff = ExponentialBackoff(base_delay=0.01, jitter=False)

        start = time.time()
        await backoff.wait()
        elapsed = time.time() - start

        # Should wait approximately base_delay
        assert 0.005 <= elapsed <= 0.05

    def test_get_delay_for_attempt(self):
        """Test getting delay for specific attempt without changing state."""
        backoff = ExponentialBackoff(base_delay=2.0, max_delay=50.0, jitter=False)

        # Get delays without changing internal state
        assert backoff.get_delay_for_attempt(0) == 2.0
        assert backoff.get_delay_for_attempt(1) == 4.0
        assert backoff.get_delay_for_attempt(2) == 8.0
        assert backoff.get_delay_for_attempt(3) == 16.0
        assert backoff.get_delay_for_attempt(4) == 32.0
        assert backoff.get_delay_for_attempt(5) == 50.0  # Capped at max

        # Internal state should still be at 0
        assert backoff._attempt == 0


@pytest.mark.asyncio
class TestRateLimitHandler:
    """Test cases for RateLimitHandler."""

    async def test_handle_with_retry_after(self):
        """Test handling rate limit with Retry-After header."""
        handler = RateLimitHandler()

        start = time.time()
        await handler.handle_rate_limit(retry_after=0.05)  # 50ms
        elapsed = time.time() - start

        # Should respect Retry-After
        assert 0.04 <= elapsed <= 0.1

        # Backoff should be reset after explicit retry_after
        assert handler.backoff._attempt == 0

    async def test_handle_with_backoff(self):
        """Test handling rate limit with exponential backoff."""
        backoff = ExponentialBackoff(base_delay=0.01, jitter=False)
        handler = RateLimitHandler(backoff=backoff)

        start = time.time()
        await handler.handle_rate_limit(retry_after=None, use_backoff=True)
        elapsed = time.time() - start

        # Should use backoff delay
        assert 0.005 <= elapsed <= 0.05

    async def test_handle_with_default_delay(self):
        """Test handling rate limit with default delay."""
        handler = RateLimitHandler(default_retry_after=0.05)

        start = time.time()
        await handler.handle_rate_limit(retry_after=None, use_backoff=False)
        elapsed = time.time() - start

        # Should use default delay
        assert 0.04 <= elapsed <= 0.1

    def test_handler_reset(self):
        """Test resetting rate limit handler."""
        handler = RateLimitHandler()

        # Advance backoff
        handler.backoff._attempt = 5

        # Reset
        handler.reset()

        # Backoff should be reset
        assert handler.backoff._attempt == 0


@pytest.mark.asyncio
class TestWithRateLimitRetry:
    """Test cases for with_rate_limit_retry decorator."""

    async def test_successful_call(self):
        """Test decorator with successful function call."""
        call_count = 0

        @with_rate_limit_retry(max_attempts=3)
        async def test_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await test_func()

        assert result == "success"
        assert call_count == 1

    async def test_rate_limit_retry(self):
        """Test decorator retrying on rate limit error."""
        call_count = 0

        @with_rate_limit_retry(max_attempts=3, backoff=ExponentialBackoff(base_delay=0.01))
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RateLimitError("Rate limited", retry_after=0.01)
            return "success"

        result = await test_func()

        assert result == "success"
        assert call_count == 3

    async def test_rate_limit_exhausted(self):
        """Test decorator when max attempts exhausted."""
        call_count = 0

        @with_rate_limit_retry(max_attempts=2)
        async def test_func():
            nonlocal call_count
            call_count += 1
            raise RateLimitError("Rate limited", retry_after=0.01)

        with pytest.raises(RateLimitError) as exc_info:
            await test_func()

        assert call_count == 2
        assert "Rate limited" in str(exc_info.value)

    async def test_non_rate_limit_error(self):
        """Test decorator with non-rate-limit error."""
        call_count = 0

        @with_rate_limit_retry(max_attempts=3)
        async def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Different error")

        with pytest.raises(ValueError) as exc_info:
            await test_func()

        # Should not retry on non-RateLimitError
        assert call_count == 1
        assert "Different error" in str(exc_info.value)


class TestAdaptiveRateLimiter:
    """Test cases for AdaptiveRateLimiter."""

    def test_initial_rate(self):
        """Test initial rate configuration."""
        limiter = AdaptiveRateLimiter(initial_rate=5.0)

        assert limiter.current_rate == 5.0
        assert limiter.get_delay() == 0.2  # 1/5

    def test_record_success_increases_rate(self):
        """Test that success increases rate."""
        limiter = AdaptiveRateLimiter(
            initial_rate=10.0,
            increase_factor=1.5,
            max_rate=20.0
        )

        limiter.record_success()
        assert limiter.current_rate == 15.0

        limiter.record_success()
        assert limiter.current_rate == 20.0  # Capped at max

    def test_record_rate_limit_decreases_rate(self):
        """Test that rate limit decreases rate."""
        limiter = AdaptiveRateLimiter(
            initial_rate=10.0,
            decrease_factor=0.5,
            min_rate=1.0
        )

        limiter.record_rate_limit()
        assert limiter.current_rate == 5.0

        limiter.record_rate_limit()
        assert limiter.current_rate == 2.5

        limiter.record_rate_limit()
        assert limiter.current_rate == 1.25

        limiter.record_rate_limit()
        assert limiter.current_rate == 1.0  # Capped at min

    @pytest.mark.asyncio
    async def test_acquire_delays(self):
        """Test that acquire introduces appropriate delay."""
        limiter = AdaptiveRateLimiter(initial_rate=100.0)  # 100 req/s = 0.01s delay

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        # Should delay approximately 0.01 seconds
        assert 0.005 <= elapsed <= 0.02

    def test_reset_to_default(self):
        """Test resetting rate limiter."""
        limiter = AdaptiveRateLimiter(initial_rate=10.0)

        # Change rate
        limiter.record_rate_limit()
        assert limiter.current_rate != 10.0

        # Reset to default
        limiter.reset()
        assert limiter.current_rate == 10.0

    def test_reset_to_specific_rate(self):
        """Test resetting to specific rate."""
        limiter = AdaptiveRateLimiter(min_rate=1.0, max_rate=100.0)

        limiter.reset(50.0)
        assert limiter.current_rate == 50.0

        # Should respect bounds
        limiter.reset(200.0)
        assert limiter.current_rate == 100.0

        limiter.reset(0.5)
        assert limiter.current_rate == 1.0

    def test_zero_rate_infinite_delay(self):
        """Test that zero rate returns infinite delay."""
        limiter = AdaptiveRateLimiter(initial_rate=0.0)

        delay = limiter.get_delay()
        assert delay == float("inf")