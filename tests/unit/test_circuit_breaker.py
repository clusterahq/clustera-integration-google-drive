"""Unit tests for circuit breaker pattern."""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock

from google_drive_worker.utils.circuit_breaker import CircuitBreaker, CircuitState


class TestCircuitBreaker:
    """Test circuit breaker behavior."""

    @pytest.fixture
    def circuit_breaker(self):
        """Create a circuit breaker instance."""
        return CircuitBreaker(
            name="test_breaker",
            failure_threshold=3,
            recovery_timeout=60,
            expected_exception=ValueError,
            half_open_max_calls=1,
        )

    @pytest.mark.asyncio
    async def test_initial_state(self, circuit_breaker):
        """Test that circuit breaker starts in CLOSED state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.last_failure_time is None

    @pytest.mark.asyncio
    async def test_successful_call(self, circuit_breaker):
        """Test successful calls through the circuit breaker."""
        async def successful_func():
            return "success"

        result = await circuit_breaker.call(successful_func)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_failure_increments_count(self, circuit_breaker):
        """Test that failures increment the failure count."""
        async def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await circuit_breaker.call(failing_func)

        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_threshold_opens_circuit(self, circuit_breaker):
        """Test that reaching failure threshold opens the circuit."""
        async def failing_func():
            raise ValueError("test error")

        # Fail 3 times to reach threshold
        for i in range(3):
            with pytest.raises(ValueError):
                await circuit_breaker.call(failing_func)

        assert circuit_breaker.failure_count == 3
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.last_failure_time is not None

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_calls(self, circuit_breaker):
        """Test that open circuit rejects new calls."""
        async def failing_func():
            raise ValueError("test error")

        # Open the circuit
        for i in range(3):
            with pytest.raises(ValueError):
                await circuit_breaker.call(failing_func)

        # Now circuit is open, next call should be rejected
        async def normal_func():
            return "success"

        with pytest.raises(Exception, match="Circuit breaker 'test_breaker' is OPEN"):
            await circuit_breaker.call(normal_func)

    @pytest.mark.asyncio
    async def test_recovery_timeout_transitions_to_half_open(self, circuit_breaker):
        """Test that after recovery timeout, circuit transitions to HALF_OPEN."""
        async def failing_func():
            raise ValueError("test error")

        # Open the circuit
        for i in range(3):
            with pytest.raises(ValueError):
                await circuit_breaker.call(failing_func)

        assert circuit_breaker.state == CircuitState.OPEN

        # Mock time to simulate timeout passing
        with patch.object(circuit_breaker, "_should_attempt_reset", return_value=True):
            async def test_func():
                return "success"

            # This should transition to HALF_OPEN and allow the call
            result = await circuit_breaker.call(test_func)
            assert result == "success"
            assert circuit_breaker.state == CircuitState.CLOSED
            assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_half_open_success_closes_circuit(self, circuit_breaker):
        """Test that successful call in HALF_OPEN state closes the circuit."""
        # Set to HALF_OPEN state
        circuit_breaker.state = CircuitState.HALF_OPEN

        async def successful_func():
            return "success"

        result = await circuit_breaker.call(successful_func)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens_circuit(self, circuit_breaker):
        """Test that failure in HALF_OPEN state reopens the circuit."""
        # Set to HALF_OPEN state
        circuit_breaker.state = CircuitState.HALF_OPEN
        circuit_breaker.last_failure_time = datetime.utcnow()

        async def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await circuit_breaker.call(failing_func)

        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_half_open_max_calls_limit(self, circuit_breaker):
        """Test that HALF_OPEN state respects max calls limit."""
        # Set to HALF_OPEN state
        circuit_breaker.state = CircuitState.HALF_OPEN
        circuit_breaker.half_open_max_calls = 2

        async def successful_func():
            return "success"

        # First call should succeed
        result = await circuit_breaker.call(successful_func)
        assert result == "success"

        # Second call should succeed
        result = await circuit_breaker.call(successful_func)
        assert result == "success"

        # Circuit should be closed now
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_unexpected_exception_not_counted(self, circuit_breaker):
        """Test that unexpected exceptions don't affect the circuit."""
        async def unexpected_error():
            raise RuntimeError("unexpected")

        with pytest.raises(RuntimeError):
            await circuit_breaker.call(unexpected_error)

        # Should not increment failure count
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == CircuitState.CLOSED

    def test_is_open_property(self, circuit_breaker):
        """Test is_open property."""
        assert not circuit_breaker.is_open
        circuit_breaker.state = CircuitState.OPEN
        assert circuit_breaker.is_open

    def test_is_closed_property(self, circuit_breaker):
        """Test is_closed property."""
        assert circuit_breaker.is_closed
        circuit_breaker.state = CircuitState.OPEN
        assert not circuit_breaker.is_closed

    def test_get_status(self, circuit_breaker):
        """Test get_status method."""
        status = circuit_breaker.get_status()
        assert status["name"] == "test_breaker"
        assert status["state"] == "closed"
        assert status["failure_count"] == 0
        assert status["threshold"] == 3
        assert status["recovery_timeout"] == 60

    @pytest.mark.asyncio
    async def test_reset_clears_state(self, circuit_breaker):
        """Test that reset clears all state."""
        # Create some failure state
        circuit_breaker.failure_count = 5
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.last_failure_time = datetime.utcnow()
        circuit_breaker.half_open_calls = 2

        # Reset
        circuit_breaker._reset()

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.half_open_calls == 0
        assert circuit_breaker.last_failure_time is None

    @pytest.mark.asyncio
    async def test_concurrent_calls_handling(self, circuit_breaker):
        """Test that circuit breaker handles concurrent calls correctly."""
        call_count = 0

        async def increment_func():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise ValueError("fail")
            return "success"

        # Run multiple concurrent calls
        tasks = [circuit_breaker.call(increment_func) for _ in range(5)]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # First 3 should fail with ValueError
        assert sum(1 for r in results if isinstance(r, ValueError)) == 3
        # Rest should be rejected due to open circuit
        assert sum(1 for r in results if isinstance(r, Exception) and "is OPEN" in str(r)) >= 1

        assert circuit_breaker.state == CircuitState.OPEN