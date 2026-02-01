"""Tests for Clock abstraction."""

import pytest

from limitpal import MockClock, MonotonicClock


class TestMonotonicClock:
    """Test MonotonicClock implementation."""

    def test_now_returns_float(self) -> None:
        clock = MonotonicClock()
        assert isinstance(clock.now(), float)

    def test_now_increases(self) -> None:
        clock = MonotonicClock()
        t1 = clock.now()
        t2 = clock.now()
        assert t2 >= t1

    def test_sleep_blocks(self) -> None:
        import time

        clock = MonotonicClock()
        duration = 0.05

        start = time.monotonic()
        clock.sleep(duration)
        elapsed = time.monotonic() - start

        assert elapsed >= duration * 0.9

    def test_sleep_zero(self) -> None:
        clock = MonotonicClock()
        clock.sleep(0)

    def test_sleep_negative(self) -> None:
        clock = MonotonicClock()
        clock.sleep(-1)


@pytest.mark.asyncio
class TestMonotonicClockAsync:
    """Test MonotonicClock async methods."""

    async def test_sleep_async_blocks(self) -> None:
        import time

        clock = MonotonicClock()
        duration = 0.05

        start = time.monotonic()
        await clock.sleep_async(duration)
        elapsed = time.monotonic() - start

        assert elapsed >= duration * 0.9

    async def test_sleep_async_zero(self) -> None:
        clock = MonotonicClock()
        await clock.sleep_async(0)

    async def test_sleep_async_negative(self) -> None:
        clock = MonotonicClock()
        await clock.sleep_async(-1)


class TestMockClock:
    """Test MockClock implementation."""

    def test_initial_time(self) -> None:
        clock = MockClock(start_time=100.0)
        assert clock.now() == 100.0

    def test_default_initial_time(self) -> None:
        clock = MockClock()
        assert clock.now() == 0.0

    def test_advance(self) -> None:
        clock = MockClock()
        clock.advance(5.0)
        assert clock.now() == 5.0
        clock.advance(3.0)
        assert clock.now() == 8.0

    def test_advance_fractional(self) -> None:
        clock = MockClock()
        clock.advance(0.5)
        assert clock.now() == 0.5
        clock.advance(0.25)
        assert clock.now() == 0.75

    def test_advance_negative_raises(self) -> None:
        clock = MockClock()
        with pytest.raises(ValueError, match="negative"):
            clock.advance(-1.0)

    def test_set_time(self) -> None:
        clock = MockClock()
        clock.set_time(50.0)
        assert clock.now() == 50.0

    def test_set_time_backwards(self) -> None:
        clock = MockClock(start_time=100.0)
        clock.set_time(50.0)
        assert clock.now() == 50.0

    def test_sleep_advances_time(self) -> None:
        clock = MockClock()
        clock.sleep(5.0)
        assert clock.now() == 5.0

    def test_sleep_zero(self) -> None:
        clock = MockClock()
        clock.sleep(0)
        assert clock.now() == 0.0

    def test_sleep_negative(self) -> None:
        clock = MockClock()
        clock.sleep(-1)
        assert clock.now() == 0.0


@pytest.mark.asyncio
class TestMockClockAsync:
    """Test MockClock async methods."""

    async def test_sleep_async_advances_time(self) -> None:
        clock = MockClock()
        await clock.sleep_async(5.0)
        assert clock.now() == 5.0

    async def test_sleep_async_zero(self) -> None:
        clock = MockClock()
        await clock.sleep_async(0)
        assert clock.now() == 0.0

    async def test_sleep_async_negative(self) -> None:
        clock = MockClock()
        await clock.sleep_async(-1)
        assert clock.now() == 0.0


class TestMockClockDeterminism:
    """Test MockClock deterministic behavior."""

    def test_deterministic_sequence(self) -> None:
        def run_sequence() -> list[float]:
            clock = MockClock()
            results = [clock.now()]
            clock.advance(1.0)
            results.append(clock.now())
            clock.sleep(0.5)
            results.append(clock.now())
            return results

        assert run_sequence() == run_sequence()

    def test_no_real_time_dependency(self) -> None:
        import time

        clock = MockClock()
        t1 = clock.now()
        time.sleep(0.1)
        t2 = clock.now()

        assert t1 == t2
