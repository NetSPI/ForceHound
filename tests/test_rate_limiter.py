"""Tests for forcehound.utils.rate_limiter."""

import pytest
from forcehound.utils.rate_limiter import with_backoff


class TestWithBackoff:
    @pytest.mark.asyncio
    async def test_success_no_retry(self):
        call_count = 0

        @with_backoff(max_retries=3, base_delay=0.01)
        async def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await succeed()
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_then_succeed(self):
        call_count = 0

        @with_backoff(max_retries=3, base_delay=0.01)
        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient error")
            return "ok"

        result = await fail_then_succeed()
        assert result == "ok"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        call_count = 0

        @with_backoff(max_retries=2, base_delay=0.01)
        async def always_fail():
            nonlocal call_count
            call_count += 1
            raise ValueError("permanent error")

        with pytest.raises(ValueError, match="permanent error"):
            await always_fail()
        assert call_count == 3  # 1 initial + 2 retries

    @pytest.mark.asyncio
    async def test_specific_exception_types(self):
        call_count = 0

        @with_backoff(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(ConnectionError,),
        )
        async def wrong_exception():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            await wrong_exception()
        assert call_count == 1  # No retry for ValueError

    @pytest.mark.asyncio
    async def test_retryable_exception_retried(self):
        call_count = 0

        @with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ConnectionError,),
        )
        async def conn_error_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("retry me")
            return "ok"

        result = await conn_error_then_ok()
        assert result == "ok"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_zero_retries(self):
        call_count = 0

        @with_backoff(max_retries=0, base_delay=0.01)
        async def always_fail():
            nonlocal call_count
            call_count += 1
            raise ValueError("fail")

        with pytest.raises(ValueError):
            await always_fail()
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_preserves_function_name(self):
        @with_backoff(max_retries=1, base_delay=0.01)
        async def my_named_function():
            return True

        assert my_named_function.__name__ == "my_named_function"

    @pytest.mark.asyncio
    async def test_max_delay_caps_backoff(self):
        """Ensure the delay doesn't exceed max_delay."""
        call_count = 0

        @with_backoff(max_retries=5, base_delay=0.01, max_delay=0.02)
        async def fail_several():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise ValueError("fail")
            return "ok"

        result = await fail_several()
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_passes_args_and_kwargs(self):
        @with_backoff(max_retries=1, base_delay=0.01)
        async def add(a, b, extra=0):
            return a + b + extra

        result = await add(1, 2, extra=3)
        assert result == 6

    @pytest.mark.asyncio
    async def test_return_value_preserved(self):
        @with_backoff(max_retries=1, base_delay=0.01)
        async def return_dict():
            return {"key": "value", "nested": [1, 2, 3]}

        result = await return_dict()
        assert result == {"key": "value", "nested": [1, 2, 3]}
