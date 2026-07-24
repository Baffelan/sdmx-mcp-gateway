"""Shared retry policy for individual checks."""

import asyncio
from collections.abc import Awaitable, Callable

from storage import CheckResult

RETRY_DELAY_S = 5.0


async def with_retry(run: Callable[[], Awaitable[CheckResult]]) -> CheckResult:
    """Run a check; on failure, retry once after a short delay.

    A single network blip must not flip an endpoint's status. The second
    result wins and carries attempts=2.
    """
    import checks_common  # read module attribute at call time for test patching

    first = await run()
    if first.ok or first.skipped:
        return first
    await asyncio.sleep(checks_common.RETRY_DELAY_S)
    second = await run()
    second.attempts = 2
    return second
