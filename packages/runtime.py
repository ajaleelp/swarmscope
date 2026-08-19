import asyncio
import signal
from datetime import timedelta


async def sleep_unless_shutdown(shutdown: asyncio.Event, delay: timedelta) -> None:
    """Wait for the delay, but wake immediately if shutdown is requested.

    A plain sleep would leave a process unresponsive to SIGTERM for up to a full
    interval, which is long enough for Kubernetes to escalate to SIGKILL in the
    middle of useful work.
    """
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=delay.total_seconds())
    except TimeoutError:
        pass


def install_shutdown_handlers(shutdown: asyncio.Event) -> None:
    """Translate termination signals into a shutdown request."""
    loop = asyncio.get_running_loop()
    for received in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(received, shutdown.set)
