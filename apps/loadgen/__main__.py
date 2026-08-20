import argparse
import asyncio
import logging
from datetime import timedelta

import httpx

from apps.loadgen.generator import DEFAULT_RATE_PER_SECOND, HttpOrderSender, run_load
from packages.runtime import install_shutdown_handlers

logger = logging.getLogger("apps.loadgen")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="apps.loadgen",
        description="Place orders at a steady rate against the Orders API.",
    )
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE_PER_SECOND,
                        help="orders per second (default: %(default)s)")
    parser.add_argument("--duration", type=float, default=None,
                        help="seconds to run for; omit to run until interrupted")
    parser.add_argument("--seed", type=int, default=None,
                        help="seed the order generator so a run can be repeated")
    parser.add_argument("--base-url", default="http://localhost:8000",
                        help="Orders API base URL (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="per-request timeout in seconds (default: %(default)s)")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    shutdown = asyncio.Event()
    install_shutdown_handlers(shutdown)

    logger.info(
        "load starting: %.1f/s against %s%s",
        args.rate,
        args.base_url,
        f" for {args.duration:.0f}s" if args.duration else "",
    )

    async with httpx.AsyncClient(base_url=args.base_url, timeout=args.timeout) as client:
        stats = await run_load(
            sender=HttpOrderSender(client),
            shutdown=shutdown,
            rate_per_second=args.rate,
            duration=timedelta(seconds=args.duration) if args.duration else None,
            seed=args.seed,
        )

    logger.info("load finished: %s", stats.summary())


if __name__ == "__main__":
    asyncio.run(main())
