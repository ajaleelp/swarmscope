import argparse
import asyncio
import sys
from pathlib import Path

from chaos.catalogue import load_catalogue, symptoms
from chaos.environment import DEFAULT_API_BASE_URL, production_environment
from chaos.runner import FaultRunner, FaultRunResult, RunStatus, render_plan
from chaos.state import DEFAULT_STATE_PATH, FaultStateError, FaultStateStore
from packages.observability import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos",
        description="Inspect and safely run reversible faults without revealing causes.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="list every fault an investigation can be given")

    show = sub.add_parser("symptom", help="print one fault's symptom as reported")
    show.add_argument("fault_id")

    run = sub.add_parser("run", help="inject, verify, and revert one fault")
    run.add_argument("fault_id")
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="print the complete plan without reading or changing runtime state",
    )
    _add_runtime_arguments(run)

    revert = sub.add_parser("revert", help="recover the fault named by the state file")
    _add_runtime_arguments(revert)
    return parser


def _add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help="Orders API base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="recovery state path (default: repository-local state file)",
    )


def _report_failure(prefix: str, error: BaseException) -> int:
    """Print the failure and the notes describing what was left behind.

    The runner records recovery outcomes with ``add_note``. An f-string never
    renders ``__notes__``, so without this the one sentence saying whether the
    environment was restored is silently discarded, which is the sentence an
    operator most needs.
    """
    print(f"{prefix}: {type(error).__name__}: {error}", file=sys.stderr)
    for note in getattr(error, "__notes__", ()):
        print(f"  {note}", file=sys.stderr)
    return 1


def _unknown_fault(fault_id: str, catalogue: dict) -> int:
    print(f"unknown fault: {fault_id}", file=sys.stderr)
    print("known faults: " + ", ".join(sorted(catalogue)), file=sys.stderr)
    return 1


def _report(result: FaultRunResult) -> int:
    if result.injected is not None:
        # How strongly the symptom presented is the run's most useful number. A
        # count barely over the threshold and one many times past it are equally
        # a pass, and only one of them is worth trusting.
        print(
            f"{result.fault_id}: symptom detected "
            f"({result.injected.description}: {result.injected.observed})"
        )

    observation = result.recovery.observation
    observed = observation.observed if observation is not None else "unavailable"
    if result.status is RunStatus.RECOVERED:
        print(f"{result.fault_id}: reverted and healthy (observed {observed})")
        return 0

    print(
        f"{result.fault_id}: revert applied but recovery is still pending "
        f"(observed {observed}); state retained",
        file=sys.stderr,
    )
    return 2


async def _run_live(args: argparse.Namespace, fault) -> int:
    store = FaultStateStore(args.state_file)
    async with production_environment(api_base_url=args.api_base_url) as environment:
        result = await FaultRunner(environment, store).run(fault)
    return _report(result)


async def _revert_live(args: argparse.Namespace, fault) -> int:
    store = FaultStateStore(args.state_file)
    async with production_environment(api_base_url=args.api_base_url) as environment:
        result = await FaultRunner(environment, store).revert(fault)
    return _report(result)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    catalogue = load_catalogue()

    if args.command == "list":
        for symptom in symptoms():
            print(f"{symptom.fault_id:32s} {symptom.title}")
        return 0

    if args.command == "revert":
        store = FaultStateStore(args.state_file)
        try:
            state = store.load()
            if state is None:
                raise FaultStateError(f"no fault state exists at {store.path}")
            fault = catalogue.get(state.fault_id)
            if fault is None:
                return _unknown_fault(state.fault_id, catalogue)
            configure_logging()
            return asyncio.run(_revert_live(args, fault))
        except KeyboardInterrupt:
            return 130
        except Exception as error:
            return _report_failure("revert failed", error)

    if args.fault_id not in catalogue:
        return _unknown_fault(args.fault_id, catalogue)

    fault = catalogue[args.fault_id]
    if args.command == "symptom":
        symptom = fault.public()
        print(symptom.title)
        print()
        print(symptom.reported)
        return 0

    if args.dry_run:
        print(render_plan(fault, args.state_file), end="")
        return 0

    configure_logging()
    try:
        return asyncio.run(_run_live(args, fault))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        return _report_failure("fault run failed", error)


if __name__ == "__main__":
    raise SystemExit(main())
