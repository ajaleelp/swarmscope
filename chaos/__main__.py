import argparse

from chaos.catalogue import load_catalogue, symptoms


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="chaos",
        description="Inspect the fault catalogue. Prints symptoms, never causes.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="list every fault an investigation can be given")
    show = sub.add_parser("symptom", help="print one fault's symptom as reported")
    show.add_argument("fault_id")
    args = parser.parse_args(argv)

    if args.command == "list":
        for symptom in symptoms():
            print(f"{symptom.fault_id:32s} {symptom.title}")
        return 0

    catalogue = load_catalogue()
    if args.fault_id not in catalogue:
        print(f"unknown fault: {args.fault_id}")
        print("known faults: " + ", ".join(sorted(catalogue)))
        return 1

    symptom = catalogue[args.fault_id].public()
    print(symptom.title)
    print()
    print(symptom.reported)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
