"""Command-line entry point for the provider-neutral local lab."""

import argparse
from datetime import datetime, timezone
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sys


def summary(report: dict) -> str:
    if report.get('experiment') == 'candidate-selection-v1':
        rows = ['arm                 accepted / task-runs    attributed microUSD']
        for arm in report['contract']['arms']:
            outcomes = [case['arms'][arm] for case in report['cases']]
            rows.append(f"{arm:<20} {sum(o['accepted'] for o in outcomes):>3} / {len(outcomes):<12} {sum(o['attributed_micro_usd'] for o in outcomes):>15}")
        rows.append(f"Status: {report['status']}; unexecuted task-runs: {len(report['unexecuted'])}")
        rows.append(f"Shared ledger microUSD: {report['budget']}")
        rows.append('Oracle-assisted selection; attributed costs reuse full candidate-pool charges and do not sum to actual spending.')
        return '\n'.join(rows)
    if report.get('experiment') == 'shared-discovery-study':
        rows = ['trial  arm                   outcome                calls  validation']
        for case in report['cases']:
            rows.append(f"{case['trial']:>5}  {case['variant']:<21} {case['status']:<22} {case['counters']['worker_calls']:>5}  {case['counters']['validation_calls']:>10}")
        rows.append(f"Run ledger budget in microUSD: {report['budget']}")
        rows.append('Gossip uses exact broker-response replay; calls there are logical invocations, not additional API requests.')
        rows.append(f"Unexecuted: {report.get('unexecuted', [])}")
        return '\n'.join(rows)
    if report.get('experiment') == 'integration-recovery-study':
        rows = ['transport  fault             seed  outcome    published round']
        for case in report['cases']:
            rows.append(f"{case['transport']:<10} {case['fault']:<17} {case['seed']:>4}  {case['status']:<10} {case['published_round']:>14}")
        rows.append(f"Repair worker invocations ({report['mode']}): {report['repair']['calls']}; replays make no API requests.")
        rows.append(f"Run ledger budget in microUSD: {report['budget']}")
        rows.append(f"Unexecuted: {report.get('unexecuted', [])}")
        return '\n'.join(rows)
    if report.get('experiment') == 'practical-coding-pilot':
        rows = ['variant               outcome                 worker calls  checks']
        for case in report['cases']:
            rows.append(f"{case['variant']:<21} {case['status']:<23} {case['counters']['worker_calls']:>12}  {case['counters']['validation_calls']:>6}")
        rows.append(f"Budget in microUSD: {report['budget']}")
        rows.append(f"Unexecuted: {report.get('unexecuted_variants', [])}")
        rows.append('Small coding smoke test; these results do not establish comparative superiority.')
        return '\n'.join(rows)
    rows = ["scenario             topology     transport accepted checks contacts rounds"]
    for case in report["cases"]:
        metrics = case["metrics"]
        transport = metrics["transport"]
        rows.append(
            f"{case['scenario']:<20} {case['topology']:<12} {case['transport']:<9} "
            f"{len(case['accepted_task_ids']):>8} {metrics['verification_calls']:>6} "
            f"{transport['contacts']:>8} {transport['rounds']:>6}"
        )
    rows.append("Checks include every integration level; contacts/rounds are modeled, not wall-clock or token costs.")
    return "\n".join(rows)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Git coordination lab and bounded practical coding pilot.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run four scenarios across two integration topologies and two transports")
    run.add_argument("--output", type=Path, help="Fresh output directory; existing results are never overwritten")
    run.add_argument("--seeds", type=int, nargs="+", default=[0], help="Transport RNG seeds (default: 0)")
    inspect = commands.add_parser("inspect", help="Summarize a saved results.json without running Git")
    inspect.add_argument("results", type=Path)
    inspect.add_argument("--json", action="store_true")
    pilot = commands.add_parser('pilot', help='Run the small coding project; defaults to a no-API rehearsal')
    pilot.add_argument('--output', type=Path, required=True)
    pilot.add_argument('--live', action='store_true', help='Make paid OpenAI requests after an exact passing rehearsal')
    pilot.add_argument('--rehearsal', type=Path, help='Passing rehearsal results.json required for live mode')
    pilot.add_argument('--budget-usd', default='10', help='Shared live cap, at most the authorized $10')
    pilot.add_argument('--budget-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
    pilot.add_argument('--env-file', type=Path, default=Path('.env.local'))
    pilot.add_argument('--image', help='Locally available immutable Docker image sha256 ID')
    for name, help_text in (('discovery', 'Compare shared discoveries on a richer existing project'),
                            ('recovery', 'Repair a combined semantic conflict and replay transport faults'),
                            ('swarm', 'Compare independent candidates and evidence-based selectors')):
        study = commands.add_parser(name, help=help_text)
        study.add_argument('--output', type=Path, required=True)
        study.add_argument('--live', action='store_true')
        study.add_argument('--rehearsal', type=Path)
        study.add_argument('--budget-usd', default='10')
        study.add_argument('--budget-ledger', type=Path, default=Path('runs/first-live-budget.sqlite'))
        study.add_argument('--env-file', type=Path, default=Path('.env.local'))
        study.add_argument('--image')
        if name == 'discovery':
            study.add_argument('--repetitions', type=int, default=3)
        if name == 'swarm':
            study.add_argument('--repetitions', type=int, default=2)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            from .experiment import run_matrix
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
            output = args.output or Path("runs") / stamp
            print(f"Local scripted experiment: {output.resolve()}", flush=True)
            report = run_matrix(output, seeds=tuple(args.seeds), progress=lambda message: print(message, flush=True))
            print(summary(report))
            print(f"Results: {(output / 'results.json').resolve()}")
            print(f"Trace: {(output / 'trace.jsonl').resolve()}")
        elif args.command in ('pilot', 'discovery', 'recovery', 'swarm'):
            from .pilot import DEFAULT_IMAGE, KnownSolutionWorker, run_pilot
            worker = KnownSolutionWorker()
            units = 0
            if args.live:
                from .worker import OpenAIWorker
                try:
                    amount = Decimal(args.budget_usd)
                    maximum = 50 if args.command == 'swarm' else 10
                    if not amount.is_finite() or not 0 < amount <= maximum or amount * 1_000_000 != int(amount * 1_000_000):
                        raise ValueError(f'Live budget must be positive, at most ${maximum}, and exact to microUSD')
                    units = int(amount * 1_000_000)
                except InvalidOperation:
                    raise ValueError('Invalid dollar budget') from None
                if args.env_file.is_symlink() or not args.env_file.is_file():
                    raise ValueError('Approved credential file is missing or is a symlink')
                values = []
                for line in args.env_file.read_text().splitlines():
                    match = re.match(r'^\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*$', line)
                    if match:
                        values.append(match.group(1).strip().strip('\"\''))
                if len(values) != 1 or not values[0].startswith('sk-'):
                    raise ValueError('Expected one configured OPENAI_API_KEY; its value will not be displayed')
                worker = OpenAIWorker(values[0], max_output_tokens=4096 if args.command == 'swarm' else 8192)
            runner = run_pilot
            extra = {}
            if args.command == 'discovery':
                from .discovery_experiment import RehearsalWorker, run_discovery_experiment
                runner = run_discovery_experiment
                extra['repetitions'] = args.repetitions
                if not args.live:
                    worker = RehearsalWorker()
            elif args.command == 'recovery':
                from .recovery_experiment import KnownRepairWorker, run_recovery_experiment
                runner = run_recovery_experiment
                if not args.live:
                    worker = KnownRepairWorker()
            elif args.command == 'swarm':
                from .swarm_experiment import KnownSwarmWorker, run_swarm_experiment
                from .worker import STRONG_MODEL
                runner = run_swarm_experiment
                extra['repetitions'] = args.repetitions
                if args.live:
                    extra['strong'] = OpenAIWorker(values[0], model=STRONG_MODEL, max_output_tokens=4096)
                else:
                    worker = KnownSwarmWorker()
                    extra['strong'] = KnownSwarmWorker()
            report = runner(args.output, worker, image=args.image or DEFAULT_IMAGE,
                            budget_ledger=args.budget_ledger if args.live else None,
                            budget_units=units, mode='live' if args.live else 'rehearsal',
                            rehearsal_results=args.rehearsal,
                            progress=lambda message: print(message, flush=True), **extra)
            print(summary(report))
            print(f"Budget (microUSD): {report['budget']}")
            print(f"Results: {(args.output / 'results.json').resolve()}")
            if args.command == 'swarm':
                return 0 if report['status'] == 'finished' and not report['unexecuted'] else 2
            return 0 if all(c['project_accepted'] for c in report['cases']) and not report.get('unexecuted_variants', report.get('unexecuted', [])) else 2
        else:
            report = json.loads(args.results.read_text())
            if report.get("schema_version") != 1 or "cases" not in report:
                raise ValueError("Expected a version-1 lab results file")
            print(json.dumps(report, indent=2) if args.json else summary(report))
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"gossip-lab: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
