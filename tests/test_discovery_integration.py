"""Opt-in real Git/Docker proof of failed-patch repair and exact response replay.

GOSSIP_RUN_DOCKER_TESTS=1 enables the test with the pinned local image. Its
worker is entirely offline, and no provider implementation is instantiated.
Set GOSSIP_DISCOVERY_OUTPUT to a fresh path to retain the actual run artifacts.
"""

import json
import os
import threading
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.discovery_experiment import (
    RehearsalWorker, deterministic_feedback, run_discovery_experiment,
)
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.worker import WorkerResult


class FailThenRepairDiscoveryWorker(RehearsalWorker):
    """Fail each independently sampled parser attempt once; replay needs no call."""

    def __init__(self):
        self.requests = []
        self.lock = threading.Lock()

    def run(self, request):
        with self.lock:
            self.requests.append(request)
        if request.task_id == 'parser' and request.attempt == 1:
            return WorkerResult(
                {'task_report/parser.py': (
                    'def parse_tasks(csv_text):\n'
                    '    raise ValueError("Deliberate offline regression")\n')},
                'Deliberately fail the first scoped parser attempt', 0,
                {'runner': 'offline-repair-replay-regression', 'api_calls': 0},
            )
        return super().run(request)


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1',
                     'Set GOSSIP_RUN_DOCKER_TESTS=1 for real Docker/Git replay verification')
class DiscoveryDockerIntegrationTests(unittest.TestCase):
    def assert_retained_report(self, output, report):
        """Check retained evidence without repeating Git/Docker execution."""
        self.assertEqual(report['contract']['status'], 'finished')
        self.assertEqual(report['unexecuted'], [])
        cases = {case['variant']: case for case in report['cases']}
        self.assertEqual(set(cases), {'single', 'team-isolated', 'team-shared', 'team-gossip'})
        self.assertTrue(all(case['project_accepted'] for case in cases.values()))
        self.assertEqual(report['budget']['spent_or_reserved'], 0)
        self.assertEqual(cases['single']['counters']['worker_calls'], 1)
        for arm in ('team-isolated', 'team-shared', 'team-gossip'):
            self.assertEqual(cases[arm]['counters']['worker_calls'], 3)
            self.assertEqual(cases[arm]['task_states'], {'parser': 'complete', 'summary': 'complete'})

        shared = [row for row in cases['team-shared']['controlled_worker_audit']
                  if row['kind'] == 'result']
        replay = [row for row in cases['team-gossip']['controlled_worker_audit']
                  if row['kind'] == 'result']
        fingerprint = lambda rows: {
            (row['task_id'], row['attempt']):
            (row['request_sha256'], tuple(row['included_note_ids'])) for row in rows}
        self.assertEqual(fingerprint(shared), fingerprint(replay))
        self.assertEqual(set(fingerprint(replay)), {('parser', 1), ('parser', 2), ('summary', 1)})
        self.assertTrue(all(row['replay'] and row['usage_units'] == 0 for row in replay))
        self.assertTrue(all(not row['replay'] for row in shared))

        rows = [json.loads(line) for line in (output / 'trace.jsonl').read_text().splitlines()]
        actual_calls = sum(row['kind'] == 'discovery_started' for row in rows)
        actual_calls += sum(row['kind'] == 'result' and not row['replay']
                            for case in cases.values() for row in case['controlled_worker_audit'])
        self.assertEqual(actual_calls, 9)  # two scouts + 1 + 3 + 3; gossip calls none
        for arm in ('team-shared', 'team-gossip'):
            failures = [row for row in rows if row['kind'] == 'verification'
                        and row.get('variant') == arm and not row['passed']]
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]['stage'], 'local:parser')
            # A severely broken module can fill the bounded log before the
            # runner's timing footer. Its truncated failure still must replay
            # exactly. The unit regression separately exercises timing removal.
            if not failures[0]['receipt']['output_truncated']:
                self.assertRegex(failures[0]['detail'], r'Ran \d+ tests in \d+(?:\.\d+)?s')
            repairs = [row for row in rows if row['kind'] == 'repair_needed'
                       and row.get('variant') == arm]
            self.assertEqual(len(repairs), 1)
            self.assertEqual(repairs[0]['feedback'], deterministic_feedback(failures[0]['detail']))
            self.assertIn('Deliberate offline regression', repairs[0]['feedback'])
            entries = json.loads((output / 'trial-0' / arm / 'responses.json').read_text())
            repair_requests = [entry['request'] for entry in entries.values()
                               if entry['request']['task_id'] == 'parser'
                               and entry['request']['attempt'] == 2]
            self.assertEqual(len(repair_requests), 1)
            self.assertEqual(repair_requests[0]['feedback'], repairs[0]['feedback'])
            releases = [row for row in rows if row['kind'] == 'project_accepted'
                        and row.get('variant') == arm]
            self.assertEqual(len(releases), 1)
            self.assertEqual(releases[0]['exact_tested_sha'], cases[arm]['release_head'])

    def test_failed_patch_and_repair_replay_exactly_after_real_docker_checks(self):
        started = time.monotonic()
        with ArtifactDirectory(type(self).__name__, output_env='GOSSIP_DISCOVERY_OUTPUT',
                               retain_success=True) as artifacts:
            output = artifacts.output
            worker = FailThenRepairDiscoveryWorker()
            report = run_discovery_experiment(
                output, worker, image=DEFAULT_IMAGE, repetitions=1,
                mode='rehearsal', progress=lambda message: print(message, flush=True),
            )
            self.assert_retained_report(output, report)
            self.assertEqual(len(worker.requests), 9)
            for request in worker.requests:
                if request.task_id == 'parser' and request.attempt == 2:
                    self.assertEqual(request.feedback, deterministic_feedback(request.feedback))
                    self.assertIn('Deliberate offline regression', request.feedback)
        print(f'Discovery failure/repair replay verified in {time.monotonic() - started:.3f}s; '
              'four accepted arms, nine offline worker calls, three exact gossip replays', flush=True)


if __name__ == '__main__':
    unittest.main()
