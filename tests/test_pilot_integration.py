"""Opt-in real Docker/Git regression; no provider calls or credentials.

Run explicitly with GOSSIP_RUN_DOCKER_TESTS=1 and the pinned pilot image already
available locally. Ordinary unit discovery skips this test without Docker use.
"""

import json
import os
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.gitstore import GitStore
from gossip_harness.ledger import Ledger
from gossip_harness.pilot import DEFAULT_IMAGE, KnownSolutionWorker, run_pilot
from gossip_harness.worker import WorkerResult


class RepairAfterLocalFailureWorker:
    """Return unfinished allowed modules once, then the trusted offline solution."""

    def __init__(self):
        self.requests = []
        self.solution = KnownSolutionWorker()

    def reservation_units(self, request):
        return 0

    def run(self, request):
        self.requests.append(request)
        if len(self.requests) == 1:
            return WorkerResult(
                changes={path: request.files[path] + "\n# Deliberately unfinished first attempt.\n"
                         for path in request.allowed_paths},
                summary="Offline regression: leave both allowed modules unfinished",
                usage_units=0,
                metadata={"runner": "local-repair-regression", "api_calls": 0},
            )
        return self.solution.run(request)


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1",
                     "Set GOSSIP_RUN_DOCKER_TESTS=1 for the real Docker/Git regression")
class PilotDockerIntegrationTests(unittest.TestCase):
    def test_local_failure_feedback_repairs_and_promotes_exact_tested_commit(self):
        started = time.monotonic()
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            output = artifacts.output
            worker = RepairAfterLocalFailureWorker()
            report = run_pilot(
                output, worker, image=DEFAULT_IMAGE, variants=("single",),
                mode="rehearsal", attempts=2,
                progress=lambda message: print(message, flush=True),
            )
            self.assertEqual(report["unexecuted_variants"], [])
            self.assertEqual(report["contract"]["mode"], "rehearsal")
            self.assertEqual(len(report["cases"]), 1)
            case = report["cases"][0]
            self.assertEqual(case["variant"], "single")
            self.assertEqual(case["status"], "accepted")
            self.assertTrue(case["project_accepted"])
            self.assertEqual(case["counters"]["worker_calls"], 2)
            self.assertEqual(case["counters"]["validation_calls"], 4)
            self.assertEqual(case["counters"]["prepare_calls"], 4)

            self.assertEqual(len(worker.requests), 2)
            first, second = worker.requests
            self.assertEqual((first.attempt, second.attempt), (1, 2))
            self.assertEqual(first.feedback, "")
            self.assertTrue(second.feedback.strip())
            self.assertEqual(first.task_id, second.task_id)
            self.assertEqual(case["task_states"], {"whole-project": "complete"})
            ledger = Ledger(output / "ledger.sqlite")
            task = ledger.task(second.task_id)
            self.assertEqual(task["status"], "complete")
            self.assertEqual(task["accepted_commit"], case["release_head"])
            self.assertEqual(ledger.pending_intents(), [])
            self.assertEqual(report["budget"]["spent_or_reserved"], 0)

            rows = [json.loads(line) for line in
                    (output / "trace.jsonl").read_text().splitlines()]
            verifications = [row for row in rows if row["kind"] == "verification"]
            self.assertEqual([row["stage"] for row in verifications], [
                "local:whole-project", "local:whole-project",
                "integration:whole-project", "release",
            ])
            self.assertEqual([row["passed"] for row in verifications],
                             [False, True, True, True])
            repairs = [row for row in rows if row["kind"] == "repair_needed"]
            self.assertEqual(len(repairs), 1)
            self.assertEqual(repairs[0]["status"], "validation_failed")
            self.assertEqual(repairs[0]["feedback"], second.feedback)
            calls = [row for row in rows if row["kind"] == "worker_started"]
            self.assertEqual(len(calls), 2)
            self.assertLess(repairs[0]["index"], calls[1]["index"])

            prepared = [row for row in rows
                        if row["kind"] == "prepared" and row["stage"] == "release"]
            accepted = [row for row in rows if row["kind"] == "project_accepted"]
            self.assertEqual(len(prepared), 1)
            self.assertEqual(len(accepted), 1)
            candidate = prepared[0]["candidate"]
            promotion = accepted[0]
            self.assertEqual(candidate["status"], "prepared")
            self.assertEqual(promotion["exact_tested_sha"], candidate["candidate_sha"])
            self.assertEqual(promotion["outcome"]["head"], candidate["candidate_sha"])
            self.assertEqual(case["release_head"], candidate["candidate_sha"])
            self.assertEqual(case["integration_head"], candidate["candidate_sha"])
            self.assertEqual(GitStore(output / "single" / "release.git").head(),
                             candidate["candidate_sha"])
            self.assertLess(verifications[-1]["index"], prepared[0]["index"])
            self.assertLess(prepared[0]["index"], promotion["index"])
        print(f"Docker local-repair regression verified in {time.monotonic() - started:.3f}s; "
              "2 worker calls, 4 validations, complete ledger, exact tested release SHA",
              flush=True)


if __name__ == "__main__":
    unittest.main()
