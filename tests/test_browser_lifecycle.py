"""Include browser cancellation ownership in the ordinary offline inventory."""

from pathlib import Path
import re
import shutil
import subprocess
import unittest


class BrowserLifecycleTests(unittest.TestCase):
    def test_node_lifecycle_contracts_without_browser_launch(self):
        root = Path(__file__).resolve().parents[1]
        node = shutil.which("node")
        self.assertIsNotNone(node, "Node.js >=20 is required for browser lifecycle checks")
        assert node is not None
        result = subprocess.run(
            [node, "--test", "--test-reporter=tap", "devtools/browser/lifecycle.test.cjs"],
            cwd=root, text=True, capture_output=True, timeout=15, check=False,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, output)
        count = re.search(r"^# tests (\d+)$", output, re.MULTILINE)
        self.assertIsNotNone(count, output)
        assert count is not None
        self.assertEqual(count.group(1), "6", output)
        self.assertRegex(output, r"(?m)^# pass 6$")
        self.assertRegex(output, r"(?m)^# fail 0$")
