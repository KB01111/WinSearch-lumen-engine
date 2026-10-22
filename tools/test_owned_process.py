"""Actual Windows containment before startup; no engine/model qualification."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location("indexed_media", Path(__file__).with_name("test-indexed-media.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


@unittest.skipUnless(os.name == "nt", "Windows Job Object fixture")
class OwnedProcessTests(unittest.TestCase):
    def test_suspended_start_cannot_escape_before_assignment_and_descendants_stay_owned(self):
        with tempfile.TemporaryDirectory(prefix="winsearch-owned-start-") as directory:
            marker = Path(directory) / "started.txt"
            owner = harness.OwnedJob()
            child = None
            try:
                script = "import subprocess,sys,time; from pathlib import Path; subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],creationflags=0x08000000); Path(sys.argv[1]).write_text('started'); time.sleep(60)"
                child = subprocess.Popen([sys.executable, "-c", script, str(marker)],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW | 0x00000004)
                owner.assign(child)
                self.assertIn(child.pid, owner.sample())
                self.assertFalse(marker.exists(), "the suspended process must not execute user code")
                owner.resume(child)
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(marker.exists())
                self.assertGreaterEqual(len(owner.sample()), 2, "spawned descendant must inherit the job")
            finally:
                owner.terminate()
                if child:
                    child.wait(timeout=5)
                deadline = time.monotonic() + 5
                while owner.sample() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertFalse(owner.sample())
                owner.close()


if __name__ == "__main__":
    unittest.main()
