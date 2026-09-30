import subprocess
import time
from pathlib import Path
import unittest

class WranglerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        project_root = Path(__file__).resolve().parents[1]
        pywrangler = project_root / ".venv" / "bin" / "pywrangler"

        cls.wrangler_process = subprocess.Popen(
            [
                str(pywrangler),
                "dev",
                "--port",
                "8787",
            ],
            cwd=project_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        if cls.wrangler_process.poll() is None:
            cls.wrangler_process.terminate()

            try:
                cls.wrangler_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.wrangler_process.kill()
                cls.wrangler_process.wait()