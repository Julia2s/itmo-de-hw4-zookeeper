import importlib
import io
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
with patch.dict(sys.modules, {"psutil": types.ModuleType("psutil")}):
    pause_process = importlib.import_module("pause_process")


class PauseTests(unittest.TestCase):
    def test_waits_for_minimum_and_backup_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "backup.log"
            log.write_text("run complete at 2026-11-09T14:01:45+00:00\n", encoding="utf-8")
            with patch.object(pause_process.time, "monotonic", side_effect=[0, 10, 45]), \
                    patch.object(pause_process.time, "sleep") as sleep, \
                    redirect_stdout(io.StringIO()):
                pause_process.wait_for_backup(log, minimum_seconds=45, timeout_seconds=60)
            sleep.assert_called_once_with(0.2)

    def test_timeout_still_resumes_suspended_process(self):
        process = Mock()
        args = ["pause_process.py", "--pid", "123", "--seconds", "1",
                "--until-log", "backup.log", "--timeout-seconds", "2"]
        with patch.object(sys, "argv", args), \
                patch.object(pause_process.psutil, "Process", return_value=process, create=True), \
                patch.object(pause_process, "wait_for_backup", side_effect=TimeoutError), \
                redirect_stdout(io.StringIO()):
            with self.assertRaises(TimeoutError):
                pause_process.main()
        process.suspend.assert_called_once()
        process.resume.assert_called_once()


if __name__ == "__main__":
    unittest.main()
