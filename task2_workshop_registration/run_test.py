"""Simple test runner to verify the crash resilience fix"""
import sys
import subprocess

result = subprocess.run([
    sys.executable, "-m", "pytest", 
    "tests/test_resilience.py::test_crash_resilience_50_members",
    "-v", "-s", "--tb=short"
], cwd=".")

sys.exit(result.returncode)
