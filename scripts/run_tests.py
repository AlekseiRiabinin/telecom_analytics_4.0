#!/usr/bin/env python3

import subprocess
import sys


def run_tests():
    """Run the test suite."""

    print("Running PySpark unit tests...")
    
    result = subprocess.run([
        "python", "-m", "pytest", 
        "tests/unit/", 
        "-v", 
        "--cov=spark",
        "--cov-report=term-missing",
        "--cov-report=html:reports/coverage"
    ], cwd=".")
    
    return result.returncode


if __name__ == "__main__":
    sys.exit(run_tests())
