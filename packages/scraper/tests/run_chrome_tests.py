#!/usr/bin/env python3
"""
Test runner for Chrome driver tests.
Provides easy commands to run different types of Chrome driver tests.
"""

import os
import sys
import subprocess
import argparse
from typing import Dict, List, Optional
from pathlib import Path


def run_command(cmd: List[str], env_vars: Optional[Dict[str, str]] = None) -> int:
    """Run a command with optional environment variables."""
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)

    print(f"Running: {' '.join(cmd)}")
    if env_vars:
        print(f"Environment: {env_vars}")
    print("-" * 50)

    result = subprocess.run(cmd, env=env)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run Chrome driver tests")
    parser.add_argument(
        "test_type",
        choices=[
            "unit",
            "integration",
            "process",
            "all",
            "fast",
            "coverage",
            "chrome-required",
        ],
        help="Type of tests to run",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--with-coverage", action="store_true", help="Run with coverage reporting"
    )
    parser.add_argument(
        "--force-chrome",
        action="store_true",
        help="Force Chrome tests even if Chrome might not be available",
    )
    parser.add_argument("--parallel", "-n", type=int, help="Number of parallel workers")

    args = parser.parse_args()

    # Base directory containing tests
    test_dir = Path(__file__).parent

    # Base pytest command
    cmd = ["python", "-m", "pytest"]

    if args.verbose:
        cmd.extend(["-v", "-s"])

    if args.parallel:
        cmd.extend(["-n", str(args.parallel)])

    # Environment variables
    env_vars = {}

    # Test-specific configurations
    if args.test_type == "unit":
        cmd.extend(
            [
                str(
                    test_dir
                    / "test_chrome_driver_manager.py::TestChromeDriverManagerUnit"
                ),
                "-m",
                "not slow",
            ]
        )

    elif args.test_type == "integration":
        cmd.extend(
            [
                str(
                    test_dir
                    / "test_chrome_driver_manager.py::TestChromeDriverManagerIntegration"
                ),
                "--tb=short",
            ]
        )

    elif args.test_type == "process":
        cmd.extend([str(test_dir / "test_chrome_process_management.py"), "--tb=short"])
        env_vars["RUN_CHROME_PROCESS_TESTS"] = "1"

    elif args.test_type == "chrome-required":
        cmd.extend(
            ["-m", "chrome_required", "--force-chrome" if args.force_chrome else ""]
        )
        env_vars["RUN_CHROME_PROCESS_TESTS"] = "1"

    elif args.test_type == "fast":
        cmd.extend(
            [str(test_dir), "-m", "not slow and not chrome_required", "--tb=line"]
        )

    elif args.test_type == "coverage":
        cmd = [
            "python",
            "-m",
            "pytest",
            "--cov=scraper_app.utils.selenium.chrome_driver_manager",
        ]
        cmd.extend(
            [
                "--cov-report=html",
                "--cov-report=term",
                str(test_dir),
                "-m",
                "not chrome_required",
            ]
        )

    elif args.test_type == "all":
        cmd.extend([str(test_dir)])
        if args.force_chrome:
            cmd.append("--force-chrome")
            env_vars["RUN_CHROME_PROCESS_TESTS"] = "1"

    # Add coverage if requested
    if args.with_coverage and args.test_type != "coverage":
        cmd.extend(
            [
                "--cov=scraper_app.utils.selenium.chrome_driver_manager",
                "--cov-report=term-missing",
            ]
        )

    # Run the tests
    return run_command(cmd, env_vars)


if __name__ == "__main__":
    sys.exit(main())
