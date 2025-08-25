#!/usr/bin/env python3
"""
Test runner script for the data-scripts package.
Provides convenient commands for running tests with different configurations.
"""

import subprocess
import sys
import argparse
from pathlib import Path


def run_command(command: list[str], description: str) -> int:
    """Run a command and return the exit code."""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(command)}")
    print()
    
    result = subprocess.run(command, cwd=Path(__file__).parent)
    
    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
    else:
        print(f"❌ {description} - FAILED")
    
    return result.returncode


def run_tests(args):
    """Run the test suite with various options."""
    base_cmd = ["python", "-m", "pytest"]
    
    if args.verbose:
        base_cmd.append("-v")
    
    if args.coverage:
        base_cmd.extend(["--cov=.", "--cov-report=term-missing"])
    
    if args.file:
        base_cmd.append(f"tests/{args.file}")
    
    if args.pattern:
        base_cmd.extend(["-k", args.pattern])
    
    if args.markers:
        base_cmd.extend(["-m", args.markers])
    
    return run_command(base_cmd, "Running Tests")


def run_linting():
    """Run code linting checks."""
    commands = [
        (["python", "-m", "ruff", "check", "."], "Ruff Linting"),
        (["python", "-m", "black", "--check", "."], "Black Code Formatting Check"),
        (["python", "-m", "mypy", "."], "MyPy Type Checking"),
    ]
    
    all_passed = True
    for cmd, desc in commands:
        try:
            exit_code = run_command(cmd, desc)
            if exit_code != 0:
                all_passed = False
        except FileNotFoundError:
            print(f"⚠️  Skipping {desc} - tool not installed")
    
    return 0 if all_passed else 1


def run_format():
    """Run code formatting."""
    commands = [
        (["python", "-m", "black", "."], "Black Code Formatting"),
        (["python", "-m", "ruff", "--fix", "."], "Ruff Auto-fix"),
    ]
    
    all_passed = True
    for cmd, desc in commands:
        try:
            exit_code = run_command(cmd, desc)
            if exit_code != 0:
                all_passed = False
        except FileNotFoundError:
            print(f"⚠️  Skipping {desc} - tool not installed")
    
    return 0 if all_passed else 1


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(description="Test runner for data-scripts package")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Run tests")
    test_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    test_parser.add_argument("-c", "--coverage", action="store_true", help="Run with coverage")
    test_parser.add_argument("-f", "--file", help="Run specific test file (without tests/ prefix)")
    test_parser.add_argument("-k", "--pattern", help="Run tests matching pattern")
    test_parser.add_argument("-m", "--markers", help="Run tests with specific markers")
    
    # Lint command
    subparsers.add_parser("lint", help="Run linting checks")
    
    # Format command  
    subparsers.add_parser("format", help="Run code formatting")
    
    # All command
    subparsers.add_parser("all", help="Run tests, linting, and formatting")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == "test":
        return run_tests(args)
    elif args.command == "lint":
        return run_linting()
    elif args.command == "format":
        return run_format()
    elif args.command == "all":
        print("🚀 Running complete test suite...")
        
        # Run formatting first
        format_result = run_format()
        
        # Run linting
        lint_result = run_linting()
        
        # Run tests with coverage
        args.coverage = True
        args.verbose = True
        test_result = run_tests(args)
        
        if format_result == 0 and lint_result == 0 and test_result == 0:
            print("\n🎉 All checks passed!")
            return 0
        else:
            print("\n💥 Some checks failed!")
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())