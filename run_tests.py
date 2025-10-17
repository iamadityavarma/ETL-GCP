#!/usr/bin/env python
"""
Test Runner Script for GCP ETL Pipeline
Provides convenient commands for running different test suites
"""

import sys
import subprocess
import argparse


def run_command(cmd):
    """Execute shell command and return exit code"""
    print(f"\n{'='*70}")
    print(f"Running: {' '.join(cmd)}")
    print('='*70)
    result = subprocess.run(cmd, shell=False)
    return result.returncode


def run_all_tests(verbose=False):
    """Run all unit tests"""
    cmd = ['pytest']
    if verbose:
        cmd.append('-vv')
    return run_command(cmd)


def run_api_tests(verbose=False):
    """Run API extraction tests only"""
    cmd = ['pytest', 'tests/test_api_extraction.py']
    if verbose:
        cmd.append('-vv')
    return run_command(cmd)


def run_postgresql_tests(verbose=False):
    """Run PostgreSQL insert logic tests only"""
    cmd = ['pytest', 'tests/test_postgresql_loader.py']
    if verbose:
        cmd.append('-vv')
    return run_command(cmd)


def run_bigquery_tests(verbose=False):
    """Run BigQuery push logic tests only"""
    cmd = ['pytest', 'tests/test_bigquery_writer.py']
    if verbose:
        cmd.append('-vv')
    return run_command(cmd)


def run_with_coverage(html=False):
    """Run tests with coverage report"""
    cmd = ['pytest', '--cov=.', '--cov-report=term-missing']
    if html:
        cmd.append('--cov-report=html')
    exit_code = run_command(cmd)

    if html and exit_code == 0:
        print("\n✅ Coverage HTML report generated in 'htmlcov/index.html'")

    return exit_code


def run_fast_tests():
    """Run only fast unit tests (skip slow/integration tests)"""
    cmd = ['pytest', '-m', 'not slow']
    return run_command(cmd)


def main():
    parser = argparse.ArgumentParser(
        description='Test runner for GCP ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py --all              # Run all tests
  python run_tests.py --api              # Run API tests only
  python run_tests.py --postgresql       # Run PostgreSQL tests only
  python run_tests.py --bigquery         # Run BigQuery tests only
  python run_tests.py --coverage         # Run with coverage report
  python run_tests.py --coverage --html  # Generate HTML coverage report
  python run_tests.py --fast             # Run only fast tests
        """
    )

    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--api', action='store_true', help='Run API extraction tests')
    parser.add_argument('--postgresql', action='store_true', help='Run PostgreSQL tests')
    parser.add_argument('--bigquery', action='store_true', help='Run BigQuery tests')
    parser.add_argument('--coverage', action='store_true', help='Run with coverage report')
    parser.add_argument('--html', action='store_true', help='Generate HTML coverage report')
    parser.add_argument('--fast', action='store_true', help='Run only fast tests')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    exit_code = 0

    if args.all:
        exit_code = run_all_tests(verbose=args.verbose)
    elif args.api:
        exit_code = run_api_tests(verbose=args.verbose)
    elif args.postgresql:
        exit_code = run_postgresql_tests(verbose=args.verbose)
    elif args.bigquery:
        exit_code = run_bigquery_tests(verbose=args.verbose)
    elif args.coverage:
        exit_code = run_with_coverage(html=args.html)
    elif args.fast:
        exit_code = run_fast_tests()

    # Print summary
    print("\n" + "="*70)
    if exit_code == 0:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed. Please review the output above.")
    print("="*70)

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
