"""Run all ElectionStats integration tests."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

print("\n" + "=" * 70)
print("ElectionStats Test Suite")
print("=" * 70)

# Import and run test modules
test_results = {}


def run_test_module(module_name, display_name):
    """Run a test module and capture results."""
    print(f"\n{'=' * 70}")
    print(f"Running: {display_name}")
    print("=" * 70)

    try:
        if module_name == "classic_parser":
            from ElectionStats.tests.integration import test_classic_parser
            test_classic_parser.test_parser_selection()
            test_classic_parser.test_virginia_parser()
            test_classic_parser.test_colorado_parser()

        elif module_name == "classic_fetch":
            from ElectionStats.tests.integration import test_classic_fetch
            test_classic_fetch.test_virginia()
            test_classic_fetch.test_massachusetts()
            test_classic_fetch.test_colorado()

        elif module_name == "v2_parser":
            from ElectionStats.tests.integration import test_v2_parser
            # This module runs on import
            pass

        elif module_name == "playwright":
            from ElectionStats.tests.integration import test_playwright
            test_playwright.test_south_carolina()

        test_results[display_name] = "✓ PASSED"
        print(f"\n✓ {display_name} PASSED")

    except Exception as e:
        test_results[display_name] = f"✗ FAILED: {str(e)[:50]}"
        print(f"\n✗ {display_name} FAILED: {e}")
        import traceback
        traceback.print_exc()


# Run all tests
print("\nRunning integration tests (this may take a few minutes)...")
print("These tests hit live election websites with small samples.\n")

run_test_module("classic_parser", "Classic State Parsers (VA/MA/CO)")
run_test_module("classic_fetch", "Classic State Fetching (VA/MA/CO)")
run_test_module("playwright", "Playwright Client (SC)")
run_test_module("v2_parser", "V2 Parser (SC/NM)")

# Print summary
print("\n" + "=" * 70)
print("Test Summary")
print("=" * 70)

for test_name, result in test_results.items():
    print(f"{result:15} {test_name}")

passed = sum(1 for r in test_results.values() if "PASSED" in r)
total = len(test_results)

print("\n" + "=" * 70)
print(f"Results: {passed}/{total} tests passed")
print("=" * 70 + "\n")

sys.exit(0 if passed == total else 1)
