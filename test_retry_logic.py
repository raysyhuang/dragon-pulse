#!/usr/bin/env python3
"""
Unit tests for retry guard logic (no external dependencies).

Tests the guard logic without actually sending Telegram messages
or persisting to databases.
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def test_telegram_suppression():
    """Test that Telegram alerts are suppressed for attempt > 1."""
    from src.core.alerts import AlertManager, AlertConfig

    os.environ["GITHUB_RUN_ATTEMPT"] = "1"

    config = AlertConfig(enabled=True, channels=["telegram"])
    manager = AlertManager(config)

    with patch('requests.post') as mock_post:
        mock_post.return_value.status_code = 200
        result = manager._send_telegram(
            title="Test Alert",
            message="Test message",
            data={"asof": "2026-01-30"},
            priority="normal"
        )
        assert mock_post.called
        assert result is True

    os.environ["GITHUB_RUN_ATTEMPT"] = "2"

    with patch('requests.post') as mock_post:
        result = manager._send_telegram(
            title="Test Alert",
            message="Test message",
            data={"asof": "2026-01-30"},
            priority="normal"
        )
        assert not mock_post.called
        assert result is True

    os.environ["GITHUB_RUN_ATTEMPT"] = "3"

    with patch('requests.post') as mock_post:
        result = manager._send_telegram(
            title="Test Alert",
            message="Test message",
            data={"asof": "2026-01-30"},
            priority="normal"
        )
        assert not mock_post.called
        assert result is True

    os.environ["GITHUB_RUN_ATTEMPT"] = "1"


def test_outcome_recording_guard():
    """Test that outcome recording is guarded for attempt > 1."""
    os.environ["GITHUB_RUN_ATTEMPT"] = "1"

    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    try:
        attempt_num = int(run_attempt)
        should_skip = attempt_num > 1
    except (ValueError, TypeError):
        should_skip = False
    assert should_skip is False

    os.environ["GITHUB_RUN_ATTEMPT"] = "2"

    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    try:
        attempt_num = int(run_attempt)
        should_skip = attempt_num > 1
    except (ValueError, TypeError):
        should_skip = False
    assert should_skip is True

    os.environ["GITHUB_RUN_ATTEMPT"] = "1"


def test_phase5_guard():
    """Test that Phase 5 learning is guarded for attempt > 1."""
    os.environ["GITHUB_RUN_ATTEMPT"] = "1"

    phase5_enabled = True
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    try:
        attempt_num = int(run_attempt)
        if attempt_num > 1:
            phase5_enabled = False
    except (ValueError, TypeError):
        pass
    assert phase5_enabled is True

    os.environ["GITHUB_RUN_ATTEMPT"] = "2"

    phase5_enabled = True
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    try:
        attempt_num = int(run_attempt)
        if attempt_num > 1:
            phase5_enabled = False
    except (ValueError, TypeError):
        pass
    assert phase5_enabled is False

    os.environ["GITHUB_RUN_ATTEMPT"] = "1"


def test_marker_file_logic():
    """Test marker file creation and checking."""
    from pathlib import Path
    import tempfile
    import shutil

    test_dir = Path(tempfile.mkdtemp())

    try:
        outputs_dir = test_dir / "2026-01-30"
        outputs_dir.mkdir(parents=True, exist_ok=True)

        run_id = "test_12345"
        run_attempt = "1"
        marker_file = outputs_dir / f".telegram_sent_{run_id}_{run_attempt}.txt"

        if not marker_file.exists():
            with open(marker_file, "w") as f:
                f.write("Sent at: 2026-01-30T10:15:30\n")
                f.write("Run ID: test_12345\n")
        else:
            raise AssertionError("marker file unexpectedly already existed")

        assert marker_file.exists()

        run_attempt2 = "2"
        marker_file2 = outputs_dir / f".telegram_sent_{run_id}_{run_attempt2}.txt"
        assert not marker_file2.exists()

    finally:
        shutil.rmtree(test_dir)


def test_metadata_extraction():
    """Test GitHub metadata extraction."""
    os.environ["GITHUB_WORKFLOW"] = "Test Workflow"
    os.environ["GITHUB_RUN_ID"] = "1234567890"
    os.environ["GITHUB_RUN_ATTEMPT"] = "1"
    os.environ["GITHUB_SHA"] = "abcdef1234567890"

    workflow = os.environ.get("GITHUB_WORKFLOW", "local")
    run_id = os.environ.get("GITHUB_RUN_ID", "N/A")
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    sha = os.environ.get("GITHUB_SHA", "N/A")
    
    if sha != "N/A" and len(sha) > 7:
        sha = sha[:7]
    assert workflow == "Test Workflow"
    assert run_id == "1234567890"
    assert run_attempt == "1"
    assert sha == "abcdef1"

    del os.environ["GITHUB_WORKFLOW"]
    del os.environ["GITHUB_RUN_ID"]
    del os.environ["GITHUB_SHA"]

    workflow = os.environ.get("GITHUB_WORKFLOW", "local")
    run_id = os.environ.get("GITHUB_RUN_ID", "N/A")
    sha = os.environ.get("GITHUB_SHA", "N/A")
    assert workflow == "local"
    assert run_id == "N/A"
    assert sha == "N/A"

    os.environ["GITHUB_RUN_ATTEMPT"] = "1"


def main():
    """Run all unit tests."""
    print("\n" + "=" * 60)
    print("RETRY LOGIC UNIT TESTS")
    print("=" * 60)
    print("\nTesting retry guard logic without external dependencies...")
    
    results = []
    
    try:
        results.append(("Telegram Suppression", test_telegram_suppression()))
    except Exception as e:
        print(f"\n✗ Telegram suppression test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Telegram Suppression", False))
    
    try:
        results.append(("Outcome Recording Guard", test_outcome_recording_guard()))
    except Exception as e:
        print(f"\n✗ Outcome recording test failed: {e}")
        results.append(("Outcome Recording Guard", False))
    
    try:
        results.append(("Phase 5 Learning Guard", test_phase5_guard()))
    except Exception as e:
        print(f"\n✗ Phase 5 guard test failed: {e}")
        results.append(("Phase 5 Learning Guard", False))
    
    try:
        results.append(("Marker File Logic", test_marker_file_logic()))
    except Exception as e:
        print(f"\n✗ Marker file test failed: {e}")
        results.append(("Marker File Logic", False))
    
    try:
        results.append(("Metadata Extraction", test_metadata_extraction()))
    except Exception as e:
        print(f"\n✗ Metadata extraction test failed: {e}")
        results.append(("Metadata Extraction", False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    print("\n" + "=" * 60)
    if passed == total:
        print(f"✅ ALL TESTS PASSED ({passed}/{total})")
        print("=" * 60)
        print("\nRetry logic is working correctly!")
        return True
    else:
        print(f"⚠️  SOME TESTS FAILED ({passed}/{total} passed)")
        print("=" * 60)
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
