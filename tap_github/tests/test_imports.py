"""Test that all modules can be imported without syntax errors."""

import pytest


def test_search_count_streams_import():
    """Test that search_count_streams module imports successfully."""
    try:
        import tap_github.search_count_streams  # noqa: F401
    except SyntaxError as e:
        pytest.fail(f"Syntax error in search_count_streams module: {e}")
    except ImportError as e:
        pytest.fail(f"Import error in search_count_streams module: {e}")


def test_streams_module_import():
    """Test that streams module imports successfully."""
    try:
        import tap_github.streams  # noqa: F401
    except SyntaxError as e:
        pytest.fail(f"Syntax error in streams module: {e}")
    except ImportError as e:
        pytest.fail(f"Import error in streams module: {e}")


def test_tap_module_import():
    """Test that tap module imports successfully."""
    try:
        import tap_github.tap  # noqa: F401
    except SyntaxError as e:
        pytest.fail(f"Syntax error in tap module: {e}")
    except ImportError as e:
        pytest.fail(f"Import error in tap module: {e}")


def test_discovery_command():
    """Test that discovery command runs without errors."""
    import subprocess
    import sys
    
    # Run discovery command to ensure no import errors
    result = subprocess.run([
        sys.executable, "-m", "tap_github.tap", 
        "--config", "test-config.json", 
        "--discover"
    ], capture_output=True, text=True, cwd=".")
    
    if result.returncode != 0:
        pytest.fail(f"Discovery command failed: {result.stderr}")
