def test_commands_package_imports_cleanly():
    from src.commands import all, movers, pro30, weekly  # noqa: F401

