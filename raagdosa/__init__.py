"""RaagDosa — deterministic music library cleanup for DJs and collectors."""

try:
    from importlib.metadata import version as _pkg_version
    APP_VERSION = _pkg_version("raagdosa")
except Exception:
    APP_VERSION = "10.0.0"


def main():
    """Entry point — delegates to cli.main()."""
    from raagdosa.cli import main as _main
    _main()
