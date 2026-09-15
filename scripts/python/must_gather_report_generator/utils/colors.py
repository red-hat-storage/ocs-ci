"""ANSI color codes and printing utilities"""

_MAP = {
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "RED": "\033[91m",
    "BLUE": "\033[94m",
    "CYAN": "\033[96m",
    "BOLD": "\033[1m",
    "END": "\033[0m",
}


class _ColorsMeta(type):
    """Resolve ``Colors.RED`` etc.; return empty strings when disabled (file/pipe output)."""

    _disabled: bool = False

    def __getattr__(cls, name: str):
        if name == "disable":

            def disable() -> None:
                cls._disabled = True

            return disable
        if name == "enable":

            def enable() -> None:
                cls._disabled = False

            return enable
        if name in _MAP:
            return "" if cls._disabled else _MAP[name]
        raise AttributeError(f"type object {cls.__name__!r} has no attribute {name!r}")


class Colors(metaclass=_ColorsMeta):
    """ANSI colors; use ``Colors.disable()`` / ``Colors.enable()`` around plain-text output."""

    pass


def print_header(text):
    """Print section header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}\n")


def print_status(label, status, message=""):
    """Print status with color coding"""
    if status in ["HEALTH_OK", "Ready", "Running", "Succeeded", "Connected", "True"]:
        color = Colors.GREEN
        icon = "✓"
    elif status in ["HEALTH_WARN", "Progressing", "Creating", "Pending"]:
        color = Colors.YELLOW
        icon = "⚠"
    else:
        color = Colors.RED
        icon = "✗"

    print(f"{color}{icon} {label}: {status}{Colors.END}", end="")
    if message:
        print(f" - {message}")
    else:
        print()
