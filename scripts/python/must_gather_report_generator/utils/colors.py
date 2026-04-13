"""ANSI color codes and printing utilities"""


class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


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
