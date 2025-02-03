import ast
import sys


class RetryValidator(ast.NodeVisitor):
    def __init__(self, filename):
        self.errors = []
        self.filename = filename
        self.processed_lines = set()  # Track processed lines

    def visit_Call(self, node):
        """Visit function calls and check for retry usage."""
        if isinstance(node.func, ast.Name) and node.func.id == "retry":
            self._validate_retry_call(node)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        """Visit function definitions and check for retry decorator."""
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "retry"
            ):
                self._validate_retry_call(decorator)
        self.generic_visit(node)

    def _validate_retry_call(self, node):
        """Validate the arguments of a retry call."""
        if node.lineno in self.processed_lines:
            return  # Skip already processed lines
        self.processed_lines.add(node.lineno)
        kwargs = {arg.arg: arg.value for arg in node.keywords}

        # Retrieve and evaluate values
        tries = self._eval_value(kwargs.get("tries"), default=4)
        delay = self._eval_value(kwargs.get("delay"), default=3)
        backoff = self._eval_value(kwargs.get("backoff"), default=2)
        max_delay = self._eval_value(kwargs.get("max_delay"), default=600)
        max_timeout = self._eval_value(kwargs.get("max_timeout"), default=14400)

        # Calculate total retry time
        total_time = self._calculate_total_time(tries, delay, backoff, max_delay)

        # Validation rules
        if tries is not None and tries <= 0:
            self.errors.append((node.lineno, "Invalid 'tries': must be > 0."))
        if delay is not None and delay <= 0:
            self.errors.append((node.lineno, "Invalid 'delay': must be > 0."))
        if backoff is not None and backoff < 1:
            self.errors.append((node.lineno, "Invalid 'backoff': must be >= 1."))
        if max_delay is not None and max_delay <= 0:
            self.errors.append((node.lineno, "Invalid 'max_delay': must be > 0."))
        if max_timeout is not None and max_timeout <= 0:
            self.errors.append((node.lineno, "Invalid 'max_timeout': must be > 0."))
        if total_time > max_timeout:
            self.errors.append(
                (
                    node.lineno,
                    f"Total retry time ({total_time}s or {total_time // 3600}h {total_time % 3600 // 60}m) "
                    f"exceeds max_timeout ({max_timeout}s or {max_timeout // 3600}h {max_timeout % 3600 // 60}m).",
                )
            )

    def _calculate_total_time(self, tries, delay, backoff, max_delay):
        """Calculate the total retry time."""
        total_time = 0
        current_delay = delay
        for _ in range(tries):
            total_time += current_delay
            current_delay = min(current_delay * backoff, max_delay)
        return total_time

    def _eval_value(self, node, default=None):
        """Evaluate a constant value from an AST node."""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Num):
            return node.n
        return default


def validate_file(filename):
    with open(filename, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=filename)

    validator = RetryValidator(filename)
    validator.visit(tree)

    if validator.errors:
        print("\n" + "=" * 50)
        print(f"ERROR in file: {filename}")
        print("=" * 50)
        for lineno, error in validator.errors:
            print(f"{filename}:{lineno}: {error}")
        print("=" * 50 + "\n")
        # Exit with failure if any errors were detected
        sys.exit(1)


if __name__ == "__main__":
    # Get list of files to validate
    files_to_check = sys.argv[1:]
    if not files_to_check:
        print("No files provided for validation.")
        sys.exit(1)

    for file in files_to_check:
        validate_file(file)
