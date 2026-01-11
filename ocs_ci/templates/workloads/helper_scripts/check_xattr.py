import os
import errno
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


def print_help():
    """Print help message and exit."""
    help_text = """
Usage: python check_xattr.py [DIRECTORY] [FILE_COUNT] [MAX_WORKERS]

Creates test files and continuously toggles extended attributes (xattr) on them in parallel.

Arguments:
  DIRECTORY     Directory name to store test files (default: test_directory)
  FILE_COUNT    Number of test files to create (default: 5)
  MAX_WORKERS   Number of parallel worker threads (default: 10, max: 100)
  -h, --help    Show this help message and exit

Examples:
  python check_xattr.py              # 5 files, 10 workers, test_directory
  python check_xattr.py 20           # 20 files, 10 workers, test_directory
  python check_xattr.py 20 5         # 20 files, 5 workers, test_directory
  python check_xattr.py my_dir       # 5 files, 10 workers, my_dir
  python check_xattr.py my_dir 20    # 20 files, 10 workers, my_dir
  python check_xattr.py my_dir 20 5  # 20 files, 5 workers, my_dir
  python check_xattr.py -h           # Show this help message

The program will continuously toggle the 'user.author' extended attribute
on each file between 'kevin' and 'michael'. Press Ctrl+C to stop gracefully.
    """
    print(help_text)
    sys.exit(0)


def create_files(prefix: str, count: int, directory: str = ".") -> list[str]:
    """
    Create `count` empty files named <prefix>1, <prefix>2, ... in `directory`.
    Returns a list of created file paths (strings).
    """
    if count < 1:
        return []
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    created = []
    for i in range(1, count + 1):
        p = dir_path / f"{prefix}{i}"
        p.touch(exist_ok=True)
        created.append(str(p))
    return created


def toggle_author_xattr(path: str) -> bytes:
    """
    Ensure file at `path` has user.author xattr.
    - If attribute missing: set to b'kevin'
    - If present and equals b'kevin': change to b'michael'
    - If present and not b'kevin': change to b'kevin'
    Returns the new value (bytes).
    Raises OSError on underlying errors.
    """
    name = b"user.author"
    try:
        val = os.getxattr(path, name)
    except OSError as e:
        # ENODATA (Linux) / ENOATTR (macOS) when attribute is missing
        # Other errors (like file not found, permission) propagate
        if e.errno in (errno.ENODATA, errno.ENOTSUP) or getattr(e, "errno", None) in (
            None,
        ):
            val = None
        else:
            raise

    new = b"kevin"
    # If attribute present
    if val == new:
        new = b"michael"

    os.setxattr(path, name, new)
    return new


if __name__ == "__main__":
    # Check for help flag anywhere in sys.argv
    if "-h" in sys.argv or "--help" in sys.argv:
        print_help()

    # Parse arguments: if first arg is a string (not a number), use it as directory name
    directory = "test_directory"
    file_count = 5
    max_workers = 10

    if len(sys.argv) > 1:
        try:
            # Try to convert first argument to int
            file_count = int(sys.argv[1])
            # Get max_workers from second argument if provided
            if len(sys.argv) > 2:
                max_workers = int(sys.argv[2]) if sys.argv[2].isdigit() else max_workers
        except ValueError:
            # First argument is a string, use it as directory name
            directory = sys.argv[1]
            # Get file_count from second argument if provided
            if len(sys.argv) > 2:
                file_count = int(sys.argv[2]) if sys.argv[2].isdigit() else file_count
            # Get max_workers from third argument if provided
            if len(sys.argv) > 3:
                max_workers = int(sys.argv[3]) if sys.argv[3].isdigit() else max_workers

    # Cap max_workers to 100 and ensure it doesn't exceed file_count
    max_workers = min(max_workers, 100, file_count)

    # Example usage
    files = create_files("testfile_", file_count, directory)
    print(f"Creation of {len(files)} files in '{directory}' directory completed.")

    # Toggle author xattr for each file continuously in parallel batches
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                futures = [executor.submit(toggle_author_xattr, file) for file in files]
                for file, future in zip(files, futures):
                    _ = future.result()
                print(".", end="", flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Shutting down gracefully...")
        # ThreadPoolExecutor context manager automatically calls shutdown(wait=True)
        # which waits for all pending tasks to complete before exiting
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        print(f"All resources released. Worker threads: {max_workers}. Exiting...")
