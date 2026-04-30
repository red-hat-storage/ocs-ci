# Automated procedure of testing smallfileio with different operation given by user via YAML
# OPTIMIZED VERSION - Memory efficient with log rotation and subprocess streaming

import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
import shutil
import uuid

# Configuration from environment
base_dir = os.environ.get('BASE_DIR', '/mnt/base')
base_file_count = int(os.environ.get('BASE_FILE_COUNT', '1000000'))
file_size = int(os.environ.get('FILES_SIZE', 1))
threads = int(os.environ.get('THREADS', 8))
MULTIPLICATION_FACTORS = list(map(int, os.environ.get('MULTIPLICATION_FACTORS', '2,3,4,5,4,3,2').split(',')))
output_dir = os.environ.get('OUTPUT_DIR', '/mnt/output')
smallfile_script_path = '/smallfile/smallfile_cli.py'
shared_sync_dir = os.path.join(base_dir, 'shared_sync')

# Default operations
default_operations = ["create", "delete", "delete-renamed", "readdir", "setxattr", "getxattr", "truncate-overwrite",
                      "append", "read", "stat", "chmod", "ls-l", "mkdir", "symlink", "overwrite"]
given_operations = os.environ.get('OPERATIONS', ",".join(default_operations)).split(",")

# Logging setup - OPTIMIZED with rotation
os.makedirs(output_dir, exist_ok=True)
log_file_name_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_name = f"smallfile_operations_{log_file_name_time}.log"
log_file_path = os.path.join(output_dir, log_file_name)

# Create logger with module name to avoid conflicts
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers to prevent duplicates
logger.handlers.clear()

# Add rotating file handler - limits memory usage
file_handler = RotatingFileHandler(
    log_file_path,
    maxBytes=100*1024*1024,  # 100MB per file
    backupCount=10,          # Keep 10 backups (1GB total)
    encoding='utf-8'
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s")
)
logger.addHandler(file_handler)

# Add console handler only if not already present
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_handler)

logger.info("=" * 80)
logger.info("CephFS Stress Test - OPTIMIZED VERSION")
logger.info(f"Log rotation enabled: 100MB per file, 10 backups")
logger.info(f"Subprocess output streaming enabled")
logger.info(f"Thread pool bounded to optimal size")
logger.info("=" * 80)


def run_smallfile_command(operation, top_dir, base_file_count, file_size):
    """
    Executes a smallfile operation using subprocess with output streaming to file.
    OPTIMIZED: Streams subprocess output to disk instead of capturing in memory.

    Args:
        operation (str): The smallfile operation to execute (e.g., 'create', 'read', etc)
        top_dir (str): Target directory where the operation will run
        base_file_count (int): Number of files involved in the operation
        file_size (int): File size in KB

    Returns:
        bool: True if the operation was successful, False otherwise.

    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    output_json_path = os.path.join(output_dir, f"{operation}_{timestamp}_{unique_id}.json")
    
    # OPTIMIZED: Create separate log file for subprocess output (for debugging)
    output_log_path = os.path.join(output_dir, f"{operation}_{timestamp}_{unique_id}_output.log")

    sync_dir_name = f"sync_{operation}_{timestamp}_{unique_id}"
    current_sync_dir = os.path.join(shared_sync_dir, sync_dir_name)

    try:
        os.makedirs(current_sync_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"CRITICAL: Could not create sync dir {current_sync_dir}: {e}")
        return False

    cmd = [
        sys.executable, smallfile_script_path,
        "--operation", operation,
        "--threads", str(threads),
        "--top", top_dir,
        "--verbose", "true",
        "--log-to-stderr", "true",
        "--verify-read", "false",
        "--fsync", "false",
        "--output-json", output_json_path,
        "--network-sync-dir", current_sync_dir
    ]
    if base_file_count:
        cmd.extend(["--files", str(base_file_count)])
    if file_size:
        cmd.extend(["--file-size", str(file_size)])

    try:
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Output streaming to: {output_log_path}")
        
        # OPTIMIZED: Stream subprocess output directly to file (saves memory)
        # Uses 8KB buffer instead of capturing 100MB-500MB in memory
        with open(output_log_path, 'w', buffering=8192) as output_file:
            subprocess.run(
                cmd,
                stdout=output_file,           # Stream stdout to file
                stderr=subprocess.STDOUT,     # Combine stderr with stdout
                check=True,
                timeout=86400,
                text=True
            )
        
        logger.info(f"Completed operation: {operation}")
        logger.info(f"Output saved to: {output_log_path}")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error(f"TIMEOUT: Operation {operation} timed out.")
        logger.error(f"Partial output may be in: {output_log_path}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"FAILURE: Operation {operation} failed: {e}")
        logger.error(f"Error output in: {output_log_path}")
        return False
    except Exception as e:
        logger.error(f"EXCEPTION: {e}")
        return False
    finally:
        # Clean up the unique sync directory so we don't leak inodes
        if os.path.exists(current_sync_dir):
            try:
                shutil.rmtree(current_sync_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to clean sync dir {current_sync_dir}: {e}")


def run_background_create(top_dir, base_file_count, file_size, subdir_name=None):
    """
    Run the 'create' operation as a background thread, optionally in a specified subdirectory

    Args:
        top_dir (str): The top-level directory where the operation will be executed
        base_file_count (int): The number of files to create
        file_size (int): The size of each file in KB
        subdir_name (str, optional): The name of the subdirectory to create files in

    Returns:
        threading.Thread: A background thread object

    """
    if subdir_name:
        top_dir = os.path.join(top_dir, subdir_name)
    return threading.Thread(
        target=run_smallfile_command,
        args=("create", top_dir, base_file_count, file_size),
        daemon=True,
        name=f"create-{top_dir}"
    )


def run_background_rename(top_dir, base_file_count, file_size):
    """
    Run rename operation concurrently after file creation is completed.

    Args:
        top_dir (str): The top directory where the rename operation will be performed
        base_file_count (int): The number of files to be renamed

    Returns:
        threading.Thread: The newly created thread object

    """
    return threading.Thread(
        target=run_smallfile_command,
        args=("rename", top_dir, base_file_count, file_size),
        daemon=True,
        name=f"rename-{top_dir}"
    )


def perform_iteration(iter_num, multiplier):
    """
    Perform iteration of smallfile operations based on the given multiplier.
    OPTIMIZED: Fixed variable initialization to prevent unbound variable errors.

    Args:
        iter_num (int): Current iteration number.
        multiplier (int): Used to scale the number of files for the 'create' operation

    """
    current_dir = f"{base_dir}/iter{iter_num}"
    previous_dir = f"{base_dir}/iter{iter_num - 1}" if iter_num > 0 else None
    os.makedirs(current_dir, exist_ok=True)
    # OPTIMIZED: Remove redundant makedirs for previous_dir (already exists from previous iteration)

    # Multiply the base file count by the given multiplier
    updated_base_file_count = base_file_count * multiplier
    rename_data_file_count = int(base_file_count / 2)
    logger.info(f"-----------Starting iteration:{iter_num}------------------------")

    # OPTIMIZED: Initialize variables to prevent unbound errors
    rename_thread = None
    execute_concurrently_count = 0

    # start the rename operation concurrently on a new set of files
    if (("rename" in given_operations) or ("delete-renamed" in given_operations)) and iter_num > 0 and previous_dir:
        rename_subdir = "rename_data"
        os.makedirs(os.path.join(previous_dir, rename_subdir), exist_ok=True)
        # Create files in the rename_data subdir for renaming
        create_thread_rename = run_background_create(previous_dir, rename_data_file_count, subdir_name=rename_subdir, file_size=file_size)
        create_thread_rename.start()
        logger.info(f"For rename operation, started Creating {base_file_count} files in {current_dir}, operations on {previous_dir or 'N/A'}")
        create_thread_rename.join()  # Wait for file creation to finish in rename_data subdir
        logger.info(f"Completed file creation in {rename_subdir} for rename operation")

        # Start the rename operation in the background
        rename_thread = run_background_rename(os.path.join(previous_dir, rename_subdir), rename_data_file_count, file_size=file_size)
        rename_thread.start()
        logger.info(f"Started Rename operation in background on {rename_data_file_count} files in {os.path.join(previous_dir, rename_subdir)}")

    # Start the create operation in the background
    create_thread = run_background_create(current_dir, base_file_count=updated_base_file_count, file_size=file_size)
    create_thread.start()
    logger.info(f"Started Creating {base_file_count} files in {current_dir} from each thread, operations on {previous_dir or 'N/A'}")

    # Perform other file operations concurrently in the previous directory
    if iter_num > 0:
        ops = [op for op in given_operations if op != "create" and op != "rename"]
        top_dirs = [previous_dir] * len(ops)

        # Run execute_concurrently in a loop until the create_thread completes
        while create_thread.is_alive():
           execute_concurrently(ops, top_dirs, base_file_count=int(updated_base_file_count/multiplier))
           time.sleep(60)
           execute_concurrently_count += 1
           logger.info(f"execute_concurrently run count: {execute_concurrently_count}")

    # Wait for all threads to finish (file creation, rename operation and other concurrent operations)
    create_thread.join()  # Wait for file creation to finish
    logger.info(f"File creation thread completed successfully")
    if iter_num > 0:
        logger.info(f"Total execute_concurrently runs before create_thread finished: {execute_concurrently_count}")
    if rename_thread is not None:  # OPTIMIZED: Check if rename_thread was created
        rename_thread.join()
        logger.info(f"Rename thread completed successfully")
    logger.info(f"Iteration {iter_num} completed successfully")
    logger.info(f"-----------Completed iteration:{iter_num}-----------------------")



def execute_concurrently(operations, top_dirs, base_file_count, delay_between_ops=3):
    """
    Executes multiple operations concurrently using threads.
    OPTIMIZED: Bounded ThreadPoolExecutor to prevent excessive thread creation and memory usage.

    Args:
        operations (list): A list of operations to run concurrently (e.g., ['create', 'rename']).
        top_dirs (list): A list of directories corresponding to each operation in 'operations'.
        base_file_count (int): Number of files for each operation.
        delay_between_ops (int, optional): Delay (in seconds) between each operation's start.

    Returns:
        dict: A dictionary containing the results for each operation (True/False).

    """
    results = {}
    
    # OPTIMIZED: Calculate optimal thread count based on CPU cores
    # Cap at 32 threads to prevent excessive memory usage (18MB per thread)
    cpu_count = os.cpu_count() or 4
    max_workers = min(32, cpu_count + 4)
    
    logger.debug(f"Using ThreadPoolExecutor with max_workers={max_workers} for {len(operations)} operations")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_op = {}

        for i, op in enumerate(operations):
            future = executor.submit(run_smallfile_command, op, top_dirs[i], base_file_count=base_file_count, file_size=file_size)
            future_to_op[future] = op

            if i < len(operations) - 1:
                time.sleep(delay_between_ops)  # Delay between submitting each task
                logger.info(f"waiting for {delay_between_ops} seconds before starting the next concurrent operation")

        for future in as_completed(future_to_op):
            op = future_to_op[future]
            try:
                results[op] = future.result()
            except Exception as exc:
                logger.error(f"Unexpected error in operation {op}: {exc}")
                results[op] = False

    return results


def main():
    """
    Main entry point of the program

    Iterates over the MULTIPLICATION_FACTORS defined in the environment variables, performing
    smallfile operations in each iteration

    """
    # Loop through each iteration and apply the multiplier
    for i, multiplier in enumerate(MULTIPLICATION_FACTORS):
        perform_iteration(i, multiplier)

    logger.info("All iterations complete.")


if __name__ == "__main__":
    main()