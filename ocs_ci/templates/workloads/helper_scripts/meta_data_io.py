"""
This is a helper script, it will be copied to an app pod and run, to create millions of files.
This also do some file operations like 'change file mode', 'Rename the file', 'Change group' etc
This script will run only for 10 mins and exit automatically in app pods.
"""

import time
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import fcntl

logging.basicConfig(
    filename="metadata_operations.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def log_metadata_operation(operation, file_path, from_metadata, to_metadata):
    log_message = (
        f"{operation} metadata for file {file_path}: {from_metadata} -> {to_metadata}"
    )
    logger.info(log_message)


def create_files(base_path, num_files):
    try:
        file_paths = []
        for i in range(1, num_files + 1):
            file_path = os.path.join(base_path, f"file{i}.txt")
            with open(file_path, "w") as f:
                f.write(f"This is file {i}")
                pass
            file_paths.append(file_path)
        return file_paths
    except Exception as e:
        logger.error(f"Error during file creation: {str(e)}")
        return []


def get_extended_attribute(file_path, attr_name):
    try:
        attr_value = os.getxattr(file_path, attr_name)
        return attr_value.decode("utf-8")
    except (OSError, IOError) as e:
        logger.error(f"Error getting extended attribute: {str(e)}")
        return None


def set_extended_attribute(file_path, attr_name, attr_value):
    try:
        os.setxattr(file_path, attr_name, attr_value.encode("utf-8"))
    except (OSError, IOError) as e:
        logger.error(f"Error setting extended attribute: {str(e)}")


def perform_metadata_operations(file_path):
    try:
        # Acquire a lock on the file
        with open(file_path, "a") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            # Get initial metadata
            initial_metadata = os.stat(file_path)
            log_metadata_operation("Initial", file_path, None, initial_metadata)

            # Modify metadata (example: change file mode)
            os.chmod(file_path, 0o755)
            modified_metadata = os.stat(file_path)
            log_metadata_operation(
                "ModifyPermission", file_path, initial_metadata, modified_metadata
            )

            # Rename the file
            new_file_path = file_path + "_renamed"
            os.rename(file_path, new_file_path)
            renamed_metadata = os.stat(new_file_path)
            log_metadata_operation(
                "Rename", new_file_path, initial_metadata, renamed_metadata
            )

            # Change group (example: change to the group 'users')
            os.chown(new_file_path, -1, os.getgid())  # Set to the default group
            group_changed_metadata = os.stat(new_file_path)
            log_metadata_operation(
                "ChangeGroup", new_file_path, renamed_metadata, group_changed_metadata
            )

            # Set extended attribute using setfattr
            attr_name = "user.test_attr"
            attr_value = "test_value"
            set_extended_attribute(new_file_path, attr_name, attr_value)
            retrieved_attr_value = get_extended_attribute(new_file_path, attr_name)
            log_metadata_operation(
                "Setxattr", new_file_path, None, f"{attr_name}={retrieved_attr_value}"
            )

            # Release the lock
            fcntl.flock(lock_file, fcntl.LOCK_UN)

    except Exception as e:
        logger.error(f"Error during metadata operations: {str(e)}")


if __name__ == "__main__":
    base_path = "/mnt/sample_directory"
    num_files = 5000000
    num_clients = 50
    duration_seconds = 600
    os.makedirs(base_path, exist_ok=True)
    file_paths = create_files(base_path, num_files)

    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            executor.map(perform_metadata_operations, file_paths)
