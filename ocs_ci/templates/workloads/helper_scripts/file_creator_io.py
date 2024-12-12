# This is a helper script where it will be copied to an app pod and run, to create millions of files concurrently
from concurrent.futures import ThreadPoolExecutor
import os
import logging

logger = logging.getLogger(__name__)


def create_files(base_path, num_files):
    try:
        file_paths = []
        for i in range(1, num_files + 1):
            file_path = os.path.join(base_path, f"file{i}.txt")
            with open(file_path, "w") as f:
                f.write(f"This is file {i}")
            file_paths.append(file_path)
        return file_paths
    except Exception as e:
        logger.error(f"Error during file creation: {str(e)}")
        return []


def create_files_concurrently(base_path, num_files, num_clients):
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [
            executor.submit(create_files, base_path, num_files)
            for _ in range(num_clients)
        ]
        results = [future.result() for future in futures]
        return results


if __name__ == "__main__":
    base_directory_prefix = "/mnt/sample_directory"
    num_files = 500000
    num_times = 10
    num_clients = 50
    concurrent_across_iterations = True

    if concurrent_across_iterations:
        directories = [f"{base_directory_prefix}_{i + 1}" for i in range(num_times)]
        for base_path in directories:
            os.makedirs(base_path, exist_ok=True)

        with ThreadPoolExecutor(max_workers=num_times) as executor:
            futures = [
                executor.submit(
                    create_files_concurrently, base_path, num_files, num_clients
                )
                for base_path in directories
            ]
            results = [future.result() for future in futures]

            for result in results:
                for file_paths in result:
                    print(f"Files created: {file_paths}")
    else:
        for i in range(num_times):
            base_path = f"{base_directory_prefix}_{i + 1}"
            os.makedirs(base_path, exist_ok=True)
            create_files_concurrently(base_path, num_files, num_clients)
