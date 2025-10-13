# import os
import threading
import sys
import subprocess
import logging

logger = logging.getLogger(__name__)


def write_to_file(filename, data, pod_namespace, client_pod_name):
    # with open(filename, 'a') as f:
    #     f.write(data + '\n')
    #     f.flush()
    #     os.fsync(f.fileno())
    command = [
        "oc",
        "rsh",
        "-n",
        pod_namespace,
        client_pod_name,
        "python3",
        "-c",
        f"import os; with open('{filename}', 'a') as f: f.write('{data}\n'); f.flush(); os.fsync(f.fileno())",
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"An error occurred while writing to the file: {e}")


def read_lines_from_server(filename, pod_namespace, server_pod_name):
    command = [
        "oc",
        "rsh",
        "-n",
        pod_namespace,
        server_pod_name,
        "python3",
        "-c",
        f"import os; with open('{filename}', 'r') as f: print(f.readlines())",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    return result.stdout.strip().split("\n")


if __name__ == "__main__":
    filename = "/mnt/shared_filed.html"
    data = "Test fsync\n"
    lines_to_write = [f"{data}{i}" for i in range(0, 2500)]

    if len(sys.argv) != 4:
        print(
            "Usage: python3 <filename> <pod_namespace> <client_pod_name> <server_pod_name>"
        )
        sys.exit(1)

    filename = sys.argv[0]
    pod_namespace = sys.argv[1]
    client_pod_name = sys.argv[2]
    server_pod_name = sys.argv[3]

    def write_and_read():
        try:
            for line in lines_to_write:
                write_to_file(filename, line, pod_namespace, client_pod_name)
                print("Wrote line:", line)
                threading.Thread(
                    target=lambda: read_lines_from_server,
                    args=(filename, pod_namespace, server_pod_name),
                ).start()
        except Exception as e:
            logger.error(f"Error during write and read of file: {str(e)}")

    threads = [threading.Thread(target=write_and_read) for _ in range(2500)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    server_content = read_lines_from_server(filename, pod_namespace, server_pod_name)
    print("Server content:", server_content)
