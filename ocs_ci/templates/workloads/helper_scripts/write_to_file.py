import os


def write_to_file(filename, data):
    with open(filename, "a") as f:
        f.write(data + "\n")
        f.flush()
        os.fsync(f.fileno())


if __name__ == "__main__":
    filename = "/mnt/shared_filed.html"
    data = "Test fsync\n"
    for _ in range(2500):
        write_to_file(filename, data)
