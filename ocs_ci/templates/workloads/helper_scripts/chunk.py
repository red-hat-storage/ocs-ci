# this script generates IO with large file chunk
file_path = "/mnt/chunk_large_file.txt"
file_size = 1024 * 1024 * 10000
with open(file_path, "wb") as file:
    chunk_size = 1
    num_chunks = file_size // chunk_size
    for _ in range(num_chunks):
        chunk = b"\0" * chunk_size
        file.write(chunk)
print(f"File '{file_path}' created with a size of {file_size} bytes.")
