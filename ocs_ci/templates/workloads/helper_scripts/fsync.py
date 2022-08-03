import os


def fsync():
    while True:
        fd = os.open("/mnt/mydir/", os.O_DIRECTORY | os.O_RDONLY)
        print("type of fd: ", type(fd))
        if fd < -1:
            raise Exception("create")
        os.fsync(fd)
        os.close(fd)

    return 9


fsync()
