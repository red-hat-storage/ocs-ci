import subprocess
from multiprocessing import Process


def creates_files():
    while True:
        for i in range(125):
            subprocess.check_output(
                [
                    "dd",
                    "if=/dev/zero",
                    "of=/var/lib/www/html/mydir/emp{}".format(i),
                    "bs=2048",
                    "count=1024",
                ]
            )


def remove_files():
    while True:
        for i in range(125):
            subprocess.check_output(["rm", "/var/lib/www/html/mydir/emp{}".format(i)])


Process(target=creates_files()).start()
Process(target=remove_files()).start()
