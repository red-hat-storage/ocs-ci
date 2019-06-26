import sys

from .framework import main as runner


def main():
    arguments = sys.argv[1:]
    exit(runner.main(arguments))


if __name__ == "__main__":
    main()
