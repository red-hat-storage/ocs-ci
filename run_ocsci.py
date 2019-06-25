import sys

import framework.main


def main():
    arguments = sys.argv[1:]
    exit(framework.main.main(arguments))


if __name__ == "__main__":
    main()
