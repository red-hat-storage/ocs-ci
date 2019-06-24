import sys

import ocsci.main


def main():
    arguments = sys.argv[1:]
    exit(ocsci.main.main(arguments))


if __name__ == "__main__":
    main()
