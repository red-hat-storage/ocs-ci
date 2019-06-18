import os
import sys

import ocsci.main


def main():
    HERE = os.path.abspath(os.path.dirname(__file__))
    from utility.utils import add_path_to_env_path
    add_path_to_env_path(os.path.join(HERE, 'bin'))
    arguments = sys.argv[1:]
    exit(ocsci.main.main(arguments))


if __name__ == "__main__":
    main()
