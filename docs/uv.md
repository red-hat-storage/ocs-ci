# UV Usage

## Overview
We now have the ability to manage the project dependencies and execution with [uv](https://docs.astral.sh/uv/).

## Basic Usage
For more detailed information about uv, please refer to their own [documentation](https://docs.astral.sh/uv/).

### Project Setup
#### Python Versions
If needed, uv can be used to install different versions of python on your system: [Installing Python](https://docs.astral.sh/uv/guides/install-python/). Otherwise it will detect and use the versions installed on your system already.

#### Project Dependencies
You can sync the project dependencies by running `uv sync` from the project directory. This will create a venv in `<PROJECT_DIR>/.venv` based on the dependencies defined in the `pyproject.toml`.
> [!WARNING]
> If you already have a venv at `<PROJECT_DIR>/.venv` you will want to move or remove it before running `uv sync`
```
uv sync
```

### Executing Entry-points
All of our existing project entry points are accessible through `uv run`. All of the same CLI parameters exist here as the code entry points are the same. The main difference is that uv will ensure we are running these with our projects virtual environment, and that all of our dependencies are installed.

Examples:
```
uv run run-ci --help
uv run deploy-fusion --help
uv run vsphere-cleanup --help
```

### Managing Dependencies
Now that uv is handling the project dependencies, when adding or updating dependencies we will want to do so through uv. This is to ensure that our files get updated properly, as well as ensure we are using compatible versions.

For in depth information on adding, removing, or updating dependencies, please refer to the uv [dependency documentation](https://docs.astral.sh/uv/concepts/projects/dependencies/)
