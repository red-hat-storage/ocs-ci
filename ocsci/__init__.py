"""
Avoid already-imported warning cause of we are importing this package from
run wrapper for loading config.

You can see documentation here:
https://docs.pytest.org/en/latest/reference.html
under section PYTEST_DONT_REWRITE
"""
# Use the new python 3.7 dataclass decorator, which provides an object similar
# to a namedtuple, but allows type enforcement and defining methods.
from dataclasses import dataclass, field, fields


@dataclass
class Config:
    DEPLOYMENT: dict = field(default_factory=dict)
    ENV_DATA: dict = field(default_factory=dict)
    REPORTING: dict = field(default_factory=dict)
    RUN: dict = field(default_factory=dict)

    def __post_init__(self):
        self.reset()

    def reset(self):
        for f in fields(self):
            setattr(self, f.name, f.default_factory())
        self.RUN['cli_params'] = dict()

    def to_dict(self):
        # We don't use dataclasses.asdict() here, because that function appears
        # to create copies of fields - meaning changes to the return value of
        # this method will not be reflected in the field themselves.
        field_names = [f.name for f in fields(self)]
        return {
            name: getattr(self, name) for name in field_names
        }


config = Config()
