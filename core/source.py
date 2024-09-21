from dataclasses import dataclass, replace
from typing import Tuple
from functools import cached_property
from os.path import basename


class TypeException(TypeError):
    def __init__(self, name: str, expected: str, gotit: str):
        super().__init__(f"{name} expected type '{expected}', but got '{gotit}' instead.")


@dataclass(frozen=True)
class Source:
    file: str
    exclude: Tuple[str, ...] = tuple()
    include: Tuple[str, ...] = tuple()
    rename: Tuple[str, ...] = tuple()
    prefix: str = ""
    sufix: str = ""

    def __post_init__(self):
        self.validate()

    @cached_property
    def name(self):
        return basename(self.file)

    def merge(self, **kwargs):
        nw = replace(self, **kwargs)
        nw.validate()
        return nw

    def validate(self):
        if not isinstance(self.prefix, str):
            raise TypeException('prefix', 'int', type(self.prefix).__name__)

        if not isinstance(self.sufix, str):
            raise TypeException('sufix', 'int', type(self.sufix).__name__)

        if len(self.exclude) > 0 and len(self.include) > 0:
            raise ValueError("Incompatible arguments: exclude, include")
