from __future__ import annotations

import contextlib
import io
import re
import sys
from re import Pattern
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    from collections.abc import Generator


class FilterStdOutput:
    """Filter out stdout/sterr given a regex pattern."""

    def __init__(self, stream: TextIO, re_pattern: str | Pattern) -> None:
        self.stream = stream
        self.pattern = (
            re.compile(re_pattern) if isinstance(re_pattern, str) else re_pattern
        )
        self.triggered = False

    def __getattr__(self, attr_name: str) -> object:
        return getattr(self.stream, attr_name)

    def write(self, data: str) -> None:
        if data == "\n" and self.triggered:
            self.triggered = False
        else:
            if self.pattern.search(data) is None:
                self.stream.write(data)
                self.stream.flush()
            else:
                # caught bad pattern
                self.triggered = True

    def flush(self) -> None:
        self.stream.flush()


@contextlib.contextmanager
def nostdout() -> Generator[None, None, None]:
    save_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield
    sys.stdout = save_stdout
