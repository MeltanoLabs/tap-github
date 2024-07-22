from __future__ import annotations

import contextlib
import io
import re
import sys
from typing import Pattern, TextIO


class FilterStdOutput:
    """Filter out stdout/stderr given a regex pattern."""

    def __init__(self, stream: TextIO, re_pattern: str | Pattern):  # noqa: FA100
        self.stream = stream
        self.pattern = (
            re.compile(re_pattern) if isinstance(re_pattern, str) else re_pattern
        )
        self.triggered = False

    def __getattr__(self, attr_name: str):
        return getattr(self.stream, attr_name)

    def write(self, data):
        if data == "\n" and self.triggered:
            self.triggered = False
        elif self.pattern.search(data) is None:
            self.stream.write(data)
            self.stream.flush()
        else:
            # caught bad pattern
            self.triggered = True

    def flush(self):
        self.stream.flush()


@contextlib.contextmanager
def nostdout():
    save_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield
    sys.stdout = save_stdout
