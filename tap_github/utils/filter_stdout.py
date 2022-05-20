import contextlib
import io
import re
import sys


class FilterStdOutput(object):
    """Filter out stdout/sterr given a regex pattern."""

    def __init__(self, stream, re_pattern):
        self.stream = stream
        self.pattern = (
            re.compile(re_pattern) if isinstance(re_pattern, str) else re_pattern
        )
        self.triggered = False

    def __getattr__(self, attr_name):
        return getattr(self.stream, attr_name)

    def write(self, data):
        if data == "\n" and self.triggered:
            self.triggered = False
        else:
            if self.pattern.search(data) is None:
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
    sys.stdout = io.BytesIO()
    yield
    sys.stdout = save_stdout
