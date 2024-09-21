import logging
import re
import subprocess
import sys
import os
from threading import Thread

logger = logging.getLogger(__name__)


class LogPipe(Thread):

    def __init__(self, level: int):
        super().__init__()
        self.daemon = False
        self.level = level
        self.fdRead, self.fdWrite = os.pipe()
        self.pipeReader = os.fdopen(self.fdRead)
        self.start()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return self.close()

    def fileno(self):
        return self.fdWrite

    def run(self):
        for line in iter(self.pipeReader.readline, ''):
            logger.log(self.level, line.strip('\n'))
        self.pipeReader.close()

    def close(self):
        os.close(self.fdWrite)


class Shell:

    @staticmethod
    def to_str(*args: str):
        arr = []
        for a in args:
            if " " in a or "!" in a:
                a = "'" + a + "'"
            arr.append(a)
        return " ".join(arr)

    @staticmethod
    def run(*args: str, **kwargs) -> int:
        logger.info("$ " + Shell.to_str(*args))
        out = subprocess.call(args, **kwargs)
        if out != 0:
            logger.error("# exit code", out)
        return out

    @staticmethod
    def safe_get(*args, **kwargs) -> int:
        try:
            return Shell.get(*args, **kwargs)
        except subprocess.CalledProcessError:
            pass
        return None

    @staticmethod
    def get(*args: str, **kargv) -> str:
        logger.info("$ " + Shell.to_str(*args))
        with LogPipe(logging.ERROR) as logpipe:
            output = subprocess.check_output(args, stderr=logpipe)
        text: str = output.decode(sys.stdout.encoding)
        lines = len(list(ln for ln in re.split(r"\s+", text) if ln.strip()))
        logging.debug("> %s lines", lines)
        return text
