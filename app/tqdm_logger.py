from io import StringIO


class TqdmToLogger(StringIO):
    """
        Output stream for tqdm which will output to logger module instead of
        the StdOut.
    """

    logger = None
    level = None
    buf = ""

    def __init__(self, logger, level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip("\r\n\t ")
        # self.buf = buf.strip("\n\t ") + "\r"

    def flush(self):
        self.logger.log(self.level, self.buf)
