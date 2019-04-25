try:
    from IOHandler import *
except SystemError or ImportError:
    from .IOHandler import *
import warnings

IODECO_DEFAULT_NUM_OBJECT_IN_QUEUE = 150
default_iohandler = IOHandler(IODECO_DEFAULT_NUM_OBJECT_IN_QUEUE)


def create_session(max_len):
    iohandler = IOHandler(max_len)
    return iohandler


def _get_iohandler(_iohandler):
    if _iohandler is None:
        warnings.warn("iohandler is not provided. Using default shared iohandler with max_len = 150")
        iohandler = default_iohandler
    else:
        iohandler = _iohandler
    return iohandler


def rio(_iohandler=None):
    iohandler = _get_iohandler(_iohandler)
    return iohandler.read


def wio(_iohandler=None):
    iohandler = _get_iohandler(_iohandler)
    return iohandler.write


def pio(_iohandler=None):
    iohandler = _get_iohandler(_iohandler)
    return iohandler.process
