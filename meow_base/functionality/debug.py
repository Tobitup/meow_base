"""
This file contains functions for debugging and logging.

Author(s): David Marchant
"""

from typing import Any, Tuple

from .validation import check_type
from ..core.vars import DEBUG_INFO, DEBUG_WARNING, DEBUG_DEBUG


def setup_debugging(print:Any=None, logging:int=0)->Tuple[Any,int]:
    """Create a place for debug messages to be sent. Always returns a place, 
    along with a logging level."""
    check_type(logging, int)
    if print is None:
        return None, 0
    else:
        if not isinstance(print, object):
            raise TypeError(f"Invalid print location provided")
        writeable = getattr(print, "write", None)
        if not writeable or not callable(writeable):
            raise TypeError(f"Print object does not implement required "
                "'write' function")

    return print, logging


def print_debug(print_target, debug_level, msg, level)->None:
    """Function to print a message to the debug target, if its level exceeds 
    the given one."""
    if print_target is None:
        return
    else:
        if level <= debug_level:
            status = "ERROR"
            if level == DEBUG_INFO:
                status = "INFO"
            elif level == DEBUG_WARNING:
                status = "WARNING"
            elif level == DEBUG_DEBUG:
                status = "DEBUG"
            print(f"{status}: {msg}", file=print_target)
