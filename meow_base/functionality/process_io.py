"""
This file contains functions for reading and writing between processes.

Author(s): David Marchant
"""

from os import name as osName
from typing import List

from multiprocessing.connection import Connection, wait as multi_wait
# Need to import additional Connection type for Windows machines
if osName == 'nt':
    from multiprocessing.connection import PipeConnection
from multiprocessing.queues import Queue

from ..core.vars import VALID_CHANNELS


def wait(inputs:List[VALID_CHANNELS])->List[VALID_CHANNELS]:
    if osName == 'nt':
        return wait_windows(inputs)
    return wait_linux(inputs)

def wait_windows(inputs:List[VALID_CHANNELS])->List[VALID_CHANNELS]:
    all_connections = [i for i in inputs if type(i) is Connection] \
        + [i for i in inputs if type(i) is PipeConnection] \
        + [i._reader for i in inputs if type(i) is Queue]
    ready = multi_wait(all_connections)
    ready_inputs = [i for i in inputs if \
        (type(i) is Connection and i in ready) \
        or (type(i) is PipeConnection and i in ready) \
        or (type(i) is Queue and i._reader in ready)]
    return ready_inputs

def wait_linux(inputs:List[VALID_CHANNELS])->List[VALID_CHANNELS]:
    all_connections = [i for i in inputs if type(i) is Connection] \
        + [i._reader for i in inputs if type(i) is Queue]
    ready = multi_wait(all_connections)
    ready_inputs = [i for i in inputs if \
        (type(i) is Connection and i in ready) \
        or (type(i) is Queue and i._reader in ready)]
    return ready_inputs
