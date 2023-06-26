"""
This file contains definitions for a MEOW pattern based off of socket events, 
along with an appropriate monitor for said events.

Author(s): David Marchant, Nikolaj Ingemann Gade
"""

import sys
import socket
import threading
import tempfile
import hashlib
import os

from time import time

from typing import Any, Dict, List, Union

from meow_base.functionality.validation import valid_string, valid_dict
from meow_base.core.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, DEBUG_INFO
from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.meow import EVENT_KEYS, valid_meow_dict
from meow_base.core.base_monitor import BaseMonitor
from meow_base.core.base_pattern import BasePattern
from meow_base.functionality.meow import create_event, create_rule
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.patterns.file_event_pattern import WATCHDOG_EVENT_KEYS, \
    create_watchdog_event

# socket events
EVENT_TYPE_SOCKET = "socket"
TRIGGERING_PORT = "triggering_port"

SOCKET_EVENT_KEYS = {
    TRIGGERING_PORT: str,
    **EVENT_KEYS
}

def create_socket_file_event(temp_path:str, rule:Any, time:float, 
        extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW event dictionary."""

    with open(temp_path, "rb") as file_pointer:
        file_hash = hashlib.sha256(file_pointer.read()).hexdigest()

    if temp_path.startswith(tempfile.gettempdir()):
        temp_path = temp_path[len(tempfile.gettempdir()):]

    return create_watchdog_event(
        temp_path, 
        rule, 
        tempfile.gettempdir(), 
        time, 
        file_hash, 
        extras=extras
    )

def create_socket_signal_event(port:int, rule:Any, time:float, 
        extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW event dictionary."""
    return create_event(
        EVENT_TYPE_SOCKET,
        port,
        rule,
        time,
        extras={
            TRIGGERING_PORT: port,
            **extras
        }
    )

def valid_socket_file_event(event:Dict[str,Any])->None:
    valid_meow_dict(event, "Socket file event", WATCHDOG_EVENT_KEYS)

def valid_socket_signal_event(event:Dict[str,Any])->None:
    valid_meow_dict(event, "Socket signal event", SOCKET_EVENT_KEYS)

class SocketPattern(BasePattern):
    # The port to monitor
    triggering_port:int

    def __init__(self, name:str, triggering_port:int, recipe:str, 
            parameters: Dict[str,Any]={}, outputs:Dict[str,Any]={}, 
            sweep:Dict[str,Any]={}):
        super().__init__(name, recipe, parameters, outputs, sweep)
        self._is_valid_port(triggering_port)
        self.triggering_port = triggering_port

    def _is_valid_port(self, port:int)->None:
        if not isinstance(port, int):
            raise ValueError (
                f"Port '{port}' is not of type int."
            )
        elif not (1023 < port < 49152):
            raise ValueError (
                f"Port '{port}' is not valid."
            )

    def _is_valid_recipe(self, recipe:str)->None:
        """Validation check for 'recipe' variable from main constructor.
        Called within parent BasePattern constructor."""
        valid_string(recipe, VALID_RECIPE_NAME_CHARS)

    def _is_valid_parameters(self, parameters:Dict[str,Any])->None:
        """Validation check for 'parameters' variable from main constructor.
        Called within parent BasePattern constructor."""
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_output(self, outputs:Dict[str,str])->None:
        """Validation check for 'output' variable from main constructor.
        Called within parent BasePattern constructor."""
        valid_dict(outputs, str, str, strict=False, min_length=0)
        for k in outputs.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

# TODO make sure these are actually updating ports 
class SocketMonitor(BaseMonitor):
    def __init__(self, patterns:Dict[str,SocketPattern],
            recipes:Dict[str,BaseRecipe], autostart=False,
            name:str="", print:Any=sys.stdout, logging:int=0) -> None:
        super().__init__(patterns, recipes, name=name)
        self._print_target, self.debug_level = setup_debugging(print, logging)
        self.ports = set()
        self.listeners = []
        self.temp_files = []
        if not hasattr(self, "listener_type"):
            self.listener_type = SocketListener
        if autostart:
            self.start()

    def start(self)->None:
        """Function to start the monitor as an ongoing process/thread. Must be
        implemented by any child process. Depending on the nature of the
        monitor, this may wish to directly call apply_retroactive_rules before
        starting."""

        self.ports = set(
            rule.pattern.triggering_port for rule in self._rules.values()
        )
        self.listeners = [
            self.listener_type("127.0.0.1", i, 2048, self) for i in self.ports
        ]

        for listener in self.listeners:
            listener.start()

    def match(self, event)->None:
        """Function to determine if a given event matches the current rules."""

        self._rules_lock.acquire()
        try:
            self.temp_files.append(event["tmp file"])
            for rule in self._rules.values():
                # Match event port against rule ports
                hit = event["triggering port"] == rule.pattern.triggering_port

                # If matched, the create a watchdog event
                if hit:
                    meow_event = create_socket_file_event(
                        event["tmp file"],
                        rule,
                        event["time stamp"],
                    )
                    print_debug(self._print_target, self.debug_level,
                        f"Event at {event['triggering port']} hit rule {rule.name}",
                        DEBUG_INFO)
                    # Send the event to the runner
                    self.send_event_to_runner(meow_event)

        except Exception as e:
            self._rules_lock.release()
            raise e

        self._rules_lock.release()

    def _delete_temp_files(self):
        for file in self.temp_files:
            if os.path.exists(file):
                os.unlink(file)
        self.temp_files = []

    def stop(self)->None:
        """Function to stop the monitor as an ongoing process/thread. Must be
        implemented by any child process"""
        for listener in self.listeners:
            listener.stop()

        self._delete_temp_files()

    def _create_new_rule(self, pattern:BasePattern, recipe:BaseRecipe)->None:
        rule = create_rule(pattern, recipe)
        self._rules_lock.acquire()
        try:
            if rule.name in self._rules:
                raise KeyError("Cannot create Rule with name of "
                    f"'{rule.name}' as already in use")
            self._rules[rule.name] = rule

        except Exception as e:
            self._rules_lock.release()
            raise e
        
        try:
            self.ports.add(rule.pattern.triggering_port)
        except Exception as e:
            self._rules.pop(rule.name)
            self._rules_lock.release()
            raise e

        try:
            listener = self.listener_type(
                "127.0.0.1", 
                rule.pattern.triggering_port, 
                2048, 
                self
            )
            self.listeners.append(listener)
        except Exception as e:
            self._rules.pop(rule.name)
            self.ports.remove(rule.pattern.triggering_port)
            self._rules_lock.release()
            raise e

        try:
            listener.start()
        except Exception as e:
            self._rules.pop(rule.name)
            self.ports.remove(rule.pattern.triggering_port)
            self.listeners.pop(listener)
            self._rules_lock.release()
            raise e

        self._rules_lock.release()

        self._apply_retroactive_rule(rule)

    def _is_valid_recipes(self, recipes:Dict[str,BaseRecipe])->None:
        """Validation check for 'recipes' variable from main constructor. Is
        automatically called during initialisation."""
        valid_dict(recipes, str, BaseRecipe, min_length=0, strict=False)

    def _get_valid_pattern_types(self)->List[type]:
        return [SocketPattern]

    def _get_valid_recipe_types(self)->List[type]:
        return [BaseRecipe]

class SocketListener():
    def __init__(self, host:int, port:int, buff_size:int,
                 monitor:SocketMonitor, timeout=0.5) -> None:
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(timeout)
        self._stopped = False
        self.buff_size = buff_size
        self.monitor = monitor

    def start(self):
        self._handle_thread = threading.Thread(
            target=self.main_loop
        )
        self._handle_thread.start()

    def main_loop(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(1)
        while not self._stopped:
            try:
                conn, _ = self.socket.accept()
            except socket.timeout:
                pass
            except OSError:
                if self._stopped:
                    return
                else:
                    raise
            except:
                raise
            else:
                threading.Thread(
                    target=self.handle_event,
                    args=(conn,time(),)
                ).start()

        self.socket.close()

    def receive_data(self,conn):
        with conn:
            with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
                while True:
                    data = conn.recv(self.buff_size)
                    if not data:
                        break
                    tmp.write(data)

                tmp_name = tmp.name

        return tmp_name

    def handle_event(self, conn, time_stamp):
        tmp_name = self.receive_data(conn)

        event = {
            "triggering port": self.port,
            "tmp file": tmp_name,
            "time stamp": time_stamp,
        }
        self.monitor.match(event)

    def stop(self):
        self._stopped = True
        self.socket.close()

