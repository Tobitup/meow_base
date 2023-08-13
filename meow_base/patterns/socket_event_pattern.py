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

from typing import Any, Dict, List, Tuple

from .file_event_pattern import WATCHDOG_EVENT_KEYS, \
    create_watchdog_event, KEYWORD_BASE, KEYWORD_REL_PATH, KEYWORD_REL_DIR, \
    KEYWORD_DIR, KEYWORD_FILENAME, KEYWORD_PREFIX, KEYWORD_EXTENSION, \
    WATCHDOG_BASE
from ..core.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, DEBUG_INFO
from ..core.base_recipe import BaseRecipe
from ..core.meow import EVENT_KEYS, EVENT_PATH, valid_meow_dict
from ..core.base_monitor import BaseMonitor
from ..core.base_pattern import BasePattern
from ..functionality.meow import create_event, create_rule
from ..functionality.debug import setup_debugging, print_debug
from ..functionality.validation import valid_string, valid_dict, \
    valid_dir_path

# socket events
EVENT_TYPE_SOCKET = "socket"
TRIGGERING_PORT = "triggering_port"

SOCKET_EVENT_KEYS = {
    TRIGGERING_PORT: str,
    **EVENT_KEYS
}

def create_socket_file_event(temp_path:str, rule:Any, base:str, time:float, 
        extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW event dictionary."""

    with open(temp_path, "rb") as file_pointer:
        file_hash = hashlib.sha256(file_pointer.read()).hexdigest()

    if base not in temp_path:
        raise ValueError("Cannot convert from socket event to watchdog event "
                         f"if temp file '{temp_path}' not placed in base "
                         f"directory '{base}'.")

    return create_watchdog_event(
        temp_path[temp_path.index(base):], 
        rule, 
        base,#tempfile.gettempdir(), 
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
    # The variable to refer to the message within the job
    triggering_message:str

    def __init__(self, name:str, triggering_port:int, recipe:str, 
            triggering_message:str, parameters: Dict[str,Any]={}, 
            outputs:Dict[str,Any]={}, sweep:Dict[str,Any]={}, 
            notifications:Dict[str,Any]={}, tracing:str=""):
        super().__init__(name, recipe, parameters=parameters, outputs=outputs, 
            sweep=sweep, notifications=notifications, tracing=tracing)
        self._is_valid_port(triggering_port)
        self.triggering_port = triggering_port
        self._is_valid_triggering_message(triggering_message)
        self.triggering_message = triggering_message

    def _is_valid_port(self, port:int)->None:
        if not isinstance(port, int):
            raise ValueError (
                f"Port '{port}' is not of type int."
            )
        elif not (1023 < port < 49152):
            raise ValueError (
                f"Port '{port}' is not valid."
            )

    def _is_valid_triggering_message(self, triggering_message:str)->None:
        """Validation check for 'triggering_message' variable from main 
        constructor."""
        valid_string(
            triggering_message, 
            VALID_VARIABLE_NAME_CHARS,
            hint="SocketPattern.triggering_message"
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

    #TODO test me
    def assemble_params_dict(self, event:Dict[str,Any])->Dict[str,Any]|List[Dict[str,Any]]:
        base_params = super().assemble_params_dict(event)
        if isinstance(base_params, list):
            for i in range(len(base_params)):
                base_params[i][self.triggering_message] = event[EVENT_PATH]
        else:
            base_params[self.triggering_message] = event[EVENT_PATH]
        return base_params

    #TODO test me
    def get_additional_replacement_keywords(self
            )->Tuple[Dict[str,str],List[str]]:
        return ({
            KEYWORD_BASE: 
                f"val.replace('{KEYWORD_BASE}', event['{WATCHDOG_BASE}'])",
            KEYWORD_REL_PATH: 
                f"val.replace('{KEYWORD_REL_PATH}', relpath(event[EVENT_PATH], event['{WATCHDOG_BASE}']))",
            KEYWORD_REL_DIR: 
                f"val.replace('{KEYWORD_REL_DIR}', dirname(relpath(event[EVENT_PATH], event['{WATCHDOG_BASE}'])))",
            KEYWORD_DIR: 
                f"val.replace('{KEYWORD_DIR}', dirname(event[EVENT_PATH]))",
            KEYWORD_FILENAME: 
                f"val.replace('{KEYWORD_FILENAME}', basename(event[EVENT_PATH]))",
            KEYWORD_PREFIX: 
                f"val.replace('{KEYWORD_PREFIX}', splitext(basename(event[EVENT_PATH]))[0])",
            KEYWORD_EXTENSION: 
                f"val.replace('{KEYWORD_EXTENSION}', splitext(basename(event[EVENT_PATH]))[1])"
        },(
            "from os.path import dirname",
            "from os.path import relpath"
        ))

class SocketMonitor(BaseMonitor):
    def __init__(self, base_dir:str, patterns:Dict[str,SocketPattern],
            recipes:Dict[str,BaseRecipe], autostart=False,
            name:str="", print:Any=sys.stdout, logging:int=0) -> None:
        super().__init__(patterns, recipes, name=name)
        self._is_valid_base_dir(base_dir)
        self.base_dir = base_dir
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
            print(f"matching against rules: {[i for i in self._rules]}")
            for rule in self._rules.values():
                # Match event port against rule ports
                hit = event["triggering port"] == rule.pattern.triggering_port

                # If matched, the create a watchdog event
                if hit:
                    meow_event = create_socket_file_event(
                        event["tmp file"],
                        rule,
                        self.base_dir,
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

    def _is_valid_base_dir(self, base_dir:str)->None:
        """Validation check for 'base_dir' variable from main constructor. Is 
        automatically called during initialisation."""
        valid_dir_path(base_dir, must_exist=True)

    #TODO do this at runtime, not just when monitor is done
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

    def _create_new_rule(self, pattern:SocketPattern, recipe:BaseRecipe)->None:
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
            no_start = False
            for l in self.listeners:
                if l.port == rule.pattern.triggering_port:
                    no_start = True
            if not no_start:
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

        if not no_start:
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

    def _identify_lost_rules(self, lost_pattern:str=None, 
            lost_recipe:str=None)->None:
        """Function to remove rules that should be deleted in response to a 
        pattern or recipe having been deleted."""
        to_delete = []
        self._rules_lock.acquire()

        try:
            # Identify any offending rules
            for name, rule in self._rules.items():
                if lost_pattern and rule.pattern.name == lost_pattern:
                    to_delete.append(name)
                if lost_recipe and rule.recipe.name == lost_recipe:
                    to_delete.append(name)

            # Now delete them
            for delete in to_delete:
                if delete in self._rules.keys():
                    self._rules.pop(delete)

            # Now stop their listener and close the port
            old_len = len(self.ports)
            self.ports = set(
                rule.pattern.triggering_port for rule in self._rules.values()
            )
            if old_len > len(self.ports):
                to_stop = [i for i in self.listeners if i.port not in self.ports]
                for ts in to_stop:
                    ts.stop()
                    self.listeners.remove(ts)

        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()

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
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=self.monitor.base_dir) as tmp:
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
