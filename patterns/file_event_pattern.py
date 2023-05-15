
"""
This file contains definitions for a MEOW pattern based off of file events, 
along with an appropriate monitor for said events.

Author(s): David Marchant
"""
import glob
import threading
import sys
import os

from fnmatch import translate
from re import match
from time import time, sleep
from typing import Any, Union, Dict, List, Tuple
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.base_monitor import BaseMonitor
from meow_base.core.base_pattern import BasePattern
from meow_base.core.meow import EVENT_KEYS, valid_meow_dict
from meow_base.core.rule import Rule
from meow_base.functionality.validation import check_type, valid_string, \
    valid_dict, valid_list, valid_dir_path
from meow_base.core.vars import VALID_RECIPE_NAME_CHARS, \
    VALID_VARIABLE_NAME_CHARS, FILE_EVENTS, FILE_CREATE_EVENT, \
    FILE_MODIFY_EVENT, FILE_MOVED_EVENT, DEBUG_INFO, DIR_EVENTS, \
    FILE_RETROACTIVE_EVENT, SHA256, VALID_PATH_CHARS, FILE_CLOSED_EVENT, \
    DIR_RETROACTIVE_EVENT, EVENT_PATH
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.hashing import get_hash
from meow_base.functionality.meow import create_event

# Events that are monitored by default
_DEFAULT_MASK = [
    FILE_CREATE_EVENT,
    FILE_MODIFY_EVENT,
    FILE_MOVED_EVENT,
    FILE_RETROACTIVE_EVENT,
    FILE_CLOSED_EVENT
]

# file event trigger keyword replacements
KEYWORD_BASE = "{BASE}"
KEYWORD_REL_PATH = "{REL_PATH}"
KEYWORD_REL_DIR = "{REL_DIR}"

# watchdog events
EVENT_TYPE_WATCHDOG = "watchdog"
WATCHDOG_BASE = "monitor_base"
WATCHDOG_HASH = "file_hash"

WATCHDOG_EVENT_KEYS = {
    WATCHDOG_BASE: str,
    WATCHDOG_HASH: str,    
    **EVENT_KEYS
}

def create_watchdog_event(path:str, rule:Any, base:str, time:float, 
            hash:str, extras:Dict[Any,Any]={})->Dict[Any,Any]:
    """Function to create a MEOW event dictionary."""
    return create_event(
        EVENT_TYPE_WATCHDOG, 
        path, 
        rule,
        time,
        extras={
            **extras,
            **{
                WATCHDOG_HASH: hash,
                WATCHDOG_BASE: base
            }
        }
    )

def valid_watchdog_event(event:Dict[str,Any])->None:
    valid_meow_dict(event, "Watchdog event", WATCHDOG_EVENT_KEYS)


class FileEventPattern(BasePattern):
    # The path at which events will trigger this pattern
    triggering_path:str
    # The variable name given to the triggering file within recipe code
    triggering_file:str
    # Which types of event the pattern responds to
    event_mask:List[str]
    def __init__(self, name:str, triggering_path:str, recipe:str, 
            triggering_file:str, event_mask:List[str]=_DEFAULT_MASK, 
            parameters:Dict[str,Any]={}, outputs:Dict[str,Any]={}, 
            sweep:Dict[str,Any]={}, notifications:Dict[str,Any]={}):
        """FileEventPattern Constructor. This is used to match against file 
        system events, as caught by the python watchdog module."""
        super().__init__(name, recipe, parameters=parameters, outputs=outputs, 
            sweep=sweep, notifications=notifications)
        self._is_valid_triggering_path(triggering_path)
        self.triggering_path = triggering_path
        self._is_valid_triggering_file(triggering_file)
        self.triggering_file = triggering_file
        self._is_valid_event_mask(event_mask)
        self.event_mask = event_mask

    def _is_valid_triggering_path(self, triggering_path:str)->None:
        """Validation check for 'triggering_path' variable from main 
        constructor."""
        valid_string(
            triggering_path, 
            VALID_PATH_CHARS+'*', 
            min_length=1, 
            hint="FileEventPattern.triggering_path"
        )
        if len(triggering_path) < 1:
            raise ValueError (
                f"triggiering path '{triggering_path}' is too short. " 
                "Minimum length is 1"
        )

    def _is_valid_triggering_file(self, triggering_file:str)->None:
        """Validation check for 'triggering_file' variable from main 
        constructor."""
        valid_string(
            triggering_file, 
            VALID_VARIABLE_NAME_CHARS,
            hint="FileEventPattern.triggering_file"
        )

    def _is_valid_recipe(self, recipe:str)->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BasePattern constructor."""
        valid_string(
            recipe, 
            VALID_RECIPE_NAME_CHARS,
            hint="FileEventPattern.recipe"
        )

    def _is_valid_parameters(self, parameters:Dict[str,Any])->None:
        """Validation check for 'parameters' variable from main constructor. 
        Called within parent BasePattern constructor."""
        valid_dict(
            parameters, 
            str, 
            Any, 
            strict=False, 
            min_length=0, 
            hint="FileEventPattern.parameters"
        )
        for k in parameters.keys():
            valid_string(
                k, 
                VALID_VARIABLE_NAME_CHARS,
                hint=f"FileEventPattern.parameters[{k}]"
            )

    def _is_valid_output(self, outputs:Dict[str,str])->None:
        """Validation check for 'output' variable from main constructor. 
        Called within parent BasePattern constructor."""
        valid_dict(
            outputs, 
            str, 
            str, 
            strict=False, 
            min_length=0,
            hint="FileEventPattern.outputs"
        )
        for k in outputs.keys():
            valid_string(
                k, 
                VALID_VARIABLE_NAME_CHARS,
                hint=f"FileEventPattern.outputs[{k}]"
            )

    def _is_valid_event_mask(self, event_mask)->None:
        """Validation check for 'event_mask' variable from main constructor."""
        valid_list(
            event_mask, 
            str, 
            min_length=1, 
            hint="FileEventPattern.event_mask"
        )
        for mask in event_mask:
            if mask not in FILE_EVENTS + DIR_EVENTS:
                raise ValueError(f"Invalid event mask '{mask}'. Valid are: "
                    f"{FILE_EVENTS + DIR_EVENTS}")

    def _is_valid_sweep(self, sweep: Dict[str,Union[int,float,complex]]) -> None:
        """Validation check for 'sweep' variable from main constructor."""
        return super()._is_valid_sweep(sweep)

    #TODO test me
    def assemble_params_dict(self, event:Dict[str,Any])->Dict[str,Any]|List[Dict[str,Any]]:
        base_params = super().assemble_params_dict(event)
        if isinstance(base_params, list):
            for i in range(len(base_params)):
                base_params[i][self.triggering_file] = event[EVENT_PATH]
        else:
            base_params[self.triggering_file] = event[EVENT_PATH]
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
                f"val.replace('{KEYWORD_REL_DIR}', dirname(relpath(event[EVENT_PATH], event['{WATCHDOG_BASE}'])))"
        },(
            "from os.path import dirname",
            "from os.path import relpath"
        ))
    

class WatchdogMonitor(BaseMonitor):
    # A handler object, to catch events
    event_handler:PatternMatchingEventHandler
    # The watchdog observer object
    monitor:Observer
    # The base monitored directory
    base_dir:str
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, base_dir:str, patterns:Dict[str,FileEventPattern], 
            recipes:Dict[str,BaseRecipe], autostart=False, settletime:int=1, 
            name:str="", print:Any=sys.stdout, logging:int=0)->None:
        """WatchdogEventHandler Constructor. This uses the watchdog module to 
        monitor a directory and all its sub-directories. Watchdog will provide 
        the monitor with an caught events, with the monitor comparing them 
        against its rules, and informing the runner of match."""
        super().__init__(patterns, recipes, name=name)
        self._is_valid_base_dir(base_dir)
        self.base_dir = base_dir
        check_type(settletime, int, hint="WatchdogMonitor.settletime")
        self._print_target, self.debug_level = setup_debugging(print, logging)       
        self.event_handler = WatchdogEventHandler(self, settletime=settletime)
        self.monitor = Observer()
        self.monitor.schedule(
            self.event_handler,
            self.base_dir,
            recursive=True
        )
        print_debug(self._print_target, self.debug_level, 
            "Created new WatchdogMonitor instance", DEBUG_INFO)

        if autostart:
            self.start()

    def start(self)->None:
        """Function to start the monitor."""
        print_debug(self._print_target, self.debug_level, 
            "Starting WatchdogMonitor", DEBUG_INFO)
        self._apply_retroactive_rules()
        self.monitor.start()

    def stop(self)->None:
        """Function to stop the monitor."""
        print_debug(self._print_target, self.debug_level, 
            "Stopping WatchdogMonitor", DEBUG_INFO)
        self.monitor.stop()

    def match(self, event)->None:
        """Function to determine if a given event matches the current rules."""
        src_path = event.src_path

        prepend = "dir_" if event.is_directory else "file_" 
        event_types = [prepend+i for i in event.event_type]

        # Remove the base dir from the path as trigger paths are given relative
        # to that
        handle_path = src_path.replace(self.base_dir, '', 1)
        # Also remove leading slashes, so we don't go off of the root directory
        while handle_path.startswith(os.path.sep):
            handle_path = handle_path[1:]

        self._rules_lock.acquire()
        try:
            for rule in self._rules.values():
                # Skip events not within the event mask
                if any(i in event_types for i in rule.pattern.event_mask) \
                        != True:
                    continue
                                
                # Use regex to match event paths against rule paths
                target_path = rule.pattern.triggering_path
                recursive_regexp = translate(target_path)
                if os.name == 'nt':
                    direct_regexp = recursive_regexp.replace(
                        '.*', '[^'+ os.path.sep + os.path.sep +']*')
                else:
                    direct_regexp = recursive_regexp.replace(
                        '.*', '[^'+ os.path.sep +']*')
                recursive_hit = match(recursive_regexp, handle_path)
                direct_hit = match(direct_regexp, handle_path)

                # If matched, the create a watchdog event
                if direct_hit or recursive_hit:
                    meow_event = create_watchdog_event(
                        event.src_path,
                        rule,
                        self.base_dir,
                        event.time_stamp,
                        get_hash(event.src_path, SHA256) 
                    )
                    print_debug(self._print_target, self.debug_level,  
                        f"Event at {src_path} hit rule {rule.name}", 
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

    def _is_valid_patterns(self, patterns:Dict[str,FileEventPattern])->None:
        """Validation check for 'patterns' variable from main constructor. Is 
        automatically called during initialisation."""
        valid_dict(patterns, str, FileEventPattern, min_length=0, strict=False)

    def _is_valid_recipes(self, recipes:Dict[str,BaseRecipe])->None:
        """Validation check for 'recipes' variable from main constructor. Is 
        automatically called during initialisation."""
        valid_dict(recipes, str, BaseRecipe, min_length=0, strict=False)

    def _get_valid_pattern_types(self)->List[type]:
        return [FileEventPattern]

    def _get_valid_recipe_types(self)->List[type]:
        return [BaseRecipe]

    def _apply_retroactive_rule(self, rule:Rule)->None:
        """Function to determine if a rule should be applied to the existing 
        file structure, were the file structure created/modified now."""
        self._rules_lock.acquire()
        try:
            # Check incase rule deleted since this function first called
            if rule.name not in self._rules:
                self._rules_lock.release()
                return

            if FILE_RETROACTIVE_EVENT in rule.pattern.event_mask \
                    or DIR_RETROACTIVE_EVENT in rule.pattern.event_mask:
                # Determine what paths are potentially triggerable and gather
                # files at those paths
                testing_path = os.path.join(
                    self.base_dir, rule.pattern.triggering_path)

                globbed = glob.glob(testing_path)

                # For each file create a fake event.
                for globble in globbed:

                    meow_event = create_watchdog_event(
                        globble,
                        rule,
                        self.base_dir,
                        time(),
                        get_hash(globble, SHA256)
                    )
                    print_debug(self._print_target, self.debug_level,  
                        f"Retroactive event for file at at {globble} hit rule "
                        f"{rule.name}", DEBUG_INFO)
                    # Send it to the runner
                    self.send_event_to_runner(meow_event)

        except Exception as e:
            self._rules_lock.release()
            raise e
        self._rules_lock.release()

    def _apply_retroactive_rules(self)->None:
        """Function to determine if any rules should be applied to the existing 
        file structure, were the file structure created/modified now."""
        for rule in self._rules.values():
            self._apply_retroactive_rule(rule)


class WatchdogEventHandler(PatternMatchingEventHandler):
    # The monitor class running this handler
    monitor:WatchdogMonitor
    # A time to wait per event path, during which extra events are discared
    _settletime:int
    # TODO clean this struct occasionally
    # A Dict of recent job timestamps
    _recent_jobs:Dict[str, Any]
    # A lock to solve race conditions on '_recent_jobs'
    _recent_jobs_lock:threading.Lock
    def __init__(self, monitor:WatchdogMonitor, settletime:int=1):
        """WatchdogEventHandler Constructor. This inherits from watchdog 
        PatternMatchingEventHandler, and is used to catch events, then filter 
        out excessive events at the same location."""
        super().__init__()
        self.monitor = monitor
        self._settletime = settletime
        self._recent_jobs = {}
        self._recent_jobs_lock = threading.Lock()

    def threaded_handler(self, event):
        """Function to determine if the given event shall be sent on to the 
        monitor. After each event we wait for '_settletime', to catch 
        subsequent events at the same location, so as to not swamp the system 
        with repeated events."""
 
        self._recent_jobs_lock.acquire()
        try:
            if event.src_path in self._recent_jobs: 
                if event.time_stamp > self._recent_jobs[event.src_path][0]+self._settletime:
                    self._recent_jobs[event.src_path] = \
                        [event.time_stamp, {event.event_type}]

                elif event.time_stamp > self._recent_jobs[event.src_path][0]:
                    self._recent_jobs[event.src_path][0] = event.time_stamp
                    self._recent_jobs[event.src_path][1].add(event.event_type)

                else:
                    self._recent_jobs_lock.release()
                    return
            else:
                self._recent_jobs[event.src_path] = \
                    [event.time_stamp, {event.event_type}]

            # If we have a closed event then short-cut the wait and send event
            # immediately        
            if event.event_type == FILE_CLOSED_EVENT:
                self.monitor.match(event)
                self._recent_jobs_lock.release()
                return

        except Exception as ex:
            self._recent_jobs_lock.release()
            raise Exception(ex)
        self._recent_jobs_lock.release()

        sleep(self._settletime)

        self._recent_jobs_lock.acquire()
        try:
            if event.src_path in self._recent_jobs \
                    and event.time_stamp < self._recent_jobs[event.src_path][0]:
                self._recent_jobs_lock.release()
                return
        except Exception as ex:
            self._recent_jobs_lock.release()
            raise Exception(ex)
        event.event_type = self._recent_jobs[event.src_path][1]
        self._recent_jobs_lock.release()

        self.monitor.match(event)

    def handle_event(self, event):
        """Handler function, called by all specific event functions. Will 
        attach a timestamp to the event immediately, and attempt to start a 
        threaded_handler so that the monitor can resume monitoring as soon as 
        possible."""
        event.time_stamp = time()

        waiting_for_threaded_resources = True
        while waiting_for_threaded_resources:
            try:
                worker = threading.Thread(
                    target=self.threaded_handler,
                    args=[event])
                worker.daemon = True
                worker.start()
                waiting_for_threaded_resources = False
            except threading.ThreadError:
                sleep(1)
    
    def on_created(self, event):
        """Function called when a file created event occurs."""
        self.handle_event(event)

    def on_modified(self, event):
        """Function called when a file modified event occurs."""
        self.handle_event(event)

    def on_moved(self, event):
        """Function called when a file moved event occurs."""
        self.handle_event(event)

    def on_deleted(self, event):
        """Function called when a file deleted event occurs."""
        self.handle_event(event)
    
    def on_closed(self, event):
        """Function called when a file closed event occurs."""
        self.handle_event(event)
