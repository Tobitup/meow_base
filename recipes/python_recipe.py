
"""
This file contains definitions for a MEOW recipe based off of python code,
along with an appropriate handler for said events.

Author(s): David Marchant
"""
import os
import stat
import sys

from typing import Any, Tuple, Dict, List

from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.base_handler import BaseHandler
from meow_base.core.meow import valid_event
from meow_base.functionality.validation import check_script, valid_string, \
    valid_dict
from meow_base.core.vars import VALID_VARIABLE_NAME_CHARS, \
    DEBUG_INFO, DEFAULT_JOB_QUEUE_DIR, EVENT_RULE, \
    JOB_TYPE_PYTHON, EVENT_TYPE, EVENT_RULE
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.file_io import write_file, \
    lines_to_string
from meow_base.functionality.parameterisation import parameterize_python_script
from meow_base.patterns.file_event_pattern import EVENT_TYPE_WATCHDOG

class PythonRecipe(BaseRecipe):
    def __init__(self, name:str, recipe:List[str], parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}):
        """PythonRecipe Constructor. This is used to execute python analysis 
        code."""
        super().__init__(name, recipe, parameters, requirements)

    def _is_valid_recipe(self, recipe:List[str])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_script(recipe)

    def _is_valid_parameters(self, parameters:Dict[str,Any])->None:
        """Validation check for 'parameters' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:Dict[str,Any])->None:
        """Validation check for 'requirements' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(requirements, str, Any, strict=False, min_length=0)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)


class PythonHandler(BaseHandler):
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, name:str="",
            print:Any=sys.stdout, logging:int=0, pause_time:int=5)->None:
        """PythonHandler Constructor. This creates jobs to be executed as 
        python functions. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir will be overwridden by its"""
        super().__init__(name=name, job_queue_dir=job_queue_dir,
            pause_time=pause_time)
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new PythonHandler instance", DEBUG_INFO)

    def valid_handle_criteria(self, event:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an event defintion, if this handler can 
        process it or not. This handler accepts events from watchdog with 
        Python recipes"""
        try:
            valid_event(event)
            msg = ""
            if type(event[EVENT_RULE].recipe) != PythonRecipe:
                msg = "Recipe is not a PythonRecipe. "
            if event[EVENT_TYPE] != EVENT_TYPE_WATCHDOG:
                msg += f"Event type is not {EVENT_TYPE_WATCHDOG}."
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)

    def get_created_job_type(self)->str:
        return JOB_TYPE_PYTHON

    def create_job_recipe_file(self, job_dir:str, event:Dict[str,Any], 
            params_dict: Dict[str,Any])->str:
        # parameterise recipe and write as executeable script
        base_script = parameterize_python_script(
            event[EVENT_RULE].recipe.recipe, params_dict
        )
        base_file = os.path.join(job_dir, "recipe.py")

        write_file(lines_to_string(base_script), base_file)
        os.chmod(base_file, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH )

        return f"python3 {base_file}"

