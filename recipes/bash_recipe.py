
import os
import stat
import sys

from typing import Any, Dict, List, Tuple

from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.meow import valid_event
from meow_base.functionality.validation import check_type, valid_dict, \
    valid_string, valid_dir_path
from meow_base.core.vars import DEBUG_INFO, DEFAULT_JOB_QUEUE_DIR, \
    VALID_VARIABLE_NAME_CHARS, EVENT_RULE, EVENT_TYPE, \
    JOB_TYPE_BASH
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.file_io import valid_path, make_dir, write_file, \
    lines_to_string
from meow_base.functionality.parameterisation import parameterize_bash_script
from meow_base.patterns.file_event_pattern import EVENT_TYPE_WATCHDOG

class BashRecipe(BaseRecipe):
    # A path to the bash script used to create this recipe
    def __init__(self, name:str, recipe:Any, parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}, source:str=""):
        """BashRecipe Constructor. This is used to execute bash scripts, 
        enabling anything not natively supported by MEOW."""
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        """Validation check for 'source' variable from main constructor."""
        if source:
            valid_path(source, extension=".sh", min_length=0)

    def _is_valid_recipe(self, recipe:List[str])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_type(recipe, List, hint="BashRecipe.recipe")
        for line in recipe:
            check_type(line, str, hint="BashRecipe.recipe[line]")

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


class BashHandler(BaseHandler):
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, name:str="",
            print:Any=sys.stdout, logging:int=0, pause_time:int=5)->None:
        """BashHandler Constructor. This creates jobs to be executed as 
        bash scripts. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir will be overwridden by its"""
        super().__init__(name=name, job_queue_dir=job_queue_dir, 
                pause_time=pause_time)
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new BashHandler instance", DEBUG_INFO)

    def valid_handle_criteria(self, event:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an event defintion, if this handler can 
        process it or not. This handler accepts events from watchdog with 
        Bash recipes"""
        try:
            valid_event(event)
            msg = ""
            if type(event[EVENT_RULE].recipe) != BashRecipe:
                msg = "Recipe is not a BashRecipe. "
            if event[EVENT_TYPE] != EVENT_TYPE_WATCHDOG:
                msg += f"Event type is not {EVENT_TYPE_WATCHDOG}."
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)

    def get_created_job_type(self)->str:
        return JOB_TYPE_BASH
    
    def create_job_recipe_file(self, job_dir:str, event:Dict[str,Any], 
            params_dict:Dict[str,Any])->str:
        # parameterise recipe and write as executeable script
        base_script = parameterize_bash_script(
            event[EVENT_RULE].recipe.recipe, params_dict
        )
        base_file = os.path.join(job_dir, "recipe.sh")
        write_file(lines_to_string(base_script), base_file)
        os.chmod(base_file, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH )

        return os.path.join("$(dirname $0)", "recipe.sh")
