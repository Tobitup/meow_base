
"""
This file contains definitions for a MEOW recipe based off of jupyter 
notebooks, along with an appropriate handler for said events.

Author(s): David Marchant
"""
import os
import nbformat
import sys
import stat

from typing import Any, Tuple, Dict

from ..core.base_recipe import BaseRecipe
from ..core.base_handler import BaseHandler
from ..core.meow import valid_event
from ..core.vars import VALID_VARIABLE_NAME_CHARS, \
    DEBUG_INFO, DEFAULT_JOB_QUEUE_DIR, \
    JOB_TYPE_PAPERMILL, EVENT_RULE, EVENT_TYPE, EVENT_RULE
from ..functionality.debug import setup_debugging, print_debug
from ..functionality.file_io import read_notebook, write_notebook
from ..functionality.parameterisation import \
    parameterize_jupyter_notebook
from ..functionality.validation import check_type, valid_string, \
    valid_dict, valid_path, valid_existing_file_path
from ..patterns.file_event_pattern import EVENT_TYPE_WATCHDOG

class JupyterNotebookRecipe(BaseRecipe):
    # A path to the jupyter notebook used to create this recipe
    source:str
    def __init__(self, name:str, recipe:Any, parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}, source:str=""):
        """JupyterNotebookRecipe Constructor. This is used to execute analysis 
        code using the papermill module."""
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        """Validation check for 'source' variable from main constructor."""
        if source:
            valid_path(source, extension=".ipynb", min_length=0)

    def _is_valid_recipe(self, recipe:Dict[str,Any])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_type(recipe, Dict, hint="JupyterNotebookRecipe.recipe")
        nbformat.validate(recipe)

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

class PapermillHandler(BaseHandler):
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, name:str="",
            print:Any=sys.stdout, logging:int=0, pause_time:int=5)->None:
        """PapermillHandler Constructor. This creats jobs to be executed using 
        the papermill module. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir will be overwridden."""
        super().__init__(name=name, job_queue_dir=job_queue_dir,
            pause_time=pause_time)
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new PapermillHandler instance", DEBUG_INFO)

    def valid_handle_criteria(self, event:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an event defintion, if this handler can 
        process it or not. This handler accepts events from watchdog with 
        jupyter notebook recipes."""
        try:
            valid_event(event)
            msg = ""
            if type(event[EVENT_RULE].recipe) != JupyterNotebookRecipe: 
                msg = "Recipe is not a JupyterNotebookRecipe. "
            if event[EVENT_TYPE] != EVENT_TYPE_WATCHDOG:
                msg += f"Event type is not {EVENT_TYPE_WATCHDOG}."
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)

    def get_created_job_type(self)->str:
        return JOB_TYPE_PAPERMILL
    
    def create_job_recipe_file(self, job_dir:str, event:Dict[str,Any], 
            params_dict:Dict[str,Any])->str: 
        # parameterise recipe and write as executeable script
        base_script = parameterize_jupyter_notebook(
            event[EVENT_RULE].recipe.recipe, params_dict
        )
        base_file = os.path.join(job_dir, "recipe.ipynb")

        write_notebook(base_script, base_file)

        os.chmod(base_file, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH )

        return f"papermill {base_file} {os.path.join(job_dir, 'result.ipynb')}"

def get_recipe_from_notebook(name:str, notebook_filename:str, 
        parameters:Dict[str,Any]={}, requirements:Dict[str,Any]={}
        )->JupyterNotebookRecipe:
    valid_existing_file_path(notebook_filename, extension=".ipynb")
    check_type(name, str, hint="get_recipe_from_notebook.name")

    notebook_code = read_notebook(notebook_filename)

    return JupyterNotebookRecipe(
        name, 
        notebook_code, 
        parameters=parameters, 
        requirements=requirements, 
        source=notebook_filename
    )
