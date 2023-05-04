
"""
This file contains definitions for the LocalPythonConductor, in order to 
execute Python jobs on the local resource.

Author(s): David Marchant
"""
from typing import Any, Tuple, Dict

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.meow import valid_job
from meow_base.core.vars import JOB_TYPE_PYTHON, JOB_TYPE, \
    JOB_TYPE_PAPERMILL, DEFAULT_JOB_QUEUE_DIR, DEFAULT_JOB_OUTPUT_DIR

class LocalPythonConductor(BaseConductor):
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, 
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR, name:str="", 
            pause_time:int=5)->None:
        """LocalPythonConductor Constructor. This should be used to execute 
        Python jobs, and will then pass any internal job runner files to the 
        output directory. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir and job_output_dir will be overwridden."""
        super().__init__(name=name, job_output_dir=job_output_dir,
            job_queue_dir=job_queue_dir, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._is_valid_job_output_dir(job_output_dir)
        self.job_output_dir = job_output_dir

    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an job defintion, if this conductor can 
        process it or not. This conductor will accept any Python job type"""
        try:
            valid_job(job)
            msg = ""
            if job[JOB_TYPE] not in [JOB_TYPE_PYTHON, JOB_TYPE_PAPERMILL]:
                msg = "Job type was not in python or papermill. "
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)
