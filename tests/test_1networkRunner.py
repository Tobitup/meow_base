
import importlib
import os
import socket
import unittest
import tempfile
import shutil
import aiosmtpd
import unittest.mock
import json
import time
from unittest.mock import MagicMock, patch, mock_open, call, Mock


from multiprocessing import Pipe
from random import shuffle
from shutil import copy
from time import sleep
from warnings import warn

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_monitor import BaseMonitor
from meow_base.conductors import LocalPythonConductor
from meow_base.core.vars import JOB_ERROR, META_FILE, JOB_CREATE_TIME, \
    NOTIFICATION_EMAIL, JOB_EVENT, EVENT_PATH, JOB_CREATED_FILES, JOB_ID
from meow_base.core.runner import MeowRunner
from meow_base.functionality.file_io import make_dir, read_file, \
    read_notebook, read_yaml, write_file, lines_to_string
from meow_base.functionality.meow import create_parameter_sweep
from meow_base.functionality.requirements import create_python_requirements
from meow_base.patterns.file_event_pattern import WatchdogMonitor, \
    FileEventPattern
from meow_base.patterns.socket_event_pattern import SocketMonitor, \
    SocketPattern
from meow_base.recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe
from meow_base.recipes.python_recipe import PythonHandler, PythonRecipe

from tests.shared import TEST_JOB_QUEUE, TEST_JOB_OUTPUT, \
    TEST_MONITOR_BASE, MAKER_RECIPE, APPENDING_NOTEBOOK, \
    COMPLETE_PYTHON_SCRIPT, TEST_DIR, FILTER_RECIPE, POROSITY_CHECK_NOTEBOOK, \
    SEGMENT_FOAM_NOTEBOOK, GENERATOR_NOTEBOOK, FOAM_PORE_ANALYSIS_NOTEBOOK, \
    IDMC_UTILS_PYTHON_SCRIPT, TEST_DATA, GENERATE_PYTHON_SCRIPT, \
    MULTI_PYTHON_SCRIPT, setup, teardown, backup_before_teardown, \
    count_non_locks, check_shutdown_port_in_timeout, check_port_in_use

FILE_BASE = os.path.join(tempfile.gettempdir(), "runner_base")

    
FILE_BASE = "runner_base"
INPUT_DIR = "input_dir"

hello_pattern = FileEventPattern(
    "hello_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)

new_pattern = FileEventPattern(
    "new_pattern", 
    os.path.join(INPUT_DIR, "*"), 
    "hello_recipe", 
    "infile", 
)


hello_recipe = PythonRecipe(
    "hello_recipe", 
    [
        "infile = 'placeholder'",
        "message = f'Hello as triggered by {infile}\\n'",
        "print(message)"
    ]
)

patterns = {
    hello_pattern.name: hello_pattern,
}
recipes = {
    hello_recipe.name: hello_recipe,
}


# This is for testing the networking functionality of the MeowRunner
class TestNetworkRunner(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()
        make_dir(FILE_BASE)  
        make_dir(os.path.join(FILE_BASE, INPUT_DIR))

    def tearDown(self)->None:
        super().tearDown()
        # Add proper network cleanup
        if hasattr(self, 'local_runner'):
            self.local_runner.stop()
            time.sleep(2)  # Allow sockets to fully close
        if os.path.exists(FILE_BASE):
            shutil.rmtree(FILE_BASE)
        teardown()
        
        

    def testMeowRunnerPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", os.path.join("start", "A.txt"), "recipe_one", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        local_runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            name="Test Local Runner", network=1, ssh_config_alias="Own-System", debug_port=10002
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        local_runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(local_runner.job_connections)):
            _, obj = local_runner.job_connections[i]

            if obj == local_runner.conductors[0]:
                local_runner.job_connections[i] = (test_to_runner_runner, local_runner.job_connections[i][1])
      
   
        local_runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        while loops < 5:
            # Initial prompt
            if conductor_to_test_test.poll(5):
                msg = conductor_to_test_test.recv()
                
            else:
                raise Exception("Timed out")        
            self.assertEqual(msg, 1)
            test_to_runner_test.send(msg)

            # Reply
            if test_to_runner_test.poll(5):
                msg = test_to_runner_test.recv()
            else:
                raise Exception("Timed out")        
            job_dir = msg
            conductor_to_test_test.send(msg)

            if isinstance(job_dir, str):
                # Prompt again once complete
                if conductor_to_test_test.poll(5):
                    msg = conductor_to_test_test.recv()
                else:
                    raise Exception("Timed out")        
                self.assertEqual(msg, 1)
                loops = 5

            loops += 1

        job_dir = job_dir.replace(TEST_JOB_QUEUE, TEST_JOB_OUTPUT)

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertTrue(os.path.exists(job_dir))

        # ----------------------------------------------------------------------------- #

        # Check if local runner is instantiated correctly
        self.assertEqual(local_runner.name, "Test Local Runner")
        self.assertEqual(local_runner.role, "local")
        self.assertEqual(local_runner.network, 1)
        self.assertEqual(local_runner.ssh_config_alias, "Own-System")
        
        
        # ------------------------ Local monitor tests get/add ------------------------ #
        
        # Check if local runner's count of attached monitors is as expected        
        montor_names = local_runner.get_attached_monitors(target="local")
        
        # Count the number of monitors
        self.assertEqual(len(montor_names), 1)
        
        # Add a second monitor
        second_monitor = WatchdogMonitor(
            FILE_BASE,
            patterns,
            recipes,
            name="monitor_2",
        )
        
        local_runner.add_monitor(monitor=second_monitor, target="local")
        
        # Get new count of attached monitors
        montor_names = local_runner.get_attached_monitors(target="local")
        
        self.assertEqual(len(montor_names), 2)
        self.assertEqual(local_runner.monitors[1].name, "monitor_2")
        

        # ------------------------- Local pattern tests get/add ------------------------ #
              
        attached_patterns = local_runner.get_attached_patterns(monitor="monitor_2", target="local")
        
        self.assertEqual(len(attached_patterns), 1)
        self.assertEqual(attached_patterns[0], "pattern_one")
        
        # Check if type of the pattern is as expected (FileEventPattern)
        target_monitor = local_runner.get_monitor_by_name("monitor_2")    
        self.assertEqual(type(target_monitor.get_patterns().get("pattern_one")), FileEventPattern)
        

        # Add a second pattern
        new_pattern = FileEventPattern(
            "new_pattern", 
            os.path.join(INPUT_DIR, "*"), 
            "hello_recipe", 
            "infile", 
        )       
        
        # Add the new pattern to the second monitor
        local_runner.add_pattern(target="local", monitor_name = "monitor_2", pattern = new_pattern)
        
        # Get new count of attached patterns
        attached_patterns = local_runner.get_attached_patterns(monitor="monitor_2", target="local")
        self.assertEqual(len(attached_patterns), 2)
        
        # Check if the new pattern is attached
        self.assertEqual(attached_patterns[1], "new_pattern")
        
        # Check if type of the new pattern is as expected (FileEventPattern)
        self.assertEqual(type(target_monitor.get_patterns().get("new_pattern")), FileEventPattern)
    
        
        # --------------------------- Local runner get test --------------------------- #
        # # ---------------------- get attached conductors test ----------------------# #
        
        conductors = local_runner.get_attached_conductors(target="local")
        self.assertEqual(len(conductors), 1)
        self.assertEqual(conductors[0], "LocalPythonConductor")
        self.assertEqual(type(local_runner.conductors[0]), LocalPythonConductor)
        
        
        # # ----------------------- get attached handlers test -----------------------# #
        
        handlers = local_runner.get_attached_handlers(target="local")
        self.assertEqual(len(handlers), 1)
        self.assertEqual(handlers[0], "PythonHandler")
        self.assertEqual(type(local_runner.handlers[0]), PythonHandler)
        
        
        # # ------------------------ get attached queue test -------------------------# #
        
        job_queue = local_runner.get_queue(target="local")
        self.assertEqual(job_queue, [])
    

        # ----------------------------- Remote alive test ----------------------------- #
        
        # Check if remote local_runner's IP address is as expected
        self.assertEqual(local_runner.remote_runner_ip, local_runner.local_ip_addr)
        
        # Check if remote local_runner is alive 
        self.assertTrue(local_runner.remote_alive)
        
        
        # ----------------------------------------------------------------------------- #

        local_runner.stop()

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        result_path = os.path.join(job_dir, "stdout.txt")
        self.assertTrue(os.path.exists(result_path))
        result = read_file(os.path.join(result_path))
        self.assertEqual(
            result, "12505000.0\ndone\n")

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        output = read_file(os.path.join(output_path))
        self.assertEqual(output, "12505000.0")

        # ----------------------------- Remote dead test ------------------------------ #

        # Check if remote is "dead"
        self.assertEqual(local_runner.remote_alive, False)
        
        # ----------------------------------------------------------------------------- #

    def testNetconfig(self):          
        netconf_path = os.path.expanduser("~/meow_base/meow_base/.netconfs/transfered_network_config.json")
        # Check if the file exists
        if not os.path.exists(netconf_path):
            raise FileNotFoundError(f"File {netconf_path} does not exist.")
        
        # Check if the file is empty
        if os.path.getsize(netconf_path) == 0:
            raise ValueError(f"File {netconf_path} is empty.")
        
        # Read the file        
        with open(netconf_path, "r") as f:
            data = f.read()
            # Check if the file is a valid JSON
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                raise ValueError(f"File {netconf_path} is not a valid JSON.")
            
        # Check if the IP address is correct
        self.assertEqual(data["ip"], "127.0.1.1") # should be: local_runner.local_ip_addr
        
        # Check if the name is correct
        self.assertEqual(data["name"], "Test Local Runner") # should be: local_runner.name           
                

if __name__ == '__main__':
    unittest.main()