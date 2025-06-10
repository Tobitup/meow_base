import os
import unittest
import tempfile
import shutil
import time
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
        if os.path.exists(FILE_BASE):
            shutil.rmtree(FILE_BASE)
        teardown()
        
        

    def testLocalMeowRunner(self)->None:
        """Test the MeowRunner with local runners functionality."""
        
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
            name="Test Local Runner1", role = "local", network = 1, ssh_config_alias="Own-System", runners_to_start=2, msg_port=10003
            )

        local_runner.start()

        time.sleep(5)  # Allow the runner to start and settle

    #     # ----------------------------------------------------------------------------- #

        # Check if local runner is instantiated correctly
        self.assertEqual(local_runner.name, "Test Local Runner1")
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
    
    
        
        # --------------------------- Local Runner Get Tests --------------------------- #
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
    

        # ------------------------- Remote dead/deleted test -------------------------- #
        print(f"remote runners in 1st test: {local_runner.remote_runners}")
                
        local_runner.stop()
        time.sleep(5)  # Allow the runner to stop and settle

        # ----------------------------------------------------------------------------- #

    def testRemoteMeowRunners(self)->None:
        """Test the MeowRunner with remote runners functionality."""
        
        # Create a MeowRunner instance with network enabled
        local_runner = MeowRunner(
            WatchdogMonitor(
                FILE_BASE,
                patterns,
                recipes,
                name="monitor_1",
            ),
            PythonHandler(
                pause_time=1
            ),
            LocalPythonConductor(
                pause_time=1
            ),
            logging = 10,
            name="Test Local Runner2", role = "local", network = 1, ssh_config_alias="Own-System", runners_to_start=2, msg_port=10002
        )

        local_runner.start()
        time.sleep(5) # Allow the runner to start and settle
        
        # -------------------------- Remote Initialized test -------------------------- #
        
        # Check if the remote runners are initialized correctly
        self.assertIsInstance(local_runner.remote_runners, dict)
        
        # Check if the remote runners dictionary is not empty i.e. there are remote runners connected
        self.assertTrue(local_runner.remote_runners != {})
        
        for rname in list(local_runner.remote_runners.keys()):
            print(f"Remote runner {rname}")
        # print(f"Remote runners1: {local_runner.remote_runners}")
        print(f"Remote runners2: {len(local_runner.remote_runners)}")
        
        # Check if number of remote runners is as expected
        self.assertEqual(len(local_runner.remote_runners), local_runner.runners_to_start) 

        remote_runner_names = list(local_runner.remote_runners.keys())
        for rname in list(local_runner.remote_runners.keys()):
            info = local_runner.remote_runners[rname]
            # test if name is unique
            self.assertNotEqual(rname, "Remote runner")
            # test if info contains the expected keys (means that handshake was successful)
            self.assertIn("ip", info)
            self.assertIn("last_hb", info)
            self.assertIn("ssh_hostname", info)
            self.assertIn("ssh_user", info)
            self.assertIn("ssh_port", info)
            self.assertIn("runner_file", info)
            self.assertIn("restart_attempted", info)
        
        # --------------------------- Remote Heartbeat test --------------------------- #
        
        # Check if the remote's last_hb gets updated after a heartbeat
        for rname in local_runner.remote_runners.keys():
            info = local_runner.remote_runners[rname]
            last_hb_before = info["last_hb"]
            print(f"Remote runner {rname} info_last_hb_before: {last_hb_before}")
            break
        

        
        # -------------------------- Remote runner get test --------------------------- #

        second_monitor = WatchdogMonitor(
            FILE_BASE,
            patterns,
            recipes,
            name="monitor_2",
        )
        
        monitors_before = local_runner.get_attached_monitors(target="remote")
        local_runner.add_monitor(monitor=second_monitor, target="remote")
        monitors_after = local_runner.get_attached_monitors(target="remote")

        self.assertNotEqual(monitors_after, monitors_before)

        for i in range(1, 100):
            monitors_before = local_runner.get_attached_monitors(target="remote")
            monitor = WatchdogMonitor(
            FILE_BASE,
            patterns,
            recipes,
            name="monitor_" + str(i),
            )
            local_runner.add_monitor(monitor=monitor, target="remote")
            self.assertGreater(len(local_runner.get_attached_monitors(target="remote")), len(monitors_before))



        # ----------------------------- Remote pattern get/add test ------------------------- #

        new_pattern = FileEventPattern(
            "new_pattern", 
            os.path.join(INPUT_DIR, "*"), 
            "hello_recipe", 
            "infile", 
            )
        
        patterns_before = local_runner.get_attached_patterns(monitor="monitor_2", target="remote")
        local_runner.add_pattern(target="remote", monitor_name = "monitor_2", pattern = new_pattern)
        patterns_after = local_runner.get_attached_patterns(monitor="monitor_2", target="remote")
        self.assertNotEqual(patterns_after, patterns_before)
        
        # Check if the new pattern is attached
        for i in range(1, 100):
            patterns_before = local_runner.get_attached_patterns(monitor="monitor_" + str(i), target="remote")
            pattern = FileEventPattern(
                "new_pattern_" + str(i), 
                os.path.join(INPUT_DIR, "*"), 
                "hello_recipe", 
                "infile", 
            )
            local_runner.add_pattern(target="remote", monitor_name = "monitor_" + str(i), pattern = pattern)
            print(local_runner.get_attached_patterns(monitor="monitor_" + str(i), target="remote"))
            self.assertGreater(len(local_runner.get_attached_patterns(monitor="monitor_" + str(i), target="remote")), len(patterns_before))

        time.sleep(10)

        # Stop the runners
        local_runner.stop()
        time.sleep(5)  # Allow the runner to stop and settle

        # Check if remote runners are "dead"
        self.assertTrue(local_runner.remote_runners == {})

        # Check if remote runner names are in remote_runners
        for rname in remote_runner_names:
            self.assertNotIn(rname, local_runner.remote_runners.keys())
            

if __name__ == '__main__':
    unittest.main()