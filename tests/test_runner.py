
import importlib
import os
import socket
import unittest
import tempfile

from aiosmtpd.controller import Controller
from multiprocessing import Pipe
from random import shuffle
from shutil import copy
from time import sleep
from warnings import warn

from ..meow_base.core.base_conductor import BaseConductor
from ..meow_base.core.base_handler import BaseHandler
from ..meow_base.core.base_monitor import BaseMonitor
from ..meow_base.conductors import LocalPythonConductor
from ..meow_base.core.vars import JOB_ERROR, META_FILE, JOB_CREATE_TIME, \
    NOTIFICATION_EMAIL, JOB_EVENT, EVENT_PATH
from ..meow_base.core.runner import MeowRunner
from ..meow_base.functionality.file_io import make_dir, read_file, \
    read_notebook, read_yaml, write_file, lines_to_string
from ..meow_base.functionality.meow import create_parameter_sweep
from ..meow_base.functionality.requirements import create_python_requirements
from ..meow_base.patterns.file_event_pattern import WatchdogMonitor, \
    FileEventPattern
from ..meow_base.patterns.socket_event_pattern import SocketMonitor, \
    SocketPattern
from ..meow_base.recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe
from ..meow_base.recipes.python_recipe import PythonHandler, PythonRecipe
from .shared import EmailHandler, TEST_JOB_QUEUE, TEST_JOB_OUTPUT, \
    TEST_MONITOR_BASE, MAKER_RECIPE, APPENDING_NOTEBOOK, \
    COMPLETE_PYTHON_SCRIPT, TEST_DIR, FILTER_RECIPE, POROSITY_CHECK_NOTEBOOK, \
    SEGMENT_FOAM_NOTEBOOK, GENERATOR_NOTEBOOK, FOAM_PORE_ANALYSIS_NOTEBOOK, \
    IDMC_UTILS_PYTHON_SCRIPT, TEST_DATA, GENERATE_PYTHON_SCRIPT, \
    setup, teardown, backup_before_teardown, count_non_locks, \
    check_shutdown_port_in_timeout, check_port_in_use

 
class FileEventTests(unittest.TestCase):
    pattern_check = FileEventPattern(
        "pattern_check", 
        os.path.join("foam_ct_data", "*"), 
        "recipe_check", 
        "input_filename",
        parameters={
            "output_filedir_accepted": 
                os.path.join("{BASE}", "foam_ct_data_accepted"),
            "output_filedir_discarded": 
                os.path.join("{BASE}", "foam_ct_data_discarded"),
            "porosity_lower_threshold": 0.8,
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_segment = FileEventPattern(
        "pattern_segment",
        os.path.join("foam_ct_data_accepted", "*"),
        "recipe_segment",
        "input_filename",
        parameters={
            "output_filedir": os.path.join("{BASE}", "foam_ct_data_segmented"),
            "input_filedir": os.path.join("{BASE}", "foam_ct_data"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_analysis = FileEventPattern(
        "pattern_analysis",
        os.path.join("foam_ct_data_segmented", "*"),
        "recipe_analysis",
        "input_filename",
        parameters={
            "output_filedir": os.path.join("{BASE}", "foam_ct_data_pore_analysis"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_regenerate = FileEventPattern(
        "pattern_regenerate",
        os.path.join("foam_ct_data_discarded", "*"),
        "recipe_generator",
        "discarded",
        parameters={
            "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
            "gen_path": os.path.join("{BASE}", "generator.py"),
            "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
            "vx": 64,
            "vy": 64,
            "vz": 64,
            "res": 3/64,
            "chance_good": 1,
            "chance_small": 0,
            "chance_big": 0
        })

    recipe_check_key, recipe_check_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib"])
    recipe_check = JupyterNotebookRecipe(
        'recipe_check',
        POROSITY_CHECK_NOTEBOOK, 
        requirements={recipe_check_key: recipe_check_req}
    )

    recipe_segment_key, recipe_segment_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
    recipe_segment = JupyterNotebookRecipe(
        'recipe_segment',
        SEGMENT_FOAM_NOTEBOOK, 
        requirements={recipe_segment_key: recipe_segment_req}
    )

    recipe_analysis_key, recipe_analysis_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
    recipe_analysis = JupyterNotebookRecipe(
        'recipe_analysis',
        FOAM_PORE_ANALYSIS_NOTEBOOK, 
        requirements={recipe_analysis_key: recipe_analysis_req}
    )

    recipe_generator_key, recipe_generator_req = create_python_requirements(
        modules=["numpy", "matplotlib", "random"])
    recipe_generator = JupyterNotebookRecipe(
        'recipe_generator',
        GENERATOR_NOTEBOOK, 
        requirements={recipe_generator_key: recipe_generator_req}           
    )

    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test MeowRunner creation
    def testMeowRunnerSetup(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(pause_time=0)
        handler_two = PapermillHandler(pause_time=0)
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(pause_time=0)
        conductor_two = LocalPythonConductor(pause_time=0)
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitor_one, handler_one, conductor_one)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), 1)
        self.assertEqual(runner.monitors[0], monitor_one)

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)
        self.assertEqual(len(runner.handlers), 1)
        self.assertEqual(runner.handlers[0], handler_one)

        self.assertIsInstance(runner.conductors, list)        
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)
        self.assertEqual(len(runner.conductors), 1)
        self.assertEqual(runner.conductors[0], conductor_one)

        self.assertIsInstance(runner.event_connections, list)
        self.assertEqual(len(runner.event_connections), 2)

        runner.monitors[0].send_event_to_runner("monitor test message")
        message = None
        from_monitor = [i[0] for i in runner.event_connections \
                        if isinstance(i[1], BaseMonitor)][0]
        if from_monitor.poll(3):
            message = from_monitor.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "monitor test message")

        runner.handlers[0].prompt_runner_for_event()
        message = None
        from_handler = [i[0] for i in runner.event_connections \
                        if isinstance(i[1], BaseHandler)][0]
        if from_handler.poll(3):
            message = from_handler.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, 1)

        self.assertIsInstance(runner.job_connections, list)
        self.assertEqual(len(runner.job_connections), 2)

        runner.handlers[0].send_job_to_runner("handler test message")
        message = None
        from_handler = [i[0] for i in runner.job_connections \
                        if isinstance(i[1], BaseHandler)][0]
        if from_handler.poll(3):
            message = from_handler.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "handler test message")

        runner.conductors[0].prompt_runner_for_job()
        message = None
        from_conductor = [i[0] for i in runner.job_connections \
                        if isinstance(i[1], BaseConductor)][0]
        if from_conductor.poll(3):
            message = from_conductor.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, 1)

        runner = MeowRunner(monitors, handlers, conductors)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), len(monitors))
        for monitor in monitors:
            self.assertIn(monitor, monitors)

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)
        self.assertEqual(len(runner.handlers), 2)
        for handler in handlers:
            self.assertIn(handler, handlers)

        self.assertIsInstance(runner.conductors, list)        
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)
        self.assertEqual(len(runner.conductors), len(conductors))
        for conductor in conductors:
            self.assertIn(conductor, conductors)

        self.assertIsInstance(runner.event_connections, list)
        self.assertEqual(len(runner.event_connections), 4)

        for monitor in monitors:
            monitor.send_event_to_runner("monitor test message")
    
            message = None
            from_monitor = [i[0] for i in runner.event_connections \
                            if i[1] is monitor][0]
            if from_monitor.poll(3):
                message = from_monitor.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, "monitor test message")

        for handler in handlers:
            handler.prompt_runner_for_event()
            message = None
            from_handler = [i[0] for i in runner.event_connections \
                            if i[1] is handler][0]
            if from_handler.poll(3):
                message = from_handler.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, 1)

        self.assertIsInstance(runner.job_connections, list)
        self.assertEqual(len(runner.job_connections), 4)

        for handler in handlers:
            handler.send_job_to_runner("handler test message")
            message = None
            from_handler = [i[0] for i in runner.job_connections \
                            if i[1] is handler][0]
            if from_handler.poll(3):
                message = from_handler.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, "handler test message")

        for conductor in conductors:
            conductor.prompt_runner_for_job()
            message = None
            from_conductor = [i[0] for i in runner.job_connections \
                            if i[1] is conductor][0]
            if from_conductor.poll(3):
                message = from_conductor.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, 1)
    
    # Test meow runner directory overrides
    def testMeowRunnerDirOverridesSetup(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})

        original_queue_dir = os.path.join(TEST_DIR, "original_queue")
        original_output_dir = os.path.join(TEST_DIR, "original_output")
        overridden_queue_dir = os.path.join(TEST_DIR, "overridden_queue")
        overridden_output_dir = os.path.join(TEST_DIR, "overridden_output")

        handler_one = PapermillHandler(job_queue_dir=original_queue_dir)
 
        conductor_one = LocalPythonConductor(
            job_queue_dir=original_queue_dir, 
            job_output_dir=original_output_dir
        )

        self.assertTrue(os.path.exists(original_queue_dir))
        self.assertTrue(os.path.exists(original_output_dir))
        self.assertFalse(os.path.exists(overridden_queue_dir))
        self.assertFalse(os.path.exists(overridden_output_dir))

        self.assertEqual(handler_one.job_queue_dir, original_queue_dir)
        self.assertEqual(conductor_one.job_queue_dir, original_queue_dir)
        self.assertEqual(conductor_one.job_output_dir, original_output_dir)

        MeowRunner(
            monitor_one, 
            handler_one, 
            conductor_one, 
            job_queue_dir=overridden_queue_dir, 
            job_output_dir=overridden_output_dir
        )

        self.assertTrue(os.path.exists(original_queue_dir))
        self.assertTrue(os.path.exists(original_output_dir))
        self.assertTrue(os.path.exists(overridden_queue_dir))
        self.assertTrue(os.path.exists(overridden_output_dir))

        self.assertEqual(handler_one.job_queue_dir, overridden_queue_dir)
        self.assertEqual(conductor_one.job_queue_dir, overridden_queue_dir)
        self.assertEqual(conductor_one.job_output_dir, overridden_output_dir)

    # Test single meow papermill job execution
    def testMeowRunnerPapermillExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            },
            notifications={
                NOTIFICATION_EMAIL: "user@localhost"
            }
        )
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(),
            LocalPythonConductor(pause_time=2, 
                notification_email="alert@localhost",
                notification_email_smtp="localhost:1025"
            ),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        email_controller = Controller(
            EmailHandler(), hostname="localhost", port=1025
        )
        email_controller.start()

        self.assertEqual(len(email_controller.handler.messages), 0)

        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        loops = 0
        while loops < 10:
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
                loops = 10

            loops += 1

        job_dir = job_dir.replace(TEST_JOB_QUEUE, TEST_JOB_OUTPUT)
        job_id = job_dir.replace(f"{TEST_JOB_OUTPUT}{os.path.sep}", "")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertTrue(os.path.exists(job_dir))

        runner.stop()

        email_controller.stop()
        self.assertEqual(len(email_controller.handler.messages), 1)
        self.assertEqual(
            email_controller.handler.messages[0].mail_from, 
            "alert@localhost"
        )
        self.assertEqual(
            email_controller.handler.messages[0].rcpt_tos, 
            ["user@localhost"]
        )
        self.assertEqual(
            email_controller.handler.messages[0].content.decode(), 
            f'Job {job_id} completed with status done.\r\n'
        )

        print(os.listdir(job_dir))
        self.assertEqual(count_non_locks(job_dir), 6)

        result = read_notebook(
            os.path.join(job_dir, "result.ipynb"))
        self.assertIsNotNone(result)

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        
        with open(output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from a test Pattern")

    # Test meow papermill job chaining within runner
    def testMeowRunnerLinkedPapermillExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from Pattern 1",
                "outfile":os.path.join("{BASE}", "middle", "{FILENAME}")
            },
            notifications={
                NOTIFICATION_EMAIL: "user@localhost"
            }
        )
        pattern_two = FileEventPattern(
            "pattern_two", os.path.join("middle", "A.txt"), "recipe_one", "infile", 
            parameters={
                "extra":"A line from Pattern 2",
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            },
            notifications={
                NOTIFICATION_EMAIL: "user@localhost"
            }
        )
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK
        )

        patterns = {
            pattern_one.name: pattern_one,
            pattern_two.name: pattern_two,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE,
            ),
            LocalPythonConductor(pause_time=2, 
                notification_email="alert@localhost",
                notification_email_smtp="localhost:1025"
            ),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])
   
        email_controller = Controller(
            EmailHandler(), hostname="localhost", port=1025
        )
        email_controller.start()

        self.assertEqual(len(email_controller.handler.messages), 0)
    
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.append(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        runner.stop()

        email_controller.stop()
        self.assertEqual(len(email_controller.handler.messages), 2)
        for msg in email_controller.handler.messages:
            self.assertEqual(msg.mail_from, "alert@localhost")
            self.assertEqual(msg.rcpt_tos, ["user@localhost"])
            self.assertIn("Job ", msg.content.decode())
            self.assertIn(
                " completed with status done.\r\n", msg.content.decode())
            job_id = msg.content.decode().replace("Job ", "")\
                .replace(" completed with status done.\r\n", "")
            self.assertIn(job_id, job_ids)

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        self.assertIn(job_ids[0], os.listdir(TEST_JOB_OUTPUT))
        self.assertIn(job_ids[1], os.listdir(TEST_JOB_OUTPUT))

        mid_job_dir = os.path.join(TEST_JOB_OUTPUT, job_ids[0])
        self.assertEqual(count_non_locks(mid_job_dir), 6)

        result = read_notebook(
            os.path.join(mid_job_dir, "result.ipynb"))
        self.assertIsNotNone(result)

        mid_output_path = os.path.join(TEST_MONITOR_BASE, "middle", "A.txt")
        self.assertTrue(os.path.exists(mid_output_path))
        
        with open(mid_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from Pattern 1")

        final_job_dir = os.path.join(TEST_JOB_OUTPUT, job_ids[1])
        self.assertEqual(count_non_locks(final_job_dir), 6)

        result = read_notebook(os.path.join(final_job_dir, 
            "result.ipynb"))
        self.assertIsNotNone(result)

        final_output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(final_output_path))
        
        with open(final_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, 
            "Initial Data\nA line from Pattern 1\nA line from Pattern 2")

    # Test single meow python job execution
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

        runner = MeowRunner(
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
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])
      
   
        runner.start()

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

        runner.stop()

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

    # Test meow python job chaining within runner
    def testMeowRunnerLinkedPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "num":250,
                "outfile":os.path.join("{BASE}", "middle", "{FILENAME}")
            })
        pattern_two = FileEventPattern(
            "pattern_two", 
            os.path.join("middle", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "num":40,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
            pattern_two.name: pattern_two,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner = MeowRunner(
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
            job_output_dir=TEST_JOB_OUTPUT
        )        

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("100")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))


        loops = 0
        job_ids = []
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.append(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        runner.stop()

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        self.assertIn(job_ids[0], os.listdir(TEST_JOB_OUTPUT))
        self.assertIn(job_ids[1], os.listdir(TEST_JOB_OUTPUT))

        meta0 = os.path.join(TEST_JOB_OUTPUT, job_ids[0], META_FILE)
        status0 = read_yaml(meta0)
        create0 = status0[JOB_CREATE_TIME]
        meta1 = os.path.join(TEST_JOB_OUTPUT, job_ids[1], META_FILE)
        status1 = read_yaml(meta1)
        create1 = status1[JOB_CREATE_TIME]
        if create0 < create1:
            mid_job_id = job_ids[0]
            final_job_id = job_ids[1]
        else:
            mid_job_id = job_ids[1]
            final_job_id = job_ids[0]

        mid_job_dir = os.path.join(TEST_JOB_OUTPUT, mid_job_id)
        self.assertEqual(count_non_locks(mid_job_dir), 5)

        mid_metafile = os.path.join(mid_job_dir, META_FILE)
        mid_status = read_yaml(mid_metafile)
        self.assertNotIn(JOB_ERROR, mid_status)

        mid_result_path = os.path.join(mid_job_dir, "stdout.txt")
        self.assertTrue(os.path.exists(mid_result_path))
        mid_result = read_file(os.path.join(mid_result_path))
        self.assertEqual(
            mid_result, "7806.25\ndone\n")

        mid_output_path = os.path.join(TEST_MONITOR_BASE, "middle", "A.txt")
        self.assertTrue(os.path.exists(mid_output_path))
        mid_output = read_file(os.path.join(mid_output_path))
        self.assertEqual(mid_output, "7806.25")

        final_job_dir = os.path.join(TEST_JOB_OUTPUT, final_job_id)
        self.assertEqual(count_non_locks(final_job_dir), 5)

        final_metafile = os.path.join(final_job_dir, META_FILE)
        final_status = read_yaml(final_metafile)
        self.assertNotIn(JOB_ERROR, final_status)

        final_result_path = os.path.join(final_job_dir, "stdout.txt")
        self.assertTrue(os.path.exists(final_result_path))
        final_result = read_file(os.path.join(final_result_path))
        self.assertEqual(
            final_result, "2146.5625\ndone\n")

        final_output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(final_output_path))
        final_output = read_file(os.path.join(final_output_path))
        self.assertEqual(final_output, "2146.5625")

    # Test single meow python job execution
    def testMeowRunnerSweptPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile",
            sweep=create_parameter_sweep("num", 1000, 10000, 200),
            parameters={
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

        runner = MeowRunner(
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
            job_output_dir=TEST_JOB_OUTPUT
        )   

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while loops < 60:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 46:
                break

            if isinstance(msg, str):
                job_ids.append(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        runner.stop()

        self.assertIsNotNone(job_ids)
        self.assertEqual(len(job_ids), 46)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 46)

        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(job_dir, "stdout.txt")
            self.assertTrue(os.path.exists(result_path))
            
            output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
            self.assertTrue(os.path.exists(output_path))

    # Test monitor meow editting
    def testMeowRunnerMEOWEditting(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", os.path.join("start", "A.txt"), "recipe_one", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        pattern_two = FileEventPattern(
            "pattern_two", os.path.join("start", "A.txt"), "recipe_two", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe_one = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )
        recipe_two = PythonRecipe(
            "recipe_two", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe_one.name: recipe_one,
        }

        runner = MeowRunner(
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
            job_output_dir=TEST_JOB_OUTPUT
        ) 

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = set()
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        self.assertEqual(len(job_ids), 1)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.monitors[0].add_pattern(pattern_two)

        loops = 0
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        self.assertEqual(len(job_ids), 1)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.monitors[0].add_recipe(recipe_two)

        loops = 0
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

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

    def testSelfModifyingAnalysis(self)->None:
        maker_pattern = FileEventPattern(
            "maker_pattern", 
            os.path.join("confs", "*.yml"), 
            "maker_recipe", 
            "input_yaml",
            parameters={
                "meow_dir": "self-modifying",
                "filter_recipe": "recipe_filter",
                "recipe_input_image": "input_image",
                "recipe_output_image": "output_image",
                "recipe_args": "args",
                "recipe_method": "method"
            })
        patterns = {
            "maker_pattern": maker_pattern,
        }

        filter_recipe = JupyterNotebookRecipe(
            "filter_recipe", FILTER_RECIPE
        )
        maker_recipe = JupyterNotebookRecipe(
            "maker_recipe", MAKER_RECIPE
        )

        recipes = {
            filter_recipe.name: filter_recipe,
            maker_recipe.name: maker_recipe
        }

        runner = MeowRunner(
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
            job_output_dir=TEST_JOB_OUTPUT
        )    

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])    

        # TODO finish me
#        runner.start()

    # Test some actual scientific analysis, but in a simple progression
    def testScientificAnalysisAllGood(self)->None:
        try:
            if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
                warn("Skipping testScientificAnalysisAllGood")
                return
        except KeyError:
            pass
        
        patterns = {
            'pattern_check': self.pattern_check,
            'pattern_segment': self.pattern_segment,
            'pattern_analysis': self.pattern_analysis,
            'pattern_regenerate': self.pattern_regenerate
        }

        recipes = {
            'recipe_check': self.recipe_check,
            'recipe_segment': self.recipe_segment,
            'recipe_analysis': self.recipe_analysis,
            'recipe_generator': self.recipe_generator
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        good = 3
        big = 0
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)

        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)

        runner.start()

        loops = 0
        idles = 0
        while loops < 150 and idles < 15:
            # Initial prompt
            if conductor_to_test_test.poll(45):
                msg = conductor_to_test_test.recv()
            else:
                break       
            self.assertEqual(msg, 1)
            test_to_runner_test.send(msg)

            # Reply
            if test_to_runner_test.poll(5):
                msg = test_to_runner_test.recv()
                if msg == 1:
                    idles += 1
            else:
                break      
            conductor_to_test_test.send(msg)

            loops += 1

        runner.stop()

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), good * 3)
        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, "result.ipynb")
            self.assertTrue(os.path.exists(result_path))

    # Test some actual scientific analysis, in a predicatable loop
    def testScientificAnalysisPredictableLoop(self)->None:
        try:
            if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
                warn("Skipping testScientificAnalysisAllGood")
                return
        except KeyError:
            pass

        
        patterns = {
            'pattern_check': self.pattern_check,
            'pattern_segment': self.pattern_segment,
            'pattern_analysis': self.pattern_analysis,
            'pattern_regenerate': self.pattern_regenerate
        }

        recipes = {
            'recipe_check': self.recipe_check,
            'recipe_segment': self.recipe_segment,
            'recipe_analysis': self.recipe_analysis,
            'recipe_generator': self.recipe_generator
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        good = 10
        big = 5
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        loops = 0
        idles = 0
        while loops < 600 and idles < 15:
            # Initial prompt
            if conductor_to_test_test.poll(45):
                msg = conductor_to_test_test.recv()
            else:
                break       
            self.assertEqual(msg, 1)
            test_to_runner_test.send(msg)

            # Reply
            if test_to_runner_test.poll(15):
                msg = test_to_runner_test.recv()
                if msg == 1:
                    idles += 1
                else:
                    idles = 0
            else:
                break      
            conductor_to_test_test.send(msg)

            loops += 1

        runner.stop()
        print(f"total_loops:{loops}, idle_loops:{idles}")

        jobs = len(os.listdir(TEST_JOB_OUTPUT))
        if jobs != (good*3 + big*5 + small*5):
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-predictable-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-predictable-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-predictable-{TEST_MONITOR_BASE}")
        
        self.assertEqual(jobs, good*3 + big*5 + small*5)
        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-predictable-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-predictable-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-predictable-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, "result.ipynb")
            self.assertTrue(os.path.exists(result_path))

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))
        if results != good+big+small:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-predictable-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-predictable-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-predictable-{TEST_MONITOR_BASE}")

        self.assertEqual(results, good+big+small)              

    # Test some actual scientific analysis, in an unpredicatable loop
    def testScientificAnalysisRandomLoop(self)->None:
        try:
            if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
                warn("Skipping testScientificAnalysisAllGood")
                return
        except KeyError:
            pass

        pattern_regenerate_random = FileEventPattern(
            "pattern_regenerate_random",
            os.path.join("foam_ct_data_discarded", "*"),
            "recipe_generator",
            "discarded",
            parameters={
                "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
                "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
                "gen_path": os.path.join("{BASE}", "generator.py"),
                "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
                "vx": 64,
                "vy": 64,
                "vz": 64,
                "res": 3/64,
                "chance_good": 1,
                "chance_small": 0,
                "chance_big": 1
            })

        patterns = {
            'pattern_check': self.pattern_check,
            'pattern_segment': self.pattern_segment,
            'pattern_analysis': self.pattern_analysis,
            'pattern_regenerate_random': pattern_regenerate_random
        }

        recipes = {
            'recipe_check': self.recipe_check,
            'recipe_segment': self.recipe_segment,
            'recipe_analysis': self.recipe_analysis,
            'recipe_generator': self.recipe_generator
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        good = 10
        big = 5
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        loops = 0
        idles = 0
        while loops < 600 and idles < 15:
            # Initial prompt
            if conductor_to_test_test.poll(60):
                msg = conductor_to_test_test.recv()
            else:
                break       
            self.assertEqual(msg, 1)
            test_to_runner_test.send(msg)

            # Reply
            if test_to_runner_test.poll(15):
                msg = test_to_runner_test.recv()
                if msg == 1:
                    idles += 1
                else:
                    idles = 0
            else:
                break      
            conductor_to_test_test.send(msg)

            loops += 1

        runner.stop()
        print(f"total_loops:{loops}, idle_loops:{idles}")

        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-random-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-random-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-random-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, "result.ipynb")
            self.assertTrue(os.path.exists(result_path))

        outputs = len(os.listdir(TEST_JOB_OUTPUT))
        if outputs < good*3 + big*5 + small*5:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-random-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-random-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-random-{TEST_MONITOR_BASE}")

        self.assertTrue(outputs >= good*3 + big*5 + small*5)

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))

        self.assertEqual(results, good+big+small)

    # Test some actual scientific analysis, in an unpredicatable loop
    def testScientificAnalysisMassiveRandomLoop(self)->None:
        try:
            if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
                warn("Skipping testScientificAnalysisAllGood")
                return
        except KeyError:
            pass


        pattern_regenerate_random = FileEventPattern(
            "pattern_regenerate_random",
            os.path.join("foam_ct_data_discarded", "*"),
            "recipe_generator",
            "discarded",
            parameters={
                "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
                "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
                "gen_path": os.path.join("{BASE}", "generator.py"),
                "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
                "vx": 32,
                "vy": 32,
                "vz": 32,
                "res": 3/32,
                "chance_good": 1,
                "chance_small": 0,
                "chance_big": 3
            })

        patterns = {
            'pattern_check': self.pattern_check,
            'pattern_segment': self.pattern_segment,
            'pattern_analysis': self.pattern_analysis,
            'pattern_regenerate_random': pattern_regenerate_random
        }

        recipes = {
            'recipe_check': self.recipe_check,
            'recipe_segment': self.recipe_segment,
            'recipe_analysis': self.recipe_analysis,
            'recipe_generator': self.recipe_generator
        }

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        good = 5
        big = 15
        small = 0
        vx = 32
        vy = 32
        vz = 32
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        loops = 0
        idles = 0
        while loops < 1200 and idles < 15:
            # Initial prompt
            if conductor_to_test_test.poll(60):
                msg = conductor_to_test_test.recv()
            else:
                break       
            self.assertEqual(msg, 1)
            test_to_runner_test.send(msg)

            # Reply
            if test_to_runner_test.poll(15):
                msg = test_to_runner_test.recv()
                if msg == 1:
                    idles += 1
                else:
                    idles = 0
            else:
                break      
            conductor_to_test_test.send(msg)

            loops += 1

        runner.stop()
        print(f"total_loops:{loops}, idle_loops:{idles}")

        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-massive-random-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-massive-random-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-massive-random-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, "result.ipynb")
            self.assertTrue(os.path.exists(result_path))

        outputs = len(os.listdir(TEST_JOB_OUTPUT))
        if outputs < good*3 + big*5 + small*5:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-massive-random-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-massive-random-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-massive-random-{TEST_MONITOR_BASE}")
        self.assertTrue(outputs >= good*3 + big*5 + small*5)

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))

        self.assertEqual(results, good+big+small)

    def testMonitorIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        m1 = runner.get_monitor_by_name("m1")
        self.assertEqual(monitor_one, m1)
        m2 = runner.get_monitor_by_name("m2")
        self.assertEqual(monitor_two, m2)
        m3 = runner.get_monitor_by_name("m3")
        self.assertIsNone(m3)

        mt = runner.get_monitor_by_type(WatchdogMonitor)
        self.assertIn(mt, monitors)

    def testHandlerIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        h1 = runner.get_handler_by_name("h1")
        self.assertEqual(handler_one, h1)
        h2 = runner.get_handler_by_name("h2")
        self.assertEqual(handler_two, h2)
        h3 = runner.get_handler_by_name("h3")
        self.assertIsNone(h3)

        mt = runner.get_handler_by_type(PapermillHandler)
        self.assertIn(mt, handlers)
        mn = runner.get_handler_by_type(PythonHandler)
        self.assertIsNone(mn)

    def testConductorIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        c1 = runner.get_conductor_by_name("c1")
        self.assertEqual(conductor_one, c1)
        c2 = runner.get_conductor_by_name("c2")
        self.assertEqual(conductor_two, c2)
        c3 = runner.get_conductor_by_name("c3")
        self.assertIsNone(c3)

        ct = runner.get_conductor_by_type(LocalPythonConductor)
        self.assertIn(ct, conductors)

class NetworkEventTests(unittest.TestCase):
    pattern_check = SocketPattern(
        "pattern_check", 
        8080, 
        "recipe_check", 
        "msg",
        parameters={
            "output_filedir_accepted": 
                os.path.join("{BASE}", "foam_ct_data_accepted"),
            "output_filedir_discarded": 
                os.path.join("{BASE}", "foam_ct_data_discarded"),
            "porosity_lower_threshold": 0.8,
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_segment = SocketPattern(
        "pattern_segment",
        8081,
        "recipe_segment",
        "msg",
        parameters={
            "output_filedir": os.path.join("{BASE}", "foam_ct_data_segmented"),
            "input_filedir": os.path.join("{BASE}", "foam_ct_data"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_analysis = SocketPattern(
        "pattern_analysis",
        8082,
        "recipe_analysis",
        "msg",
        parameters={
            "output_filedir": os.path.join("{BASE}", "foam_ct_data_pore_analysis"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
        })

    pattern_regenerate = SocketPattern(
        "pattern_regenerate",
        8083,
        "recipe_generator",
        "msg",
        parameters={
            "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
            "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
            "gen_path": os.path.join("{BASE}", "generator.py"),
            "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
            "vx": 64,
            "vy": 64,
            "vz": 64,
            "res": 3/64,
            "chance_good": 1,
            "chance_small": 0,
            "chance_big": 0
        })

    recipe_check_key, recipe_check_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib"])
    recipe_check = JupyterNotebookRecipe(
        'recipe_check',
        POROSITY_CHECK_NOTEBOOK, 
        requirements={recipe_check_key: recipe_check_req}
    )

    recipe_segment_key, recipe_segment_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
    recipe_segment = JupyterNotebookRecipe(
        'recipe_segment',
        SEGMENT_FOAM_NOTEBOOK, 
        requirements={recipe_segment_key: recipe_segment_req}
    )

    recipe_analysis_key, recipe_analysis_req = create_python_requirements(
        modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
    recipe_analysis = JupyterNotebookRecipe(
        'recipe_analysis',
        FOAM_PORE_ANALYSIS_NOTEBOOK, 
        requirements={recipe_analysis_key: recipe_analysis_req}
    )

    recipe_generator_key, recipe_generator_req = create_python_requirements(
        modules=["numpy", "matplotlib", "random"])
    recipe_generator = JupyterNotebookRecipe(
        'recipe_generator',
        GENERATOR_NOTEBOOK, 
        requirements={recipe_generator_key: recipe_generator_req}           
    )

    def setUp(self)->None:
        super().setUp()
        setup()
        for pattern in [self.pattern_check, self.pattern_segment, 
                        self.pattern_analysis, self.pattern_regenerate]:
            check_port_in_use(pattern.triggering_port)

    def tearDown(self)->None:
        super().tearDown()
        teardown()
        for pattern in [self.pattern_check, self.pattern_segment, 
                        self.pattern_analysis, self.pattern_regenerate]:
            check_shutdown_port_in_timeout(pattern.triggering_port, 5)

    # Test MeowRunner creation
    def testMeowRunnerSetup(self)->None:
        monitor_one = SocketMonitor(TEST_MONITOR_BASE, {}, {})
        monitor_two = SocketMonitor(TEST_MONITOR_BASE, {}, {})
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(pause_time=0)
        handler_two = PapermillHandler(pause_time=0)
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(pause_time=0)
        conductor_two = LocalPythonConductor(pause_time=0)
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitor_one, handler_one, conductor_one)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), 1)
        self.assertEqual(runner.monitors[0], monitor_one)

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)
        self.assertEqual(len(runner.handlers), 1)
        self.assertEqual(runner.handlers[0], handler_one)

        self.assertIsInstance(runner.conductors, list)        
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)
        self.assertEqual(len(runner.conductors), 1)
        self.assertEqual(runner.conductors[0], conductor_one)

        self.assertIsInstance(runner.event_connections, list)
        self.assertEqual(len(runner.event_connections), 2)

        runner.monitors[0].send_event_to_runner("monitor test message")
        message = None
        from_monitor = [i[0] for i in runner.event_connections \
                        if isinstance(i[1], BaseMonitor)][0]
        if from_monitor.poll(3):
            message = from_monitor.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "monitor test message")

        runner.handlers[0].prompt_runner_for_event()
        message = None
        from_handler = [i[0] for i in runner.event_connections \
                        if isinstance(i[1], BaseHandler)][0]
        if from_handler.poll(3):
            message = from_handler.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, 1)

        self.assertIsInstance(runner.job_connections, list)
        self.assertEqual(len(runner.job_connections), 2)

        runner.handlers[0].send_job_to_runner("handler test message")
        message = None
        from_handler = [i[0] for i in runner.job_connections \
                        if isinstance(i[1], BaseHandler)][0]
        if from_handler.poll(3):
            message = from_handler.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "handler test message")

        runner.conductors[0].prompt_runner_for_job()
        message = None
        from_conductor = [i[0] for i in runner.job_connections \
                        if isinstance(i[1], BaseConductor)][0]
        if from_conductor.poll(3):
            message = from_conductor.recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, 1)

        runner = MeowRunner(monitors, handlers, conductors)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), len(monitors))
        for monitor in monitors:
            self.assertIn(monitor, monitors)

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)
        self.assertEqual(len(runner.handlers), 2)
        for handler in handlers:
            self.assertIn(handler, handlers)

        self.assertIsInstance(runner.conductors, list)        
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)
        self.assertEqual(len(runner.conductors), len(conductors))
        for conductor in conductors:
            self.assertIn(conductor, conductors)

        self.assertIsInstance(runner.event_connections, list)
        self.assertEqual(len(runner.event_connections), 4)

        for monitor in monitors:
            monitor.send_event_to_runner("monitor test message")
    
            message = None
            from_monitor = [i[0] for i in runner.event_connections \
                            if i[1] is monitor][0]
            if from_monitor.poll(3):
                message = from_monitor.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, "monitor test message")

        for handler in handlers:
            handler.prompt_runner_for_event()
            message = None
            from_handler = [i[0] for i in runner.event_connections \
                            if i[1] is handler][0]
            if from_handler.poll(3):
                message = from_handler.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, 1)

        self.assertIsInstance(runner.job_connections, list)
        self.assertEqual(len(runner.job_connections), 4)

        for handler in handlers:
            handler.send_job_to_runner("handler test message")
            message = None
            from_handler = [i[0] for i in runner.job_connections \
                            if i[1] is handler][0]
            if from_handler.poll(3):
                message = from_handler.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, "handler test message")

        for conductor in conductors:
            conductor.prompt_runner_for_job()
            message = None
            from_conductor = [i[0] for i in runner.job_connections \
                            if i[1] is conductor][0]
            if from_conductor.poll(3):
                message = from_conductor.recv()
            self.assertIsNotNone(message)
            self.assertEqual(message, 1)
    
    # Test single meow papermill job execution
    def testMeowRunnerPapermillExecution(self)->None:
        pattern_one = SocketPattern(
            "pattern_one", 
            8080, 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            },
            notifications={
                NOTIFICATION_EMAIL: "user@localhost"
            }
        )
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner = MeowRunner(
            SocketMonitor(
                TEST_MONITOR_BASE, 
                patterns,
                recipes,
            ), 
            PapermillHandler(),
            LocalPythonConductor(pause_time=2, 
                notification_email="alert@localhost",
                notification_email_smtp="localhost:1025"
            ),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        email_controller = Controller(
            EmailHandler(), hostname="localhost", port=1025
        )
        email_controller.start()

        self.assertEqual(len(email_controller.handler.messages), 0)

        runner.start()

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect(("localhost", pattern_one.triggering_port))
        sender.sendall(b"test message")
        sender.close()

        loops = 0
        while loops < 10:
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
                loops = 10

            loops += 1


        job_dir = job_dir.replace(TEST_JOB_QUEUE, TEST_JOB_OUTPUT)
        job_id = job_dir.replace(f"{TEST_JOB_OUTPUT}{os.path.sep}", "")

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)
        
        self.assertIn(JOB_EVENT, status)
        self.assertIn(EVENT_PATH, status[JOB_EVENT])

        tmp_file = status[JOB_EVENT][EVENT_PATH]
        self.assertTrue(os.path.exists(tmp_file))

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertTrue(os.path.exists(job_dir))

        runner.stop()

        self.assertFalse(os.path.exists(tmp_file))

        email_controller.stop()
        self.assertEqual(len(email_controller.handler.messages), 1)
        self.assertEqual(
            email_controller.handler.messages[0].mail_from, 
            "alert@localhost"
        )
        self.assertEqual(
            email_controller.handler.messages[0].rcpt_tos, 
            ["user@localhost"]
        )

        self.assertEqual(
            email_controller.handler.messages[0].content.decode(), 
            f'Job {job_id} completed with status done.\r\n'
        )

        self.assertEqual(count_non_locks(job_dir), 6)

        result = read_notebook(
            os.path.join(job_dir, "result.ipynb"))
        self.assertIsNotNone(result)

        output_filename = tmp_file[len(TEST_MONITOR_BASE)+1:]
        output_path = os.path.join(
            TEST_MONITOR_BASE, "output", output_filename
        )
        self.assertTrue(os.path.exists(output_path))
        
        with open(output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "test message\nA line from a test Pattern")

    # Test single meow python job execution
    def testMeowRunnerPythonExecution(self)->None:
        pattern_one = SocketPattern(
            "pattern_one", 
            8080, 
            "recipe_one", 
            "infile", 
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

        runner = MeowRunner(
            SocketMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        )

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])
      
   
        runner.start()

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect(("localhost", pattern_one.triggering_port))
        sender.sendall(b"25000")
        sender.close()

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

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)
        
        self.assertIn(JOB_EVENT, status)
        self.assertIn(EVENT_PATH, status[JOB_EVENT])

        tmp_file = status[JOB_EVENT][EVENT_PATH]
        self.assertTrue(os.path.exists(tmp_file))

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertTrue(os.path.exists(job_dir))

        runner.stop()

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        result_path = os.path.join(job_dir, "stdout.txt")
        self.assertTrue(os.path.exists(result_path))
        result = read_file(os.path.join(result_path))
        self.assertEqual(
            result, "12505000.0\ndone\n")

        output_filename = tmp_file[len(TEST_MONITOR_BASE)+1:]
        output_path = os.path.join(
            TEST_MONITOR_BASE, "output", output_filename
        )
        self.assertTrue(os.path.exists(output_path))

        output = read_file(os.path.join(output_path))
        self.assertEqual(output, "12505000.0")

    # Test monitor meow editting
    def testMeowRunnerMEOWEditting(self)->None:
        pattern_one = SocketPattern(
            "pattern_one", 
            8080, 
            "recipe_one", 
            "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        pattern_two = SocketPattern(
            "pattern_two", 
            8080, 
            "recipe_two", 
            "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe_one = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )
        recipe_two = PythonRecipe(
            "recipe_two", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe_one.name: recipe_one,
        }

        runner = MeowRunner(
            SocketMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(pause_time=2),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT
        ) 

        # Intercept messages between the conductor and runner for testing
        conductor_to_test_conductor, conductor_to_test_test = Pipe(duplex=True)
        test_to_runner_runner, test_to_runner_test = Pipe(duplex=True)

        runner.conductors[0].to_runner_job = conductor_to_test_conductor

        for i in range(len(runner.job_connections)):
            _, obj = runner.job_connections[i]

            if obj == runner.conductors[0]:
                runner.job_connections[i] = (test_to_runner_runner, runner.job_connections[i][1])

        runner.start()

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect(("localhost", pattern_one.triggering_port))
        sender.sendall(b"25000")
        sender.close()

        loops = 0
        job_ids = set()
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        print(f"job_ids: {job_ids}")

        self.assertEqual(len(job_ids), 1)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))
            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)
        
            self.assertIn(JOB_EVENT, status)
            self.assertIn(EVENT_PATH, status[JOB_EVENT])

            tmp_file = status[JOB_EVENT][EVENT_PATH]
            self.assertTrue(os.path.exists(tmp_file))

        runner.monitors[0].add_pattern(pattern_two)

        sleep(1)

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect(("localhost", 8080))
        sender.sendall(b"25000")
        sender.close()

        print("BAM")
        print(runner.monitors[0].get_rules())
        print(runner.monitors[0]._rules)
        print(runner.monitors[0].listeners)
        print(runner.monitors[0].ports)
        print("BOM")

        loops = 0
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 2:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        print(f"job_ids: {job_ids}")

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))
            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)
        
            self.assertIn(JOB_EVENT, status)
            self.assertIn(EVENT_PATH, status[JOB_EVENT])

            tmp_file = status[JOB_EVENT][EVENT_PATH]
            self.assertTrue(os.path.exists(tmp_file))

        runner.monitors[0].add_recipe(recipe_two)

        sleep(1)

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect(("localhost", 8080))
        sender.sendall(b"25000")
        sender.close()

        loops = 0
        while loops < 15:
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
            conductor_to_test_test.send(msg)

            if len(job_ids) == 4:
                break

            if isinstance(msg, str):
                job_ids.add(msg.replace(TEST_JOB_QUEUE+os.path.sep, ''))

            loops += 1

        self.assertEqual(len(runner.monitors[0].get_rules()), 2)
        print(runner.monitors[0].get_rules())
        print(runner.monitors[0]._rules)
        print(runner.monitors[0].listeners)
        print(runner.monitors[0].ports)
        self.assertEqual(len(job_ids), 4)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 4)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))
            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)
        
            self.assertIn(JOB_EVENT, status)
            self.assertIn(EVENT_PATH, status[JOB_EVENT])

            tmp_file = status[JOB_EVENT][EVENT_PATH]
            self.assertTrue(os.path.exists(tmp_file))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        result_path = os.path.join(job_dir, "stdout.txt")
        self.assertTrue(os.path.exists(result_path))
        result = read_file(os.path.join(result_path))
        self.assertEqual(
            result, "12505000.0\ndone\n")

        for job_id in job_ids:
            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)
            tmp_file = status[JOB_EVENT][EVENT_PATH]

            output_filename = tmp_file[len(TEST_MONITOR_BASE)+1:]
            output_path = os.path.join(
                TEST_MONITOR_BASE, "output", output_filename
            )
            self.assertTrue(os.path.exists(output_path))

            output = read_file(os.path.join(output_path))
            self.assertEqual(output, "12505000.0")

    # TODO add the rest of the tests here
