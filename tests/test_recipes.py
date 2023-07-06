
import jsonschema
import os
import unittest

from multiprocessing import Pipe
from time import time

from ..meow_base.core.meow import valid_job
from ..meow_base.core.vars import EVENT_TYPE, EVENT_RULE, EVENT_PATH, SHA256, \
    JOB_PARAMETERS, JOB_FILE, META_FILE, SWEEP_STOP, SWEEP_JUMP, \
    SWEEP_START, EVENT_TIME
from ..meow_base.core.rule import Rule
from ..meow_base.functionality.file_io import read_yaml, write_notebook, \
    threadsafe_read_status
from ..meow_base.functionality.hashing import get_hash
from ..meow_base.functionality.meow import create_rules, create_rule
from ..meow_base.patterns.file_event_pattern import FileEventPattern, \
    WATCHDOG_BASE, WATCHDOG_HASH, EVENT_TYPE_WATCHDOG
from ..meow_base.recipes.bash_recipe import BashRecipe, BashHandler
from ..meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, \
    PapermillHandler, get_recipe_from_notebook
from ..meow_base.recipes.python_recipe import PythonRecipe, PythonHandler
from .shared import BAREBONES_PYTHON_SCRIPT, COMPLETE_PYTHON_SCRIPT, \
    TEST_JOB_QUEUE, TEST_MONITOR_BASE, TEST_JOB_OUTPUT, BAREBONES_NOTEBOOK, \
    COMPLETE_NOTEBOOK, BAREBONES_BASH_SCRIPT, COMPLETE_BASH_SCRIPT, \
    setup, teardown

class JupyterNotebookTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test JupyterNotebookRecipe can be created
    def testJupyterNotebookRecipeCreationMinimum(self)->None:
        JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe can be created with source
    def testJupyterNotebookRecipeCreationSource(self)->None:
        JupyterNotebookRecipe(
            "test_recipe", BAREBONES_NOTEBOOK, source="notebook.ipynb")

    # Test JupyterNotebookRecipe cannot be created without name
    def testJupyterNotebookRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid name
    def testJupyterNotebookRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("@test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid recipe
    def testJupyterNotebookRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            JupyterNotebookRecipe("test_recipe", {})

    # Test JupyterNotebookRecipe cannot be created with invalid source
    def testJupyterNotebookRecipeCreationInvalidSourceExtension(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="notebook")

    # Test JupyterNotebookRecipe cannot be created with invalid source name
    def testJupyterNotebookRecipeCreationInvalidSoureChar(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="@notebook.ipynb")

    # Test JupyterNotebookRecipe name setup correctly
    def testJupyterNotebookRecipeSetupName(self)->None:
        name = "name"
        jnr = JupyterNotebookRecipe(name, BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.name, name)

    # Test JupyterNotebookRecipe recipe setup correctly
    def testJupyterNotebookRecipeSetupRecipe(self)->None:
        jnr = JupyterNotebookRecipe("name", BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.recipe, BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe parameters setup correctly
    def testJupyterNotebookRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, parameters=parameters)
        self.assertEqual(jnr.parameters, parameters)

    # Test JupyterNotebookRecipe requirements setup correctly
    def testJupyterNotebookRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, requirements=requirements)
        self.assertEqual(jnr.requirements, requirements)

    # Test JupyterNotebookRecipe source setup correctly
    def testJupyterNotebookRecipeSetupSource(self)->None:
        source = "source.ipynb"
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, source=source)
        self.assertEqual(jnr.source, source)

class PapermillHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test PapermillHandler can be created
    def testPapermillHanderMinimum(self)->None:
        PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test PapermillHandler naming
    def testPapermillHandlerNaming(self)->None:
        test_name = "test_name"
        handler = PapermillHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = PapermillHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test PapermillHandler will handle given events
    def testPapermillHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test PapermillHandler will create enough jobs from single sweep
    def testPapermillHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test PapermillHandler will create enough jobs from multiple sweeps
    def testPapermillHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = PapermillHandler()

        pattern = FileEventPattern(
            "pattern_ne", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule.name,
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule",
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule,
            EVENT_TIME: time()
        })
        self.assertTrue(status)

    # Test function correctly creates Recipe directly from notebook
    def testGetRecipeFromNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "notebook.ipynb")
        write_notebook(COMPLETE_NOTEBOOK, notebook_path)

        self.assertTrue(os.path.exists(notebook_path))

        recipe = get_recipe_from_notebook("name", notebook_path)

        self.assertIsInstance(recipe, JupyterNotebookRecipe)
        self.assertEqual(recipe.name, "name")
        self.assertEqual(recipe.recipe, COMPLETE_NOTEBOOK)

    # Test handler starts and stops appropriatly
    def testPapermillHandlerStartStop(self)->None:
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testPapermillHandlerOngoingHandling(self)->None:
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)


class PythonTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
 
    # Test PythonRecipe can be created
    def testPythonRecipeCreationMinimum(self)->None:
        PythonRecipe("test_recipe", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created without name
    def testPythonRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            PythonRecipe("", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created with invalid name
    def testPythonRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            PythonRecipe("@test_recipe", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created with invalid recipe
    def testPythonRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(TypeError):
            PythonRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test PythonRecipe name setup correctly
    def testPythonRecipeSetupName(self)->None:
        name = "name"
        pr = PythonRecipe(name, BAREBONES_PYTHON_SCRIPT)
        self.assertEqual(pr.name, name)

    # Test PythonRecipe recipe setup correctly
    def testPythonRecipeSetupRecipe(self)->None:
        pr = PythonRecipe("name", BAREBONES_PYTHON_SCRIPT)
        self.assertEqual(pr.recipe, BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe parameters setup correctly
    def testPythonRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        pr = PythonRecipe(
            "name", BAREBONES_PYTHON_SCRIPT, parameters=parameters)
        self.assertEqual(pr.parameters, parameters)

    # Test PythonRecipe requirements setup correctly
    def testPythonRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        pr = PythonRecipe(
            "name", BAREBONES_PYTHON_SCRIPT, requirements=requirements)
        self.assertEqual(pr.requirements, requirements)

class PythonHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test PythonHandler can be created
    def testPythonHandlerMinimum(self)->None:
        PythonHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test PythonHandler naming
    def testPythonHandlerNaming(self)->None:
        test_name = "test_name"
        handler = PythonHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = PythonHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test PythonHandler will handle given events
    def testPythonHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test PythonHandler will create enough jobs from single sweep
    def testPythonHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test PythonHandler will create enough jobs from multiple sweeps
    def testPythonHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = PythonHandler()

        pattern = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", BAREBONES_PYTHON_SCRIPT
        )

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule,
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule",
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, s = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule,
            EVENT_TIME: time()
        })
        self.assertTrue(status)

    # Test handler starts and stops appropriatly
    def testPythonHandlerStartStop(self)->None:
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testPythonHandlerOngoingHandling(self)->None:
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)


class BashTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
 
    # Test BashRecipe can be created
    def testBashRecipeCreationMinimum(self)->None:
        BashRecipe("test_recipe", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created without name
    def testBashRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            BashRecipe("", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created with invalid name
    def testBashRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            BashRecipe("@test_recipe", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created with invalid recipe
    def testBashRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(TypeError):
            BashRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test BashRecipe name setup correctly
    def testBashRecipeSetupName(self)->None:
        name = "name"
        pr = BashRecipe(name, BAREBONES_BASH_SCRIPT)
        self.assertEqual(pr.name, name)

    # Test BashRecipe recipe setup correctly
    def testBashRecipeSetupRecipe(self)->None:
        pr = BashRecipe("name", BAREBONES_BASH_SCRIPT)
        self.assertEqual(pr.recipe, BAREBONES_BASH_SCRIPT)

    # Test BashRecipe parameters setup correctly
    def testBashRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        pr = BashRecipe(
            "name", BAREBONES_BASH_SCRIPT, parameters=parameters)
        self.assertEqual(pr.parameters, parameters)

    # Test BashRecipe requirements setup correctly
    def testBashRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        pr = BashRecipe(
            "name", BAREBONES_BASH_SCRIPT, requirements=requirements)
        self.assertEqual(pr.requirements, requirements)

class BashHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BashHandler can be created
    def testBashHandlerMinimum(self)->None:
        BashHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test BashHandler naming
    def testBashHandlerNaming(self)->None:
        test_name = "test_name"
        handler = BashHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = BashHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test BashHandler will handle given events
    def testBashHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test BashHandler will create enough jobs from single sweep
    def testBashHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test BashHandler will create enough jobs from multiple sweeps
    def testBashHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    def testJobSetup(self)->None:
        from_handler_to_runner_reader, from_handler_to_runner_writer = Pipe()
        bh = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        bh.to_runner_job = from_handler_to_runner_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        params_dict = {
            "file_one": os.path.join(TEST_MONITOR_BASE, "A")
        }

        bh.setup_job(event, params_dict)

        if from_handler_to_runner_reader.poll(3):
            job_dir = from_handler_to_runner_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        self.assertTrue(len(os.listdir(job_dir)), 3)
        for f in [META_FILE, "recipe.sh", JOB_FILE]:
            self.assertTrue(os.path.exists(os.path.join(job_dir, f)))

        job = threadsafe_read_status(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = BashHandler()

        pattern = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", BAREBONES_BASH_SCRIPT
        )

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule,
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule",
            EVENT_TIME: time()
        })
        self.assertFalse(status)

        status, s = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule,
            EVENT_TIME: time()
        })
        self.assertTrue(status)

    # Test handler starts and stops appropriatly
    def testBashHandlerStartStop(self)->None:
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testBashHandlerOngoingHandling(self)->None:
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            EVENT_TIME: time(),
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)
