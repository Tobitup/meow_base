
import io
import json
import unittest
import os

from aiosmtpd.controller import Controller
from datetime import datetime
from multiprocessing import Pipe, Queue
from os.path import basename
from sys import prefix, base_prefix
from time import sleep, time
from typing import Dict

from meow_base.core.meow import EVENT_KEYS
from meow_base.core.rule import Rule
from meow_base.core.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    SHA256, EVENT_TYPE, EVENT_PATH, LOCK_EXT, EVENT_RULE, JOB_PARAMETERS, \
    PYTHON_FUNC, JOB_ID, JOB_EVENT, JOB_ERROR, STATUS_DONE, \
    JOB_TYPE, JOB_PATTERN, JOB_RECIPE, JOB_RULE, JOB_STATUS, JOB_CREATE_TIME, \
    JOB_REQUIREMENTS, JOB_TYPE_PAPERMILL, STATUS_CREATING
from meow_base.functionality.debug import setup_debugging
from meow_base.functionality.file_io import lines_to_string, make_dir, \
    read_file, read_file_lines, read_notebook, read_yaml, rmtree, write_file, \
    write_notebook, write_yaml, threadsafe_read_status, \
    threadsafe_update_status, threadsafe_write_status
from meow_base.functionality.hashing import get_hash
from meow_base.functionality.meow import KEYWORD_DIR, \
    KEYWORD_EXTENSION, KEYWORD_FILENAME, KEYWORD_JOB, KEYWORD_PATH, \
    KEYWORD_PREFIX, \
    create_event, create_job_metadata_dict, create_rule, create_rules, \
    replace_keywords, create_parameter_sweep
from meow_base.functionality.naming import _generate_id
from meow_base.functionality.notifications import send_email
from meow_base.functionality.parameterisation import \
    parameterize_jupyter_notebook, parameterize_python_script
from meow_base.functionality.process_io import wait
from meow_base.functionality.requirements import REQUIREMENT_PYTHON, \
    REQ_PYTHON_ENVIRONMENT, REQ_PYTHON_MODULES, REQ_PYTHON_VERSION, \
    create_python_requirements, check_requirements
from meow_base.patterns.file_event_pattern import FileEventPattern, \
    EVENT_TYPE_WATCHDOG, WATCHDOG_BASE, WATCHDOG_HASH, KEYWORD_BASE, \
    KEYWORD_REL_DIR, KEYWORD_REL_PATH, create_watchdog_event
from meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe
from shared import EmailHandler, SharedTestRecipe, TEST_MONITOR_BASE, \
    COMPLETE_NOTEBOOK, APPENDING_NOTEBOOK, COMPLETE_PYTHON_SCRIPT, \
    valid_recipe_two, valid_recipe_one, valid_pattern_one, valid_pattern_two, \
    setup, teardown

class DebugTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test setup_debugging will create writeable location
    def testSetupDebugging(self)->None:
        stream = io.StringIO("")

        target, level = setup_debugging(stream, 1)

        self.assertIsInstance(target, io.StringIO)
        self.assertIsInstance(level, int)

        with self.assertRaises(TypeError):
            setup_debugging("stream", 1)

        with self.assertRaises(TypeError):
            setup_debugging(stream, "1")


class FileIoTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that write_file can write files 
    def testWriteFile(self)->None:
        data = """Some
short
data"""

        filepath = os.path.join(TEST_MONITOR_BASE, "test.txt")

        self.assertFalse(os.path.exists(filepath))
        write_file(data, filepath)
        self.assertTrue(os.path.exists(filepath))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'Some\n', 
            'short\n',
            'data'
        ]
        
        self.assertEqual(data, expected_bytes)

    # Test that write_file can read files 
    def testReadFile(self)->None:
        data = """Some
short
data"""

        filepath = os.path.join(TEST_MONITOR_BASE, "test.txt")

        self.assertFalse(os.path.exists(filepath))
        write_file(data, filepath)

        read_data = read_file(filepath)
        self.assertEqual(data, read_data)

        with self.assertRaises(FileNotFoundError):
            read_file("doesNotExist")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        data = read_file(filepath)
        self.assertEqual(data, "Data")

    # Test that write_file can read files 
    def testReadFileLines(self)->None:
        data = """Some
short
data"""

        filepath = os.path.join(TEST_MONITOR_BASE, "test.txt")

        self.assertFalse(os.path.exists(filepath))
        write_file(data, filepath)

        read_data = read_file_lines(filepath)
        self.assertEqual(read_data, [
            'Some\n', 
            'short\n',
            'data'
        ])

        with self.assertRaises(FileNotFoundError):
            read_file_lines("doesNotExist")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        data = read_file_lines(filepath)
        self.assertEqual(data, ["Data"])

    # Test that write_notebook can read jupyter notebooks to files
    def testWriteNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "test_notebook.ipynb")
        self.assertFalse(os.path.exists(notebook_path))
        write_notebook(APPENDING_NOTEBOOK, notebook_path)
        self.assertTrue(os.path.exists(notebook_path))

        with open(notebook_path, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            '{"cells": [{"cell_type": "code", "execution_count": null, '
            '"metadata": {}, "outputs": [], "source": ["# Default parameters '
            'values\\n", "# The line to append\\n", "extra = \'This line '
            'comes from a default pattern\'\\n", "# Data input file '
            'location\\n", "infile = \'start'+ os.path.sep +'alpha.txt\'\\n", '
            '"# Output file location\\n", "outfile = \'first'+ os.path.sep 
            +'alpha.txt\'"]}, {"cell_type": "code", "execution_count": null, '
            '"metadata": {}, "outputs": [], "source": ["# load in dataset. '
            'This should be a text file\\n", "with open(infile) as '
            'input_file:\\n", "    data = input_file.read()"]}, {"cell_type": '
            '"code", "execution_count": null, "metadata": {}, "outputs": [], '
            '"source": ["# Append the line\\n", "appended = data + \'\\\\n\' '
            '+ extra"]}, {"cell_type": "code", "execution_count": null, '
            '"metadata": {}, "outputs": [], "source": ["import os\\n", "\\n", '
            '"# Create output directory if it doesn\'t exist\\n", '
            '"output_dir_path = os.path.dirname(outfile)\\n", "\\n", '
            '"if output_dir_path:\\n", "    os.makedirs(output_dir_path, '
            'exist_ok=True)\\n", "\\n", "# Save added array as new '
            'dataset\\n", "with open(outfile, \'w\') as output_file:\\n", "   '
            'output_file.write(appended)"]}], "metadata": {"kernelspec": '
            '{"display_name": "Python 3", "language": "python", "name": '
            '"python3"}, "language_info": {"codemirror_mode": {"name": '
            '"ipython", "version": 3}, "file_extension": ".py", "mimetype": '
            '"text/x-python", "name": "python", "nbconvert_exporter": '
            '"python", "pygments_lexer": "ipython3", "version": "3.10.6 '
            '(main, Nov 14 2022, 16:10:14) [GCC 11.3.0]"}, "vscode": '
            '{"interpreter": {"hash": "916dbcbb3f70747c44a77c7bcd40155683ae19c'
            '65e1c03b4aa3499c5328201f1"}}}, "nbformat": 4, "nbformat_minor": '
            '4}'
        ]

        self.assertEqual(data, expected_bytes)

    # Test that read_notebook can read jupyter notebooks from files
    def testReadNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "test_notebook.ipynb")
        write_notebook(APPENDING_NOTEBOOK, notebook_path)

        notebook = read_notebook(notebook_path)
        self.assertEqual(notebook, APPENDING_NOTEBOOK)

        with self.assertRaises(FileNotFoundError):
            read_notebook("doesNotExist.ipynb")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(ValueError):
            read_notebook(filepath)

        filepath = os.path.join(TEST_MONITOR_BASE, "T.ipynb")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(json.decoder.JSONDecodeError):
            read_notebook(filepath)

    # Test that write_yaml can write dicts to yaml files
    def testWriteYaml(self)->None:
        yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        self.assertFalse(os.path.exists(filepath))
        write_yaml(yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'A: a\n', 
            'B: 1\n',
            'C:\n', 
            '  D: true\n',
            '  E:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n'
        ]
        
        self.assertEqual(data, expected_bytes)

    # Test that read_yaml can read yaml files
    def testReadYaml(self)->None:
        yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")
        write_yaml(yaml_dict, filepath)

        read_dict = read_yaml(filepath)
        self.assertEqual(yaml_dict, read_dict)

        with self.assertRaises(FileNotFoundError):
            read_yaml("doesNotExist")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        data = read_yaml(filepath)
        self.assertEqual(data, "Data")

    # Test that make_dir creates a directory and path to it
    def testMakeDir(self)->None:
        testDir = os.path.join(TEST_MONITOR_BASE, "Test")
        self.assertFalse(os.path.exists(testDir))
        make_dir(testDir)
        self.assertTrue(os.path.exists(testDir))
        self.assertTrue(os.path.isdir(testDir))

        nested = os.path.join(TEST_MONITOR_BASE, "A", "B", "C", "D")
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        make_dir(nested)
        self.assertTrue(os.path.exists(nested))

        with self.assertRaises(FileExistsError):
            make_dir(nested, can_exist=False)

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(ValueError):
            make_dir(filepath)

        halfway = os.path.join(TEST_MONITOR_BASE, "A", "B")
        make_dir(halfway, ensure_clean=True)
        self.assertTrue(os.path.exists(halfway))
        self.assertEqual(len(os.listdir(halfway)), 0)

    # Test that rmtree removes a directory, its content, and subdirectory
    def testRemoveTree(self)->None:
        nested = os.path.join(TEST_MONITOR_BASE, "A", "B")
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        make_dir(nested)
        self.assertTrue(os.path.exists(nested))

        rmtree(os.path.join(TEST_MONITOR_BASE, "A"))
        self.assertTrue(os.path.exists(TEST_MONITOR_BASE))
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        self.assertFalse(os.path.exists(
            os.path.join(TEST_MONITOR_BASE, "A", "B")))

    # Test lines to str
    def testLinesToStr(self)->None:
        l = ["a", "b", "c"]

        self.assertEqual(lines_to_string(l), "a\nb\nc")

    def testThreadsafeWriteStatus(self)->None:
        first_yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        self.assertFalse(os.path.exists(filepath))
        threadsafe_write_status(first_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'A: a\n', 
            'B: 1\n',
            'C:\n', 
            '  D: true\n',
            '  E:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n'
        ]
        
        self.assertEqual(data, expected_bytes)

        second_yaml_dict = {
            "F": "a",
            "G": 1,
            "H": {
                "I": True,
                "J": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        threadsafe_write_status(second_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'F: a\n', 
            'G: 1\n',
            'H:\n', 
            '  I: true\n',
            '  J:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n'
        ]
        
        self.assertEqual(data, expected_bytes)

    def testThreadsafeReadStatus(self)->None:
        yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")
        threadsafe_write_status(yaml_dict, filepath)

        read_dict = threadsafe_read_status(filepath)
        self.assertEqual(yaml_dict, read_dict)

        with self.assertRaises(FileNotFoundError):
            threadsafe_read_status("doesNotExist")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        data = threadsafe_read_status(filepath)
        self.assertEqual(data, "Data")

    def testThreadsafeUpdateStatus(self)->None:
        first_yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        self.assertFalse(os.path.exists(filepath))
        threadsafe_write_status(first_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'A: a\n', 
            'B: 1\n',
            'C:\n', 
            '  D: true\n',
            '  E:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n'
        ]
        
        self.assertEqual(data, expected_bytes)

        second_yaml_dict = {
            "B": 42,
            "C": {
                "E": [
                    1, 2, 3, 4
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        threadsafe_update_status(second_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'A: a\n', 
            'B: 42\n',
            'C:\n', 
            '  E:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n',
            '  - 4\n'
        ]
        
        self.assertEqual(data, expected_bytes)

    def testThreadsafeUpdateProctectedStatus(self)->None:
        first_yaml_dict = {
            JOB_CREATE_TIME: "now",
            JOB_STATUS: "Wham",
            JOB_ERROR: "first error.",
            JOB_ID: "id",
            JOB_TYPE: "type"
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        self.assertFalse(os.path.exists(filepath))
        threadsafe_write_status(first_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        status = threadsafe_read_status(filepath)
        
        self.assertEqual(first_yaml_dict, status)

        second_yaml_dict = {
            JOB_CREATE_TIME: "changed",
            JOB_STATUS: STATUS_DONE,
            JOB_ERROR: "changed.",
            JOB_ID: "changed"
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        threadsafe_update_status(second_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        status = threadsafe_read_status(filepath)

        expected_second_yaml_dict = {
            JOB_CREATE_TIME: "now",
            JOB_STATUS: STATUS_DONE,
            JOB_ERROR: "first error. changed.",
            JOB_ID: "changed",
            JOB_TYPE: "type"
        }
        
        self.assertEqual(expected_second_yaml_dict, status)

        third_yaml_dict = {
            JOB_CREATE_TIME: "editted",
            JOB_STATUS: "editted",
            JOB_ERROR: "editted.",
            JOB_ID: "editted",
            "something_new": "new"
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        threadsafe_update_status(third_yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(os.path.exists(filepath + LOCK_EXT))

        status = threadsafe_read_status(filepath)

        expected_third_yaml_dict = {
            JOB_CREATE_TIME: "now",
            JOB_STATUS: STATUS_DONE,
            JOB_ERROR: "first error. changed. editted.",
            JOB_ID: "editted",
            JOB_TYPE: "type",
            "something_new": "new"
        }

        print(expected_third_yaml_dict)
        print(status)
        
        self.assertEqual(expected_third_yaml_dict, status)


class HashingTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that get_hash produces the expected hash
    def testGetFileHashSha256(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "hased_file.txt")
        with open(file_path, 'w') as hashed_file:
            hashed_file.write("Some data\n")
        expected_hash = \
            "8557122088c994ba8aa5540ccbb9a3d2d8ae2887046c2db23d65f40ae63abade"
        
        hash = get_hash(file_path, SHA256)
        self.assertEqual(hash, expected_hash)
    
    # Test that get_hash raises on a missing file
    def testGetFileHashSha256NoFile(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "file.txt")

        with self.assertRaises(FileNotFoundError):        
            get_hash(file_path, SHA256)


class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that create_event produces valid event dictionary
    def testCreateEvent(self)->None:
        pattern = FileEventPattern(
            "pattern", 
            "file_path", 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":"result_path"
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        event = create_event("test", "path", rule, time())

        self.assertEqual(type(event), dict)
        self.assertEqual(len(event.keys()), len(EVENT_KEYS))
        for key, value in EVENT_KEYS.items():
            self.assertTrue(key in event.keys())
            self.assertIsInstance(event[key], value)
        self.assertEqual(event[EVENT_TYPE], "test")
        self.assertEqual(event[EVENT_PATH], "path")
        self.assertEqual(event[EVENT_RULE], rule)

        event2 = create_event(
            "test2", "path2", rule, time(), extras={"a":1}
        )

        self.assertEqual(type(event2), dict)
        self.assertEqual(len(event.keys()), len(EVENT_KEYS))
        for key, value in EVENT_KEYS.items():
            self.assertTrue(key in event.keys())
            self.assertIsInstance(event[key], value)
        self.assertEqual(event2[EVENT_TYPE], "test2")
        self.assertEqual(event2[EVENT_PATH], "path2")
        self.assertEqual(event2[EVENT_RULE], rule)
        self.assertEqual(event2["a"], 1)

    # Test that create_job_metadata_dict produces valid job dictionary
    def testCreateJob(self)->None:
        pattern = FileEventPattern(
            "pattern", 
            "file_path", 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":"result_path"
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        event = create_event(
            EVENT_TYPE_WATCHDOG,
            "file_path",
            rule,
            time(),
            extras={
                WATCHDOG_BASE: TEST_MONITOR_BASE,
                EVENT_RULE: rule,
                WATCHDOG_HASH: "file_hash"
            }
        )

        job_dict = create_job_metadata_dict(
            JOB_TYPE_PAPERMILL,
            event,
            extras={
                JOB_PARAMETERS:{
                    "extra":"extra",
                    "infile":"file_path",
                    "outfile":"result_path"
                },
                PYTHON_FUNC:max
            }
        )

        self.assertIsInstance(job_dict, dict)
        self.assertIn(JOB_ID, job_dict)
        self.assertIsInstance(job_dict[JOB_ID], str)
        self.assertIn(JOB_EVENT, job_dict)
        self.assertEqual(job_dict[JOB_EVENT], event)
        self.assertIn(JOB_TYPE, job_dict)
        self.assertEqual(job_dict[JOB_TYPE], JOB_TYPE_PAPERMILL)
        self.assertIn(JOB_PATTERN, job_dict)
        self.assertEqual(job_dict[JOB_PATTERN], pattern.name)
        self.assertIn(JOB_RECIPE, job_dict)
        self.assertEqual(job_dict[JOB_RECIPE], recipe.name)
        self.assertIn(JOB_RULE, job_dict)
        self.assertEqual(job_dict[JOB_RULE], rule.name)
        self.assertIn(JOB_STATUS, job_dict)
        self.assertEqual(job_dict[JOB_STATUS], STATUS_CREATING)
        self.assertIn(JOB_CREATE_TIME, job_dict)
        self.assertIsInstance(job_dict[JOB_CREATE_TIME], datetime)
        self.assertIn(JOB_REQUIREMENTS, job_dict)
        self.assertEqual(job_dict[JOB_REQUIREMENTS], {})

    # Test that replace_keywords replaces MEOW keywords in a given dictionary
    def testReplaceKeywords(self)->None:
        test_dict = {
            "A": f"--{KEYWORD_PATH}--",
            "B": f"--{KEYWORD_REL_PATH}--",
            "C": f"--{KEYWORD_DIR}--",
            "D": f"--{KEYWORD_REL_DIR}--",
            "E": f"--{KEYWORD_FILENAME}--",
            "F": f"--{KEYWORD_PREFIX}--",
            "G": f"--{KEYWORD_BASE}--",
            "H": f"--{KEYWORD_EXTENSION}--",
            "I": f"--{KEYWORD_JOB}--",
            "J": f"--{KEYWORD_PATH}-{KEYWORD_PATH}--",
            "K": f"{KEYWORD_PATH}",
            "L": f"--{KEYWORD_PATH}-{KEYWORD_REL_PATH}-{KEYWORD_DIR}-"
                 f"{KEYWORD_REL_DIR}-{KEYWORD_FILENAME}-{KEYWORD_PREFIX}-"
                 f"{KEYWORD_BASE}-{KEYWORD_EXTENSION}-{KEYWORD_JOB}--",
            "M": "A",
            "N": 1
        }

        p = FileEventPattern("p", "tp", "r", "tf")
        r = SharedTestRecipe("r", "something")
        rule = Rule(p, r)

        event = create_watchdog_event(
            os.path.join("base", "src", "dir", "file.ext"),
            rule,
            os.path.join("base", "monitor", "dir"),
            time(),
            "hash"
        )

        replaced = replace_keywords(
            test_dict, 
            "job_id", 
            event
        )

        self.assertIsInstance(replaced, dict)
        self.assertEqual(len(test_dict.keys()), len(replaced.keys()))
        for k in test_dict.keys():
            self.assertIn(k, replaced)

        self.assertEqual(replaced["A"], 
            os.path.join("--base", "src", "dir", "file.ext--"))
        self.assertEqual(replaced["B"], 
            os.path.join("--..", "..", "src", "dir", "file.ext--"))
        self.assertEqual(replaced["C"], 
            os.path.join("--base", "src", "dir--"))
        self.assertEqual(replaced["D"], 
            os.path.join("--..", "..", "src", "dir--"))
        self.assertEqual(replaced["E"], "--file.ext--")
        self.assertEqual(replaced["F"], "--file--")
        self.assertEqual(replaced["G"], 
            os.path.join("--base", "monitor", "dir--"))
        self.assertEqual(replaced["H"], "--.ext--")
        self.assertEqual(replaced["I"], "--job_id--")
        self.assertEqual(replaced["J"], 
            os.path.join("--base", "src", "dir", "file.ext-base", "src", "dir", "file.ext--"))
        self.assertEqual(replaced["K"], 
            os.path.join("base", "src", "dir", "file.ext"))
        self.assertEqual(replaced["L"], 
            os.path.join("--base", "src", "dir", "file.ext-..", "..", "src", "dir", "file.ext-base", "src", "dir-"
            "..", "..", "src", "dir-file.ext-file-base", "monitor", "dir-.ext-job_id--"))
        self.assertEqual(replaced["M"], "A") 
        self.assertEqual(replaced["N"], 1) 

    # Test that create_rule creates a rule from pattern and recipe
    def testCreateRule(self)->None:
        rule = create_rule(valid_pattern_one, valid_recipe_one)

        self.assertIsInstance(rule, Rule)

        with self.assertRaises(ValueError):
            rule = create_rule(valid_pattern_one, valid_recipe_two)
    
    # Test that create_rules creates nothing from nothing
    def testCreateRulesMinimum(self)->None:
        rules = create_rules({}, {})

        self.assertEqual(len(rules), 0)

    # Test that create_rules creates rules from meow_base.patterns and recipes
    def testCreateRulesPatternsAndRecipesDicts(self)->None:
        patterns = {
            valid_pattern_one.name: valid_pattern_one,
            valid_pattern_two.name: valid_pattern_two
        }
        recipes = {
            valid_recipe_one.name: valid_recipe_one,
            valid_recipe_two.name: valid_recipe_two
        }
        rules = create_rules(patterns, recipes)
        self.assertIsInstance(rules, Dict)
        self.assertEqual(len(rules), 2)
        for k, rule in rules.items():
            self.assertIsInstance(k, str)
            self.assertIsInstance(rule, Rule)
            self.assertEqual(k, rule.name)

    # Test that create_rules creates nothing from invalid pattern inputs
    def testCreateRulesMisindexedPatterns(self)->None:
        patterns = {
            valid_pattern_two.name: valid_pattern_one,
            valid_pattern_one.name: valid_pattern_two
        }
        with self.assertRaises(KeyError):
            create_rules(patterns, {})

    # Test that create_rules creates nothing from invalid recipe inputs
    def testCreateRulesMisindexedRecipes(self)->None:
        recipes = {
            valid_recipe_two.name: valid_recipe_one,
            valid_recipe_one.name: valid_recipe_two
        }
        with self.assertRaises(KeyError):
            create_rules({}, recipes)

    # Test create parameter sweep function
    def testCreateParameterSweep(self)->None:
        create_parameter_sweep("name", 0, 10, 2)
        create_parameter_sweep("name", 10, 0, -2)
        create_parameter_sweep("name", 0.0, 10.0, 1.3)
        create_parameter_sweep("name", 10.0, 0.0, -1.3)

        with self.assertRaises(TypeError):
            create_parameter_sweep(0, 0, 10, 2)

        with self.assertRaises(TypeError):
            create_parameter_sweep("name", "0", 10, 2)

        with self.assertRaises(TypeError):
            create_parameter_sweep("name", 0, "10", 2)

        with self.assertRaises(TypeError):
            create_parameter_sweep("name", 0, 10, "2")

        with self.assertRaises(ValueError):
            create_parameter_sweep("name", 0, 10, 0)

        with self.assertRaises(ValueError):
            create_parameter_sweep("name", 0, 10, -1)

        with self.assertRaises(ValueError):
            create_parameter_sweep("name", 10, 0, 1)


class NamingTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that generate_id creates unique ids    
    def testGenerateIDWorking(self)->None:
        id = _generate_id()
        self.assertEqual(len(id), 16)
        for i in range(len(id)):
            self.assertIn(id[i], CHAR_UPPERCASE+CHAR_LOWERCASE)

        # In extrememly rare cases this may fail due to randomness in algorithm
        new_id = _generate_id(existing_ids=[id])
        self.assertNotEqual(id, new_id)

        another_id = _generate_id(length=32)
        self.assertEqual(len(another_id), 32)

        again_id = _generate_id(charset="a")
        for i in range(len(again_id)):
            self.assertIn(again_id[i], "a")

        with self.assertRaises(ValueError):
            _generate_id(length=2, charset="a", existing_ids=["aa"])

        prefix_id = _generate_id(length=4, prefix="Test")
        self.assertEqual(prefix_id, "Test")

        prefix_id = _generate_id(prefix="Test")
        self.assertEqual(len(prefix_id), 16)
        self.assertTrue(prefix_id.startswith("Test"))


class NotificationTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that send email sends emails   
    def testSendEmail(self)->None:
        controller = Controller(EmailHandler(), hostname="localhost", port=1025)
        controller.start()

        self.assertEqual(len(controller.handler.messages), 0)

        send_email(
            "localhost:1025",
            "sender@localhost",
            "reciever@localhost",
            "test test test"
        )

        sleep(1)

        controller.stop()
        self.assertEqual(len(controller.handler.messages), 1)
        self.assertEqual(
            controller.handler.messages[0].mail_from, "sender@localhost")
        self.assertEqual(
            controller.handler.messages[0].rcpt_tos, ["reciever@localhost"])
        self.assertEqual(
            controller.handler.messages[0].content, b"test test test\r\n")


class ParameterisationTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that parameterize_jupyter_notebook parameterises given notebook
    def testParameteriseNotebook(self)->None:
        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"a": 4})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"s": 4})

        self.assertNotEqual(pn, COMPLETE_NOTEBOOK)
        self.assertEqual(
            pn["cells"][0]["source"], 
            "# The first cell\n\ns = 4\nnum = 1000")

    # Test that parameterize_python_script parameterises given script
    def testParameteriseScript(self)->None:
        ps = parameterize_python_script(
            COMPLETE_PYTHON_SCRIPT, {})
        
        self.assertEqual(ps, COMPLETE_PYTHON_SCRIPT)

        ps = parameterize_python_script(
            COMPLETE_PYTHON_SCRIPT, {"a": 50})
        
        self.assertEqual(ps, COMPLETE_PYTHON_SCRIPT)

        ps = parameterize_python_script(
            COMPLETE_PYTHON_SCRIPT, {"num": 50})

        self.assertNotEqual(ps, COMPLETE_PYTHON_SCRIPT)
        self.assertEqual(ps[2], "num = 50")


class ProcessIoTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test that wait can wait on multiple pipes
    def testWaitPipes(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        
        inputs = [
            pipe_one_reader, pipe_two_reader
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            elif readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

    # Test that wait can wait on multiple queues
    def testWaitQueues(self)->None:
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            queue_one, queue_two
        ]

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

    # Test that wait can wait on multiple pipes and queues
    def testWaitPipesAndQueues(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            pipe_one_reader, pipe_two_reader, queue_one, queue_two
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            if readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

        queue_one.put(1)
        pipe_one_writer.send(1)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == pipe_one_reader:
                msg = readable.recv()
                self.assertEqual(msg, 1)


class RequirementsTest(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test structure of Python requirement testings
    def testPythonRequirementStructuring(self)->None:
        key, reqs = create_python_requirements()

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(reqs, {})

        key, reqs = create_python_requirements(modules="first")

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(len(reqs), 1)
        self.assertIn(REQ_PYTHON_MODULES, reqs)
        self.assertEqual(len(reqs[REQ_PYTHON_MODULES]), 1)
        self.assertIsInstance(reqs[REQ_PYTHON_MODULES], list)
        self.assertEqual(reqs[REQ_PYTHON_MODULES], ["first"])

        key, reqs = create_python_requirements(modules=["first", "second"])

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(len(reqs), 1)
        self.assertIn(REQ_PYTHON_MODULES, reqs)
        self.assertEqual(len(reqs[REQ_PYTHON_MODULES]), 2)
        self.assertIsInstance(reqs[REQ_PYTHON_MODULES], list)
        self.assertEqual(reqs[REQ_PYTHON_MODULES], ["first", "second"])

        key, reqs = create_python_requirements(version="3.10.6")

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(len(reqs), 1)
        self.assertIn(REQ_PYTHON_VERSION, reqs)
        self.assertIsInstance(reqs[REQ_PYTHON_VERSION], str)
        self.assertEqual(reqs[REQ_PYTHON_VERSION], "3.10.6")

        key, reqs = create_python_requirements(environment="env")

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(len(reqs), 1)
        self.assertIn(REQ_PYTHON_ENVIRONMENT, reqs)
        self.assertIsInstance(reqs[REQ_PYTHON_ENVIRONMENT], str)
        self.assertEqual(reqs[REQ_PYTHON_ENVIRONMENT], "env")

        key, reqs = create_python_requirements(
            modules=["first", "second"],
            version="3.10.6",
            environment="env"
        )

        self.assertIsInstance(key, str)
        self.assertEqual(key, REQUIREMENT_PYTHON)
        self.assertIsInstance(reqs, dict)
        self.assertEqual(len(reqs), 3)
        self.assertIn(REQ_PYTHON_MODULES, reqs)
        self.assertEqual(len(reqs[REQ_PYTHON_MODULES]), 2)
        self.assertIsInstance(reqs[REQ_PYTHON_MODULES], list)
        self.assertEqual(reqs[REQ_PYTHON_MODULES], ["first", "second"])
        self.assertIn(REQ_PYTHON_VERSION, reqs)
        self.assertIsInstance(reqs[REQ_PYTHON_VERSION], str)
        self.assertEqual(reqs[REQ_PYTHON_VERSION], "3.10.6")
        self.assertIn(REQ_PYTHON_ENVIRONMENT, reqs)
        self.assertIsInstance(reqs[REQ_PYTHON_ENVIRONMENT], str)
        self.assertEqual(reqs[REQ_PYTHON_ENVIRONMENT], "env")

    # Test version values of Python requirement testings
    def testPythonRequirementsVersion(self)->None:
        key, python_reqs = create_python_requirements(version="3.10.6")

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertTrue(status)

        key, python_reqs = create_python_requirements(version="2.5.9")

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertTrue(status)

        key, python_reqs = create_python_requirements(version="4.1.1")

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertFalse(status)

    # Test module values of Python requirement testings
    def testPythonRequirementsModules(self)->None:
        key, python_reqs = create_python_requirements(modules=[
            "papermill", "sys", "typing"
        ])

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertTrue(status)

        key, python_reqs = create_python_requirements(modules=[
            "does", "not", "exist"
        ])

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertFalse(status)

        key, python_reqs = create_python_requirements(modules=[
            "papermill", "sys", "doesnotexist"
        ])

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertFalse(status)

    # TODO make this test portable
    def testPythonRequirementModuleVersions(self)->None:
        key, python_reqs = create_python_requirements(
            modules="papermill==2.4.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill<4")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill<1.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertFalse(status)

        key, python_reqs = create_python_requirements(
            modules="papermill>4")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertFalse(status)

        key, python_reqs = create_python_requirements(
            modules="papermill>1.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill<=4")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill<=2.4.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill<=1.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertFalse(status)

        key, python_reqs = create_python_requirements(
            modules="papermill>=4")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertFalse(status)

        key, python_reqs = create_python_requirements(
            modules="papermill>=2.4.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

        key, python_reqs = create_python_requirements(
            modules="papermill>=1.0")
        reqs = { key: python_reqs }
        status, _ = check_requirements(reqs)
        self.assertTrue(status)

    # Test environment value of Python requirement testings
    def testPythonRequirementsEnvironment(self)->None:
        # TODO rework this test so that it actually creates and runs in a new 
        # environment
        if prefix != base_prefix: 
            key, python_reqs = create_python_requirements(
                environment=basename(prefix)
            )

            reqs = {
                key: python_reqs
            }

            status, _ = check_requirements(reqs)

            self.assertTrue(status)

        key, python_reqs = create_python_requirements(environment="bad_env")

        reqs = {
            key: python_reqs
        }

        status, _ = check_requirements(reqs)

        self.assertFalse(status)
